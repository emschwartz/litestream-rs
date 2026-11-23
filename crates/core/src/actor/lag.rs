//! What the actor feeds the fail-stop clock (`crate::failstop`): after each tick, whether
//! anything committed is off the replica and the oldest pending staged file's stamp; on each
//! failure of the sync or upload task, the time; at the bound, the probe that lets those tasks'
//! waits go.
//!
//! Lag is judged only by a booted engine: before the first boot of the process nothing says
//! which staged files the replica lacks, so the clock covers nothing yet, and fail-stop can
//! only trip for a stop or a refusal at boot (`docs/decisions.md` D80). A rebuilt engine
//! keeps the clock it had.

use std::collections::BTreeSet;

use lsr_ltx::{Timestamp, Txid};
use lsr_wal::CommittedEnd;

use super::{Actor, Task, Tick, Waiter};
use crate::failstop::{Observed, Trip, WalMark, trip};
use crate::fault::{Fault, Next, SnapshotCause};
use crate::layout::FileKey;
use crate::source::WalSource;
use crate::sync::StepEnd;

/// Whether a snapshot for `cause` is a doubt about continuity, not a finding of commits the
/// staged files lack.
const fn doubt_only(cause: SnapshotCause) -> bool {
    match cause {
        SnapshotCause::OperatorRequested
        | SnapshotCause::GenerationChanged
        | SnapshotCause::WalMissing
        | SnapshotCause::PrevFrameMismatch
        | SnapshotCause::LastPageMismatch
        | SnapshotCause::NewestStagedFileUnreadable => true,
        // The database's first copy, a replica ahead of us, frames a checkpoint or a failed
        // sync moved past staging, a rewind, staged files deleted: commits may be in the file.
        SnapshotCause::FirstSync
        | SnapshotCause::BehindRemote { .. }
        | SnapshotCause::LocalRewound { .. }
        | SnapshotCause::CheckpointBoundary
        | SnapshotCause::StagedUnverified { .. } => false,
    }
}

/// The clock's mark of a committed WAL end.
const fn mark_of(end: CommittedEnd) -> WalMark {
    WalMark::new(end.salts(), end.offset().frame_index())
}

/// Whether a failure of the task can keep a commit off the replica: the tasks a commit passes
/// through on its way there. A blocked checkpoint, a prune or maintenance never does.
pub(super) const fn carries_commits(task: Task) -> bool {
    match task {
        Task::Sync | Task::Upload => true,
        Task::Checkpoint | Task::Prune | Task::Maintenance => false,
    }
}

impl Actor {
    /// Notes a failure for the clock, and for the refusal at boot.
    pub(super) fn lag_failed(&mut self, waiter: Waiter, fault: Option<Fault>, now: Timestamp) {
        let Waiter::Task(task) = waiter else {
            return;
        };
        if !carries_commits(task) {
            return;
        }
        self.kept.lag.failed(now);
        if task != Task::Sync {
            return;
        }
        if self.kept.listed {
            self.kept.sync_failed_after_listing = true;
        } else {
            self.kept.boot_failed = true;
            self.kept.refused_at_boot = if let Some(Fault::StoreDenied(why)) = fault {
                Some(why)
            } else {
                None
            };
        }
    }

    /// Lets the sync and upload waits go when the bound has passed with no probe made for it.
    pub(super) fn probe_at_bound(&mut self, now: Timestamp) {
        if self
            .kept
            .lag
            .probe_due(self.kept.cfg.max_unreplicated_age, now)
        {
            self.kept.tasks.get_mut(Task::Sync).probe();
            self.kept.tasks.get_mut(Task::Upload).probe();
        }
    }

    /// After a tick: what is off the replica, for the clock.
    pub(super) fn observe_lag<W: WalSource>(&mut self, wal: &mut W, now: Timestamp) {
        if self.booted() {
            self.kept.listed = true;
            self.kept.refused_at_boot = None;
        } else {
            // Nothing to judge until a boot says what the replica holds; a rebuilt engine
            // keeps the clock it had, untouched.
            return;
        }
        if let Some(replicated) = self.replicated() {
            self.kept.quiet.retain(|txid| *txid > replicated);
        }
        let end = wal.committed().ok().map(|read| read.end());
        self.note_doubt_baseline(end);
        let seen = self.observed(end);
        let bound = self.kept.cfg.max_unreplicated_age;
        self.kept.lag.observe(seen, bound, now);
    }

    /// While a snapshot is owed for a doubt alone, with no synced position and every staged
    /// file on the replica, notes where the WAL ended when the doubt was first observed: the
    /// engine cannot tell which of those frames the replica holds, and a doubt is not a finding
    /// of commits (`docs/decisions.md` D81), so only frames past that end are lag. Noted only
    /// while no clock runs; a commit that landed between the boot and this observation is
    /// missed until the snapshot stages it, a late trip, never a false one.
    fn note_doubt_baseline(&mut self, end: Option<CommittedEnd>) {
        let doubting = self.position().is_none()
            && self.pending().is_some_and(doubt_only)
            && self.staged() <= self.replicated();
        if !doubting {
            self.kept.doubt_baseline = None;
        } else if self.kept.doubt_baseline.is_none() && self.kept.lag.since().is_none() {
            self.kept.doubt_baseline = end.map(mark_of);
        }
    }

    /// What is off the replica after a tick: WAL frames past the synced position (or past the
    /// doubt's baseline), the newest and the oldest pending staged file holding a commit, and
    /// a snapshot owed while lag was already running for a cause that found commits past
    /// staging (they are in the database file only). A wal-index that cannot be read says
    /// nothing new: frames the clock dated stay dated.
    fn observed(&self, end: Option<CommittedEnd>) -> Observed {
        let was_behind = self.kept.lag.since().is_some();
        let ahead = match end {
            Some(end) => Some(mark_of(end))
                .filter(|mark| self.wal_ahead(end) && self.kept.doubt_baseline != Some(*mark)),
            None => self.kept.lag.unstaged_mark(),
        };
        let synced = self.position().map(|position| {
            WalMark::new(
                position.cursor().salts(),
                position.synced().offset().frame_index(),
            )
        });
        Observed {
            ahead,
            synced,
            staged: self.staged(),
            pending: self.newest_pending_with_commits(),
            replicated: self.replicated(),
            oldest: self.engine.witnesses.oldest_stamp_but(&self.kept.quiet),
            owed: was_behind && self.pending().is_some_and(|cause| !doubt_only(cause)),
        }
    }

    /// The newest staged file the replica lacks that is not known to hold no commit.
    fn newest_pending_with_commits(&self) -> Option<Txid> {
        let staged = self.staged()?;
        let first = self.replicated().map_or(Some(Txid::MIN), Txid::next)?;
        let mut at = staged;
        // Bounded by the quiet marks: each step down passes one.
        while self.kept.quiet.contains(&at) {
            at = at.prev().filter(|prev| *prev >= first)?;
        }
        (at >= first).then_some(at)
    }

    /// Notes the snapshot a step just staged as `key` as quiet when it was staged while nothing
    /// was known off the replica, for a cause that is a doubt about the WAL's continuity rather
    /// than a finding of unstaged frames, and the WAL held nothing past the synced position: it
    /// holds what the replica already holds, as far as the engine can tell. A commit the engine never saw (made and checkpointed away by
    /// another process between two ticks) would be missed until the next commit starts the
    /// clock: a late trip, never a false one.
    pub(super) fn note_quiet_snapshot(
        &mut self,
        cause: SnapshotCause,
        key: FileKey,
        end: &StepEnd,
    ) {
        // No clock running: the last observation found every staged file on the replica, or
        // only quiet ones.
        let caught_up = self.kept.lag.since().is_none();
        let quiet_end = self.ended_where_synced(end) || self.ended_at_doubt_baseline(end);
        if doubt_only(cause) && caught_up && quiet_end {
            self.kept.quiet.insert(key.range().max());
        }
    }

    /// Whether a snapshot step ended where the WAL held nothing past the synced position: the
    /// same generation and offset. A commit made since the last tick is in the snapshot, and
    /// then the snapshot is lag; so is one over a new generation, which the engine sees only
    /// once it holds a commit.
    fn ended_where_synced(&self, end: &StepEnd) -> bool {
        let StepEnd::Through(reached) = end else {
            return false;
        };
        let at = |position: &crate::position::Position| {
            (position.cursor().salts(), position.synced().offset())
        };
        self.position()
            .is_some_and(|was| at(&was) == at(reached.position()))
    }

    /// Whether a snapshot step with no synced position before it ended where the WAL ended when
    /// its doubt was first observed: it holds the frames the doubt was about and nothing newer.
    fn ended_at_doubt_baseline(&self, end: &StepEnd) -> bool {
        let StepEnd::Through(reached) = end else {
            return false;
        };
        let position = reached.position();
        let mark = WalMark::new(
            position.cursor().salts(),
            position.synced().offset().frame_index(),
        );
        self.kept.doubt_baseline == Some(mark)
    }

    /// Notes a file a sync step just staged as `key` as quiet: it changes no page of the state
    /// the files before it left, so it holds the engine's own write (the driver's start, the
    /// frame that moves the WAL past a checkpoint), no commit of the application's. The clock
    /// may already run for its frames while they were unstaged; they were never lag.
    pub(super) fn note_quiet_file(&mut self, key: FileKey) {
        self.kept.quiet.insert(key.range().max());
    }

    /// Takes the quiet files a boot found among the pending ones, and lets go of marks the
    /// replica has passed.
    pub(super) fn quiet_at_boot(&mut self, quiet: BTreeSet<Txid>) {
        self.kept.quiet = quiet;
    }

    /// Whether the committed WAL holds a frame past the synced position.
    fn wal_ahead(&self, end: CommittedEnd) -> bool {
        match self.position() {
            Some(position) if position.cursor().salts() == end.salts() => {
                end.offset().frame_index() > position.synced().offset().frame_index()
            }
            Some(_) | None => end.offset().frame_index() > 0,
        }
    }

    /// The tick, waking no later than the next probe is due.
    pub(super) fn wake_for_probe(&self, tick: Tick) -> Tick {
        let Some(probe) = self.kept.lag.next_probe(self.kept.cfg.max_unreplicated_age) else {
            return tick;
        };
        match tick {
            Tick::Ran { event, next } => {
                let next = match next {
                    Next::Now => Next::Now,
                    Next::At(at) => Next::At(at.min(probe)),
                    Next::OnWalChange => Next::At(probe),
                };
                Tick::Ran { event, next }
            }
            other @ (Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_)) => other,
        }
    }

    /// Why fail-stop trips for this engine now, if it does (`crate::failstop`).
    #[must_use]
    pub fn trip(&self) -> Option<Trip> {
        let stopped = match self.phase() {
            super::Phase::Stopped(cause) => Some(cause),
            super::Phase::Boot | super::Phase::Running | super::Phase::Draining { .. } => None,
        };
        let refused = self.kept.refused_at_boot.filter(|_| !self.kept.listed);
        trip(stopped, refused, self.kept.lag.tripped())
    }

    /// The start of the fail-stop clock: the oldest commit off the replica as far as it is
    /// known, `None` while caught up or before the first boot.
    #[must_use]
    pub const fn unreplicated_since(&self) -> Option<Timestamp> {
        self.kept.lag.since()
    }

    /// Whether this process's boot has been judged, for [`crate::AppGate`]: the replica was
    /// listed and the snapshot the boot asked for, if any, was staged or tried (a stop it finds
    /// is a stop at boot); or a boot attempt failed for anything but the store refusing access
    /// (a store that times out at boot does not hold the application).
    #[must_use]
    pub fn boot_judged(&self) -> bool {
        let settled = self.pending().is_none() || self.kept.sync_failed_after_listing;
        (self.kept.listed && settled)
            || (self.kept.boot_failed && self.kept.refused_at_boot.is_none())
    }

    /// Whether an engine of this process has listed the replica.
    #[must_use]
    pub const fn listed(&self) -> bool {
        self.kept.listed
    }
}
