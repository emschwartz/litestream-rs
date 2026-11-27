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
use std::time::Duration;

use lsr_ltx::{Timestamp, Txid};
use lsr_wal::CommittedEnd;

use super::{Actor, Task, Tick, Waiter};
use crate::failstop::{Observed, Trip, WalMark, trip};
use crate::fault::{Fault, Next, SnapshotCause};
use crate::layout::FileKey;
use crate::source::WalSource;
use crate::sync::StepEnd;

/// How long boot attempts may keep failing, none by a refusal, before the boot is judged without
/// having decided how to start, so a store that is down does not hold the application's writes
/// (Go holds nothing at start). One failed attempt never judges it: the boot fails on purpose to
/// read a staged file twice, and one transient error says nothing about the replica's files, so
/// the attempt after it may still find another writer (`docs/decisions/D207.md`).
pub(crate) const BOOT_UNDECIDED_BOUND: Duration = Duration::from_secs(15);

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
        // The database's first copy, a replica ahead of us, a rewind, staged files deleted:
        // commits may be in the file.
        SnapshotCause::FirstSync
        | SnapshotCause::BehindRemote { .. }
        | SnapshotCause::LocalRewound { .. }
        | SnapshotCause::StagedUnverified { .. }
        | SnapshotCause::AheadOfReplica { .. } => false,
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
        // Held writes judge no boot: nothing a boot does before it decides writes to the
        // database, so a hold can only meet a judged start. Nor does a failure of any task
        // but the boot's (a local course's own sync step fails on the sync task).
        if task != self.boot_task() || matches!(fault, Some(Fault::WritesHeld)) {
            return;
        }
        if !self.kept.listed {
            self.kept.refused_at_boot = if let Some(Fault::StoreDenied(why)) = fault {
                Some(why)
            } else {
                None
            };
            // A refusal ends the run: it holds until healed, and what follows starts afresh.
            self.kept.boot_failing_since = match self.kept.refused_at_boot {
                Some(_) => None,
                None => self.kept.boot_failing_since.or(Some(now)),
            };
        }
    }

    /// The time the boot is judged for having failed too long, if a run of failures is on.
    pub(super) fn boot_overdue_at(&self) -> Option<Timestamp> {
        self.kept
            .boot_failing_since
            .filter(|_| !self.kept.listed && self.kept.refused_at_boot.is_none())
            .map(|since| since.saturating_add(BOOT_UNDECIDED_BOUND))
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
        self.kept.boot_overdue = self.boot_overdue_at().is_some_and(|at| now >= at);
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

    /// Notes the snapshot a step just staged as `key` as quiet when it was staged for a cause
    /// that is a doubt about the WAL's continuity rather than a finding of unstaged frames, and
    /// the WAL held nothing past the synced position (or past the doubt's baseline): it holds no
    /// commit the staged files before it do not, as far as the engine can tell. Whether those
    /// files are on the replica yet does not matter, as it does not for the files a boot finds
    /// quiet (`crate::pages`): while one is pending the clock runs for it, and once the replica
    /// holds them a quiet snapshot is no lag. A commit the engine never saw (made and
    /// checkpointed away by another process between two ticks) would be missed until the next
    /// commit starts the clock: a late trip, never a false one.
    pub(super) fn note_quiet_snapshot(
        &mut self,
        cause: SnapshotCause,
        key: FileKey,
        end: &StepEnd,
    ) {
        let quiet_end = self.ended_where_synced(end) || self.ended_at_doubt_baseline(end);
        if doubt_only(cause) && quiet_end {
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

    /// Takes the quiet files a boot found among the pending ones.
    pub(super) fn quiet_at_boot(&mut self, quiet: BTreeSet<Txid>) {
        self.kept.quiet = quiet;
    }

    fn wal_ahead(&self, end: CommittedEnd) -> bool {
        match self.position() {
            Some(position) if position.cursor().salts() == end.salts() => {
                end.offset().frame_index() > position.synced().offset().frame_index()
            }
            Some(_) | None => end.offset().frame_index() > 0,
        }
    }

    /// The tick, waking no later than the next probe is due, or the bound on a failing boot.
    pub(super) fn wake_for_probe(&self, tick: Tick) -> Tick {
        let probe = self.kept.lag.next_probe(self.kept.cfg.max_unreplicated_age);
        // A boot that keeps failing is judged at the bound, however long its retry waits.
        let overdue = self.boot_overdue_at().filter(|_| !self.kept.boot_overdue);
        let Some(probe) = probe.into_iter().chain(overdue).min() else {
            return tick;
        };
        tick.map_next(|next| match next {
            Next::Now => Next::Now,
            Next::At(at) => Next::At(at.min(probe)),
            Next::OnWalChange => Next::At(probe),
        })
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
    /// listed and the boot decided how this start proceeds (continue from a position, or
    /// snapshot for a named cause), without waiting for that snapshot to be staged; a stop the
    /// snapshot finds later is an ordinary stop (`docs/decisions/D201.md`). Or boot attempts
    /// have kept failing for [`BOOT_UNDECIDED_BOUND`], none of the last run by the store refusing
    /// access (a store that is down at boot does not hold the application for long). One
    /// transient error judges nothing.
    #[must_use]
    pub const fn boot_judged(&self) -> bool {
        self.kept.listed || self.kept.boot_overdue
    }

    /// Whether an engine of this process has listed the replica.
    #[must_use]
    pub const fn listed(&self) -> bool {
        self.kept.listed
    }
}
