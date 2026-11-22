//! What the actor feeds the fail-stop clock (`crate::failstop`): after each tick, whether
//! anything committed is off the replica and the oldest pending staged file's stamp; on each
//! failure of the sync or upload task, the time; at the bound, the probe that lets those tasks'
//! waits go.
//!
//! Lag is judged only by a booted engine: before the first boot of the process nothing says
//! which staged files the replica lacks, so the clock covers nothing yet, and fail-stop can
//! only trip for a stop or a refusal at boot (`docs/decisions.md` D25). A rebuilt engine
//! keeps the clock it had.

use std::collections::BTreeSet;

use lsr_ltx::{Timestamp, Txid};
use lsr_wal::CommittedEnd;

use super::{Actor, Task, Tick, Waiter};
use crate::failstop::{Trip, trip};
use crate::fault::{Fault, Next, SnapshotCause};
use crate::layout::FileKey;
use crate::source::WalSource;

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
        | SnapshotCause::WalSyncFailed
        | SnapshotCause::StagedUnverified { .. } => false,
    }
}

/// Whether a failure of the task can keep a commit off the replica: the tasks a commit passes
/// through on its way there. A blocked checkpoint, a prune or maintenance never does.
const fn carries_commits(task: Task) -> bool {
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
        if !self.kept.listed && task == Task::Sync {
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
        let was_behind = self.kept.lag.since().is_some();
        let end = wal.committed().ok().map(|read| read.end());
        let behind = self.behind(end, was_behind);
        let oldest = self.engine.witnesses.oldest_stamp_but(&self.kept.quiet);
        let bound = self.kept.cfg.max_unreplicated_age;
        self.kept.lag.observe(behind, oldest, bound, now);
    }

    /// Whether anything committed is off the replica: WAL frames past the synced position, a
    /// staged file not confirmed on the replica, or a snapshot owed while lag was already
    /// running for a cause that found commits past staging (they are in the database file
    /// only). A wal-index that cannot be read
    /// says nothing new: the lag stands as it was.
    fn behind(&self, end: Option<CommittedEnd>, was_behind: bool) -> bool {
        let wal_ahead = end.map_or(was_behind, |end| self.wal_ahead(end));
        let unconfirmed = self.staged() > self.replicated() && !self.only_quiet_pending();
        let owed = was_behind && self.pending().is_some_and(|cause| !doubt_only(cause));
        wal_ahead || unconfirmed || owed
    }

    /// Whether every staged file the replica lacks holds no commit of the application's.
    fn only_quiet_pending(&self) -> bool {
        let (Some(staged), replicated) = (self.staged(), self.replicated()) else {
            return false;
        };
        let first = replicated.map_or(Some(Txid::MIN), Txid::next);
        let Some(first) = first.filter(|first| *first <= staged) else {
            return false;
        };
        let pending = staged.get().saturating_sub(first.get()).saturating_add(1);
        let quiet = self.kept.quiet.range(first..=staged).count();
        u64::try_from(quiet).is_ok_and(|quiet| quiet == pending)
    }

    /// Notes the snapshot a step just staged as `key` as quiet when it was staged while nothing
    /// was known off the replica, for a cause that is a doubt about the WAL's continuity rather
    /// than a finding of unstaged frames: it holds what the replica already holds, as far as
    /// the engine can tell. A commit the engine never saw (made and checkpointed away by
    /// another process between two ticks) would be missed until the next commit starts the
    /// clock: a late trip, never a false one.
    pub(super) fn note_quiet_snapshot(&mut self, cause: SnapshotCause, key: FileKey) {
        let caught_up = self.kept.lag.since().is_none() && self.staged() == self.replicated();
        if doubt_only(cause) && caught_up {
            self.kept.quiet.insert(key.range().max());
        }
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

    /// Whether an engine of this process has listed the replica.
    #[must_use]
    pub const fn listed(&self) -> bool {
        self.kept.listed
    }
}
