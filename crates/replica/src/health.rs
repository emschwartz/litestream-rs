//! What a replicator reports: a [`Health`] snapshot the thread writes after every tick, and
//! the outcomes of the first boot ([`Readiness`]) and of a stop ([`StopOutcome`]).
//!
//! A snapshot is a copy: reading it takes one lock for as long as a clone, never across I/O.
//!
//! The application's own checkpoints are a notice ([`AppNotice`]), never a degraded state: the read lock
//! keeps them from costing the replica anything, and what they cost the application is said in the
//! notice's text.

use std::fmt;

use lsr_core::{
    AppCheckpoint, AppCheckpoints, Degraded, DegradedCause, FinalRetention, FinalSnapshot, Gate,
    Severity, SnapshotCause, SnapshotCounts, StopCause, Task, Trip, Unfinished, WhyBlocked,
    WorkCounts,
};
use lsr_ltx::{Timestamp, Txid};

/// What the replicator is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The first boot (listing the replica, recovering the position, deciding how to start)
    /// has not finished.
    Booting,
    /// Replicating.
    Running,
    /// A stop was asked for: only sync and upload run, until the deadline.
    Draining,
    /// Stopped by a cause, or by a drain that ended. Nothing runs.
    Stopped(StopCause),
    /// The thread is not there.
    Gone(Gone),
}

/// Why the replicator thread is not there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gone {
    /// The thread ended without saying why: a panic outside the part of the loop that catches
    /// them. Under `panic = "abort"` nothing is caught at all, and the process ends instead.
    ThreadDied,
    /// This process is a fork of the one that started the replicator: the thread exists only
    /// in the parent.
    Forked,
}

/// How the first boot went, as [`crate::Replicator::wait_booted`] reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Readiness {
    /// The replica was listed and the position recovered.
    Ready,
    /// The boot keeps failing with this cause; it keeps trying.
    Degraded(DegradedCause),
    /// The wait ran out first.
    TimedOut,
    /// The replicator stopped before it booted.
    Stopped(StopCause),
    /// The thread is not there.
    Gone(Gone),
}

/// How a stop ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopOutcome {
    /// Everything committed before the stop is on the replica, through `txid` (`None` when the
    /// database never committed anything).
    Drained {
        /// The newest transaction on the replica.
        txid: Option<Txid>,
    },
    /// Time ran out with work left. The wal-index gives a frame offset, not a txid, so what is
    /// left is measured in bytes and files.
    DeadlineReached(Unfinished),
    /// Already stopped by a cause (another writer, or a configured stop) before the drain.
    Stopped(StopCause),
    /// The thread is not there.
    Gone(Gone),
}

/// One store operation's totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OpCounters {
    /// Calls made.
    pub calls: u64,
    /// Calls that returned an error.
    pub errors: u64,
    /// Time spent in them on the replicator's clock, in milliseconds.
    pub millis: u64,
}

/// Every store operation's totals (Go's per-operation replica metrics).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StoreCounters {
    /// Listing pages.
    pub list: OpCounters,
    /// Reads, with or without the created-at time.
    pub read: OpCounters,
    /// Created-at time reads.
    pub created_at: OpCounters,
    /// Uploads begun.
    pub begin_upload: OpCounters,
    /// Upload chunks written.
    pub write: OpCounters,
    /// Uploads committed.
    pub commit: OpCounters,
    /// Uploads aborted.
    pub abort: OpCounters,
    /// Delete calls.
    pub delete: OpCounters,
    /// Bytes of committed uploads.
    pub bytes_uploaded: u64,
}

/// Running totals since the replicator started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Counters {
    /// Ticks run.
    pub ticks: u64,
    /// Sync steps that staged a file.
    pub syncs: u64,
    /// Checkpoints run, and how many of them SQLite could not finish.
    pub checkpoints: u64,
    /// Checkpoints SQLite reported busy.
    pub checkpoints_busy: u64,
    /// Files uploaded.
    pub uploads: u64,
    /// Failed attempts that were retried, degraded ones included.
    pub retries: u64,
    /// Panics the thread caught, each of which rebuilt or stopped the engine.
    pub panics: u64,
    /// Panics in the application's event sink, caught and dropped.
    pub sink_panics: u64,
    /// The replica's store, operation by operation.
    pub store: StoreCounters,
    /// Snapshots staged, per cause: each one uploads the whole database, so
    /// a cause that keeps counting up is a cost to look into.
    pub snapshots: SnapshotCounts,
    /// What the engine's sync steps and the checkpoints it asked SQLite for did.
    pub work: WorkCounts,
    /// Recoveries that found commits lost to a WAL restart the read lock should have
    /// prevented: a broken lock or a bug in the continuity proof. Each takes a snapshot, counted
    /// as `generation_changed`; any count above zero is worth a report.
    pub read_lock_violations: u64,
    /// 1 once the replica refused the boot's sweep of scratch files and open uploads for want of
    /// permission (an S3 policy without `s3:ListBucketMultipartUploads` or
    /// `s3:AbortMultipartUpload`, as Go's published one); the sweep is skipped for the rest of
    /// the process and replication is unaffected.
    pub scratch_sweeps_denied: u64,
}

/// One copy of the replicator's state, written by its thread after every tick.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Health {
    /// When the thread last wrote this; a stale value means the thread is stuck inside a
    /// blocking call (one store request can take up to its own deadline).
    pub reported_at: Timestamp,
    /// What it is doing.
    pub phase: Phase,
    /// Per task, in [`Task::ALL`] order: read through [`Health::degraded`] and
    /// [`Health::task`].
    pub(crate) degraded: [Option<Degraded>; 5],
    /// The newest transaction staged locally, and when the thread first saw it.
    pub synced: Option<(Txid, Timestamp)>,
    /// The newest transaction confirmed on the replica, and when the thread first saw it.
    pub replicated: Option<(Txid, Timestamp)>,
    /// When the oldest commit not yet on the replica was first seen; `None` when caught up.
    /// This is the lag to alert on: the age of `replicated` grows on an idle database too.
    /// Only a booted engine can say the replica is caught up: before the first boot, and while
    /// a rebuilt engine waits to boot again, this holds when that wait began, never `None`.
    pub unreplicated_since: Option<Timestamp>,
    /// The WAL file's size: a WAL the read lock holds while uploads are degraded fills the disk.
    pub wal_bytes: u64,
    /// Where in the WAL the newest staged file ends: the offset the last sync read to (Go's
    /// `litestream_wal_size`). 0 before the first sync.
    pub wal_synced_offset: u64,
    /// The database file's size.
    pub db_bytes: u64,
    /// Why the next sync is a snapshot, when it is one.
    pub resnapshot: Option<SnapshotCause>,
    /// Checkpoints someone else ran: the application promised not to (PLAN section 6).
    pub app_checkpoints_ran: u64,
    /// What the application's checkpoints did, those that cannot run included; read it as text
    /// through [`Health::app_notice`].
    pub app_checkpoints: AppCheckpoints,
    /// Running totals.
    pub counters: Counters,
    /// What the scheduled verify found (`Config::verify_interval`); all empty while it is off.
    pub verification: Verification,
    /// Fail-stop (`Config::if_replication_stops`): whether replication has tripped, and what
    /// that does to the application.
    pub fail_stop: FailStop,
    /// Whether the replicator holds the application's writes now, and why.
    pub writes: Writes,
    /// The retention pass a stop was asked to end with
    /// ([`crate::Replicator::request_final_retention`]): not asked, running, or how it ended.
    pub final_retention: FinalRetention,
    /// The snapshot a stop was asked to end with ([`crate::Replicator::request_final_snapshot`]):
    /// not asked, owed, written, or why it was not.
    pub final_snapshot: FinalSnapshot,
}

/// Fail-stop's state (`lsr_core::failstop`): whether replication has tripped, and what that
/// does to the application under `Config::if_replication_stops`. While it is
/// [`FailStop::Held`], and while it is [`FailStop::Pending`] under `hold-start` and
/// `stop-app`, the replicator holds the application's writes ([`Health::writes`]).
///
/// A trip is an unsafe stop, a commit off the replica past `Config::max_unreplicated_age`, or
/// a refusal by the store before the first listing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailStop {
    /// The boot has not been judged yet, and nothing has tripped.
    Pending,
    /// The application may run: nothing tripped, or the trip only reports under the mode.
    Allowed {
        /// The trip that only reports, if any.
        trip: Option<Trip>,
    },
    /// The trip holds the application.
    Held(Trip),
}

impl FailStop {
    /// The engine's trip and gate, which agree by construction: a held gate holds the trip,
    /// and a pending one has none.
    pub(crate) fn of(trip: Option<Trip>, gate: Gate) -> Self {
        match gate {
            Gate::Pending => {
                debug_assert!(trip.is_none(), "a trip judges the boot");
                Self::Pending
            }
            Gate::Allowed => Self::Allowed { trip },
            Gate::Held(held) => {
                debug_assert_eq!(trip, Some(held), "the gate holds the trip it was given");
                Self::Held(held)
            }
        }
    }

    /// Why replication tripped, if it has.
    #[must_use]
    pub const fn trip(self) -> Option<Trip> {
        match self {
            Self::Pending => None,
            Self::Allowed { trip } => trip,
            Self::Held(trip) => Some(trip),
        }
    }

    /// The engine's gate for this state.
    #[must_use]
    pub const fn gate(self) -> Gate {
        match self {
            Self::Pending => Gate::Pending,
            Self::Allowed { .. } => Gate::Allowed,
            Self::Held(trip) => Gate::Held(trip),
        }
    }
}

/// Whether the replicator holds the application's writes: a write transaction of its own on
/// the database, which every other writer, in any process, meets as SQLite's "database is
/// locked" (`SQLITE_BUSY`). Reads are never held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Writes {
    /// Writes go through.
    Allowed,
    /// Writes are to be held, and the replicator is waiting for a write transaction of the
    /// application's to end before it can take the lock: that one transaction may still
    /// commit. It tries again on every turn, at least once per `Config::poll_interval`.
    Blocking(WhyBlocked),
    /// The replicator holds the write lock: every write fails.
    Blocked(WhyBlocked),
}

/// What the scheduled verify found: the replica's listing checked, and a restore of the
/// transaction the replica holds compared with the live database at that transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Verification {
    /// When the last run ended; `None` before the first, and always while the verify is off.
    pub last_run: Option<Timestamp>,
    /// The newest transaction a run restored exactly as the live database held it, and when.
    pub last_good: Option<(Txid, Timestamp)>,
    /// The replica restores something the live database never held, or does not restore:
    /// a degraded state, which the next run that passes clears. Replication goes on.
    pub failing: Option<VerifyFailure>,
    /// The restore point retention keeps while `failing` stands: the last good transaction,
    /// or the failing one when no run passed before it.
    pub kept_restore_point: Option<Txid>,
    /// The most severe finding of the last run's listing check, if it found any.
    pub worst_finding: Option<Severity>,
    /// Runs, by how they ended.
    pub runs: VerifyRuns,
}

/// Scheduled verify runs, by how they ended.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyRuns {
    /// The restore equalled the live database.
    pub passed: u64,
    /// The restore differed, or failed.
    pub failed: u64,
    /// The transaction was merged into a compacted file before the run read it: nothing to
    /// compare, nothing wrong.
    pub merged: u64,
    /// A listing or a read failed (or the run panicked): it says nothing about the replica.
    pub incomplete: u64,
}

/// A scheduled verify that did not pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyFailure {
    /// The first failing run of this stretch.
    pub since: Timestamp,
    /// The transaction the newest failing run compared.
    pub txid: Txid,
    /// What it found.
    pub found: Mismatch,
}

/// What a failing scheduled verify found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mismatch {
    /// The restore holds other pages than the live database at the same transaction.
    Pages {
        /// The lowest page that differs.
        first: u32,
        /// How many pages differ.
        pages: u32,
        /// The live database's length in pages.
        live_commit: u32,
        /// The restored database's.
        restored_commit: u32,
    },
    /// The restore of the transaction fails, or the listing shows the newest state cannot
    /// be restored.
    Unrestorable,
}

/// How one scheduled verify ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// Page for page the same.
    Passed {
        /// Pages compared.
        pages: u32,
    },
    /// A degraded state: see [`Mismatch`].
    Failed(Mismatch),
    /// The transaction was merged into a compacted file before the run read it.
    Merged,
    /// A listing or a read failed, or the run panicked.
    Incomplete,
}

impl Health {
    /// The state before the thread has written anything, reported at `at`: what a replicator
    /// says before its first tick, and what a test outside this crate builds a health from.
    #[must_use]
    pub fn new(at: Timestamp) -> Self {
        Self {
            reported_at: at,
            phase: Phase::Booting,
            degraded: [None; 5],
            synced: None,
            replicated: None,
            unreplicated_since: None,
            wal_bytes: 0,
            wal_synced_offset: 0,
            db_bytes: 0,
            resnapshot: None,
            app_checkpoints_ran: 0,
            app_checkpoints: AppCheckpoints::NONE,
            counters: Counters::default(),
            fail_stop: FailStop::Pending,
            writes: Writes::Allowed,
            verification: Verification::default(),
            final_retention: FinalRetention::NotAsked,
            final_snapshot: FinalSnapshot::NotAsked,
        }
    }

    /// The task's degraded state.
    #[must_use]
    pub fn task(&self, task: Task) -> Option<Degraded> {
        let index = Task::ALL.iter().position(|each| *each == task)?;
        self.degraded.get(index).copied().flatten()
    }

    /// Each task that is degraded, with the persistent failure it keeps retrying, in
    /// [`Task::ALL`] order (sync, checkpoint, upload, prune, maintenance).
    pub fn degraded(&self) -> impl Iterator<Item = (Task, Degraded)> + '_ {
        Task::ALL
            .into_iter()
            .zip(self.degraded)
            .filter_map(|(task, degraded)| Some((task, degraded?)))
    }

    /// Whether any task is degraded, or the scheduled verify is failing.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        self.degraded.iter().any(Option::is_some) || self.verification.failing.is_some()
    }

    /// What the application's checkpoints cost, from the newest one seen; `None` while none was.
    #[must_use]
    pub const fn app_notice(&self) -> Option<AppNotice> {
        let app = &self.app_checkpoints;
        match app.last {
            None => None,
            Some(AppCheckpoint::Stalled { frames }) => Some(AppNotice::Stalled { frames }),
            Some(AppCheckpoint::Backfilled { .. } | AppCheckpoint::Restarted) => {
                Some(AppNotice::Checkpointing { ran: app.ran() })
            }
        }
    }
}

/// What the application's checkpoints cost. Not a degraded state: replication stays correct either
/// way. Its text says what to change in the application.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppNotice {
    /// The application ran checkpoints the replicator did not, `ran` of them so far. They copy only
    /// frames below the replicator's read lock into the database file and never restart the WAL
    /// under it.
    Checkpointing {
        /// Application checkpoints that changed the wal-index.
        ran: u64,
    },
    /// The WAL grew past the application's automatic checkpoint threshold with nothing new copied:
    /// each application commit runs a checkpoint attempt that does nothing.
    Stalled {
        /// Frames in the WAL when it was seen.
        frames: u32,
    },
}

/// What the application changes to stop both: its connections' settings, which a replicator in
/// another process cannot set.
const ADVICE: &str = "set wal_autocheckpoint = 0 on every connection the application opens \
     (app_connection does), and tell the replicator (app_autocheckpoint: None)";

impl fmt::Display for AppNotice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Checkpointing { ran } => write!(
                f,
                "the application ran {ran} checkpoint(s) of its own: they stop at the replicator's \
                 read transaction and cost the replica nothing, but past its automatic \
                 checkpoint threshold every application commit runs one; {ADVICE}"
            ),
            Self::Stalled { frames } => write!(
                f,
                "the WAL holds {frames} frames, past the application's automatic checkpoint \
                 threshold, with nothing new copied into the database: every application commit now \
                 runs a checkpoint attempt that does nothing until the replicator \
                 checkpoints, and the WAL keeps growing until then; {ADVICE}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with(last: Option<AppCheckpoint>) -> Health {
        let mut health = Health::new(Timestamp::from_unix_millis(0));
        health.app_checkpoints = AppCheckpoints {
            backfilled: 2,
            restarted: 1,
            stalled: 1,
            last,
        };
        health
    }

    /// The notice follows the newest observation, says what it costs and what to change,
    /// and is never a degraded state.
    #[test]
    fn the_app_notice_says_what_it_costs() {
        assert_eq!(with(None).app_notice(), None);
        let ran = with(Some(AppCheckpoint::Restarted));
        assert_eq!(ran.app_notice(), Some(AppNotice::Checkpointing { ran: 3 }));
        assert!(!ran.is_degraded());
        let stalled = with(Some(AppCheckpoint::Stalled { frames: 1200 }));
        let Some(notice) = stalled.app_notice() else {
            panic!("no notice");
        };
        let text = notice.to_string();
        for words in [
            "1200 frames",
            "every application commit now runs a checkpoint attempt that does nothing",
            "until the replicator checkpoints",
            "the WAL keeps growing",
            "wal_autocheckpoint = 0",
        ] {
            assert!(text.contains(words), "{text}");
        }
        assert!(!stalled.is_degraded());
    }
}
#[cfg(test)]
mod degraded_tests {
    use super::*;
    use lsr_core::DegradedCause;

    /// The iterator names each degraded task by its slot, and only those.
    #[test]
    fn degraded_lists_each_degraded_task_by_name() {
        let at = Timestamp::from_unix_millis(1_700_000_000_000);
        let mut health = Health::new(at);
        assert_eq!(health.degraded().count(), 0);
        let upload = Degraded {
            cause: DegradedCause::StoreRefused,
            since: at,
            retries: 3,
        };
        let index = Task::ALL
            .iter()
            .position(|task| *task == Task::Upload)
            .unwrap();
        health.degraded[index] = Some(upload);
        assert_eq!(
            health.degraded().collect::<Vec<_>>(),
            vec![(Task::Upload, upload)]
        );
        assert_eq!(health.task(Task::Upload), Some(upload));
        assert!(health.is_degraded());
    }

    /// A held gate and a pending one are read back as the engine judged them.
    #[test]
    fn fail_stop_reads_back_the_engine_gate_and_trip() {
        let trip = Trip::Stopped(StopCause::OtherWriter);
        let held = FailStop::of(Some(trip), Gate::Held(trip));
        assert_eq!((held.trip(), held.gate()), (Some(trip), Gate::Held(trip)));
        let reported = FailStop::of(Some(trip), Gate::Allowed);
        assert_eq!(
            (reported.trip(), reported.gate()),
            (Some(trip), Gate::Allowed)
        );
        let pending = FailStop::of(None, Gate::Pending);
        assert_eq!((pending.trip(), pending.gate()), (None, Gate::Pending));
    }
}
