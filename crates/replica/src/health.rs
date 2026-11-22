//! What a replicator reports: a [`Health`] snapshot the thread writes after every tick, and
//! the outcomes of the first boot ([`Booted`]) and of a stop ([`Stopped`]).
//!
//! A snapshot is a copy: reading it takes one lock for as long as a clone, never across I/O.
//!
//! The application's own checkpoints are a notice ([`AppNotice`]), never a degraded state: the read lock
//! keeps them from costing the replica anything, and what they cost the application is said in the
//! notice's text.

use std::fmt;

use lsr_core::{
    AppCheckpoint, AppCheckpoints, Degraded, Gate, JournalSignal, PersistentCause, Severity,
    SnapshotCause, SnapshotCounts, StopCause, Task, Trip,
};
use lsr_ltx::{Timestamp, Txid};

/// What the replicator is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The first boot (listing the replica, recovering the position) has not finished.
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
pub enum Booted {
    /// The replica was listed and the position recovered.
    Booted,
    /// The boot keeps failing with this cause; it keeps trying.
    Degraded(PersistentCause),
    /// The wait ran out first.
    TimedOut,
    /// The replicator stopped before it booted.
    Stopped(StopCause),
    /// The thread is not there.
    Gone(Gone),
}

/// How a stop ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stopped {
    /// Everything committed before the stop is on the replica, through `txid` (`None` when the
    /// database never committed anything).
    Drained {
        /// The newest transaction on the replica.
        txid: Option<Txid>,
    },
    /// Time ran out with work left. The wal-index gives a frame offset, not a txid, so what is
    /// left is measured in bytes and files.
    DeadlineReached {
        /// Committed WAL bytes not yet staged.
        unsynced_wal_bytes: u64,
        /// Staged files the replica was not confirmed to hold.
        staged_not_uploaded: u32,
    },
    /// Already stopped by a cause (another writer, or a configured stop) before the drain.
    Stopped(StopCause),
    /// The thread is not there.
    Gone(Gone),
}

/// One store operation's totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
}

/// One copy of the replicator's state, written by its thread after every tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Health {
    /// When the thread last wrote this; a stale value means the thread is stuck inside a
    /// blocking call (one store request can take up to its own deadline).
    pub reported_at: Timestamp,
    /// What it is doing.
    pub phase: Phase,
    /// Per task, in [`Task::ALL`] order (sync, checkpoint, upload, prune, maintenance): the
    /// persistent failure it keeps retrying, if any.
    pub degraded: [Option<Degraded>; 5],
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
    /// What the engine journal (`docs/journal.md`) says, when it says something an operator
    /// should see: what boot read of it, or that the debt of a failed WAL sync is held only in
    /// memory because the journal cannot be written. `None` while it is read and written.
    pub journal: Option<JournalSignal>,
    /// Fail-stop (`Config::if_replication_stops`): whether replication has tripped, and what
    /// that does to the application.
    pub held_by: FailStop,
}

/// Fail-stop's state (`lsr_core::failstop`, `docs/proposals/fail-stop.md`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailStop {
    /// Why replication tripped, if it has: an unsafe stop, a commit off the replica past
    /// `Config::max_unreplicated_age`, or a refusal by the store before the first listing.
    pub trip: Option<Trip>,
    /// What the trip does to the application under `Config::if_replication_stops`. While it is
    /// [`Gate::Held`], the commit hook refuses the application's commits.
    pub gate: Gate,
}

/// What the scheduled verify found: the replica's listing checked, and a restore of the
/// transaction the replica holds compared with the live database at that transaction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
    pub const fn new(at: Timestamp) -> Self {
        Self {
            reported_at: at,
            phase: Phase::Booting,
            degraded: [None; 5],
            synced: None,
            replicated: None,
            unreplicated_since: None,
            wal_bytes: 0,
            db_bytes: 0,
            resnapshot: None,
            app_checkpoints_ran: 0,
            app_checkpoints: AppCheckpoints::NONE,
            counters: Counters {
                ticks: 0,
                syncs: 0,
                checkpoints: 0,
                checkpoints_busy: 0,
                uploads: 0,
                retries: 0,
                panics: 0,
                sink_panics: 0,
                store: StoreCounters {
                    list: OpCounters::ZERO,
                    read: OpCounters::ZERO,
                    created_at: OpCounters::ZERO,
                    begin_upload: OpCounters::ZERO,
                    write: OpCounters::ZERO,
                    commit: OpCounters::ZERO,
                    abort: OpCounters::ZERO,
                    delete: OpCounters::ZERO,
                    bytes_uploaded: 0,
                },
                snapshots: SnapshotCounts::ZERO,
            },
            held_by: FailStop {
                trip: None,
                gate: Gate::Pending,
            },
            verification: Verification {
                last_run: None,
                last_good: None,
                failing: None,
                kept_restore_point: None,
                worst_finding: None,
                runs: VerifyRuns {
                    passed: 0,
                    failed: 0,
                    merged: 0,
                    incomplete: 0,
                },
            },
            journal: None,
        }
    }

    /// The task's degraded state.
    #[must_use]
    pub fn task(&self, task: Task) -> Option<Degraded> {
        let index = Task::ALL.iter().position(|each| *each == task)?;
        self.degraded.get(index).copied().flatten()
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
const ADVICE: &str = "set wal_autocheckpoint = 0 and SQLITE_FCNTL_PERSIST_WAL on every \
     connection the application opens (app_connection does both), and tell the replicator \
     (app_autocheckpoint: None)";

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

impl OpCounters {
    const ZERO: Self = Self {
        calls: 0,
        errors: 0,
        millis: 0,
    };
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
