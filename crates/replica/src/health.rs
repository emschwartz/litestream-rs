//! What a replicator reports: a [`Health`] snapshot the thread writes after every tick, and
//! the outcomes of the first boot ([`Booted`]) and of a stop ([`Stopped`]).
//!
//! A snapshot is a copy: reading it takes one lock for as long as a clone, never across I/O.
//!
//! The host's own checkpoints are a notice ([`HostNotice`]), never a degraded state: the pin
//! keeps them from costing the replica anything, and what they cost the host is said in the
//! notice's text.

use std::fmt;

use lsr_core::{
    Degraded, HaltCause, HostCheckpoint, HostCheckpoints, Lane, PersistentCause, SnapshotCause,
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
    Stopped(HaltCause),
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
    Stopped(HaltCause),
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
    /// Already stopped by a cause (a second writer, or a configured stop) before the drain.
    Stopped(HaltCause),
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
    /// Reads, with or without the stamp.
    pub read: OpCounters,
    /// Stamp reads.
    pub stamp: OpCounters,
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
    /// Panics in the host's event sink, caught and dropped.
    pub sink_panics: u64,
    /// The replica's store, operation by operation.
    pub store: StoreCounters,
}

/// One copy of the replicator's state, written by its thread after every tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Health {
    /// When the thread last wrote this; a stale value means the thread is stuck inside a
    /// blocking call (one store request can take up to its own deadline).
    pub observed_at: Timestamp,
    /// What it is doing.
    pub phase: Phase,
    /// Per lane, in [`Lane::ALL`] order (sync, checkpoint, upload, prune, maintenance): the
    /// persistent failure it keeps retrying, if any.
    pub degraded: [Option<Degraded>; 5],
    /// The newest transaction staged locally, and when the thread first saw it.
    pub synced: Option<(Txid, Timestamp)>,
    /// The newest transaction confirmed on the replica, and when the thread first saw it.
    pub replicated: Option<(Txid, Timestamp)>,
    /// When the oldest commit not yet on the replica was first seen; `None` when caught up.
    /// This is the lag to alert on: the age of `replicated` grows on an idle database too.
    pub unreplicated_since: Option<Timestamp>,
    /// The WAL file's size: a WAL the pin holds while uploads are degraded fills the disk.
    pub wal_bytes: u64,
    /// The database file's size.
    pub db_bytes: u64,
    /// Why the next sync is a snapshot, when it is one.
    pub resnapshot: Option<SnapshotCause>,
    /// Checkpoints someone else ran: the host promised not to (PLAN section 6).
    pub foreign_checkpoints: u64,
    /// What the host's checkpoints did, those that cannot run included; read it as text
    /// through [`Health::host_notice`].
    pub host_checkpoints: HostCheckpoints,
    /// Running totals.
    pub counters: Counters,
}

impl Health {
    /// The state before the thread has written anything.
    pub(crate) const fn new(at: Timestamp) -> Self {
        Self {
            observed_at: at,
            phase: Phase::Booting,
            degraded: [None; 5],
            synced: None,
            replicated: None,
            unreplicated_since: None,
            wal_bytes: 0,
            db_bytes: 0,
            resnapshot: None,
            foreign_checkpoints: 0,
            host_checkpoints: HostCheckpoints::NONE,
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
                    stamp: OpCounters::ZERO,
                    begin_upload: OpCounters::ZERO,
                    write: OpCounters::ZERO,
                    commit: OpCounters::ZERO,
                    abort: OpCounters::ZERO,
                    delete: OpCounters::ZERO,
                    bytes_uploaded: 0,
                },
            },
        }
    }

    /// The lane's degraded state.
    #[must_use]
    pub fn lane(&self, lane: Lane) -> Option<Degraded> {
        let index = Lane::ALL.iter().position(|each| *each == lane)?;
        self.degraded.get(index).copied().flatten()
    }

    /// Whether any lane is degraded.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        self.degraded.iter().any(Option::is_some)
    }

    /// What the host's checkpoints cost, from the newest one seen; `None` while none was.
    #[must_use]
    pub const fn host_notice(&self) -> Option<HostNotice> {
        let host = &self.host_checkpoints;
        match host.last {
            None => None,
            Some(HostCheckpoint::Stalled { frames }) => Some(HostNotice::Stalled { frames }),
            Some(HostCheckpoint::Backfilled { .. } | HostCheckpoint::Restarted) => {
                Some(HostNotice::Checkpointing { ran: host.ran() })
            }
        }
    }
}

/// What the host's checkpoints cost. Not a degraded state: replication stays correct either
/// way. Its text says what to change in the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostNotice {
    /// The host ran checkpoints the replicator did not, `ran` of them so far. They copy only
    /// frames below the replicator's pin into the database file and never restart the WAL
    /// under it.
    Checkpointing {
        /// Host checkpoints that changed the wal-index.
        ran: u64,
    },
    /// The WAL grew past the host's automatic checkpoint threshold with nothing new copied:
    /// each host commit runs a checkpoint attempt that does nothing.
    Stalled {
        /// Frames in the WAL when it was seen.
        frames: u32,
    },
}

/// What the host changes to stop both: its connections' settings, which a replicator in
/// another process cannot set.
const ADVICE: &str = "set wal_autocheckpoint = 0 and SQLITE_FCNTL_PERSIST_WAL on every \
     connection the host opens (app_connection does both), and tell the replicator \
     (host_autocheckpoint: None)";

impl fmt::Display for HostNotice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Checkpointing { ran } => write!(
                f,
                "the host ran {ran} checkpoint(s) of its own: they stop at the replicator's \
                 read transaction and cost the replica nothing, but past its automatic \
                 checkpoint threshold every host commit runs one; {ADVICE}"
            ),
            Self::Stalled { frames } => write!(
                f,
                "the WAL holds {frames} frames, past the host's automatic checkpoint \
                 threshold, with nothing new copied into the database: every host commit now \
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

    fn with(last: Option<HostCheckpoint>) -> Health {
        let mut health = Health::new(Timestamp::from_unix_millis(0));
        health.host_checkpoints = HostCheckpoints {
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
    fn the_host_notice_says_what_it_costs() {
        assert_eq!(with(None).host_notice(), None);
        let ran = with(Some(HostCheckpoint::Restarted));
        assert_eq!(
            ran.host_notice(),
            Some(HostNotice::Checkpointing { ran: 3 })
        );
        assert!(!ran.is_degraded());
        let stalled = with(Some(HostCheckpoint::Stalled { frames: 1200 }));
        let Some(notice) = stalled.host_notice() else {
            panic!("no notice");
        };
        let text = notice.to_string();
        for words in [
            "1200 frames",
            "every host commit now runs a checkpoint attempt that does nothing",
            "until the replicator checkpoints",
            "the WAL keeps growing",
            "wal_autocheckpoint = 0",
        ] {
            assert!(text.contains(words), "{text}");
        }
        assert!(!stalled.is_degraded());
    }
}
