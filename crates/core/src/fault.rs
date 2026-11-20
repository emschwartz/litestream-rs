//! What the library cannot prevent, and what to do about each.
//!
//! [`Fault`] is closed: one variant per thing that can go wrong, each with a
//! policy from [`policy`], a single exhaustive match. The policy never depends
//! on state, so the simulator can assert it per variant, and a new variant
//! breaks the build here before it can be mishandled anywhere.
//!
//! Core runs no timer. A [`Policy::Retry`] says how long to wait ([`Backoff`],
//! [`retry_wait`]); the actor that owns the clock does the waiting and the
//! state machine hands it a [`Next`].
//!
//! Two things are deliberately not faults. A clock that steps backwards is
//! clamped and reported as [`Clamped`]. A power-loss WAL rewind is a snapshot
//! with cause [`SnapshotCause::LocalRewound`]: it is expected under
//! `synchronous=NORMAL`, and halting would leave every later write unreplicated.

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{Timestamp, Txid};
use lsr_wal::{PageMapError, ReadPageError, ResumeError, WalError, WalOffset};

use crate::source::SourceError;
use crate::store::{CorruptCause, StoreError};

/// The longest a retry waits (Go `DefaultSyncBackoffMax`).
pub const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

/// A fault the sync engine can meet. The doc on each names what happened; the
/// action is [`policy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The store failed in a way that may pass.
    StoreTransient,
    /// The store asked for slower requests, and maybe said for how long.
    StoreThrottled {
        /// The wait the store named.
        retry_after: Option<Duration>,
    },
    /// The store refuses our credentials.
    StoreAuth,
    /// An object we listed or staged is gone from the replica.
    RemoteObjectMissing,
    /// An upload's commit failed after the object may have become visible.
    UploadAmbiguous,
    /// A replica object is unusable as stored.
    RemoteCorrupt,
    /// The replica is ahead of this database mid-run: another writer.
    RemoteAheadOfLocal,
    /// The replica's newest transaction went backwards mid-run.
    RemoteRegressed,
    /// A staged file fails verification before upload.
    StagedCorrupt,
    /// The staging directory or a staged file vanished.
    StagedLost,
    /// The local disk is full.
    LocalDiskFull,
    /// A local read or write failed.
    LocalIo,
    /// An fsync of a staged file failed; encode and write it again.
    LocalFsyncFailed,
    /// SQLite is busy, or the wal-index is mid-update.
    SqliteBusy,
    /// The WAL moved behind the reader; the next recover classifies how.
    WalMoved,
    /// The wal-index names a committed end inside a transaction.
    WalIndexInconsistent,
    /// A WAL page changed between the scan and the read.
    WalChangedUnderRead,
    /// The database has no pages to snapshot yet.
    NothingToSnapshot,
    /// The read transaction could not be retaken.
    ReadTxLost,
    /// The database page size changed.
    PageSizeChanged,
    /// A committed WAL frame fails its checksum or salt.
    WalCorruptBelowCommitted,
    /// The WAL file ends inside the committed region.
    WalTruncatedBelowCommitted,
    /// The WAL file was deleted under us.
    WalUnlinked,
    /// The shutdown deadline arrived mid-work.
    ShutdownDeadline,
    /// A condition the code rules out. A property test asserts it never fires.
    Bug,
}

/// Why a snapshot is taken instead of continuing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotCause {
    /// There is no local position and the replica is empty.
    FirstSync,
    /// At boot the replica is ahead of the local position; `local` is ours.
    BehindRemote {
        /// The local transaction id.
        local: Txid,
    },
    /// The newest staged file cannot be read.
    LocalTipUnreadable,
    /// The WAL file is missing or its header is torn.
    WalMissing,
    /// The database page size changed.
    PageSizeChanged,
    /// The WAL restarted and continuity does not prove nothing was lost.
    GenerationChanged,
    /// The frame before the position does not verify, or the position is mid-transaction.
    PrevFrameMismatch,
    /// The last page covered differs from the WAL at the position.
    LastPageMismatch,
    /// Same generation, but the committed end or the file ends before the
    /// synced end: a local rewind, as after a power loss under `synchronous=NORMAL`.
    LocalRewound {
        /// The synced end.
        synced: WalOffset,
        /// The committed end read now.
        committed: WalOffset,
    },
    /// A truncating checkpoint moved the WAL past what was synced.
    CheckpointBoundary,
    /// An operator asked.
    OperatorRequested,
}

/// Why the engine stops and waits for a person.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaltCause {
    /// Credentials or permissions refuse the store.
    Credentials,
    /// A replica object is corrupt.
    RemoteCorrupt,
    /// Another writer replicates into this replica.
    ForeignWriter,
    /// The replica went backwards.
    RemoteRegressed,
    /// A staged file is corrupt.
    StagedCorrupt,
    /// The staging area is gone.
    StagedLost,
    /// A committed WAL frame is corrupt.
    WalCorrupt,
    /// The WAL is truncated below its committed end.
    WalTruncated,
    /// The WAL file is gone.
    WalUnlinked,
    /// The shutdown deadline arrived.
    Shutdown,
    /// A bug in this program.
    Bug,
}

/// How long to wait before a retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backoff {
    /// [`retry_wait`] from the attempt count.
    Doubling,
    /// A wait the store named.
    After(Duration),
}

/// What to do about a [`Fault`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Try again after a wait.
    Retry(Backoff),
    /// Take a snapshot.
    Resnapshot(SnapshotCause),
    /// Stop.
    Halt(HaltCause),
}

/// The action for a fault. Total and pure.
#[must_use]
pub const fn policy(fault: &Fault) -> Policy {
    match fault {
        Fault::StoreThrottled {
            retry_after: Some(wait),
        } => Policy::Retry(Backoff::After(*wait)),
        Fault::StoreThrottled { retry_after: None }
        | Fault::StoreTransient
        | Fault::RemoteObjectMissing
        | Fault::UploadAmbiguous
        | Fault::LocalDiskFull
        | Fault::LocalIo
        | Fault::LocalFsyncFailed
        | Fault::SqliteBusy
        | Fault::WalMoved
        | Fault::WalIndexInconsistent
        | Fault::WalChangedUnderRead
        | Fault::NothingToSnapshot
        | Fault::ReadTxLost => Policy::Retry(Backoff::Doubling),
        Fault::PageSizeChanged => Policy::Resnapshot(SnapshotCause::PageSizeChanged),
        Fault::StoreAuth => Policy::Halt(HaltCause::Credentials),
        Fault::RemoteCorrupt => Policy::Halt(HaltCause::RemoteCorrupt),
        Fault::RemoteAheadOfLocal => Policy::Halt(HaltCause::ForeignWriter),
        Fault::RemoteRegressed => Policy::Halt(HaltCause::RemoteRegressed),
        Fault::StagedCorrupt => Policy::Halt(HaltCause::StagedCorrupt),
        Fault::StagedLost => Policy::Halt(HaltCause::StagedLost),
        Fault::WalCorruptBelowCommitted => Policy::Halt(HaltCause::WalCorrupt),
        Fault::WalTruncatedBelowCommitted => Policy::Halt(HaltCause::WalTruncated),
        Fault::WalUnlinked => Policy::Halt(HaltCause::WalUnlinked),
        Fault::ShutdownDeadline => Policy::Halt(HaltCause::Shutdown),
        Fault::Bug => Policy::Halt(HaltCause::Bug),
    }
}

/// `interval * 2^(attempt - 1)`, at most [`MAX_BACKOFF`] (Go `replica.go:449-457`).
/// No jitter, as in Go: one actor retries one thing.
#[must_use]
pub fn retry_wait(attempt: NonZeroU32, interval: Duration) -> Duration {
    let doublings = attempt.get().saturating_sub(1);
    1u32.checked_shl(doublings)
        .and_then(|factor| interval.checked_mul(factor))
        .map_or(MAX_BACKOFF, |wait| wait.min(MAX_BACKOFF))
}

impl Fault {
    /// An error from the replica's store.
    ///
    /// `NotFound` is `RemoteObjectMissing` whatever was being done: the answer
    /// is the same (list again, trust the listing).
    #[must_use]
    pub fn from_remote(err: &StoreError) -> Self {
        match err {
            StoreError::Transient { .. } => Self::StoreTransient,
            StoreError::Throttled { retry_after, .. } => Self::StoreThrottled {
                retry_after: retry_after.filter(|wait| !wait.is_zero()),
            },
            StoreError::Auth { .. } => Self::StoreAuth,
            StoreError::NotFound { .. } => Self::RemoteObjectMissing,
            StoreError::ScratchExhausted { .. } => Self::LocalDiskFull,
            StoreError::Corrupt { cause, .. } => match cause {
                CorruptCause::CommitSizeMismatch { .. } => Self::UploadAmbiguous,
                CorruptCause::NotARegularFile | CorruptCause::TimestampOutOfRange => {
                    Self::RemoteCorrupt
                }
            },
        }
    }

    /// An error from the staging store, a local directory nothing else writes.
    ///
    /// The same error means something else here. A missing staged file is a
    /// halt, not a relist. A write whose stored size differs is the torn write
    /// to encode again. A permission error retries, so an operator can fix the
    /// directory without a restart.
    #[must_use]
    pub const fn from_staging(err: &StoreError) -> Self {
        match err {
            StoreError::Transient { .. }
            | StoreError::Throttled { .. }
            | StoreError::Auth { .. } => Self::LocalIo,
            StoreError::NotFound { .. } => Self::StagedLost,
            StoreError::ScratchExhausted { .. } => Self::LocalDiskFull,
            StoreError::Corrupt { cause, .. } => match cause {
                CorruptCause::CommitSizeMismatch { .. } => Self::LocalFsyncFailed,
                CorruptCause::NotARegularFile | CorruptCause::TimestampOutOfRange => {
                    Self::StagedCorrupt
                }
            },
        }
    }

    /// A WAL scan error. Go turns every bad frame into end of file
    /// (`wal_reader.go:180-182`, `191-193`), so a corrupt committed frame looks
    /// like the end of the WAL there; here it is a halt.
    #[must_use]
    pub fn from_wal(err: WalError) -> Self {
        match err {
            WalError::CorruptBelowCommitted { .. } => Self::WalCorruptBelowCommitted,
            WalError::TruncatedBelowCommitted { .. } => Self::WalTruncatedBelowCommitted,
            WalError::Restarted { .. } | WalError::CursorPastEnd { .. } => Self::WalMoved,
            WalError::PageSizeMismatch { .. } => Self::PageSizeChanged,
            WalError::EndNotAtCommit { .. } => Self::WalIndexInconsistent,
            WalError::TooManyFrames | WalError::Io(_) => Self::LocalIo,
        }
    }

    /// A resume error that `recover` did not map to one of its own rules.
    /// `recover` owns `PrevFrame` and `NotAtCommit` (snapshot causes); one
    /// reaching this fallback is a bug.
    #[must_use]
    pub fn from_resume(err: ResumeError) -> Self {
        match err {
            ResumeError::Io(_) => Self::LocalIo,
            ResumeError::PageSizeMismatch { .. } => Self::PageSizeChanged,
            ResumeError::Truncated { .. } | ResumeError::Restarted { .. } => Self::WalMoved,
            ResumeError::PrevFrame { .. } | ResumeError::NotAtCommit { .. } => Self::Bug,
        }
    }

    /// A page re-read error. A changed page is never encoded; the step rescans.
    #[must_use]
    pub fn from_read_page(err: ReadPageError) -> Self {
        match err {
            ReadPageError::Io(_) => Self::LocalIo,
            ReadPageError::ChangedSinceScan { .. } => Self::WalChangedUnderRead,
            ReadPageError::Truncated { .. } => Self::WalTruncatedBelowCommitted,
            ReadPageError::BufferSize { .. } | ReadPageError::PageSizeMismatch { .. } => Self::Bug,
        }
    }

    /// A page map error. The step feeds the map from one scan, so every one is a bug.
    #[must_use]
    pub const fn from_page_map(err: PageMapError) -> Self {
        match err {
            PageMapError::PageSizeMismatch { .. }
            | PageMapError::NotAfterEnd { .. }
            | PageMapError::Gap { .. }
            | PageMapError::Generation { .. } => Self::Bug,
        }
    }

    /// A source error. A short database page read is retried as local I/O:
    /// the file may be mid-truncate.
    #[must_use]
    pub const fn from_source(err: SourceError) -> Self {
        match err {
            SourceError::Io(_) | SourceError::Short { .. } => Self::LocalIo,
            SourceError::DiskFull => Self::LocalDiskFull,
            SourceError::FsyncFailed => Self::LocalFsyncFailed,
            SourceError::Busy => Self::SqliteBusy,
            SourceError::Unlinked => Self::WalUnlinked,
            SourceError::ReadTxLost => Self::ReadTxLost,
        }
    }
}

/// What the actor does after a step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Next {
    /// Step again immediately.
    Now,
    /// Step again at this time.
    At(Timestamp),
    /// Step when the WAL changes.
    OnWalChange,
}

/// The clock read earlier than the last timestamp used; the later one is used
/// instead and this is reported. An event, not a fault: nothing is retried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Clamped {
    /// The last timestamp used.
    pub prev: Timestamp,
    /// The earlier time the clock read.
    pub now: Timestamp,
}

#[cfg(test)]
mod tests;
