//! What the library cannot prevent, and what to do about each.
//!
//! [`Fault`] is closed: one variant per thing that can go wrong, each with a
//! policy from [`policy`], a single exhaustive match. The policy never depends
//! on state, so the simulator can assert it per variant, and a new variant
//! breaks the build here before it can be mishandled anywhere.
//!
//! Core runs no timer. A [`Policy::Retry`] says how long to wait ([`Backoff`],
//! [`backoff_wait`]); the actor that owns the clock does the waiting and the
//! state machine hands it a [`Next`].
//!
//! A [`Policy::Persistent`] fault is one that waiting alone is not expected to
//! fix: credentials refused, a corrupt object, a damaged WAL, a second writer.
//! What happens then is configuration, one field of [`Responses`] per
//! [`PersistentCause`] (`docs/PLAN.md`, the degraded-state section). [`Response::KeepTrying`]
//! puts the lane that failed in a degraded state: it retries with the doubling
//! wait, capped at [`MAX_BACKOFF`] as Go's sync loop is, the other lanes keep
//! running, and every retry that fails is reported. [`Response::Stop`] stops
//! the whole engine for a person, as every persistent fault did before.
//! [`Responses::GO_LIKE`] (the default) keeps trying everywhere except on a
//! second writer; [`Responses::STRICT`] stops on every cause.
//!
//! What keeping trying never does, whatever the configuration, follows from
//! the code rather than from a check of the response: the engine has one
//! replica and never writes anywhere else; a WAL frame reaches a staged file
//! only from a scan that verified it; and every snapshot first scans the whole
//! WAL to the committed end, so a WAL corrupt below that end fails the snapshot
//! before a database page is read. Two causes cannot be retried from the
//! state they left: a bug rebuilds the engine and boots again, and a replica
//! that went backwards is listed again and taken as it now is.
//!
//! Two things are deliberately not faults. A clock that steps backwards is
//! clamped and reported as [`Clamped`]. A power-loss WAL rewind is a snapshot
//! with cause [`SnapshotCause::LocalRewound`]: it is expected under
//! `synchronous=NORMAL`, and halting would leave every later write unreplicated.

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
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
    /// The store acknowledged an upload whose digest is not the one sent. The
    /// object may be visible with wrong bytes, so the uploader deletes the key
    /// and puts it again, and a later listing confirms it only by content.
    UploadChecksumMismatch,
    /// The store refused a request that retrying cannot fix and that is not an
    /// authorization failure.
    StoreRefused,
    /// A replica object is unusable as stored.
    RemoteCorrupt,
    /// The replica is ahead of this database mid-run: another writer.
    RemoteAheadOfLocal,
    /// The replica's newest transaction went backwards mid-run.
    RemoteRegressed,
    /// A staged level-0 file is missing or fails verification. Staged files
    /// are derived state: the engine deletes that file and every staged file
    /// after it, and the next file is a snapshot taking the first transaction
    /// the replica lacks, so the chain has no hole (`upload::sweep_staged` at
    /// boot, the upload lane mid-run; elsewhere the lane retries and the next
    /// boot sweeps).
    ///
    /// A file that tears this way was never acknowledged as staged: its
    /// commit had not returned. The filesystem store syncs a file before its
    /// rename and the directory after, so it cannot tear one at all. The
    /// simulator's power loss tears unsynced keys anyway, harsher than a real
    /// filesystem; the handling is right either way, since nothing the
    /// replica promised depends on the lost file.
    StagedLost,
    /// The local disk is full.
    LocalDiskFull,
    /// A local read or write failed.
    LocalIo,
    /// An fsync of a staged file failed; encode and write it again.
    LocalFsyncFailed,
    /// SQLite is busy, or the wal-index is mid-update.
    SqliteBusy,
    /// A TRUNCATE checkpoint was refused before SQLite was asked: the host
    /// said its readers hold the WAL (or tracks none). Not `SqliteBusy`:
    /// no lock was contended, and it lasts as long as the readers do.
    ReadersHeld,
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
    /// The database's page size is not the one its replica holds.
    PageSizeChanged {
        /// The replica's page size.
        was: PageSize,
        /// The database's page size now.
        now: PageSize,
    },
    /// A committed WAL frame fails its checksum or salt.
    WalCorruptBelowCommitted,
    /// The WAL file ends inside the committed region.
    WalTruncatedBelowCommitted,
    /// The WAL file was deleted under us.
    WalUnlinked,
    /// A database-file page is not what the page checksum table holds for it
    /// (`crate::pages`): something other than SQLite's backfill changed it.
    DatabasePageMismatch {
        /// The page.
        pgno: PageNo,
    },
    /// The shutdown deadline arrived mid-work.
    ShutdownDeadline,
    /// A condition the code rules out, and where it was met. A property test
    /// asserts it never fires.
    Bug(BugSite),
}

/// Where a condition the code rules out was met: the invariant to look at
/// when [`Fault::Bug`] is ever reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BugSite {
    /// The transaction after the newest has no successor.
    TxidExhausted,
    /// The encoder refused a header or a page the writer laid out itself.
    Encode,
    /// A staged file had neither a WAL frame nor a page to name its end.
    StepWithoutPages,
    /// A snapshot's scan of the WAL disagrees with the position it was
    /// proven for.
    SnapshotScan,
    /// The checkpoint's uncapped sync step stopped at a cap.
    CappedCheckpointStep,
    /// An upload had bytes to send and no open upload to send them to.
    UploadNeverOpened,
    /// The decoder took no byte of a non-empty chunk.
    DecoderStalled,
    /// Compaction was planned with inputs it cannot take.
    CompactionInputs,
    /// A file's stamp was asked for twice in one resolve.
    StampRepeated,
    /// The WAL reader was resumed from a cursor its own scan did not give.
    WalResume,
    /// A WAL page was read back into a buffer of the wrong size.
    WalPageRead,
    /// The page map was fed out of the order one scan produces.
    PageMap,
}

/// Why a snapshot is taken instead of continuing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotCause {
    /// There is no local position.
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

/// Why the engine stopped and waits for a person: a [`PersistentCause`]
/// whose configured response is to stop, or a drain whose deadline passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaltCause {
    /// Credentials or permissions refuse the store.
    Credentials,
    /// The store refuses a request outright (not credentials).
    StoreRefused,
    /// A replica object is corrupt.
    RemoteCorrupt,
    /// Another writer replicates into this replica.
    ForeignWriter,
    /// The replica went backwards.
    RemoteRegressed,
    /// A committed WAL frame is corrupt.
    WalCorrupt,
    /// The WAL is truncated below its committed end.
    WalTruncated,
    /// The WAL file is gone.
    WalUnlinked,
    /// The database's page size is not the one its replica holds. A replica
    /// prefix holds one page size, as Go's does (it reads the size once,
    /// `db.go:1191`, and its compactor refuses mixed sizes,
    /// `ltx/compactor.go:122`); the way out is a new prefix, and the old one
    /// keeps every state up to the change.
    PageSizeChanged {
        /// The replica's page size.
        was: PageSize,
        /// The database's page size now.
        now: PageSize,
    },
    /// A database-file page is not what the page checksum table holds.
    DatabasePageMismatch {
        /// The page.
        pgno: PageNo,
    },
    /// The shutdown deadline arrived.
    Shutdown,
    /// A bug in this program.
    Bug,
}

/// A failure that waiting alone is not expected to fix. The configured
/// [`Responses`] say whether the engine keeps trying or stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistentCause {
    /// Credentials or permissions refuse the store.
    Credentials,
    /// The store refuses a request outright (not credentials).
    StoreRefused,
    /// A replica object is corrupt.
    RemoteCorrupt,
    /// Another writer replicates into this replica.
    ForeignWriter,
    /// The replica went backwards.
    RemoteRegressed,
    /// A committed WAL frame is corrupt.
    WalCorrupt,
    /// The WAL is truncated below its committed end.
    WalTruncated,
    /// The WAL file is gone.
    WalUnlinked,
    /// The database's page size is not the one its replica holds.
    PageSizeChanged {
        /// The replica's page size.
        was: PageSize,
        /// The database's page size now.
        now: PageSize,
    },
    /// A database-file page is not what the page checksum table holds.
    DatabasePageMismatch {
        /// The page.
        pgno: PageNo,
    },
    /// A bug in this program.
    Bug,
}

impl PersistentCause {
    /// The persistent cause behind a stop cause; `None` for the one stop that
    /// is not configurable, the end of a drain.
    #[must_use]
    pub const fn of(cause: HaltCause) -> Option<Self> {
        match cause {
            HaltCause::Credentials => Some(Self::Credentials),
            HaltCause::StoreRefused => Some(Self::StoreRefused),
            HaltCause::RemoteCorrupt => Some(Self::RemoteCorrupt),
            HaltCause::ForeignWriter => Some(Self::ForeignWriter),
            HaltCause::RemoteRegressed => Some(Self::RemoteRegressed),
            HaltCause::WalCorrupt => Some(Self::WalCorrupt),
            HaltCause::WalTruncated => Some(Self::WalTruncated),
            HaltCause::WalUnlinked => Some(Self::WalUnlinked),
            HaltCause::PageSizeChanged { was, now } => Some(Self::PageSizeChanged { was, now }),
            HaltCause::DatabasePageMismatch { pgno } => Some(Self::DatabasePageMismatch { pgno }),
            HaltCause::Bug => Some(Self::Bug),
            HaltCause::Shutdown => None,
        }
    }
}

impl From<PersistentCause> for HaltCause {
    fn from(cause: PersistentCause) -> Self {
        match cause {
            PersistentCause::Credentials => Self::Credentials,
            PersistentCause::StoreRefused => Self::StoreRefused,
            PersistentCause::RemoteCorrupt => Self::RemoteCorrupt,
            PersistentCause::ForeignWriter => Self::ForeignWriter,
            PersistentCause::RemoteRegressed => Self::RemoteRegressed,
            PersistentCause::WalCorrupt => Self::WalCorrupt,
            PersistentCause::WalTruncated => Self::WalTruncated,
            PersistentCause::WalUnlinked => Self::WalUnlinked,
            PersistentCause::PageSizeChanged { was, now } => Self::PageSizeChanged { was, now },
            PersistentCause::DatabasePageMismatch { pgno } => Self::DatabasePageMismatch { pgno },
            PersistentCause::Bug => Self::Bug,
        }
    }
}

/// What the engine does about one [`PersistentCause`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Response {
    /// The lane that failed is degraded and retries with the capped doubling
    /// wait; the other lanes keep running.
    KeepTrying,
    /// The engine stops until a person restarts it.
    Stop,
}

/// What the engine does when the replica holds another writer's files.
/// Its own type: continuing damages the history, so it cannot be set by
/// copying another cause's [`Response`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForeignWriter {
    /// The engine stops: no upload, no compaction, no delete.
    Stop,
    /// The engine boots again and snapshots after the other writer's newest
    /// transaction, so the replica's one history interleaves two databases,
    /// and retention may delete the other writer's files.
    ContinueAndInterleaveHistories,
}

/// The response to each persistent cause: one named field per cause, never a
/// map, so a new cause does not build until every preset names its response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Responses {
    /// The store refuses the credentials.
    pub credentials: Response,
    /// The store refuses a request outright.
    pub store_refused: Response,
    /// A replica object is corrupt.
    pub remote_corrupt: Response,
    /// The replica went backwards. Keeping trying lists the replica again and
    /// continues from what it now holds.
    pub remote_regressed: Response,
    /// A committed WAL frame is corrupt. Keeping trying rescans: nothing is
    /// staged until the frame verifies or the WAL starts a new generation.
    pub wal_corrupt: Response,
    /// The WAL is truncated below its committed end.
    pub wal_truncated: Response,
    /// The WAL file was deleted.
    pub wal_unlinked: Response,
    /// The database's page size is not the replica's. Keeping trying never
    /// writes the new size: the way out is a restart onto a new prefix.
    pub page_size_changed: Response,
    /// A database-file page differs from the page checksum table. Keeping
    /// trying ships the page as the file holds it (the database the host
    /// serves), keeps the lane degraded and retention pinned until a verified
    /// WAL frame rewrites the page or an operator accepts it.
    pub database_page_mismatch: Response,
    /// A condition the code rules out happened. Keeping trying rebuilds the
    /// engine and boots again after the wait; it never continues from the
    /// state the failure left.
    pub bug: Response,
    /// Another writer replicates into this replica.
    pub foreign_writer: ForeignWriter,
}

impl Responses {
    /// Go's behaviour (the default): keep trying on every cause, stop on a
    /// second writer.
    pub const GO_LIKE: Self = Self {
        credentials: Response::KeepTrying,
        store_refused: Response::KeepTrying,
        remote_corrupt: Response::KeepTrying,
        remote_regressed: Response::KeepTrying,
        wal_corrupt: Response::KeepTrying,
        wal_truncated: Response::KeepTrying,
        wal_unlinked: Response::KeepTrying,
        page_size_changed: Response::KeepTrying,
        database_page_mismatch: Response::KeepTrying,
        bug: Response::KeepTrying,
        foreign_writer: ForeignWriter::Stop,
    };

    /// Stop on every cause.
    pub const STRICT: Self = Self {
        credentials: Response::Stop,
        store_refused: Response::Stop,
        remote_corrupt: Response::Stop,
        remote_regressed: Response::Stop,
        wal_corrupt: Response::Stop,
        wal_truncated: Response::Stop,
        wal_unlinked: Response::Stop,
        page_size_changed: Response::Stop,
        database_page_mismatch: Response::Stop,
        bug: Response::Stop,
        foreign_writer: ForeignWriter::Stop,
    };

    /// Whether `cause` stops the engine.
    #[must_use]
    pub const fn stops(&self, cause: PersistentCause) -> bool {
        match self.action(cause) {
            Action::Stop => true,
            Action::Retry | Action::Relist | Action::Rebuild => false,
        }
    }

    /// What the actor does about `cause`.
    pub(crate) const fn action(&self, cause: PersistentCause) -> Action {
        let response = match cause {
            PersistentCause::Credentials => self.credentials,
            PersistentCause::StoreRefused => self.store_refused,
            PersistentCause::RemoteCorrupt => self.remote_corrupt,
            PersistentCause::WalCorrupt => self.wal_corrupt,
            PersistentCause::WalTruncated => self.wal_truncated,
            PersistentCause::WalUnlinked => self.wal_unlinked,
            PersistentCause::PageSizeChanged { .. } => self.page_size_changed,
            PersistentCause::DatabasePageMismatch { .. } => self.database_page_mismatch,
            PersistentCause::RemoteRegressed => {
                return match self.remote_regressed {
                    Response::KeepTrying => Action::Relist,
                    Response::Stop => Action::Stop,
                };
            }
            PersistentCause::Bug => {
                return match self.bug {
                    Response::KeepTrying => Action::Rebuild,
                    Response::Stop => Action::Stop,
                };
            }
            PersistentCause::ForeignWriter => {
                return match self.foreign_writer {
                    ForeignWriter::ContinueAndInterleaveHistories => Action::Rebuild,
                    ForeignWriter::Stop => Action::Stop,
                };
            }
        };
        match response {
            Response::KeepTrying => Action::Retry,
            Response::Stop => Action::Stop,
        }
    }
}

impl Default for Responses {
    fn default() -> Self {
        Self::GO_LIKE
    }
}

/// What the actor does about a persistent cause, from its [`Response`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Degrade the lane and retry the same step after the wait.
    Retry,
    /// Degrade the upload lane, forget what the replica was known to hold,
    /// and list it again after the wait.
    Relist,
    /// Degrade the sync lane, drop every piece of engine state, and boot
    /// again after the wait.
    Rebuild,
    /// Stop the engine.
    Stop,
}

/// How long to wait before a retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backoff {
    /// [`retry_wait`] from the attempt count.
    Doubling,
    /// A wait the store named: the retry waits at least this long, and at
    /// least the doubling one ([`backoff_wait`]).
    After(Duration),
}

/// What to do about a [`Fault`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Policy {
    /// Try again after a wait.
    Retry(Backoff),
    /// Keep trying in a degraded state, or stop: the configured [`Responses`]
    /// for this cause decide.
    Persistent(PersistentCause),
    /// Stop: a drain's deadline passed with work left. Not configurable.
    Shutdown,
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
        | Fault::UploadChecksumMismatch
        | Fault::LocalDiskFull
        | Fault::LocalIo
        | Fault::LocalFsyncFailed
        | Fault::SqliteBusy
        | Fault::ReadersHeld
        | Fault::WalMoved
        | Fault::WalIndexInconsistent
        | Fault::WalChangedUnderRead
        | Fault::NothingToSnapshot
        | Fault::StagedLost
        | Fault::ReadTxLost => Policy::Retry(Backoff::Doubling),
        Fault::PageSizeChanged { was, now } => {
            Policy::Persistent(PersistentCause::PageSizeChanged {
                was: *was,
                now: *now,
            })
        }
        Fault::StoreAuth => Policy::Persistent(PersistentCause::Credentials),
        Fault::StoreRefused => Policy::Persistent(PersistentCause::StoreRefused),
        Fault::RemoteCorrupt => Policy::Persistent(PersistentCause::RemoteCorrupt),
        Fault::RemoteAheadOfLocal => Policy::Persistent(PersistentCause::ForeignWriter),
        Fault::RemoteRegressed => Policy::Persistent(PersistentCause::RemoteRegressed),
        Fault::WalCorruptBelowCommitted => Policy::Persistent(PersistentCause::WalCorrupt),
        Fault::WalTruncatedBelowCommitted => Policy::Persistent(PersistentCause::WalTruncated),
        Fault::WalUnlinked => Policy::Persistent(PersistentCause::WalUnlinked),
        Fault::DatabasePageMismatch { pgno } => {
            Policy::Persistent(PersistentCause::DatabasePageMismatch { pgno: *pgno })
        }
        Fault::ShutdownDeadline => Policy::Shutdown,
        Fault::Bug(_) => Policy::Persistent(PersistentCause::Bug),
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

/// The wait before retry `attempt`: the later of the doubling
/// [`retry_wait`] and the store's named wait, never shorter than either. The
/// one throttle rule: every actor lane and every `MaintainState` level waits
/// this long. The count, not the last wait, drives the doubling.
#[must_use]
pub fn backoff_wait(backoff: Backoff, attempt: NonZeroU32, interval: Duration) -> Duration {
    let doubling = retry_wait(attempt, interval);
    match backoff {
        Backoff::Doubling => doubling,
        Backoff::After(named) => doubling.max(named),
    }
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
            StoreError::Refused { .. } => Self::StoreRefused,
            StoreError::NotFound { .. } => Self::RemoteObjectMissing,
            StoreError::ScratchExhausted { .. } => Self::LocalDiskFull,
            StoreError::Corrupt { cause, .. } => match cause {
                CorruptCause::CommitSizeMismatch { .. } => Self::UploadAmbiguous,
                CorruptCause::ChecksumMismatch { .. } => Self::UploadChecksumMismatch,
                CorruptCause::NotARegularFile
                | CorruptCause::TimestampOutOfRange
                | CorruptCause::BadStamp => Self::RemoteCorrupt,
            },
        }
    }

    /// An error from the staging store, a local directory nothing else writes.
    ///
    /// The same error means something else here. A missing or unusable staged
    /// file is lost, not a relist. A write whose stored size differs is the
    /// torn write to encode again. A permission error retries, so an operator
    /// can fix the directory without a restart.
    #[must_use]
    pub const fn from_staging(err: &StoreError) -> Self {
        match err {
            StoreError::Transient { .. }
            | StoreError::Throttled { .. }
            | StoreError::Auth { .. }
            | StoreError::Refused { .. } => Self::LocalIo,
            StoreError::NotFound { .. } => Self::StagedLost,
            StoreError::ScratchExhausted { .. } => Self::LocalDiskFull,
            StoreError::Corrupt { cause, .. } => match cause {
                CorruptCause::CommitSizeMismatch { .. } | CorruptCause::ChecksumMismatch { .. } => {
                    Self::LocalFsyncFailed
                }
                CorruptCause::NotARegularFile
                | CorruptCause::TimestampOutOfRange
                | CorruptCause::BadStamp => Self::StagedLost,
            },
        }
    }

    /// A WAL scan error. Go turns every bad frame into end of file
    /// (`wal_reader.go:180-182`, `191-193`), so a corrupt committed frame looks
    /// like the end of the WAL there; here it is a persistent fault, and
    /// nothing past it is staged.
    #[must_use]
    pub fn from_wal(err: WalError) -> Self {
        match err {
            WalError::CorruptBelowCommitted { .. } => Self::WalCorruptBelowCommitted,
            WalError::TruncatedBelowCommitted { .. } => Self::WalTruncatedBelowCommitted,
            WalError::Restarted { .. } | WalError::CursorPastEnd { .. } => Self::WalMoved,
            WalError::PageSizeMismatch { expected, found } => Self::PageSizeChanged {
                was: found,
                now: expected,
            },
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
            ResumeError::PageSizeMismatch { expected, found } => Self::PageSizeChanged {
                was: found,
                now: expected,
            },
            ResumeError::Truncated { .. } | ResumeError::Restarted { .. } => Self::WalMoved,
            ResumeError::PrevFrame { .. } | ResumeError::NotAtCommit { .. } => {
                Self::Bug(BugSite::WalResume)
            }
        }
    }

    /// A page re-read error. A changed page is never encoded; the step rescans.
    #[must_use]
    pub fn from_read_page(err: ReadPageError) -> Self {
        match err {
            ReadPageError::Io(_) => Self::LocalIo,
            ReadPageError::ChangedSinceScan { .. } => Self::WalChangedUnderRead,
            ReadPageError::Truncated { .. } => Self::WalTruncatedBelowCommitted,
            ReadPageError::BufferSize { .. } | ReadPageError::PageSizeMismatch { .. } => {
                Self::Bug(BugSite::WalPageRead)
            }
        }
    }

    /// A page map error. The step feeds the map from one scan, so every one is a bug.
    #[must_use]
    pub const fn from_page_map(err: PageMapError) -> Self {
        match err {
            PageMapError::PageSizeMismatch { .. }
            | PageMapError::NotAfterEnd { .. }
            | PageMapError::Gap { .. }
            | PageMapError::Generation { .. } => Self::Bug(BugSite::PageMap),
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
            SourceError::ReadersHeld => Self::ReadersHeld,
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
