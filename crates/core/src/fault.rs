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
//! fix: credentials refused, a corrupt object, a damaged WAL, another writer.
//! What happens then is configuration, one field of [`Responses`] per
//! [`PersistentCause`] (`docs/PLAN.md`, the degraded-state section). [`Response::KeepTrying`]
//! puts the task that failed in a degraded state: it retries with the doubling
//! wait, capped at [`MAX_BACKOFF`] as Go's sync loop is, the other tasks keep
//! running, and every retry that fails is reported. [`Response::Stop`] stops
//! the whole engine for a person, as every persistent fault did before.
//! [`Responses::GO_LIKE`] (the default) keeps trying everywhere except on a
//! other writer; [`Responses::STRICT`] stops on every cause.
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
//! `synchronous=NORMAL`, and stopping would leave every later write unreplicated.

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{PageMapError, ReadPageError, ResumeError, WalError, WalOffset};

use crate::compact::CompactError;
use crate::created_at::ResolveError;
use crate::maintain::MaintainError;
use crate::source::SourceError;
use crate::store::{CorruptCause, ListingStalled, StoreError};

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
    /// are derived state: when a read on a later tick fails on the same file
    /// too (one failed read says nothing about the stored bytes), the engine
    /// deletes that file and every staged file after it, and the next file is
    /// a snapshot taking the first transaction the replica lacks, so the chain
    /// has no hole (`upload::sweep_staged` at boot, the upload task mid-run;
    /// elsewhere the task retries and the next boot sweeps).
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
    /// An fsync failed. Of a staged file: encode and write it again. Of the
    /// WAL, before staging (`WalSource::sync`): nothing is staged, and the
    /// source keeps refusing, so the retries end degraded.
    LocalFsyncFailed,
    /// SQLite is busy, or the wal-index is mid-update.
    SqliteBusy,
    /// A TRUNCATE checkpoint was refused before SQLite was asked: the application
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
    /// A file's created-at time was asked for twice in one resolve.
    CreatedAtRepeated,
    /// The WAL reader was resumed from a cursor its own scan did not give.
    WalResume,
    /// A WAL page was read back into a buffer of the wrong size.
    WalPageRead,
    /// The page map was fed out of the order one scan produces.
    PageMap,
}

impl BugSite {
    /// The fault to report for a condition the code rules out.
    ///
    /// Release builds never panic here: the result is [`Fault::Bug`], which
    /// rebuilds the engine instead of killing the host (`panic = "abort"` hosts
    /// included). Debug and test builds panic at the caller's line, so the
    /// simulator and fuzzers stop on the first bad step rather than on a
    /// restore diff many operations later.
    #[must_use]
    #[track_caller]
    #[cfg_attr(
        debug_assertions,
        expect(clippy::panic, reason = "the debug half of the invariant tier")
    )]
    pub fn hit(self) -> Fault {
        #[cfg(debug_assertions)]
        if PANIC_ON_BUG.with(std::cell::Cell::get) {
            panic!("invariant violated: {self:?}");
        }
        Fault::Bug(self)
    }
}

#[cfg(debug_assertions)]
thread_local! {
    static PANIC_ON_BUG: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

/// Runs `f` with [`BugSite::hit`] reporting instead of panicking, on this
/// thread, so a test can provoke a bug deliberately and read the fault.
/// A no-op wrapper in release builds, where `hit` never panics.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub fn reporting_bugs<R>(f: impl FnOnce() -> R) -> R {
    #[cfg(debug_assertions)]
    let before = PANIC_ON_BUG.with(|flag| flag.replace(false));
    let out = f();
    #[cfg(debug_assertions)]
    PANIC_ON_BUG.with(|flag| flag.set(before));
    out
}

/// Whether the `verify` feature is on: expensive checks run only when it is.
/// A const rather than a `cfg` at each site, so the check always compiles.
pub const VERIFY: bool = cfg!(feature = "verify");

/// Returns `Err(BugSite::hit(site))` from the enclosing function when `cond`
/// is false: a panic in debug and test builds, [`Fault::Bug`] in release.
///
/// For invariants whose violation release must report. A cheap pre or
/// postcondition that release may ignore is a `debug_assert!`; a check too
/// expensive for release goes behind [`VERIFY`].
///
/// ```ignore
/// invariant!(map.commit() == Some(commit), BugSite::SnapshotScan);
/// ```
#[macro_export]
macro_rules! invariant {
    ($cond:expr, $site:expr $(,)?) => {
        if !$cond {
            return ::core::result::Result::Err($crate::BugSite::hit($site));
        }
    };
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
    NewestStagedFileUnreadable,
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
    /// At boot, staged files the replica did not have yet were not vouched
    /// for by the live WAL (a staging backup put back, a file replaced, or a
    /// chain from a WAL generation that is gone), and the WAL could not
    /// regenerate them: they were deleted unpublished, and the snapshot
    /// holds their end state. Their restore points are given up.
    StagedUnwitnessed {
        /// The first transaction deleted.
        from: Txid,
    },
}

/// Why the engine stopped and waits for a person: a [`PersistentCause`]
/// whose configured response is to stop, or a drain whose deadline passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopCause {
    /// Credentials or permissions refuse the store.
    Credentials,
    /// The store refuses a request outright (not credentials).
    StoreRefused,
    /// A replica object is corrupt.
    RemoteCorrupt,
    /// Another writer replicates into this replica.
    OtherWriter,
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
    OtherWriter,
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
    pub const fn of(cause: StopCause) -> Option<Self> {
        match cause {
            StopCause::Credentials => Some(Self::Credentials),
            StopCause::StoreRefused => Some(Self::StoreRefused),
            StopCause::RemoteCorrupt => Some(Self::RemoteCorrupt),
            StopCause::OtherWriter => Some(Self::OtherWriter),
            StopCause::RemoteRegressed => Some(Self::RemoteRegressed),
            StopCause::WalCorrupt => Some(Self::WalCorrupt),
            StopCause::WalTruncated => Some(Self::WalTruncated),
            StopCause::WalUnlinked => Some(Self::WalUnlinked),
            StopCause::PageSizeChanged { was, now } => Some(Self::PageSizeChanged { was, now }),
            StopCause::DatabasePageMismatch { pgno } => Some(Self::DatabasePageMismatch { pgno }),
            StopCause::Bug => Some(Self::Bug),
            StopCause::Shutdown => None,
        }
    }
}

impl From<PersistentCause> for StopCause {
    fn from(cause: PersistentCause) -> Self {
        match cause {
            PersistentCause::Credentials => Self::Credentials,
            PersistentCause::StoreRefused => Self::StoreRefused,
            PersistentCause::RemoteCorrupt => Self::RemoteCorrupt,
            PersistentCause::OtherWriter => Self::OtherWriter,
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
    /// The task that failed is degraded and retries with the capped doubling
    /// wait; the other tasks keep running.
    KeepTrying,
    /// The engine stops until a person restarts it.
    Stop,
}

/// What the engine does when the replica holds another writer's files.
/// Its own type: continuing damages the history, so it cannot be set by
/// copying another cause's [`Response`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OtherWriter {
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
    /// trying ships the page as the file holds it (the database the application
    /// serves), keeps the task degraded and retention pinned until a verified
    /// WAL frame rewrites the page or an operator accepts it.
    pub database_page_mismatch: Response,
    /// A condition the code rules out happened. Keeping trying rebuilds the
    /// engine and boots again after the wait; it never continues from the
    /// state the failure left.
    pub bug: Response,
    /// Another writer replicates into this replica.
    pub other_writer: OtherWriter,
}

impl Responses {
    /// Go's behaviour (the default): keep trying on every cause, stop on a
    /// other writer.
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
        other_writer: OtherWriter::Stop,
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
        other_writer: OtherWriter::Stop,
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
            PersistentCause::OtherWriter => {
                return match self.other_writer {
                    OtherWriter::ContinueAndInterleaveHistories => Action::Rebuild,
                    OtherWriter::Stop => Action::Stop,
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
    /// Degrade the task and retry the same step after the wait.
    Retry,
    /// Degrade the upload task, forget what the replica was known to hold,
    /// and list it again after the wait.
    Relist,
    /// Degrade the sync task, drop every piece of engine state, and boot
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
        Fault::RemoteAheadOfLocal => Policy::Persistent(PersistentCause::OtherWriter),
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
/// one throttle rule: every actor task and every `MaintainState` level waits
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
                | CorruptCause::BadCreatedAt => Self::RemoteCorrupt,
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
                | CorruptCause::BadCreatedAt => Self::StagedLost,
            },
        }
    }

    /// A staging listing whose cursor does not advance. The local store is
    /// misbehaving, so this is [`Self::LocalIo`]: retried with backoff and
    /// reported degraded, never [`Self::StagedLost`], which would delete
    /// staged files that may be sound.
    #[must_use]
    pub const fn from_staging_listing(_stalled: ListingStalled) -> Self {
        Self::LocalIo
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

    /// An error from compaction.
    #[must_use]
    pub(crate) fn from_compact(err: &CompactError) -> Self {
        match err {
            CompactError::InputVanished { .. } => Self::RemoteObjectMissing,
            CompactError::Source(err) | CompactError::Dest(err) => Self::from_remote(err),
            CompactError::Input { .. } | CompactError::Encode(_) => Self::RemoteCorrupt,
            CompactError::Inputs(_) | CompactError::SnapshotLevel { .. } => {
                Self::Bug(BugSite::CompactionInputs)
            }
        }
    }

    /// Whether a failed snapshot may have left an object under its key: the store
    /// answered the commit with a size or a checksum that is not the one sent.
    #[must_use]
    pub(crate) const fn leaves_bad_snapshot(self) -> bool {
        match self {
            Self::UploadAmbiguous | Self::UploadChecksumMismatch => true,
            Self::StoreTransient
            | Self::StoreThrottled { .. }
            | Self::StoreAuth
            | Self::RemoteObjectMissing
            | Self::StoreRefused
            | Self::RemoteCorrupt
            | Self::RemoteAheadOfLocal
            | Self::RemoteRegressed
            | Self::StagedLost
            | Self::LocalDiskFull
            | Self::LocalIo
            | Self::LocalFsyncFailed
            | Self::SqliteBusy
            | Self::ReadersHeld
            | Self::WalMoved
            | Self::WalIndexInconsistent
            | Self::WalChangedUnderRead
            | Self::NothingToSnapshot
            | Self::ReadTxLost
            | Self::PageSizeChanged { .. }
            | Self::WalCorruptBelowCommitted
            | Self::WalTruncatedBelowCommitted
            | Self::WalUnlinked
            | Self::DatabasePageMismatch { .. }
            | Self::ShutdownDeadline
            | Self::Bug(_) => false,
        }
    }

    /// An error resolving a file's created-at time.
    #[must_use]
    pub(crate) fn from_created_at(err: &ResolveError) -> Self {
        match err {
            ResolveError::Store(err) => Self::from_remote(err),
            ResolveError::Repeated { .. } => Self::Bug(BugSite::CreatedAtRepeated),
        }
    }

    /// An error from a maintenance step.
    #[must_use]
    pub(crate) fn from_maintain(err: &MaintainError) -> Self {
        match err {
            MaintainError::Compact(err) | MaintainError::OutputDistrusted { source: err, .. } => {
                Self::from_compact(err)
            }
            MaintainError::Delete(err) => Self::from_remote(err),
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
