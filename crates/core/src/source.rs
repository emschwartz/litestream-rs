//! The doors from core to SQLite and to the files beside it.
//!
//! Core decides; these traits do. Nothing in core opens the WAL, reads the
//! wal-index, stats a file or runs a pragma except through them, so the
//! simulator can stand a fake in for each and script every fault below.
//!
//! The one read that matters most is [`WalSource::committed`]: the frame
//! count, the salts and `nBackfill` come from a single read of the wal-index
//! header, never two. Go takes the count from `PRAGMA wal_checkpoint(NOOP)` and
//! then rereads the WAL header to learn the generation (`db.go:1927-1960`),
//! which leaves a window where the generation changes between the two.

use std::io::{self, Read, Seek};

use lsr_ltx::PageNo;
use lsr_wal::{CommittedEnd, Salts};
use thiserror::Error;

use crate::position::{ReadEpoch, WalFileLen};

#[cfg(any(test, feature = "testing"))]
mod fake;
#[cfg(any(test, feature = "testing"))]
pub use fake::{
    CheckpointCall, CheckpointEffect, CheckpointScript, FakeCheckpointer, FakeDb, FakeWal, SourceOp,
};

/// What went wrong reaching SQLite or a local file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum SourceError {
    /// A read, write or stat failed.
    #[error("local I/O failed ({0})")]
    Io(io::ErrorKind),
    /// The disk has no room.
    #[error("the disk is full")]
    DiskFull,
    /// An fsync failed: the bytes it covered may not be durable, so a caller
    /// writes them again from scratch and never fsyncs the old file twice.
    #[error("fsync failed")]
    FsyncFailed,
    /// SQLite reported busy, or the wal-index header is mid-update (its two copies differ).
    #[error("SQLite is busy")]
    Busy,
    /// The checkpoint was not attempted: the host's readers hold the WAL, and
    /// a TRUNCATE would wait on them. Nothing is wrong with SQLite.
    #[error("the host's readers hold the WAL")]
    ReadersHeld,
    /// A database page read returned fewer bytes than a page.
    #[error("short read of page {pgno:?}")]
    Short {
        /// The page.
        pgno: PageNo,
    },
    /// The WAL file was deleted while it was open.
    #[error("the WAL file was unlinked")]
    Unlinked,
    /// The read transaction could not be retaken after a checkpoint.
    #[error("the read transaction was lost")]
    ReadTxLost,
}

/// One read of the wal-index.
///
/// In phase 3 this is the header at `-shm` offset 0, read under the read lock:
/// the two 48-byte copies must be equal with a valid `aCksum`; `mxFrame` is at
/// byte 16, `aSalt` at byte 32, `nBackfill` at byte 96. Unequal copies mean a
/// writer is mid-update and the read is `SourceError::Busy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedRead {
    end: CommittedEnd,
    backfilled: u32,
}

impl CommittedRead {
    /// `None` when `backfilled` exceeds the frame count, which no wal-index holds.
    #[must_use]
    pub const fn new(end: CommittedEnd, backfilled: u32) -> Option<Self> {
        if backfilled > end.frame_count() {
            None
        } else {
            Some(Self { end, backfilled })
        }
    }

    /// The committed end and the generation it counts frames of.
    #[must_use]
    pub const fn end(&self) -> CommittedEnd {
        self.end
    }

    /// How many frames of this generation SQLite has copied into the database file.
    #[must_use]
    pub const fn backfilled(&self) -> u32 {
        self.backfilled
    }

    /// Whether every frame is in the database file. A connection on read mark 0
    /// has no committed end of its own, but this read answers anyway.
    #[must_use]
    pub const fn fully_backfilled(&self) -> bool {
        self.backfilled == self.end.frame_count()
    }

    /// Whether the wal-index names a generation with no frame in it that the
    /// WAL file's header, whose salts are `header`, does not name.
    ///
    /// SQLite resets the wal-index before it writes the new header: a log
    /// restart whose header write failed, a truncating checkpoint whose
    /// truncate failed, and its recovery of a header-only WAL (which leaves the
    /// salts at zero) all stop here until the next commit writes a header.
    /// Either way the database file holds everything the index counted: a
    /// restart or a truncate needs every frame backfilled first, and a recovery
    /// that read no header found no frame.
    #[must_use]
    pub fn reset_ahead_of(&self, header: Salts) -> bool {
        self.end.frame_count() == 0 && self.end.salts() != header
    }
}

/// The WAL file and the wal-index beside it.
#[expect(
    clippy::len_without_is_empty,
    reason = "len is a fallible stat answering None for no file; an is_empty would blur that"
)]
pub trait WalSource {
    /// The open WAL file.
    type Wal: Read + Seek;

    /// Opens the WAL; `None` means there is no WAL file.
    ///
    /// # Errors
    /// [`SourceError`] when the file exists and cannot be opened.
    fn open(&mut self) -> Result<Option<Self::Wal>, SourceError>;

    /// The WAL file's length on disk; `None` when there is no file.
    ///
    /// # Errors
    /// [`SourceError`] when the stat fails.
    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError>;

    /// One read of the wal-index.
    ///
    /// # Errors
    /// [`SourceError::Busy`] while a writer is mid-update; others when the read fails.
    fn committed(&mut self) -> Result<CommittedRead, SourceError>;

    /// Which hold of the read transaction the answers above belong to.
    fn epoch(&self) -> ReadEpoch;
}

/// The database file, read for snapshots.
pub trait PageSource {
    /// The database file length over the page size.
    ///
    /// # Errors
    /// [`SourceError`] when the stat fails.
    fn page_count(&mut self) -> Result<u32, SourceError>;

    /// Reads one page into `buf`, which is exactly one page long.
    ///
    /// # Errors
    /// [`SourceError::Short`] when the file ends inside the page (Go ignores
    /// short reads, `db.go:2493`); others when the read fails.
    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError>;
}

/// How hard a checkpoint tries. These are the only two modes Go's own policy
/// uses (`db.go:1524`, `1546`, `1555`, `1580`); its RESTART and FULL path
/// (`db.go:2743-2751`) is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointMode {
    /// Copy what no reader needs and return.
    Passive,
    /// Copy everything, then truncate the WAL file, waiting for readers.
    Truncate,
}

/// What a checkpoint reported: SQLite's three result columns, plus the epoch
/// of the read transaction retaken afterwards. Go discards `busy`
/// (`db.go:2838-2848`); here the caller decides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointRun {
    /// The checkpoint could not finish.
    pub busy: bool,
    /// Frames in the WAL when it ended.
    pub log: u32,
    /// Frames copied into the database file.
    pub checkpointed: u32,
    /// The epoch of the read transaction retaken after the checkpoint.
    pub epoch: ReadEpoch,
}

/// The write barrier on `_litestream_lock`, held between `begin_barrier` and `end_barrier`.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a barrier must be ended with Checkpointer::end_barrier"]
pub struct Barrier(u64);

impl Barrier {
    /// A token with the implementation's own id.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The implementation's id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.0
    }
}

/// The database write lock, held between `begin_write_lock` and `end_write_lock`.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a write lock must be ended with Checkpointer::end_write_lock"]
pub struct WriteLock(u64);

impl WriteLock {
    /// A token with the implementation's own id.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The implementation's id.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.0
    }
}

/// Everything the checkpoint protocol does to SQLite besides reading.
pub trait Checkpointer {
    /// `BEGIN; INSERT INTO _litestream_lock (id) VALUES (1)` (Go `db.go:2660-2674`).
    ///
    /// # Errors
    /// [`SourceError::Busy`] when a writer holds the lock; others on failure.
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError>;

    /// Rolls the barrier back.
    ///
    /// # Errors
    /// [`SourceError`] when the rollback fails.
    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError>;

    /// Releases the read transaction, runs `PRAGMA wal_checkpoint(mode)`, then
    /// takes the read transaction again (Go `db.go:2805-2849`).
    ///
    /// # Errors
    /// [`SourceError::ReadTxLost`] when the retake fails; others on failure.
    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError>;

    /// Writes the frame that moves the WAL past the checkpoint (Go `db.go:1667`).
    ///
    /// # Errors
    /// [`SourceError`] when the write fails.
    fn bump_seq(&mut self) -> Result<(), SourceError>;

    /// Takes the write lock (Go `db.go:2759-2771`).
    ///
    /// # Errors
    /// [`SourceError::Busy`] when another writer holds it.
    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError>;

    /// Releases the write lock (Go `db.go:2795`).
    ///
    /// # Errors
    /// [`SourceError`] when the release fails.
    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError>;
}
