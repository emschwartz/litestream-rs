//! Where the replica is in the WAL, in types that cannot be mixed up.
//!
//! Two numbers named "offset" caused Go's bug #997: the end of the WAL bytes a
//! staged file covers, and the length of the WAL file on disk (Go compares them
//! as raw `int64`, `db.go:1811-1813`). Here they are different types with no
//! conversion; the one allowed question is [`WalFileLen::covers`].
//!
//! A [`Position`] is what the next sync step resumes from. `Continuity` records
//! whether the read transaction has been held since the position was proven to
//! equal the committed end, which is the only thing that makes a WAL restart
//! harmless (see [`Continuity`]).

use std::fmt;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};

use lsr_ltx::{PageChecksum, PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{FrameCursor, FrameRef, WalOffset};

use crate::source::WalSource;

/// The end of the WAL bytes covered by the newest staged L0 file.
///
/// Not a file length: see [`WalFileLen`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SyncedOffset(WalOffset);

impl SyncedOffset {
    /// Only a position (and so only the sync step) says what is synced.
    pub(crate) const fn new(offset: WalOffset) -> Self {
        Self(offset)
    }

    /// The offset, for scans and for events.
    #[must_use]
    pub const fn offset(self) -> WalOffset {
        self.0
    }
}

/// The length of the WAL file on disk, as a `WalSource` reports it.
///
/// A file can be longer than what is synced (SQLite reuses a restarted WAL
/// without shrinking it) and shorter (a crash or a truncate). The only
/// comparison offered is [`Self::covers`]; there is deliberately no `From`
/// between this and [`SyncedOffset`]:
///
/// ```compile_fail,E0277
/// use lsr_core::{SyncedOffset, WalFileLen};
///
/// fn mixed_up(synced: SyncedOffset) {
///     let _: WalFileLen = synced.into();
/// }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalFileLen(u64);

impl WalFileLen {
    /// A length in bytes. Only a `WalSource` builds one, from a stat of the file.
    #[must_use]
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// The length in bytes, for events.
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.0
    }

    /// Whether the file holds every byte up to the synced end.
    #[must_use]
    pub fn covers(self, end: SyncedOffset) -> bool {
        self.0 >= end.0.get()
    }
}

/// One hold of the read transaction: which holder took it, and how many
/// times that holder has taken it, counting from the first.
///
/// The checkpoint protocol releases and retakes the transaction, so a position
/// pinned to an epoch is pinned to one unbroken hold. The holder is drawn from
/// a process-wide counter when a source opens, so the first hold of a reopened
/// source is never equal to the first hold of the source it replaces: a pin
/// taken under one open cannot pass for a pin under the next.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReadEpoch {
    holder: u64,
    count: u64,
}

impl fmt::Debug for ReadEpoch {
    /// The count only. The holder is whatever the process counter had reached,
    /// so printing it would make one scenario print differently from run to
    /// run (simulator traces, the live-against-model comparisons).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadEpoch").field(&self.count).finish()
    }
}

/// The holders handed out so far in this process.
static HOLDERS: AtomicU64 = AtomicU64::new(0);

impl ReadEpoch {
    /// The first hold of a new holder, equal to no epoch minted before it.
    /// Only a `WalSource` (or a test standing in for one) mints one, once per open.
    #[must_use]
    pub fn first() -> Self {
        Self {
            holder: HOLDERS.fetch_add(1, Ordering::Relaxed),
            count: 1,
        }
    }

    /// How many times this holder has taken the read transaction.
    #[must_use]
    pub const fn count(self) -> u64 {
        self.count
    }

    /// The same holder's epoch after a release and a retake.
    #[must_use]
    pub const fn next(self) -> Self {
        Self {
            holder: self.holder,
            count: self.count.saturating_add(1),
        }
    }
}

/// Whether a WAL restart can have lost frames this position never saw.
///
/// `Pinned(e)` means: during read epoch `e`, this position's synced end
/// equalled the committed end. While the epoch holds, a read mark above 0 blocks
/// every restart, and under mark 0 a restart can happen once and cannot be
/// followed by another, so nothing past the position can be discarded.
///
/// It is assigned in exactly three places:
/// 1. `recover` returning `Continue` when the tip's end equals the committed
///    end of the same `WalView`.
/// 2. A sync step ending `Idle` or `Through` whose epoch is the same before the
///    scan and after the staging commit.
/// 3. Step 6 of the checkpoint protocol.
///
/// Everything else is `Unpinned`: a `Continue` whose cursor is below the committed
/// end, and any step ending `Limited`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Continuity {
    /// Proven complete during this read epoch.
    Pinned(ReadEpoch),
    /// Not proven; a salt change must be treated as possible loss.
    Unpinned,
}

/// The page of the last frame a position covers, to notice a WAL rewritten with the same salts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LastPage {
    pgno: PageNo,
    digest: PageChecksum,
}

impl LastPage {
    /// The page `pgno` whose bytes checksum to `digest`.
    #[must_use]
    pub const fn new(pgno: PageNo, digest: PageChecksum) -> Self {
        Self { pgno, digest }
    }

    /// The page of a scanned frame.
    #[must_use]
    pub const fn of_frame(frame: &FrameRef) -> Self {
        Self::new(frame.pgno(), frame.digest())
    }

    /// The page number.
    #[must_use]
    pub const fn pgno(&self) -> PageNo {
        self.pgno
    }

    /// The checksum of the page's bytes.
    #[must_use]
    pub const fn digest(&self) -> PageChecksum {
        self.digest
    }
}

/// What the newest staged L0 file covers, and how far that can be trusted.
///
/// The synced offset and page size are the cursor's: one source of truth, so a
/// position cannot hold a cursor for one place and an offset for another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    txid: Txid,
    cursor: FrameCursor,
    commit: NonZeroU32,
    last_page: LastPage,
    timestamp: Timestamp,
    continuity: Continuity,
}

impl Position {
    /// Only the sync step, `recover` and the checkpoint protocol build positions.
    pub(crate) const fn new(
        txid: Txid,
        cursor: FrameCursor,
        commit: NonZeroU32,
        last_page: LastPage,
        timestamp: Timestamp,
        continuity: Continuity,
    ) -> Self {
        Self {
            txid,
            cursor,
            commit,
            last_page,
            timestamp,
            continuity,
        }
    }

    /// The same position with its cursor replaced: a scan that found nothing
    /// new ends at the cursor it read up to.
    #[must_use]
    pub(crate) const fn with_cursor(self, cursor: FrameCursor) -> Self {
        Self { cursor, ..self }
    }

    /// The same position with its continuity replaced.
    #[must_use]
    pub(crate) const fn with_continuity(self, continuity: Continuity) -> Self {
        Self { continuity, ..self }
    }

    /// The transaction id of the newest staged file.
    #[must_use]
    pub const fn txid(&self) -> Txid {
        self.txid
    }

    /// Where the next frame starts and the checksum it continues from.
    #[must_use]
    pub const fn cursor(&self) -> FrameCursor {
        self.cursor
    }

    /// The end of the WAL bytes the newest staged file covers.
    #[must_use]
    pub const fn synced(&self) -> SyncedOffset {
        SyncedOffset::new(self.cursor.next())
    }

    /// The database size in pages after the last transaction covered.
    #[must_use]
    pub const fn commit(&self) -> NonZeroU32 {
        self.commit
    }

    /// The database page size.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.cursor.next().page_size()
    }

    /// The page of the last frame covered.
    #[must_use]
    pub const fn last_page(&self) -> LastPage {
        self.last_page
    }

    /// The timestamp of the newest staged file.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Whether a WAL restart can have lost frames.
    #[must_use]
    pub const fn continuity(&self) -> Continuity {
        self.continuity
    }
}

/// A position a sync step found at the committed end of the wal-index read it
/// ran under, during one read epoch: half of the proof. The other half is that
/// the epoch still held once the step's file was staged, and only
/// [`Self::confirm`], which reads the epoch again, turns this into a
/// [`SyncedThrough`].
#[derive(Debug, PartialEq, Eq)]
pub struct ReachedEnd {
    position: Position,
    epoch: ReadEpoch,
}

impl ReachedEnd {
    /// Only the sync step finds one.
    pub(crate) const fn new(position: Position, epoch: ReadEpoch) -> Self {
        Self {
            position: position.with_continuity(Continuity::Unpinned),
            epoch,
        }
    }

    /// The position, unpinned: only [`Self::confirm`] pins it.
    #[must_use]
    pub const fn position(&self) -> &Position {
        &self.position
    }

    /// The read epoch the step ran under.
    #[must_use]
    pub const fn epoch(&self) -> ReadEpoch {
        self.epoch
    }

    /// The position, unpinned, giving up the chance to prove it.
    #[must_use]
    pub const fn into_position(self) -> Position {
        self.position
    }

    /// Reads `wal`'s epoch again. The same epoch is the proof.
    ///
    /// # Errors
    /// The position, unpinned, when the read transaction was released and
    /// retaken since the step began: a restart in between could have dropped
    /// frames the step never saw.
    pub fn confirm<W: WalSource>(self, wal: &W) -> Result<SyncedThrough, Position> {
        if wal.epoch() == self.epoch {
            let pinned = Continuity::Pinned(self.epoch);
            Ok(SyncedThrough(self.position.with_continuity(pinned)))
        } else {
            Err(self.position)
        }
    }
}

/// The proof that a completed sync reached the committed end.
///
/// Built only by [`ReachedEnd::confirm`], from a sync step's end and a second
/// read of the epoch, and consumed by the checkpoint protocol; it has no
/// `Clone`, so a proof cannot outlive the position it describes.
#[derive(Debug, PartialEq, Eq)]
pub struct SyncedThrough(Position);

impl SyncedThrough {
    /// The position that equals the committed end.
    #[must_use]
    pub const fn position(&self) -> &Position {
        &self.0
    }

    /// The position, giving up the proof.
    #[must_use]
    pub fn into_position(self) -> Position {
        self.0
    }
}

#[cfg(test)]
mod tests;
