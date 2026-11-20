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

use std::num::NonZeroU32;

use lsr_ltx::{Checksum, PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{FrameCursor, FrameRef, WalOffset};

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

/// How many times the read transaction has been taken, counting from the first.
///
/// The checkpoint protocol releases and retakes the transaction, so a position
/// pinned to an epoch is pinned to one unbroken hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadEpoch(u64);

impl ReadEpoch {
    /// An epoch from its count; only a `WalSource` or a `Checkpointer` mints one.
    #[must_use]
    pub const fn new(count: u64) -> Self {
        Self(count)
    }

    /// The count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The epoch after a release and a retake.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
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
    digest: Checksum,
}

impl LastPage {
    /// The page `pgno` whose bytes checksum to `digest`.
    #[must_use]
    pub const fn new(pgno: PageNo, digest: Checksum) -> Self {
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
    pub const fn digest(&self) -> Checksum {
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
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "first caller is task 4b's recover; remove this then"
        )
    )]
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

    /// The same position with its continuity replaced.
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "first caller is task 4b's sync step; remove this then"
        )
    )]
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

/// The proof that a completed sync reached the committed end.
///
/// Built only by the sync step, in `StepEnd::Through`, and consumed by the
/// checkpoint protocol; it has no `Clone`, so a proof cannot outlive the
/// position it describes.
#[derive(Debug, PartialEq, Eq)]
pub struct SyncedThrough(Position);

impl SyncedThrough {
    /// Only the sync step proves it.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "first caller is task 4b's sync step; remove this then"
        )
    )]
    pub(crate) const fn new(position: Position) -> Self {
        Self(position)
    }

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
