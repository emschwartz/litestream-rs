//! WAL frames: offsets, the verified-frame check and the ways a frame can be bad.

use std::fmt;

use lsr_ltx::{Checksum, PageNo, PageSize};

use crate::checksum::{ByteOrder, WalChecksum};
use crate::conv::Fields;
use crate::header::{HEADER_LEN, Salts};

/// Length of a frame header.
pub const FRAME_HEADER_LEN: usize = 24;
/// Frame header bytes a frame checksum covers before it moves on to the page.
const CHECKSUMMED_HEADER_LEN: usize = 8;

/// Bytes in one frame: its header and one page. At most 65560, so every
/// product of it and a frame index fits a `u64`.
pub(crate) fn frame_len(page_size: PageSize) -> u64 {
    u64::from(page_size.get()).saturating_add(24)
}

/// Where a frame starts: the header's 32 bytes plus a whole number of frames.
///
/// It remembers its page size, so an offset made for one WAL is refused by
/// another WAL's reader instead of landing between its frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WalOffset {
    index: u32,
    page_size: PageSize,
}

impl WalOffset {
    /// The offset of the first frame, right after the header.
    #[must_use]
    pub const fn start(page_size: PageSize) -> Self {
        Self {
            index: 0,
            page_size,
        }
    }

    /// The offset of the frame after `index` whole frames.
    #[must_use]
    pub const fn from_frame_index(index: u32, page_size: PageSize) -> Self {
        Self { index, page_size }
    }

    /// `None` unless `bytes` is the header plus a whole number of frames (at most `u32::MAX`).
    #[must_use]
    pub fn new(bytes: u64, page_size: PageSize) -> Option<Self> {
        let after_header = bytes.checked_sub(u64::try_from(HEADER_LEN).ok()?)?;
        let len = frame_len(page_size);
        if after_header.checked_rem(len)? != 0 {
            return None;
        }
        let index = u32::try_from(after_header.checked_div(len)?).ok()?;
        Some(Self { index, page_size })
    }

    /// The byte offset in the WAL file.
    #[must_use]
    pub fn get(self) -> u64 {
        // Cannot saturate: 2^32 frames of at most 65560 bytes is below 2^49.
        frame_len(self.page_size)
            .saturating_mul(u64::from(self.index))
            .saturating_add(u64::try_from(HEADER_LEN).unwrap_or(0))
    }

    /// How many whole frames lie before this offset.
    #[must_use]
    pub const fn frame_index(self) -> u32 {
        self.index
    }

    /// The page size this offset was made for.
    #[must_use]
    pub const fn page_size(self) -> PageSize {
        self.page_size
    }

    pub(crate) fn next_frame(self) -> Option<Self> {
        Some(Self {
            index: self.index.checked_add(1)?,
            ..self
        })
    }

    pub(crate) fn prev_frame(self) -> Option<Self> {
        Some(Self {
            index: self.index.checked_sub(1)?,
            ..self
        })
    }
}

impl fmt::Display for WalOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "byte {} (frame {})", self.get(), self.index)
    }
}

/// The end of the committed WAL: where SQLite's wal-index says the last
/// commit ends.
///
/// Only construct it from the wal-index `mxFrame` read under the read lock.
/// Frames below it are guaranteed committed, so a bad one there is corruption
/// and never the end of the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CommittedEnd(WalOffset);

impl CommittedEnd {
    /// The end after `mx_frame` frames; zero means an empty WAL.
    #[must_use]
    pub const fn from_frame_count(mx_frame: u32, page_size: PageSize) -> Self {
        Self(WalOffset::from_frame_index(mx_frame, page_size))
    }

    /// The offset of the end.
    #[must_use]
    pub const fn offset(self) -> WalOffset {
        self.0
    }

    /// The `mxFrame` this was built from.
    #[must_use]
    pub const fn frame_count(self) -> u32 {
        self.0.frame_index()
    }
}

/// Why a frame is not part of the valid WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameFault {
    /// The frame's salts are not the generation's: an old generation's leftover.
    SaltMismatch {
        /// The salts the frame carries.
        found: Salts,
    },
    /// The cumulative checksum does not match.
    ChecksumMismatch,
    /// The frame checks out but names page zero or the lock page.
    BadPageNumber {
        /// The page number as stored.
        raw: u32,
    },
}

/// A frame found by a scan: the page it holds is not kept, only a way to fetch it again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameRef {
    pub(crate) pgno: PageNo,
    pub(crate) offset: WalOffset,
    pub(crate) digest: Checksum,
}

impl FrameRef {
    /// The database page this frame holds.
    #[must_use]
    pub const fn pgno(&self) -> PageNo {
        self.pgno
    }

    /// Where the frame starts in the WAL.
    #[must_use]
    pub const fn offset(&self) -> WalOffset {
        self.offset
    }

    /// The LTX page checksum of the page as the scan verified it.
    #[must_use]
    pub const fn digest(&self) -> Checksum {
        self.digest
    }
}

/// A frame that passed salt, checksum and page-number checks.
pub(crate) struct VerifiedFrame {
    pub(crate) pgno: PageNo,
    /// Database size in pages after this frame; non-zero on a commit frame.
    pub(crate) commit: u32,
    /// The running checksum after this frame.
    pub(crate) checksum: WalChecksum,
    pub(crate) digest: Checksum,
}

/// Checks one whole frame (`FRAME_HEADER_LEN` plus a page) against the
/// generation's salts and the checksum so far. The caller passes exactly that
/// many bytes; any other length cannot checksum and reports a mismatch.
pub(crate) fn verify_frame(
    frame: &[u8],
    order: ByteOrder,
    page_size: PageSize,
    salts: Salts,
    running: WalChecksum,
) -> Result<VerifiedFrame, FrameFault> {
    let (header, page) = frame
        .split_at_checked(FRAME_HEADER_LEN)
        .ok_or(FrameFault::ChecksumMismatch)?;
    let mut fields = Fields::new(header);
    let raw_pgno = fields.u32();
    let commit = fields.u32();
    let found = Salts {
        salt1: fields.u32(),
        salt2: fields.u32(),
    };
    let stored = WalChecksum::new(fields.u32(), fields.u32());
    if found != salts {
        return Err(FrameFault::SaltMismatch { found });
    }
    let computed = header
        .get(..CHECKSUMMED_HEADER_LEN)
        .and_then(|head| running.extend(order, head))
        .and_then(|sum| sum.extend(order, page));
    let Some(checksum) = computed.filter(|sum| *sum == stored) else {
        return Err(FrameFault::ChecksumMismatch);
    };
    let pgno =
        PageNo::new(raw_pgno, page_size).ok_or(FrameFault::BadPageNumber { raw: raw_pgno })?;
    Ok(VerifiedFrame {
        pgno,
        commit,
        checksum,
        digest: Checksum::of_page(pgno, page),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn size(bytes: u32) -> PageSize {
        PageSize::new(bytes).unwrap()
    }

    #[test]
    fn offsets_are_the_header_plus_whole_frames() {
        let ps = size(512);
        assert_eq!(WalOffset::new(32, ps), Some(WalOffset::start(ps)));
        assert_eq!(
            WalOffset::new(32 + 536, ps).map(WalOffset::frame_index),
            Some(1)
        );
        for bad in [0, 31, 33, 32 + 535, 32 + 537] {
            assert_eq!(WalOffset::new(bad, ps), None, "offset {bad}");
        }
        assert_eq!(WalOffset::from_frame_index(3, ps).get(), 32 + 3 * 536);
        assert_eq!(
            WalOffset::new(WalOffset::from_frame_index(7, ps).get(), ps)
                .map(WalOffset::frame_index),
            Some(7)
        );
    }

    #[test]
    fn offsets_past_the_frame_counter_are_refused() {
        let ps = size(65536);
        let past = 32 + (u64::from(u32::MAX) + 1) * 65560;
        assert_eq!(WalOffset::new(past, ps), None);
        assert!(WalOffset::new(past - 65560, ps).is_some());
    }

    #[test]
    fn neighbours_stop_at_the_ends() {
        let ps = size(512);
        assert_eq!(WalOffset::start(ps).prev_frame(), None);
        assert_eq!(WalOffset::from_frame_index(u32::MAX, ps).next_frame(), None);
        assert_eq!(
            WalOffset::start(ps).next_frame(),
            Some(WalOffset::from_frame_index(1, ps))
        );
    }

    #[test]
    fn committed_end_counts_frames() {
        let end = CommittedEnd::from_frame_count(4, size(4096));
        assert_eq!(end.frame_count(), 4);
        assert_eq!(end.offset().get(), 32 + 4 * 4120);
    }
}
