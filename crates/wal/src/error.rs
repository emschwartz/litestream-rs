//! The error boundaries: opening, resuming, scanning, re-reading a page and mapping.

use std::io;

use thiserror::Error;

use lsr_ltx::{PageNo, PageSize};

use crate::frame::{FrameFault, WalOffset};
use crate::header::{HeaderError, Salts};

/// Why a WAL could not be opened.
#[derive(Debug, Error)]
pub enum OpenError {
    /// Reading the file failed.
    #[error("reading the WAL header: {0}")]
    Io(#[from] io::Error),
    /// The file is shorter than a header, as after a crash mid-restart.
    #[error("WAL is {found} bytes, shorter than its 32-byte header")]
    Truncated {
        /// How many bytes the file holds.
        found: usize,
    },
    /// The header is invalid.
    #[error("WAL header: {0}")]
    Header(#[from] HeaderError),
}

/// Why a position could not be resumed.
#[derive(Debug, Error)]
pub enum ResumeError {
    /// Reading the file failed.
    #[error("reading the frame before the resume offset: {0}")]
    Io(#[from] io::Error),
    /// The offset was made for another page size.
    #[error("offset is for page size {found:?}, the WAL's is {expected:?}")]
    PageSizeMismatch {
        /// The WAL's page size.
        expected: PageSize,
        /// The offset's page size.
        found: PageSize,
    },
    /// The WAL ends before the frame preceding the offset: it was truncated behind the caller.
    #[error("WAL ends before the frame preceding {at}")]
    Truncated {
        /// The offset asked for.
        at: WalOffset,
    },
    /// The frame preceding the offset belongs to another generation: the WAL was restarted behind the caller.
    #[error("frame before {at} has salts {found:?}, the WAL header's are {expected:?}")]
    PrevFrameMismatch {
        /// The offset asked for.
        at: WalOffset,
        /// The salts the preceding frame carries.
        found: Salts,
        /// The salts of the WAL header.
        expected: Salts,
    },
}

/// Why a scan could not produce an outcome.
#[derive(Debug, Error)]
pub enum WalError {
    /// Reading the file failed.
    #[error("reading a WAL frame: {0}")]
    Io(#[from] io::Error),
    /// The cursor or the committed end was made for another page size.
    #[error("cursor or end is for page size {found:?}, the WAL's is {expected:?}")]
    PageSizeMismatch {
        /// The WAL's page size.
        expected: PageSize,
        /// The offending value's page size.
        found: PageSize,
    },
    /// The cursor's generation is not the WAL's: the WAL was restarted behind the caller.
    #[error("cursor is in generation {cursor:?}, the WAL header's is {header:?}")]
    Restarted {
        /// The salts the cursor carries.
        cursor: Salts,
        /// The salts of the WAL header.
        header: Salts,
    },
    /// The cursor lies beyond the committed end: the WAL shrank behind the caller.
    #[error("cursor at {cursor} is past the committed end {end}")]
    CursorPastEnd {
        /// Where the scan was asked to start.
        cursor: WalOffset,
        /// The committed end.
        end: WalOffset,
    },
    /// A frame below the committed end failed verification: corruption, not the end of the WAL.
    #[error("frame at {offset} below the committed end is bad: {fault:?}")]
    CorruptBelowCommitted {
        /// Where the frame starts.
        offset: WalOffset,
        /// What is wrong with it.
        fault: FrameFault,
    },
    /// The file ends before the committed end does.
    #[error("WAL file ends inside the committed region, at {offset}")]
    TruncatedBelowCommitted {
        /// Where the missing or partial frame starts.
        offset: WalOffset,
    },
    /// The committed end falls inside a transaction.
    #[error("committed end {end} is not right after a commit frame")]
    EndNotAtCommit {
        /// The committed end.
        end: WalOffset,
    },
    /// The frame counter would pass `u32::MAX`.
    #[error("WAL has more frames than a 32-bit frame counter holds")]
    TooManyFrames,
}

/// Why a page could not be read back.
#[derive(Debug, Error)]
pub enum ReadPageError {
    /// Reading the file failed.
    #[error("reading a WAL page: {0}")]
    Io(#[from] io::Error),
    /// The buffer is not one page long.
    #[error("buffer is {found} bytes, the page size is {expected}")]
    BufferSize {
        /// The page size.
        expected: usize,
        /// The buffer length.
        found: usize,
    },
    /// The frame was found in a WAL of another page size.
    #[error("frame is for page size {found:?}, the WAL's is {expected:?}")]
    PageSizeMismatch {
        /// The WAL's page size.
        expected: PageSize,
        /// The frame's page size.
        found: PageSize,
    },
    /// The WAL ends before the page does.
    #[error("WAL ends before the page of {pgno} at {offset}")]
    Truncated {
        /// The page.
        pgno: PageNo,
        /// Where its frame starts.
        offset: WalOffset,
    },
    /// The page's bytes differ from what the scan verified: the WAL changed underneath.
    #[error("page {pgno} at {offset} changed since the scan verified it")]
    ChangedSinceScan {
        /// The page.
        pgno: PageNo,
        /// Where its frame starts.
        offset: WalOffset,
    },
}

/// Why a transaction could not be applied to a page map.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PageMapError {
    /// The transaction came from a WAL of another page size.
    #[error("transaction is for page size {found:?}, the map's is {expected:?}")]
    PageSizeMismatch {
        /// The map's page size.
        expected: PageSize,
        /// The transaction's page size.
        found: PageSize,
    },
    /// The transaction starts before the end of the ones already applied.
    #[error("transaction starts at {found}, before the map's end {end}")]
    NotAfterEnd {
        /// The end of the applied transactions.
        end: WalOffset,
        /// Where the offered transaction starts.
        found: WalOffset,
    },
}
