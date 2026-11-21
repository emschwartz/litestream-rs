//! The two error boundaries: reading a file and writing one.

use std::io;

use thiserror::Error;

use crate::{
    DbChecksum, FileChecksum, FrameError, HeaderError, IndexError, OrderError, TrailerError,
};

/// The part of a file the input ended in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Section {
    /// The 100-byte header.
    Header,
    /// A page frame or the end-of-pages marker.
    Page,
    /// The page index.
    Index,
    /// The 16-byte trailer.
    Trailer,
}

/// Why an LTX file failed to decode.
#[derive(Debug, Error)]
pub enum DecodeError {
    /// The header is invalid.
    #[error("header: {0}")]
    Header(#[from] HeaderError),
    /// A page frame is malformed.
    #[error("page frame: {0}")]
    Frame(#[from] FrameError),
    /// A page number is out of place.
    #[error("page order: {0}")]
    Order(#[from] OrderError),
    /// The page index is malformed.
    #[error("page index: {0}")]
    Index(#[from] IndexError),
    /// The page index does not list exactly the frames the file holds.
    #[error("page index does not match the page frames")]
    IndexMismatch,
    /// The trailer is invalid.
    #[error("trailer: {0}")]
    Trailer(#[from] TrailerError),
    /// A frame holds another page than the index entry that located it.
    #[error("frame holds page {found}, the index entry is for page {expected}")]
    EntryMismatch {
        /// The entry's page number.
        expected: u32,
        /// The frame's page number.
        found: u32,
    },
    /// The file's contents do not match its file checksum.
    #[error("file checksum mismatch: computed {computed}, stored {stored}")]
    FileChecksum {
        /// The checksum over the bytes read.
        computed: FileChecksum,
        /// The trailer's checksum.
        stored: FileChecksum,
    },
    /// A tracked snapshot's pages do not match its post-apply checksum.
    #[error("snapshot post-apply checksum mismatch: computed {computed}, stored {stored:?}")]
    PostApplyMismatch {
        /// The checksum over the snapshot's pages.
        computed: DbChecksum,
        /// The trailer's post-apply checksum.
        stored: Option<DbChecksum>,
    },
    /// The input ended before the file did.
    #[error("file truncated in the {section:?} section")]
    Truncated {
        /// Where the input ended.
        section: Section,
    },
    /// Bytes follow the trailer (or a lone frame).
    #[error("bytes after the end of the file")]
    TrailingBytes,
    /// A call after the decoder already failed.
    #[error("decoder already failed")]
    Poisoned,
    /// Reading the input failed.
    #[error("reading LTX input: {0}")]
    Io(#[from] io::Error),
}

/// Why an LTX file could not be written.
#[derive(Debug, Error)]
pub enum EncodeError {
    /// A page is not exactly one page long.
    #[error("page is {found} bytes, expected {expected}")]
    PageLen {
        /// Bytes passed.
        found: usize,
        /// The header's page size.
        expected: usize,
    },
    /// The header has commit 0: SQLite never commits an empty database in WAL
    /// mode, so no file of one is worth writing.
    #[error("header commit is 0, a database of no pages")]
    ZeroCommit,
    /// A page number is out of place, or a snapshot is incomplete.
    #[error("page order: {0}")]
    Order(#[from] OrderError),
    /// The post-apply checksum does not fit the header.
    #[error("trailer: {0}")]
    Trailer(#[from] TrailerError),
    /// A tracked snapshot's post-apply checksum is not its pages' checksum.
    #[error("snapshot post-apply checksum {given} does not match its pages, {computed}")]
    PostApplyMismatch {
        /// The checksum over the pages written.
        computed: DbChecksum,
        /// The checksum the caller passed.
        given: DbChecksum,
    },
    /// A call after a write already failed, leaving partial output.
    #[error("encoder already failed")]
    Poisoned,
    /// Writing the output failed.
    #[error("writing LTX output: {0}")]
    Io(#[from] io::Error),
}
