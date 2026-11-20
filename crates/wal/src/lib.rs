//! The SQLite WAL as a source of committed transactions.
//!
//! This crate is pure: no I/O beyond `std::io::{Read, Seek}`, no clock, and it
//! forbids `unsafe`. The format is SQLite's own (fileformat2 "WAL format"; the
//! authority for details is `walDecodeFrame` and `walIndexRecover`).
//!
//! A [`WalReader`] verifies the header, then walks frames from a
//! [`FrameCursor`]. A frame is valid when its salts equal the header's and its
//! cumulative checksum matches; a transaction is the valid frames up to a frame
//! with a non-zero database size ([`CommittedTx`]). Pages are never kept by a
//! scan, only their checksum ([`FrameRef`]); [`WalReader::read_page`] reads a
//! page again and refuses it if it changed, so an LTX file only ever gets bytes
//! that were verified on the read that produced them.
//!
//! Two limits ([`ScanLimit`]) decide what a bad frame means. With a
//! [`CommittedEnd`] taken from the wal-index, everything below it is committed,
//! so a bad frame there is an error ([`WalError::CorruptBelowCommitted`]) and
//! the reader never touches a byte at or past the end. Without one, the first
//! invalid frame is the end of the WAL ([`Tail`]), as in SQLite's own recovery.
//! Go's reader treats both as end of file; this one must not.
//!
//! [`PageMap`] folds transactions into the newest frame of each page.

#![forbid(unsafe_code)]

mod checksum;
mod conv;
mod error;
mod frame;
mod header;
mod pagemap;
mod scan;

pub use checksum::{ByteOrder, WalChecksum};
pub use error::{OpenError, PageMapError, ReadPageError, ResumeError, WalError};
pub use frame::{CommittedEnd, FRAME_HEADER_LEN, FrameFault, FrameRef, WalOffset};
pub use header::{HEADER_LEN, HeaderError, Salts, WalHeader};
pub use pagemap::PageMap;
pub use scan::{CommittedTx, FrameCursor, ScanLimit, Tail, TxOutcome, WalReader};
