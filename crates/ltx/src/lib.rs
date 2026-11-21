//! The LTX v3 file format as Litestream 0.5.x writes it.
//!
//! This crate is pure: no I/O beyond `std::io::Read` and `Write`, no clock, no
//! allocation that an input can grow without bound. It forbids `unsafe`.
//!
//! A file is a [`Header`], page frames (see [`FrameError`] for both page
//! encodings Go has written), an end-of-pages marker, a page index and a
//! [`Trailer`]. [`Decoder`] and [`ReadDecoder`] read and verify a whole file in
//! order; [`Encoder`] writes one. With the `testing` feature, `PageIndex` and
//! `decode_frame` read one page out of order, for fuzzing: no checksum covers
//! a page read that way, so nothing else may.

#![forbid(unsafe_code)]

mod checksum;
mod conv;
mod decoder;
mod encoder;
mod error;
mod frame;
mod header;
mod ids;
mod index;
mod order;
mod page;
mod trailer;

pub use checksum::{DbChecksum, FileChecksum, PageChecksum};
pub use decoder::{Decoder, Event, ReadDecoder, Verified};
pub use encoder::Encoder;
/// Test support for crates that read LTX files; not part of the format.
// Compiled for this crate's own tests too, so `just mutants ltx` (which does not
// enable the feature) exercises it.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::encoder::encode_commit_zero;
}
pub use error::{DecodeError, EncodeError, Section};
pub use frame::FrameError;
pub use header::{HEADER_LEN, Header, HeaderError, HeaderFields, WalOrigin};
pub use ids::{ParseTxidError, TimeField, Timestamp, TimestampParseError, Txid, TxidRange};
pub use index::{INDEX_TAIL_LEN, IndexError};
#[cfg(any(test, feature = "testing"))]
pub use index::{IndexEntry, PageIndex, decode_frame};
pub use order::OrderError;
pub use page::{DbPages, PageNo, PageRef, PageSize, lock_page};
pub use trailer::{TRAILER_LEN, Trailer, TrailerError};
