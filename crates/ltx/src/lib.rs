//! The LTX v3 file format as Litestream 0.5.x writes it.
//!
//! This crate is pure: no I/O beyond `std::io::Read` and `Write`, no clock, no
//! allocation that an input can grow without bound. It forbids `unsafe`.
//!
//! A file is a [`Header`], page frames (see [`FrameError`] for both page
//! encodings Go has written), an end-of-pages marker, a page index
//! ([`PageIndex`]) and a [`Trailer`]. [`Decoder`] and [`ReadDecoder`] read and
//! verify a whole file in order; [`PageIndex`] and [`decode_frame`] read one
//! page out of order; [`Encoder`] writes one.

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

pub use checksum::Checksum;
pub use decoder::{Decoder, Event, ReadDecoder, Verified};
pub use encoder::Encoder;
pub use error::{DecodeError, EncodeError, Section};
pub use frame::FrameError;
pub use header::{HEADER_LEN, Header, HeaderError, HeaderFields, WalOrigin};
pub use ids::{ParseTxidError, Timestamp, Txid, TxidRange};
pub use index::{INDEX_TAIL_LEN, IndexEntry, IndexError, PageIndex, decode_frame};
pub use order::OrderError;
pub use page::{PageNo, PageSize, lock_page};
pub use trailer::{TRAILER_LEN, Trailer, TrailerError};
