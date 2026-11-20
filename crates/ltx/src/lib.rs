//! The LTX v3 file format as Litestream 0.5.x writes it.
//!
//! This crate is pure: no I/O beyond `std::io::Read` and `Write`, no clock, no
//! allocation that an input can grow without bound. It forbids `unsafe`.

#![forbid(unsafe_code)]

mod ids;

pub use ids::{ParseTxidError, Timestamp, Txid, TxidRange};
