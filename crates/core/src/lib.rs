//! Replica layout and the object-store contract.
//!
//! Sans-IO: this crate names no runtime, reads no clock and opens no file. The
//! backends (`lsr-store-fs` today) implement [`ObjectStore`]; [`MemStore`] is the
//! model they are tested against.

#![forbid(unsafe_code)]

#[cfg(any(test, feature = "testing"))]
pub mod conformance;
mod layout;
mod mem;
mod store;

pub use layout::{
    FileInfo, FileKey, Level, ParseKeyError, RangeSide, cmp_range, file_name, parse_file_name,
};
pub use mem::{MemStore, MemUpload};
pub use store::{
    CorruptCause, Listing, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp, Upload,
    build_listing,
};
