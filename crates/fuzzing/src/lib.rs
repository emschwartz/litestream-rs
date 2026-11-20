//! Fuzz targets as plain functions over bytes.
//!
//! Each target takes the raw bytes libFuzzer hands over, builds typed inputs
//! from them, calls the code under test and checks the contract the code
//! promises. A violated contract panics (see [`violation`]), which is how both
//! libFuzzer and the test runner see it. The `fuzz/` crate wraps each function
//! in one line; `tests/corpus.rs` replays every committed corpus file through
//! it, so the corpus runs in `just check` on the stable toolchain without
//! libFuzzer.
//!
//! The input of each target is a small binary format of its own, documented
//! on the function. Raw files of the format under test are turned into seeds by
//! the ignored test `tests/seed.rs` (`just fuzz-seed`).

#![forbid(unsafe_code)]

mod bytes;
mod decode;
mod index;
mod key;
mod plan;

pub use bytes::violation;
pub use decode::decode;
pub use index::{HEADER_SPEC_LEN, header_spec, index};
pub use key::key;
pub use plan::{PLAN_RECORD_LEN, plan, plan_input};
