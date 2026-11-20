//! Replica layout and the object-store contract.
//!
//! Sans-IO: this crate names no runtime, reads no clock and opens no file. The
//! backends (`lsr-store-fs` today) implement [`ObjectStore`]; [`MemStore`] is the
//! model they are tested against.

#![forbid(unsafe_code)]

#[cfg(any(test, feature = "testing"))]
pub mod conformance;
mod env;
mod inventory;
mod layout;
mod mem;
mod plan;
mod restore;
mod retry;
mod store;
mod verify;

pub use env::{Clock, Entropy, Sleeper};
#[cfg(any(test, feature = "testing"))]
pub use env::{FakeClock, RecordingSleeper, SeededEntropy};
pub use inventory::{Inventory, InventoryError, is_snapshot_start};
pub use layout::{
    FileInfo, FileKey, Level, ParseKeyError, RangeSide, cmp_range, file_name, is_scratch_name,
    parse_file_name,
};
pub use mem::{MemStore, MemUpload};
pub use plan::{ContiguousChain, PlanOutcome, RestoreTarget, plan};
#[cfg(any(test, feature = "testing"))]
pub use restore::{Database, MemSink};
pub use restore::{DatabaseSink, FileFault, MAX_ATTEMPTS, RestoreError, Restored, SinkOp, restore};
pub use retry::{Class, RetryPolicy, RetryStats, Retrying, RetryingUpload, classify};
pub use store::{
    CorruptCause, Listing, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp, Upload,
    build_listing,
};
pub use verify::{
    Depth, Finding, LevelSummary, MAX_FINDINGS, Report, Severity, VerifyError, verify,
};
