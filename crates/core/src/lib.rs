//! Replica layout and the object-store contract.
//!
//! Sans-IO: this crate names no runtime, reads no clock and opens no file. The
//! backends (`lsr-store-fs` today) implement [`ObjectStore`]; [`MemStore`] is the
//! model they are tested against.

#![forbid(unsafe_code)]

mod compact;
#[cfg(any(test, feature = "testing"))]
pub mod conformance;
mod env;
mod fault;
mod inventory;
mod layout;
mod maintain;
mod mem;
mod plan;
mod position;
mod restore;
mod retry;
mod source;
mod store;
mod verify;

pub use compact::{
    CompactError, Compacted, CompactionInputs, InputFault, InputsError, MAX_INPUTS, compact,
};
pub use env::{Clock, Entropy, Sleeper};
#[cfg(any(test, feature = "testing"))]
pub use env::{FakeClock, RecordingSleeper, SeededEntropy};
pub use fault::{
    Backoff, Clamped, Fault, HaltCause, MAX_BACKOFF, Next, Policy, SnapshotCause, policy,
    retry_wait,
};
pub use inventory::{Inventory, InventoryError, is_snapshot_start};
pub use layout::{
    FileInfo, FileKey, Level, ParseKeyError, RangeSide, cmp_range, file_name, is_scratch_name,
    parse_file_name,
};
pub use maintain::{
    CompactJob, ConfigError, DeleteJob, Due, Interval, Job, JobError, L0Retention, MAX_DELETE,
    MaintainError, Maintenance, Outcome, Setting, SnapshotJob, due, go_truncate, run_compact,
    run_delete,
};
pub use mem::{MemStore, MemUpload};
pub use plan::{ContiguousChain, PlanOutcome, RestoreTarget, plan};
pub use position::{
    Continuity, LastPage, Position, ReadEpoch, SyncedOffset, SyncedThrough, WalFileLen,
};
#[cfg(any(test, feature = "testing"))]
pub use restore::{Database, MemSink};
pub use restore::{
    DatabaseSink, FileFault, MAX_ATTEMPTS, NoPlan, RestoreError, Restored, SinkOp, restore,
};
pub use retry::{Class, RetryPolicy, RetryStats, Retrying, RetryingUpload, classify};
pub use source::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, SourceError,
    WalSource, WriteLock,
};
pub use store::{
    CorruptCause, Listing, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp, Upload,
    build_listing,
};
pub use verify::{
    Depth, Finding, LevelSummary, MAX_FINDINGS, Report, Severity, VerifyError, verify,
};
