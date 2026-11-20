//! Replica layout and the object-store contract.
//!
//! Sans-IO: this crate names no runtime, reads no clock and opens no file. The
//! backends (`lsr-store-fs` today) implement [`ObjectStore`]; [`MemStore`] is the
//! model they are tested against.

#![forbid(unsafe_code)]

mod actor;
mod checkpoint;
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
mod recover;
mod restore;
mod retry;
mod snapshot;
mod source;
mod stamps;
mod store;
mod sync;
mod upload;
mod verify;

pub use actor::{Actor, ActorConfig, Env, Event, MaintainedEvent, Phase, Tick};
pub use checkpoint::{
    CheckpointConfig, CheckpointFailed, CheckpointOutcome, CheckpointState, Checkpointed, Decision,
    Observed, Why, after_checkpoint, after_sync, checkpoint, decide, next_wake,
};
pub use compact::{
    CompactError, Compacted, CompactionInputs, InputFault, InputsError, MAX_INPUTS, compact,
};
pub use env::{Clock, Entropy, Sleeper};
#[cfg(any(test, feature = "testing"))]
pub use env::{FakeClock, RecordingSleeper, SeededEntropy};
pub use fault::{
    Backoff, Clamped, Fault, HaltCause, MAX_BACKOFF, Next, Policy, SnapshotCause, backoff_wait,
    policy, retry_wait,
};
pub use inventory::{Inventory, InventoryError, is_snapshot_start};
pub use layout::{
    FileInfo, FileKey, Level, ListedStamp, ParseKeyError, RangeSide, StampOrigin, StoredStamp,
    cmp_range, file_name, is_scratch_name, parse_file_name,
};
pub use maintain::{
    CompactJob, ConfigError, DeleteJob, Due, Hold, Interval, IntervalError, Job, JobError,
    L0Retention, MAX_DELETE, MaintainError, MaintainState, Maintenance, Outcome, Setting,
    SnapshotJob, due, go_truncate, run_compact, run_delete,
};
pub use mem::{MemStore, MemUpload};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use plan::plan_reference;
pub use plan::{AsOfPlanner, ContiguousChain, KeyTarget, PlanOutcome, RestoreTarget, plan};
pub use position::{
    Continuity, LastPage, Position, ReadEpoch, SyncedOffset, SyncedThrough, WalFileLen,
};
pub use recover::{
    FrameCheck, LastPageCheck, Miss, Probes, RecoverEvent, Recovered, Tip, TipView, Verdict,
    WalView, recover, recover_traced,
};
#[cfg(any(test, feature = "testing"))]
pub use restore::{Database, MemSink};
pub use restore::{
    DatabaseSink, FileFault, MAX_ATTEMPTS, NoPlan, RestoreError, Restored, SinkOp, restore,
};
pub use retry::{Class, RetryPolicy, RetryStats, Retrying, RetryingUpload, classify};
pub use snapshot::{SnapshotFailed, write_snapshot};
pub use source::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, SourceError,
    WalSource, WriteLock,
};
#[cfg(any(test, feature = "testing"))]
pub use source::{
    CheckpointCall, CheckpointEffect, CheckpointScript, FakeCheckpointer, FakeDb, FakeWal, SourceOp,
};
pub use stamps::{Resolve, ResolveError, Resolved, Stamps, Step as StampStep, resolve};
pub use store::{
    CorruptCause, Listing, ObjectStore, PageLimit, PartNumber, ReadLen, StoreError, StoreOp,
    Upload, build_listing,
};
pub use sync::{
    PageFrom, Step, StepEnd, StepFailed, SyncConfig, Synced, clamp, header_fields, plan_pages,
    sync_step,
};
pub use upload::{
    Did, Ticked, UploadConfig, UploadFailed, UploadPlan, UploadState, Uploaded, plan_upload,
    upload_next, upload_tick,
};
pub use verify::{
    Depth, Finding, LevelSummary, MAX_FINDINGS, Report, Severity, VerifyError, verify,
};
