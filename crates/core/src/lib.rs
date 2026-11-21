//! The replication engine and everything it decides, with no I/O of its own.
//!
//! Sans-IO: this crate names no runtime, spawns no thread, reads no clock and
//! opens no file. What it needs comes in through traits: the replica and the
//! local staging directory are both an [`ObjectStore`] (`lsr-store-s3`,
//! `lsr-store-fs`; [`MemStore`] is the model they are tested against), SQLite
//! is a [`WalSource`], a [`PageSource`] and a [`Checkpointer`] (`lsr-sqlite`),
//! a restore writes into a [`DatabaseSink`] (`lsr-local`), and time, waiting
//! and chance are [`Clock`], [`Sleeper`] and [`Entropy`].
//!
//! What is here:
//! - the replica's layout and the store contract (`layout`, `store`,
//!   `inventory`, `stamps`);
//! - reading a replica: the restore planner and executor and the verifier
//!   (`plan`, `restore`, `verify`);
//! - writing one: position recovery, the sync step, the checkpoint policy
//!   and protocol, uploads, compaction, snapshots and retention (`recover`,
//!   `sync`, `checkpoint`, `upload`, `compact`, `snapshot`, `maintain`);
//! - the [`Actor`] that runs those in one priority order, one unit of work a
//!   tick, and the closed set of faults with a policy each (`actor`, `fault`).

#![forbid(unsafe_code)]

mod actor;
mod checkpoint;
mod compact;
#[cfg(any(test, feature = "testing"))]
pub mod conformance;
mod env;
mod fault;
mod host;
mod inventory;
mod layout;
mod ltx_read;
mod maintain;
mod mem;
mod pages;
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

pub use actor::{
    Actor, ActorConfig, Degraded, Env, Event, Lane, MaintainedEvent, Phase, Tick, Unfinished,
};
pub use checkpoint::{
    CheckpointConfig, CheckpointFailed, CheckpointOutcome, CheckpointState, Checkpointed, Decision,
    Observed, Staged, Why, after_checkpoint, after_sync, checkpoint, decide, next_wake,
};
pub use compact::{
    CompactError, Compacted, CompactionInputs, InputFault, InputsError, MAX_INPUTS, compact,
};
pub use env::{Clock, Entropy, Sleeper};
#[cfg(any(test, feature = "testing"))]
pub use env::{FakeClock, RecordingSleeper, SeededEntropy};
pub use fault::{
    Backoff, BugSite, Clamped, Fault, ForeignWriter, HaltCause, MAX_BACKOFF, Next, PersistentCause,
    Policy, Response, Responses, SnapshotCause, backoff_wait, policy, retry_wait,
};
pub use host::{
    HostCheckpoint, HostCheckpoints, HostObserver, SQLITE_AUTOCHECKPOINT, Watch, observe,
};
pub use inventory::{Inventory, InventoryError, is_snapshot_start};
pub use layout::{
    FileInfo, FileKey, Level, ListedStamp, ParseKeyError, RangeSide, RealStamp, StampOrigin,
    StoredStamp, cmp_range, file_name, is_scratch_name, parse_file_name,
};
pub use maintain::{
    CompactJob, ConfigError, DeleteJob, Due, Hold, Interval, IntervalError, Job, JobError,
    L0Retention, MAX_DELETE, MaintainError, MaintainState, Maintenance, Outcome, RetentionMode,
    Setting, SnapshotJob, due, go_truncate, listing_after_upload, run_compact, run_delete,
};
pub use mem::{MemStore, MemUpload};
pub use pages::{Mismatch, PageTable, snapshot_clears};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use plan::plan_reference;
pub use plan::{AsOfPlanner, ContiguousChain, KeyTarget, PlanOutcome, RestoreTarget, plan};
pub use position::{
    Continuity, LastPage, Position, ReachedEnd, ReadEpoch, SyncedOffset, SyncedThrough, WalFileLen,
};
pub use recover::{
    FrameCheck, LastPageCheck, Miss, Probes, RecoverEvent, Recovered, Tip, TipView, Verdict,
    WalView, recover, recover_traced,
};
pub use restore::{
    AsOfInside, DatabaseSink, FileFault, MAX_ATTEMPTS, NoPlan, PublishFailure, PublishLeft,
    RestoreError, RestoreOptions, Restored, SinkOp, restore, restore_with,
};
#[cfg(any(test, feature = "testing"))]
pub use restore::{Database, MemPublishError, MemSink};
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
pub use stamps::{
    MAX_AHEAD, Need, Resolve, ResolveError, Resolved, Stamps, Step as StampStep, resolve,
    resolve_counted,
};
pub use store::{
    CorruptCause, ListAt, Listing, ObjectStore, PageLimit, PartNumber, ReadAt, ReadLen, StoreError,
    StoreOp, Upload, build_listing,
};
pub use sync::{
    PageFrom, Step, StepEnd, StepFailed, SyncConfig, Synced, clamp, header_fields, plan_pages,
    sync_step,
};
pub use upload::{
    Did, UploadConfig, UploadFailed, UploadPlan, UploadState, Uploaded, plan_upload, upload_next,
};
pub use verify::{
    Compared, Depth, Finding, LevelSummary, LivePages, MAX_FINDINGS, Report, Severity, Verified,
    VerifyError, verify, verify_against,
};
