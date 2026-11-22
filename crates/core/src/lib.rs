//! The replication engine and everything it decides, with no I/O of its own.
//!
//! Sans-IO: this crate names no runtime, spawns no thread, reads no clock and
//! opens no file. What it needs comes in through traits: the replica and the
//! local staging directory are both an [`ObjectStore`] (`lsr-store-s3`,
//! `lsr-store-fs`; `MemStore`, behind the `testing` feature, is the model they are tested against), SQLite
//! is a [`WalSource`], a [`PageSource`] and a [`Checkpointer`] (`lsr-sqlite`),
//! a restore writes into a [`DatabaseSink`] (`lsr-local`), and time, waiting
//! and chance are [`Clock`], [`Sleeper`] and [`Entropy`].
//!
//! What is here:
//! - the replica's layout and the store contract (`layout`, `store`,
//!   `inventory`, `resolved`);
//! - reading a replica: the restore planner and executor and the verifier
//!   (`plan`, `restore`, `verify`);
//! - writing one: position recovery, the sync step, the checkpoint policy
//!   and protocol, uploads, compaction, snapshots and retention (`recover`,
//!   `sync`, `checkpoint`, `upload`, `compact`, `snapshot`, `maintain`);
//! - the [`Actor`] that runs those in one priority order, one unit of work a
//!   tick, and the closed set of faults with a policy each (`actor`, `fault`).

#![forbid(unsafe_code)]

// First, so `mark!` is in scope in every module after it.
#[macro_use]
pub mod marks;
mod actor;
mod app;
mod checked;
mod checkpoint;
mod compact;
#[cfg(any(test, feature = "testing"))]
pub mod conformance;
mod created_at;
mod env;
mod failstop;
mod fault;
mod inventory;
mod journal;
mod layout;
mod ltx_read;
mod maintain;
#[cfg(any(test, feature = "testing"))]
mod mem;
#[cfg(any(test, feature = "testing"))]
pub mod needs;
mod pages;
mod plan;
mod position;
mod probe;
mod recover;
mod regression_watch;
mod restore;
mod retry;
mod snapshot;
mod source;
#[cfg(feature = "testing")]
pub mod spec_view;
mod store;
#[cfg(test)]
mod stuck;
mod sync;
mod takeover;
mod upload;
mod verify;
mod witness;

pub use actor::{
    Actor, ActorConfig, Degraded, Env, Event, MaintainedEvent, Phase, Task, Tick, Unfinished,
};
pub use app::{
    AppCheckpoint, AppCheckpointObserver, AppCheckpoints, SQLITE_AUTOCHECKPOINT, Watch, observe,
};
pub use checkpoint::{
    CheckpointConfig, CheckpointFailed, CheckpointOutcome, CheckpointState, CheckpointTrigger,
    Checkpointed, Decision, Observed, Staged, checkpoint, decide,
};
pub use compact::{
    CompactError, Compacted, CompactionInputs, InputFault, InputsError, MAX_INPUTS, compact,
};
pub use created_at::{
    MAX_AHEAD, Need, Resolve, ResolveError, Resolved, ResolvedCreatedAt, Step as CreatedAtStep,
    resolve, resolve_counted,
};
pub use env::{Clock, Entropy, Sleeper};
#[cfg(any(test, feature = "testing"))]
pub use env::{FakeClock, RecordingSleeper, SeededEntropy};
pub use failstop::{AppGate, Gate, IfReplicationStops, Lag, MaxUnreplicatedAge, Trip, gate, trip};
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub use fault::reporting_bugs;
pub use fault::{
    Backoff, BugSite, Clamped, Fault, MAX_BACKOFF, Next, OtherWriter, PersistentCause, Policy,
    Response, Responses, STALLED_AFTER, SnapshotCause, SnapshotCauseKind, SnapshotCounts, Stall,
    StopCause, VERIFY, backoff_wait, policy, retry_wait,
};
pub use inventory::{Inventory, InventoryError, is_snapshot_start};
pub use journal::{JournalFile, JournalSignal, JournalWriteError, JournalWriter, judge};
pub use layout::{
    CreatedAt, CreatedAtSource, Dating, FileInfo, FileKey, Level, ListedTime, NonZeroCreatedAt,
    ParseKeyError, RangeSide, Stamp, cmp_range, file_name, is_scratch_name, parse_file_name,
};
pub use maintain::{
    CompactJob, ConfigError, DeleteJob, Due, Interval, IntervalError, Job, JobError, L0Retention,
    MAX_DELETE, MaintainError, MaintainState, Maintenance, Outcome, RetentionHold, RetentionMode,
    Setting, SnapshotJob, due, run_delete,
};
#[cfg(any(test, feature = "testing"))]
pub use mem::{MemStore, MemUpload};
pub use pages::{Mismatch, PageChecksumTable};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use plan::plan_reference;
pub use plan::{AsOfPlanner, ContiguousChain, KeyTarget, PlanOutcome, RestoreTarget, plan};
pub use position::{
    Continuity, LastPage, Position, ReachedEnd, ReadLockHold, SyncedOffset, SyncedThrough,
    WalFileLen,
};
pub use probe::Starts;
pub use recover::{
    BootWal, FrameCheck, LastPageCheck, Miss, NewestStagedFile, PositionSource, Probes,
    RecoverEvent, Recovered, Verdict, WalView, recover, recover_traced,
};
pub use restore::{
    AsOfInside, DatabaseSink, FileFault, MAX_ATTEMPTS, NoPlan, PublishFailure, PublishLeft,
    RestoreError, RestoreOptions, Restored, SinkOp, listed_past, restore, restore_with,
};
#[cfg(any(test, feature = "testing"))]
pub use restore::{Database, MemPublishError, MemSink};
pub use retry::{Class, RetryPolicy, RetryStats, Retrying, RetryingUpload, classify};
pub use snapshot::SnapshotFailed;
pub use source::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, SourceError,
    WalSource, WriteLock,
};
#[cfg(any(test, feature = "testing"))]
pub use source::{
    CheckpointCall, CheckpointEffect, CheckpointScript, FakeCheckpointer, FakeDb, FakeWal, SourceOp,
};
pub use store::{
    BatchKind, CorruptCause, Denied, ListAt, Listing, ListingStalled, ObjectStore, PageLimit,
    PartNumber, ReadAt, ReadLen, StoreError, StoreOp, Upload, WriteMode, build_listing,
};
pub use sync::{PageFrom, Step, StepEnd, StepFailed, SyncConfig, Synced, clamp, sync_step};
pub use takeover::{TakeoverRecord, TakeoverRecordError};
pub use upload::{
    Did, PROBE_CAP, PROBE_GAP, UploadConfig, UploadFailed, UploadPlan, UploadState, Uploaded,
    fresh_probe_reads, upload_next,
};
pub use verify::{
    Compared, Depth, Finding, LevelSummary, LivePages, MAX_FINDINGS, Report, Severity, Verified,
    VerifyError, verify, verify_against,
};
pub use witness::{Witnessed, Witnesses};
