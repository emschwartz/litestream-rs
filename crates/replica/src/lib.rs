//! Replicate a live SQLite database in the application's process, Litestream-compatible.
//!
//! An application builds a store (`S3Store` over ureq or reqwest, `FsStore`, or its own), wraps it once
//! in an `Arc`, and calls [`Replicator::start`] once per database before its connection pool
//! opens. The replicator owns one OS thread and two SQLite connections, holds a read
//! transaction that pins the WAL from before the application's first write, and returns a handle.
//!
//! - Each connection the application opens goes through [`Replicator::app_connection`] (feature
//!   `rusqlite`) or [`app_connection_raw`], which sets `wal_autocheckpoint = 0` after checking
//!   it opens this database (PLAN section 6).
//! - Each long read the application starts holds a [`ReaderGuard`] from [`Replicator::long_reader`];
//!   under [`ReaderTracking::Tracked`] no TRUNCATE checkpoint runs while one lives.
//! - [`Replicator::health`] can be read at any time: one lock, no I/O.
//! - [`Replicator::stop`] runs at shutdown, after the application's connections close, and reports
//!   whether everything committed reached the replica ([`Stopped`]). The replicator's own
//!   connections keep the WAL when they close last, and the next start continues from it. An
//!   application connection that closes after them deletes the WAL, as SQLite does by default,
//!   and the next start takes a snapshot (`WalMissing`).
//!
//! Nothing runs on the application's threads, no async runtime is required, and time, waiting and
//! chance come in through [`Runtime`], so tests drive all of them ([`Runtime::fake`], feature
//! `testing`). Not covered by `Runtime`: an `S3Store` signs with its own clock, and HTTP request
//! deadlines run on real time inside the HTTP client.
//!
//! No panic crosses [`Replicator::start`], [`Replicator::stop`] or [`Replicator::health`]: the
//! thread catches its own (see the `thread` module), and a thread that ends without saying
//! why, or a process forked from the one that started it, reports [`Gone`].
//!
//! A scheduled verify (`Config::verify_interval`, off by default as in Go) restores the
//! transaction the replica holds and compares it with the live database at that transaction;
//! its store calls run on a thread of their own, and a mismatch is a degraded state in
//! [`Health::verification`], never a stop (see the `scheduled` module).
//!
//! [`restore`], [`verify`] and [`restore_if_missing`] are free functions over the same
//! `Arc<dyn DynStore>`, never methods on the replicator: restoring over a live database must
//! not look easy.
//!
//! One engine per meta directory: `start` takes an exclusive lock on `<meta>/lsr-lock` and
//! refuses with [`StartError::MetaLocked`] while another replicator, in this process or another
//! on the same host, holds it (the `meta` module says on which filesystems the lock excludes).
//! Go takes no lock, so a Go process on the same meta directory is not refused; nor is a
//! second writer on the bucket prefix (`start` makes no store call). Either is caught when the
//! replica shows files above everything this engine staged, which stops the replicator by
//! default (`Responses::GO_LIKE`, `other_writer: Stop`). A restart stops again while the
//! other writer's files are on the replica, since the boot refuses replica files nothing local
//! vouches for, and replicates once an operator has removed them or run `lsr take-over`
//! ([`lsr_core::TakeoverRecord`]).

#![forbid(unsafe_code)]

mod config;
mod error;
mod events;
mod exec;
mod health;
pub mod local;
mod meta;
mod readers;
mod replicator;
mod restore;
mod runtime;
mod scheduled;
pub mod stack;
mod store;
mod thread;
#[cfg(feature = "testing")]
pub mod waiting;

pub use config::{Config, NonZeroDuration, ReaderTracking, SyncLimits};
pub use error::{RestoreError, StartError, TakeOverError};
pub use events::{ChannelSink, Event, EventReceiver, EventSink, TakeoverUnusable};
pub use exec::block_on;
pub use health::{
    AheadOfWal, AheadOfWalUnchecked, AppNotice, Booted, Counters, FailStop, Gone, Health, Mismatch,
    OpCounters, Phase, Stopped, StoreCounters, Verification, VerifyFailure, VerifyOutcome,
    VerifyRuns,
};
pub use readers::ReaderGuard;
pub use replicator::{FinalSync, Replicator};
pub use restore::{record_takeover, restore, restore_if_missing, restore_with, take_over, verify};
#[cfg(feature = "testing")]
pub use runtime::FakeClockHandle;
pub use runtime::{CALLER_SLICE, OsEntropy, Park, Runtime};
pub use store::{BoxFuture, DynStore, DynUpload};

pub use lsr_core::{
    AppCheckpoint, AppCheckpoints, AsOfInside, CallTotals, CheckpointConfig, CheckpointMode, Clock,
    Degraded, Denied, Depth, Entropy, FinalRetention, FinalSnapshot, FinalSnapshotFailure, Gate,
    IfReplicationStops, JournalSignal, L0Retention, Maintenance, MaxUnreplicatedAge, OtherWriter,
    PersistentCause as DegradedCause, ReplicaId, Report, Response, Responses, RestoreOptions,
    RestoreTarget, Restored, Severity, SnapshotCause, SnapshotCauseKind, SnapshotCounts, Stall,
    StopCause, TakeoverRecord, Task, Trip, WorkCounts,
};
pub use lsr_ltx::{Timestamp, Txid};
pub use lsr_sqlite::{DatabaseFile, SetupError, app_connection_raw, ffi};
