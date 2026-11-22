//! Replicate a live SQLite database in the application's process, Litestream-compatible.
//!
//! An application builds a store (`S3Store` over ureq or reqwest, `FsStore`, or its own), wraps it once
//! in an `Arc`, and calls [`Replicator::start`] once per database before its connection pool
//! opens. The replicator owns one OS thread and two SQLite connections, holds a read
//! transaction that pins the WAL from before the application's first write, and returns a handle.
//!
//! - Each connection the application opens goes through [`Replicator::app_connection`] (feature
//!   `rusqlite`) or [`app_connection_raw`], which sets `SQLITE_FCNTL_PERSIST_WAL` and
//!   `wal_autocheckpoint = 0` after checking it opens this database (PLAN section 6).
//! - Each long read the application starts holds a [`ReaderGuard`] from [`Replicator::long_reader`];
//!   under [`ReaderTracking::Tracked`] no TRUNCATE checkpoint runs while one lives.
//! - [`Replicator::health`] can be read at any time: one lock, no I/O.
//! - [`Replicator::stop`] runs at shutdown, after the application's writers close, and reports
//!   whether everything committed reached the replica ([`Stopped`]).
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
//! What a second Go process on the same meta directory or bucket prefix does is not refused
//! at start (Go leaves no reliable local sign, and `start` makes no store call); it is caught
//! when the replica shows files above the local newest staged file, which stops the replicator by default
//! (`Responses::GO_LIKE`, `other_writer: Stop`).
//!
//! # Example
//!
//! The whole life of a replicator, against a directory replica. An S3 replica differs only in
//! the store: `Arc::new(lsr_store_s3::S3Store::new(config, credentials)?)`. The README's
//! quick start is this example.
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use lsr_replica::{Config, DynStore, Replicator, RestoreTarget, Stopped, restore};
//! use lsr_store_fs::FsStore;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::tempdir()?;
//! # let db = dir.path().join("app.db");
//! # let replica = dir.path().join("replica");
//! // The database file exists before the replicator starts.
//! let setup = rusqlite::Connection::open(&db)?;
//! setup.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))?;
//! drop(setup);
//!
//! // One store, one replicator per database, started before the application's connections open.
//! let store: Arc<dyn DynStore> = Arc::new(FsStore::new(&replica));
//! let replicator = Replicator::start(&db, Arc::clone(&store), Config::default())?;
//!
//! // Every application connection: keep the WAL file, and leave checkpoints to the replicator.
//! let app = rusqlite::Connection::open(&db)?;
//! replicator.app_connection(&app)?;
//! app.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1);")?;
//!
//! // At shutdown, after the writers finish: upload everything committed, or say what is left.
//! let stopped = replicator.stop(Duration::from_secs(30)).wait();
//! assert!(matches!(stopped, Stopped::Drained { txid: Some(_) }));
//!
//! // A restore is a free function. It builds a new file and never touches an existing one.
//! let restored = dir.path().join("restored.db");
//! restore(&*store, RestoreTarget::Latest, &restored)?;
//! # let copy = rusqlite::Connection::open(&restored)?;
//! # assert_eq!(copy.query_row("SELECT v FROM t", [], |row| row.get::<_, i64>(0))?, 1);
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod config;
mod error;
mod events;
mod exec;
mod health;
mod readers;
mod replicator;
mod restore;
mod runtime;
mod scheduled;
pub mod stack;
mod store;
mod thread;

pub use config::{Config, NonZeroDuration, ReaderTracking, SyncLimits};
pub use error::{RestoreError, StartError};
pub use events::{ChannelSink, Event, EventReceiver, EventSink};
pub use exec::block_on;
pub use health::{
    AppNotice, Booted, Counters, Gone, Health, Mismatch, OpCounters, Phase, Stopped, StoreCounters,
    Verification, VerifyFailure, VerifyOutcome, VerifyRuns,
};
pub use readers::ReaderGuard;
pub use replicator::{FinalSync, Replicator};
pub use restore::{restore, restore_if_missing, restore_with, verify};
#[cfg(feature = "testing")]
pub use runtime::FakeClockHandle;
pub use runtime::{CALLER_SLICE, OsEntropy, Park, Runtime};
pub use store::{BoxFuture, DynStore, DynUpload};

pub use lsr_core::{
    AppCheckpoint, AppCheckpoints, AsOfInside, CheckpointConfig, Clock, Degraded, Depth, Entropy,
    L0Retention, Maintenance, OtherWriter, PersistentCause as DegradedCause, Report, Response,
    Responses, RestoreOptions, RestoreTarget, Restored, Severity, SnapshotCause, StopCause, Task,
};
pub use lsr_ltx::{Timestamp, Txid};
pub use lsr_sqlite::{DatabaseFile, SetupError, app_connection_raw, ffi};
