//! Replicate a live SQLite database in the application's process, Litestream-compatible.
//!
//! An application builds a store (`S3Store` over ureq or reqwest, `FsStore`, or its own), wraps it once
//! in an `Arc`, and calls [`Replicator::start`] once per database before its connection pool
//! opens. The replicator owns one OS thread and three SQLite connections, holds a read
//! transaction that pins the WAL from before the application's first write, and returns a handle.
//!
//! - Writes are held until the first boot is judged and while fail-stop holds
//!   ([`Config::if_replication_stops`]): the replicator keeps the database's write lock, so every
//!   writer, in this process or another, gets SQLite's "database is locked"
//!   (`docs/decisions/D192.md`). The boot is judged once it has listed the replica and decided
//!   how to start; a first snapshot it asks for is taken after, while the application writes
//!   (`docs/decisions/D201.md`). A start with an empty meta directory first compares the
//!   database with the replica's newest state, which takes as long as a restore, and holds
//!   writes through it ([`WhyBlocked::CheckingIdentity`]). [`Replicator::ready`] (a future
//!   that needs no runtime) and [`Replicator::wait_booted`] say when writes go through; [`Health::writes`] and the
//!   `WritesBlocked` and `WritesAllowed` events say whether they are held, and why.
//! - Each connection the application opens may go through [`Replicator::app_connection`]
//!   (feature `rusqlite`) or [`app_connection_raw`]: optional, it checks the connection opens
//!   this database, sets `wal_autocheckpoint = 0` (PLAN section 6), and makes a blocked write
//!   fail at once instead of after the busy timeout.
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
//! # Memory
//!
//! The live heap of the library's own buffers peaks at the sum of the terms below. Each is
//! pinned by a gate that fails when the buffer grows (the tests are named). The configuration
//! terms are: part size `P` (`S3Config::with_part_size`, 5 MiB by default and 16 MiB on
//! Tigris), upload concurrency `C` (`S3Config::with_concurrency`, 5),
//! the compaction fan-in `F` (`MAX_INPUTS`, 32), and the restore read concurrency `R`
//! (`READ_CONCURRENCY`, 8). Larger parts or more concurrency raise `C x P`, and nothing else.
//!
//! - **Upload:** `C x P` plus 4 MiB, the parts of the batch being sent. A part body is sent by
//!   reference, so nothing holds it twice: 26 MiB at the defaults, at most 84 MiB on Tigris
//!   (`crates/store-s3/tests/store-s3/memory.rs`, `an_upload_through_the_real_client_holds_five_parts`
//!   and `the_bound_follows_part_size_and_concurrency`).
//! - **Compaction:** `F x (window + page)` plus a 1 MiB output buffer. A level-1 compaction reads
//!   each staged file in a 128 KiB window: 5 MiB at `F` = 32 (`crates/store-fs/tests/store-fs/memory.rs`,
//!   `a_level_one_compaction_holds_a_staging_window_per_input`). A compaction above level 1 reads
//!   the replica a mebibyte at a time: 33 MiB (`a_compaction_holds_a_chunk_per_replica_input`).
//! - **Replicating:** the compaction and the upload of its output at once, the busiest moment:
//!   about 30 MiB with the defaults (`a_compaction_into_s3_holds_its_inputs_and_one_batch_of_parts`).
//!   A compaction above level 1 holds its 33 MiB of windows instead of 5, to about 60 MiB (the
//!   sum of the two gates above).
//! - **Restore:** `R x 4 MiB` plus 3 MiB: 35 MiB, whatever the database's size or the chain's
//!   length (`a_restore_through_the_real_client_holds_one_read_window`; the same law at one
//!   to four GiB in `crates/core/tests/core/mem_big.rs`).
//!
//! Not in these bounds: the page checksum table (about 20 bytes a page, 5 MiB at 1 GiB; issue
//! #64), SQLite's own page cache, the `reqwest` transport (the gates run the `ureq` one that
//! `S3Store::new` builds), and what the process's allocator keeps after a free: a resident
//! size can sit well above the live heap (about three times on macOS, where freed large blocks
//! stay mapped), which no gate here sees.
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
//! vouches for. It replicates once an operator has removed them, has run `lsr trust-local`
//! because this database is the right one ([`trust_local`], which writes a
//! [`lsr_core::TakeoverRecord`]), or has restored from the replica because the replica is
//! ([`restore`], `lsr restore`).

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
pub use error::{RestoreError, StartError, TrustLocalError};
pub use events::{ChannelSink, Event, EventReceiver, EventSink, TakeoverUnusable};
pub use exec::block_on;
pub use health::{
    AppNotice, Booted, Counters, FailStop, Gone, Health, Mismatch, OpCounters, Phase, Stopped,
    StoreCounters, Verification, VerifyFailure, VerifyOutcome, VerifyRuns, Writes,
};
pub use readers::ReaderGuard;
pub use replicator::{FinalSync, Ready, Replicator};
pub use restore::{
    record_takeover, restore, restore_if_missing, restore_with, trust_local, verify,
};
#[cfg(feature = "testing")]
pub use runtime::FakeClockHandle;
pub use runtime::{CALLER_SLICE, OsEntropy, Park, Runtime};
pub use store::{BoxFuture, DynStore, DynUpload};

pub use lsr_core::{
    AppCheckpoint, AppCheckpoints, AsOfInside, CallTotals, CheckpointConfig, CheckpointMode, Clock,
    Degraded, Denied, Depth, Entropy, FinalRetention, FinalSnapshot, FinalSnapshotFailure, Gate,
    IfReplicationStops, L0Retention, Maintenance, MaxUnreplicatedAge, OtherWriter,
    PersistentCause as DegradedCause, Report, Response, Responses, RestoreOptions, RestoreTarget,
    Restored, Severity, SnapshotCause, SnapshotCauseKind, SnapshotCounts, Stall, StopCause,
    TakeoverRecord, Task, Trip, WhyBlocked, WorkCounts,
};
pub use lsr_ltx::{Timestamp, Txid};
pub use lsr_sqlite::{DatabaseFile, SetupError, WritesHeld, app_connection_raw, ffi};
