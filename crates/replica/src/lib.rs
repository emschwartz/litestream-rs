//! Replicate a live SQLite database in the application's process, Litestream-compatible.
//!
//! An application builds a store (`S3Store` over ureq or reqwest, `FsStore`, or its own), wraps it once
//! in an `Arc`, and starts one [`Replicator`] per database ([`Replicator::builder`]) before its connection pool
//! opens. The replicator owns one OS thread and three SQLite connections, holds a read
//! transaction that pins the WAL from before the application's first write, and returns a handle.
//!
//! An `S3Store`'s TLS provider is the one the application installed as rustls's process default,
//! else the one provider rustls's features enable in the application's build; `lsr-store-s3`
//! enables none. When rustls cannot pick, building the store is `ConfigError::NoTlsProvider`.
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
//! - Each connection the application opens may go through `Replicator::app_connection`
//!   (feature `rusqlite`, any rusqlite from 0.39 to 0.43) or [`app_connection_raw`] (any
//!   binding's raw handle: sqlx, diesel, its own FFI): optional, it checks the connection opens
//!   this database, sets `wal_autocheckpoint = 0` (PLAN section 6), and makes a blocked write
//!   fail at once instead of after the busy timeout.
//! - The application and the replicator share one SQLite: `libsqlite3-sys` 0.37 to 0.40 (SQLite
//!   3.51.3 to 3.54.1), so an application's rusqlite, sqlx or other binding in that range builds
//!   beside this crate. The default `bundled` feature compiles the SQLite `libsqlite3-sys`
//!   bundles; with default features off the application links its own, which must be one of the
//!   builds `lsr-sqlite` lists as verified, or [`ReplicatorBuilder::start`] refuses it
//!   ([`StartError::SqliteBuild`], `docs/decisions/D200.md`).
//! - Each long read the application starts holds a [`ReaderGuard`] from [`Replicator::long_reader`];
//!   under [`ReaderTracking::Tracked`] no TRUNCATE checkpoint runs while one lives.
//! - [`Replicator::health`] can be read at any time: one lock, no I/O.
//! - [`Replicator::stop`] runs at shutdown, after the application's connections close, and reports
//!   whether everything committed reached the replica ([`StopOutcome`]). The replicator's own
//!   connections keep the WAL when they close last, and the next start continues from it. An
//!   application connection that closes after them deletes the WAL, as SQLite does by default,
//!   and the next start takes a snapshot (`WalMissing`).
//!
//! Nothing runs on the application's threads, no async runtime is required, and time, waiting and
//! chance come in through [`Runtime`], so tests drive all of them ([`Runtime::fake`], feature
//! `testing`). Not covered by `Runtime`: an `S3Store` signs with its own clock, and HTTP request
//! deadlines run on real time inside the HTTP client.
//!
//! No panic crosses [`ReplicatorBuilder::start`], [`Replicator::stop`] or [`Replicator::health`]: the
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
//! Tigris), upload concurrency `C` (`S3Config::with_concurrency`, 5), and the compaction fan-in
//! `F` (`MAX_INPUTS`, 32).
//! Larger parts or more concurrency raise `C x P`, and nothing else.
//!
//! - **Upload:** `C x P` plus 4 MiB, the parts of the batch being sent. For a file over 10000
//!   parts of `P` (48.8 GiB at 5 MiB, 977 GiB at 100 MB) the part is the file's length over
//!   10000 instead, 524 MB at S3's 5 TiB ceiling (`docs/decisions/D208.md`). A part body is sent by
//!   reference, so nothing holds it twice: 26 MiB at the defaults, at most 84 MiB on Tigris
//!   (`crates/store-s3/tests/store-s3/memory.rs`, `an_upload_through_the_real_client_holds_five_parts`
//!   and `the_bound_follows_part_size_and_concurrency`). A part is the one unit a request
//!   must hold whole (its `Content-MD5` is signed before the body goes out), so this term
//!   grows with the part size the operator sets; see `docs/decisions/D212.md`.
//! - **Compaction:** the inputs' windows plus a 256 KiB output buffer that is kept for the
//!   file. A level-1 compaction reads each staged file in a 128 KiB window: 5 MiB at `F` = 32
//!   (`crates/store-fs/tests/store-fs/memory.rs`, `a_level_one_compaction_holds_a_staging_window_per_input`).
//!   A compaction above level 1 reads the replica with a 16 MiB budget shared by its inputs,
//!   at most a mebibyte each: 17 MiB at `F` = 32, and a merge of 16 or fewer inputs still reads
//!   a mebibyte at a time (`a_compaction_holds_its_share_of_the_budget_per_replica_input`).
//!   The output buffer is allocated once, not per flush (`a_compaction_allocates_few_large_blocks`).
//! - **Replicating:** the compaction and the upload of its output at once, the busiest moment:
//!   about 30 MiB with the defaults (`a_compaction_into_s3_holds_its_inputs_and_one_batch_of_parts`).
//!   A compaction above level 1 holds its 17 MiB of windows instead of 5, to about 42 MiB (the
//!   sum of the two gates above).
//! - **Restore:** one 4 MiB chunk of the file being read plus 3 MiB: 7 MiB, whatever the
//!   database's size or the chain's length, since each file is one GET whose body is read as the
//!   decoder takes it (`a_restore_through_the_real_client_holds_one_chunk`; the same law at one
//!   to four GiB in `crates/core/tests/core/mem_big.rs`).
//! - **Idle:** a replicator on a quiet database holds about 1 MiB of live heap (the page
//!   checksum table and the HTTP client's idle connections: up to 16, 160 KiB of buffers each, which only a
//!   16-wide batch leaves open).
//! - **Snapshot:** 18 bytes a page on top of the above, 4 MiB at 1 GiB of 4 KiB pages and 74 MiB
//!   at 16 GiB: an 8-byte slot a page in the page checksum table, which a replicator holds for
//!   good, and about 9 bytes of the file's page index, which it holds while the snapshot is
//!   written (`snapshot_peak_heap_is_linear_in_pages_with_a_pinned_coefficient`; issue #64 and
//!   `docs/decisions/D213.md`).
//!
//! Not in these bounds: SQLite's own page cache, the `reqwest` transport (the gates run the `ureq` one that
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
#[doc(hidden)]
pub mod local;
mod meta;
mod readers;
mod replicator;
mod restore;
mod runtime;
mod scheduled;
#[doc(hidden)]
pub mod stack;
mod store;
mod thread;

pub use config::{Config, NonZeroDuration, ReaderTracking, SyncLimits};
pub use error::{RestoreError, StartError, TrustLocalError};
pub use events::{ChannelSink, Event, EventReceiver, EventSink, TrustRecordUnusable};
#[doc(hidden)]
pub use exec::block_on;
pub use health::{
    AppNotice, Counters, FailStop, Gone, Health, Mismatch, OpCounters, Phase, Readiness,
    StopOutcome, StoreCounters, Verification, VerifyFailure, VerifyOutcome, VerifyRuns, Writes,
};
pub use local::{ClearError, CreateError, IntegrityCheck, IntegrityFailure, clear_journal_files};
pub use readers::ReaderGuard;
pub use replicator::{Ready, Replicator, ReplicatorBuilder, StopFuture};
pub use restore::{record_trust, restore, restore_if_missing, restore_with, trust_local, verify};
#[cfg(feature = "testing")]
pub use runtime::FakeClockHandle;
pub use runtime::{CALLER_SLICE, Park, Runtime};
#[doc(hidden)]
pub use runtime::{OsEntropy, lock};
pub use store::{BoxFuture, DynStore, DynUpload};

pub use lsr_core::{
    AppCheckpoint, AppCheckpoints, AsOfInside, CallTotals, CheckpointConfig, CheckpointMode, Clock,
    Degraded, DegradedCause, Denied, Depth, Entropy, FinalRetention, FinalSnapshot,
    FinalSnapshotFailure, Gate, IfReplicationStops, L0Retention, Maintenance, MaxUnreplicatedAge,
    OtherWriter, Report, Response, Responses, RestoreOptions, RestoreTarget, Restored, Severity,
    SnapshotCause, SnapshotCauseKind, SnapshotCounts, Stall, StopCause, TakeoverRecord, Task, Trip,
    WhyBlocked, WorkCounts,
};
pub use lsr_ltx::{Timestamp, Txid};
pub use lsr_sqlite::{DatabaseFile, SetupError, WritesHeld, app_connection_raw, ffi};
