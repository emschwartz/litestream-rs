# Phase 3: the public API, draft

Status: draft, revised 2026-11-20 after an independent review. It refines task 6 of `docs/plans/phase-3-syncing.md` into signatures; nothing here is built. Rulings it follows: P0 (a SQLite read transaction holds the pin), P3 (TRUNCATE only on the host's word), P4 (1-second polling), D6 (copy Go; no new files or formats), D7 (keep trying; stop only for a second writer; configurable responses). "Engine change" marks what the core must gain for this API.

## Shape in one paragraph

A host links `lsr-replica`, builds a store (`S3Store` over ureq or reqwest, `FsStore`, or a test store), wraps it once in an `Arc`, and calls `Replicator::start` once per database before its connection pool opens. The replicator owns one OS thread and two SQLite connections, and returns a handle. Each connection the host opens goes through `replicator.app_connection`; each long read the host starts holds a `ReaderGuard`; `health()` can be read at any time; `stop` runs at shutdown after the host's writers close. Nothing runs on the host's threads, no async runtime is required, and time, waiting and chance come in through `Runtime` so tests can drive all of them.

## Lifecycle

```rust
/// One replicated database. Cheap to clone: every clone is the same thread.
/// The last handle is found by a dedicated token, not `Arc::strong_count`.
#[derive(Clone)]
pub struct Replicator { /* Arc<Shared>, command sender, last-handle token */ }

impl Replicator {
    /// Opens the replication connections, takes the read transaction that pins
    /// the WAL, starts the thread, and returns. The first boot (listing the
    /// replica, recovering the position) runs on the thread afterwards and shows
    /// in `health()`. No store call happens on the caller's thread.
    ///
    /// If a replicator for the same file (device and inode, not path) is still
    /// draining in this process, waits for it, up to its shutdown grace.
    pub fn start(database: &Path, store: Arc<dyn DynStore>, config: Config) -> Result<Self, StartError>;

    /// `start` with every source of time, waiting and chance supplied.
    pub fn start_with(database: &Path, store: Arc<dyn DynStore>, config: Config, runtime: Runtime)
        -> Result<Self, StartError>;

    /// Blocks until the first boot has finished or `within` passes.
    pub fn wait_booted(&self, within: Duration) -> Booted;

    /// Stops after syncing and uploading everything committed. The deadline is
    /// the earlier of this call's and any earlier `stop`'s (a second SIGTERM
    /// with `within = 0` shortens it). The future needs no runtime; `.await` it
    /// or call `wait()`. It resolves at the deadline on the caller's side even
    /// if the thread is still inside a request (one request can take up to its
    /// own deadline, 60 s by default); the thread then finishes in the background.
    pub fn stop(&self, within: Duration) -> FinalSync;

    pub fn health(&self) -> Health;
    pub fn request_snapshot(&self);
}

/// Dropping the last handle is a best-effort `stop(config.shutdown_grace)` with
/// no I/O and no wait on the caller's thread. A host whose `main` returns right
/// after drop gets no drain: call `stop(..).wait()` instead.
impl Drop for Replicator { /* ... */ }

pub struct FinalSync { /* one-shot set by the thread, plus a caller-side deadline */ }
impl Future for FinalSync { type Output = Stopped; }
impl FinalSync { pub fn wait(self) -> Stopped; }

pub enum Stopped {
    /// Everything committed before the stop is on the replica (engine change:
    /// the drain reports its last txid).
    Drained { txid: Option<Txid> },
    /// Time ran out with work left. The wal-index gives a frame offset, not a
    /// txid, so what is left is measured in bytes and files.
    DeadlineReached { unsynced_wal_bytes: u64, staged_not_uploaded: u32 },
    /// Already stopped by a cause (a second writer, or a configured stop).
    Stopped(StopCause),
    /// The thread died (a panic that was not caught, under `panic = "abort"`
    /// nothing is caught at all), or this process is a fork of the one that
    /// started the replicator (the thread does not exist here).
    Gone(Gone),
}
pub enum Gone { ThreadDied, Forked }
pub enum Booted { Booted, Degraded(DegradedCause), TimedOut, Stopped(StopCause) }
```

### Time, waiting and chance

```rust
/// Everything the thread waits on. The thread parks until the tick's next wake,
/// the next poll, or a command (which unparks it), so a test with a fake clock
/// drives the poll interval, the backoff and the stop deadline without sleeping.
pub trait Park: Send + Sync {
    fn park(&self, until: Option<Timestamp>);
    fn unpark(&self);
}

pub struct Runtime {
    pub clock: Arc<dyn Clock>,
    pub park: Arc<dyn Park>,
    pub entropy: Arc<dyn Entropy>,
}
impl Runtime {
    /// The wall clock, a condvar park and the OS's randomness.
    pub fn system() -> Self;
    /// A fake clock and a park that advances it: the deterministic test setup.
    #[cfg(feature = "testing")] pub fn fake(start: Timestamp, seed: u64) -> (Self, FakeClockHandle);
}
```

Not covered by `Runtime`, and said so in the crate docs: the `S3Store` signs with its own clock (`S3Store::with_transport` takes it), and HTTP request deadlines run on real time inside the HTTP client.

### The store, type-erased

```rust
/// `ObjectStore` returns `impl Future`, so it cannot be a trait object. This
/// adapter boxes each call's future (one allocation per HTTP call) so the
/// engine is compiled once inside this crate, not once per host store type.
pub trait DynStore: Send + Sync { /* boxed-future versions of the ObjectStore methods */ }
impl<S: ObjectStore + Send + Sync> DynStore for S { /* ... */ }
```

The host keeps its own `Arc` clone, so it can call `restore` or `verify` on the same store while replicating.

## The host's connections and readers

```rust
impl Replicator {
    /// Sets `SQLITE_FCNTL_PERSIST_WAL` on and `wal_autocheckpoint = 0` on one of
    /// the host's connections, after checking it opens this replicator's database
    /// (device and inode). A method, so a pool cannot be configured before the
    /// replicator exists. Feature `rusqlite`, since the type pins the host to
    /// this crate's rusqlite version.
    #[cfg(feature = "rusqlite")]
    pub fn app_connection(&self, conn: &rusqlite::Connection) -> Result<(), SetupError>;

    /// The same for a host on sqlx, diesel or raw FFI.
    ///
    /// # Safety
    /// `db` is an open connection on this replicator's database, used by no
    /// other thread during the call, and opened by the same SQLite library this
    /// crate links (a second copy of SQLite in the process breaks the lock
    /// sharing between connections, the hazard of PLAN.md section 6). Note that
    /// `wal_autocheckpoint` replaces any `sqlite3_wal_hook` the host set.
    pub unsafe fn app_connection_raw(&self, db: *mut ffi::sqlite3) -> Result<(), SetupError>;

    /// Held for the length of a long read. While any guard lives, the replicator
    /// does not TRUNCATE (ruling P3). Counted, so overlapping readers are safe.
    pub fn long_reader(&self) -> ReaderGuard;
}
pub struct ReaderGuard { /* decrements on drop */ }
```

`Config::readers` is `ReaderTracking::Untracked` by default, under which TRUNCATE never runs, as ruling P3 says for a host that gives no signal. `Tracked` means the host promises to hold a guard for every long read.

The library also detects a broken obligation it cannot prevent: a backfill it did not run is a host checkpoint, counted in `Health::foreign_checkpoints` and sent as an event.

## Configuration

```rust
/// Go's behaviour by default (D6). `Clone + Debug + PartialEq`; the event sink is
/// not part of it.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    /// Go's meta directory, `<dir>/.<name>-litestream` unless set (D4).
    pub meta_dir: Option<PathBuf>,
    /// How often the WAL is polled (P4; Go: 1 s). Never zero.
    pub poll_interval: NonZeroDuration,
    /// First retry wait, doubling to 5 minutes (Go's `sync-interval` and backoff cap).
    pub retry_interval: NonZeroDuration,
    /// Go: PASSIVE at 1,000 pages or every minute, TRUNCATE at 121,359 pages (`db.go:32-35`).
    pub checkpoint: CheckpointConfig,
    /// Compaction levels, snapshot interval and retention, level 0 retention,
    /// and whether compaction outputs are read back (Go's `verify-compaction`).
    pub maintenance: Maintenance,
    /// Bytes and files one sync step may stage.
    pub sync_limits: SyncLimits,
    /// What each persistent failure does (D7). Default `Responses::GO_LIKE`.
    pub responses: Responses,
    pub readers: ReaderTracking,
    /// How long a drop may drain (Go's shutdown sync timeout, 30 s, `db.go:36`).
    pub shutdown_grace: Duration,
}

/// One field per persistent cause, so a new cause breaks the build. Public
/// fields make each new cause a semver-major change; intended.
pub struct Responses {
    pub credentials: Response, pub store_refused: Response,
    pub remote_corrupt: Response, pub remote_regressed: Response,
    pub wal_corrupt: Response, pub wal_truncated: Response, pub wal_unlinked: Response,
    pub page_size_changed: Response, pub database_page_mismatch: Response,
    /// `KeepTrying` rebuilds the engine and boots again, at most once per
    /// interval; it never resumes state a panic unwound through.
    pub bug: Response,
    /// Its own type: continuing past a second writer damages the history.
    pub foreign_writer: ForeignWriter,
}
pub enum Response { KeepTrying, Stop }
pub enum ForeignWriter { Stop, ContinueAndInterleaveHistories }
impl Responses { pub const GO_LIKE: Self; pub const STRICT: Self; }
```

Not configurable: the SQLite busy timeout (zero, so the engine's own policy decides what waits).

## Health and events

```rust
#[derive(Clone, Debug)]
pub struct Health {
    /// When the thread last wrote this; a stale value means the thread is stuck
    /// inside a blocking call.
    pub observed_at: Timestamp,
    pub phase: Phase,                         // Booting, Running, Draining, Stopped(StopCause), Gone(Gone)
    pub degraded: [Option<Degraded>; 5],      // per lane: sync, checkpoint, upload, prune, maintenance
    pub synced: Option<(Txid, Timestamp)>,    // newest staged locally
    pub replicated: Option<(Txid, Timestamp)>,// newest confirmed on the replica
    /// When the oldest commit not yet on the replica was first seen; `None` when
    /// caught up. This is the lag to alert on (the age of `replicated` fires
    /// falsely on an idle database).
    pub unreplicated_since: Option<Timestamp>,
    /// WAL and database size: a WAL held by the pin while uploads are degraded
    /// fills the host's disk.
    pub wal_bytes: u64,
    pub db_bytes: u64,
    pub resnapshot: Option<SnapshotCause>,
    pub foreign_checkpoints: u64,
    /// Go's metrics: per-operation counts, errors and durations for sync,
    /// checkpoint and each store call; bytes uploaded; retries; events dropped.
    pub counters: Counters,
}
pub struct Degraded { pub cause: DegradedCause, pub since: Timestamp, pub retries: u32 }

/// Called on the replicator's thread for every event except `Idle`; must not
/// block. A panic in it is caught and counted, never fatal.
pub trait EventSink: Send { fn event(&mut self, event: &Event); }
/// A bounded channel that drops the oldest event when full and counts drops.
pub struct ChannelSink { /* ... */ }
impl Replicator { pub fn set_events(&self, sink: Box<dyn EventSink>); }
```

## Restore and verify

Implemented in `lsr-core`, re-exported here as free functions over the same shared store, never as methods on `Replicator`: restoring over a live database must not look easy.

```rust
pub fn restore(store: &dyn DynStore, target: RestoreTarget, to: &Path) -> Result<Restored, RestoreError>;
pub fn verify(store: &dyn DynStore, depth: Depth) -> Result<Report, VerifyError>;
/// Go's `restore -if-db-not-exists`: restore Latest only when `to` is absent.
pub fn restore_if_missing(store: &dyn DynStore, to: &Path) -> Result<Option<Restored>, RestoreError>;
```

## Errors

```rust
pub enum StartError {
    Open { kind: io::ErrorKind },
    NotWal { journal_mode: String },
    /// A replicator for this file (device and inode) is running in this process.
    AlreadyReplicated,
    MetaDir { kind: io::ErrorKind },
    Sqlite { code: i32 },
    Thread { kind: io::ErrorKind },
}
```

## Engine changes this API needs

- The actor's wait becomes `Park`: it parks until the next wake, poll or command.
- `begin_shutdown(deadline)` takes the deadline per call, and a second call keeps the earlier one.
- The drain reports its last txid; a missed deadline reports unsynced WAL bytes and staged files.
- `Bug` under `KeepTrying` rebuilds the `Actor`.
- Host checkpoints are counted.

## Decided, with the alternatives

1. **The store is passed in built, type-erased.** The host chooses the HTTP client and credentials. The engine is compiled once. A `ReplicaUrl` in the config, as Go's `litestream.yml` has, belongs to the phase 4 sidecar.
2. **`start` returns before the first boot,** with `wait_booted` for hosts that want to wait.
3. **Deadlines are durations measured by the injected clock,** not `Instant`s.
4. **Events by callback** (the right primitive), with a ready-made bounded `ChannelSink`.
5. **One replicator and one thread per database,** as PLAN section 3.

## The review's answers to the open questions

- **Refuse to start when a Go process uses the same meta directory?** No. `start` makes no store call, and Go leaves no reliable local sign. A second writer is caught when the replica shows files above the local tip (the default stop). Document it.
- **`Readers`, or "pinned until a time"?** Neither: counted guards. A time is a guess, and with busy timeouts at zero a wrong answer costs a busy result, not a stall.
- **WAL size and the age of the oldest unreplicated commit in `Health`?** Yes, both.
- **Where do `restore` and `verify` live?** In `lsr-core`, re-exported here as free functions.
