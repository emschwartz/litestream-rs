# Phase 3: the public API, draft

Status: draft for review, 2026-11-20. It refines task 6 of `docs/plans/phase-3-syncing.md` into signatures; nothing here is built. Rulings it follows: P0 (a SQLite read transaction holds the pin), P3 (TRUNCATE only on the host's word), P4 (1-second polling), D6 (copy Go; no new files or formats), D7 (keep trying; stop only for a second writer; configurable responses).

## Shape in one paragraph

A host links `lsr-replica`, builds a store (any `ObjectStore`: `S3Store` over ureq or reqwest, or `FsStore`), and calls `Replicator::start` once per database before its connection pool opens. The replicator owns one OS thread and two SQLite connections, and returns a handle. The host calls `app_connection` on each connection it opens, tells the replicator when long readers start and stop, reads `health()` whenever it likes, and calls `stop` at shutdown after its writers close. Nothing runs on the host's threads, no async runtime is required, and time, randomness and sleep come in through traits so tests can stub them.

## The crate: `lsr-replica`

```rust
/// One replicated database. Cheap to clone: every clone is the same thread.
#[derive(Clone)]
pub struct Replicator { /* Arc<Shared>, command sender */ }

impl Replicator {
    /// Opens the database's replication connections, takes the read transaction
    /// that pins the WAL, starts the thread, and returns. The first boot (listing
    /// the replica, recovering the position) runs on the thread afterwards; its
    /// result shows in `health()`.
    ///
    /// Blocks the caller for the open only: no store call happens here.
    pub fn start<S>(database: &Path, store: S, config: Config) -> Result<Self, StartError>
    where
        S: ObjectStore + Send + Sync + 'static;

    /// `start` with every source of time and chance supplied: what the tests
    /// and the simulator use.
    pub fn start_with<S, C, P, E>(
        database: &Path,
        store: S,
        config: Config,
        env: Env<C, P, E>,
    ) -> Result<Self, StartError>
    where
        S: ObjectStore + Send + Sync + 'static,
        C: Clock + 'static,
        P: Sleeper + 'static,
        E: Entropy + 'static;

    /// A snapshot of the replicator's state, copied out of one `Arc`; never blocks.
    pub fn health(&self) -> Health;

    /// The host's word on long readers (ruling P3). Until it is first called the
    /// replicator assumes `Readers::Unknown`, which never allows TRUNCATE.
    pub fn set_readers(&self, readers: Readers);

    /// Asks for a level 9 snapshot at the next opportunity (an operator's
    /// request, `SnapshotCause::OperatorRequested`).
    pub fn request_snapshot(&self);

    /// Stops after syncing and uploading everything committed, within `within`
    /// as measured by the replicator's `Clock`. The future needs no runtime:
    /// `.await` it from any executor, or call `FinalSync::wait` to block.
    pub fn stop(&self, within: Duration) -> FinalSync;
}

/// Dropping the last handle sends a stop with `Config::shutdown_grace` and
/// returns at once: no I/O on the caller's thread. The thread drains, then ends.
impl Drop for Replicator { /* ... */ }

/// The outcome of `stop`.
pub struct FinalSync { /* a future over a one-shot set by the thread */ }
impl Future for FinalSync { type Output = Stopped; /* ... */ }
impl FinalSync { pub fn wait(self) -> Stopped; }

pub enum Stopped {
    /// Everything committed before the call is on the replica.
    Drained { txid: Txid },
    /// Time ran out with work left: what was synced and what was committed.
    DeadlineReached { synced: Option<Txid>, committed: Txid },
    /// Already stopped by a cause (D7: a second writer, or a configured stop).
    Stopped(StopCause),
}

/// Time, sleep and chance, injected (the clippy ban on clocks applies here too).
pub struct Env<C, P, E> { pub clock: C, pub sleeper: P, pub entropy: E }
impl Env<SystemClock, ThreadSleeper, OsEntropy> { pub fn system() -> Self; }
```

### Configuration

```rust
/// Every field has Go's default; `Config::default()` is Go's behaviour (D6).
pub struct Config {
    /// Go's meta directory, `<dir>/.<name>-litestream` unless set (D4).
    pub meta_dir: Option<PathBuf>,
    /// How often the WAL is polled for new commits (P4; Go: 1 s).
    pub poll_interval: Duration,
    /// When to checkpoint (Go: PASSIVE at 1,000 pages or every minute, TRUNCATE
    /// at 121,359 pages; `db.go:32-35`).
    pub checkpoint: CheckpointConfig,
    /// Compaction levels, snapshot interval, retention (Go's `store.go` defaults).
    pub maintenance: Maintenance,
    /// What each persistent failure does (D7). Default `Responses::GO_LIKE`.
    pub responses: Responses,
    /// How long a drop may take to drain (Go's shutdown sync timeout: 30 s, `db.go:36`).
    pub shutdown_grace: Duration,
    /// Where events go. Default: nowhere (health still reports the state).
    pub events: Option<Box<dyn EventSink>>,
}

/// D7: one field per persistent cause, so a new cause breaks the build.
pub struct Responses {
    pub credentials: Response, pub store_refused: Response,
    pub remote_corrupt: Response, pub remote_regressed: Response,
    pub wal_corrupt: Response, pub wal_truncated: Response, pub wal_unlinked: Response,
    pub page_size_changed: Response, pub database_page_mismatch: Response,
    pub bug: Response, pub foreign_writer: Response,
}
impl Responses { pub const GO_LIKE: Self; pub const STRICT: Self; }
pub enum Response { KeepTrying, Stop }

/// The host's signal for ruling P3.
pub enum Readers { Idle, Pinned, Unknown }
```

### Health and events

```rust
#[derive(Clone, Debug)]
pub struct Health {
    pub state: State,
    /// The newest transaction staged locally, and when.
    pub synced: Option<(Txid, Timestamp)>,
    /// The newest transaction confirmed on the replica, and when.
    pub replicated: Option<(Txid, Timestamp)>,
    /// The newest committed transaction the last poll saw.
    pub committed: Option<Txid>,
    /// Why the current or next sync is a snapshot, if it is one.
    pub resnapshot: Option<SnapshotCause>,
    /// Lifetime counters for the resource properties: LIST, GET, PUT, DELETE
    /// calls, bytes uploaded, checkpoints by mode, retries.
    pub counters: Counters,
}

pub enum State {
    /// Started; the first boot has not finished.
    Booting,
    Running,
    /// Some lanes are retrying a persistent failure (D7); the rest run.
    Degraded { lanes: Vec<Degraded> },
    Draining,
    Stopped(StopCause),
}
pub struct Degraded { pub lane: Lane, pub cause: DegradedCause, pub since: Timestamp, pub retries: u32 }
pub enum Lane { Sync, Checkpoint, Upload, Prune, Maintenance }

/// Receives every event the actor emits: booted (with its snapshot cause),
/// synced, checkpointed, uploaded, maintained, degraded, recovered, clamped
/// clock, stopped. Called on the replicator's thread; must not block.
pub trait EventSink: Send { fn event(&mut self, event: &Event); }
```

### The host's connections

```rust
/// Sets `SQLITE_FCNTL_PERSIST_WAL` and `wal_autocheckpoint = 0` on one of the
/// host's connections (PLAN section 6). Call it on every connection the host
/// opens, before its first statement.
pub fn app_connection(conn: &rusqlite::Connection) -> Result<(), SetupError>;
/// Reads both settings back.
pub fn check_app_connection(conn: &rusqlite::Connection) -> Result<(), SetupError>;
/// The same for a host that does not use rusqlite (sqlx, diesel, raw FFI),
/// given its `sqlite3*`.
///
/// # Safety
/// `db` is an open connection, used by no other thread during the call.
pub unsafe fn app_connection_raw(db: *mut ffi::sqlite3) -> Result<(), SetupError>;
```

### Restore and verify

Blocking wrappers over the phase 1 functions, on the caller's thread with the
crate's small executor, over the same store type the host replicates to:

```rust
pub fn restore<S: ObjectStore>(store: &S, target: RestoreTarget, to: &Path) -> Result<Restored, RestoreError>;
pub fn verify<S: ObjectStore>(store: &S, depth: Depth) -> Result<Report, VerifyError>;
```

### Errors

```rust
pub enum StartError {
    /// The path is not a SQLite database, or cannot be opened.
    Open { kind: io::ErrorKind },
    /// The database is not in WAL mode.
    NotWal { journal_mode: String },
    /// Another replicator in this process already holds this database.
    AlreadyReplicated,
    /// The meta directory cannot be created or read.
    MetaDir { kind: io::ErrorKind },
    /// SQLite refused the read transaction or a setup statement.
    Sqlite { code: i32 },
    /// The thread could not be spawned.
    Thread { kind: io::ErrorKind },
}
```

## Choices made, and the alternatives

1. **The store is passed in built, not described in `Config`.** The host picks the HTTP client, credentials and transport (ureq, or reqwest on its own runtime) when it builds the `S3Store`, and tests pass `MemStore`. The alternative is a `ReplicaUrl` in the config, as Go's `litestream.yml` has; it belongs to the phase 4 sidecar, which can parse a URL into a store.
2. **`start` returns before the first boot.** Booting lists the replica, which can be slow or fail. Returning after the pin only means a host never waits on the network to open its pool; the cost is that early `health()` says `Booting`. The alternative is a `start` that returns after boot, with a timeout.
3. **`stop(within: Duration)`, measured by the injected clock,** rather than an `Instant` deadline, so a test's fake clock drives it.
4. **Events by callback, health by snapshot.** The callback runs on the replicator's thread and must not block; a host that wants a channel wraps one. The alternative is a bounded channel the library owns, which then has to decide what to drop when it is full.
5. **One replicator per database, one thread each,** as PLAN section 3. A host with many databases pays a thread each; a shared thread pool is a later optimisation, with a measurement.
6. **`app_connection_raw` for non-rusqlite hosts,** since PLAN's phase 0 asked whether the sqlx fork exposes the raw handle. It is `unsafe` because it takes a pointer.

## Open questions for review

- Should `start` refuse when the meta directory is held by a running Go process? Go takes no lock there, so detecting it means reading Go's state, and D6 forbids adding a lock file.
- Is `Readers` enough for P3, or does the host need to say "pinned until" a time?
- Does `Health` need the WAL size and the oldest unreplicated commit's age (Go exposes metrics like these through Prometheus)?
- Should `restore` and `verify` live here or stay only in `lsr-core` and `lsr-cli`?
