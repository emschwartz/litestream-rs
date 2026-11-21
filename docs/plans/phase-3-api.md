# Phase 3: the public API, draft

Status: draft, revised 2026-11-20 after an independent review. It refines task 6 of `docs/plans/phase-3-syncing.md` into signatures; built on branch `worktree-agent-ac298cdabf3102e08`; the "As built" section at the end says where the code departs from it. Rulings it follows: P0 (a SQLite read transaction holds the pin), P3 (TRUNCATE only on the host's word), P4 (1-second polling), D6 (copy Go; no new files or formats), D7 (keep trying; stop only for a second writer; configurable responses). "Engine change" marks what the core must gain for this API.

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

## As built

Where the code (`crates/replica`, and the engine changes in `crates/core/src/actor.rs`) departs from the signatures above, and why.

- **The wait.** The actor already said when it wants its next tick (`Next`), so the engine needed no change for it: the thread parks until the earliest of that, the next poll and a command, and only there. Commands are not a channel but idempotent slots (stop with the earliest deadline, a snapshot asked, accept the database pages, the newest sink), bounded by construction; setting one unparks the thread.
- **A drain ticks again at once** after a tick that made progress (a boot, a sync, an upload, a resnapshot), because the engine reports `Drained` only on the tick after its last upload and names the drain's deadline as its wake until then; a retry still waits its backoff.
- **The drain's report.** `Tick::Drained { txid }` and a new `Tick::DeadlineReached(Unfinished { unsynced_wal_bytes, staged_not_uploaded })`; every tick after the second is `Halted(Shutdown)`. `begin_shutdown(deadline)` keeps the earlier of two deadlines. `ActorConfig::shutdown_grace` stays, read through `Actor::shutdown_deadline(now)`, so code that builds the config elsewhere still compiles.
- **Host checkpoints** are read from the wal-index on each sync tick: the backfill count grew within a generation, or the generation changed when no checkpoint of the actor's own had emptied the WAL. Two between polls count once.
- **`Bug`.** The engine's own rebuild covers a `Fault::Bug` it returns. A panic is caught by the thread, which drops the engine and both SQLite connections (the panic may have unwound through either) and, under `bug: KeepTrying`, builds a fresh actor and reopens the connections after `retry_wait` (doubling, so at most once per wait); health shows the sync lane degraded with `Bug` until the next boot. The fresh actor starts with no lanes or retention pin of its own: the thread cannot reach the engine's. Under `bug: Stop` the replicator stops with `HaltCause::Bug`.
- **`Gone`.** `ThreadDied`: an exit guard in the thread reports it when the thread unwinds past its catch, and a finished `JoinHandle` with no outcome is read the same way (tested with a sink whose panic payload panics again when dropped). `Forked`: the process id differs from the one that started the replicator. It is not tested: forking needs `unsafe`, which only `lsr-sqlite` may use.
- **The store.** `DynStore` is implemented for every `ObjectStore`, but `dyn DynStore` is not itself an `ObjectStore`: core's functions take their store by a sized type, so a private `Erased(&dyn DynStore)` adapts it, and a `Metered` wrapper inside the thread counts every call for `Health::counters.store`.
- **`app_connection_raw`** cannot be a method: it needs `unsafe`, which only `lsr-sqlite` may contain. It is `lsr_sqlite::app_connection_raw(db, &DatabaseFile)`, re-exported here, called as `app_connection_raw(db, replicator.database())`. `SetupError` is `lsr-sqlite`'s, which gained `NoFile`, `Stat` and `OtherDatabase` for the device-and-inode check, and `lsr_sqlite::app_connection_for` does that check for any rusqlite connection.
- **Readers.** Under `Untracked` the TRUNCATE threshold is switched off in the engine's config; under `Tracked` the checkpointer reports a TRUNCATE busy, without calling SQLite, while any guard lives, so it waits its backoff.
- **`Runtime::fake`** returns a held clock: it moves only by `FakeClockHandle::advance`, and the park waits for it; `release()` lets the park move the clock to each wake itself. A caller's wait (`wait_booted`, `FinalSync::wait`, `wait_until`) re-reads the injected clock at least every `CALLER_SLICE` (50 ms), since a fake clock signals nobody.
- **`FinalSync`**'s caller-side deadline cuts off only a tick that began before the deadline and is still running (the thread is inside a request); a tick that begins at or after it does no store I/O and ends at once. The future starts one watcher thread on its first pending poll, so it wakes at the deadline with no runtime.
- **The first sink goes in at the start.** `set_events` is a command the thread takes at the top of its next turn, and the thread starts booting as soon as it is spawned, so a sink set right after `start` can miss the first boot's event (the sidecar daemon lost its "replica position recovered" log line in about 3 of 66 boots that way). `Replicator::start_with_events(database, store, config, runtime, events: Box<dyn EventSink>)` gives the thread its sink before it runs, so it sees every event; the daemon starts every database with it. `set_events` stays, for replacing the sink later, and is documented as seeing only what comes after the thread takes it. The sink is not a `Config` field because `Config` is `Clone` and `PartialEq` and a sink is neither. Tested by `a_sink_given_at_start_sees_the_first_boot` (`crates/replica/tests/replicator.rs`), which also shows a sink set after the boot missing it.
- **Additions.** `Replicator::wait_until(within, done)` (a wait on a health condition; the tests use it and hosts may), `Replicator::database()`, and `Replicator::accept_database_pages()` (the operator's answer to the page checksum table's degraded state, `DegradedCause::DatabasePageMismatch`). `Event` is this crate's: the engine's events (never `Idle`) plus `ForeignCheckpoint { total }`, `Panicked { rebuilt }` and `Stopped(cause)`.
- **Errors.** `StartError` gained `Refused(Refusal)` and `PageSize { bytes }` from the backend; `Sqlite { code }` is -1 when the error was not SQLite's own. `restore` and `restore_if_missing` return this crate's `RestoreError { Destination(CreateError), Restore(lsr_core::RestoreError) }`, since the destination's refusal is not core's; neither runs SQLite's integrity check (Go's default).
- **Config.** `maintenance` is core's `Maintenance`, which has no switch for Go's `verify-compaction`: the boot read-back of each level's newest file always runs. `sync_limits` is `{ wal_bytes_per_step, files_per_upload }`. `Health::counters` holds ticks, syncs, checkpoints (and busy ones), uploads, retries (degraded ones included), caught panics, sink panics and per-operation store calls, errors and time; events dropped are counted by `ChannelSink`'s receiver.
- **The WAL size limit** (upstream Litestream #1605, where a WAL kept its 1.7 GB peak). SQLite cuts the WAL file back to `PRAGMA journal_size_limit` only on the connection whose commit restarts the log, and after the replicator's own PASSIVE or TRUNCATE that commit is its `_litestream_seq` bump on the writer connection, which had no limit. `lsr_sqlite::SqliteConfig::journal_size_limit: Option<NonZeroU64>` (bytes) now sets it on the writer only; `None` is SQLite's own no-limit, and the default is `lsr_sqlite::DEFAULT_JOURNAL_SIZE_LIMIT`, 64 MiB: above the WAL reached between two PASSIVE checkpoints at the default 1,000-frame threshold at every page size up to 64 KiB, so a steady-state WAL is not cut and regrown on each restart. The writer turns the limit off before it closes: as the database's last connection, SQLite's close checkpoints and, with a limit set, truncates the WAL to zero bytes, which would make the next start snapshot (`faults::the_wal_survives_the_replicator_closing_last` catches it). `lsr-replica` opens its connections with `Sqlite::open`, so it and the sidecar use the default; `Config` gains no field (it sets no SQLite option today) and the daemon's file no key (if Go adds one, its name is taken then). The cut can never remove a frame not yet synced: SQLite makes it only on the first commit after a restart, which needs every frame of the ended generation backfilled, and it drops only bytes past that commit's frames, which belong to the ended generation and fail any reader's salt check from the restart on. So it removes nothing the restart had not already made unreadable, and the rules that make following a restart safe (`docs/plans/phase-3-syncing.md`: the seal sync under the barrier before PASSIVE, `Continuity::Pinned`, and a snapshot on any other salt change) are unchanged. Tests: `crates/sqlite/tests/wal_size.rs` (the default limit after a library restart of a WAL past 80 MiB, failing before the fix; the actor continuing with no snapshot or fault across two shrinking restarts in both host placements, with the oracle restoring every transaction; the limit off and at another size; the last close keeping the WAL).
- **`lsr replicate <db> <replica>`** takes a directory (created if missing) or an `s3://` URL whose query carries the S3 settings. It installs its handlers before the replicator starts, prints `replicating <db>` once it runs, stops with a final sync within the 30 s grace on SIGINT, SIGTERM or SIGHUP (a second signal shortens the drain to nothing), prints the outcome, and exits 0 only when drained. Signals come through `signal-hook` 0.3 (MIT or Apache-2.0): a handler needs `unsafe`, which the CLI may not contain.
- **The scheduled verify** (`docs/PLAN.md` section 7; `crates/replica/src/scheduled.rs`, `crates/core/src/verify/against.rs`). What Go does at 041a8c3 (`store.go:267-273`, `:885-968`; `replica.go:1956-2028`; `cmd/litestream/replicate.go:360`): with `validation.interval` above zero (unset by default, so off), a ticker on the store lists every compaction level of every database (snapshots excluded) and checks the files are sorted and contiguous, logs one warning per gap, overlap or misordering, counts `litestream_validation_checks_total{db,level,result}` and the two `litestream_last_validation_success*` gauges, and changes nothing else: no file is read, nothing is restored, nothing is compared with the database, replication never stops. Its cost is one listing per level per database per interval. The replica-level `validation-interval` key is parsed, copied down, and never read. What v1 does, and where it departs:
  - `Config::verify_interval: Option<NonZeroDuration>`, `None` by default; the daemon maps Go's `validation.interval` to it for every database (zero or less is off, as in Go), and `validation-interval` is accepted with a no-effect warning. The first run is one interval after the start, each later one an interval after the previous ended (Go's ticker counts from the start and drops ticks while busy; they differ by at most one run's length). Time is the injected `Runtime`'s.
  - A run checks the listing as `verify` does at `Depth::Names` (holes, malformed snapshots, foreign names: Go's checks, and Rust's overlap rule) and, the departure Go lacks and section 7 requires, restores the transaction the replica holds and compares it with the live database at that transaction, page for page. Without it a stale page in a chain of `NoChecksum` files is found by nothing.
  - Where it runs. The live side needs the read transaction's view, so the engine reads it on the replicator's thread (`Actor::read_live_pages`, `take_live_pages`): in a sync tick once the replica holds everything staged, under that tick's `SyncedThrough` proof, with the level-9 snapshot's reads and checks (`snapshot::read_live`): WAL pages up to the synced end from the scan's verified checksums, every other page from the database through the replicator's own connection, checked against the page checksum table. No descriptor is opened. This costs one read of the pages the database file supplies, once per interval, the same work a snapshot does on the same thread. The replica side (listing, then the restore's reads into a one-checksum-a-page sink) runs on a thread of its own, `lsr-verify`, started on the first run and fed through a slot of one, so a slow or stuck store call never delays a sync and runs never overlap. Memory: 8 bytes a page on each side.
  - A failing run (a page differs, the lengths differ, the restore of that transaction fails, or the listing says the newest state does not restore) is a degraded state, not a stop: `Event::Verified { txid, outcome: VerifyOutcome::Failed(Mismatch) }` names the lowest differing page and how many, `Health::verification.failing` holds it (`Health::is_degraded` is true), and retention pins the last transaction a run found good (or the failing one when none did) through `Actor::pin_for_verify`, beside any degraded lane's pin (the older wins). The next run that passes clears both. A transaction compaction merged before the run read it is `Merged` (nothing to compare), and a failed listing or read is `Incomplete`; neither is degraded.
  - `Health::verification: Verification { last_run, last_good, failing: Option<VerifyFailure { since, txid, found }>, pinned, worst_finding, runs: VerifyRuns { passed, failed, merged, incomplete } }`. The sidecar's `/metrics` adds `lsr_verify_runs_total{db,result}`, `lsr_verify_failing{db}`, `lsr_verify_failing_txid{db}`, `lsr_verify_failing_page{db}` and `lsr_verify_last_success_timestamp_seconds{db}`; Go's per-level validation series are not emitted, since the check is not per level. Its log lines: a failing run at error, an incomplete one at warning.
  - Tests: `crates/replica/tests/verify.rs` (off by default, runs on its interval on the fake clock, a clean replica passes, a rewritten page is degraded with the page and transaction and pins the last good point, a run stuck in the store does not delay syncs), `crates/core/src/verify/tests.rs` (`against`), `crates/core/src/actor/tests/live.rs`, and the config mapping in `crates/config/tests/config/{go,map,classes}.rs`.
