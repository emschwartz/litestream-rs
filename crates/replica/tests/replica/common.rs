//! What the end-to-end tests share: a real SQLite database with the application's own connection, a
//! file replica, a store whose faults a test switches on, and the check every test ends with
//! (a restore of the replica equals the database).

#![expect(
    dead_code,
    reason = "not every helper has a caller in every build of the tests"
)]

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use lsr_core::{
    CreatedAt, FileInfo, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, StoreError,
    StoreOp,
};
use lsr_ltx::TxidRange;
use lsr_replica::{
    Booted, Config, DynStore, FakeClockHandle, Health, Phase, Replicator, RestoreTarget, Runtime,
    Stopped, Task, Timestamp, restore,
};
use lsr_store_fs::FsStore;
use rusqlite::Connection;
use tempfile::TempDir;

/// The fake clock's start: 2023-11-14, any fixed instant would do.
pub const START: Timestamp = Timestamp::from_unix_millis(1_700_000_000_000);

/// Long enough that a test never sees it run out unless it is stuck. On the fake clock it
/// never runs out at all, since the clock moves only when a test moves it: [`LOOKS`] is what
/// ends a stuck wait there.
pub const PATIENCE: Duration = Duration::from_secs(600);

/// A database in a directory of its own, with the application's connection and a replica beside it.
pub struct Site {
    pub dir: TempDir,
    pub db: PathBuf,
    pub replica: PathBuf,
    pub app: Connection,
    restores: AtomicU32,
}

impl Site {
    /// A WAL-mode database holding one empty table; the application's connection is set up as an application
    /// must set every connection up.
    pub fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("app.db");
        let app = Connection::open(&db).unwrap();
        app.busy_timeout(Duration::from_secs(5)).unwrap();
        app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .unwrap();
        lsr_sqlite::app_connection(&app).unwrap();
        app.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
            .unwrap();
        let replica = dir.path().join("replica");
        Self {
            dir,
            db,
            replica,
            app,
            restores: AtomicU32::new(0),
        }
    }

    /// The file replica as the store an application passes in.
    pub fn store(&self) -> Arc<dyn DynStore> {
        Arc::new(FsStore::new(&self.replica))
    }

    /// A file replica whose faults the test switches.
    pub fn faulty(&self) -> Arc<Faulty> {
        Arc::new(Faulty::new(FsStore::new(&self.replica)))
    }

    /// `n` committed transactions, one row each, of a few pages.
    pub fn commit(&self, n: u32) {
        for _ in 0..n {
            self.app
                .execute("INSERT INTO t(v) VALUES (randomblob(3000))", [])
                .unwrap();
        }
    }

    /// Every row, in order.
    pub fn rows(conn: &Connection) -> Vec<(i64, Vec<u8>)> {
        let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }

    /// A fresh path in the site's directory.
    pub fn path(&self, name: &str) -> PathBuf {
        let n = self.restores.fetch_add(1, Ordering::Relaxed);
        self.dir.path().join(format!("{name}-{n}.db"))
    }

    /// Restores the newest state into a new file and checks it holds what the database holds
    /// and passes SQLite's integrity check.
    pub fn assert_restores(&self, store: &dyn DynStore) {
        let to = self.path("restored");
        restore(store, RestoreTarget::Latest, &to).unwrap();
        assert_same(&self.app, &to);
    }
}

/// `restored` holds the rows `app` sees and is a sound database.
pub fn assert_same(app: &Connection, restored: &Path) {
    let copy = Connection::open(restored).unwrap();
    let check: String = copy
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    assert_eq!(Site::rows(&copy), Site::rows(app));
}

/// The deterministic runtime: the clock moves only when the test says so.
pub fn fake() -> (Runtime, FakeClockHandle) {
    Runtime::fake(START, 7)
}

/// Go's defaults.
pub fn config() -> Config {
    Config::default()
}

/// How many times a wait looks at the health before it gives up. The replicator wakes a
/// waiter on every change and at least every [`lsr_replica::CALLER_SLICE`], so a wait that sees nothing
/// change fails after about a minute, where a deadline on the fake clock would never pass.
pub const LOOKS: u32 = 1_200;

/// Waits until `done` holds, failing the test if it never does: within [`PATIENCE`] on
/// the replicator's clock, and within [`LOOKS`] looks whatever the clock does.
pub fn wait(replicator: &Replicator, done: impl Fn(&Health) -> bool) -> Health {
    let (looks, met) = (Cell::new(0_u32), Cell::new(false));
    let seen = replicator.wait_until(PATIENCE, |health| {
        looks.set(looks.get().saturating_add(1));
        met.set(done(health));
        met.get() || looks.get() >= LOOKS
    });
    match seen {
        Some(health) if met.get() => health,
        Some(health) => panic_with(&health),
        None => panic_with(&replicator.health()),
    }
}

/// [`Replicator::wait_booted`], failing the test instead of hanging if the first boot never
/// ends (its deadline is on the replicator's clock).
pub fn booted(replicator: &Replicator) -> Booted {
    wait(replicator, |health| {
        health.phase != Phase::Booting || health.task(Task::Sync).is_some()
    });
    replicator.wait_booted(PATIENCE)
}

/// `stop(PATIENCE).wait()`, failing the test instead of hanging if the thread never stops
/// (the deadline is on the replicator's clock).
pub fn stopped(replicator: &Replicator) -> Stopped {
    let stop = replicator.stop(PATIENCE);
    wait(replicator, |health| {
        matches!(health.phase, Phase::Stopped(_) | Phase::Gone(_))
    });
    stop.wait()
}

/// [`stopped`] for a replicator on a fake clock: the clock moves `step` at a time while the
/// stop is pending, so a drain that waits out a retry's backoff (a busy database or a failed
/// call during the drain) ends instead of waiting on a clock nobody moves. Fails the test
/// after [`LOOKS`] steps.
pub fn stopped_moving(replicator: &Replicator, clock: &FakeClockHandle, step: Duration) -> Stopped {
    let stop = replicator.stop(PATIENCE);
    let gone = |health: &Health| matches!(health.phase, Phase::Stopped(_) | Phase::Gone(_));
    for _ in 0..LOOKS {
        let looks = Cell::new(0_u32);
        let seen = replicator.wait_until(PATIENCE, |health| {
            looks.set(looks.get().saturating_add(1));
            gone(health) || looks.get() >= 2
        });
        if seen.as_ref().is_some_and(gone) {
            return stop.wait();
        }
        clock.advance(step);
    }
    panic_with(&replicator.health())
}

fn panic_with(health: &Health) -> ! {
    panic!("the replicator never got there: {health:?}")
}

/// `SELECT busy FROM pragma_wal_checkpoint('TRUNCATE')` on a fresh connection: `true` when
/// something holds the WAL.
pub fn truncate_is_busy(db: &Path) -> bool {
    let conn = Connection::open(db).unwrap();
    conn.busy_timeout(Duration::ZERO).unwrap();
    let busy: i64 = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| row.get(0))
        .unwrap();
    busy != 0
}

/// A file replica whose faults the test switches on and off.
pub struct Faulty {
    inner: FsStore,
    /// Uploads fail as a dropped connection does.
    pub fail_uploads: AtomicBool,
    /// Every call is refused for credentials.
    pub refuse: AtomicBool,
    /// The next this many listings panic.
    pub panics: AtomicU32,
    /// Every call panics, before any other fault.
    pub panic_always: AtomicBool,
}

impl Faulty {
    fn new(inner: FsStore) -> Self {
        Self {
            inner,
            fail_uploads: AtomicBool::new(false),
            refuse: AtomicBool::new(false),
            panics: AtomicU32::new(0),
            panic_always: AtomicBool::new(false),
        }
    }

    fn gate(&self, op: StoreOp) -> Result<(), StoreError> {
        self.panic_if_always();
        if self.refuse.load(Ordering::SeqCst) {
            return Err(StoreError::Auth { op });
        }
        Ok(())
    }

    fn panic_if_always(&self) {
        if self.panic_always.load(Ordering::SeqCst) {
            panic!("an injected panic on every store call");
        }
    }

    fn maybe_panic(&self) {
        let armed = self
            .panics
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if armed {
            panic!("an injected panic inside a store call");
        }
    }
}

impl ObjectStore for Faulty {
    type Upload = <FsStore as ObjectStore>::Upload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.gate(StoreOp::List)?;
        self.maybe_panic();
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.gate(StoreOp::Read)?;
        self.inner.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.gate(StoreOp::Read)?;
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.gate(StoreOp::Read)?;
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.gate(StoreOp::Upload)?;
        if self.fail_uploads.load(Ordering::SeqCst) {
            return Err(StoreError::Transient {
                op: StoreOp::Upload,
                kind: std::io::ErrorKind::TimedOut,
            });
        }
        self.inner.begin_upload(key, created_at).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.gate(StoreOp::Delete)?;
        self.inner.delete(keys).await
    }
}
