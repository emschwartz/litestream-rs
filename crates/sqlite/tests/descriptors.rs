//! The descriptor rule, measured: a full sync and checkpoint cycle through [`Sqlite`] opens and
//! closes no descriptor on the database or `-shm`, and leaves the process's POSIX locks on both
//! as they were; and why the rule exists (one `std::fs` open and close of `-shm` drops them).
//! Linux only: descriptors and locks are read from `/proc`.

#![cfg(target_os = "linux")]
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "clippy.toml exempts only #[test] bodies; these are the test file's own helpers, where a failure is the test failing"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::conformance::block_on;
use lsr_core::{
    CheckpointMode, MemStore, Position, Probes, Step, StepEnd, SyncConfig, Synced, SyncedThrough,
    Tip, WalSource, WalView, checkpoint, recover_traced, sync_step,
};
use lsr_ltx::Timestamp;
use lsr_sqlite::Sqlite;
use lsr_wal::WalReader;
use rusqlite::Connection;
use tempfile::TempDir;

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Every descriptor of this process on `file`, by number.
fn descriptors_on(file: &Path) -> BTreeMap<String, PathBuf> {
    fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let target = fs::read_link(entry.path()).ok()?;
            (target == file).then(|| (entry.file_name().to_string_lossy().into_owned(), target))
        })
        .collect()
}

/// How many listings in a row must add nothing before [`posix_locks_on`] answers.
const QUIET_READS: usize = 32;

/// The most listings [`posix_locks_on`] reads before it gives up.
const MAX_READS: usize = 10_000;

/// This process's POSIX record locks on `path`'s inode, from `/proc/locks` (no descriptor on it).
///
/// `/proc/locks` lists every lock on the machine, one kernel page per `read`, and other
/// processes' locks come and go between those reads, so a listing longer than a page can show
/// a line twice or skip one. Under parallel tests both were seen: this process's read-mark lock
/// on `-shm` listed twice, and the lock every open connection holds on it (byte 128) missing.
/// A torn listing never shows a lock that was not held, and this process changes none of its
/// own while it reads, so the answer is the union of its lines over listings, once
/// [`QUIET_READS`] listings in a row add nothing.
fn posix_locks_on(path: &Path) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut quiet = 0;
    for _ in 0..MAX_READS {
        let before = seen.len();
        seen.extend(posix_locks_once(path));
        quiet = if seen.len() == before {
            usize::saturating_add(quiet, 1)
        } else {
            0
        };
        if quiet == QUIET_READS {
            return seen.into_iter().collect();
        }
    }
    panic!("/proc/locks never settled in {MAX_READS} reads: {seen:?}");
}

/// One read of [`posix_locks_on`]'s listing, which may be torn.
fn posix_locks_once(path: &Path) -> Vec<String> {
    let inode = format!(":{}", fs::metadata(path).unwrap().ino());
    let pid = std::process::id().to_string();
    let mut locks: Vec<String> = fs::read_to_string("/proc/locks")
        .unwrap()
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let ours = fields.get(1) == Some(&"POSIX")
                && fields.get(4) == Some(&pid.as_str())
                && fields.get(5).is_some_and(|f| f.ends_with(&inode));
            ours.then(|| fields.get(3..).unwrap_or_default().join(" "))
        })
        .collect();
    locks.sort();
    locks
}

/// What the rule protects: descriptors and locks on the database and `-shm`.
#[derive(Debug, PartialEq, Eq)]
struct Held {
    db_fds: BTreeMap<String, PathBuf>,
    shm_fds: BTreeMap<String, PathBuf>,
    db_locks: Vec<String>,
    shm_locks: Vec<String>,
}

fn held(db: &Path) -> Held {
    let db = fs::canonicalize(db).unwrap();
    let shm = suffixed(&db, "-shm");
    Held {
        db_fds: descriptors_on(&db),
        shm_fds: descriptors_on(&shm),
        db_locks: posix_locks_on(&db),
        shm_locks: posix_locks_on(&shm),
    }
}

struct Engine {
    live: Sqlite,
    staging: MemStore,
    through: Option<SyncedThrough>,
    position: Option<Position>,
    now: i64,
}

impl Engine {
    fn sync(&mut self) {
        let file = self.live.open().unwrap().unwrap();
        let mut wal = WalReader::open(file).unwrap();
        let committed = self.live.committed().unwrap();
        let view = WalView {
            header: Some(*wal.header()),
            len: self.live.len().unwrap(),
            committed: Some(committed),
            epoch: self.live.epoch(),
        };
        let tip = self
            .position
            .as_ref()
            .map(|position| Tip::Running(position, Probes::read(&mut wal, position).unwrap()));
        let step = Step::try_from(recover_traced(tip, &view, None).recovered).unwrap();
        self.now = self.now.saturating_add(10);
        let mut db = self.live.clone();
        let synced = block_on(sync_step(
            step,
            &mut wal,
            committed,
            self.live.epoch(),
            &mut db,
            &mut lsr_core::PageTable::new(),
            &self.staging,
            &SyncConfig::default(),
            Timestamp::from_unix_millis(self.now),
        ))
        .unwrap();
        let through = match synced {
            Synced::Idle(through)
            | Synced::Wrote {
                end: StepEnd::Through(through),
                ..
            } => through,
            Synced::Wrote {
                end: StepEnd::Limited(_),
                ..
            } => panic!("the test never reaches the WAL cap"),
            Synced::IndexReset
            | Synced::Wrote {
                end: StepEnd::Unproven(_),
                ..
            } => panic!("the test never meets a wal-index reset ahead of its header"),
        };
        self.position = Some(*through.position());
        self.through = Some(through);
    }

    fn checkpoint(&mut self, mode: CheckpointMode) {
        self.now = self.now.saturating_add(10);
        let (mut wal, mut db, mut ck) = (self.live.clone(), self.live.clone(), self.live.clone());
        let done = block_on(checkpoint(
            self.through.take().unwrap(),
            mode,
            &mut wal,
            &mut db,
            &mut lsr_core::PageTable::new(),
            &mut ck,
            &self.staging,
            &SyncConfig::default(),
            Timestamp::from_unix_millis(self.now),
        ))
        .unwrap();
        self.position = Some(done.staged.position());
        self.through = done.staged.into_proof();
    }
}

/// Writes, syncs (a snapshot, then incremental files), a PASSIVE and a TRUNCATE checkpoint with
/// their syncs: the whole cycle the actor runs.
fn cycle(app: &Connection, engine: &mut Engine) {
    for n in 0..20 {
        app.execute(
            "INSERT INTO t(v) VALUES (randomblob(?1))",
            [300_i32.wrapping_add(n)],
        )
        .unwrap();
        engine.sync();
    }
    engine.checkpoint(CheckpointMode::Passive);
    engine.sync();
    app.execute("INSERT INTO t(v) VALUES (randomblob(5000))", [])
        .unwrap();
    engine.sync();
    engine.checkpoint(CheckpointMode::Truncate);
    engine.sync();
}

/// Semantics 1 of the backend: no descriptor on the database or `-shm` is opened or closed by
/// a sync and checkpoint cycle. A transient descriptor would not show in the descriptor list,
/// but its close would drop the process's locks, so the lock lists are the check that it never
/// happened. Both are taken after a first cycle, so the read transaction holds the same read
/// mark at both points.
#[test]
fn a_sync_and_checkpoint_cycle_opens_and_closes_no_descriptor_on_the_database_or_shm() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let app = Connection::open(&db).unwrap();
    app.busy_timeout(Duration::ZERO).unwrap();
    app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    lsr_sqlite::app_connection(&app).unwrap();
    app.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    let mut engine = Engine {
        live: Sqlite::open(&db).unwrap(),
        staging: MemStore::new(),
        through: None,
        position: None,
        now: 1_000,
    };
    cycle(&app, &mut engine);
    let before = held(&db);
    assert!(
        !before.db_fds.is_empty() && !before.shm_fds.is_empty(),
        "{before:?}"
    );
    assert!(
        !before.shm_locks.is_empty() && !before.db_locks.is_empty(),
        "the read transaction holds locks: {before:?}"
    );
    cycle(&app, &mut engine);
    assert_eq!(held(&db), before);
}

/// Why the crate's clippy config forbids `std::fs` opens: closing a descriptor on `-shm` drops
/// every POSIX lock the process holds on it, the read transaction's read mark and the DMS lock
/// included, while SQLite still believes it holds them.
#[test]
fn closing_a_descriptor_on_shm_drops_the_locks_sqlite_holds() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    let app = Connection::open(&db).unwrap();
    app.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    let live = Sqlite::open(&db).unwrap();
    let shm = suffixed(&db, "-shm");
    assert!(!posix_locks_on(&shm).is_empty());
    #[expect(
        clippy::disallowed_methods,
        reason = "the test opens and closes -shm on purpose, to show what that does"
    )]
    drop(fs::File::open(&shm).unwrap());
    assert!(
        posix_locks_on(&shm).is_empty(),
        "the close dropped the locks"
    );
    drop(live);
}
