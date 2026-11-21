//! The host's automatic checkpoints against a pin held in another process, measured.
//!
//! A replicator that runs beside the application (a sidecar) cannot set `wal_autocheckpoint`
//! on the application's connections, so they keep SQLite's default: after any commit that
//! leaves 1000 or more frames in the WAL, a PASSIVE checkpoint on the committing connection
//! (`sqlite3WalDefaultHook`). Here the application is a child process at those defaults (no
//! `PERSIST_WAL` either; only `synchronous = NORMAL`, which does not touch checkpoints, so the
//! run is quick), and the pin is [`Sqlite`]'s read transaction in the test process.
//!
//! What SQLite 3.54.1 does (`walCheckpoint`, `walRestartLog` in the bundled `sqlite3.c`):
//! - With the pin on a read mark above 0, the checkpoint backfills exactly up to the pin's
//!   mark and stops there; the WAL never restarts, since a restart needs the whole log
//!   backfilled.
//! - With the pin on read mark 0 (taken when the WAL was fully backfilled), the host's next
//!   write restarts the WAL once, and every checkpoint after it backfills nothing: a
//!   checkpoint needs read lock 0 exclusively before it writes a page.
//!
//! Which read mark the pin holds is read from `/proc/locks` (read lock `i` is byte `123 + i`
//! of `-shm`), so the file is Linux only; nothing here opens the database or `-shm`.

#![cfg(target_os = "linux")]
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    clippy::missing_asserts_for_indexing,
    reason = "clippy.toml exempts only #[test] bodies; these are the test file's own helpers, where a failure is the test failing"
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use lsr_core::conformance::block_on;
use lsr_core::{
    CheckpointMode, Checkpointer, Event, RestoreTarget, SnapshotCause, Tick, WalSource,
};
use lsr_local::FileSink;
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::harness::open_app;
use lsr_sqlite::testing::{Rig, RigConfig};
use lsr_store_fs::FsStore;
use lsr_wal::Salts;
use rusqlite::Connection;
use tempfile::TempDir;

#[expect(
    dead_code,
    reason = "this file reads every line the child prints, not up to one"
)]
mod common;

/// SQLite's default `wal_autocheckpoint`, in frames.
const AUTOCHECKPOINT: u32 = 1000;
const LINE: &str = "lsr-pin ";
const DONE: &str = "lsr-pin done";
/// `-shm` byte of read lock 0 (`WALINDEX_LOCK_OFFSET` 120 plus `WAL_READ_LOCK(0)` 3).
const READ_LOCK_0: u64 = 123;

/// The child processes' entry: a no-op in the test run, the application in a child.
#[test]
fn child_process_entry() {
    let Some((role, db)) = common::role() else {
        return;
    };
    let (commits, row): (u32, u32) = match role.as_str() {
        "app-4096" => (110, 40_000),
        "app-512" => (100, 5_000),
        other => panic!("unknown role {other}"),
    };
    application(&db, commits, row);
}

/// What the application saw after one commit: SQLite's frame count, the frames backfilled,
/// and the WAL header's salts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    log: u32,
    backfilled: u32,
    salts: Salts,
}

/// The application: SQLite's defaults, `commits` commits of a `row`-byte random row, and after
/// each one line saying what the commit (and its automatic checkpoint) left.
fn application(db: &Path, commits: u32, row: u32) {
    let conn = Connection::open(db).unwrap();
    let auto: u32 = conn
        .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        auto, AUTOCHECKPOINT,
        "the application keeps SQLite's default"
    );
    conn.execute_batch("PRAGMA synchronous = NORMAL").unwrap();
    let mut out = std::io::stdout().lock();
    for _ in 0..commits {
        conn.execute(&format!("INSERT INTO t(v) VALUES (randomblob({row}))"), [])
            .unwrap();
        let (log, backfilled): (u32, u32) = conn
            .query_row("PRAGMA wal_checkpoint(NOOP)", [], |r| {
                Ok((r.get(1)?, r.get(2)?))
            })
            .unwrap();
        let salts = wal_salts(db);
        writeln!(
            out,
            "{LINE}{log} {backfilled} {} {}",
            salts.salt1, salts.salt2
        )
        .unwrap();
    }
    writeln!(out, "{DONE}").unwrap();
    out.flush().unwrap();
    // Its close is the last close of an application connection: with the pin's process
    // holding the database open, SQLite keeps the WAL.
    conn.close().unwrap();
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// The salts in the WAL header (SQLite takes no lock on `-wal`).
fn wal_salts(db: &Path) -> Salts {
    #[expect(clippy::disallowed_methods, reason = "the WAL carries no POSIX lock")]
    let mut wal = std::fs::File::open(suffixed(db, "-wal")).unwrap();
    let mut header = [0; 24];
    wal.read_exact(&mut header).unwrap();
    let be = |at: usize| u32::from_be_bytes(header[at..at + 4].try_into().unwrap());
    Salts {
        salt1: be(16),
        salt2: be(20),
    }
}

/// Runs the application in a child process and returns what it saw after each commit.
fn run_application(role: &str, db: &Path) -> Vec<Seen> {
    let mut child = common::spawn("child_process_entry", role, db);
    let out = child.stdout.take().unwrap();
    let mut seen = Vec::new();
    let mut done = false;
    // Read to the end: the child's libtest still prints its verdict after the last line.
    for line in BufReader::new(out).lines() {
        let line = line.unwrap();
        // libtest prints "test <name> ... " on the line the child's first output joins.
        let Some(at) = line.find(LINE) else { continue };
        let ours = &line[at..];
        done |= ours == DONE;
        if done {
            continue;
        }
        let fields: Vec<u32> = ours[LINE.len()..]
            .split(' ')
            .map(|f| f.parse().unwrap())
            .collect();
        let [log, backfilled, salt1, salt2] = fields[..] else {
            panic!("bad line {ours:?}");
        };
        seen.push(Seen {
            log,
            backfilled,
            salts: Salts { salt1, salt2 },
        });
    }
    assert!(child.wait().unwrap().success(), "the application exited");
    assert!(done, "the application finished its commits");
    seen
}

/// The read locks this process holds on `-shm`, as read-lock numbers, from `/proc/locks`.
fn read_marks_held(db: &Path) -> Vec<u64> {
    let inode = format!(
        ":{}",
        std::fs::metadata(suffixed(db, "-shm")).unwrap().ino()
    );
    let pid = std::process::id().to_string();
    let mut marks = Vec::new();
    for line in std::fs::read_to_string("/proc/locks").unwrap().lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let ours = fields.get(1) == Some(&"POSIX")
            && fields.get(4) == Some(&pid.as_str())
            && fields.get(5).is_some_and(|f| f.ends_with(&inode));
        if !ours {
            continue;
        }
        let start: u64 = fields[6].parse().unwrap();
        let end: u64 = fields[7].parse().unwrap();
        marks.extend(
            (start..=end)
                .filter(|byte| (READ_LOCK_0..READ_LOCK_0 + 5).contains(byte))
                .map(|byte| byte - READ_LOCK_0),
        );
    }
    marks.sort_unstable();
    marks
}

/// A WAL-mode database with the application's table, every connection closed.
fn create(db: &Path, page_size: u32) {
    let conn = Connection::open(db).unwrap();
    conn.pragma_update(None, "page_size", page_size).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_| Ok(()))
        .unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    conn.close().unwrap();
}

/// Every commit kept the WAL's generation and grew it: no restart.
fn assert_one_generation(seen: &[Seen], salts: Salts) {
    assert!(seen.iter().all(|s| s.salts == salts), "{seen:?}");
    assert!(seen.windows(2).all(|w| w[0].log < w[1].log), "{seen:?}");
    assert!(
        seen.last().unwrap().log > AUTOCHECKPOINT,
        "past the threshold"
    );
}

/// SQLite belief: with the pin on a read mark above 0, the host's automatic PASSIVE
/// checkpoints backfill exactly to the pin's mark and the WAL never restarts; with the pin on
/// read mark 0, the host's first write restarts the WAL once and its automatic checkpoints
/// backfill nothing. [`Sqlite`]'s committed read reports both, and its own checkpoint, once
/// the host is done, backfills the whole log.
#[test]
fn the_hosts_autocheckpoint_stops_at_a_pin_in_another_process() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("app.db");
    create(&db, 4096);
    let mut pin = Sqlite::open(&db).unwrap();
    // The open's own schema commits are in the WAL, unbackfilled: the pin sits above mark 0.
    let start = pin.committed().unwrap();
    let mark = start.end().frame_count();
    assert!(mark > 0 && start.backfilled() == 0, "{start:?}");
    assert_eq!(read_marks_held(&db).len(), 1);
    assert_ne!(read_marks_held(&db), vec![0]);

    let seen = run_application("app-4096", &db);
    assert_one_generation(&seen, start.end().salts());
    for s in &seen {
        let expected = if s.log >= AUTOCHECKPOINT { mark } else { 0 };
        assert_eq!(s.backfilled, expected, "{s:?}");
    }
    assert!(
        pin.wal_path().exists(),
        "the host's last close kept the WAL"
    );
    let last = seen.last().unwrap().log;
    let after = pin.committed().unwrap();
    assert_eq!(
        (
            after.end().frame_count(),
            after.backfilled(),
            after.end().salts()
        ),
        (last, mark, start.end().salts())
    );
    let run = pin.checkpoint(CheckpointMode::Passive).unwrap();
    assert_eq!((run.busy, run.log, run.checkpointed), (false, last, last));
    // Retaken over a fully backfilled WAL, the pin is on read mark 0.
    assert_eq!(read_marks_held(&db), vec![0]);

    let again = run_application("app-4096", &db);
    let restarted = again[0].salts;
    assert_ne!(
        restarted,
        start.end().salts(),
        "the host's first write restarted the WAL"
    );
    assert!(again[0].log < 50, "{:?}", again[0]);
    assert_one_generation(&again, restarted);
    assert!(again.iter().all(|s| s.backfilled == 0), "{again:?}");
    let last = again.last().unwrap().log;
    let after = pin.committed().unwrap();
    assert_eq!(
        (
            after.end().frame_count(),
            after.backfilled(),
            after.end().salts()
        ),
        (last, 0, restarted)
    );
    let run = pin.checkpoint(CheckpointMode::Passive).unwrap();
    assert_eq!((run.busy, run.log, run.checkpointed), (false, last, last));
}

/// Every snapshot the ticks from the `from`th on decided, with its cause.
fn snapshots_since(rig: &Rig<Sqlite>, from: usize) -> Vec<Option<SnapshotCause>> {
    rig.ticks()
        .iter()
        .skip(from)
        .filter_map(|tick| match tick {
            Tick::Ran {
                event: Event::Resnapshot(cause),
                ..
            } => Some(Some(*cause)),
            Tick::Ran {
                event: Event::Booted { cause },
                ..
            } => Some(*cause),
            Tick::Ran { .. }
            | Tick::Halted(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_) => None,
        })
        .collect()
}

/// Puts the pin on a read mark above 0: a host reader holds the log while the actor
/// checkpoints, so the checkpoint stops short and the pin is retaken over unbackfilled frames.
fn pin_above_mark_zero(rig: &mut Rig<Sqlite>) {
    let reader = open_app(rig.db(), rig.vfs(), "NORMAL").unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM t;")
        .unwrap();
    let begin = rig.ticks().len();
    // A PASSIVE that readers stop short reports success (SQLite resets its BUSY), so the
    // checkpoint is seen as a run, and the pin's new mark in the locks.
    for _ in 0..20 {
        rig.insert(1500).unwrap();
        rig.settle().unwrap();
        if rig.checkpointed_since(begin, CheckpointMode::Passive, false) {
            break;
        }
    }
    drop(reader);
    let marks = read_marks_held(rig.db());
    assert!(marks.len() == 1 && marks[0] > 0, "{marks:?}");
}

/// The engine over a pin whose host checkpoints at SQLite's default: after an application in
/// another process wrote past 1000 frames, once with the pin above read mark 0 and once on it,
/// the actor's sync restores every committed transaction, with no snapshot.
#[test]
fn the_engine_restores_everything_a_host_at_the_default_autocheckpoint_wrote() {
    let dir = TempDir::new().unwrap();
    let cfg = RigConfig {
        page_size: 512,
        ..RigConfig::default()
    };
    let mut rig = Rig::<Sqlite>::new(dir.path(), cfg).unwrap();
    rig.settle().unwrap();
    pin_above_mark_zero(&mut rig);
    let before = rig.backend().unwrap().committed().unwrap();

    let seen = run_application("app-512", rig.db());
    assert_one_generation(&seen, before.end().salts());
    rig.observe().unwrap();
    let after = rig.backend().unwrap().committed().unwrap();
    // The host's checkpoints moved the backfill up to the pin's mark, never past the frames
    // that were there when the pin was taken.
    assert!(
        after.backfilled() > before.backfilled(),
        "{before:?} {after:?}"
    );
    assert!(after.backfilled() <= before.end().frame_count());
    assert!(
        seen.iter()
            .all(|s| s.backfilled == before.backfilled() || s.backfilled == after.backfilled())
    );
    let from = rig.ticks().len();
    rig.settle().unwrap();
    assert_eq!(snapshots_since(&rig, from), vec![]);
    assert_eq!(read_marks_held(rig.db()), vec![0]);

    let again = run_application("app-512", rig.db());
    assert!(again.iter().all(|s| s.backfilled == 0), "{again:?}");
    assert_one_generation(&again, again[0].salts);
    rig.observe().unwrap();
    let from = rig.ticks().len();
    rig.settle().unwrap();
    assert_eq!(snapshots_since(&rig, from), vec![]);

    // The oracle restores at every TXID on the replica (one per synced file, each covering
    // many of the application's commits) and compares the pages with SQLite's own image of
    // the commit it ends at; the last one is the application's last commit.
    let checked = rig.check().unwrap();
    assert!(checked.restored >= 3, "{checked:?}");
    assert_eq!(checked.latest, rig.acked(), "{checked:?}");
    let rows = |conn: &Connection| -> i64 {
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap()
    };
    let restored = dir.path().join("restored.db");
    block_on(lsr_core::restore(
        &FsStore::new(rig.replica_root()),
        RestoreTarget::Latest,
        FileSink::create(&restored).unwrap(),
    ))
    .unwrap();
    assert_eq!(
        rows(&Connection::open(&restored).unwrap()),
        rows(rig.app().unwrap())
    );
}
