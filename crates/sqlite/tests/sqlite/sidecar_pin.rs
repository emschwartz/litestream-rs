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
//! Which read mark the pin holds is read from the kernel's list of this process's locks on
//! `-shm` in `/proc/self/fdinfo` (read lock `i` is byte `123 + i` of `-shm`), so the file is
//! Linux only; nothing here opens the database or `-shm`.
//!
//! The last two tests run core's host-checkpoint observer over the same reads, in both
//! placements of the pin, with the host at SQLite's defaults and at `wal_autocheckpoint = 0`.

#![cfg(target_os = "linux")]
#![expect(
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    clippy::missing_asserts_for_indexing,
    reason = "test code: an overflow or an out-of-range slice panics, which fails the test"
)]

use std::io::{BufRead, BufReader, Read, Write};
use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use lsr_core::conformance::block_on;
use lsr_core::{
    CheckpointMode, Checkpointer, Event, HostCheckpoint, HostObserver, RestoreTarget,
    SQLITE_AUTOCHECKPOINT, SnapshotCause, Tick, WalSource,
};
use lsr_local::FileSink;
use lsr_sqlite::Sqlite;
use lsr_sqlite::testing::harness::open_app;
use lsr_sqlite::testing::{Rig, RigConfig};
use lsr_store_fs::FsStore;
use lsr_wal::Salts;
use rusqlite::Connection;
use tempfile::TempDir;

use crate::common;

/// SQLite's default `wal_autocheckpoint`, in frames.
const AUTOCHECKPOINT: u32 = 1000;
const LINE: &str = "lsr-pin ";
const DONE: &str = "lsr-pin done";
/// `-shm` byte of read lock 0 (`WALINDEX_LOCK_OFFSET` 120 plus `WAL_READ_LOCK(0)` 3).
const READ_LOCK_0: u64 = 123;

/// This file's roles for the binary's `child_process_entry` (`common::spawn`): whether `role`
/// is one, after running the application it names.
pub fn child(role: &str, db: &Path) -> bool {
    let (commits, row, automatic): (u32, u32, bool) = match role {
        "app-4096" => (110, 40_000, true),
        "app-512" => (100, 5_000, true),
        // `host-default-<commits>` or `host-off-<commits>`, at 4096-byte pages.
        other => {
            let parsed = other
                .strip_prefix("host-")
                .and_then(|r| r.split_once('-'))
                .filter(|(setting, _)| matches!(*setting, "default" | "off"))
                .and_then(|(setting, commits)| Some((commits.parse().ok()?, setting)));
            let Some((commits, setting)) = parsed else {
                return false;
            };
            (commits, 40_000, setting == "default")
        }
    };
    application(db, commits, row, automatic);
    true
}

/// What the application saw after one commit: SQLite's frame count, the frames backfilled,
/// and the WAL header's salts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    log: u32,
    backfilled: u32,
    salts: Salts,
}

/// The application: SQLite's defaults (or, unless `automatic`, `wal_autocheckpoint = 0`),
/// `commits` commits of a `row`-byte random row, and after each one line saying what the
/// commit (and its automatic checkpoint) left.
fn application(db: &Path, commits: u32, row: u32, automatic: bool) {
    let conn = Connection::open(db).unwrap();
    let auto: u32 = conn
        .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        auto, AUTOCHECKPOINT,
        "the application keeps SQLite's default"
    );
    if !automatic {
        conn.execute_batch("PRAGMA wal_autocheckpoint = 0").unwrap();
    }
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

/// The read locks this process holds on `-shm`, as read-lock numbers.
///
/// Read from `/proc/self/fdinfo` of a descriptor this process has open on `-shm`, not from
/// `/proc/locks`: the kernel prints a file's locks into fdinfo in one pass under the file's
/// lock list lock, while `/proc/locks` lists every lock on the machine a page per read, so
/// locks other processes take or drop between two reads shift the listing and one of ours
/// shows twice or not at all (seen with tests running in parallel).
fn read_marks_held(db: &Path) -> Vec<u64> {
    let shm = std::fs::metadata(suffixed(db, "-shm")).unwrap();
    let fd = std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .find(|fd| {
            std::fs::metadata(fd.path())
                .is_ok_and(|m| m.is_file() && m.dev() == shm.dev() && m.ino() == shm.ino())
        })
        .unwrap()
        .file_name();
    let info = Path::new("/proc/self/fdinfo").join(fd);
    let mut marks = Vec::new();
    for line in std::fs::read_to_string(info).unwrap().lines() {
        // `lock:\t1: POSIX  ADVISORY  READ <pid> <dev>:<inode> <start> <end>`
        let Some(lock) = line.strip_prefix("lock:") else {
            continue;
        };
        let fields: Vec<&str> = lock.split_whitespace().collect();
        if fields.get(1) != Some(&"POSIX") {
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
    // The open's own schema commits are in the WAL, unbackfilled: the pin sits above read mark 0.
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
fn pin_above_read_mark_zero(rig: &mut Rig<Sqlite>) {
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
    pin_above_read_mark_zero(&mut rig);
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
    // The actor counted the host's backfill as a checkpoint it did not run.
    let host = rig.actor().host_checkpoints();
    assert_eq!((host.backfilled, host.restarted), (1, 0), "{host:?}");

    let again = run_application("app-512", rig.db());
    assert!(again.iter().all(|s| s.backfilled == 0), "{again:?}");
    assert_one_generation(&again, again[0].salts);
    rig.observe().unwrap();
    let from = rig.ticks().len();
    rig.settle().unwrap();
    assert_eq!(snapshots_since(&rig, from), vec![]);
    // The restart at the actor's own write after its full checkpoint is no host checkpoint.
    assert_eq!(rig.actor().host_checkpoints().ran(), 1);

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

/// Where the pin sits when the host starts writing.
#[derive(Clone, Copy, Debug)]
enum Placement {
    /// Over the open's own unbackfilled schema commits: a read mark above 0.
    AboveMarkZero,
    /// Retaken after the replicator's own full checkpoint: read mark 0.
    OnMarkZero,
}

/// A database in `dir`, the pin in `placement`, and one observer per assumed host threshold
/// in `assumed`, each having seen the read where the pin was taken.
fn pinned(
    dir: &TempDir,
    placement: Placement,
    assumed: &[Option<NonZeroU32>],
) -> (PathBuf, Sqlite, Vec<HostObserver>) {
    let db = dir.path().join("app.db");
    create(&db, 4096);
    let mut pin = Sqlite::open(&db).unwrap();
    let mut observers: Vec<HostObserver> = assumed.iter().map(|a| HostObserver::new(*a)).collect();
    for observer in &mut observers {
        assert_eq!(observer.poll(pin.committed().unwrap()), None);
    }
    if let Placement::OnMarkZero = placement {
        let run = pin.checkpoint(CheckpointMode::Passive).unwrap();
        assert!(!run.busy && run.checkpointed == run.log, "{run:?}");
        let after = pin.committed().unwrap();
        for observer in &mut observers {
            observer.ours(Some(after), Some(run));
        }
    }
    let marks = read_marks_held(&db);
    match placement {
        Placement::AboveMarkZero => assert!(marks.len() == 1 && marks[0] > 0, "{marks:?}"),
        Placement::OnMarkZero => assert_eq!(marks, vec![0]),
    }
    (db, pin, observers)
}

/// The host writes 95 commits (about 1140 frames, past SQLite's threshold), then 10 more, as
/// two processes, and every observer polls after each. Returns what each observer saw per
/// poll, and the backfill counts the host itself saw after each of its commits.
fn host_writes(
    db: &Path,
    pin: &mut Sqlite,
    observers: &mut [HostObserver],
    setting: &str,
) -> (Vec<Vec<Option<HostCheckpoint>>>, Vec<u32>) {
    let mut seen = vec![Vec::new(); observers.len()];
    let mut backfills = Vec::new();
    for commits in [95, 10] {
        let host = run_application(&format!("host-{setting}-{commits}"), db);
        backfills.extend(host.iter().map(|s| s.backfilled));
        let read = pin.committed().unwrap();
        assert!(read.end().frame_count() > AUTOCHECKPOINT, "{read:?}");
        for (observer, seen) in observers.iter_mut().zip(&mut seen) {
            seen.push(observer.poll(read));
        }
    }
    (seen, backfills)
}

/// A host at SQLite's defaults raises the host-checkpoint notice wherever the pin sits. Above
/// read mark 0 its checkpoint copies the frames below the pin (a backfill the replicator did not
/// run), and its next attempts copy nothing more; on read mark 0 its attempts copy nothing at all
/// and show only as the WAL growing past the threshold with nothing backfilled.
#[test]
fn a_host_at_the_defaults_raises_the_notice_on_either_read_mark() {
    let default = Some(SQLITE_AUTOCHECKPOINT);
    let dir = TempDir::new().unwrap();
    let (db, mut pin, mut observers) = pinned(&dir, Placement::AboveMarkZero, &[default]);
    let mark = pin.committed().unwrap().end().frame_count();
    let (seen, backfills) = host_writes(&db, &mut pin, &mut observers, "default");
    assert!(backfills.contains(&mark), "the host's checkpoint ran");
    assert!(
        matches!(
            seen[0][..],
            [
                Some(HostCheckpoint::Backfilled { from: 0, to }),
                Some(HostCheckpoint::Stalled { .. })
            ] if to == mark
        ),
        "{seen:?}"
    );
    assert_eq!(observers[0].counts().ran(), 1);

    let dir = TempDir::new().unwrap();
    let (db, mut pin, mut observers) = pinned(&dir, Placement::OnMarkZero, &[default]);
    let (seen, backfills) = host_writes(&db, &mut pin, &mut observers, "default");
    assert!(backfills.iter().all(|b| *b == 0), "nothing could be copied");
    assert!(
        matches!(seen[0][..], [None, Some(HostCheckpoint::Stalled { .. })]),
        "{seen:?}"
    );
    assert_eq!(observers[0].counts().ran(), 0, "no host checkpoint ran");
}

/// A host at `wal_autocheckpoint = 0` raises nothing. Above read mark 0 the frames below the pin
/// stay unbackfilled, which a host at the defaults would have copied, so nothing is inferred
/// even at the default threshold. On read mark 0 the wal-index looks the same as for a host whose
/// attempts fail, so only the threshold the observer is told tells them apart: told `None`,
/// it raises nothing; left at SQLite's default, it infers the attempts it cannot see.
#[test]
fn a_host_at_wal_autocheckpoint_0_raises_none() {
    let default = Some(SQLITE_AUTOCHECKPOINT);
    let dir = TempDir::new().unwrap();
    let (db, mut pin, mut observers) = pinned(&dir, Placement::AboveMarkZero, &[default, None]);
    let (seen, backfills) = host_writes(&db, &mut pin, &mut observers, "off");
    assert!(
        backfills.iter().all(|b| *b == 0),
        "the host ran no checkpoint"
    );
    assert_eq!(seen, vec![vec![None, None]; 2]);

    let dir = TempDir::new().unwrap();
    let (db, mut pin, mut observers) = pinned(&dir, Placement::OnMarkZero, &[None, default]);
    let (seen, backfills) = host_writes(&db, &mut pin, &mut observers, "off");
    assert!(backfills.iter().all(|b| *b == 0));
    assert_eq!(seen[0], [None, None], "told the host runs none");
    assert!(
        matches!(seen[1][..], [None, Some(HostCheckpoint::Stalled { .. })]),
        "the one case the wal-index cannot tell apart: {seen:?}"
    );
}
