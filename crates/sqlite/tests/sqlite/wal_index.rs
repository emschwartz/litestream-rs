//! The committed read through SQLite's mapping, against the bytes of `-shm` read through one
//! descriptor the test opens after the read lock and closes only after every connection (so its close
//! drops no lock anyone still needs): the same `mxFrame`, salts and `nBackfill` at every step of
//! commits, outside checkpoints in every mode while pinned, and a WAL restart once not.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::{CheckpointMode, Checkpointer, CommittedRead, WalSource};
use lsr_sqlite::Sqlite;
use rusqlite::Connection;
use tempfile::TempDir;

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn connect(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::ZERO).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn
}

/// A WAL database with Go's tables and a user table, every connection closed.
fn fresh_db() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    let conn = Connection::open(&path).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
         CREATE TABLE _litestream_lock (id INTEGER);
         CREATE TABLE t (v INTEGER);",
    )
    .unwrap();
    (dir, path)
}

fn commit(conn: &Connection, n: i64) {
    conn.execute("INSERT INTO t (v) VALUES (?1)", [n]).unwrap();
}

fn outside(conn: &Connection, mode: &str) -> (i64, i64, i64) {
    conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })
    .unwrap()
}

/// `mxFrame`, both salts and `nBackfill` from the file's bytes (the layout `lsr_sqlite` reads).
fn from_file(shm: &mut File) -> (u32, u32, u32, u32) {
    let mut buf = [0_u8; 100];
    shm.seek(SeekFrom::Start(0)).unwrap();
    shm.read_exact(&mut buf).unwrap();
    let word = |at: usize| -> [u8; 4] { buf[at..at.wrapping_add(4)].try_into().unwrap() };
    assert_eq!(buf[..48], buf[48..96], "the test reads between commits");
    (
        u32::from_ne_bytes(word(16)),
        u32::from_be_bytes(word(32)),
        u32::from_be_bytes(word(36)),
        u32::from_ne_bytes(word(96)),
    )
}

fn agree(step: &str, live: &mut Sqlite, shm: &mut File) -> CommittedRead {
    let read = live.committed().unwrap();
    let end = read.end();
    let mapped = (
        end.frame_count(),
        end.salts().salt1,
        end.salts().salt2,
        read.backfilled(),
    );
    assert_eq!(mapped, from_file(shm), "{step}");
    read
}

#[test]
fn the_committed_read_agrees_with_the_file_across_checkpoints_and_a_restart() {
    let (_dir, path) = fresh_db();
    let app = connect(&path);
    // A first frame, so the read lock takes a read mark above 0.
    commit(&app, -1);
    let mut live = Sqlite::open(&path).unwrap();
    #[expect(
        clippy::disallowed_methods,
        reason = "the test's reference read; this descriptor is closed only after every connection"
    )]
    let mut shm = File::open(suffixed(&path, "-shm")).unwrap();

    let start = agree("open", &mut live, &mut shm);
    for n in 0..3 {
        commit(&app, n);
        agree("commit", &mut live, &mut shm);
    }
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        let ran = outside(&app, mode);
        let read = agree(mode, &mut live, &mut shm);
        assert_eq!(
            read.end().salts(),
            start.end().salts(),
            "{mode} {ran:?}: pinned, no restart"
        );
    }
    commit(&app, 3);
    agree("commit", &mut live, &mut shm);

    // The backend checkpoints fully and comes back on read mark 0, which does not block a restart.
    live.checkpoint(CheckpointMode::Passive).unwrap();
    let full = agree("backfilled", &mut live, &mut shm);
    assert!(full.fully_backfilled());
    outside(&app, "RESTART");
    commit(&app, 4);
    let restarted = agree("commit after restart", &mut live, &mut shm);
    assert_ne!(restarted.end().salts(), start.end().salts());
    assert!(restarted.end().frame_count() < full.end().frame_count());

    let ran = outside(&app, "TRUNCATE");
    agree(
        &format!("TRUNCATE while pinned {ran:?}"),
        &mut live,
        &mut shm,
    );
    drop(live);
    drop(app);
    drop(shm);
}

/// A WAL database whose last connection closed (SQLite removed `-wal` and `-shm`): the read lock's
/// first read runs recovery, and the header maps initialised with zero frames.
#[test]
fn a_database_with_no_wal_yet_reads_an_empty_committed_end() {
    let (_dir, path) = fresh_db();
    assert!(
        !suffixed(&path, "-shm").exists(),
        "the last close removed it"
    );
    let mut live = Sqlite::open(&path).unwrap();
    let read = live.committed().unwrap();
    assert_eq!((read.end().frame_count(), read.backfilled()), (0, 0));
    assert_eq!(live.len().unwrap().map(|len| len.bytes()), Some(0));
}
