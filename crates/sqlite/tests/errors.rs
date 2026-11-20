//! One test per error the backend returns: `OpenError` and `SetupError` at the boundary, and the
//! `SourceError` variants the trait methods produce on a live database. (The refusals of the
//! guard, `OpenError::PageSize` and the result-code sorting are unit tests inside the crate:
//! a real SQLite never reaches them from the outside.)

#![expect(
    clippy::unwrap_used,
    reason = "clippy.toml exempts only #[test] bodies; these are the test file's own helpers, where a failure is the test failing"
)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::{CheckpointMode, Checkpointer, PageSource, ReadEpoch, SourceError, WalSource};
use lsr_ltx::{PageNo, PageSize};
use lsr_sqlite::{
    OpenError, SetupError, Sqlite, SqliteConfig, app_connection, check_app_connection,
};
use rusqlite::Connection;
use rusqlite::limits::Limit;
use tempfile::TempDir;

fn app(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::ZERO).unwrap();
    conn
}

fn fresh() -> (TempDir, PathBuf, Connection) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("app.db");
    let conn = app(&path);
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    (dir, path, conn)
}

#[test]
fn open_a_missing_database_is_a_sqlite_error_and_creates_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("missing.db");
    let err = Sqlite::open(&path).unwrap_err();
    assert!(
        matches!(&err, OpenError::Sqlite { path: p, .. } if *p == path),
        "{err:?}"
    );
    assert!(!path.exists(), "no empty database was created");
}

#[test]
fn open_with_an_unknown_vfs_is_a_sqlite_error() {
    let (_dir, path, _app) = fresh();
    let config = SqliteConfig {
        vfs: Some("no-such-vfs".to_owned()),
    };
    let err = Sqlite::open_with(&path, &config).unwrap_err();
    assert!(matches!(err, OpenError::Sqlite { .. }), "{err:?}");
    let named = SqliteConfig {
        vfs: Some("unix".to_owned()),
    };
    Sqlite::open_with(&path, &named).unwrap();
}

#[test]
fn open_an_in_memory_database_is_not_wal() {
    let err = Sqlite::open(Path::new(":memory:")).unwrap_err();
    assert!(
        matches!(&err, OpenError::NotWal { mode } if mode == "memory"),
        "{err:?}"
    );
}

#[test]
fn setup_on_an_in_memory_connection_has_no_file_to_persist() {
    let conn = Connection::open_in_memory().unwrap();
    assert!(matches!(
        app_connection(&conn),
        Err(SetupError::PersistWal(_))
    ));
    assert!(matches!(
        check_app_connection(&conn),
        Err(SetupError::PersistWal(_))
    ));
}

#[test]
fn a_failing_setup_pragma_is_a_sqlite_error() {
    let (_dir, path, _app) = fresh();
    let conn = app(&path);
    conn.set_limit(Limit::SQLITE_LIMIT_SQL_LENGTH, 8).unwrap();
    assert!(matches!(app_connection(&conn), Err(SetupError::Sqlite(_))));
    assert!(matches!(
        check_app_connection(&conn),
        Err(SetupError::Sqlite(_))
    ));
}

#[test]
fn check_reports_each_setting_that_is_off() {
    let (_dir, path, _app) = fresh();
    let conn = app(&path);
    assert!(matches!(
        check_app_connection(&conn),
        Err(SetupError::PersistWalOff)
    ));
    app_connection(&conn).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 1000)
        .unwrap();
    assert!(matches!(
        check_app_connection(&conn),
        Err(SetupError::AutocheckpointOn { pages: 1000 })
    ));
}

/// The host's pool: every connection set up and checked, and the WAL survives the last close.
#[test]
fn host_helpers_on_a_pooled_connection() {
    let (_dir, path, first) = fresh();
    let mut pool = vec![first];
    pool.extend((0..3).map(|_| app(&path)));
    for conn in &pool {
        app_connection(conn).unwrap();
    }
    for conn in &pool {
        check_app_connection(conn).unwrap();
    }
    let live = Sqlite::open(&path).unwrap();
    for (n, conn) in (0_i64..).zip(&pool) {
        conn.execute("INSERT INTO t(v) VALUES (?1)", [n]).unwrap();
    }
    let wal = live.wal_path();
    drop(live);
    drop(pool);
    assert!(
        wal.exists(),
        "PERSIST_WAL: the last host connection to close kept the WAL"
    );
}

#[test]
fn a_contended_write_lock_is_busy() {
    let (_dir, path, app) = fresh();
    let mut live = Sqlite::open(&path).unwrap();
    app.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(live.begin_barrier().err(), Some(SourceError::Busy));
    assert_eq!(live.bump_seq().err(), Some(SourceError::Busy));
    app.execute_batch("ROLLBACK").unwrap();
    live.bump_seq().unwrap();
}

#[test]
fn a_page_past_the_end_is_short_and_a_wrong_buffer_is_invalid_input() {
    let (_dir, path, _app) = fresh();
    let mut live = Sqlite::open(&path).unwrap();
    live.checkpoint(CheckpointMode::Passive).unwrap();
    let size = PageSize::new(4096).unwrap();
    let pages = live.page_count().unwrap();
    let beyond = PageNo::new(pages + 1, size).unwrap();
    let mut buf = vec![0; 4096];
    assert_eq!(
        live.read_page(beyond, &mut buf),
        Err(SourceError::Short { pgno: beyond })
    );
    let mut small = vec![0; 100];
    assert_eq!(
        live.read_page(PageNo::new(1, size).unwrap(), &mut small),
        Err(SourceError::Io(std::io::ErrorKind::InvalidInput))
    );
}

#[test]
fn ending_a_lock_that_is_not_held_is_invalid_input() {
    let (_dir, path, _app) = fresh();
    let mut live = Sqlite::open(&path).unwrap();
    let barrier = live.begin_barrier().unwrap();
    let stale = lsr_core::Barrier::new(barrier.id().wrapping_add(7));
    assert_eq!(
        live.end_barrier(stale),
        Err(SourceError::Io(std::io::ErrorKind::InvalidInput))
    );
    live.end_barrier(barrier).unwrap();
}

/// The table the read transaction selects from is gone: the retake after a checkpoint fails,
/// every read until it is back is `ReadTxLost`, and the next read after that pins a new epoch.
#[test]
fn a_read_transaction_that_cannot_be_retaken_is_lost_until_it_can() {
    let (_dir, path, app) = fresh();
    let mut live = Sqlite::open(&path).unwrap();
    assert_eq!(live.epoch(), ReadEpoch::new(1));
    app.execute_batch("DROP TABLE _litestream_seq").unwrap();
    assert_eq!(
        live.checkpoint(CheckpointMode::Passive).err(),
        Some(SourceError::ReadTxLost)
    );
    assert_eq!(live.committed().err(), Some(SourceError::ReadTxLost));
    assert_eq!(live.page_count().err(), Some(SourceError::ReadTxLost));
    app.execute_batch("CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER)")
        .unwrap();
    live.committed().unwrap();
    assert_eq!(live.epoch(), ReadEpoch::new(2), "a new hold of the pin");
}

/// A TRUNCATE that ran but could not finish under another reader is a run with `busy` set and
/// the read transaction retaken.
#[test]
fn a_checkpoint_blocked_by_readers_reports_busy_in_the_run() {
    let (_dir, path, app) = fresh();
    let mut live = Sqlite::open(&path).unwrap();
    app.execute("INSERT INTO t(v) VALUES (zeroblob(5000))", [])
        .unwrap();
    let reader = Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _n: i64 = reader
        .query_row("SELECT COUNT(1) FROM t", [], |row| row.get(0))
        .unwrap();
    let run = live.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(run.busy, "{run:?}");
    assert_eq!(run.epoch, ReadEpoch::new(2));
}
