//! An application connection that leaves WAL mode, on real SQLite and the stock VFS.
//!
//! `SQLITE_FCNTL_PERSIST_WAL` makes `sqlite3WalClose` keep the `-wal` and `-shm` files, frames
//! and all, unless `journal_size_limit` is set. SQLite runs it at a database's last close and
//! also when a connection leaves WAL mode, and a `-wal` beside a database is recovered by the
//! next read whatever journal mode the database header names. So on a connection with the flag
//! on, `PRAGMA journal_mode = DELETE` answers `delete` and the database stays on its old WAL;
//! a `VACUUM` to another page size then leaves frames of the old size over the new file, and
//! SQLite reads the database as corrupt.
//!
//! `lsr_sqlite::app_connection` turned the flag on for every application connection. It no
//! longer does (docs/decisions.md D123); these cases fail when it does. The replicator's own two
//! connections keep the flag, and never leave WAL mode.

use std::path::{Path, PathBuf};

use lsr_sqlite::{Sqlite, app_connection};
use rusqlite::Connection;
use tempfile::TempDir;

const ROWS: i64 = 200;

fn string(conn: &Connection, sql: &str) -> String {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn number(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn app(db: &Path) -> Connection {
    let conn = Connection::open(db).unwrap();
    app_connection(&conn).unwrap();
    conn
}

/// A WAL-mode database of 4096-byte pages whose rows are still in the WAL, and the application
/// connection that wrote them.
fn seeded(dir: &TempDir) -> (PathBuf, Connection) {
    let db = dir.path().join("app.db");
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch("PRAGMA page_size = 4096").unwrap();
    assert_eq!(string(&conn, "PRAGMA journal_mode = wal"), "wal");
    app_connection(&conn).unwrap();
    conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)")
        .unwrap();
    for _ in 0..ROWS {
        conn.execute("INSERT INTO t(v) VALUES (randomblob(300))", [])
            .unwrap();
    }
    (db, conn)
}

fn wal(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push("-wal");
    PathBuf::from(name)
}

fn sound(conn: &Connection, at: &str) {
    assert_eq!(string(conn, "PRAGMA integrity_check"), "ok", "{at}");
    assert_eq!(number(conn, "SELECT count(*) FROM t"), ROWS, "{at}");
}

const CHANGE_PAGE_SIZE: &str =
    "PRAGMA journal_mode = DELETE; PRAGMA page_size = 1024; VACUUM; PRAGMA journal_mode = WAL;";

/// The defect: the page size changed through rollback mode on one application connection, no
/// replicator and no other connection. With `PERSIST_WAL` on, the old WAL outlived the change
/// and the integrity check failed on the same connection.
#[test]
fn a_page_size_change_outside_wal_mode_leaves_a_sound_database() {
    let dir = TempDir::new().unwrap();
    let (db, conn) = seeded(&dir);
    conn.execute_batch(CHANGE_PAGE_SIZE).unwrap();
    assert_eq!(number(&conn, "PRAGMA page_size"), 1024);
    sound(&conn, "the connection that changed the page size");
    conn.execute("INSERT INTO t(v) VALUES (randomblob(300))", [])
        .unwrap();
    conn.execute("DELETE FROM t WHERE id = (SELECT max(id) FROM t)", [])
        .unwrap();
    drop(conn);
    let reopened = app(&db);
    assert_eq!(number(&reopened, "PRAGMA page_size"), 1024);
    sound(&reopened, "a connection opened after it");
}

/// Leaving WAL mode leaves it: no `-wal` stays behind for a later read to recover, writes go
/// to the database file, and the next connection finds a rollback-mode database. With
/// `PERSIST_WAL` on, the pragma answered `delete` and the database stayed in WAL mode.
#[test]
fn leaving_wal_mode_removes_the_wal() {
    let dir = TempDir::new().unwrap();
    let (db, conn) = seeded(&dir);
    assert_eq!(string(&conn, "PRAGMA journal_mode = DELETE"), "delete");
    assert!(!wal(&db).exists(), "the WAL outlived WAL mode");
    conn.execute("UPDATE t SET v = randomblob(300)", [])
        .unwrap();
    assert!(!wal(&db).exists(), "a rollback-mode write made a WAL");
    drop(conn);
    let reopened = app(&db);
    assert_eq!(string(&reopened, "PRAGMA journal_mode"), "delete");
    sound(&reopened, "a connection opened after it");
}

/// The replicator's own connections keep `PERSIST_WAL`, so the WAL it leaves when it closes
/// last still has its frames. The connection that leaves WAL mode decides what happens to that
/// WAL, and an application connection deletes it: the same page-size change, made before the
/// next replicator starts, is sound.
#[test]
fn a_page_size_change_after_the_replicator_closed_last_leaves_a_sound_database() {
    let dir = TempDir::new().unwrap();
    let (db, conn) = seeded(&dir);
    let live = Sqlite::open(&db).unwrap();
    drop(conn);
    drop(live);
    let kept = std::fs::metadata(wal(&db)).unwrap().len();
    assert!(kept > 32, "the replicator's close kept {kept} bytes of WAL");
    let conn = app(&db);
    conn.execute_batch(CHANGE_PAGE_SIZE).unwrap();
    assert_eq!(number(&conn, "PRAGMA page_size"), 1024);
    sound(&conn, "the connection that changed the page size");
    drop(conn);
    sound(&app(&db), "a connection opened after it");
}
