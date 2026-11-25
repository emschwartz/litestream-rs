use std::sync::atomic::Ordering;

use rusqlite::Connection;

use super::*;

fn database() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let conn = Connection::open(&path).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (0)")
        .unwrap();
    (dir, path)
}

/// A connection that waits for no lock: a write the gate blocks fails at once.
fn probe(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn
}

fn busy(err: &rusqlite::Error) -> bool {
    err.sqlite_error().map(|e| e.code) == Some(rusqlite::ErrorCode::DatabaseBusy)
}

fn rows(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap()
}

/// The handler's timeout and call count on `conn`, read back.
fn state(conn: &Connection) -> (c_int, u32) {
    Handle::of_rusqlite(conn)
        .unwrap()
        .busy_state(|busy: &Busy| (busy.timeout_ms, busy.calls.load(Ordering::Relaxed)))
        .unwrap()
}

fn calls(conn: &Connection) -> u32 {
    state(conn).1
}

/// While the gate holds, a connection nothing set up cannot write (SQLite's own "database is
/// locked"), and a read begun before the hold reads on; the release lets the next write
/// through, and the shared state follows the lock both ways.
#[test]
fn a_held_gate_blocks_a_writer_nothing_set_up_and_no_reader() {
    let (_dir, path) = database();
    let held = WritesHeld::new();
    let mut gate = WriteGate::open(&path, &SqliteConfig::default(), held.clone()).unwrap();
    let app = probe(&path);
    let reader = probe(&path);
    reader.execute_batch("BEGIN").unwrap();
    assert_eq!(rows(&reader), 1);
    gate.hold().unwrap();
    assert!(gate.is_held() && held.is_held());
    let err = app.execute("INSERT INTO t VALUES (1)", []).unwrap_err();
    assert!(busy(&err), "{err}");
    assert_eq!(rows(&reader), 1, "the read goes on");
    assert_eq!(rows(&app), 1, "a new read too");
    gate.hold().unwrap();
    gate.release().unwrap();
    assert!(!gate.is_held() && !held.is_held());
    app.execute("INSERT INTO t VALUES (1)", []).unwrap();
    assert_eq!(rows(&app), 2);
}

/// A write transaction open when the gate is asked for keeps the lock: the gate is busy, the
/// transaction commits, and the next try takes the lock.
#[test]
fn a_write_in_flight_commits_and_the_gate_takes_the_lock_after_it() {
    let (_dir, path) = database();
    let held = WritesHeld::new();
    let mut gate = WriteGate::open(&path, &SqliteConfig::default(), held.clone()).unwrap();
    let app = probe(&path);
    app.execute_batch("BEGIN; INSERT INTO t VALUES (1)")
        .unwrap();
    assert_eq!(gate.hold(), Err(SourceError::Busy));
    assert!(!held.is_held());
    app.execute_batch("COMMIT").unwrap();
    gate.hold().unwrap();
    assert!(held.is_held());
    let err = app.execute("INSERT INTO t VALUES (2)", []).unwrap_err();
    assert!(busy(&err), "{err}");
    assert_eq!(rows(&app), 2);
}

/// With the handler set up and a busy timeout of 30 s, a write the held gate blocks fails at
/// once: SQLite called the handler once, and it declined to wait.
#[test]
fn a_set_up_connection_fails_at_once_while_the_gate_holds() {
    let (_dir, path) = database();
    let held = WritesHeld::new();
    let mut gate = WriteGate::open(&path, &SqliteConfig::default(), held.clone()).unwrap();
    let app = Connection::open(&path).unwrap();
    app.busy_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    fail_fast_while(&app, &held).unwrap();
    gate.hold().unwrap();
    let err = app.execute("INSERT INTO t VALUES (1)", []).unwrap_err();
    assert!(busy(&err), "{err}");
    assert_eq!(calls(&app), 1, "one call, no wait");
    gate.release().unwrap();
    app.execute("INSERT INTO t VALUES (1)", []).unwrap();
}

/// While the gate is clear the handler waits as SQLite's default one would: its waits, cut
/// at the timeout, and none once the timeout is spent.
#[test]
fn while_the_gate_is_clear_the_handler_waits_as_sqlite_s_default() {
    assert_eq!(default_delay(0, 1000), Some(1));
    assert_eq!(default_delay(3, 1000), Some(10));
    assert_eq!(default_delay(11, 1000), Some(100));
    assert_eq!(default_delay(17, 1000), Some(100));
    assert_eq!(default_delay(3, 10), Some(2), "cut at the timeout");
    assert_eq!(default_delay(4, 10), None, "the timeout is spent");
    assert_eq!(default_delay(0, 0), None, "no timeout, no wait");
    assert_eq!(default_delay(18, 1000), Some(72), "cut at the timeout");
    assert_eq!(default_delay(19, 1000), None);
    let state = |held: bool, timeout_ms| {
        let flag = WritesHeld::new();
        flag.set(held);
        Busy {
            held: flag,
            timeout_ms,
            calls: std::sync::atomic::AtomicU32::new(0),
        }
    };
    for (held, timeout_ms, retry) in [(false, 1000, true), (false, 0, false), (true, 1000, false)] {
        let busy = state(held, timeout_ms);
        assert_eq!(busy.retry(0), retry, "held={held} timeout={timeout_ms}");
    }
}

/// Setting the handler up twice keeps the connection's first timeout (SQLite reports 0 once a
/// handler of ours is in place) and one state, which goes when the connection closes.
#[test]
fn the_handler_is_set_up_once_and_freed_with_its_connection() {
    let (_dir, path) = database();
    let held = WritesHeld::new();
    let app = Connection::open(&path).unwrap();
    app.busy_timeout(std::time::Duration::from_millis(1500))
        .unwrap();
    fail_fast_while(&app, &held).unwrap();
    fail_fast_while(&app, &held).unwrap();
    assert_eq!(state(&app).0, 1500);
    assert_eq!(
        Arc::strong_count(&held.0),
        2,
        "one state, holding one count"
    );
    drop(app);
    assert_eq!(
        Arc::strong_count(&held.0),
        1,
        "closing the connection freed it"
    );
}
