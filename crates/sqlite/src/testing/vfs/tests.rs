use std::time::Duration;

use rusqlite::{Connection, ErrorCode, OpenFlags};
use tempfile::TempDir;

use super::*;

fn vfs(name: &str) -> FaultVfs {
    FaultVfs::register(&format!("{name}-{}", std::process::id())).unwrap()
}

fn open(db: &std::path::Path, vfs: &FaultVfs) -> Connection {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags_and_vfs(db, flags, vfs.name()).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
        .unwrap();
    conn
}

fn fault(kind: FileKind, op: Op, nth: u32, effect: Effect) -> Fault {
    Fault {
        kind,
        op,
        nth: NonZeroU32::new(nth).unwrap(),
        effect,
    }
}

/// Every VFS and io method this VFS has is called at least once, and does what `unix` does:
/// a WAL database opens, writes, checkpoints, tells the time, waits on a busy lock, answers the
/// methods SQLite itself rarely calls, and its last close deletes the WAL.
#[test]
fn every_method_delegates_to_unix() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs("every-method");
    let a = open(&db, &vfs);
    a.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (randomblob(9000));")
        .unwrap();
    let day: f64 = a
        .query_row("SELECT julianday('now')", [], |row| row.get(0))
        .unwrap();
    assert!(day > 2_400_000.0);
    a.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_row| Ok(()))
        .unwrap();
    let file = crate::pages::file_pointer(&a, rusqlite::MAIN_DB).unwrap();
    let methods = methods_of(file);
    let mut reserved = -1;
    let (check, sector, device) = (
        methods.xCheckReservedLock.unwrap(),
        methods.xSectorSize.unwrap(),
        methods.xDeviceCharacteristics.unwrap(),
    );
    #[expect(unsafe_code, reason = "a method SQLite calls rarely, called directly")]
    // SAFETY: the connection's own open file and its method; it writes one `int`.
    let rc = unsafe { check(file.as_ptr(), &raw mut reserved) };
    #[expect(unsafe_code, reason = "a method SQLite calls rarely, called directly")]
    // SAFETY: the connection's own open file and its method.
    let sector = unsafe { sector(file.as_ptr()) };
    #[expect(unsafe_code, reason = "a method SQLite calls rarely, called directly")]
    // SAFETY: the connection's own open file and its method.
    let device = unsafe { device(file.as_ptr()) };
    assert_eq!((rc, reserved), (ffi::SQLITE_OK, 0));
    assert!(sector > 0);
    assert!(device >= 0);
    let b = open(&db, &vfs);
    b.busy_timeout(Duration::from_millis(2)).unwrap();
    a.execute_batch("BEGIN IMMEDIATE").unwrap();
    let busy = b.execute_batch("BEGIN IMMEDIATE").unwrap_err();
    assert_eq!(busy.sqlite_error_code(), Some(ErrorCode::DatabaseBusy));
    a.execute_batch("ROLLBACK").unwrap();
    drop(b);
    drop(a);
    assert!(
        !dir.path().join("a.db-wal").exists(),
        "the last close deleted the WAL"
    );
    assert!(vfs.syncs(FileKind::Wal) > 0 && vfs.syncs(FileKind::Main) > 0);
}

fn methods_of(file: NonNull<ffi::sqlite3_file>) -> &'static ffi::sqlite3_io_methods {
    #[expect(unsafe_code, reason = "reading an open file's method table")]
    // SAFETY: the file is open, so its first field is set.
    let table = unsafe { (*file.as_ptr()).pMethods };
    #[expect(unsafe_code, reason = "reading an open file's method table")]
    // SAFETY: an open file's `pMethods` is this VFS's static table.
    unsafe {
        &*table
    }
}

/// A fault fires on the nth call of its kind inside the window, once, and SQLite sees its code.
#[test]
fn a_fault_fires_once_on_its_call_inside_the_window() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs("fires-once");
    let conn = open(&db, &vfs);
    conn.execute_batch("CREATE TABLE t(v)").unwrap();
    vfs.set_window(false);
    vfs.arm(fault(
        FileKind::Wal,
        Op::Write,
        1,
        Effect::Error(ffi::SQLITE_IOERR_WRITE),
    ));
    conn.execute_batch("INSERT INTO t VALUES (1)").unwrap();
    assert!(vfs.fired().is_empty(), "outside the window nothing counts");
    vfs.set_window(true);
    let err = conn.execute_batch("INSERT INTO t VALUES (2)").unwrap_err();
    assert_eq!(
        err.sqlite_error().map(|e| e.extended_code),
        Some(ffi::SQLITE_IOERR_WRITE)
    );
    conn.execute_batch("INSERT INTO t VALUES (3)").unwrap();
    let fired = vfs.fired();
    assert_eq!(fired.len(), 1);
    assert_eq!(
        fired[0].path.as_deref(),
        Some(dir.path().join("a.db-wal").as_path())
    );
    assert!(vfs.calls(FileKind::Wal, Op::Write) >= 2);
    vfs.reset_calls();
    assert_eq!(vfs.calls(FileKind::Wal, Op::Write), 0);
    vfs.arm(fault(FileKind::Wal, Op::Write, 5, Effect::ShortWrite));
    vfs.disarm();
    conn.execute_batch("INSERT INTO t VALUES (4)").unwrap();
    assert_eq!(vfs.fired().len(), 1, "a disarmed fault never fires");
}

/// A short write lands half its bytes and is `SQLITE_FULL`, as `unixWrite` answers one.
#[test]
fn a_short_write_is_disk_full() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs("short-write");
    let conn = open(&db, &vfs);
    conn.execute_batch("CREATE TABLE t(v)").unwrap();
    vfs.arm(fault(FileKind::Wal, Op::Write, 1, Effect::ShortWrite));
    let err = conn
        .execute_batch("INSERT INTO t VALUES (randomblob(100))")
        .unwrap_err();
    assert_eq!(err.sqlite_error_code(), Some(ErrorCode::DiskFull));
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0);
}

/// A power loss undoes every write since each file's last sync, then fails every write until
/// the power is back; the database reopens as of its last sync.
#[test]
fn a_power_loss_keeps_only_what_was_synced() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs("power-loss");
    vfs.track_durability();
    let conn = open(&db, &vfs);
    conn.execute_batch(
        "PRAGMA synchronous = NORMAL; CREATE TABLE t(v); INSERT INTO t VALUES (1);
         PRAGMA wal_checkpoint(PASSIVE); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3);",
    )
    .unwrap();
    vfs.arm(fault(FileKind::Wal, Op::Sync, 1, Effect::PowerLoss));
    assert!(
        conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_row| Ok(()))
            .is_err()
    );
    assert!(!vfs.powered());
    assert!(
        conn.execute_batch("INSERT INTO t VALUES (4)").is_err(),
        "the machine is dark"
    );
    drop(conn);
    vfs.restore_power().unwrap();
    let conn = open(&db, &vfs);
    let rows: Vec<i64> = conn
        .prepare("SELECT v FROM t ORDER BY v")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows, [1], "only the checkpointed commit survived");
}

/// A wal-index lock request held elsewhere is retried by SQLite itself; an unlock never fails.
#[test]
fn a_refused_wal_index_lock_is_retried_by_sqlite() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs("shm-lock");
    let conn = open(&db, &vfs);
    conn.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES (1);")
        .unwrap();
    vfs.reset_calls();
    vfs.arm(fault(
        FileKind::Main,
        Op::ShmLock,
        1,
        Effect::Error(ffi::SQLITE_BUSY),
    ));
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(vfs.fired().len(), 1);
}

#[test]
fn a_name_with_a_nul_is_refused() {
    assert_eq!(
        FaultVfs::register("bad\0name").err(),
        Some(VfsError::NameHasNul)
    );
}
