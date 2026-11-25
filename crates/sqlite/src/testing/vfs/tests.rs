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
    let file = crate::pages::file_pointer(
        crate::capi::Handle::of_rusqlite(&a).unwrap(),
        crate::capi::MAIN,
    )
    .unwrap();
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
        Some(
            std::fs::canonicalize(dir.path())
                .unwrap()
                .join("a.db-wal")
                .as_path()
        ),
        "SQLite names the file by its canonical path (macOS's /var is a symlink)"
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

/// Under the block law a loss keeps any mix of the unsynced WAL blocks, and SQLite's own
/// recovery makes a consistent database of each: over seeds it keeps none or some of the
/// unsynced commits, and every image passes `integrity_check`.
#[test]
fn under_the_block_law_sqlite_recovers_every_image() {
    let vfs = vfs("block-law");
    let mut kept = std::collections::BTreeSet::new();
    for seed in 0..40_u64 {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("a.db");
        vfs.set_crash_law(CrashLaw::drawn(seed));
        let conn = open(&db, &vfs);
        // A new database file's directory entry is never synced by SQLite: create it first.
        conn.execute_batch("CREATE TABLE t(v)").unwrap();
        vfs.track_durability();
        conn.execute_batch(
            "PRAGMA synchronous = NORMAL; INSERT INTO t VALUES (randomblob(5000));
             PRAGMA wal_checkpoint(PASSIVE);",
        )
        .unwrap();
        for _ in 0..6 {
            conn.execute_batch("INSERT INTO t VALUES (randomblob(3000))")
                .unwrap();
        }
        vfs.cut_power();
        drop(conn);
        vfs.restore_power().unwrap();
        let conn = open(&db, &vfs);
        let check: String = conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(check, "ok", "seed {seed}");
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        kept.insert(rows);
    }
    assert!(
        kept.contains(&1) && kept.iter().any(|rows| *rows > 1),
        "{kept:?}"
    );
}

/// Under the block law the WAL that the last close deleted (`sqlite3WalClose` asks for no
/// directory sync) may be back after a loss, and SQLite opens the database either way.
#[test]
fn under_the_block_law_an_unsynced_delete_may_come_back() {
    let vfs = vfs("block-unlink");
    let mut back = std::collections::BTreeSet::new();
    for seed in 0..16_u64 {
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("a.db");
        vfs.set_crash_law(CrashLaw::drawn(seed));
        let conn = open(&db, &vfs);
        conn.execute_batch("CREATE TABLE t(v)").unwrap();
        vfs.track_durability();
        conn.execute_batch("INSERT INTO t VALUES (randomblob(3000))")
            .unwrap();
        drop(conn);
        let wal = dir.path().join("a.db-wal");
        assert!(!wal.exists(), "the last close deleted the WAL");
        vfs.cut_power();
        vfs.restore_power().unwrap();
        back.insert(wal.exists());
        let conn = open(&db, &vfs);
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            rows, 1,
            "seed {seed}: the checkpoint was synced before the delete"
        );
    }
    assert_eq!(back.len(), 2, "{back:?}");
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

/// The rows a reopened database holds after the power came back.
fn rows_after_power_loss(vfs: &FaultVfs, db: &std::path::Path, conn: Connection) -> Vec<i64> {
    vfs.cut_power();
    drop(conn);
    vfs.restore_power().unwrap();
    let conn = open(db, vfs);
    conn.prepare("SELECT v FROM t ORDER BY v")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// A database with row 1 checkpointed and rows 2 and 3 in the WAL, not synced.
fn two_unsynced_rows(name: &str) -> (TempDir, std::path::PathBuf, FaultVfs, Connection) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs(name);
    vfs.track_durability();
    let conn = open(&db, &vfs);
    conn.execute_batch(
        "PRAGMA synchronous = NORMAL; CREATE TABLE t(v); INSERT INTO t VALUES (1);
         PRAGMA wal_checkpoint(PASSIVE); INSERT INTO t VALUES (2); INSERT INTO t VALUES (3);",
    )
    .unwrap();
    (dir, db, vfs, conn)
}

/// A durability query answers for the file under any spelling of its directory (SQLite records
/// the resolved path, and a temporary directory sits behind a symlink on macOS), and refuses a
/// file the VFS never saw: "durable" for an unknown file would pass whatever a test asked.
#[test]
fn a_durability_query_follows_a_symlink_and_refuses_an_unknown_file() {
    let (dir, db, vfs, _conn) = two_unsynced_rows("spelling");
    let wal = db.with_extension("db-wal");
    assert!(
        !vfs.durable(&wal, 0, u64::MAX).unwrap(),
        "rows 2 and 3 are in the WAL, not synced"
    );
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(dir.path(), &link).unwrap();
    assert!(!vfs.durable(&link.join("a.db-wal"), 0, u64::MAX).unwrap());
    assert!(matches!(
        vfs.durable(&dir.path().join("never.db"), 0, u64::MAX),
        Err(VfsError::UnknownFile)
    ));
}

/// A sync made while the disk lies succeeds and keeps nothing; the same sync on an honest
/// disk keeps every commit.
#[test]
fn a_sync_on_a_lying_disk_keeps_nothing() {
    let (_dir, db, vfs, conn) = two_unsynced_rows("lying-sync");
    vfs.lie_on_syncs(true);
    crate::pages::sync_wal(crate::capi::Handle::of_rusqlite(&conn).unwrap()).unwrap();
    vfs.lie_on_syncs(false);
    assert_eq!(rows_after_power_loss(&vfs, &db, conn), [1]);

    let (_dir, db, vfs, conn) = two_unsynced_rows("honest-sync");
    crate::pages::sync_wal(crate::capi::Handle::of_rusqlite(&conn).unwrap()).unwrap();
    assert_eq!(rows_after_power_loss(&vfs, &db, conn), [1, 2, 3]);
}

/// A failed sync of the WAL loses the writes it covered for good: a later sync that succeeds
/// does not write them, so the commits they carried, and every commit chained after them, are
/// gone after a power loss. Without the failure the later sync keeps them all.
#[test]
fn a_failed_wal_sync_loses_its_writes_through_a_later_good_sync() {
    let (_dir, db, vfs, conn) = two_unsynced_rows("lost-writeback");
    vfs.set_window(true);
    vfs.arm(fault(
        FileKind::Wal,
        Op::Sync,
        1,
        Effect::Error(ffi::SQLITE_IOERR_FSYNC),
    ));
    assert!(crate::pages::sync_wal(crate::capi::Handle::of_rusqlite(&conn).unwrap()).is_err());
    vfs.set_window(false);
    conn.execute_batch("INSERT INTO t VALUES (4)").unwrap();
    crate::pages::sync_wal(crate::capi::Handle::of_rusqlite(&conn).unwrap()).unwrap();
    assert_eq!(rows_after_power_loss(&vfs, &db, conn), [1]);

    let (_dir, db, vfs, conn) = two_unsynced_rows("kept-writeback");
    conn.execute_batch("INSERT INTO t VALUES (4)").unwrap();
    crate::pages::sync_wal(crate::capi::Handle::of_rusqlite(&conn).unwrap()).unwrap();
    assert_eq!(rows_after_power_loss(&vfs, &db, conn), [1, 2, 3, 4]);
}

/// The database file's bytes, read while no connection has it open.
fn db_bytes(db: &std::path::Path) -> Vec<u8> {
    #[expect(
        clippy::disallowed_methods,
        reason = "no connection has the database open: the power loss killed them all"
    )]
    let bytes = std::fs::read(db).unwrap();
    bytes
}

/// Runs a partial PASSIVE backfill under `law` and cuts the power: returns the database file
/// before the backfill, after the loss, and whether SQLite left the backfill's writes unsynced.
fn partial_backfill_then_power_loss(name: &str, law: CrashLaw) -> (Vec<u8>, Vec<u8>, bool) {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("a.db");
    let vfs = vfs(name);
    let conn = open(&db, &vfs);
    conn.execute_batch(
        "PRAGMA page_size = 512; VACUUM; CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB);
         WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 40)
         INSERT INTO t SELECT i, randomblob(300) FROM n;",
    )
    .unwrap();
    drop(conn);
    let base = db_bytes(&db);
    let app = open(&db, &vfs);
    app.pragma_update(None, "synchronous", "NORMAL").unwrap();
    vfs.track_durability();
    vfs.set_crash_law(law);
    app.execute_batch("UPDATE t SET v = randomblob(300)")
        .unwrap();
    let reader = open(&db, &vfs);
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM t;")
        .unwrap();
    app.execute_batch("UPDATE t SET v = zeroblob(300) WHERE id <= 3")
        .unwrap();
    let (busy, log, done): (i64, i64, i64) = app
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert!(
        busy == 0 && done > 0 && done < log,
        "a partial backfill: {done} of {log}"
    );
    let unsynced = !vfs.durable(&db, 0, u64::MAX).unwrap();
    vfs.cut_power();
    drop((reader, app));
    vfs.restore_power().unwrap();
    let after = db_bytes(&db);
    let app = open(&db, &vfs);
    let zeroed: i64 = app
        .query_row(
            "SELECT count(*) FROM t WHERE v = zeroblob(300)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        zeroed, 3,
        "the WAL, synced before the backfill, recovers every commit"
    );
    let check: String = app
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
    (base, after, unsynced)
}

/// Real SQLite's partial PASSIVE backfill writes database pages and advances `nBackfill` with
/// no sync of the database file, so a power loss under the block law can tear that file (the
/// simulator's model draws the same tear, `lsr_sim`'s `sqlite::tests`). The WAL still carries
/// every frame, so SQLite's recovery masks the tear.
#[test]
fn a_partial_backfill_leaves_database_writes_a_power_loss_can_tear() {
    let mut torn = 0;
    for seed in 0..8 {
        let (base, after, unsynced) =
            partial_backfill_then_power_loss(&format!("partial-{seed}"), CrashLaw::drawn(seed));
        assert!(unsynced, "SQLite left the backfilled pages unsynced");
        if after != base {
            torn += 1;
        }
    }
    assert!(torn > 0, "no seed kept a backfilled block");
    let (base, after, _) = partial_backfill_then_power_loss("partial-none", CrashLaw::AllOrNothing);
    assert_eq!(
        after, base,
        "all-or-nothing keeps none of the backfill's writes"
    );
}
