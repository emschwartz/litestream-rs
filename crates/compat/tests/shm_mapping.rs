//! Spike S1 of `docs/plans/phase-3-syncing.md` (answers in `docs/spikes.md`, "Phase 3 spikes"):
//! can the library read the wal-index header through SQLite's own mapping of `-shm`, with no
//! descriptor of its own, from a rusqlite connection?
//!
//! The probe takes the main database's `sqlite3_file*` with `SQLITE_FCNTL_FILE_POINTER` and asks
//! its `xShmMap` for region 0 (32 KiB, no extend). Before that call it refuses, with a typed
//! [`Refusal`], every state in which `unixShmMap` would open the `-shm` file itself (no read
//! transaction, not WAL, exclusive locking mode, which uses heap memory) or in which the method is
//! not there (`pMethods` null, `iVersion` below 2, `xShmMap` null). The header is read as
//! 32-bit atomic loads in SQLite's own order (copy 0, a full fence, copy 1, `walIndexTryHdr`),
//! then checked as SQLite checks it: equal copies, `isInit`, the known `iVersion`, and `aCksum`.
//! A read that fails a check is [`NotReady`], which a caller retries.
//!
//! Phase 3 puts this code in `lsr-sqlite`; here it is a test so the answers stay measured.
//! Linux only: descriptors and locks are read from `/proc`.

#![cfg(target_os = "linux")]
#![expect(
    clippy::unwrap_used,
    reason = "the probe's helpers are test code; a failure there is the test failing"
)]
#![expect(
    unsafe_code,
    reason = "spike S1 calls SQLite's VFS methods through raw pointers; lsr-sqlite will own this code"
)]

use std::collections::BTreeMap;
use std::ffi::{c_int, c_void};
use std::fs;
use std::mem::offset_of;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering, fence};

use lsr_compat::LiveSqlite;
use lsr_core::{CommittedRead, WalSource};
use rusqlite::{Connection, ffi};
use tempfile::TempDir;

/// The wal-index region size every SQLite caller passes (`WALINDEX_PGSZ`, 32 KiB).
const REGION_BYTES: c_int = 32_768;
const REGION_WORDS: usize = 8_192;
/// One `WalIndexHdr` copy is 48 bytes, twelve words.
const COPY_WORDS: usize = 12;
/// `WalCkptInfo.nBackfill`, byte 96.
const N_BACKFILL_WORD: usize = 24;
/// `WALINDEX_MAX_VERSION` in SQLite 3.54.1 (`sqlite3.c:67150`).
const WALINDEX_VERSION: u32 = 3_007_000;

/// Why the probe would not map the wal-index. Each case would otherwise open a `-shm`
/// descriptor inside SQLite, or call through a pointer that is not there.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// The connection holds no read transaction, so its WAL may not have mapped `-shm` yet.
    NoReadTransaction,
    /// The connection's journal mode is not WAL (it reported this one).
    NotWal(String),
    /// Exclusive locking keeps the wal-index in heap memory, not in `-shm`.
    NotNormalLocking(String),
    /// `sqlite3_file_control` failed.
    NoFilePointer { rc: c_int },
    /// The main database file is not open (`pMethods` is null).
    FileNotOpen,
    /// The VFS's methods predate shared memory (`iVersion` below 2).
    OldIoMethods { version: c_int },
    /// The VFS has no `xShmMap`.
    NoShmMap,
    /// `xShmMap` returned an error (`SQLITE_READONLY` included: the mapping is read-only).
    MapFailed { rc: c_int },
    /// `xShmMap` succeeded but region 0 does not exist yet.
    Unmapped,
    /// The region is not 4-byte aligned, so it cannot be read as `u32` words.
    Misaligned,
}

/// A header read that a retry may fix, as SQLite's own readers retry.
#[derive(Debug, PartialEq, Eq)]
enum NotReady {
    /// The two copies differ: a writer was between its two `memcpy`s.
    Torn,
    /// `isInit` is zero: no connection has built the header.
    Uninitialised,
    /// The copies agree but `aCksum` does not match them.
    BadChecksum,
    /// A wal-index format this code does not know.
    UnknownVersion(u32),
}

/// The fields of one checked header read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Header {
    change: u32,
    mx_frame: u32,
    salt1: u32,
    salt2: u32,
    backfilled: u32,
}

impl Header {
    fn same_as(&self, read: &CommittedRead) -> bool {
        let end = read.end();
        end.frame_count() == self.mx_frame
            && end.salts().salt1 == self.salt1
            && end.salts().salt2 == self.salt2
            && read.backfilled() == self.backfilled
    }
}

type ShmMap =
    unsafe extern "C" fn(*mut ffi::sqlite3_file, c_int, c_int, c_int, *mut *mut c_void) -> c_int;

type ShmUnmap = unsafe extern "C" fn(*mut ffi::sqlite3_file, c_int) -> c_int;

fn handle(conn: &Connection) -> *mut ffi::sqlite3 {
    // SAFETY: the pointer is only passed to SQLite calls made while `conn` is borrowed.
    unsafe { conn.handle() }
}

/// The states in which the connection's WAL has already mapped `-shm` (`walIndexReadHdr`
/// maps region 0 when a read transaction starts).
fn check_reader(conn: &Connection) -> Result<(), Refusal> {
    // SAFETY: a live handle and a NUL-terminated schema name.
    let state = unsafe { ffi::sqlite3_txn_state(handle(conn), c"main".as_ptr()) };
    if state != ffi::SQLITE_TXN_READ && state != ffi::SQLITE_TXN_WRITE {
        return Err(Refusal::NoReadTransaction);
    }
    let mode: String = conn
        .query_row("PRAGMA main.journal_mode", [], |row| row.get(0))
        .map_err(|_e| Refusal::NotWal(String::new()))?;
    if mode != "wal" {
        return Err(Refusal::NotWal(mode));
    }
    let locking: String = conn
        .query_row("PRAGMA main.locking_mode", [], |row| row.get(0))
        .map_err(|_e| Refusal::NotNormalLocking(String::new()))?;
    if locking != "normal" {
        return Err(Refusal::NotNormalLocking(locking));
    }
    Ok(())
}

fn file_pointer(conn: &Connection) -> Result<NonNull<ffi::sqlite3_file>, Refusal> {
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    // SAFETY: FILE_POINTER writes one `sqlite3_file*` through the argument (`sqlite3.c:192592`).
    let rc = unsafe {
        ffi::sqlite3_file_control(
            handle(conn),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            (&raw mut file).cast::<c_void>(),
        )
    };
    if rc != ffi::SQLITE_OK {
        return Err(Refusal::NoFilePointer { rc });
    }
    NonNull::new(file).ok_or(Refusal::FileNotOpen)
}

/// `xShmMap` of the file's methods, reading `iVersion` before the field a version 1 table lacks.
fn shm_map_of(file: NonNull<ffi::sqlite3_file>) -> Result<ShmMap, Refusal> {
    // SAFETY: `file` points to a live `sqlite3_file`, whose first field is `pMethods`.
    let methods = unsafe { (*file.as_ptr()).pMethods };
    if methods.is_null() {
        return Err(Refusal::FileNotOpen);
    }
    let version_at = methods
        .wrapping_byte_add(offset_of!(ffi::sqlite3_io_methods, iVersion))
        .cast::<c_int>();
    // SAFETY: every io_methods table starts with `iVersion`; no reference to the whole table
    // is made, so a shorter version 1 table is never read past its end.
    let version = unsafe { version_at.read() };
    if version < 2 {
        return Err(Refusal::OldIoMethods { version });
    }
    let map_at = methods
        .wrapping_byte_add(offset_of!(ffi::sqlite3_io_methods, xShmMap))
        .cast::<Option<ShmMap>>();
    // SAFETY: version 2 and later tables include `xShmMap` (`sqlite3.h`, `sqlite3_io_methods`).
    let map = unsafe { map_at.read() };
    map.ok_or(Refusal::NoShmMap)
}

/// Region 0 of the connection's own wal-index mapping, borrowed for as long as the connection.
///
/// The region stays mapped while the connection's WAL is open: `unixShmUnmap` runs only when the
/// WAL closes (connection close, or a journal-mode change that `lsr-sqlite` never makes on its
/// own reader).
fn map(conn: &Connection) -> Result<&[AtomicU32], Refusal> {
    check_reader(conn)?;
    let file = file_pointer(conn)?;
    let shm_map = shm_map_of(file)?;
    let mut region: *mut c_void = ptr::null_mut();
    // SAFETY: the method belongs to `file`; with `pShm` already set it only returns
    // `apRegion[0]` under the node's mutex (`sqlite3.c:45760`).
    let rc = unsafe { shm_map(file.as_ptr(), 0, REGION_BYTES, 0, &raw mut region) };
    if rc != ffi::SQLITE_OK {
        return Err(Refusal::MapFailed { rc });
    }
    let words = NonNull::new(region.cast::<AtomicU32>()).ok_or(Refusal::Unmapped)?;
    if !words.is_aligned() {
        return Err(Refusal::Misaligned);
    }
    // SAFETY: region 0 is 32 KiB of mapped, writable memory owned by the connection's shm
    // node, which outlives the borrow of `conn`; other writers use it as shared memory, which
    // atomic loads tolerate.
    Ok(unsafe { std::slice::from_raw_parts(words.as_ptr().cast_const(), REGION_WORDS) })
}

/// SQLite's `walChecksumBytes` in native order over the first 40 bytes of a header copy.
fn header_checksum(copy: &[u32; COPY_WORDS]) -> (u32, u32) {
    let (pairs, _rest) = copy.as_chunks::<2>();
    pairs
        .iter()
        .take(5)
        .fold((0_u32, 0_u32), |(s1, s2), &[a, b]| {
            let s1 = s1.wrapping_add(a).wrapping_add(s2);
            let s2 = s2.wrapping_add(b).wrapping_add(s1);
            (s1, s2)
        })
}

fn load_copy(words: &[AtomicU32], first: usize) -> [u32; COPY_WORDS] {
    let mut copy = [0_u32; COPY_WORDS];
    for (slot, word) in copy.iter_mut().zip(words.iter().skip(first)) {
        *slot = word.load(Ordering::Relaxed);
    }
    copy
}

/// One header read in `walIndexTryHdr`'s order, checked as it checks.
fn read_header(words: &[AtomicU32]) -> Result<Header, NotReady> {
    let first = load_copy(words, 0);
    fence(Ordering::SeqCst);
    let second = load_copy(words, COPY_WORDS);
    fence(Ordering::SeqCst);
    let backfilled = words
        .get(N_BACKFILL_WORD)
        .map_or(0, |w| w.load(Ordering::Relaxed));
    if first != second {
        return Err(NotReady::Torn);
    }
    let [
        version,
        _pad,
        change,
        init,
        mx_frame,
        _pages,
        _c1,
        _c2,
        s1,
        s2,
        k1,
        k2,
    ] = first;
    if init.to_ne_bytes().first() == Some(&0) {
        return Err(NotReady::Uninitialised);
    }
    if version != WALINDEX_VERSION {
        return Err(NotReady::UnknownVersion(version));
    }
    if header_checksum(&first) != (k1, k2) {
        return Err(NotReady::BadChecksum);
    }
    Ok(Header {
        change,
        mx_frame,
        // The salts are the WAL header's bytes copied verbatim, so big-endian.
        salt1: u32::from_be_bytes(s1.to_ne_bytes()),
        salt2: u32::from_be_bytes(s2.to_ne_bytes()),
        backfilled,
    })
}

fn connect(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn
}

/// Go's read transaction (`db.go:1288`), as `LiveSqlite` takes it.
fn begin_read(conn: &Connection) {
    conn.execute_batch("BEGIN").unwrap();
    let _n: i64 = conn
        .query_row("SELECT COUNT(1) FROM _litestream_seq", [], |row| row.get(0))
        .unwrap();
}

fn checkpoint(conn: &Connection, mode: &str) -> (i64, i64, i64) {
    conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })
    .unwrap()
}

fn commit(conn: &Connection, n: i64) {
    conn.execute("INSERT INTO t (v) VALUES (?1)", [n]).unwrap();
}

/// A database in WAL mode with Go's tables and one user table.
fn fresh_db() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    let conn = Connection::open(&path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode = wal", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    conn.execute_batch(
        "CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
         CREATE TABLE _litestream_lock (id INTEGER);
         CREATE TABLE t (v INTEGER);",
    )
    .unwrap();
    (dir, path)
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Every descriptor of this process whose target is under `dir`, by number.
fn descriptors_under(dir: &Path) -> BTreeMap<String, PathBuf> {
    fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let target = fs::read_link(entry.path()).ok()?;
            target
                .starts_with(dir)
                .then(|| (entry.file_name().to_string_lossy().into_owned(), target))
        })
        .collect()
}

/// This process's POSIX record locks on `path`'s inode, from `/proc/locks` (no descriptor on it).
fn posix_locks_on(path: &Path) -> Vec<String> {
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

/// Reads through the mapping and through `LiveSqlite`'s file read, and asserts they agree.
fn agree(step: &str, region: &[AtomicU32], live: &mut LiveSqlite) -> Header {
    let mapped = read_header(region).unwrap();
    let filed = live.committed().unwrap();
    eprintln!("{step:<34} mapped {mapped:?}");
    assert!(
        mapped.same_as(&filed),
        "{step}: mapped {mapped:?} vs file {filed:?}"
    );
    mapped
}

#[test]
fn the_mapping_is_the_one_every_connection_in_the_process_uses() {
    let (_dir, path) = fresh_db();
    let lib = connect(&path);
    let other = connect(&path);
    begin_read(&lib);
    begin_read(&other);
    let ours = map(&lib).unwrap();
    let theirs = map(&other).unwrap();
    assert!(
        ptr::eq(ours.as_ptr(), theirs.as_ptr()),
        "one shm node per inode, one region 0"
    );
    other.execute_batch("COMMIT").unwrap();

    let before = read_header(ours).unwrap();
    commit(&other, 1);
    let after = read_header(ours).unwrap();
    assert!(
        after.mx_frame > before.mx_frame,
        "{before:?} then {after:?}"
    );
    assert_eq!(after.change, before.change.wrapping_add(1));
}

/// Under the library's read transaction, the mapping and `LiveSqlite`'s file read agree at every
/// step: commits, outside checkpoints in every mode while pinned, and a WAL restart once not.
#[test]
fn the_mapping_agrees_with_the_file_read_across_checkpoints_and_a_restart() {
    let (_dir, path) = fresh_db();
    let app = connect(&path);
    // A first frame, so both read transactions take a read mark above 0 and pin the log.
    commit(&app, -1);
    let mut live = LiveSqlite::open(&path).unwrap();
    let lib = connect(&path);
    begin_read(&lib);
    let region = map(&lib).unwrap();

    let start = agree("open", region, &mut live);
    for n in 0..3 {
        commit(&app, n);
        agree("commit", region, &mut live);
    }
    for mode in ["PASSIVE", "RESTART", "TRUNCATE"] {
        let ran = checkpoint(&app, mode);
        let h = agree(&format!("{mode} while pinned {ran:?}"), region, &mut live);
        assert_eq!(
            (h.salt1, h.salt2),
            (start.salt1, start.salt2),
            "pinned: no restart"
        );
    }
    commit(&app, 3);
    agree("commit", region, &mut live);

    // Unpin: the library ends its read, LiveSqlite checkpoints fully and comes back on read
    // mark 0, which does not block a restart.
    lib.execute_batch("COMMIT").unwrap();
    lsr_core::Checkpointer::checkpoint(&mut live, lsr_core::CheckpointMode::Passive).unwrap();
    let full = agree("backfilled", region, &mut live);
    assert_eq!(full.backfilled, full.mx_frame);
    let ran = checkpoint(&app, "RESTART");
    agree(&format!("RESTART unpinned {ran:?}"), region, &mut live);
    commit(&app, 4);
    let restarted = agree("commit after restart", region, &mut live);
    assert_ne!(
        (restarted.salt1, restarted.salt2),
        (start.salt1, start.salt2)
    );
    assert!(restarted.mx_frame < full.mx_frame, "the log began again");

    begin_read(&lib);
    let region = map(&lib).unwrap();
    agree("library read retaken", region, &mut live);
    let ran = checkpoint(&app, "TRUNCATE");
    agree(&format!("TRUNCATE while pinned {ran:?}"), region, &mut live);
}

/// No descriptor on the database, `-wal` or `-shm` is opened or closed by mapping and reading,
/// across commits and a checkpoint; and the process's POSIX locks on both files are unchanged
/// (a closed descriptor would have dropped them, which is how a transient one would show).
#[test]
fn mapping_and_reading_opens_and_closes_no_descriptor() {
    let (dir, path) = fresh_db();
    let shm = suffixed(&path, "-shm");
    let app = connect(&path);
    let lib = connect(&path);
    begin_read(&lib);
    // The application's first write opens its own `-wal` descriptor; take that before the count.
    commit(&app, -1);
    let fds = descriptors_under(dir.path());
    let locks = (posix_locks_on(&path), posix_locks_on(&shm));
    eprintln!("descriptors {fds:?}");
    eprintln!("locks db {:?} shm {:?}", locks.0, locks.1);
    assert!(!locks.1.is_empty(), "the read transaction holds -shm locks");

    for n in 0..50 {
        let region = map(&lib).unwrap();
        commit(&app, n);
        read_header(region).unwrap();
    }
    checkpoint(&app, "PASSIVE");
    read_header(map(&lib).unwrap()).unwrap();

    assert_eq!(descriptors_under(dir.path()), fds);
    assert_eq!((posix_locks_on(&path), posix_locks_on(&shm)), locks);
}

/// The hazard the global constraint names, measured on `LiveSqlite` as it is today: its
/// `committed` opens and closes `-shm`, and the close drops every POSIX lock the process holds
/// on that file, the read transaction's read-mark lock and the DMS lock included.
#[test]
fn live_sqlite_file_read_drops_the_process_shm_locks() {
    let (_dir, path) = fresh_db();
    let shm = suffixed(&path, "-shm");
    let mut live = LiveSqlite::open(&path).unwrap();
    let held = posix_locks_on(&shm);
    eprintln!("before LiveSqlite::committed: {held:?}");
    assert!(!held.is_empty());
    live.committed().unwrap();
    let after = posix_locks_on(&shm);
    eprintln!("after LiveSqlite::committed: {after:?}");
    assert!(
        after.is_empty(),
        "closing a -shm descriptor dropped the locks SQLite holds"
    );
}

/// The fallback the plan names: one `-shm` descriptor opened at start and never closed. Its
/// reads equal the mapping's, and holding it changes no lock.
#[test]
fn one_long_lived_shm_descriptor_reads_the_same_header() {
    use std::io::{Read, Seek, SeekFrom};
    let (_dir, path) = fresh_db();
    let shm = suffixed(&path, "-shm");
    let app = connect(&path);
    let lib = connect(&path);
    begin_read(&lib);
    let region = map(&lib).unwrap();
    let locks = posix_locks_on(&shm);
    let mut file = fs::File::open(&shm).unwrap();
    for n in 0..5 {
        commit(&app, n);
        let mut buf = [0_u8; 100];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut buf).unwrap();
        let words: Vec<AtomicU32> = buf
            .as_chunks::<4>()
            .0
            .iter()
            .map(|w| AtomicU32::new(u32::from_ne_bytes(*w)))
            .collect();
        assert_eq!(read_header(&words), read_header(region));
    }
    assert_eq!(posix_locks_on(&shm), locks);
    // The descriptor outlives every connection: closing it earlier would drop their locks.
    drop(lib);
    drop(app);
    drop(file);
}

#[test]
fn the_probe_refuses_every_connection_whose_wal_index_is_not_mapped() {
    let (_dir, path) = fresh_db();
    let lib = connect(&path);
    assert_eq!(map(&lib).err(), Some(Refusal::NoReadTransaction));

    let exclusive = connect(&path);
    exclusive
        .pragma_update(None, "locking_mode", "exclusive")
        .unwrap();
    begin_read(&exclusive);
    assert_eq!(
        map(&exclusive).err(),
        Some(Refusal::NotNormalLocking("exclusive".to_owned()))
    );
    drop(exclusive);

    let dir = TempDir::new().unwrap();
    let rollback_path = dir.path().join("rollback");
    let rollback = Connection::open(&rollback_path).unwrap();
    rollback
        .execute_batch("CREATE TABLE _litestream_seq (id INTEGER)")
        .unwrap();
    begin_read(&rollback);
    assert_eq!(
        map(&rollback).err(),
        Some(Refusal::NotWal("delete".to_owned()))
    );
    assert!(
        !suffixed(&rollback_path, "-shm").exists(),
        "the refusal touched nothing"
    );
}

/// What the guard prevents: `xShmMap` on a connection whose WAL never mapped `-shm` opens
/// (and creates) the file itself, a descriptor this library would then have to close.
#[test]
fn unguarded_shm_map_on_a_rollback_connection_opens_a_descriptor() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("rollback");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    let before = descriptors_under(dir.path());
    let file = file_pointer(&conn).unwrap();
    let shm_map = shm_map_of(file).unwrap();
    let mut region: *mut c_void = ptr::null_mut();
    // SAFETY: the method belongs to `file`; region 0 is asked for without extending.
    let rc = unsafe { shm_map(file.as_ptr(), 0, REGION_BYTES, 0, &raw mut region) };
    let after = descriptors_under(dir.path());
    eprintln!(
        "rc {rc} region null {} fds before {before:?} after {after:?}",
        region.is_null()
    );
    assert_eq!(rc, ffi::SQLITE_OK);
    assert!(
        region.is_null(),
        "a new -shm is empty, so no region 0 without extend"
    );
    assert_eq!(after.len(), before.len().wrapping_add(1));
    assert!(after.values().any(|p| p.ends_with("rollback-shm")));
    // SAFETY: `file` is the live main-database file of `conn`; `pMethods` is its first field.
    let methods = unsafe { (*file.as_ptr()).pMethods };
    let unmap_at = methods
        .wrapping_byte_add(offset_of!(ffi::sqlite3_io_methods, xShmUnmap))
        .cast::<Option<ShmUnmap>>();
    // SAFETY: version 2 or later, checked by `shm_map_of`, so the table has `xShmUnmap`.
    let unmap = unsafe { unmap_at.read() }.unwrap();
    // SAFETY: undoes the map above on the file's own method (`unixClose` asserts `pShm` is
    // null, `sqlite3.c:43021`).
    assert_eq!(unsafe { unmap(file.as_ptr(), 1) }, ffi::SQLITE_OK);
}

#[test]
fn missing_methods_are_refused_before_any_call() {
    let mut no_methods = ffi::sqlite3_file {
        pMethods: ptr::null(),
    };
    assert_eq!(
        shm_map_of(NonNull::from(&mut no_methods)).err(),
        Some(Refusal::FileNotOpen)
    );
    // SAFETY: every field is an integer or an `Option` of a function pointer; zero is valid.
    let mut methods: ffi::sqlite3_io_methods = unsafe { std::mem::zeroed() };
    for version in [0, 1] {
        methods.iVersion = version;
        let mut file = ffi::sqlite3_file {
            pMethods: &raw const methods,
        };
        assert_eq!(
            shm_map_of(NonNull::from(&mut file)).err(),
            Some(Refusal::OldIoMethods { version })
        );
    }
    methods.iVersion = 3;
    let mut file = ffi::sqlite3_file {
        pMethods: &raw const methods,
    };
    assert_eq!(
        shm_map_of(NonNull::from(&mut file)).err(),
        Some(Refusal::NoShmMap)
    );
}

#[test]
fn header_checks_refuse_what_sqlite_refuses() {
    let zeroed: Vec<AtomicU32> = (0..REGION_WORDS).map(|_| AtomicU32::new(0)).collect();
    assert_eq!(read_header(&zeroed), Err(NotReady::Uninitialised));

    let (_dir, path) = fresh_db();
    let lib = connect(&path);
    begin_read(&lib);
    let region = map(&lib).unwrap();
    let copy: Vec<AtomicU32> = region
        .iter()
        .take(32)
        .map(|w| AtomicU32::new(w.load(Ordering::Relaxed)))
        .collect();
    assert_eq!(read_header(&copy), read_header(region));
    copy[COPY_WORDS + 4].fetch_add(1, Ordering::Relaxed);
    assert_eq!(read_header(&copy), Err(NotReady::Torn));
    copy[4].fetch_add(1, Ordering::Relaxed);
    assert_eq!(read_header(&copy), Err(NotReady::BadChecksum));
    copy[0].store(1, Ordering::Relaxed);
    copy[COPY_WORDS].store(1, Ordering::Relaxed);
    assert_eq!(read_header(&copy), Err(NotReady::UnknownVersion(1)));
}

/// "No WAL yet": a WAL-mode database whose last connection closed (SQLite removes `-wal` and
/// `-shm`). The first read transaction runs recovery, which builds the header; both reads
/// then see zero frames.
#[test]
fn a_database_with_no_wal_yet_maps_an_initialised_empty_header() {
    let (_dir, path) = fresh_db();
    assert!(
        !suffixed(&path, "-shm").exists(),
        "the last close removed it"
    );
    let lib = connect(&path);
    begin_read(&lib);
    let region = map(&lib).unwrap();
    // LiveSqlite's open writes nothing here: the tables exist and the mode is already WAL.
    let mut live = LiveSqlite::open(&path).unwrap();
    let h = agree("no WAL yet", region, &mut live);
    assert_eq!((h.mx_frame, h.backfilled), (0, 0));
}

/// Reads while another thread's connection rewrites the header: every read is a checked
/// header or `NotReady`, and the checked ones never go backwards.
#[test]
fn reads_racing_a_writer_are_consistent_or_retryable() {
    const COMMITS: i64 = 2_000;
    let (_dir, path) = fresh_db();
    // A first frame, so the salts are set and the read mark is above 0.
    let first = connect(&path);
    commit(&first, -1);
    let lib = connect(&path);
    begin_read(&lib);
    let region = map(&lib).unwrap();
    let done = AtomicBool::new(false);
    let mut tally: BTreeMap<String, u64> = BTreeMap::new();
    let mut last = read_header(region).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| {
            let writer = connect(&path);
            writer.pragma_update(None, "synchronous", "OFF").unwrap();
            for n in 0..COMMITS {
                commit(&writer, n);
            }
            done.store(true, Ordering::Release);
        });
        while !done.load(Ordering::Acquire) {
            let key = match read_header(region) {
                Ok(h) => {
                    assert!(h.change >= last.change && h.mx_frame >= last.mx_frame);
                    assert_eq!((h.salt1, h.salt2), (last.salt1, last.salt2));
                    last = h;
                    "ok".to_owned()
                }
                Err(e) => format!("{e:?}"),
            };
            let count = tally.entry(key).or_default();
            *count = count.wrapping_add(1);
        }
    });
    let end = read_header(region).unwrap();
    eprintln!("tally {tally:?}; end {end:?}");
    assert!(end.change >= last.change);
    assert_eq!(
        tally.get("BadChecksum"),
        None,
        "equal copies were always whole"
    );
    assert_eq!(tally.get("Uninitialised"), None);
}
