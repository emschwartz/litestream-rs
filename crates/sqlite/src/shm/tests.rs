//! The guard, the mapping and the header checks, on raw connections. These carry over the
//! answers of the wal-index spike (`docs/spikes.md`, "Phase 3 spikes") that need the
//! crate's internals; the ones visible through `Sqlite` are in `tests/`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;

use tempfile::TempDir;

use super::*;
use crate::pages::file_pointer;

fn connect(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn
}

fn begin_read(conn: &Connection) {
    conn.execute_batch("BEGIN").unwrap();
    let _n: i64 = conn
        .query_row("SELECT COUNT(1) FROM _litestream_seq", [], |row| row.get(0))
        .unwrap();
}

fn commit(conn: &Connection, n: i64) {
    conn.execute("INSERT INTO t (v) VALUES (?1)", [n]).unwrap();
}

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
         CREATE TABLE t (v INTEGER);",
    )
    .unwrap();
    (dir, path)
}

fn guarded_map(conn: &Connection, path: &Path) -> Result<Region, OpenError> {
    guard(conn, path)?;
    let file = FileHandle::new(file_pointer(conn, rusqlite::MAIN_DB)?)?;
    Ok(map(&file)?)
}

fn refusal(result: Result<Region, OpenError>) -> Refusal {
    match result {
        Err(OpenError::Refused(refusal)) => refusal,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn the_mapping_is_the_one_every_connection_in_the_process_uses() {
    let (_dir, path) = fresh_db();
    let lib = connect(&path);
    let other = connect(&path);
    begin_read(&lib);
    begin_read(&other);
    let ours = guarded_map(&lib, &path).unwrap();
    let theirs = guarded_map(&other, &path).unwrap();
    assert_eq!(
        ours.words, theirs.words,
        "one shm node per inode, one region 0"
    );
    other.execute_batch("COMMIT").unwrap();

    let before = read_header(ours.words()).unwrap();
    commit(&other, 1);
    let after = read_header(ours.words()).unwrap();
    assert!(
        after.mx_frame > before.mx_frame,
        "{before:?} then {after:?}"
    );
    assert_eq!(after.change, before.change.wrapping_add(1));
}

#[test]
fn the_guard_refuses_every_connection_whose_wal_index_is_not_mapped() {
    let (_dir, path) = fresh_db();
    let lib = connect(&path);
    assert_eq!(
        refusal(guarded_map(&lib, &path)),
        Refusal::NoReadTransaction
    );

    let exclusive = connect(&path);
    exclusive
        .pragma_update(None, "locking_mode", "exclusive")
        .unwrap();
    begin_read(&exclusive);
    assert_eq!(
        refusal(guarded_map(&exclusive, &path)),
        Refusal::NotNormalLocking {
            mode: "exclusive".to_owned()
        }
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
        refusal(guarded_map(&rollback, &rollback_path)),
        Refusal::NotWal {
            mode: "delete".to_owned()
        }
    );
    assert!(
        !dir.path().join("rollback-shm").exists(),
        "the refusal touched nothing"
    );
}

#[cfg(target_os = "linux")]
fn descriptors_under(dir: &Path) -> BTreeMap<String, PathBuf> {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let target = std::fs::read_link(entry.path()).ok()?;
            target
                .starts_with(dir)
                .then(|| (entry.file_name().to_string_lossy().into_owned(), target))
        })
        .collect()
}

#[cfg(target_os = "linux")]
type XShmUnmap = unsafe extern "C" fn(*mut ffi::sqlite3_file, c_int) -> c_int;

/// What the guard prevents: `xShmMap` on a connection whose WAL never mapped `-shm` opens (and
/// creates) the file itself, a descriptor this library would then have to close.
#[cfg(target_os = "linux")]
#[test]
#[expect(unsafe_code, reason = "undoing the unguarded map needs xShmUnmap")]
fn an_unguarded_map_on_a_rollback_connection_opens_a_descriptor() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("rollback");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    let before = descriptors_under(dir.path());
    let file = FileHandle::new(file_pointer(&conn, rusqlite::MAIN_DB).unwrap()).unwrap();
    assert_eq!(
        map(&file).err(),
        Some(Refusal::Unmapped),
        "a new -shm is empty"
    );
    let after = descriptors_under(dir.path());
    assert_eq!(after.len(), before.len().wrapping_add(1));
    assert!(after.values().any(|p| p.ends_with("rollback-shm")));
    // SAFETY: `file` is the live main-database file of `conn`; `pMethods` is its first field.
    let methods = unsafe { (*file.file.as_ptr()).pMethods };
    let unmap_at = methods
        .wrapping_byte_add(std::mem::offset_of!(ffi::sqlite3_io_methods, xShmUnmap))
        .cast::<Option<XShmUnmap>>();
    // SAFETY: the unix VFS's table is version 3, so it has `xShmUnmap`.
    let unmap = unsafe { unmap_at.read() }.unwrap();
    // SAFETY: undoes the map above on the file's own method (`unixClose` asserts `pShm` is null).
    assert_eq!(unsafe { unmap(file.file.as_ptr(), 1) }, ffi::SQLITE_OK);
}

extern "C" fn fake_read(_: *mut ffi::sqlite3_file, _: *mut c_void, _: c_int, _: i64) -> c_int {
    ffi::SQLITE_IOERR
}

extern "C" fn fake_size(_: *mut ffi::sqlite3_file, _: *mut i64) -> c_int {
    ffi::SQLITE_IOERR
}

extern "C" fn map_fails(
    _: *mut ffi::sqlite3_file,
    _: c_int,
    _: c_int,
    _: c_int,
    _: *mut *mut c_void,
) -> c_int {
    ffi::SQLITE_READONLY
}

extern "C" fn map_nothing(
    _: *mut ffi::sqlite3_file,
    _: c_int,
    _: c_int,
    _: c_int,
    _: *mut *mut c_void,
) -> c_int {
    ffi::SQLITE_OK
}

static ODD: [u32; 2] = [0, 0];

#[expect(unsafe_code, reason = "a fake xShmMap writes its out-pointer")]
extern "C" fn map_misaligned(
    _: *mut ffi::sqlite3_file,
    _: c_int,
    _: c_int,
    _: c_int,
    out: *mut *mut c_void,
) -> c_int {
    let odd = ODD.as_ptr().cast::<u8>().wrapping_add(1).cast_mut();
    // SAFETY: `map` passes a pointer to a live local.
    unsafe { out.write(odd.cast::<c_void>()) };
    ffi::SQLITE_OK
}

/// A version 2 io_methods table with the given methods and every other field null.
#[expect(
    unsafe_code,
    reason = "a synthetic io_methods table for the refusal tests"
)]
fn table(
    read: bool,
    size: bool,
    shm_map: Option<crate::pages::XShmMap>,
) -> ffi::sqlite3_io_methods {
    // SAFETY: every field is an integer or an `Option` of a function pointer; zero is valid.
    let mut methods: ffi::sqlite3_io_methods = unsafe { std::mem::zeroed() };
    methods.iVersion = 2;
    if read {
        methods.xRead = Some(fake_read);
    }
    if size {
        methods.xFileSize = Some(fake_size);
    }
    methods.xShmMap = shm_map;
    methods
}

fn handle_of(
    methods: &ffi::sqlite3_io_methods,
) -> (Box<ffi::sqlite3_file>, Result<FileHandle, Refusal>) {
    let mut file = Box::new(ffi::sqlite3_file {
        pMethods: std::ptr::from_ref(methods),
    });
    let handle = FileHandle::new(NonNull::from(file.as_mut()));
    (file, handle)
}

#[test]
fn missing_methods_and_failed_maps_are_refused() {
    let missing = |read, size, shm| {
        let methods = table(read, size, shm);
        handle_of(&methods).1.err()
    };
    assert_eq!(
        missing(false, true, Some(map_fails)),
        Some(Refusal::MissingMethod { name: "xRead" })
    );
    assert_eq!(
        missing(true, false, Some(map_fails)),
        Some(Refusal::MissingMethod { name: "xFileSize" })
    );
    assert_eq!(
        missing(true, true, None),
        Some(Refusal::MissingMethod { name: "xShmMap" })
    );
    let cases: [(crate::pages::XShmMap, Refusal); 3] = [
        (
            map_fails,
            Refusal::MapFailed {
                rc: ffi::SQLITE_READONLY,
            },
        ),
        (map_nothing, Refusal::Unmapped),
        (map_misaligned, Refusal::Misaligned),
    ];
    for (fake, refused) in cases {
        let methods = table(true, true, Some(fake));
        let (_file, handle) = handle_of(&methods);
        assert_eq!(map(&handle.unwrap()).err(), Some(refused));
    }
}

#[test]
fn header_checks_refuse_what_sqlite_refuses() {
    let zeroed: Vec<AtomicU32> = (0..REGION_WORDS).map(|_| AtomicU32::new(0)).collect();
    assert_eq!(read_header(&zeroed), Err(NotReady::Uninitialised));

    let (_dir, path) = fresh_db();
    let lib = connect(&path);
    begin_read(&lib);
    let region = guarded_map(&lib, &path).unwrap();
    let copy: Vec<AtomicU32> = region
        .words()
        .iter()
        .take(32)
        .map(|w| AtomicU32::new(w.load(Ordering::Relaxed)))
        .collect();
    assert_eq!(read_header(&copy), read_header(region.words()));
    copy[COPY_WORDS + 4].fetch_add(1, Ordering::Relaxed);
    assert_eq!(read_header(&copy), Err(NotReady::Torn));
    copy[4].fetch_add(1, Ordering::Relaxed);
    assert_eq!(read_header(&copy), Err(NotReady::BadChecksum));
    copy[0].store(1, Ordering::Relaxed);
    copy[COPY_WORDS].store(1, Ordering::Relaxed);
    assert_eq!(read_header(&copy), Err(NotReady::UnknownVersion(1)));
}

/// Reads while another thread's connection rewrites the header: every read is a checked header
/// or `NotReady`, and the checked ones never go backwards.
#[test]
fn reads_racing_a_writer_are_consistent_or_retryable() {
    const COMMITS: i64 = 2_000;
    let (_dir, path) = fresh_db();
    // A first frame, so the salts are set; the connection stays open so the WAL does too.
    let first = connect(&path);
    commit(&first, -1);
    let lib = connect(&path);
    begin_read(&lib);
    let region = guarded_map(&lib, &path).unwrap();
    let done = AtomicBool::new(false);
    let mut tally: BTreeMap<String, u64> = BTreeMap::new();
    let mut last = read_header(region.words()).unwrap();
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
            let key = match read_header(region.words()) {
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
    let end = read_header(region.words()).unwrap();
    assert!(end.change >= last.change);
    assert_eq!(
        tally.get("BadChecksum"),
        None,
        "equal copies were always whole: {tally:?}"
    );
    assert_eq!(tally.get("Uninitialised"), None);
}
