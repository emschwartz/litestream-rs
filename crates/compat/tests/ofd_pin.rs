//! The pure-Rust pin's two Linux questions (#9; answers in `docs/spikes.md`, "Phase 3 spikes";
//! background in `docs/notes/pure-rust-pin.md`, sections 4A and 5).
//!
//! A pin held without a SQLite connection is a shared lock on a `-shm` read-mark byte
//! (`120 + 3 + slot`), taken through a descriptor of our own. SQLite in the same process keeps
//! per-process lock counts and calls `fcntl(F_SETLK)` on its own descriptor, and classic POSIX
//! record locks never conflict within one process, so only an open-file-description lock
//! (`F_OFD_SETLK`) can be seen by it. The probes take the pin as the pure-Rust pin would: an
//! exclusive OFD lock on the slot, the slot's mark written as the header's `mxFrame`, then a
//! downgrade to shared. They then ask SQLite's own checkpoints and writers what they see.
//!
//! Second question: whether the bundled build has `SQLITE_ENABLE_SETLK_TIMEOUT`, under which
//! SQLite could block in `F_SETLKW` on our lock instead of reporting busy.
//!
//! Measurement only; the pin itself is not built (it is deferred, see
//! `docs/plans/phase-3-syncing.md`, "Task 2: deferred").

#![cfg(target_os = "linux")]
#![expect(
    clippy::unwrap_used,
    reason = "the probe's helpers are test code; a failure there is the test failing"
)]
#![expect(
    unsafe_code,
    reason = "the probes call fcntl with OFD commands, which no safe crate in the workspace exposes"
)]

use std::ffi::{CStr, c_int, c_short};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use rusqlite::{Connection, ffi};
use tempfile::TempDir;

/// First wal-index lock byte (`UNIX_SHM_BASE`, `sqlite3.c:45117`).
const SHM_BASE: i64 = 120;
/// `WAL_READ_LOCK(0)` is lock 3 (`sqlite3.c:67170`).
const READ_LOCK_0: i64 = 3;
/// The dead-man-switch byte (`UNIX_SHM_DMS`), held shared by every process using `-shm`.
const SHM_DMS: i64 = 128;
/// `WalCkptInfo.aReadMark[0]` is byte 100 of `-shm` (after two 48-byte headers and `nBackfill`).
const READ_MARK_0: u64 = 100;
/// `WalIndexHdr.mxFrame` is byte 16 of header copy 0.
const MX_FRAME: u64 = 16;
/// The pure-Rust pin's slot: any of 1 to 4 blocks a WAL restart; 0 does not (walformat.html).
const SLOT: i64 = 1;

/// A record lock taken through `fcntl`, either kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// `F_OFD_SETLK`: owned by the open file description, conflicts within the process.
    Ofd,
    /// `F_SETLK`: owned by the process, never conflicts with the process's other locks.
    Posix,
}

fn flock(ty: c_int, start: i64, len: i64) -> libc::flock {
    // SAFETY: `flock` is a plain C struct of integers; all-zero is a valid value, and `l_pid`
    // must be zero for OFD commands.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = c_short::try_from(ty).unwrap();
    lock.l_whence = c_short::try_from(libc::SEEK_SET).unwrap();
    lock.l_start = start;
    lock.l_len = len;
    lock
}

/// One non-blocking (`wait == false`) or blocking lock call on `file`.
fn lock(file: &File, kind: Kind, ty: c_int, start: i64, len: i64, wait: bool) -> io::Result<()> {
    let cmd = match (kind, wait) {
        (Kind::Ofd, false) => libc::F_OFD_SETLK,
        (Kind::Ofd, true) => libc::F_OFD_SETLKW,
        (Kind::Posix, false) => libc::F_SETLK,
        (Kind::Posix, true) => libc::F_SETLKW,
    };
    let lock = flock(ty, start, len);
    // SAFETY: the descriptor is open for the life of `file`, `cmd` is a lock command, and
    // `lock` is a valid `flock` that outlives the call.
    let rc = unsafe { libc::fcntl(file.as_raw_fd(), cmd, &raw const lock) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn read_lock_byte(slot: i64) -> i64 {
    SHM_BASE
        .checked_add(READ_LOCK_0)
        .and_then(|b| b.checked_add(slot))
        .unwrap()
}

fn read_u32(file: &File, offset: u64) -> u32 {
    let mut word = [0_u8; 4];
    file.read_exact_at(&mut word, offset).unwrap();
    u32::from_ne_bytes(word)
}

fn read_mark_offset(slot: i64) -> u64 {
    let slot = u64::try_from(slot).unwrap();
    slot.checked_mul(4)
        .and_then(|b| b.checked_add(READ_MARK_0))
        .unwrap()
}

fn read_mark(shm: &File, slot: i64) -> u32 {
    read_u32(shm, read_mark_offset(slot))
}

/// The pin as the pure-Rust pin would take it: exclusive on the slot, mark := `mxFrame`,
/// downgrade to shared (an OFD lock is replaced atomically). Returns the mark.
fn pin(shm: &File, kind: Kind, slot: i64) -> u32 {
    let byte = read_lock_byte(slot);
    lock(shm, kind, libc::F_WRLCK, byte, 1, false).unwrap();
    let mx_frame = read_u32(shm, MX_FRAME);
    let offset = read_mark_offset(slot);
    shm.write_all_at(&mx_frame.to_ne_bytes(), offset).unwrap();
    lock(shm, kind, libc::F_RDLCK, byte, 1, false).unwrap();
    mx_frame
}

fn unpin(shm: &File, kind: Kind, slot: i64) {
    lock(shm, kind, libc::F_UNLCK, read_lock_byte(slot), 1, false).unwrap();
}

fn connect(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(std::time::Duration::ZERO).unwrap();
    conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
    conn
}

/// `(busy, log, checkpointed)` as `PRAGMA wal_checkpoint` reports it.
fn checkpoint(conn: &Connection, mode: &str) -> (i64, i64, i64) {
    conn.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })
    .unwrap()
}

fn commit(conn: &Connection, n: i64) {
    conn.execute("INSERT INTO t (v) VALUES (?1)", [n]).unwrap();
}

fn count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(1) FROM t", [], |row| row.get(0))
        .unwrap()
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// The WAL header's two salts (bytes 16..24), read from the file (SQLite holds no lock on it).
fn salts(path: &Path) -> [u8; 8] {
    let bytes = fs::read(suffixed(path, "-wal")).unwrap();
    bytes
        .get(16..24)
        .and_then(|salts| salts.try_into().ok())
        .unwrap()
}

fn wal_len(path: &Path) -> u64 {
    fs::metadata(suffixed(path, "-wal")).unwrap().len()
}

/// A WAL database with one table and four committed frames, the host connection that wrote
/// them (kept open so `-wal` and `-shm` stay), and our own read-write `-shm` descriptor.
fn fresh_db() -> (TempDir, PathBuf, Connection, File) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    let host = connect(&path);
    let mode: String = host
        .query_row("PRAGMA journal_mode = wal", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    host.execute_batch("CREATE TABLE t (v INTEGER)").unwrap();
    commit(&host, 1);
    commit(&host, 2);
    let shm = OpenOptions::new()
        .read(true)
        .write(true)
        .open(suffixed(&path, "-shm"))
        .unwrap();
    (dir, path, host, shm)
}

/// This process's locks on `path`'s inode from `/proc/locks`, as `KIND TYPE START END`.
/// OFD locks show as `OFDLCK` with pid -1; blocked waiters as lines with `->`.
fn locks_on(path: &Path) -> Vec<String> {
    let inode = format!(":{}", fs::metadata(path).unwrap().ino());
    let pid = std::process::id().to_string();
    let mut locks: Vec<String> = fs::read_to_string("/proc/locks")
        .unwrap()
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (waiter, rest) = match fields.get(1) {
                Some(&"->") => (true, fields.get(2..)?),
                _ => (false, fields.get(1..)?),
            };
            let ours = (rest.first() == Some(&"OFDLCK") || rest.get(3) == Some(&pid.as_str()))
                && rest.get(4).is_some_and(|f| f.ends_with(&inode));
            let words = [*rest.first()?, *rest.get(2)?, *rest.get(5)?, *rest.get(6)?];
            ours.then(|| format!("{}{}", if waiter { "-> " } else { "" }, words.join(" ")))
        })
        .collect();
    locks.sort();
    locks
}

#[test]
fn an_ofd_pin_on_a_read_mark_makes_restart_and_truncate_busy() {
    let (_dir, path, host, shm) = fresh_db();
    let outside = connect(&path);
    let mark = pin(&shm, Kind::Ofd, SLOT);
    assert_eq!(
        mark, 4,
        "four frames: CREATE TABLE (two pages) and two inserts"
    );
    let before = salts(&path);

    // The pin is at the WAL's end, so PASSIVE may backfill everything; RESTART and TRUNCATE
    // must then wait for readers on slots 1..4, and SQLite sees ours.
    assert_eq!(checkpoint(&outside, "PASSIVE"), (0, 4, 4));
    assert_eq!(checkpoint(&outside, "RESTART"), (1, 4, 4));
    let len = wal_len(&path);
    assert_eq!(checkpoint(&outside, "TRUNCATE"), (1, 4, 4));
    assert_eq!(wal_len(&path), len, "TRUNCATE left the WAL's length alone");

    // A writer on a fully backfilled WAL would restart it; with the pin it appends instead.
    commit(&host, 3);
    assert_eq!(salts(&path), before, "the writer did not restart the WAL");
    // Frames past the pin are never backfilled: the checkpoint stops at the mark.
    assert_eq!(checkpoint(&outside, "PASSIVE"), (0, 5, 4));
    assert_eq!(read_mark(&shm, SLOT), mark, "SQLite left our mark alone");

    // Released, everything proceeds as normal: RESTART succeeds, the next commit restarts.
    unpin(&shm, Kind::Ofd, SLOT);
    assert_eq!(checkpoint(&outside, "RESTART"), (0, 5, 5));
    commit(&host, 4);
    assert_ne!(
        salts(&path),
        before,
        "the WAL restarted once the pin was gone"
    );
    drop((outside, host));
}

#[test]
fn a_posix_lock_from_our_own_descriptor_pins_nothing() {
    let (_dir, path, host, shm) = fresh_db();
    let outside = connect(&path);
    pin(&shm, Kind::Posix, SLOT);
    let before = salts(&path);
    assert!(
        locks_on(&suffixed(&path, "-shm")).contains(&"POSIX READ 124 124".to_owned()),
        "the process holds the lock"
    );
    // SQLite's exclusive lock on the same byte is the process's own: it upgrades ours.
    assert_eq!(checkpoint(&outside, "RESTART"), (0, 4, 4));
    commit(&host, 3);
    assert_ne!(salts(&path), before, "the WAL restarted under the 'pin'");
    drop((outside, host));
}

#[test]
fn the_ofd_pin_survives_another_descriptor_closing_and_the_posix_locks_do_not() {
    let (_dir, path, host, shm) = fresh_db();
    let shm_path = suffixed(&path, "-shm");
    // The host's reader holds read mark 0's byte or a slot's, and the DMS byte, as POSIX locks.
    host.execute_batch("BEGIN").unwrap();
    assert_eq!(count(&host), 2);
    pin(&shm, Kind::Ofd, 2);
    let held = locks_on(&shm_path);
    assert!(held.contains(&"OFDLCK READ 125 125".to_owned()), "{held:?}");
    assert!(held.contains(&"POSIX READ 128 128".to_owned()), "{held:?}");

    // close(open(-shm)), the howtocorrupt.html hazard.
    drop(File::open(&shm_path).unwrap());
    let after = locks_on(&shm_path);
    assert_eq!(
        after,
        vec!["OFDLCK READ 125 125".to_owned()],
        "only the OFD lock is left"
    );

    // SQLite still sees the pin: RESTART from a new connection is busy on slot 2.
    let outside = connect(&path);
    let (busy, _, _) = checkpoint(&outside, "RESTART");
    assert_eq!(busy, 1);
    host.execute_batch("COMMIT").unwrap();
    drop((outside, host));
    drop(shm);
}

/// The last host connection closing: SQLite takes an EXCLUSIVE lock on the database file, which
/// a `-shm` read-mark lock does not stop, and then checkpoints in exclusive mode, where its
/// read-mark locks are no-ops (`walLockExclusive`, `sqlite3.c`), and deletes `-wal`.
#[test]
fn the_hosts_last_close_ignores_a_read_mark_pin_but_not_a_pin_on_the_dms_byte() {
    for hold_dms in [false, true] {
        let (_dir, path, host, shm) = fresh_db();
        pin(&shm, Kind::Ofd, SLOT);
        if hold_dms {
            lock(&shm, Kind::Ofd, libc::F_RDLCK, SHM_DMS, 1, false).unwrap();
        }
        commit(&host, 3);
        drop(host);
        let wal_left = suffixed(&path, "-wal").exists();
        let shm_left = suffixed(&path, "-shm").exists();
        assert_eq!(
            (wal_left, shm_left),
            (hold_dms, hold_dms),
            "hold_dms = {hold_dms}"
        );
        // Without the DMS lock the frame after the pin went into the database file and the
        // WAL is gone, so a pin reader's view of the database at frame 3 no longer exists.
        let reopened = connect(&path);
        assert_eq!(count(&reopened), 3);
        drop(reopened);
        drop(shm);
    }
}

fn compile_option_used(name: &CStr) -> bool {
    // SAFETY: `name` is a NUL-terminated string that outlives the call; the function only reads
    // it and needs no open connection.
    unsafe { ffi::sqlite3_compileoption_used(name.as_ptr()) != 0 }
}

#[test]
fn the_bundled_sqlite_is_built_without_setlk_timeout() {
    // `LIBSQLITE3_FLAGS` is the only way the libsqlite3-sys build script adds options; the
    // flags it always passes (build.rs, `-DSQLITE_CORE` and the rest) do not include this one.
    let requested =
        option_env!("LIBSQLITE3_FLAGS").is_some_and(|flags| flags.contains("SETLK_TIMEOUT"));
    let used = compile_option_used(c"ENABLE_SETLK_TIMEOUT");
    println!("ENABLE_SETLK_TIMEOUT used: {used}, requested through LIBSQLITE3_FLAGS: {requested}");
    assert_eq!(used, requested);
    assert!(
        compile_option_used(c"THREADSAFE=1"),
        "the probe reads options"
    );
}

/// SQLite's busy handler with a timeout, and, in a `SQLITE_ENABLE_SETLK_TIMEOUT` build, its
/// lock timeout too (`sqlite3_busy_timeout` sets both): the checkpoint still reports busy.
#[test]
fn with_a_busy_timeout_the_checkpoint_still_reports_busy() {
    let (_dir, path, host, shm) = fresh_db();
    let outside = connect(&path);
    outside
        .busy_timeout(std::time::Duration::from_millis(1))
        .unwrap();
    pin(&shm, Kind::Ofd, SLOT);
    assert_eq!(checkpoint(&outside, "RESTART"), (1, 4, 4));
    assert_eq!(checkpoint(&outside, "TRUNCATE"), (1, 4, 4));
    drop((outside, host));
}

/// What a blocking caller would do: the kernel queues a POSIX `F_SETLKW` behind an OFD lock in
/// the same process (it shows in `/proc/locks` as a waiter) and grants it on release.
#[test]
fn a_blocking_posix_lock_waits_behind_an_ofd_lock_in_the_same_process() {
    let (_dir, path, host, shm) = fresh_db();
    let shm_path = suffixed(&path, "-shm");
    pin(&shm, Kind::Ofd, SLOT);
    let theirs = File::options()
        .read(true)
        .write(true)
        .open(&shm_path)
        .unwrap();
    // Non-blocking, it is refused at once, as SQLite's F_SETLK is.
    let refused = lock(&theirs, Kind::Posix, libc::F_WRLCK, 124, 1, false).unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(libc::EAGAIN));

    let (done, granted) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let result = lock(&theirs, Kind::Posix, libc::F_WRLCK, 124, 1, true);
        done.send(result.map_err(|e| e.kind())).unwrap();
        theirs
    });
    let queued = "-> POSIX WRITE 124 124".to_owned();
    let mut seen = false;
    for _ in 0..1_000_000 {
        if locks_on(&shm_path).contains(&queued) {
            seen = true;
            break;
        }
        thread::yield_now();
    }
    assert!(seen, "the blocking request never queued");
    assert!(
        granted.try_recv().is_err(),
        "it waits while the pin is held"
    );
    unpin(&shm, Kind::Ofd, SLOT);
    assert_eq!(granted.recv().unwrap(), Ok(()));
    // Its descriptor is closed only after the host's: closing it drops the host's POSIX locks.
    let theirs = waiter.join().unwrap();
    drop(host);
    drop(theirs);
}
