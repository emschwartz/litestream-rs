//! The checkpoint, the write barrier and the sequence bump.
//!
//! A PASSIVE or FULL checkpoint runs on the reader through `sqlite3_wal_checkpoint_v2`, after its
//! read transaction ends and before it is retaken, as Go defers the retake (`db.go:2805-2849`).
//! Ending and retaking is what moves the [`ReadLockHold`]. A TRUNCATE runs on the writer
//! connection instead and leaves the read transaction, and so the hold, as it was: the engine's
//! read is what makes a TRUNCATE busy when anything was committed since the hold began
//! (`lsr_core`'s checkpoint protocol). The C call, not the pragma, because its result
//! code tells a checkpoint that ran but was blocked by readers (`SQLITE_BUSY` with both frame
//! counts set: `busy` in the run) from one that never ran because another connection held the
//! checkpoint lock (`SQLITE_BUSY` with counts of -1: [`SourceError::Busy`]); the pragma reports
//! both as `busy = 1`.

use std::ffi::c_int;
use std::io;

use crate::capi::Handle;
use crate::error::{rc_error, source_error};
use crate::{Sqlite, ffi};
use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, SourceError};

fn wal_checkpoint(conn: Handle<'_>, mode: CheckpointMode) -> (c_int, c_int, c_int) {
    let mode = match mode {
        CheckpointMode::Passive => ffi::SQLITE_CHECKPOINT_PASSIVE,
        CheckpointMode::Truncate => ffi::SQLITE_CHECKPOINT_TRUNCATE,
        CheckpointMode::Full => ffi::SQLITE_CHECKPOINT_FULL,
    };
    conn.wal_checkpoint(mode)
}

/// How many descriptors this process holds on the file at `path`, `None` when they cannot be
/// listed. One copy of SQLite opens `-shm` once for all its connections in a process
/// (`unixOpenSharedMemory`) and keeps its locks in that one node, so a second descriptor is
/// another copy (or another opener) on the wal-index whose locks this copy cannot see.
#[cfg(unix)]
fn descriptors_on(path: &std::path::Path) -> Option<usize> {
    use std::os::unix::fs::MetadataExt;
    let target = std::fs::metadata(path).ok()?;
    let listed = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    let mut found = 0_usize;
    for entry in std::fs::read_dir(listed).ok()? {
        // A descriptor closed since the listing (the listing's own among them) names nothing.
        // macOS reports devfs's device for a `/dev/fd` entry, so there only the inode is
        // compared: a match on another file system's inode counts one too many, which costs a
        // snapshot, never a proof.
        if let Ok(meta) = std::fs::metadata(entry.ok()?.path())
            && (cfg!(target_os = "macos") || meta.dev() == target.dev())
            && meta.ino() == target.ino()
        {
            found = found.saturating_add(1);
        }
    }
    Some(found)
}

#[cfg(not(unix))]
const fn descriptors_on(_path: &std::path::Path) -> Option<usize> {
    None
}

/// Whether readers blocked the checkpoint, and its two frame counts; an error when it did not run.
pub(crate) fn sort_run(
    rc: c_int,
    log: c_int,
    checkpointed: c_int,
) -> Result<(bool, u32, u32), SourceError> {
    let busy = match rc & 0xff {
        ffi::SQLITE_OK => false,
        ffi::SQLITE_BUSY => true,
        _ => return Err(rc_error(rc)),
    };
    // -1 means it never ran: another connection held the checkpoint lock, or not WAL.
    let (Ok(log), Ok(checkpointed)) = (u32::try_from(log), u32::try_from(checkpointed)) else {
        return Err(if busy {
            SourceError::Busy
        } else {
            SourceError::Io(io::ErrorKind::InvalidData)
        });
    };
    Ok((busy, log, checkpointed))
}

impl Checkpointer for Sqlite {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.lock().begin_writer().map(Barrier::new)
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.lock().end_writer(barrier.id())
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let mut inner = self.lock();
        let (rc, log, checkpointed, one_sqlite, lost) = if mode == CheckpointMode::Truncate {
            let shm = inner.shm_path();
            let alone = descriptors_on(&shm) == Some(1);
            let (rc, log, checkpointed) = wal_checkpoint(inner.free_writer()?, mode);
            let one_sqlite = alone && descriptors_on(&shm) == Some(1);
            // The writer inside a transaction: SQLite ran nothing.
            if rc & 0xff == ffi::SQLITE_LOCKED {
                return Err(SourceError::Busy);
            }
            (rc, log, checkpointed, one_sqlite, false)
        } else {
            inner.end_read()?;
            let (rc, log, checkpointed) = wal_checkpoint(inner.reader.handle(), mode);
            // The read transaction comes back whether or not the checkpoint ran, as Go defers it.
            let lost = inner.begin_read().is_err();
            (rc, log, checkpointed, true, lost)
        };
        let (busy, log, checkpointed) = sort_run(rc, log, checkpointed)?;
        if lost {
            return Err(SourceError::ReadTxLost);
        }
        Ok(CheckpointRun {
            busy,
            log,
            checkpointed,
            epoch: inner.epoch,
            one_sqlite,
        })
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.lock()
            .free_writer()?
            .exec(
                "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1",
            )
            .map_err(|e| source_error(&e))
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_char, c_void};

    use rusqlite::Connection;

    use super::*;

    #[expect(
        unsafe_code,
        reason = "SQLite calls the authorizer through a C function pointer"
    )]
    extern "C" fn deny_rollback(
        _arg: *mut c_void,
        action: c_int,
        what: *const c_char,
        _two: *const c_char,
        _db: *const c_char,
        _trigger: *const c_char,
    ) -> c_int {
        if action != ffi::SQLITE_TRANSACTION || what.is_null() {
            return ffi::SQLITE_OK;
        }
        // SAFETY: for SQLITE_TRANSACTION SQLite passes the operation as a NUL-terminated string.
        let what = unsafe { CStr::from_ptr(what) };
        if what.to_bytes() == b"ROLLBACK" {
            ffi::SQLITE_DENY
        } else {
            ffi::SQLITE_OK
        }
    }

    /// Sets or clears the writer's authorizer on the raw handle (rusqlite before 0.40 refuses an
    /// authorizer on a handle it does not own).
    #[expect(unsafe_code, reason = "sqlite3_set_authorizer is a C call")]
    fn deny_rollbacks(sqlite: &Sqlite, on: bool) {
        let db = sqlite.lock().writer.handle().as_ptr();
        // SAFETY: the writer is open for the test and used by this thread alone; the callback is
        // a plain function with no state.
        let rc = unsafe {
            ffi::sqlite3_set_authorizer(db, on.then_some(deny_rollback), std::ptr::null_mut())
        };
        assert_eq!(rc, ffi::SQLITE_OK);
    }

    fn open() -> (tempfile::TempDir, std::path::PathBuf, Sqlite) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let app = Connection::open(&path).unwrap();
        app.execute_batch("PRAGMA journal_mode = WAL; CREATE TABLE t (v);")
            .unwrap();
        let sqlite = Sqlite::open(&path).unwrap();
        (dir, path, sqlite)
    }

    fn seq(path: &std::path::Path) -> Option<i64> {
        Connection::open(path)
            .unwrap()
            .query_row("SELECT seq FROM _litestream_seq WHERE id = 1", [], |row| {
                row.get(0)
            })
            .ok()
    }

    /// A release whose ROLLBACK fails leaves the writer's transaction open. The next bump must
    /// not run inside it, where the next lock's ROLLBACK would undo it unseen.
    #[test]
    fn a_bump_after_a_release_that_failed_is_committed() {
        let (_dir, path, mut sqlite) = open();
        let barrier = sqlite.begin_barrier().unwrap();
        deny_rollbacks(&sqlite, true);
        assert!(
            sqlite.end_barrier(barrier).is_err(),
            "the ROLLBACK was refused"
        );
        deny_rollbacks(&sqlite, false);
        sqlite.bump_seq().unwrap();
        assert_eq!(seq(&path), Some(1), "another connection sees the bump");
        let barrier = sqlite.begin_barrier().unwrap();
        sqlite.end_barrier(barrier).unwrap();
        assert_eq!(seq(&path), Some(1), "the next barrier's release kept it");
    }

    /// A bump while a lock is outstanding would run inside the lock's transaction: refused.
    #[test]
    fn a_bump_under_a_lock_is_refused() {
        let (_dir, path, mut sqlite) = open();
        let barrier = sqlite.begin_barrier().unwrap();
        assert_eq!(
            sqlite.bump_seq(),
            Err(SourceError::Io(io::ErrorKind::InvalidInput))
        );
        sqlite.end_barrier(barrier).unwrap();
        sqlite.bump_seq().unwrap();
        assert_eq!(seq(&path), Some(1));
    }

    #[test]
    fn checkpoint_results_sort_by_code_and_counts() {
        assert_eq!(sort_run(ffi::SQLITE_OK, 4, 4), Ok((false, 4, 4)));
        assert_eq!(sort_run(ffi::SQLITE_BUSY, 4, 1), Ok((true, 4, 1)));
        assert_eq!(sort_run(ffi::SQLITE_BUSY, -1, -1), Err(SourceError::Busy));
        assert_eq!(
            sort_run(ffi::SQLITE_OK, -1, -1),
            Err(SourceError::Io(io::ErrorKind::InvalidData))
        );
        assert_eq!(
            sort_run(ffi::SQLITE_FULL, -1, -1),
            Err(SourceError::DiskFull)
        );
        assert_eq!(
            sort_run(ffi::SQLITE_IOERR_WRITE, -1, -1),
            Err(SourceError::Io(io::ErrorKind::Other))
        );
    }
}
