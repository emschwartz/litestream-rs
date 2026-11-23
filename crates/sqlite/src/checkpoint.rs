//! The checkpoint, the write barrier, the write lock and the sequence bump.
//!
//! The checkpoint runs on the reader through `sqlite3_wal_checkpoint_v2`, after its read
//! transaction ends and before it is retaken, as Go defers the retake (`db.go:2805-2849`). Ending
//! and retaking is what moves the [`ReadLockHold`]. The C call, not the pragma, because its result
//! code tells a checkpoint that ran but was blocked by readers (`SQLITE_BUSY` with both frame
//! counts set: `busy` in the run) from one that never ran because another connection held the
//! checkpoint lock (`SQLITE_BUSY` with counts of -1: [`SourceError::Busy`]); the pragma reports
//! both as `busy = 1`.

use std::ffi::c_int;
use std::io;

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, SourceError, WriteLock};
use rusqlite::{Connection, ffi};

use crate::Sqlite;
use crate::error::{rc_error, source_error};
use crate::pages::handle;

/// SQLite's result code and its two frame counts.
#[expect(
    unsafe_code,
    reason = "sqlite3_wal_checkpoint_v2's frame counts on SQLITE_BUSY are lost by rusqlite's wrapper"
)]
fn wal_checkpoint(conn: &Connection, mode: CheckpointMode) -> (c_int, c_int, c_int) {
    let mode = match mode {
        CheckpointMode::Passive => ffi::SQLITE_CHECKPOINT_PASSIVE,
        CheckpointMode::Truncate => ffi::SQLITE_CHECKPOINT_TRUNCATE,
        CheckpointMode::Full => ffi::SQLITE_CHECKPOINT_FULL,
    };
    let (mut log, mut checkpointed) = (-1, -1);
    let db = handle(conn);
    // SAFETY: a live handle, a NUL-terminated schema name, and two `int`s it writes.
    let rc = unsafe {
        ffi::sqlite3_wal_checkpoint_v2(
            db,
            rusqlite::MAIN_DB.as_ptr(),
            mode,
            &raw mut log,
            &raw mut checkpointed,
        )
    };
    (rc, log, checkpointed)
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
        inner.end_read()?;
        let (rc, log, checkpointed) = wal_checkpoint(&inner.reader, mode);
        // The read transaction comes back whether or not the checkpoint ran, as Go defers it.
        let retaken = inner.begin_read();
        let (busy, log, checkpointed) = sort_run(rc, log, checkpointed)?;
        if mode == CheckpointMode::Truncate && !busy {
            // SQLite wrote every committed page into the database file again and synced it,
            // then emptied the WAL: nothing a failed sync of the WAL covered is left in it.
            inner.wal_sync_failed = false;
        }
        retaken.map_err(|_lost| SourceError::ReadTxLost)?;
        Ok(CheckpointRun {
            busy,
            log,
            checkpointed,
            epoch: inner.epoch,
        })
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.lock()
            .free_writer()?
            .execute(
                "INSERT INTO _litestream_seq (id, seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET seq = seq + 1",
                [],
            )
            .map(drop)
            .map_err(|e| source_error(&e))
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        self.lock().begin_writer().map(WriteLock::new)
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.lock().end_writer(lock.id())
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};

    use super::*;

    /// A database in WAL mode under a temporary directory, and our handle on it.
    fn open() -> (tempfile::TempDir, std::path::PathBuf, Sqlite) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let app = Connection::open(&path).unwrap();
        app.execute_batch("PRAGMA journal_mode = WAL; CREATE TABLE t (v);")
            .unwrap();
        let sqlite = Sqlite::open(&path).unwrap();
        (dir, path, sqlite)
    }

    fn deny_rollback(ctx: AuthContext<'_>) -> Authorization {
        let rollback = AuthAction::Transaction {
            operation: TransactionOperation::Rollback,
        };
        if ctx.action == rollback {
            Authorization::Deny
        } else {
            Authorization::Allow
        }
    }

    /// The sequence number another connection reads.
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
        sqlite
            .lock()
            .writer
            .authorizer(Some(deny_rollback))
            .unwrap();
        assert!(
            sqlite.end_barrier(barrier).is_err(),
            "the ROLLBACK was refused"
        );
        sqlite
            .lock()
            .writer
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .unwrap();
        sqlite.bump_seq().unwrap();
        assert_eq!(seq(&path), Some(1), "another connection sees the bump");
        let lock = sqlite.begin_write_lock().unwrap();
        sqlite.end_write_lock(lock).unwrap();
        assert_eq!(seq(&path), Some(1), "the next lock's release kept it");
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
