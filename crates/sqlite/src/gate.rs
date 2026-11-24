//! The fail-stop commit gate: a commit hook that refuses an application connection's commits
//! while a flag is set (`lsr_core::failstop`).
//!
//! SQLite calls the hook (`sqlite3_commit_hook`) before every commit on the connection; a
//! non-zero return turns the COMMIT into a ROLLBACK, and the application's COMMIT fails with
//! `SQLITE_CONSTRAINT_COMMITHOOK` (531). Reads are untouched.
//!
//! The hook's argument is the flag, one `Arc` count handed to SQLite. The connection keeps the
//! same pointer as client data (`sqlite3_set_clientdata`) with a destructor that releases the
//! count when the connection closes, so no allocation outlives its connection. Setting the gate
//! up again replaces the client data, and SQLite releases the earlier count then.
//!
//! SQLite keeps one commit hook per connection, and installing returns only the previous hook's
//! argument, never its function: the gate cannot call or put back a hook the application set.
//! So it does not pretend to: when the hook it replaced was not its own, setup fails with
//! [`SetupError::CommitHookReplaced`], loudly, instead of the application's hook going quiet. A
//! hook the application sets afterwards replaces the gate the same way; nothing can see that.

use std::ffi::{CStr, c_int, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rusqlite::Connection;
use rusqlite::ffi;

use crate::SetupError;

/// The name the gate's flag is kept under in the connection's client data.
const KEY: &CStr = c"lsr-commit-gate";

/// The hook: refuse while the flag is set.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer with the argument it was given"
)]
extern "C" fn refuse_while_set(flag: *mut c_void) -> c_int {
    // SAFETY: `flag` is the pointer `refuse_commits_while` handed to SQLite, from an `Arc`
    // count the connection's client data owns until the connection closes or the gate is set
    // up again, and the hook is replaced in the same call that replaces that count.
    let flag = unsafe { &*flag.cast_const().cast::<AtomicBool>() };
    c_int::from(flag.load(Ordering::Acquire))
}

/// The client data's destructor: releases the count the hook's argument was made from.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer when it drops the client data"
)]
extern "C" fn release(flag: *mut c_void) {
    // SAFETY: `flag` came from `Arc::into_raw` in `refuse_commits_while`, and SQLite calls the
    // destructor once per pointer it was given.
    drop(unsafe { Arc::from_raw(flag.cast_const().cast::<AtomicBool>()) });
}

/// Installs the gate on `conn`: from now on its commits fail while `refused` is set.
///
/// # Errors
/// [`SetupError::CommitHookReplaced`] when the commit hook it replaced was not its own (the
/// gate is installed, the application's hook is gone); [`SetupError::Sqlite`] when SQLite
/// could not store the flag (out of memory; nothing is changed).
pub fn refuse_commits_while(
    conn: &Connection,
    refused: &Arc<AtomicBool>,
) -> Result<(), SetupError> {
    #[expect(
        unsafe_code,
        reason = "rusqlite lends the raw handle only through an unsafe accessor"
    )]
    // SAFETY: the handle belongs to `conn`, which this call borrows; nothing closes it here.
    let db = unsafe { conn.handle() };
    let arg = Arc::into_raw(Arc::clone(refused))
        .cast_mut()
        .cast::<c_void>();
    // `db` is the open connection `conn` borrows, used by this thread alone for the calls
    // below, so no commit runs between them.
    #[expect(
        unsafe_code,
        reason = "client data is reached only through SQLite's C API"
    )]
    // SAFETY: `db` is open (above); the pointer returned is compared, never read.
    let ours = unsafe { ffi::sqlite3_get_clientdata(db, KEY.as_ptr()) };
    #[expect(
        unsafe_code,
        reason = "client data is reached only through SQLite's C API"
    )]
    // SAFETY: `db` is open (above). The client data takes `arg` and `release` frees it once: on
    // failure at once, with the hook untouched; on success when the connection closes or the
    // data is replaced. The earlier gate's count it releases now is named only by the hook,
    // which the next call replaces before any commit can run.
    let stored = unsafe { ffi::sqlite3_set_clientdata(db, KEY.as_ptr(), arg, Some(release)) };
    if stored != ffi::SQLITE_OK {
        return Err(SetupError::Sqlite(rusqlite::Error::SqliteFailure(
            ffi::Error::new(stored),
            None,
        )));
    }
    #[expect(
        unsafe_code,
        reason = "commit hooks are reached only through SQLite's C API"
    )]
    // SAFETY: `db` is open (above); the hook is a plain function and `arg` lives as long as the
    // client data that owns it. The previous argument returned is not ours to free.
    let previous = unsafe { ffi::sqlite3_commit_hook(db, Some(refuse_while_set), arg) };
    if !previous.is_null() && previous != ours {
        return Err(SetupError::CommitHookReplaced);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(dir.path().join("db")).unwrap();
        conn.execute_batch("CREATE TABLE t(x)").unwrap();
        (dir, conn)
    }

    /// While the flag is set every commit is refused with SQLite's commit-hook code, and the
    /// rows stay unwritten; reads go on, and clearing the flag lets commits through again.
    #[test]
    fn commits_fail_while_the_flag_is_set_and_reads_go_on() {
        let (_dir, conn) = conn();
        let refused = Arc::new(AtomicBool::new(false));
        refuse_commits_while(&conn, &refused).unwrap();
        conn.execute("INSERT INTO t VALUES (1)", []).unwrap();
        refused.store(true, Ordering::Release);
        let err = conn.execute("INSERT INTO t VALUES (2)", []).unwrap_err();
        assert_eq!(
            err.sqlite_error().map(|e| e.extended_code),
            Some(rusqlite::ffi::SQLITE_CONSTRAINT_COMMITHOOK),
            "{err}"
        );
        let rows: i64 = conn
            .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1, "the refused row was rolled back; reads work");
        refused.store(false, Ordering::Release);
        conn.execute("INSERT INTO t VALUES (3)", []).unwrap();
    }

    /// A commit hook the application set first cannot be kept beside the gate: setup says so
    /// rather than leaving the application's hook to go quiet, and the gate still works.
    #[test]
    fn an_application_hook_set_first_is_reported_not_silently_replaced() {
        let (_dir, mut conn) = conn();
        conn.commit_hook(Some(|| false)).unwrap();
        let refused = Arc::new(AtomicBool::new(true));
        let err = refuse_commits_while(&conn, &refused).unwrap_err();
        assert!(matches!(err, SetupError::CommitHookReplaced), "{err}");
        let refusal = conn.execute("INSERT INTO t VALUES (1)", []).unwrap_err();
        assert_eq!(
            refusal.sqlite_error().map(|e| e.extended_code),
            Some(rusqlite::ffi::SQLITE_CONSTRAINT_COMMITHOOK)
        );
    }

    /// The gate's count on the flag goes when its connection closes, and setting the gate up
    /// twice on one connection keeps one count, not two: no allocation outlives a connection.
    #[test]
    fn the_gate_is_freed_with_its_connection() {
        let (_dir, conn) = conn();
        let refused = Arc::new(AtomicBool::new(false));
        refuse_commits_while(&conn, &refused).unwrap();
        refuse_commits_while(&conn, &refused).unwrap();
        assert_eq!(
            Arc::strong_count(&refused),
            2,
            "one gate, holding one count"
        );
        drop(conn);
        assert_eq!(
            Arc::strong_count(&refused),
            1,
            "closing the connection released it"
        );
    }
}
