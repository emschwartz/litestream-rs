//! The fail-stop commit gate: a commit hook that refuses an application connection's commits
//! while a flag is set (`lsr_core::failstop`; `docs/proposals/fail-stop.md` section 3).
//!
//! SQLite calls the hook (`sqlite3_commit_hook`) before every commit on the connection; a
//! non-zero return turns the COMMIT into a ROLLBACK, and the application's COMMIT fails with
//! `SQLITE_CONSTRAINT_COMMITHOOK` (531). Reads are untouched.
//!
//! The hook's argument is the flag, kept alive by one `Arc` count handed to SQLite and never
//! taken back: a connection has no destructor hook for it, and the hook may run until the
//! connection closes. That is one small allocation per connection set up, for the process's
//! life. Installing replaces any commit hook the application set, as `wal_autocheckpoint`
//! replaces its WAL hook.

use std::ffi::{c_int, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rusqlite::Connection;

/// The hook: refuse while the flag is set.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer with the argument it was given"
)]
extern "C" fn refuse_while_set(flag: *mut c_void) -> c_int {
    // SAFETY: `flag` is the pointer `refuse_commits_while` handed to SQLite, from an `Arc`
    // count that is never released, so the `AtomicBool` it names outlives every call.
    let flag = unsafe { &*flag.cast_const().cast::<AtomicBool>() };
    c_int::from(flag.load(Ordering::Acquire))
}

/// Installs the gate on `conn`: from now on its commits fail while `refused` is set.
pub fn refuse_commits_while(conn: &Connection, refused: &Arc<AtomicBool>) {
    let arg = Arc::into_raw(Arc::clone(refused))
        .cast_mut()
        .cast::<c_void>();
    #[expect(
        unsafe_code,
        reason = "rusqlite lends the raw handle only through an unsafe accessor"
    )]
    // SAFETY: the handle belongs to `conn`, which this call borrows; nothing closes it here.
    let db = unsafe { conn.handle() };
    #[expect(
        unsafe_code,
        reason = "rusqlite's commit hook needs a mutable connection; the C call does not"
    )]
    // SAFETY: `db` is the open connection `conn` borrows, used by this thread for the call; the
    // hook is a plain function and `arg` stays valid for the process's life (see
    // `refuse_while_set`). The previous argument SQLite returns is not ours to free.
    unsafe {
        rusqlite::ffi::sqlite3_commit_hook(db, Some(refuse_while_set), arg);
    }
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
        refuse_commits_while(&conn, &refused);
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
}
