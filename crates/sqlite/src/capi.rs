//! The SQLite C calls the engine makes, behind a thin safe wrapper.
//!
//! The engine's own connections are opened here on the raw C API (libsqlite3-sys), not through a
//! binding crate, so the library places no bound of its own on which libsqlite3-sys an application
//! links (the `version` module decides which builds are accepted). Every call into SQLite's C API
//! goes through this module, each at one `unsafe` site with its invariant stated: open and close,
//! a statement run (prepare, step, column reads, finalize), the transaction state, the busy
//! timeout and busy handler, the file controls, the checkpoint, and the library's identity. The
//! VFS method calls and private-layout reads that need the file handle these hand out stay in the
//! modules that explain them (`pages`, `shm`, `identity`).
//!
//! [`Connection`] owns a handle it opened and closes it on drop. [`Handle`] is a borrowed
//! view of an open handle, the engine's or an application's, that cannot outlive its lifetime;
//! every call is a method on it. A handle is used by one thread at a time: [`Connection`] is
//! `Send`, never `Sync`, and [`Handle`] neither.

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::{self, NonNull};

use crate::ffi;

/// The main database's schema name.
pub(crate) const MAIN: &CStr = c"main";

/// What SQLite said when a call failed, or why a statement's answer was not usable.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SqliteError {
    /// SQLite returned a result code other than success.
    #[error("{message}")]
    Code {
        /// The extended result code.
        code: c_int,
        /// `sqlite3_errmsg`'s text for it.
        message: String,
    },
    /// A query that must return a row returned none.
    #[error("the query returned no row")]
    NoRow,
    /// A query's first column was not of the type asked for.
    #[error("the query's value is not {wanted}")]
    Type {
        /// The type asked for.
        wanted: &'static str,
    },
    /// A path, statement or name handed to SQLite held a NUL byte, or was not UTF-8.
    #[error("a string handed to SQLite holds a NUL byte or is not UTF-8")]
    BadString,
}

impl SqliteError {
    /// `SQLITE_MISUSE`, for a null handle.
    pub(crate) fn misuse() -> Self {
        Self::Code {
            code: ffi::SQLITE_MISUSE,
            message: errstr(ffi::SQLITE_MISUSE),
        }
    }

    /// SQLite's extended result code, when the failure was SQLite's own.
    #[must_use]
    pub const fn code(&self) -> Option<c_int> {
        match self {
            Self::Code { code, .. } => Some(*code),
            Self::NoRow | Self::Type { .. } | Self::BadString => None,
        }
    }
}

fn c_string(text: &str) -> Result<CString, SqliteError> {
    CString::new(text).map_err(|_nul| SqliteError::BadString)
}

/// A connection this crate opened; closed when dropped.
#[derive(Debug)]
pub(crate) struct Connection {
    db: NonNull<ffi::sqlite3>,
}

#[expect(
    unsafe_code,
    reason = "a connection moves between threads; SQLite in multi-thread mode allows it when one thread uses it at a time"
)]
// SAFETY: the handle is SQLite memory owned by this value alone, opened with
// SQLITE_OPEN_NO_MUTEX, which SQLite allows on any thread so long as no two use it at once; that
// holds because `Connection` is not `Sync` and every `Handle` borrows it.
unsafe impl Send for Connection {}

/// The name to give SQLite for the file `path`. The bundled SQLite is built with
/// `SQLITE_USE_URI`, so it reads every name that starts with `file:` as a URI, and a relative
/// path that does (`file:x?mode=memory`) would open something else than the file it names.
/// `./` in front names the same file and is never a URI.
fn plain_name(path: &[u8]) -> Vec<u8> {
    if path.starts_with(b"file:") {
        [b"./", path].concat()
    } else {
        path.to_vec()
    }
}

impl Connection {
    /// Opens `path` with `flags` on the VFS named `vfs` (SQLite's default when `None`), with
    /// extended result codes on, as rusqlite and sqlx open theirs.
    pub(crate) fn open(path: &Path, flags: c_int, vfs: Option<&str>) -> Result<Self, SqliteError> {
        #[cfg(unix)]
        let bytes = {
            use std::os::unix::ffi::OsStrExt;
            path.as_os_str().as_bytes()
        };
        #[cfg(not(unix))]
        let bytes = path.to_str().ok_or(SqliteError::BadString)?.as_bytes();
        let name = CString::new(plain_name(bytes)).map_err(|_nul| SqliteError::BadString)?;
        Self::open_c(&name, flags, vfs)
    }

    /// [`Connection::open`] on a file name or `file:` URI given as text.
    pub(crate) fn open_name(
        name: &str,
        flags: c_int,
        vfs: Option<&str>,
    ) -> Result<Self, SqliteError> {
        Self::open_c(&c_string(name)?, flags, vfs)
    }

    #[expect(unsafe_code, reason = "sqlite3_open_v2 is a C call")]
    fn open_c(name: &CStr, flags: c_int, vfs: Option<&str>) -> Result<Self, SqliteError> {
        let vfs = vfs.map(c_string).transpose()?;
        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        // SAFETY: two NUL-terminated strings (or null for the default VFS) that outlive the call,
        // and an out-pointer SQLite writes one handle through.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                name.as_ptr(),
                &raw mut db,
                flags,
                vfs.as_ref().map_or(ptr::null(), |vfs| vfs.as_ptr()),
            )
        };
        let Some(db) = NonNull::new(db) else {
            return Err(SqliteError::Code {
                code: ffi::SQLITE_NOMEM,
                message: "out of memory".to_owned(),
            });
        };
        let conn = Self { db };
        if rc != ffi::SQLITE_OK {
            // The handle SQLite left holds the error; the drop closes it.
            return Err(conn.handle().error(rc));
        }
        // SAFETY: `db` is open (above); the call only sets a flag on it.
        let _was = unsafe { ffi::sqlite3_extended_result_codes(conn.db.as_ptr(), 1) };
        Ok(conn)
    }

    /// The open handle, borrowed for as long as this connection is.
    pub(crate) const fn handle(&self) -> Handle<'_> {
        Handle {
            db: self.db,
            life: PhantomData,
        }
    }

    /// Closes the connection, reporting SQLite's answer, which a drop cannot.
    #[expect(unsafe_code, reason = "sqlite3_close is a C call")]
    pub(crate) fn close(self) -> Result<(), SqliteError> {
        let db = std::mem::ManuallyDrop::new(self).db;
        // SAFETY: `db` is open and owned by the value whose drop was just suppressed, so nothing
        // uses or closes it again; every statement this module prepares is finalized before its call returns.
        let rc = unsafe { ffi::sqlite3_close(db.as_ptr()) };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(SqliteError::Code {
                code: rc,
                message: errstr(rc),
            })
        }
    }
}

impl Drop for Connection {
    #[expect(unsafe_code, reason = "sqlite3_close is a C call")]
    fn drop(&mut self) {
        // SAFETY: the handle is open and owned by this value, which is going away; every
        // statement this module prepares is finalized before its call returns, so the close
        // frees it. A failure leaks it, and a drop has no one to report to.
        let _rc = unsafe { ffi::sqlite3_close(self.db.as_ptr()) };
    }
}

/// SQLite's text for a result code, for a failure with no handle to ask.
#[expect(unsafe_code, reason = "sqlite3_errstr is a C call")]
fn errstr(rc: c_int) -> String {
    // SAFETY: `sqlite3_errstr` returns a pointer to a static NUL-terminated string for any code.
    let text = unsafe { ffi::sqlite3_errstr(rc) };
    owned(text).unwrap_or_default()
}

/// A copy of a NUL-terminated C string SQLite owns; `None` for null or text that is not UTF-8.
#[expect(
    unsafe_code,
    reason = "the string is C memory read through a raw pointer"
)]
fn owned(text: *const c_char) -> Option<String> {
    if text.is_null() {
        return None;
    }
    // SAFETY: every caller passes a pointer SQLite returned to a NUL-terminated string that
    // stays valid until the next call on the same handle, and copies it out before that.
    let text = unsafe { CStr::from_ptr(text) };
    text.to_str().ok().map(str::to_owned)
}

/// A borrowed view of an open SQLite handle, valid for `'a`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Handle<'a> {
    db: NonNull<ffi::sqlite3>,
    life: PhantomData<&'a ffi::sqlite3>,
}

/// A prepared statement, finalized when dropped.
struct Statement<'a> {
    stmt: NonNull<ffi::sqlite3_stmt>,
    conn: Handle<'a>,
}

impl Drop for Statement<'_> {
    #[expect(unsafe_code, reason = "sqlite3_finalize is a C call")]
    fn drop(&mut self) {
        // SAFETY: the statement came from `sqlite3_prepare_v2` on a handle still open (the
        // lifetime), and is finalized once, here. Its result repeats the last step's error,
        // which the caller already has.
        let _rc = unsafe { ffi::sqlite3_finalize(self.stmt.as_ptr()) };
    }
}

impl Statement<'_> {
    /// One step: `true` for a row, `false` when the statement is done.
    #[expect(unsafe_code, reason = "sqlite3_step is a C call")]
    fn step(&self) -> Result<bool, SqliteError> {
        // SAFETY: a live statement on an open handle (the lifetime), used by this thread alone.
        let rc = unsafe { ffi::sqlite3_step(self.stmt.as_ptr()) };
        match rc {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            _ => Err(self.conn.error(rc)),
        }
    }

    /// Column 0 of the current row as an integer.
    #[expect(unsafe_code, reason = "column reads are C calls")]
    fn int(&self) -> Result<i64, SqliteError> {
        // SAFETY: a live statement on a row (the caller stepped to one); column 0 exists in
        // every statement this module reads.
        let kind = unsafe { ffi::sqlite3_column_type(self.stmt.as_ptr(), 0) };
        if kind != ffi::SQLITE_INTEGER {
            return Err(SqliteError::Type {
                wanted: "an integer",
            });
        }
        // SAFETY: as above.
        Ok(unsafe { ffi::sqlite3_column_int64(self.stmt.as_ptr(), 0) })
    }

    /// Column 0 of the current row as text.
    #[expect(unsafe_code, reason = "column reads are C calls")]
    fn text(&self) -> Result<String, SqliteError> {
        let wrong = SqliteError::Type { wanted: "text" };
        // SAFETY: a live statement on a row (the caller stepped to one).
        let kind = unsafe { ffi::sqlite3_column_type(self.stmt.as_ptr(), 0) };
        if kind != ffi::SQLITE_TEXT {
            return Err(wrong);
        }
        // SAFETY: as above; the pointer stays valid until the next step, column call or finalize,
        // and is copied before any of them.
        let text = unsafe { ffi::sqlite3_column_text(self.stmt.as_ptr(), 0) };
        // SAFETY: as above, called after the text conversion, as SQLite's docs require.
        let len = unsafe { ffi::sqlite3_column_bytes(self.stmt.as_ptr(), 0) };
        let len = usize::try_from(len).map_err(|_neg| wrong.clone())?;
        if text.is_null() {
            return Ok(String::new());
        }
        // SAFETY: SQLite returned `len` bytes of text at `text`, valid until the next call on
        // this statement (above).
        let bytes = unsafe { std::slice::from_raw_parts(text, len) };
        String::from_utf8(bytes.to_vec()).map_err(|_utf8| wrong)
    }
}

impl<'a> Handle<'a> {
    /// A view of `db`.
    ///
    /// # Safety
    /// `db` is an open handle that stays open, and is used by no other thread, for `'a`.
    #[expect(
        unsafe_code,
        reason = "only the caller can promise the handle is open for the lifetime"
    )]
    pub(crate) unsafe fn borrow(db: *mut ffi::sqlite3) -> Option<Self> {
        NonNull::new(db).map(|db| Self {
            db,
            life: PhantomData,
        })
    }

    /// A rusqlite connection's handle, borrowed while the connection is.
    ///
    /// # Errors
    /// `SQLITE_MISUSE` for a null handle, which an open rusqlite connection never has.
    #[expect(
        unsafe_code,
        reason = "rusqlite lends its raw handle only through an unsafe accessor"
    )]
    #[cfg(any(test, feature = "rusqlite"))]
    pub(crate) fn of_rusqlite(conn: &'a rusqlite::Connection) -> Result<Self, SqliteError> {
        // SAFETY: the handle belongs to `conn`, open while it is borrowed for `'a`, and
        // nothing here closes it.
        let db = unsafe { conn.handle() };
        NonNull::new(db)
            .map(|db| Self {
                db,
                life: PhantomData,
            })
            .ok_or_else(SqliteError::misuse)
    }

    /// The raw pointer, for tests that lend the handle to rusqlite.
    #[cfg(test)]
    pub(crate) const fn as_ptr(self) -> *mut ffi::sqlite3 {
        self.db.as_ptr()
    }

    /// The error SQLite holds for `rc`, with its message.
    #[expect(
        unsafe_code,
        reason = "sqlite3_extended_errcode and sqlite3_errmsg are C calls"
    )]
    fn error(self, rc: c_int) -> SqliteError {
        // SAFETY: an open handle (the lifetime); the call only reads its last error.
        let code = unsafe { ffi::sqlite3_extended_errcode(self.db.as_ptr()) };
        // A primary code that does not match the handle's last error came from no statement on
        // it (a misuse, say): the code returned is the one to report.
        let code = if code & 0xff == rc & 0xff { code } else { rc };
        // SAFETY: as above; the message is copied before any other call on the handle.
        let message = owned(unsafe { ffi::sqlite3_errmsg(self.db.as_ptr()) });
        SqliteError::Code {
            code,
            message: message.unwrap_or_else(|| errstr(code)),
        }
    }

    #[expect(unsafe_code, reason = "sqlite3_prepare_v2 is a C call")]
    fn prepare(self, sql: &CStr) -> Result<Option<(Statement<'a>, *const c_char)>, SqliteError> {
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        let mut tail: *const c_char = ptr::null();
        // SAFETY: an open handle (the lifetime), NUL-terminated SQL that outlives the call (-1:
        // read to the NUL), and two out-pointers SQLite writes one value each through.
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                self.db.as_ptr(),
                sql.as_ptr(),
                -1,
                &raw mut stmt,
                &raw mut tail,
            )
        };
        if rc != ffi::SQLITE_OK {
            return Err(self.error(rc));
        }
        // A null statement is SQL of only whitespace or comments.
        Ok(NonNull::new(stmt).map(|stmt| (Statement { stmt, conn: self }, tail)))
    }

    /// Runs every statement in `sql`, ignoring any rows.
    #[expect(
        unsafe_code,
        reason = "the tail SQLite returns is a pointer into the caller's string"
    )]
    pub(crate) fn exec(self, sql: &str) -> Result<(), SqliteError> {
        let sql = c_string(sql)?;
        let mut rest: &CStr = &sql;
        while !rest.is_empty() {
            let Some((stmt, tail)) = self.prepare(rest)? else {
                break;
            };
            while stmt.step()? {}
            drop(stmt);
            if tail.is_null() {
                break;
            }
            // SAFETY: SQLite's tail points into `sql` at or before its NUL, which `sql` keeps
            // alive, so it is a NUL-terminated string inside it.
            rest = unsafe { CStr::from_ptr(tail) };
        }
        Ok(())
    }

    /// The first row's first column, read by `read`; [`SqliteError::NoRow`] when there is none.
    fn first<T>(
        self,
        sql: &str,
        read: impl FnOnce(&Statement<'a>) -> Result<T, SqliteError>,
    ) -> Result<T, SqliteError> {
        let sql = c_string(sql)?;
        let (stmt, _tail) = self.prepare(&sql)?.ok_or(SqliteError::NoRow)?;
        if !stmt.step()? {
            return Err(SqliteError::NoRow);
        }
        read(&stmt)
    }

    /// The first row's first column as an integer.
    pub(crate) fn query_int(self, sql: &str) -> Result<i64, SqliteError> {
        self.first(sql, Statement::int)
    }

    /// The first row's first column as text.
    pub(crate) fn query_text(self, sql: &str) -> Result<String, SqliteError> {
        self.first(sql, Statement::text)
    }

    /// The first column of at most `limit` rows, as text.
    pub(crate) fn query_texts(self, sql: &str, limit: usize) -> Result<Vec<String>, SqliteError> {
        let sql = c_string(sql)?;
        let Some((stmt, _tail)) = self.prepare(&sql)? else {
            return Ok(Vec::new());
        };
        let mut rows = Vec::new();
        while rows.len() < limit && stmt.step()? {
            rows.push(stmt.text()?);
        }
        Ok(rows)
    }

    /// Whether the connection is outside any transaction.
    #[expect(unsafe_code, reason = "sqlite3_get_autocommit is a C call")]
    pub(crate) fn is_autocommit(self) -> bool {
        // SAFETY: an open handle (the lifetime); the call only reads a flag.
        unsafe { ffi::sqlite3_get_autocommit(self.db.as_ptr()) != 0 }
    }

    /// Whether the main database is in a read or write transaction.
    #[expect(unsafe_code, reason = "sqlite3_txn_state is a C call")]
    pub(crate) fn in_transaction(self) -> bool {
        // SAFETY: an open handle (the lifetime) and a NUL-terminated schema name.
        let state = unsafe { ffi::sqlite3_txn_state(self.db.as_ptr(), MAIN.as_ptr()) };
        matches!(state, ffi::SQLITE_TXN_READ | ffi::SQLITE_TXN_WRITE)
    }

    /// `sqlite3_busy_timeout`: replaces any busy handler.
    #[expect(unsafe_code, reason = "sqlite3_busy_timeout is a C call")]
    pub(crate) fn busy_timeout(self, ms: c_int) -> Result<(), SqliteError> {
        // SAFETY: an open handle (the lifetime).
        let rc = unsafe { ffi::sqlite3_busy_timeout(self.db.as_ptr(), ms) };
        self.check(rc)
    }

    fn check(self, rc: c_int) -> Result<(), SqliteError> {
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(self.error(rc))
        }
    }

    /// The main database's file name: empty for a temporary or in-memory database, `None`
    /// when SQLite gives none.
    #[expect(unsafe_code, reason = "sqlite3_db_filename is a C call")]
    pub(crate) fn main_filename(self) -> Option<String> {
        // SAFETY: an open handle (the lifetime) and a NUL-terminated schema name; the name is
        // copied before any other call.
        owned(unsafe { ffi::sqlite3_db_filename(self.db.as_ptr(), MAIN.as_ptr()) })
    }

    /// A file control that writes one pointer through its argument.
    #[expect(unsafe_code, reason = "sqlite3_file_control is a C call")]
    fn pointer_control<T>(self, schema: &CStr, op: c_int) -> Result<*mut T, c_int> {
        let mut out: *mut T = ptr::null_mut();
        // SAFETY: an open handle (the lifetime), a NUL-terminated schema name, and an op from
        // the three callers, each of which writes exactly one pointer of type `T` through its
        // argument (`sqlite3.h`, FILE_POINTER, JOURNAL_POINTER, VFS_POINTER).
        let rc = unsafe {
            ffi::sqlite3_file_control(
                self.db.as_ptr(),
                schema.as_ptr(),
                op,
                (&raw mut out).cast::<c_void>(),
            )
        };
        if rc == ffi::SQLITE_OK {
            Ok(out)
        } else {
            Err(rc)
        }
    }

    /// `SQLITE_FCNTL_FILE_POINTER`: the pager's `sqlite3_file` for `schema`, which lives as long
    /// as the connection. Null when the file is not open.
    pub(crate) fn file_pointer(self, schema: &CStr) -> Result<*mut ffi::sqlite3_file, c_int> {
        self.pointer_control(schema, ffi::SQLITE_FCNTL_FILE_POINTER)
    }

    /// `SQLITE_FCNTL_JOURNAL_POINTER`: in WAL mode the WAL's `sqlite3_file`, null when no WAL
    /// is open.
    pub(crate) fn journal_pointer(self) -> Result<*mut ffi::sqlite3_file, c_int> {
        self.pointer_control(MAIN, ffi::SQLITE_FCNTL_JOURNAL_POINTER)
    }

    /// The name of the VFS the main database was opened with (`SQLITE_FCNTL_VFS_POINTER`).
    #[expect(
        unsafe_code,
        reason = "the VFS's name is read through the pointer SQLite hands out"
    )]
    pub(crate) fn vfs_name(self) -> Option<String> {
        let vfs = self
            .pointer_control::<ffi::sqlite3_vfs>(MAIN, ffi::SQLITE_FCNTL_VFS_POINTER)
            .ok()?;
        if vfs.is_null() {
            return None;
        }
        // SAFETY: a registered VFS lives at least as long as a connection that uses it, and
        // `zName` is its first-version field.
        owned(unsafe { (*vfs).zName })
    }

    /// `SQLITE_FCNTL_PERSIST_WAL` on: the connection's close, when it is the last, keeps the WAL.
    #[expect(unsafe_code, reason = "sqlite3_file_control is a C call")]
    pub(crate) fn persist_wal(self) -> Result<(), SqliteError> {
        let mut on: c_int = 1;
        // SAFETY: an open handle (the lifetime), a NUL-terminated schema name, and PERSIST_WAL
        // reads and writes one `int` through its argument.
        let rc = unsafe {
            ffi::sqlite3_file_control(
                self.db.as_ptr(),
                MAIN.as_ptr(),
                ffi::SQLITE_FCNTL_PERSIST_WAL,
                (&raw mut on).cast::<c_void>(),
            )
        };
        self.check(rc)
    }

    /// `sqlite3_wal_checkpoint_v2` on the main database: SQLite's result code and its two frame
    /// counts, which the result code alone cannot carry.
    #[expect(unsafe_code, reason = "sqlite3_wal_checkpoint_v2 is a C call")]
    pub(crate) fn wal_checkpoint(self, mode: c_int) -> (c_int, c_int, c_int) {
        let (mut log, mut checkpointed) = (-1, -1);
        // SAFETY: an open handle (the lifetime), a NUL-terminated schema name, and two `int`s
        // SQLite writes.
        let rc = unsafe {
            ffi::sqlite3_wal_checkpoint_v2(
                self.db.as_ptr(),
                MAIN.as_ptr(),
                mode,
                &raw mut log,
                &raw mut checkpointed,
            )
        };
        (rc, log, checkpointed)
    }

    /// The busy handler state of type `T` installed on this connection, read by `read`.
    ///
    /// `read` must not install another state on the same connection: that frees the one it reads.
    #[expect(
        unsafe_code,
        reason = "client data is reached only through SQLite's C API and read through its pointer"
    )]
    pub(crate) fn busy_state<T: BusyState, R>(self, read: impl FnOnce(&T) -> R) -> Option<R> {
        // SAFETY: an open handle (the lifetime) and a NUL-terminated key.
        let ours = unsafe { get_clientdata(self.db.as_ptr(), T::KEY.as_ptr()) };
        if ours.is_null() {
            return None;
        }
        // SAFETY: only [`Handle::install_busy`] stores data under `T::KEY`, and it stores a
        // `Box<T>`, which lives until the data is replaced or the connection closes; neither
        // happens during `read` (above, and the handle is borrowed for the call).
        Some(read(unsafe { &*ours.cast_const().cast::<T>() }))
    }

    /// Installs `state` as this connection's busy handler: SQLite asks
    /// [`BusyState::retry`] on each busy wait. The state is the connection's client data under
    /// `T::KEY`, freed when the connection closes or a state is installed again. Nothing is
    /// changed on failure.
    #[expect(
        unsafe_code,
        reason = "client data and busy handlers are reached only through SQLite's C API"
    )]
    pub(crate) fn install_busy<T: BusyState>(self, state: T) -> Result<(), SqliteError> {
        let arg = Box::into_raw(Box::new(state)).cast::<c_void>();
        // SAFETY: an open handle (the lifetime), used by this thread alone for the two calls, so
        // no busy wait runs between them. The client data takes `arg` and `free::<T>` frees it
        // once: at once on failure, with the handler untouched; on success when the connection
        // closes or the data is replaced. An earlier state it frees now is named only by the
        // handler, which the next call replaces before any busy wait can run.
        let stored =
            unsafe { set_clientdata(self.db.as_ptr(), T::KEY.as_ptr(), arg, Some(free::<T>)) };
        self.check(stored)?;
        // SAFETY: an open handle; the handler is a plain function, and `arg` lives as long as
        // the client data that owns it, which is replaced only together with the handler.
        let set = unsafe { ffi::sqlite3_busy_handler(self.db.as_ptr(), Some(on_busy::<T>), arg) };
        self.check(set)
    }
}

/// A busy handler's state, kept as a connection's client data under its own key.
pub(crate) trait BusyState: Send + Sync + 'static {
    /// The client data key: one type per key.
    const KEY: &'static CStr;

    /// Whether SQLite should retry after busy wait `count` (from 0).
    fn retry(&self, count: c_int) -> bool;
}

/// The busy handler SQLite calls through its C function pointer.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer with the argument it was given"
)]
extern "C" fn on_busy<T: BusyState>(arg: *mut c_void, count: c_int) -> c_int {
    // SAFETY: `arg` is the `Box<T>` [`Handle::install_busy`] handed SQLite, owned by the
    // connection's client data, which is replaced only together with this handler.
    let state = unsafe { &*arg.cast_const().cast::<T>() };
    c_int::from(state.retry(count))
}

/// The client data's destructor.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer when it drops the client data"
)]
extern "C" fn free<T: BusyState>(arg: *mut c_void) {
    // SAFETY: `arg` came from `Box::into_raw` of a `Box<T>` in [`Handle::install_busy`], and
    // SQLite calls the destructor once per pointer it was given.
    drop(unsafe { Box::from_raw(arg.cast::<T>()) });
}

#[expect(
    unsafe_code,
    reason = "declared here, not taken from libsqlite3-sys, whose bindings for an unbundled build stop at SQLite 3.34"
)]
// The signatures are `sqlite3.h`'s (3.44 and later), and every build the `version` module
// accepts has them.
unsafe extern "C" {
    #[link_name = "sqlite3_get_clientdata"]
    fn get_clientdata(db: *mut ffi::sqlite3, name: *const c_char) -> *mut c_void;
    #[link_name = "sqlite3_set_clientdata"]
    fn set_clientdata(
        db: *mut ffi::sqlite3,
        name: *const c_char,
        data: *mut c_void,
        destructor: Option<unsafe extern "C" fn(*mut c_void)>,
    ) -> c_int;
}

/// Sleeps `ms` through SQLite's default VFS, as SQLite's own busy handler does.
#[expect(
    unsafe_code,
    reason = "sqlite3_sleep is reached only through the C API"
)]
pub(crate) fn sleep(ms: c_int) {
    // SAFETY: a plain sleep; it returns the time slept.
    let _slept = unsafe { ffi::sqlite3_sleep(ms) };
}

/// The running library's `sqlite3_libversion_number()`.
#[expect(unsafe_code, reason = "sqlite3_libversion_number is a C call")]
pub(crate) fn version_number() -> c_int {
    // SAFETY: takes nothing and returns a constant.
    unsafe { ffi::sqlite3_libversion_number() }
}

/// The running library's `sqlite3_sourceid()`: the check-in hash that tells two builds of one
/// version apart.
#[expect(unsafe_code, reason = "sqlite3_sourceid is a C call")]
pub(crate) fn source_id() -> String {
    // SAFETY: takes nothing and returns a pointer to a static NUL-terminated string.
    owned(unsafe { ffi::sqlite3_sourceid() }).unwrap_or_default()
}
