//! The fail-stop write gate: a write transaction the replicator holds on a connection of its
//! own, so that no other connection can write (`lsr_core::failstop`).
//!
//! [`WriteGate`] holds `BEGIN IMMEDIATE` and never commits. In WAL mode that takes the
//! database's one write lock, and every other writer, in this process or another, on any
//! binding, fails with `SQLITE_BUSY` ("database is locked") once its busy timeout runs out. In
//! an ordinary transaction the statement that fails is the first write, not `COMMIT`. Readers
//! are not affected, a long read begun before the hold included, and a PASSIVE checkpoint on
//! another connection still runs. A write transaction already open when the gate is asked for
//! makes `BEGIN IMMEDIATE` busy: that transaction commits, and the gate is taken on a later try.
//!
//! The gate's connection is opened by path, beside the engine's two, and shares nothing with
//! them, so it holds while the engine is rebuilt after a panic or gone after a stop. It sets
//! `SQLITE_FCNTL_PERSIST_WAL`, as they do, so its close never deletes the WAL.
//!
//! [`WritesHeld`] says whether the gate holds now. [`fail_fast_while`] gives an application
//! connection a busy handler that reads it: while the gate holds, a blocked write fails at once
//! instead of waiting out the busy timeout; otherwise the handler waits as SQLite's default one
//! does, for the timeout the connection had when the handler was installed. SQLite keeps one
//! busy handler per connection, and `PRAGMA busy_timeout` or `sqlite3_busy_timeout` replaces
//! it: the connection then waits out its timeout before "database is locked", and loses
//! nothing else. The handler's state is the connection's client data
//! (`sqlite3_set_clientdata`), freed when the connection closes or the handler is set up again.

use std::ffi::{CStr, c_int, c_void};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use lsr_core::SourceError;
use rusqlite::Connection;
use rusqlite::ffi;

use crate::conn::{DatabaseFile, SqliteConfig, connect, persist_wal};
use crate::error::{OpenError, SetupError, source_error};

/// Whether a [`WriteGate`] holds the database's write lock now. Cheap to clone: every clone
/// reads the same state, which only the gate changes.
#[derive(Clone, Debug, Default)]
pub struct WritesHeld(Arc<AtomicBool>);

impl WritesHeld {
    /// A state no gate holds yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the gate holds the write lock now.
    #[must_use]
    pub fn is_held(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self, held: bool) {
        self.0.store(held, Ordering::Release);
    }
}

/// The replicator's connection that holds the application's writes: a write transaction it
/// takes and rolls back, never commits.
#[derive(Debug)]
pub struct WriteGate {
    conn: Connection,
    held: WritesHeld,
}

impl WriteGate {
    /// Opens the gate's connection on `path`, released, reporting its state through `held`.
    /// The database must already be in WAL mode ([`crate::Sqlite::open`] puts it there).
    ///
    /// # Errors
    /// [`OpenError`] when SQLite refuses the file or the SQLite answering is not the bundled
    /// build.
    pub fn open(path: &Path, config: &SqliteConfig, held: WritesHeld) -> Result<Self, OpenError> {
        crate::version::check_linked()?;
        let sqlite = |source| OpenError::Sqlite {
            path: path.to_owned(),
            source,
        };
        let conn = connect(path, config).map_err(sqlite)?;
        persist_wal(&conn).map_err(sqlite)?;
        held.set(false);
        Ok(Self { conn, held })
    }

    /// The database file the gate's connection holds open, from SQLite's own descriptor; `None`
    /// when it cannot be told (as [`crate::Sqlite::database_file`]).
    #[must_use]
    pub fn database_file(&self) -> Option<DatabaseFile> {
        crate::identity::opened(&self.conn)
    }

    /// Whether the gate holds the write lock now: whether SQLite has its connection in a
    /// transaction, so an error that ended the transaction cannot leave a flag saying otherwise.
    #[must_use]
    pub fn is_held(&self) -> bool {
        !self.conn.is_autocommit()
    }

    /// Takes the write lock, unless the gate holds it already.
    ///
    /// # Errors
    /// [`SourceError::Busy`] while another connection is in a write transaction (it commits,
    /// and a later call takes the lock); others when SQLite fails.
    pub fn hold(&mut self) -> Result<(), SourceError> {
        let taken = if self.is_held() {
            Ok(())
        } else {
            self.conn
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(|err| source_error(&err))
        };
        self.held.set(self.is_held());
        taken
    }

    /// Lets writes through again, if the gate holds the lock.
    ///
    /// # Errors
    /// [`SourceError`] when the rollback fails; SQLite's own state then says whether the lock
    /// is still held, and [`WritesHeld`] follows it.
    pub fn release(&mut self) -> Result<(), SourceError> {
        let released = if self.is_held() {
            self.conn
                .execute_batch("ROLLBACK")
                .map_err(|err| source_error(&err))
        } else {
            Ok(())
        };
        self.held.set(self.is_held());
        released
    }
}

impl Drop for WriteGate {
    fn drop(&mut self) {
        // Closing the connection ends its transaction.
        self.held.set(false);
    }
}

/// The name the busy handler's state is kept under in the connection's client data.
const KEY: &CStr = c"lsr-write-gate-busy";

/// SQLite's default busy handler's waits (`sqliteDefaultBusyCallback`), in milliseconds: the
/// wait before each retry, and the time waited before it.
const DELAYS: [c_int; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];
const TOTALS: [c_int; 12] = [0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228];

/// The busy handler's state: the gate's flag and the timeout to wait out while it is clear.
struct Busy {
    held: WritesHeld,
    timeout_ms: c_int,
    /// How often SQLite called the handler, for the tests that check it fails at once.
    #[cfg(test)]
    calls: std::sync::atomic::AtomicU32,
}

/// How long SQLite's default handler sleeps before retry `count` (from 0) under a timeout of
/// `timeout_ms`; `None` once the timeout is spent.
fn default_delay(count: c_int, timeout_ms: c_int) -> Option<c_int> {
    let last = DELAYS.len().saturating_sub(1);
    let index = usize::try_from(count).unwrap_or(usize::MAX);
    let (delay, prior) = match (DELAYS.get(index), TOTALS.get(index)) {
        (Some(delay), Some(prior)) => (*delay, *prior),
        _ => {
            let delay = DELAYS.get(last).copied().unwrap_or(100);
            let past = c_int::try_from(last).unwrap_or(c_int::MAX);
            let prior = TOTALS
                .get(last)
                .copied()
                .unwrap_or(0)
                .saturating_add(delay.saturating_mul(count.saturating_sub(past)));
            (delay, prior)
        }
    };
    let delay = if prior.saturating_add(delay) > timeout_ms {
        timeout_ms.saturating_sub(prior)
    } else {
        delay
    };
    (delay > 0).then_some(delay)
}

/// The busy handler: fail at once while the gate holds, else wait as SQLite's default does.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer with the argument it was given"
)]
extern "C" fn on_busy(arg: *mut c_void, count: c_int) -> c_int {
    // SAFETY: `arg` is the `Busy` `fail_fast_while` handed SQLite, owned by the connection's
    // client data until it closes or the handler is set up again, and the handler is replaced
    // in the same call that replaces that data.
    let busy = unsafe { &*arg.cast_const().cast::<Busy>() };
    #[cfg(test)]
    busy.calls.fetch_add(1, Ordering::Relaxed);
    if busy.held.is_held() {
        return 0;
    }
    match default_delay(count, busy.timeout_ms) {
        Some(ms) => {
            #[expect(
                unsafe_code,
                reason = "sqlite3_sleep is reached only through the C API"
            )]
            // SAFETY: a plain sleep through SQLite's default VFS; it returns the time slept.
            let _slept = unsafe { ffi::sqlite3_sleep(ms) };
            1
        }
        None => 0,
    }
}

/// The client data's destructor: frees the handler's state.
#[expect(
    unsafe_code,
    reason = "SQLite calls this through a C function pointer when it drops the client data"
)]
extern "C" fn release(arg: *mut c_void) {
    // SAFETY: `arg` came from `Box::into_raw` in `fail_fast_while`, and SQLite calls the
    // destructor once per pointer it was given.
    drop(unsafe { Box::from_raw(arg.cast::<Busy>()) });
}

/// Gives `conn` the busy handler that fails a blocked write at once while `held` says the gate
/// holds, and otherwise waits as SQLite's default handler would for the busy timeout `conn`
/// has now. Setting it up again keeps that timeout.
///
/// # Errors
/// [`SetupError::Sqlite`] when the timeout cannot be read or SQLite cannot store the state
/// (nothing is changed then).
pub fn fail_fast_while(conn: &Connection, held: &WritesHeld) -> Result<(), SetupError> {
    #[expect(
        unsafe_code,
        reason = "rusqlite lends the raw handle only through an unsafe accessor"
    )]
    // SAFETY: the handle belongs to `conn`, which this call borrows; nothing closes it here.
    let db = unsafe { conn.handle() };
    #[expect(
        unsafe_code,
        reason = "client data is reached only through SQLite's C API"
    )]
    // SAFETY: `db` is open (above); a non-null pointer under this key is a `Busy` this module
    // stored, alive until the client data is replaced below.
    let ours = unsafe { ffi::sqlite3_get_clientdata(db, KEY.as_ptr()) };
    // A handler of ours already installed set the connection's timeout to 0: keep its own.
    #[expect(
        unsafe_code,
        reason = "the earlier state is read through the pointer SQLite keeps for it"
    )]
    // SAFETY: as above; the reference ends before the data is replaced.
    let kept = (!ours.is_null()).then(|| unsafe { &*ours.cast_const().cast::<Busy>() }.timeout_ms);
    let timeout_ms = match kept {
        Some(ms) => ms,
        None => conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .map_err(SetupError::Sqlite)?,
    };
    let state = Box::new(Busy {
        held: held.clone(),
        timeout_ms,
        #[cfg(test)]
        calls: std::sync::atomic::AtomicU32::new(0),
    });
    let arg = Box::into_raw(state).cast::<c_void>();
    // `db` is used by this thread alone for the calls below, so no busy wait runs between them.
    #[expect(
        unsafe_code,
        reason = "client data is reached only through SQLite's C API"
    )]
    // SAFETY: `db` is open (above). The client data takes `arg` and `release` frees it once: on
    // failure at once, with the handler untouched; on success when the connection closes or the
    // data is replaced. The earlier state it frees now is named only by the handler, which the
    // next call replaces before any busy wait can run.
    let stored = unsafe { ffi::sqlite3_set_clientdata(db, KEY.as_ptr(), arg, Some(release)) };
    if stored != ffi::SQLITE_OK {
        return Err(SetupError::Sqlite(rusqlite::Error::SqliteFailure(
            ffi::Error::new(stored),
            None,
        )));
    }
    #[expect(
        unsafe_code,
        reason = "busy handlers are reached only through SQLite's C API"
    )]
    // SAFETY: `db` is open (above); the handler is a plain function and `arg` lives as long as
    // the client data that owns it.
    let set = unsafe { ffi::sqlite3_busy_handler(db, Some(on_busy), arg) };
    if set != ffi::SQLITE_OK {
        return Err(SetupError::Sqlite(rusqlite::Error::SqliteFailure(
            ffi::Error::new(set),
            None,
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
