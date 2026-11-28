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

use std::ffi::{CStr, c_int};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use lsr_core::SourceError;

use crate::capi::{BusyState, Connection, Handle};
use crate::conn::{DatabaseFile, SqliteConfig, connect};
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
    /// [`OpenError`] when SQLite refuses the file or the SQLite answering is not a verified
    /// build.
    pub fn open(path: &Path, config: &SqliteConfig, held: WritesHeld) -> Result<Self, OpenError> {
        crate::version::check_linked()?;
        let sqlite = |source| OpenError::Sqlite {
            path: path.to_owned(),
            source,
        };
        let conn = connect(path, config).map_err(sqlite)?;
        conn.handle().persist_wal().map_err(sqlite)?;
        held.set(false);
        Ok(Self { conn, held })
    }

    /// The database file the gate's connection holds open, from SQLite's own descriptor; `None`
    /// when it cannot be told (as [`crate::Sqlite::database_file`]).
    #[must_use]
    pub fn database_file(&self) -> Option<DatabaseFile> {
        crate::identity::opened(self.conn.handle())
    }

    /// Whether the gate holds the write lock now: whether SQLite has its connection in a
    /// transaction, so an error that ended the transaction cannot leave a flag saying otherwise.
    #[must_use]
    pub fn is_held(&self) -> bool {
        !self.conn.handle().is_autocommit()
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
                .handle()
                .exec("BEGIN IMMEDIATE")
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
                .handle()
                .exec("ROLLBACK")
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

const KEY: &CStr = c"lsr-write-gate-busy";

/// SQLite's default busy handler's waits (`sqliteDefaultBusyCallback`), in milliseconds: the
/// wait before each retry, and the time waited before it.
const DELAYS: [c_int; 12] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100];

/// The busy handler's state: the gate's flag and the timeout to wait out while it is clear.
struct Busy {
    held: WritesHeld,
    timeout_ms: c_int,
    /// How often SQLite called the handler.
    #[cfg(test)]
    calls: std::sync::atomic::AtomicU32,
}

/// How long SQLite's default handler sleeps before retry `count` (from 0) under a timeout of
/// `timeout_ms`; `None` once the timeout is spent.
fn default_delay(count: c_int, timeout_ms: c_int) -> Option<c_int> {
    let last = DELAYS.last().copied().unwrap_or(100);
    let index = usize::try_from(count).unwrap_or(usize::MAX);
    let delay = DELAYS.get(index).copied().unwrap_or(last);
    // The waits before this retry: the table's, then `last` for every retry past its end.
    let past_table = c_int::try_from(DELAYS.len()).unwrap_or(c_int::MAX);
    let prior = DELAYS
        .iter()
        .take(index)
        .sum::<c_int>()
        .saturating_add(last.saturating_mul(count.saturating_sub(past_table).max(0)));
    let delay = if prior.saturating_add(delay) > timeout_ms {
        timeout_ms.saturating_sub(prior)
    } else {
        delay
    };
    (delay > 0).then_some(delay)
}

impl BusyState for Busy {
    const KEY: &'static CStr = KEY;

    fn retry(&self, count: c_int) -> bool {
        #[cfg(test)]
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.held.is_held() {
            return false;
        }
        match default_delay(count, self.timeout_ms) {
            Some(ms) => {
                crate::capi::sleep(ms);
                true
            }
            None => false,
        }
    }
}

/// Gives `conn` the busy handler that fails a blocked write at once while `held` says the gate
/// holds, and otherwise waits as SQLite's default handler would for the busy timeout `conn`
/// has now. Setting it up again keeps that timeout.
///
/// # Errors
/// [`SetupError::Sqlite`] when the timeout cannot be read or SQLite cannot store the state
/// (nothing is changed then).
#[cfg(feature = "rusqlite")]
pub fn fail_fast_while(conn: &rusqlite::Connection, held: &WritesHeld) -> Result<(), SetupError> {
    install(Handle::of_rusqlite(conn).map_err(SetupError::Sqlite)?, held)
}

/// [`fail_fast_while`] on any handle.
pub(crate) fn install(conn: Handle<'_>, held: &WritesHeld) -> Result<(), SetupError> {
    // A handler of ours already installed set the connection's timeout to 0: keep its own.
    let kept = conn.busy_state(|busy: &Busy| busy.timeout_ms);
    let timeout_ms = match kept {
        Some(ms) => ms,
        None => conn
            .query_int("PRAGMA busy_timeout")
            .map_err(SetupError::Sqlite)
            .map(|ms| c_int::try_from(ms).unwrap_or(c_int::MAX))?,
    };
    conn.install_busy(Busy {
        held: held.clone(),
        timeout_ms,
        #[cfg(test)]
        calls: std::sync::atomic::AtomicU32::new(0),
    })
    .map_err(SetupError::Sqlite)
}

#[cfg(test)]
mod tests;
