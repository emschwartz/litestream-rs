//! The backend's state and the application's own operations, made as SQL on real connections.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};

use lsr_core::{SourceError, SourceOp};
use lsr_ltx::{PageNo, PageSize};
use lsr_sim::side::Written;
use lsr_sim::sqlite::{AppCommit, CheckpointNumbers, OtherProcessMode, ReaderId};
use rusqlite::{Connection, OpenFlags};

use super::in_tick::Tick;
use super::index::Index;
use crate::testing::vfs::FaultVfs;
use crate::{Sqlite, SqliteConfig};

/// The table every drawn page is a row of: the row whose id is the page number holds the
/// first quarter of the page's drawn bytes, so a few rows share a leaf page and a commit
/// touches about as many pages as it was asked to.
pub(super) const TABLE: &str = "CREATE TABLE p(id INTEGER PRIMARY KEY, v BLOB)";

const UPSERT: &str =
    "INSERT INTO p(id, v) VALUES (?1, ?2) ON CONFLICT(id) DO UPDATE SET v = excluded.v";

/// Everything the backend holds: the files, the connections, the engine's handle and what the
/// world was last told.
pub(super) struct Real {
    pub(super) db: PathBuf,
    pub(super) vfs: FaultVfs,
    pub(super) page_size: PageSize,
    /// The application's connection; `None` only inside a power loss.
    pub(super) app: Option<Connection>,
    /// A second application connection that stays open and commits on its own; opened at
    /// its first commit.
    pub(super) other_writer: Option<Connection>,
    /// The application holds a write transaction it has not committed (a spill).
    pub(super) app_tx: bool,
    /// Other processes' read transactions.
    pub(super) readers: BTreeMap<ReaderId, Connection>,
    pub(super) next_reader: u32,
    /// The engine's connections, while its process lives.
    pub(super) engine: Option<Sqlite>,
    /// The database as the world last learned it, page by page.
    pub(super) seen: Vec<u8>,
    /// Changes read back before an operation inside a tick, owed to the world first.
    pub(super) owed: VecDeque<Written>,
    pub(super) injected: Vec<(SourceOp, SourceError)>,
    pub(super) lie_ours: bool,
    pub(super) tick: Tick,
}

pub(super) fn lock(real: &Mutex<Real>) -> MutexGuard<'_, Real> {
    real.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Upserts one row per page on `conn`.
fn upsert_on(
    conn: &Connection,
    pages: &[(PageNo, Vec<u8>)],
    size: PageSize,
    copies: usize,
) -> Option<()> {
    let mut stmt = conn.prepare(UPSERT).ok()?;
    for (pgno, bytes) in pages {
        let row = blob(bytes, size, copies);
        stmt.execute(rusqlite::params![pgno.get(), row]).ok()?;
    }
    Some(())
}

/// The first quarter of `bytes`, or `copies` times all of it for a spill, which must outgrow
/// the application's page cache.
fn blob(bytes: &[u8], size: PageSize, copies: usize) -> Vec<u8> {
    if copies == 0 {
        let keep = size.bytes().checked_div(4).unwrap_or(1).min(bytes.len());
        return bytes.get(..keep).unwrap_or(bytes).to_vec();
    }
    bytes.repeat(copies)
}

impl Real {
    /// A connection through the backend's VFS to the existing database.
    pub(super) fn connect(&self) -> Option<Connection> {
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let conn = Connection::open_with_flags_and_vfs(&self.db, flags, self.vfs.name()).ok()?;
        conn.busy_timeout(std::time::Duration::ZERO).ok()?;
        Some(conn)
    }

    /// The engine's handle, opening it first when its process has none: a database the
    /// application holds a write transaction on cannot be opened (its setup writes).
    pub(super) fn engine(&mut self) -> Option<Sqlite> {
        if self.engine.is_none() {
            let config = SqliteConfig {
                vfs: Some(self.vfs.name().to_owned()),
                ..SqliteConfig::default()
            };
            self.engine = Sqlite::open_with(&self.db, &config).ok();
        }
        self.engine.clone()
    }

    /// The database as a new reader sees it, through SQLite (`sqlite3_serialize` on a fresh
    /// connection): never read by the engine's code. Empty when no connection opens.
    pub(super) fn image(&self) -> Vec<u8> {
        let Some(mut conn) = self.connect() else {
            return Vec::new();
        };
        conn.serialize(rusqlite::MAIN_DB)
            .map(|data| data.to_vec())
            .unwrap_or_default()
    }

    /// What changed since the world last learned the database: the owed changes first.
    pub(super) fn take_written(&mut self) -> Written {
        if let Some(owed) = self.owed.pop_front() {
            return owed;
        }
        self.read_back()
    }

    /// Reads the database back and returns every page that differs from what the world last
    /// learned, or lies past its old end.
    pub(super) fn read_back(&mut self) -> Written {
        let now = self.image();
        let size = self.page_size;
        let old = self.seen.chunks_exact(size.bytes());
        let pages = now
            .chunks_exact(size.bytes())
            .zip((1u32..).zip(old.map(Some).chain(std::iter::repeat(None))))
            .filter(|(page, (_, was))| was.is_none_or(|was| was != *page))
            .filter_map(|(page, (pgno, _))| Some((PageNo::new(pgno, size)?, page.to_vec())))
            .collect();
        let db_size = now.len().checked_div(size.bytes()).unwrap_or(0);
        self.seen = now;
        Written {
            pages,
            db_size: u32::try_from(db_size).unwrap_or(u32::MAX),
        }
    }

    /// The generation and committed end after an application commit.
    pub(super) fn mark(&self) -> AppCommit {
        let index = Index::read(self);
        AppCommit {
            salts: index.salts,
            end: index.end(self.page_size),
        }
    }

    fn upsert_all(&self, pages: &[(PageNo, Vec<u8>)], copies: usize) -> Option<()> {
        upsert_on(self.app.as_ref()?, pages, self.page_size, copies)
    }

    /// The second application connection commits `pages` in a transaction of its own;
    /// `None` when SQLite refuses (another connection holds the write lock).
    pub(super) fn other_writer_commit(&mut self, pages: &[(PageNo, Vec<u8>)]) -> Option<AppCommit> {
        if pages.is_empty() {
            return None;
        }
        if self.other_writer.is_none() {
            self.other_writer = super::open_app_conn(&self.db, self.vfs);
        }
        let conn = self.other_writer.as_ref()?;
        conn.execute_batch("BEGIN IMMEDIATE").ok()?;
        let done = upsert_on(conn, pages, self.page_size, 0)
            .and_then(|()| conn.execute_batch("COMMIT").ok());
        if done.is_none() {
            // A rollback that fails leaves nothing open: SQLite ends the transaction either way.
            let _rolled_back = conn.execute_batch("ROLLBACK").is_ok();
            return None;
        }
        Some(self.mark())
    }

    /// Opens the application's write transaction unless it holds one; false when SQLite
    /// refuses (another connection holds the write lock).
    fn begin_app_tx(&mut self) -> Option<bool> {
        if self.app_tx {
            return Some(false);
        }
        self.app.as_ref()?.execute_batch("BEGIN IMMEDIATE").ok()?;
        self.app_tx = true;
        Some(true)
    }

    fn rollback_app(&mut self) {
        if let Some(app) = self.app.as_ref() {
            // A rollback that fails leaves nothing open: SQLite ends the transaction either way.
            let _rolled_back = app.execute_batch("ROLLBACK").is_ok();
        }
        self.app_tx = false;
    }

    /// The application commits `pages` after anything it spilled; `None` when SQLite refuses.
    pub(super) fn commit(
        &mut self,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
    ) -> Option<AppCommit> {
        if pages.is_empty() || db_size == 0 {
            return None;
        }
        self.begin_app_tx()?;
        let done = self
            .upsert_all(pages, 0)
            .and_then(|()| self.app.as_ref()?.execute_batch("COMMIT").ok());
        if done.is_none() {
            self.rollback_app();
            return None;
        }
        self.app_tx = false;
        Some(self.mark())
    }

    /// The application writes `pages` in a transaction it leaves open, big enough to spill
    /// frames into the WAL; false when nothing reached the WAL (a transaction this call began
    /// is rolled back then).
    pub(super) fn spill(&mut self, pages: &[(PageNo, Vec<u8>)]) -> bool {
        let before = self.wal_len();
        let Some(began) = self.begin_app_tx() else {
            return false;
        };
        let wrote = self.upsert_all(pages, 3).is_some();
        let spilled = wrote && self.wal_len() > before;
        if began && !spilled {
            self.rollback_app();
        }
        spilled
    }

    /// The application rolls its open transaction back; false when it holds none.
    pub(super) fn rollback(&mut self) -> bool {
        let held = self.app_tx;
        if held {
            self.rollback_app();
        }
        held
    }

    /// The application deletes every row past `db_size`; under `auto_vacuum = FULL` the commit
    /// gives the freed pages back. `None` when there is nothing to delete or SQLite refuses.
    pub(super) fn shrink(&mut self, db_size: u32) -> Option<AppCommit> {
        if self.app_tx || db_size == 0 {
            return None;
        }
        let deleted = self
            .app
            .as_ref()?
            .execute("DELETE FROM p WHERE id > ?1", [db_size])
            .ok()?;
        (deleted > 0).then(|| self.mark())
    }

    /// Another process's connection begins a read transaction and holds it.
    pub(super) fn reader_begin(&mut self) -> ReaderId {
        let id = ReaderId::new(self.next_reader);
        self.next_reader = self.next_reader.saturating_add(1);
        if let Some(conn) = self.connect()
            && conn.execute_batch("BEGIN; SELECT count(*) FROM p;").is_ok()
        {
            self.readers.insert(id, conn);
        }
        id
    }

    /// That process's read transaction ends, and its connection closes.
    pub(super) fn reader_end(&mut self, id: ReaderId) -> bool {
        self.readers.remove(&id).is_some()
    }

    /// `PRAGMA wal_checkpoint(mode)` on `conn`.
    pub(super) fn checkpoint_on(
        conn: &Connection,
        mode: OtherProcessMode,
    ) -> Option<CheckpointNumbers> {
        let sql = match mode {
            OtherProcessMode::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            OtherProcessMode::Restart => "PRAGMA wal_checkpoint(RESTART)",
            OtherProcessMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        };
        let (busy, log, done): (i64, i64, i64) = conn
            .query_row(sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .ok()?;
        Some(CheckpointNumbers {
            busy: busy != 0,
            log: u32::try_from(log).unwrap_or(0),
            checkpointed: u32::try_from(done).unwrap_or(0),
        })
    }

    /// Another process opens a connection, checkpoints and closes it.
    pub(super) fn other_checkpoint(&self, mode: OtherProcessMode) -> Option<CheckpointNumbers> {
        Self::checkpoint_on(&self.connect()?, mode)
    }

    /// The WAL path.
    pub(super) fn wal_path(&self) -> PathBuf {
        let mut name = self.db.clone().into_os_string();
        name.push("-wal");
        PathBuf::from(name)
    }

    /// The WAL file's bytes; `None` when there is no file. SQLite holds no POSIX lock on the
    /// WAL, so reading it here drops none of the application's.
    pub(super) fn wal_file(&self) -> Option<Vec<u8>> {
        #[expect(
            clippy::disallowed_methods,
            reason = "the WAL carries no POSIX lock; the database and -shm are never read this way"
        )]
        let read = std::fs::read(self.wal_path());
        read.ok()
    }

    fn wal_len(&self) -> u64 {
        std::fs::metadata(self.wal_path()).map_or(0, |meta| meta.len())
    }
}
