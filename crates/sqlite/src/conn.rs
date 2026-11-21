//! The two connections, the read transaction that pins the WAL, and the WAL door.

use std::ffi::{OsString, c_int};
use std::fs::{File, Metadata};
use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use lsr_core::{CommittedRead, ReadEpoch, SourceError, WalFileLen, WalSource};
use lsr_ltx::PageSize;
use lsr_wal::{CommittedEnd, Salts};
use rusqlite::{Connection, FileControl, OpenFlags};

use crate::error::{OpenError, SetupError};
use crate::pages::{FileHandle, file_pointer};
use crate::shm::{self, Region};

/// [`SqliteConfig::journal_size_limit`]'s default: 64 MiB.
///
/// Above the WAL a database reaches between two of the replicator's PASSIVE checkpoints at their
/// default threshold (1,000 frames: under 64 MiB at every page size up to 64 KiB), so a WAL in
/// steady state is not cut and regrown on every restart; low enough that one burst of writes
/// does not keep its peak size on disk for good.
pub const DEFAULT_JOURNAL_SIZE_LIMIT: NonZeroU64 = match NonZeroU64::new(64 * 1024 * 1024) {
    Some(bytes) => bytes,
    None => NonZeroU64::MIN,
};

/// How [`Sqlite::open_with`] opens its two connections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqliteConfig {
    /// The registered SQLite VFS to open both connections with; `None` is SQLite's default.
    /// A fault-injecting VFS for tests goes here.
    pub vfs: Option<String>,
    /// `PRAGMA journal_size_limit` on the writer connection, in bytes: the size SQLite cuts the
    /// WAL file back to when that connection's write restarts the log. After the replicator's
    /// own checkpoint that write is its sequence bump on this connection, and SQLite cuts the
    /// file only on the connection that restarts it, so without a limit the WAL file keeps its
    /// largest size for good. `None` is SQLite's own default, no limit. Default
    /// [`DEFAULT_JOURNAL_SIZE_LIMIT`].
    pub journal_size_limit: Option<NonZeroU64>,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            vfs: None,
            journal_size_limit: Some(DEFAULT_JOURNAL_SIZE_LIMIT),
        }
    }
}

/// The pragma's value for `limit`: -1 is no limit, and so is a limit past `i64::MAX`.
fn journal_size_limit(limit: Option<NonZeroU64>) -> i64 {
    limit.map_or(-1, |bytes| i64::try_from(bytes.get()).unwrap_or(-1))
}

/// A live SQLite database, as the three doors of the sync engine (`WalSource`, `PageSource`,
/// `Checkpointer`).
///
/// A cheap handle (`Clone`) over shared state: core's checkpoint takes the doors as three
/// arguments, and every clone is the same pair of connections and the same read transaction.
#[derive(Clone, Debug)]
pub struct Sqlite {
    inner: Arc<Mutex<Inner>>,
}

/// The reader's file handle and wal-index region: raw pointers into the reader's pager and the
/// process's shm node.
#[derive(Debug)]
struct Raw {
    file: FileHandle,
    region: Region,
}

#[expect(
    unsafe_code,
    reason = "the raw handle moves between threads together with the connection that owns it"
)]
// SAFETY: both pointers name memory owned by the reader connection's pager and shm node, not by
// a thread. `Connection` is `Send`, and `Raw` moves only together with it inside `Inner`, whose
// every use is under the mutex; the region's words are atomics and xRead is a `pread`.
unsafe impl Send for Raw {}

#[derive(Debug)]
pub(crate) struct Inner {
    // Field order is drop order: the raw pointers go before the connection they point into.
    raw: Raw,
    pub(crate) reader: Connection,
    pub(crate) writer: Connection,
    wal_path: PathBuf,
    /// The file the WAL path named once the first read transaction was held. SQLite keeps that
    /// file open for the reader's whole life, so any other file at the path, or none, means it
    /// was deleted under the pin; `None` when it could not be told.
    wal_id: Option<FileId>,
    page_size: PageSize,
    /// How many read transactions have been taken; the current one is this many.
    pub(crate) epoch: u64,
    /// Whether the reader holds its read transaction now.
    pinned: bool,
    /// The id of the barrier or write lock the writer connection holds.
    held: Option<u64>,
    next_token: u64,
}

/// Which file a path named: device and inode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileId {
    dev: u64,
    ino: u64,
}

#[cfg(unix)]
fn file_id(meta: &Metadata) -> Option<FileId> {
    use std::os::unix::fs::MetadataExt;
    Some(FileId {
        dev: meta.dev(),
        ino: meta.ino(),
    })
}

#[cfg(not(unix))]
const fn file_id(_meta: &Metadata) -> Option<FileId> {
    None
}

/// The statement that takes the write lock without leaving a row (it is always rolled back).
const TAKE_WRITE_LOCK: &str = "INSERT INTO _litestream_lock (id) VALUES (1)";

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

fn connect(path: &Path, config: &SqliteConfig) -> Result<Connection, rusqlite::Error> {
    // No CREATE: a path that names no database is an error, never a new empty one.
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = match &config.vfs {
        Some(vfs) => Connection::open_with_flags_and_vfs(path, flags, vfs.as_str())?,
        None => Connection::open_with_flags(path, flags)?,
    };
    conn.busy_timeout(Duration::ZERO)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)?;
    Ok(conn)
}

/// The size SQLite reported, as the type pages are counted in.
pub(crate) fn page_size(bytes: u32) -> Result<PageSize, OpenError> {
    PageSize::new(bytes).ok_or(OpenError::PageSize { bytes })
}

/// `SQLITE_FCNTL_PERSIST_WAL` on: the connection's close, when it is the last, keeps the WAL file.
fn persist_wal(conn: &Connection) -> Result<(), rusqlite::Error> {
    let mut on: c_int = 1;
    conn.file_control(Some(rusqlite::MAIN_DB), FileControl::PersistWal(&mut on))
}

/// `BEGIN; SELECT COUNT(1) FROM _litestream_seq` (Go `db.go:1288`): the select takes the read mark.
fn begin_read(reader: &Connection) -> Result<(), rusqlite::Error> {
    reader.execute_batch("BEGIN")?;
    let read: Result<i64, _> =
        reader.query_row("SELECT COUNT(1) FROM _litestream_seq", [], |row| row.get(0));
    if let Err(err) = read {
        // The transaction is only open to be dropped; its error is the one to report.
        drop(reader.execute_batch("ROLLBACK"));
        return Err(err);
    }
    Ok(())
}

impl Sqlite {
    /// [`Sqlite::open_with`] with the default config.
    ///
    /// # Errors
    /// As [`Sqlite::open_with`].
    pub fn open(path: &Path) -> Result<Self, OpenError> {
        Self::open_with(path, &SqliteConfig::default())
    }

    /// Opens the database at `path`, which must already exist, puts it in WAL mode with
    /// autocheckpoint off and `SQLITE_FCNTL_PERSIST_WAL` on for both connections (so the
    /// replicator closing after the host keeps the WAL), creates `_litestream_seq` and `_litestream_lock`
    /// (`db.go:1174`, `1180`, so Go and Rust leave the same schema), takes the first read
    /// transaction (epoch 1) and maps the wal-index. It returns once the read transaction is
    /// held, so nothing can checkpoint past it.
    ///
    /// # Errors
    /// [`OpenError`]; nothing stays open or pinned.
    pub fn open_with(path: &Path, config: &SqliteConfig) -> Result<Self, OpenError> {
        let sqlite = |source| OpenError::Sqlite {
            path: path.to_owned(),
            source,
        };
        let reader = connect(path, config).map_err(sqlite)?;
        let writer = connect(path, config).map_err(sqlite)?;
        let mode: String = writer
            .query_row("PRAGMA journal_mode = wal", [], |row| row.get(0))
            .map_err(sqlite)?;
        if mode != "wal" {
            return Err(OpenError::NotWal { mode });
        }
        // When the replicator closes last, SQLite checkpoints; without this it also deletes the
        // WAL, and the next start finds nothing to continue.
        for conn in [&reader, &writer] {
            persist_wal(conn).map_err(sqlite)?;
        }
        // The writer only: its sequence bump is the write that restarts the log after the
        // replicator's checkpoints, and SQLite cuts the file on the restarting connection alone.
        // Should a later step fail, the writer closes before the reader (declaration order), so
        // a last close is the reader's, which has no limit and keeps the WAL (see `Inner`'s drop).
        writer
            .pragma_update(
                None,
                "journal_size_limit",
                journal_size_limit(config.journal_size_limit),
            )
            .map_err(sqlite)?;
        writer
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
                 CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);",
            )
            .map_err(sqlite)?;
        let bytes: u32 = reader
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .map_err(sqlite)?;
        let page_size = page_size(bytes)?;
        begin_read(&reader).map_err(sqlite)?;
        shm::guard(&reader, path)?;
        let file = FileHandle::new(file_pointer(&reader, rusqlite::MAIN_DB)?)?;
        let region = shm::map(&file)?;
        let wal_path = suffixed(path, "-wal");
        // The read transaction holds the WAL open, so this is the file SQLite writes.
        let wal_id = std::fs::metadata(&wal_path)
            .ok()
            .and_then(|meta| file_id(&meta));
        let inner = Inner {
            raw: Raw { file, region },
            reader,
            writer,
            wal_path,
            wal_id,
            page_size,
            epoch: 1,
            pinned: true,
            held: None,
            next_token: 0,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// The write-ahead log's path, for tests that copy or inspect it.
    #[must_use]
    pub fn wal_path(&self) -> PathBuf {
        self.lock().wal_path.clone()
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every step leaves `Inner` consistent before it can panic, so a poisoned lock holds
        // plain data and the next call reports whatever SQLite says.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The writer closes last of the two (field order). When it is the database's last
        // connection, SQLite checkpoints on close and, with a size limit set, truncates the WAL
        // to zero bytes, so the next start would find no WAL to continue and snapshot. With the
        // limit off the close keeps the WAL, as `PERSIST_WAL` intends. A failure only leaves
        // that truncation possible, and a drop has no one to report it to.
        drop(
            self.writer
                .pragma_update(None, "journal_size_limit", journal_size_limit(None)),
        );
    }
}

impl Inner {
    pub(crate) const fn file(&self) -> &FileHandle {
        &self.raw.file
    }

    pub(crate) const fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// `Unlinked` unless `found` (the file at the WAL path now, if any) is the WAL SQLite holds.
    fn same_wal(&self, found: Option<&Metadata>) -> Result<(), SourceError> {
        let Some(id) = self.wal_id else {
            return Ok(());
        };
        match found.and_then(file_id) {
            Some(now) if now == id => Ok(()),
            Some(_) => Err(SourceError::Unlinked),
            None if found.is_some() => Ok(()),
            None => Err(SourceError::Unlinked),
        }
    }

    /// The WAL path's metadata, checked to be the WAL SQLite holds; `None` only when the WAL
    /// was never known.
    fn wal_meta(&self) -> Result<Option<Metadata>, SourceError> {
        let found = match std::fs::metadata(&self.wal_path) {
            Ok(meta) => Some(meta),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(io_error(&err)),
        };
        self.same_wal(found.as_ref())?;
        Ok(found)
    }

    /// Takes the read transaction again (a new epoch).
    pub(crate) fn begin_read(&mut self) -> Result<(), rusqlite::Error> {
        begin_read(&self.reader)?;
        self.pinned = true;
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    /// Ends the read transaction, if one is held.
    pub(crate) fn end_read(&mut self) -> Result<(), SourceError> {
        if self.pinned {
            self.reader
                .execute_batch("ROLLBACK")
                .map_err(|e| crate::error::source_error(&e))?;
            self.pinned = false;
        }
        Ok(())
    }

    /// After a retake failed, the next read takes the pin again first: its new epoch tells the
    /// engine the old one lapsed. Every answer is under a read transaction or an error.
    pub(crate) fn ensure_pinned(&mut self) -> Result<(), SourceError> {
        if self.pinned {
            return Ok(());
        }
        self.begin_read().map_err(|_lost| SourceError::ReadTxLost)
    }

    fn take_token(&mut self) -> u64 {
        self.next_token = self.next_token.saturating_add(1);
        self.next_token
    }

    /// `BEGIN` then the lock-table insert (Go `db.go:2660`, `2759`), which is rolled back.
    pub(crate) fn begin_writer(&mut self) -> Result<u64, SourceError> {
        self.writer
            .execute_batch("BEGIN")
            .map_err(|e| crate::error::source_error(&e))?;
        if let Err(err) = self.writer.execute(TAKE_WRITE_LOCK, []) {
            drop(self.writer.execute_batch("ROLLBACK"));
            return Err(crate::error::source_error(&err));
        }
        let token = self.take_token();
        self.held = Some(token);
        Ok(token)
    }

    pub(crate) fn end_writer(&mut self, token: u64) -> Result<(), SourceError> {
        if self.held != Some(token) {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        self.held = None;
        self.writer
            .execute_batch("ROLLBACK")
            .map_err(|e| crate::error::source_error(&e))
    }
}

fn io_error(err: &io::Error) -> SourceError {
    if err.kind() == io::ErrorKind::StorageFull {
        SourceError::DiskFull
    } else {
        SourceError::Io(err.kind())
    }
}

impl WalSource for Sqlite {
    type Wal = File;

    fn open(&mut self) -> Result<Option<File>, SourceError> {
        let inner = self.lock();
        #[expect(
            clippy::disallowed_methods,
            reason = "SQLite takes no POSIX lock on -wal, so this descriptor's close drops none"
        )]
        let opened = File::open(&inner.wal_path);
        let file = match opened {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                inner.same_wal(None)?;
                return Ok(None);
            }
            Err(err) => return Err(io_error(&err)),
        };
        // The file opened, not the path's state a moment earlier, is what the engine reads.
        let meta = file.metadata().map_err(|err| io_error(&err))?;
        inner.same_wal(Some(&meta))?;
        Ok(Some(file))
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        Ok(self
            .lock()
            .wal_meta()?
            .map(|meta| WalFileLen::new(meta.len())))
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        let mut inner = self.lock();
        inner.ensure_pinned()?;
        // The wal-index still counts the frames SQLite writes to a deleted WAL: refuse to name
        // an end the WAL path cannot show.
        inner.wal_meta()?;
        let header =
            shm::read_header(inner.raw.region.words()).map_err(|_retry| SourceError::Busy)?;
        let salts = Salts {
            salt1: header.salt1,
            salt2: header.salt2,
        };
        let end = CommittedEnd::new(header.mx_frame, salts, inner.page_size);
        // nBackfill is outside the copies the checksum covers: a value above mxFrame is a read
        // racing a restart, which a retry resolves.
        CommittedRead::new(end, header.backfilled).ok_or(SourceError::Busy)
    }

    fn epoch(&self) -> ReadEpoch {
        ReadEpoch::new(self.lock().epoch)
    }
}

/// Sets up one host connection as replication needs it (PLAN section 6): `SQLITE_FCNTL_PERSIST_WAL`
/// on, so the last connection to close keeps the WAL the replica follows, and
/// `wal_autocheckpoint = 0`, so only the replicator checkpoints. Call it on every connection a
/// host opens, pooled ones included.
///
/// # Errors
/// [`SetupError::PersistWal`] when the connection's database has no file; [`SetupError::Sqlite`]
/// when the pragma fails.
pub fn app_connection(conn: &Connection) -> Result<(), SetupError> {
    persist_wal(conn).map_err(SetupError::PersistWal)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(SetupError::Sqlite)
}

/// Which database file a path names: its device and inode on Unix, so a second path to the same
/// file (a hard link, a symlink, a relative path) is the same file, and a file replaced at the
/// same path is not. Elsewhere, the canonical path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatabaseFile {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(not(unix))]
    path: PathBuf,
}

impl DatabaseFile {
    /// The file `path` names now. A stat, never an open: no descriptor is opened or closed.
    ///
    /// # Errors
    /// The stat's error (the file is missing or unreadable).
    pub fn of(path: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = std::fs::metadata(path)?;
            Ok(Self {
                dev: meta.dev(),
                ino: meta.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                path: std::fs::canonicalize(path)?,
            })
        }
    }
}

/// [`app_connection`], after checking that `conn` opens `database`: a host that sets up a
/// connection to some other file gets an error, not a replicator that never sees its writes.
///
/// # Errors
/// [`SetupError::NoFile`] for a connection with no file (in-memory or temporary);
/// [`SetupError::OtherDatabase`] when its file is not `database`; else as [`app_connection`].
pub fn app_connection_for(conn: &Connection, database: &DatabaseFile) -> Result<(), SetupError> {
    let path = conn
        .path()
        .filter(|path| !path.is_empty())
        .ok_or(SetupError::NoFile)?;
    let found = DatabaseFile::of(Path::new(path)).map_err(|err| SetupError::Stat(err.kind()))?;
    if &found != database {
        return Err(SetupError::OtherDatabase);
    }
    app_connection(conn)
}

/// [`app_connection_for`] on a raw SQLite handle, for a host on sqlx, diesel or its own FFI.
/// The handle stays the host's: nothing here closes it.
///
/// # Safety
/// `db` is an open connection, used by no other thread during the call, and opened by the
/// SQLite library this crate links: a second copy of SQLite in the process does not share its
/// locks with this one (PLAN section 6). Note that `wal_autocheckpoint` replaces any
/// `sqlite3_wal_hook` the host set on the connection.
///
/// # Errors
/// As [`app_connection_for`]; [`SetupError::Sqlite`] when the handle cannot be wrapped.
#[expect(
    unsafe_code,
    reason = "a host's raw handle can only be borrowed through rusqlite's unsafe from_handle"
)]
pub unsafe fn app_connection_raw(
    db: *mut rusqlite::ffi::sqlite3,
    database: &DatabaseFile,
) -> Result<(), SetupError> {
    // SAFETY: the caller promises an open handle of this library's SQLite that no other thread
    // uses during the call; `from_handle` borrows it and its drop does not close it.
    let conn = unsafe { Connection::from_handle(db) }.map_err(SetupError::Sqlite)?;
    app_connection_for(&conn, database)
}

/// Reads back what [`app_connection`] sets.
///
/// # Errors
/// [`SetupError::PersistWalOff`] or [`SetupError::AutocheckpointOn`] for a connection that is
/// not set up; the other variants when the read itself fails.
pub fn check_app_connection(conn: &Connection) -> Result<(), SetupError> {
    // -1 asks for the current setting, which SQLite writes back.
    let mut persist: c_int = -1;
    conn.file_control(
        Some(rusqlite::MAIN_DB),
        FileControl::PersistWal(&mut persist),
    )
    .map_err(SetupError::PersistWal)?;
    if persist == 0 {
        return Err(SetupError::PersistWalOff);
    }
    let pages: i64 = conn
        .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
        .map_err(SetupError::Sqlite)?;
    if pages != 0 {
        return Err(SetupError::AutocheckpointOn { pages });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_size_sqlite_does_not_allow_is_refused() {
        assert!(matches!(
            page_size(1000),
            Err(OpenError::PageSize { bytes: 1000 })
        ));
        assert_eq!(page_size(4096).ok(), PageSize::new(4096));
    }
}
