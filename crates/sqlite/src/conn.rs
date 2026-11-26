//! The two connections, the read transaction that pins the WAL, and the WAL door.

use std::ffi::OsString;
use std::fs::{File, Metadata};
use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::capi::{Connection, Handle, MAIN, SqliteError};
use crate::error::{OpenError, SetupError, SplitOpen};
use crate::ffi;
use crate::pages::{FileHandle, WalSync, file_pointer};
use crate::shm::{self, Region};
use lsr_core::{CommittedRead, ReadLockHold, SourceError, WalFileLen, WalSource};
use lsr_ltx::{PageSize, Timestamp};
use lsr_wal::{CommittedEnd, Salts};

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
    /// The database file's path, for its modified time (`WalSource::modified`).
    db_path: PathBuf,
    /// The file the WAL path named once the first read transaction was held. SQLite keeps that
    /// file open for the reader's whole life, so any other file at the path, or none, means it
    /// was deleted under the read lock; `None` when it could not be told.
    wal_id: Option<DatabaseFile>,
    page_size: PageSize,
    /// The current hold of the read transaction: a holder of this open's own, and how many
    /// times it has been taken.
    pub(crate) epoch: ReadLockHold,
    /// The id of the last barrier or write lock handed out, and of the last one handed back:
    /// a lock is outstanding while they differ. Whether the reader is pinned and whether the
    /// writer is in its transaction are not stored: they are whether SQLite has the
    /// connection in a transaction (`is_autocommit`), so a ROLLBACK that failed, or a
    /// transaction SQLite ended itself on an error, cannot leave a flag saying otherwise.
    issued: u64,
    returned: u64,
}

/// The file a stat described, where a stat identifies one (Unix only).
#[cfg(unix)]
fn file_id(meta: &Metadata) -> Option<DatabaseFile> {
    Some(DatabaseFile::of_metadata(meta))
}

#[cfg(not(unix))]
const fn file_id(_meta: &Metadata) -> Option<DatabaseFile> {
    None
}

/// The statement that takes the write lock without leaving a row (it is always rolled back).
const TAKE_WRITE_LOCK: &str = "INSERT INTO _litestream_lock (id) VALUES (1)";

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

pub(crate) fn connect(path: &Path, config: &SqliteConfig) -> Result<Connection, SqliteError> {
    // No CREATE: a path that names no database is an error, never a new empty one.
    let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_NOMUTEX;
    let conn = Connection::open(path, flags, config.vfs.as_deref())?;
    let handle = conn.handle();
    handle.busy_timeout(0)?;
    handle.exec("PRAGMA wal_autocheckpoint = 0")?;
    // These connections run the checkpoints: set, not left to a build default that
    // `LIBSQLITE3_FLAGS` can lower, so a backfill always syncs the database file.
    handle.exec("PRAGMA synchronous = FULL")?;
    Ok(conn)
}

/// `PRAGMA journal_size_limit` set to `limit` (see [`journal_size_limit`]).
fn set_journal_size_limit(conn: Handle<'_>, limit: Option<NonZeroU64>) -> Result<(), SqliteError> {
    conn.exec(&format!(
        "PRAGMA journal_size_limit = {}",
        journal_size_limit(limit)
    ))
}

/// The size SQLite reported, as the type pages are counted in.
pub(crate) fn page_size(bytes: u32) -> Result<PageSize, OpenError> {
    PageSize::new(bytes).ok_or(OpenError::PageSize { bytes })
}

/// `BEGIN; SELECT COUNT(1) FROM _litestream_seq` (Go `db.go:1288`): the select takes the read mark.
fn begin_read(reader: Handle<'_>) -> Result<(), SqliteError> {
    reader.exec("BEGIN")?;
    if let Err(err) = reader.query_int("SELECT COUNT(1) FROM _litestream_seq") {
        // The transaction is only open to be dropped; its error is the one to report.
        drop(reader.exec("ROLLBACK"));
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
    /// replicator closing after the application keeps the WAL), creates `_litestream_seq` and `_litestream_lock`
    /// (`db.go:1174`, `1180`, so Go and Rust leave the same schema), takes the first read
    /// transaction (the first epoch of a holder no earlier open had) and maps the wal-index. It
    /// returns once the read transaction is held, so nothing can checkpoint past it.
    ///
    /// It first checks that the SQLite answering is one of the builds this crate's layout reads
    /// are verified against (the `version` module), and refuses any other.
    ///
    /// # Errors
    /// [`OpenError`]; nothing stays open or pinned.
    pub fn open_with(path: &Path, config: &SqliteConfig) -> Result<Self, OpenError> {
        crate::version::check_linked()?;
        let sqlite = |source| OpenError::Sqlite {
            path: path.to_owned(),
            source,
        };
        let reader = connect(path, config).map_err(sqlite)?;
        let writer = connect(path, config).map_err(sqlite)?;
        let mode = writer
            .handle()
            .query_text("PRAGMA journal_mode = wal")
            .map_err(sqlite)?;
        if mode != "wal" {
            return Err(OpenError::NotWal { mode });
        }
        // When the replicator closes last, SQLite checkpoints; without this it also deletes the
        // WAL, and the next start finds nothing to continue.
        for conn in [&reader, &writer] {
            conn.handle().persist_wal().map_err(sqlite)?;
        }
        // The writer only: its sequence bump is the write that restarts the log after the
        // replicator's checkpoints, and SQLite cuts the file on the restarting connection alone.
        // Should a later step fail, the writer closes before the reader (declaration order), so
        // a last close is the reader's, which has no limit and keeps the WAL (see `Inner`'s drop).
        set_journal_size_limit(writer.handle(), config.journal_size_limit).map_err(sqlite)?;
        writer
            .handle()
            .exec(
                "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
                 CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);",
            )
            .map_err(sqlite)?;
        let bytes = reader
            .handle()
            .query_int("PRAGMA page_size")
            .map_err(sqlite)?;
        let bytes = u32::try_from(bytes).map_err(|_range| {
            sqlite(SqliteError::Type {
                wanted: "a page size",
            })
        })?;
        let page_size = page_size(bytes)?;
        begin_read(reader.handle()).map_err(sqlite)?;
        shm::guard(reader.handle(), path)?;
        let file = FileHandle::new(file_pointer(reader.handle(), MAIN)?)?;
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
            db_path: path.to_owned(),
            wal_id,
            page_size,
            epoch: ReadLockHold::first(),
            issued: 0,
            returned: 0,
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// The database file both connections hold open, from SQLite's own descriptors rather than
    /// the path, which may name another file by now: a caller that checked the path before the
    /// open compares this, not a second stat, so a file swapped in after the open, or between
    /// the reader's open and the writer's, cannot pass. `Ok(None)` when it cannot be told (a VFS
    /// other than SQLite's unix ones, or not Unix); the caller falls back to the path.
    ///
    /// # Errors
    /// [`SplitOpen`] when the reader and the writer hold different files.
    pub fn database_file(&self) -> Result<Option<DatabaseFile>, SplitOpen> {
        let inner = self.lock();
        let reader = crate::identity::opened(inner.reader.handle());
        let writer = crate::identity::opened(inner.writer.handle());
        match (reader, writer) {
            (Some(reader), Some(writer)) if reader != writer => Err(SplitOpen { reader, writer }),
            (Some(reader), Some(_)) => Ok(Some(reader)),
            (None, _) | (_, None) => Ok(None),
        }
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
        drop(set_journal_size_limit(self.writer.handle(), None));
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
        let Some(id) = &self.wal_id else {
            return Ok(());
        };
        match found.and_then(file_id) {
            Some(now) if now == *id => Ok(()),
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

    /// Whether the reader holds its read transaction now.
    pub(crate) fn read_lock_held(&self) -> bool {
        !self.reader.handle().is_autocommit()
    }

    /// Whether the writer holds a barrier or write lock now.
    fn held(&self) -> bool {
        !self.writer.handle().is_autocommit()
    }

    /// Takes the read transaction again (a new epoch).
    pub(crate) fn begin_read(&mut self) -> Result<(), SqliteError> {
        begin_read(self.reader.handle())?;
        self.epoch = self.epoch.next();
        Ok(())
    }

    /// Ends the read transaction, if one is held.
    pub(crate) fn end_read(&mut self) -> Result<(), SourceError> {
        if self.read_lock_held() {
            self.reader
                .handle()
                .exec("ROLLBACK")
                .map_err(|e| crate::error::source_error(&e))?;
        }
        Ok(())
    }

    /// After a retake failed, the next read takes the read lock again first: its new epoch tells the
    /// engine the old one lapsed. Every answer is under a read transaction or an error.
    pub(crate) fn ensure_read_lock_held(&mut self) -> Result<(), SourceError> {
        if self.read_lock_held() {
            return Ok(());
        }
        self.begin_read().map_err(|_lost| SourceError::ReadTxLost)
    }

    /// The writer connection, free for a statement of its own: refused while a lock is
    /// outstanding (whatever ran now would run inside the lock's transaction, and its release
    /// would roll it back). A transaction still open with none outstanding (its ROLLBACK failed
    /// after the lock was handed back) is rolled back first: a `BEGIN` inside it would fail on
    /// every later call, and a write inside it would be undone by the next lock's release.
    pub(crate) fn free_writer(&mut self) -> Result<Handle<'_>, SourceError> {
        if self.issued != self.returned {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        if self.held() {
            self.writer
                .handle()
                .exec("ROLLBACK")
                .map_err(|e| crate::error::source_error(&e))?;
        }
        Ok(self.writer.handle())
    }

    /// `BEGIN` then the lock-table insert (Go `db.go:2660`, `2759`), which is rolled back.
    /// One holder at a time ([`Inner::free_writer`]).
    pub(crate) fn begin_writer(&mut self) -> Result<u64, SourceError> {
        let writer = self.free_writer()?;
        writer
            .exec("BEGIN")
            .map_err(|e| crate::error::source_error(&e))?;
        if let Err(err) = writer.exec(TAKE_WRITE_LOCK) {
            drop(writer.exec("ROLLBACK"));
            return Err(crate::error::source_error(&err));
        }
        self.issued = self.issued.saturating_add(1);
        Ok(self.issued)
    }

    /// Releases the lock `token` names: only the outstanding one. The token is spent whether
    /// or not the ROLLBACK succeeds; a transaction it leaves open is the next lock's to end.
    pub(crate) fn end_writer(&mut self, token: u64) -> Result<(), SourceError> {
        if token != self.issued || token == self.returned {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        self.returned = token;
        if !self.held() {
            // SQLite already ended the transaction (an error rolls it back).
            return Ok(());
        }
        self.writer
            .handle()
            .exec("ROLLBACK")
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
        inner.ensure_read_lock_held()?;
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
        CommittedRead::new(end, header.backfilled, inner.epoch).ok_or(SourceError::Busy)
    }

    fn epoch(&self) -> ReadLockHold {
        self.lock().epoch
    }

    fn sync(&mut self) -> Result<(), SourceError> {
        let mut inner = self.lock();
        inner.ensure_read_lock_held()?;
        // A failed sync is not remembered: the kernel reports a failed writeback once, so the
        // next sync may succeed over bytes that never reached the disk. v1 goes on, as Go does
        // (`docs/decisions/D173.md`).
        match crate::pages::sync_wal(inner.reader.handle()) {
            Ok(()) => Ok(()),
            Err(WalSync::NoWal) => Err(SourceError::Io(io::ErrorKind::NotFound)),
            Err(WalSync::Failed(_rc)) => Err(SourceError::FsyncFailed),
        }
    }

    fn modified(&mut self) -> Result<Option<Timestamp>, SourceError> {
        let inner = self.lock();
        let db = std::fs::metadata(&inner.db_path).map_err(|err| io_error(&err))?;
        let wal = match std::fs::metadata(&inner.wal_path) {
            Ok(meta) => Some(meta),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(io_error(&err)),
        };
        // A WAL of its header alone (or none) holds no write: SQLite creates it, header-less,
        // when a connection opens the database, and the last close may cut it to nothing.
        let header = u64::try_from(lsr_wal::HEADER_LEN).unwrap_or(u64::MAX);
        let wal = wal.filter(|meta| meta.len() > header);
        let stamp = |meta: &Metadata| {
            let since = meta
                .modified()
                .ok()?
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?;
            i64::try_from(since.as_millis())
                .ok()
                .map(Timestamp::from_unix_millis)
        };
        Ok(stamp(&db).max(wal.as_ref().and_then(stamp)))
    }
}

/// Sets up one application connection as replication needs it (PLAN section 6):
/// `wal_autocheckpoint = 0`, so only the replicator checkpoints. Call it on every connection an
/// application opens, pooled ones included.
///
/// It leaves `SQLITE_FCNTL_PERSIST_WAL` alone, and an application should too: with the flag on,
/// a connection that leaves WAL mode keeps the WAL, SQLite goes on reading it, and a page-size
/// change made then corrupts the database (docs/decisions.md D123). The replicator's own
/// connections keep the WAL when they close last, so an application closes its connections
/// before it stops the replicator; one that closes last deletes the WAL, and the next start
/// takes a snapshot.
///
/// # Errors
/// [`SetupError::Sqlite`] when the pragma fails.
#[cfg(feature = "rusqlite")]
pub fn app_connection(conn: &rusqlite::Connection) -> Result<(), SetupError> {
    setup(Handle::of_rusqlite(conn).map_err(SetupError::Sqlite)?)
}

/// [`app_connection`] on any handle.
fn setup(conn: Handle<'_>) -> Result<(), SetupError> {
    conn.exec("PRAGMA wal_autocheckpoint = 0")
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
            Ok(Self::of_metadata(&std::fs::metadata(path)?))
        }
        #[cfg(not(unix))]
        {
            Ok(Self {
                path: std::fs::canonicalize(path)?,
            })
        }
    }

    /// The file a stat or an `fstat` described.
    #[cfg(unix)]
    pub(crate) fn of_metadata(meta: &Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
        }
    }
}

/// [`app_connection`], after checking that `conn` opens `database`: an application that sets up a
/// connection to some other file gets an error, not a replicator that never sees its writes.
fn setup_for(conn: Handle<'_>, database: &DatabaseFile) -> Result<(), SetupError> {
    let path = conn
        .main_filename()
        .filter(|path| !path.is_empty())
        .ok_or(SetupError::NoFile)?;
    let found = DatabaseFile::of(Path::new(&path)).map_err(|err| SetupError::Stat(err.kind()))?;
    if &found != database {
        return Err(SetupError::OtherDatabase);
    }
    setup(conn)
}

/// What one application connection needs to be set up for a replicator: the database file it
/// must open and the replicator's write hold, which the fail-fast busy handler follows. Cheap
/// to clone, `Send` and `'static`, so a pool's per-connection callback captures it.
#[derive(Clone, Debug)]
pub struct ConnectionSetup {
    database: DatabaseFile,
    held: crate::WritesHeld,
}

impl ConnectionSetup {
    /// The setup for connections to `database`, following `held`. A replicator builds it
    /// (`lsr_replica::Replicator::connection_setup`).
    #[must_use]
    pub const fn new(database: DatabaseFile, held: crate::WritesHeld) -> Self {
        Self { database, held }
    }

    /// Sets up a rusqlite connection: checks that it opens this database (device and inode),
    /// sets `wal_autocheckpoint = 0`, and installs the fail-fast busy handler.
    ///
    /// # Errors
    /// [`SetupError::NoFile`] for a connection with no file (in-memory or temporary);
    /// [`SetupError::OtherDatabase`] when its file is not this database; else as
    /// [`app_connection`] and [`crate::fail_fast_while`].
    #[cfg(feature = "rusqlite")]
    pub fn apply(&self, conn: &rusqlite::Connection) -> Result<(), SetupError> {
        setup_for(
            Handle::of_rusqlite(conn).map_err(SetupError::Sqlite)?,
            &self.database,
        )?;
        crate::fail_fast_while(conn, &self.held)
    }

    /// As [`ConnectionSetup::apply`], on a raw SQLite handle from any binding (sqlx, diesel,
    /// its own FFI). The handle stays the application's: nothing here closes it.
    ///
    /// # Safety
    /// `db` is an open connection, used by no other thread during the call, and opened by the
    /// SQLite library this crate links: a second copy of SQLite in the process does not share
    /// its locks with this one (PLAN section 6). Note that `wal_autocheckpoint` replaces any
    /// `sqlite3_wal_hook` the application set on the connection.
    ///
    /// # Errors
    /// As [`ConnectionSetup::apply`]; [`SetupError::Sqlite`] when the handle is null.
    #[expect(
        unsafe_code,
        reason = "only the caller can promise the raw handle is open and its own for the call"
    )]
    pub unsafe fn apply_raw(&self, db: *mut ffi::sqlite3) -> Result<(), SetupError> {
        // SAFETY: the caller promises an open handle of this library's SQLite that no other
        // thread uses during the call, and the view lives only for the call; nothing closes it.
        let conn = unsafe { Handle::borrow(db) }
            .ok_or_else(|| SetupError::Sqlite(SqliteError::misuse()))?;
        setup_for(conn, &self.database)?;
        crate::gate::install(conn, &self.held)
    }

    /// Whether the replicator holds the application's writes now.
    #[must_use]
    pub fn writes_held(&self) -> bool {
        self.held.is_held()
    }
}

/// Creates an empty database at `path` in WAL mode, as an application's first open would, or
/// leaves the database already there as it is apart from putting it in WAL mode (which the
/// replicator's start does anyway). For a first start against an empty replica.
///
/// # Errors
/// [`SqliteError`] when the open, the pragma or the close fails.
pub fn create_database(path: &Path) -> Result<(), SqliteError> {
    let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_NOMUTEX;
    let conn = Connection::open(path, flags, None)?;
    conn.handle().query_text("PRAGMA journal_mode = WAL")?;
    conn.close()
}

/// Opens the `file:` URI `uri` read-only, runs `sql`, and returns the first column of at most
/// `limit` rows as text, closing the connection before it returns. For a caller outside this
/// crate that needs one query on a file of its own (`lsr-replica`'s integrity check of a
/// restored database) and may not use `unsafe`.
///
/// # Errors
/// [`SqliteError`] when the open, the query or the close fails, or a value is not text.
pub fn read_only_rows(uri: &str, sql: &str, limit: usize) -> Result<Vec<String>, SqliteError> {
    let flags = ffi::SQLITE_OPEN_READONLY | ffi::SQLITE_OPEN_URI | ffi::SQLITE_OPEN_NOMUTEX;
    let conn = Connection::open_name(uri, flags, None)?;
    let rows = conn.handle().query_texts(sql, limit)?;
    conn.close()?;
    Ok(rows)
}

/// Reads back what [`app_connection`] sets, and that the connection syncs at all: its
/// `synchronous` level is the application's to choose, except OFF.
///
/// # Errors
/// [`SetupError::AutocheckpointOn`] for a connection that is not set up;
/// [`SetupError::SynchronousOff`] for one that never syncs; [`SetupError::Sqlite`] when a read
/// itself fails.
#[cfg(feature = "rusqlite")]
pub fn check_app_connection(conn: &rusqlite::Connection) -> Result<(), SetupError> {
    let conn = Handle::of_rusqlite(conn).map_err(SetupError::Sqlite)?;
    let pages = conn
        .query_int("PRAGMA wal_autocheckpoint")
        .map_err(SetupError::Sqlite)?;
    if pages != 0 {
        return Err(SetupError::AutocheckpointOn { pages });
    }
    let level = conn
        .query_int("PRAGMA synchronous")
        .map_err(SetupError::Sqlite)?;
    if level == 0 {
        return Err(SetupError::SynchronousOff);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opened(dir: &Path) -> Sqlite {
        let db = dir.join("app.db");
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.query_row("PRAGMA journal_mode = wal", [], |_row| Ok(()))
            .unwrap();
        conn.execute_batch("CREATE TABLE t(x)").unwrap();
        drop(conn);
        Sqlite::open(&db).unwrap()
    }

    /// The replicator's connections run its checkpoints, so they sync the database file and
    /// the WAL at FULL whatever the build's default is (`LIBSQLITE3_FLAGS` can change it).
    #[test]
    fn the_replicator_s_connections_sync_at_full() {
        let dir = tempfile::TempDir::new().unwrap();
        let sqlite = opened(dir.path());
        let inner = sqlite.lock();
        for conn in [&inner.reader, &inner.writer] {
            let level = conn.handle().query_int("PRAGMA synchronous").unwrap();
            assert_eq!(level, 2, "FULL");
        }
    }

    /// A ROLLBACK that fails leaves the writer inside its transaction. When that was tracked
    /// by hand, the lock counted as released and every later `begin_writer` failed on `BEGIN`
    /// inside the open transaction. The fault VFS cannot fail a ROLLBACK (its unlocks never
    /// fail, and rolling back an unspilled WAL transaction does no other file I/O), so the
    /// test opens the transaction directly: the state such a failure leaves.
    #[test]
    fn a_writer_left_in_a_transaction_does_not_wedge_later_locks() {
        let dir = tempfile::TempDir::new().unwrap();
        let sqlite = opened(dir.path());
        let mut inner = sqlite.lock();
        inner.writer.handle().exec("BEGIN").unwrap();
        let token = inner.begin_writer().unwrap();
        assert!(inner.held());
        assert_eq!(
            inner.begin_writer(),
            Err(SourceError::Io(io::ErrorKind::InvalidInput)),
            "one holder at a time"
        );
        inner.end_writer(token).unwrap();
        assert!(!inner.held());
        let again = inner.begin_writer().unwrap();
        assert_eq!(
            inner.end_writer(token),
            Err(SourceError::Io(io::ErrorKind::InvalidInput)),
            "a stale token releases nothing"
        );
        inner.end_writer(again).unwrap();
        assert_eq!(
            inner.end_writer(again),
            Err(SourceError::Io(io::ErrorKind::InvalidInput)),
            "a lock is released once"
        );
    }

    /// The path is swapped between the reader's open and the writer's: each descriptor names
    /// its own file, and the identity is refused rather than taken from either.
    #[cfg(unix)]
    #[test]
    fn a_reader_and_writer_on_different_files_are_a_split_open() {
        let dir = tempfile::TempDir::new().unwrap();
        let sqlite = opened(dir.path());
        let reader = DatabaseFile::of(&dir.path().join("app.db")).unwrap();
        let other = tempfile::TempDir::new().unwrap();
        let swapped = opened(other.path());
        let writer = DatabaseFile::of(&other.path().join("app.db")).unwrap();
        drop(swapped);
        sqlite.lock().writer =
            connect(&other.path().join("app.db"), &SqliteConfig::default()).unwrap();
        assert_eq!(sqlite.database_file(), Err(SplitOpen { reader, writer }));
    }

    /// SQLite can end a read transaction itself (an I/O error rolls it back). The read lock is
    /// whatever SQLite says, so the next read takes it again, in a new epoch, instead of
    /// answering outside any read transaction.
    #[test]
    fn a_read_transaction_sqlite_ended_is_taken_again() {
        let dir = tempfile::TempDir::new().unwrap();
        let sqlite = opened(dir.path());
        let mut inner = sqlite.lock();
        assert!(inner.read_lock_held());
        let epoch = inner.epoch;
        inner.reader.handle().exec("ROLLBACK").unwrap();
        inner.ensure_read_lock_held().unwrap();
        assert!(inner.read_lock_held());
        assert_eq!(inner.epoch, epoch.next());
        inner.end_read().unwrap();
        assert!(!inner.read_lock_held());
        inner.end_read().unwrap();
    }

    #[test]
    fn a_page_size_sqlite_does_not_allow_is_refused() {
        assert!(matches!(
            page_size(1000),
            Err(OpenError::PageSize { bytes: 1000 })
        ));
        assert_eq!(page_size(4096).ok(), PageSize::new(4096));
    }
}
