//! A live SQLite database as a source for the sync engine: test support, not production code.
//!
//! [`LiveSqlite`] implements the three doors of `lsr_core` (`WalSource`, `PageSource`, `Checkpointer`)
//! over two rusqlite connections, the way Go's `db.go` uses its pool: a reader that holds one
//! long read transaction (`BEGIN; SELECT COUNT(1) FROM _litestream_seq`, `db.go:1288`) so SQLite
//! cannot checkpoint past what has been replicated, and a writer for the write barrier, the
//! write lock and the sequence bump. The read transaction ends only inside
//! [`Checkpointer::checkpoint`], which is what moves the [`ReadEpoch`].
//!
//! The committed end is read from the `-shm` file with plain file reads: two 48-byte copies of
//! the wal-index header at 0 and 48 must be equal (a writer mid-update leaves them different and the
//! read is [`SourceError::Busy`], as SQLite's own readers retry), `mxFrame` is at byte 16, the
//! salts at 32 and `nBackfill` at 96. `mxFrame` and `nBackfill` are native byte order (the
//! wal-index is memory, not a file format); the salts are the WAL header's own bytes copied
//! verbatim, so they read big-endian (a plan that said "native" for them was wrong: Go reads the
//! file header, and the bytes in `-shm` equal it). Pages are read from the database file at `(pgno - 1) * page_size`.
//!
//! The three doors are one object: core's `checkpoint` takes them as three arguments, so a
//! [`LiveSqlite`] is a cheap handle (`Clone`) over shared state, and every clone is the same pair
//! of connections and the same read transaction. Both connections have a zero busy timeout: a
//! contended lock is [`SourceError::Busy`] at once, and the caller's policy decides what waits.
//!
//! Phase 3's `lsr-sqlite` replaces this and must pass `tests/live_source.rs` unchanged.

use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use lsr_core::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, CommittedRead, PageSource, ReadEpoch,
    SourceError, WalFileLen, WalSource, WriteLock,
};
use lsr_ltx::{PageNo, PageSize};
use lsr_wal::{CommittedEnd, Salts};
use rusqlite::{Connection, ErrorCode};

use crate::error::CompatError;

/// The wal-index header is two identical 48-byte copies.
const HEADER_COPY: usize = 48;
/// Bytes of the `-shm` file that hold the header copies, the check point info up to `nBackfill`.
const SHM_PREFIX: usize = 100;
/// `WalIndexHdr.isInit`: zero until a writer has built the header.
const IS_INIT_AT: usize = 12;
const MX_FRAME_AT: usize = 16;
const SALT_AT: usize = 32;
const N_BACKFILL_AT: usize = 96;

/// The statement that takes the write lock without leaving a row (it is always rolled back).
const TAKE_WRITE_LOCK: &str = "INSERT INTO _litestream_lock (id) VALUES (1)";

/// A live SQLite database, as the three doors of the sync engine.
#[derive(Clone, Debug)]
pub struct LiveSqlite {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    reader: Connection,
    writer: Connection,
    wal_path: PathBuf,
    shm_path: PathBuf,
    db: File,
    page_size: PageSize,
    /// How many read transactions have been taken; the current one is this many.
    epoch: u64,
    /// The id of the barrier or write lock the writer connection holds.
    held: Option<u64>,
    next_token: u64,
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

/// Sorts a SQLite error the way a sync loop needs it.
fn source_error(err: &rusqlite::Error) -> SourceError {
    match err.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => SourceError::Busy,
        Some(ErrorCode::DiskFull) => SourceError::DiskFull,
        _ => SourceError::Io(io::ErrorKind::Other),
    }
}

fn io_error(err: &io::Error) -> SourceError {
    if err.kind() == io::ErrorKind::StorageFull {
        SourceError::DiskFull
    } else {
        SourceError::Io(err.kind())
    }
}

/// The four bytes at `at` in `buf`.
fn word(buf: &[u8], at: usize) -> Option<[u8; 4]> {
    let end = at.checked_add(4)?;
    buf.get(at..end)?.try_into().ok()
}

impl LiveSqlite {
    /// Opens the database at `path`, which must already exist, puts it in WAL mode with
    /// autocheckpoint off, creates `_litestream_seq` and `_litestream_lock` (`db.go:1174`, `1180`)
    /// and takes the first read transaction (epoch 1).
    ///
    /// # Errors
    /// [`CompatError`] when SQLite refuses a statement, the file cannot be opened, or the page size is not one SQLite allows.
    pub fn open(path: &Path) -> Result<Self, CompatError> {
        let reader = connect(path)?;
        let writer = connect(path)?;
        let mode: String = writer.query_row("PRAGMA journal_mode = wal", [], |row| row.get(0))?;
        if mode != "wal" {
            return Err(CompatError::Fixture(format!(
                "{} did not enter WAL mode: {mode}",
                path.display()
            )));
        }
        writer.execute_batch(
            "CREATE TABLE IF NOT EXISTS _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
             CREATE TABLE IF NOT EXISTS _litestream_lock (id INTEGER);",
        )?;
        let raw: u32 = reader.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        let page_size = PageSize::new(raw)
            .ok_or_else(|| CompatError::Fixture(format!("page size {raw} is not SQLite's")))?;
        let db = File::open(path).map_err(CompatError::io(path))?;
        let mut inner = Inner {
            reader,
            writer,
            wal_path: suffixed(path, "-wal"),
            shm_path: suffixed(path, "-shm"),
            db,
            page_size,
            epoch: 0,
            held: None,
            next_token: 0,
        };
        inner.begin_read()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// The write-ahead log's path, for tests that copy or inspect it.
    #[must_use]
    pub fn wal_path(&self) -> PathBuf {
        self.lock().wal_path.clone()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic while holding the lock leaves plain data behind; the next test step reports it.
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn connect(path: &Path) -> Result<Connection, CompatError> {
    let conn = Connection::open(path)?;
    conn.busy_timeout(Duration::ZERO)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)?;
    Ok(conn)
}

impl Inner {
    /// `BEGIN; SELECT COUNT(1) FROM _litestream_seq` (Go `db.go:1288`): the select takes the read mark.
    fn begin_read(&mut self) -> Result<(), rusqlite::Error> {
        self.reader.execute_batch("BEGIN")?;
        let read: Result<i64, _> =
            self.reader
                .query_row("SELECT COUNT(1) FROM _litestream_seq", [], |row| row.get(0));
        if let Err(err) = read {
            // The transaction is only open to be dropped; its error is the one to report.
            drop(self.reader.execute_batch("ROLLBACK"));
            return Err(err);
        }
        self.epoch = self.epoch.saturating_add(1);
        Ok(())
    }

    fn take_token(&mut self) -> u64 {
        self.next_token = self.next_token.saturating_add(1);
        self.next_token
    }

    /// `BEGIN` then the lock-table insert (Go `db.go:2660`, `2759`), which is rolled back.
    fn begin_writer(&mut self) -> Result<u64, SourceError> {
        self.writer
            .execute_batch("BEGIN")
            .map_err(|e| source_error(&e))?;
        if let Err(err) = self.writer.execute(TAKE_WRITE_LOCK, []) {
            drop(self.writer.execute_batch("ROLLBACK"));
            return Err(source_error(&err));
        }
        let token = self.take_token();
        self.held = Some(token);
        Ok(token)
    }

    fn end_writer(&mut self, token: u64) -> Result<(), SourceError> {
        if self.held != Some(token) {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        self.held = None;
        self.writer
            .execute_batch("ROLLBACK")
            .map_err(|e| source_error(&e))
    }
}

impl WalSource for LiveSqlite {
    type Wal = File;

    fn open(&mut self) -> Result<Option<File>, SourceError> {
        match File::open(&self.lock().wal_path) {
            Ok(file) => Ok(Some(file)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(io_error(&err)),
        }
    }

    fn len(&mut self) -> Result<Option<WalFileLen>, SourceError> {
        match std::fs::metadata(&self.lock().wal_path) {
            Ok(meta) => Ok(Some(WalFileLen::new(meta.len()))),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(io_error(&err)),
        }
    }

    fn committed(&mut self) -> Result<CommittedRead, SourceError> {
        let (path, page_size) = {
            let inner = self.lock();
            (inner.shm_path.clone(), inner.page_size)
        };
        let mut buf = [0_u8; SHM_PREFIX];
        File::open(&path)
            .and_then(|mut file| file.read_exact(&mut buf))
            .map_err(|err| io_error(&err))?;
        let (first, second) = (
            buf.get(..HEADER_COPY),
            buf.get(HEADER_COPY..2 * HEADER_COPY),
        );
        let initialized = buf.get(IS_INIT_AT).is_some_and(|init| *init != 0);
        if first != second || !initialized {
            return Err(SourceError::Busy);
        }
        let (Some(mx_frame), Some(salt1), Some(salt2), Some(backfilled)) = (
            word(&buf, MX_FRAME_AT).map(u32::from_ne_bytes),
            word(&buf, SALT_AT).map(u32::from_be_bytes),
            word(&buf, SALT_AT.saturating_add(4)).map(u32::from_be_bytes),
            word(&buf, N_BACKFILL_AT).map(u32::from_ne_bytes),
        ) else {
            return Err(SourceError::Busy);
        };
        let end = CommittedEnd::new(mx_frame, Salts { salt1, salt2 }, page_size);
        // nBackfill is not under the header's copy check: a value above mxFrame is a read
        // racing a restart, which a retry resolves.
        CommittedRead::new(end, backfilled).ok_or(SourceError::Busy)
    }

    fn epoch(&self) -> ReadEpoch {
        ReadEpoch::new(self.lock().epoch)
    }
}

impl PageSource for LiveSqlite {
    fn page_count(&mut self) -> Result<u32, SourceError> {
        let inner = self.lock();
        let len = inner.db.metadata().map_err(|err| io_error(&err))?.len();
        let pages = len
            .checked_div(u64::from(inner.page_size.get()))
            .unwrap_or(0);
        u32::try_from(pages).map_err(|_big| SourceError::Io(io::ErrorKind::InvalidData))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        let mut inner = self.lock();
        let size = u64::from(inner.page_size.get());
        if u64::try_from(buf.len()) != Ok(size) {
            return Err(SourceError::Io(io::ErrorKind::InvalidInput));
        }
        let offset = u64::from(pgno.get())
            .checked_sub(1)
            .and_then(|before| before.checked_mul(size))
            .ok_or(SourceError::Io(io::ErrorKind::InvalidInput))?;
        inner
            .db
            .seek(SeekFrom::Start(offset))
            .and_then(|_at| inner.db.read_exact(buf))
            .map_err(|err| {
                if err.kind() == io::ErrorKind::UnexpectedEof {
                    SourceError::Short { pgno }
                } else {
                    io_error(&err)
                }
            })
    }
}

impl Checkpointer for LiveSqlite {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.lock().begin_writer().map(Barrier::new)
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.lock().end_writer(barrier.id())
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let mut inner = self.lock();
        inner
            .reader
            .execute_batch("ROLLBACK")
            .map_err(|e| source_error(&e))?;
        let pragma = match mode {
            CheckpointMode::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            CheckpointMode::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        };
        let ran: Result<(i64, i64, i64), _> = inner.reader.query_row(pragma, [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        });
        // The read transaction comes back whether or not the checkpoint ran, as Go defers it.
        let retaken = inner.begin_read();
        let (busy, log, checkpointed) = ran.map_err(|e| source_error(&e))?;
        retaken.map_err(|_lost| SourceError::ReadTxLost)?;
        let frames =
            |n: i64| u32::try_from(n).map_err(|_neg| SourceError::Io(io::ErrorKind::InvalidData));
        Ok(CheckpointRun {
            busy: busy != 0,
            log: frames(log)?,
            checkpointed: frames(checkpointed)?,
            epoch: ReadEpoch::new(inner.epoch),
        })
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.lock()
            .writer
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
