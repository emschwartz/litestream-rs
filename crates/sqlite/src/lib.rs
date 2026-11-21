//! The sync engine's doors on a live SQLite database in the host's process.
//!
//! [`Sqlite`] implements `lsr_core`'s `WalSource`, `PageSource` and `Checkpointer` over two
//! rusqlite connections, the way Go's `db.go` uses its pool. The reader holds one long read
//! transaction (`BEGIN; SELECT COUNT(1) FROM _litestream_seq`, `db.go:1288`), so SQLite cannot
//! checkpoint past what has been replicated; it ends only inside `Checkpointer::checkpoint`,
//! which is what moves the `ReadEpoch`. The writer takes the write barrier, the write lock and
//! the sequence bump. Both connections have a zero busy timeout: a contended lock is
//! `SourceError::Busy` at once, and the caller's policy decides what waits.
//!
//! The one rule this crate exists to keep: **no descriptor on the database file or its `-shm`
//! is ever opened and closed here.** Closing any descriptor on a file drops every POSIX lock the
//! process holds on it, the host's SQLite locks included. So the committed end comes from the
//! wal-index header through SQLite's own mapping of `-shm` (the `shm` module's doc explains the guard and
//! the read), and database pages through the reader's own `sqlite3_file` (`xRead`, `xFileSize`). Only
//! the `-wal` file is opened with `std::fs`: SQLite takes no POSIX lock on it. The crate's
//! `clippy.toml` disallows the `std::fs` opens; the WAL open is the one reasoned exception.
//!
//! Errors inside the trait methods are core's `SourceError`: `Busy` for a contended lock or a
//! wal-index header a writer is mid-way through (retry), `DiskFull`, `Short` for a page past the
//! end of the file, `ReadTxLost` when the read transaction cannot be retaken (the next call
//! retakes it, in a new epoch), and `Io` for the rest. This backend never returns `FsyncFailed`
//! (it syncs nothing) or `Unlinked` (it does not yet detect a WAL deleted under it).
//!
//! [`app_connection`] and [`check_app_connection`] set up and check the host's own connections
//! (`SQLITE_FCNTL_PERSIST_WAL` on, `wal_autocheckpoint = 0`, PLAN section 6).
//!
//! The `testing` feature adds [`testing`]: the fault-injecting VFS and the harness of the
//! real-SQLite fault suite in `tests/`.
//!
//! `unsafe` is allowed in this crate only, one reasoned `#[expect(unsafe_code)]` per site.

mod checkpoint;
mod conn;
mod error;
mod pages;
mod shm;
#[cfg(feature = "testing")]
pub mod testing;

pub use conn::{Sqlite, SqliteConfig, app_connection, check_app_connection};
pub use error::{OpenError, Refusal, SetupError};
