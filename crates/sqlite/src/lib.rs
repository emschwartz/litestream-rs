//! The sync engine's doors on a live SQLite database in the application's process.
//!
//! [`Sqlite`] implements `lsr_core`'s `WalSource`, `PageSource` and `Checkpointer` over two
//! connections opened on SQLite's C API (`libsqlite3-sys`, through the `capi` module), the way
//! Go's `db.go` uses its pool. The reader holds one long read
//! transaction (`BEGIN; SELECT COUNT(1) FROM _litestream_seq`, `db.go:1288`), so SQLite cannot
//! checkpoint past what has been replicated; it ends only inside `Checkpointer::checkpoint`,
//! which is what moves the `ReadLockHold`. The writer takes the write barrier, the write lock and
//! the sequence bump. Both connections have a zero busy timeout: a contended lock is
//! `SourceError::Busy` at once, and the caller's policy decides what waits.
//!
//! The writer also carries `PRAGMA journal_size_limit` ([`SqliteConfig::journal_size_limit`],
//! 64 MiB by default). SQLite cuts the WAL file back to that size only on the connection whose
//! commit restarts the log, and after the replicator's own checkpoint that commit is the
//! sequence bump; without it the file keeps its largest size for good (upstream Litestream
//! #1605). The cut drops only bytes past the new generation's first transaction, which hold
//! frames of the generation the restart just ended, every one of them already backfilled into
//! the database file. The limit is turned off again before the connections close, because a
//! last close with a limit set truncates the WAL to zero bytes and the next start would snapshot.
//!
//! The one rule this crate exists to keep: **no descriptor on the database file or its `-shm`
//! is ever opened and closed here.** Closing any descriptor on a file drops every POSIX lock the
//! process holds on it, the application's SQLite locks included. So the committed end comes from the
//! wal-index header through SQLite's own mapping of `-shm` (the `shm` module's doc explains the guard and
//! the read), and database pages through the reader's own `sqlite3_file` (`xRead`, `xFileSize`). Only
//! the `-wal` file is opened with `std::fs`: SQLite takes no POSIX lock on it. The crate's
//! `clippy.toml` disallows the `std::fs` opens; the WAL open is the one reasoned exception.
//!
//! The application is held to the same rule, and to one more: **one SQLite library per
//! process.** Its connections must use the copy of SQLite this crate links (the one bundled by
//! `libsqlite3-sys`). Each copy keeps its own table of the POSIX locks it holds, so a second
//! copy (a system SQLite through pkg-config, SQLCipher, a loadable-extension build) cannot see
//! this copy's locks and drops them when it closes a descriptor on the same file: SQLite's own
//! "multiple copies of SQLite linked into the same application" corruption case. The `identity`
//! and `shm` modules also read the bundled copy's file and wal-index layouts directly. Nor may
//! the application close a descriptor it opened on the database file or its `-shm` while its
//! connections are open, for the reason above.
//!
//! Errors inside the trait methods are core's `SourceError`: `Busy` for a contended lock or a
//! wal-index header a writer is mid-way through (retry), `DiskFull`, `Short` for a page past the
//! end of the file, `ReadTxLost` when the read transaction cannot be retaken (the next call
//! retakes it, in a new epoch), `Unlinked` when the WAL path no longer names the file SQLite
//! holds open (deleted, or replaced, by another process: SQLite keeps writing the deleted file,
//! out of every reader's reach), `FsyncFailed` when its sync of the WAL failed (the next sync
//! is made again, with nothing remembered), and `Io` for the rest.
//!
//! [`WriteGate`] is fail-stop's write gate: a write transaction held on a third connection of the
//! replicator's own, which stops every other writer while it holds (the `gate` module).
//! [`fail_fast_while`] makes a blocked write on an application connection fail at once while
//! the gate holds, instead of waiting out its busy timeout.
//!
//! [`app_connection`] and [`check_app_connection`] set up and check the application's own connections
//! (`wal_autocheckpoint = 0`, PLAN section 6; never `SQLITE_FCNTL_PERSIST_WAL`, which only the
//! replicator's own two connections set, see [`app_connection`]).
//! [`app_connection_for`] first checks that the connection opens a given [`DatabaseFile`]
//! (device and inode), and [`app_connection_raw`] does the same on a raw handle, for an application
//! whose connections are not rusqlite's; it is here because only this crate may use `unsafe`.
//! [`Sqlite::database_file`] says which file the reader opened, from SQLite's own descriptor
//! (the `identity` module), for a caller that must not trust a second stat of the path.
//!
//! The `testing` feature adds [`testing`]: the fault-injecting VFS and the harness of the
//! real-SQLite fault suite in `tests/`.
//!
//! `unsafe` is allowed in this crate only, one reasoned `#[expect(unsafe_code)]` per site. Every
//! call into SQLite's C API is in the `capi` module; the calls through a VFS's methods and the
//! reads of SQLite's private layouts are in `pages`, `shm` and `identity`, beside the
//! explanation of each.

mod capi;
mod checkpoint;
mod conn;
mod error;
mod gate;
mod identity;
mod pages;
mod shm;
#[cfg(feature = "testing")]
pub mod testing;
mod version;

pub use conn::{
    DEFAULT_JOURNAL_SIZE_LIMIT, DatabaseFile, Sqlite, SqliteConfig, app_connection,
    app_connection_for, app_connection_raw, check_app_connection, read_only_rows,
};
pub use error::{OpenError, Refusal, SetupError, SplitOpen, SqliteError};
pub use gate::{WriteGate, WritesHeld, fail_fast_while};
/// SQLite's C interface (`libsqlite3-sys`), for the raw handle [`app_connection_raw`] takes.
pub use libsqlite3_sys as ffi;
