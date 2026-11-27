//! The crate's two boundary errors, the guard's refusals, and how SQLite's result codes become
//! core's [`SourceError`] inside the trait methods.

use std::ffi::c_int;
use std::io;
use std::path::PathBuf;

use lsr_core::SourceError;

pub use crate::capi::SqliteError;
use crate::ffi;

/// Why [`crate::Sqlite::open`] gave no handle. Nothing is pinned when it returns one of these.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// SQLite refused to open the file or to run a setup statement (the file is missing, not a
    /// database, or locked by a writer that will not wait).
    #[error("SQLite refused {path}: {source}")]
    Sqlite {
        /// The database path as given.
        path: PathBuf,
        /// What SQLite said.
        source: SqliteError,
    },
    /// The SQLite answering at run time is not a build this crate's reads of SQLite's private
    /// layouts have been verified against: a system library outside the verified list, SQLCipher,
    /// a loadable-extension build, a libsqlite3-sys version outside the supported range.
    #[error(
        "the SQLite in use is version {} ({source_id}), not a build this library is verified against ({}): link the SQLite bundled by libsqlite3-sys 0.37 to 0.40, or one of those builds",
        dotted(*.found),
        .verified.iter().map(|v| dotted(*v)).collect::<Vec<_>>().join(", ")
    )]
    SqliteBuild {
        /// `sqlite3_libversion_number()` of the running library.
        found: c_int,
        /// `sqlite3_sourceid()` of the running library.
        source_id: String,
        /// The version numbers of the verified builds, oldest first.
        verified: &'static [c_int],
    },
    /// `PRAGMA journal_mode = wal` left the database in another mode (an in-memory database, or a
    /// VFS without shared memory). Replication reads the WAL, so there is nothing to replicate.
    #[error("the database is in journal mode {mode:?}, not WAL")]
    NotWal {
        /// The mode SQLite reported.
        mode: String,
    },
    /// SQLite reported a page size this library has no type for. SQLite itself allows only
    /// powers of two from 512 to 65536, so this is a VFS or build that lies.
    #[error("page size {bytes} is not one SQLite allows")]
    PageSize {
        /// The size SQLite reported.
        bytes: u32,
    },
    /// The reader's file handle or wal-index could not be used without this library opening a
    /// descriptor of its own.
    #[error("SQLite's own file handle cannot be used: {0}")]
    Refused(#[from] Refusal),
}

/// A `SQLITE_VERSION_NUMBER` as SQLite writes it: 3054001 is "3.54.1".
#[must_use]
pub fn dotted(number: c_int) -> String {
    let major = number.checked_div(1_000_000).unwrap_or(0);
    let minor = number
        .checked_rem(1_000_000)
        .unwrap_or(0)
        .checked_div(1_000)
        .unwrap_or(0);
    let patch = number.checked_rem(1_000).unwrap_or(0);
    format!("{major}.{minor}.{patch}")
}

/// The reader and the writer opened different database files: the path was swapped between
/// their two opens.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("the reader and the writer hold different database files")]
pub struct SplitOpen {
    pub(crate) reader: crate::DatabaseFile,
    pub(crate) writer: crate::DatabaseFile,
}

impl SplitOpen {
    /// The reader's file.
    #[must_use]
    pub const fn reader(&self) -> &crate::DatabaseFile {
        &self.reader
    }

    /// The writer's file.
    #[must_use]
    pub const fn writer(&self) -> &crate::DatabaseFile {
        &self.writer
    }
}

/// Why the reader's `sqlite3_file` was not used. Each case would otherwise make SQLite open a
/// `-shm` descriptor on this library's behalf, or call through a pointer that is not there.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// The reader holds no read transaction, so its WAL may not have mapped `-shm` yet.
    #[error("the reader holds no read transaction")]
    NoReadTransaction,
    /// The reader's journal mode is not WAL.
    #[error("the reader's journal mode is {mode:?}")]
    NotWal {
        /// The mode it reported.
        mode: String,
    },
    /// Exclusive locking keeps the wal-index in heap memory, not in `-shm`.
    #[error("the reader's locking mode is {mode:?}")]
    NotNormalLocking {
        /// The mode it reported.
        mode: String,
    },
    /// `SQLITE_FCNTL_FILE_POINTER` failed.
    #[error("SQLITE_FCNTL_FILE_POINTER failed with {rc}")]
    NoFilePointer {
        /// SQLite's result code.
        rc: c_int,
    },
    /// The main database file is not open (`pMethods` is null).
    #[error("the database file is not open")]
    FileNotOpen,
    /// The VFS's methods predate shared memory (`iVersion` below 2).
    #[error("the VFS's io methods are version {version}, before shared memory")]
    OldIoMethods {
        /// The table's `iVersion`.
        version: c_int,
    },
    /// The VFS leaves a method this library calls null.
    #[error("the VFS has no {name}")]
    MissingMethod {
        /// The method's name in `sqlite3_io_methods`.
        name: &'static str,
    },
    /// `xShmMap` returned an error (`SQLITE_READONLY` included: the mapping is read-only).
    #[error("xShmMap failed with {rc}")]
    MapFailed {
        /// SQLite's result code.
        rc: c_int,
    },
    /// `xShmMap` succeeded but region 0 does not exist yet.
    #[error("wal-index region 0 is not mapped")]
    Unmapped,
    /// The region is not 4-byte aligned, so it cannot be read as 32-bit words.
    #[error("the wal-index region is misaligned")]
    Misaligned,
}

/// Why an application connection is not set up as replication needs (PLAN section 6).
#[derive(Debug, thiserror::Error)]
pub enum SetupError {
    /// A pragma failed.
    #[error("SQLite refused a setup pragma: {0}")]
    Sqlite(SqliteError),
    /// The connection would checkpoint on its own, behind the replicator's back.
    #[error("wal_autocheckpoint is {pages}, not 0")]
    AutocheckpointOn {
        /// The configured page threshold.
        pages: i64,
    },
    /// The connection never syncs (`synchronous = OFF`): a power loss can take back commits
    /// the replica already holds, and the application then forks history under the same WAL.
    #[error("synchronous is OFF, so commits are never synced: set NORMAL or FULL")]
    SynchronousOff,
    /// The connection has no database file (in-memory or temporary), so it cannot be the
    /// replicated database.
    #[error("the connection has no database file")]
    NoFile,
    /// The connection's file could not be looked at.
    #[error("the connection's database file cannot be read: {0:?}")]
    Stat(std::io::ErrorKind),
    /// The connection opens another file than the replicated database.
    #[error("the connection opens another database than the one replicated")]
    OtherDatabase,
}

/// Sorts a SQLite result code (primary or extended) the way a sync loop needs it.
pub(crate) const fn rc_error(rc: c_int) -> SourceError {
    match rc & 0xff {
        ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => SourceError::Busy,
        ffi::SQLITE_FULL => SourceError::DiskFull,
        _ => SourceError::Io(io::ErrorKind::Other),
    }
}

/// [`rc_error`] for a failed call; one that is not SQLite's own is an I/O error.
pub(crate) fn source_error(err: &SqliteError) -> SourceError {
    err.code()
        .map_or(SourceError::Io(io::ErrorKind::Other), rc_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(code: c_int) -> SqliteError {
        SqliteError::Code {
            code,
            message: String::new(),
        }
    }

    #[test]
    fn result_codes_sort_into_source_errors() {
        assert_eq!(source_error(&failure(ffi::SQLITE_BUSY)), SourceError::Busy);
        assert_eq!(
            source_error(&failure(ffi::SQLITE_BUSY_SNAPSHOT)),
            SourceError::Busy
        );
        assert_eq!(
            source_error(&failure(ffi::SQLITE_LOCKED)),
            SourceError::Busy
        );
        assert_eq!(
            source_error(&failure(ffi::SQLITE_FULL)),
            SourceError::DiskFull
        );
        assert_eq!(
            source_error(&failure(ffi::SQLITE_IOERR_READ)),
            SourceError::Io(io::ErrorKind::Other)
        );
        assert_eq!(
            source_error(&SqliteError::NoRow),
            SourceError::Io(io::ErrorKind::Other)
        );
    }
}
