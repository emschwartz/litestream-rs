//! The crate's boundary errors.

use std::io;

use lsr_local::CreateError;

/// Why [`crate::Replicator::start`] returned no replicator. Nothing is pinned or running when
/// it returns one of these.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The database path cannot be used (missing, unreadable, or naming no file).
    #[error("the database cannot be opened: {kind:?}")]
    Open {
        /// What the file system said.
        kind: io::ErrorKind,
    },
    /// The database is not in WAL mode and could not be put in it (an in-memory database, or
    /// a VFS without shared memory). Replication reads the WAL.
    #[error("the database is in journal mode {journal_mode:?}, not WAL")]
    NotWal {
        /// The mode SQLite reported.
        journal_mode: String,
    },
    /// A replicator for this file (device and inode, not path) is running in this process.
    #[error("this database is already replicated in this process")]
    AlreadyReplicated,
    /// The meta directory cannot be created.
    #[error("the meta directory cannot be created: {kind:?}")]
    MetaDir {
        /// What the file system said.
        kind: io::ErrorKind,
    },
    /// SQLite refused to open the database or to set it up.
    #[error("SQLite refused the database (result code {code})")]
    Sqlite {
        /// SQLite's extended result code; -1 when the error was not SQLite's own.
        code: i32,
    },
    /// SQLite's file handle or wal-index cannot be used without this library opening a
    /// descriptor of its own on the database, which would drop the host's locks.
    #[error("SQLite's own file handle cannot be used: {0}")]
    Refused(#[from] lsr_sqlite::Refusal),
    /// SQLite reported a page size no SQLite build allows.
    #[error("page size {bytes} is not one SQLite allows")]
    PageSize {
        /// The size SQLite reported.
        bytes: u32,
    },
    /// The replicator thread could not be started. Each database needs its own thread
    /// (`lsr_replica::stack`), so the usual cause is the process's thread limit.
    #[error(
        "the replicator thread cannot be started ({kind:?}): each database runs on a thread of its own, and the process may be at its thread limit (RLIMIT_NPROC, kernel.threads-max) or out of memory"
    )]
    Thread {
        /// What the OS said.
        kind: io::ErrorKind,
    },
}

impl From<lsr_sqlite::OpenError> for StartError {
    fn from(err: lsr_sqlite::OpenError) -> Self {
        match err {
            lsr_sqlite::OpenError::Sqlite { source, .. } => Self::Sqlite {
                code: source.sqlite_error().map_or(-1, |e| e.extended_code),
            },
            lsr_sqlite::OpenError::NotWal { mode } => Self::NotWal { journal_mode: mode },
            lsr_sqlite::OpenError::PageSize { bytes } => Self::PageSize { bytes },
            lsr_sqlite::OpenError::Refused(refusal) => Self::Refused(refusal),
        }
    }
}

/// Why [`crate::restore`] or [`crate::restore_if_missing`] published nothing.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The destination cannot take a restore: it exists, a stale sidecar sits beside it, or
    /// its scratch file cannot be made.
    #[error("{0}")]
    Destination(#[from] CreateError),
    /// The restore itself failed.
    #[error("{0}")]
    Restore(#[from] lsr_core::RestoreError),
}
