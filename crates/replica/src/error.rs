//! The crate's boundary errors.

use std::io;

use crate::local::CreateError;

/// Why [`crate::Replicator::start`] returned no replicator. Nothing is pinned or running when
/// it returns one of these.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    /// The database path cannot be used (missing, unreadable, or naming no file), or named
    /// another file by the time SQLite opened it (`NotFound`: the database was replaced while
    /// the replicator started).
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
    /// Another engine holds the meta directory's lock (`lsr-lock`): a replicator in this or
    /// another process on this host uses the same staging directory. Stop it, or give this
    /// database a meta directory of its own.
    #[error("another replicator holds the meta directory's lock")]
    MetaLocked,
    /// The meta directory's lock cannot be taken: its file cannot be opened, or the filesystem
    /// refuses file locks (some network and FUSE filesystems). The replicator does not run
    /// without it; put the meta directory on a local filesystem.
    #[error("the meta directory's lock cannot be taken: {kind:?}")]
    MetaLock {
        /// What the file system said.
        kind: io::ErrorKind,
    },
    /// SQLite refused to open the database, to set it up, or to create it.
    #[error("SQLite refused the database: {message}")]
    Sqlite {
        /// SQLite's extended result code; `None` when the error was not SQLite's own.
        code: Option<i32>,
        /// What SQLite (or the binding) said.
        message: String,
    },
    /// The database path names no file (`/`, or a path ending in `..`), so it has no default
    /// meta directory.
    #[error("the database path names no file")]
    NoFileName,
    /// SQLite's file handle or wal-index cannot be used without this library opening a
    /// descriptor of its own on the database, which would drop the application's locks.
    #[error("SQLite's own file handle cannot be used: {0}")]
    UnsupportedVfs(lsr_sqlite::Refusal),
    /// SQLite reported a page size no SQLite build allows.
    #[error("page size {bytes} is not one SQLite allows")]
    PageSize {
        /// The size SQLite reported.
        bytes: u32,
    },
    /// The SQLite linked into the process is not one of the builds the replicator's reads of
    /// SQLite's private layouts are verified against: a system library outside the list,
    /// SQLCipher, a loadable-extension build, or a libsqlite3-sys outside 0.37 to 0.40. Build
    /// with the `bundled` feature (the default), or link one of the verified builds.
    #[error(
        "the SQLite in use is version {} ({source_id}), not a build this library is verified against ({}): link the SQLite bundled by libsqlite3-sys 0.37 to 0.40, or one of those builds",
        lsr_sqlite::dotted(*.found),
        .verified.iter().map(|v| lsr_sqlite::dotted(*v)).collect::<Vec<_>>().join(", ")
    )]
    SqliteBuild {
        /// `sqlite3_libversion_number()` of the running library.
        found: std::ffi::c_int,
        /// `sqlite3_sourceid()` of the running library.
        source_id: String,
        /// The version numbers of the verified builds, oldest first.
        verified: &'static [std::ffi::c_int],
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

impl From<lsr_sqlite::SqliteError> for StartError {
    fn from(err: lsr_sqlite::SqliteError) -> Self {
        Self::Sqlite {
            code: err.code(),
            message: err.to_string(),
        }
    }
}

impl From<lsr_sqlite::OpenError> for StartError {
    fn from(err: lsr_sqlite::OpenError) -> Self {
        match err {
            lsr_sqlite::OpenError::Sqlite { source, .. } => source.into(),
            lsr_sqlite::OpenError::NotWal { mode } => Self::NotWal { journal_mode: mode },
            lsr_sqlite::OpenError::PageSize { bytes } => Self::PageSize { bytes },
            lsr_sqlite::OpenError::Refused(lsr_sqlite::Refusal::NotWal { mode }) => {
                Self::NotWal { journal_mode: mode }
            }
            lsr_sqlite::OpenError::Refused(refusal) => Self::UnsupportedVfs(refusal),
            lsr_sqlite::OpenError::SqliteBuild {
                found,
                source_id,
                verified,
            } => Self::SqliteBuild {
                found,
                source_id,
                verified,
            },
        }
    }
}

/// Why [`crate::restore`] or [`crate::restore_if_missing`] did not restore. Nothing is
/// published, except after a publish failure whose error says it left the database visible,
/// and after [`RestoreError::TrustRecord`].
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The destination cannot take a restore: it exists, a stale sidecar sits beside it
    /// ([`crate::clear_journal_files`] removes those), or its scratch file cannot be made.
    #[error("{0}")]
    Destination(#[from] CreateError),
    /// The destination names no file (`/`, or a path ending in `..`), so it has no default
    /// meta directory to hold the trust record. Refused before anything is written.
    #[error("{} names no file, so it has no meta directory for the trust record", .path.display())]
    NoMetaDir {
        /// The destination given.
        path: std::path::PathBuf,
    },
    /// The restore itself failed: the replica has no exact answer for the target (an empty
    /// replica is `NoPlan` with `PlanOutcome::EmptyReplica`), a read or a file failed, or the
    /// publish did (its source is a [`crate::PublishError`]).
    #[error("{0}")]
    Restore(#[from] lsr_core::RestoreError),
    /// The database was restored and published, and the trust record that lets replication
    /// build on the replica it came from (`lsr-takeover`) could not be written in its meta
    /// directory. Replication will refuse to start over that replica until one is written:
    /// [`crate::record_trust`] with `restored.reached()` retries exactly this one.
    #[error("the database was restored but its trust record could not be written: {kind:?}")]
    TrustRecord {
        /// The restore that succeeded.
        restored: lsr_core::Restored,
        /// What the file system said.
        kind: io::ErrorKind,
    },
}

/// Why [`crate::trust_local`] recorded nothing.
#[derive(Debug, thiserror::Error)]
pub enum TrustLocalError {
    /// The replica could not be listed.
    #[error("the replica cannot be listed: {0}")]
    List(#[from] lsr_core::InventoryError),
    /// The record could not be written.
    #[error("the takeover record cannot be written: {kind:?}")]
    Write {
        /// What the file system said.
        kind: io::ErrorKind,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A failure that never reached SQLite carries no SQLite code; SQLite's own carries its
    /// code and message.
    #[test]
    fn an_open_failure_outside_sqlite_has_no_code() {
        let outside = lsr_sqlite::OpenError::Sqlite {
            path: "db".into(),
            source: lsr_sqlite::SqliteError::NoRow,
        };
        assert!(matches!(
            StartError::from(outside),
            StartError::Sqlite { code: None, .. }
        ));
        let busy = lsr_sqlite::OpenError::Sqlite {
            path: "db".into(),
            source: lsr_sqlite::SqliteError::Code {
                code: 5,
                message: "database is locked".to_owned(),
            },
        };
        assert!(matches!(
            StartError::from(busy),
            StartError::Sqlite { code: Some(5), ref message } if message.contains("database is locked")
        ));
    }
}
