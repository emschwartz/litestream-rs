//! The one error type of the harness.

use std::io;
use std::path::PathBuf;

/// Everything the harness can fail on. Each variant says what a caller needs to act.
#[derive(Debug, thiserror::Error)]
pub enum CompatError {
    /// A filesystem operation failed.
    #[error("io on {path}: {source}")]
    Io {
        /// The path the operation touched.
        path: PathBuf,
        /// The underlying error.
        source: io::Error,
    },
    /// SQLite refused a statement.
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// The replication backend could not open the database.
    #[error("opening the replication backend: {0}")]
    Open(#[from] lsr_sqlite::OpenError),
    /// A manifest did not parse or serialize.
    #[error("manifest {path}: {source}")]
    Json {
        /// The manifest file.
        path: PathBuf,
        /// The underlying error.
        source: serde_json::Error,
    },
    /// A name that is not one of Go's LTX keys.
    #[error("{0:?} is not a Go LTX key")]
    BadKey(String),
    /// A pinned Go binary has not been built.
    #[error("pinned Go binary {path} is missing; run scripts/build-go-litestream.sh")]
    MissingBinary {
        /// Where it was expected.
        path: PathBuf,
    },
    /// A child process failed.
    #[error("`{command}` failed: {detail}")]
    Process {
        /// The command line.
        command: String,
        /// Exit status and the tail of its output.
        detail: String,
    },
    /// A condition did not become true in time.
    #[error("timed out waiting for {0}")]
    Timeout(String),
    /// A fixture, or the data it was built from, is not what it should be.
    #[error("fixture: {0}")]
    Fixture(String),
    /// `PRAGMA integrity_check` reported something other than ok.
    #[error("integrity_check on {path}: {report}")]
    Integrity {
        /// The checked database.
        path: PathBuf,
        /// What SQLite said.
        report: String,
    },
    /// A database file whose header or length does not describe whole pages.
    #[error("{path} is not a page-aligned SQLite file: {why}")]
    NotPages {
        /// The file.
        path: PathBuf,
        /// What was wrong.
        why: String,
    },
}

impl CompatError {
    /// An I/O error tagged with the path it happened on.
    pub fn io(path: impl Into<PathBuf>) -> impl FnOnce(io::Error) -> Self {
        let path = path.into();
        move |source| Self::Io { path, source }
    }
}
