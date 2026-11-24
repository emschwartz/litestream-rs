//! Why the daemon did not run.

use std::io;

use lsr_config::{ConfigError, MapError};

use crate::exec::ExecError;
use crate::flags::UsageError;
use crate::http::AddrError;

/// Why the daemon stopped before replicating anything. Every variant is decided before any
/// database is opened; a database that fails after that is reported in
/// [`crate::Finished`], never here.
#[derive(Debug, thiserror::Error)]
pub enum SidecarError {
    /// The command line.
    #[error("{0}")]
    Usage(#[from] UsageError),
    /// The config file is missing, unreadable or refused; every problem is listed.
    #[error("config: {}", problems(.0))]
    Config(ConfigError),
    /// The config loads but cannot be started as written.
    #[error("config: {}", map_problems(.0))]
    Map(#[from] MapError),
    /// A feature Go has that this build does not, asked for.
    #[error("{what} is not in this build yet; {instead}")]
    NotBuilt {
        /// The flag or key.
        what: &'static str,
        /// What to do instead.
        instead: &'static str,
    },
    /// The `-exec` command line (or the `exec` key) cannot run as Go would read it.
    #[error("{0}")]
    Exec(#[from] ExecError),
    /// The `addr` key cannot be bound as Go reads it.
    #[error("addr: {0}")]
    Addr(#[from] AddrError),
    /// Standard input could not be read for `-config -`.
    #[error("reading the config from standard input: {0}")]
    Stdin(#[source] io::Error),
    /// The signal handlers could not be installed.
    #[error("watching for signals: {0}")]
    Signals(#[source] io::Error),
}

impl SidecarError {
    /// Whether this is a config file that does not exist (Go's `ErrConfigFileNotFound`).
    #[must_use]
    pub fn is_missing_config(&self) -> bool {
        matches!(self, Self::Config(error) if error.is_not_found())
    }
}

fn problems(error: &ConfigError) -> String {
    error
        .problems()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

fn map_problems(error: &MapError) -> String {
    error
        .problems()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}
