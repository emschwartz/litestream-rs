//! `lsr replicate` as Go's daemon: Go's `replicate` command line and
//! `litestream.yml`, many databases in one process, one replicator thread each
//! (docs/plans/phase-4-sidecar.md).
//!
//! [`run`] reads the arguments after `replicate` ([`flags`]), installs the signal
//! handlers ([`signals`]), loads the config (`-config`, `LITESTREAM_CONFIG`, else
//! `/etc/litestream.yml`; `-` is standard input) or builds Go's one-database
//! config from `DB_PATH REPLICA_URL`, maps it onto replicators (`lsr_config::plan`,
//! which scans `dir` entries once), and runs the [`daemon`] until a signal (or,
//! with `-once`, until every database has synced and uploaded what it had).
//!
//! `-once` starts every database and drains it at once: everything committed is
//! synced and uploaded, as Go's `runOnce` syncs the database and then the replica.
//!
//! What this build refuses rather than ignores, since ignoring would change what
//! runs: `-exec` and the `exec` key (the child is the host; starting without it
//! is not the daemon the operator configured), `-force-snapshot` (Go writes a
//! level-9 snapshot; the replicator's API can only make the next sync start from
//! page 1 at level 0) and `-enforce-retention` (no one-off retention pass exists
//! in the replicator's API). `addr` is accepted and warned about: the metrics
//! listener is not served yet, and nothing replicated depends on it.
//!
//! Lines are logged in Go's slog formats ([`logging`]). Nothing in this crate
//! opens a database file: replicators open theirs through SQLite, and the
//! directory scan's header read happens in `lsr-config`, before any starts.

pub mod daemon;
mod error;
pub mod flags;
pub mod logging;
pub mod signals;

use std::io::Read as _;
use std::sync::Arc;

use lsr_config::{Expand, GoConfig, Loaded, LogLevel, ProcessEnv, expand_path};
use lsr_replica::Runtime;

pub use daemon::{End, Finished};
pub use error::SidecarError;
pub use flags::{Parsed, UsageError};

use crate::daemon::{Daemon, Started};
use crate::flags::{Flags, Source};
use crate::logging::Logger;

/// What a `replicate` run produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `-h`: the usage text, for stdout.
    Help(&'static str),
    /// The daemon ran and stopped.
    Ran(Finished),
}

/// Runs `replicate` with the arguments after it.
///
/// # Errors
/// [`SidecarError`] for anything decided before a database is touched: the command line,
/// the config, the signal handlers.
pub fn run(args: &[String]) -> Result<Outcome, SidecarError> {
    let flags = match flags::parse(args)? {
        Parsed::Help => return Ok(Outcome::Help(flags::USAGE)),
        Parsed::Run(flags) => flags,
    };
    let started = Started::default();
    let cut = started.clone();
    let watch = signals::Watch::install(move |_signal| cut.cut()).map_err(SidecarError::Signals)?;
    let loaded = load(&flags)?;
    let config = &loaded.config;
    let level = flags.log_level.unwrap_or(config.logging().level());
    let log = Arc::new(Logger::new(
        level,
        config.logging().format(),
        config.logging().stderr(),
        Runtime::system().clock,
    ));
    refuse_unbuilt(&flags, config)?;
    let plan = lsr_config::plan(config, &ProcessEnv)?;
    announce(&log, level, &loaded, &plan);
    let daemon = Arc::new(Daemon::start(
        &plan,
        flags.restore_if_db_not_exists,
        Arc::clone(&log),
        started,
    ));
    let finished = if flags.once {
        daemon.stop(None)
    } else {
        let waiter = daemon.wait_for_files();
        if let Some(signal) = watch.first() {
            daemon.signalled(signal);
        }
        daemon.stop(waiter)
    };
    if let Some(signal) = watch.try_first() {
        daemon.signalled(signal);
    }
    let fields = [("clean", finished.clean().into())];
    daemon.log().info("litestream shut down", &fields);
    Ok(Outcome::Ran(finished))
}

/// The config file, or Go's one-database config.
fn load(flags: &Flags) -> Result<Loaded, SidecarError> {
    let env = ProcessEnv;
    let expand = if flags.no_expand_env {
        Expand::Off
    } else {
        Expand::Env
    };
    match &flags.source {
        Source::OneDatabase { database, replica } => {
            GoConfig::for_one_database(database, replica, &env).map_err(SidecarError::Config)
        }
        Source::Config(flag) => {
            let path = flags::config_path(flag.as_deref(), |name| std::env::var(name).ok());
            if path.as_os_str() == "-" {
                let mut text = String::new();
                std::io::stdin()
                    .read_to_string(&mut text)
                    .map_err(SidecarError::Stdin)?;
                return GoConfig::parse(&text, expand, &env).map_err(SidecarError::Config);
            }
            let path = expand_path(&path.to_string_lossy(), &env).unwrap_or(path);
            GoConfig::load(&path, expand, &env).map_err(SidecarError::Config)
        }
    }
}

fn refuse_unbuilt(flags: &Flags, config: &GoConfig) -> Result<(), SidecarError> {
    if flags.exec.is_some() || config.exec().is_some() {
        return Err(SidecarError::NotBuilt {
            what: "-exec (and the exec key)",
            instead: "run the application separately, after the daemon logs \"initialized db\"",
        });
    }
    if flags.force_snapshot {
        return Err(SidecarError::NotBuilt {
            what: "-force-snapshot",
            instead: "a snapshot is taken on the configured snapshot interval",
        });
    }
    if flags.enforce_retention {
        return Err(SidecarError::NotBuilt {
            what: "-enforce-retention",
            instead: "retention runs in the background while the daemon replicates",
        });
    }
    Ok(())
}

fn announce(log: &Logger, level: LogLevel, loaded: &Loaded, plan: &lsr_config::Plan) {
    let fields = [
        ("version", env!("CARGO_PKG_VERSION").into()),
        ("level", format!("{level:?}").to_ascii_uppercase().into()),
    ];
    log.info("litestream-rs", &fields);
    for warning in &loaded.report.warnings {
        log.warn("config", &[("warning", warning.to_string().into())]);
    }
    for note in plan.notes() {
        log.warn("config", &[("warning", note.to_string().into())]);
    }
    if let Some(addr) = loaded.config.addr() {
        let fields = [("addr", addr.into())];
        log.warn(
            "metrics listener (addr) is not served in this build",
            &fields,
        );
    }
}
