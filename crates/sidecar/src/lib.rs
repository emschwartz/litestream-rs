//! `lsr replicate` as Go's daemon: Go's `replicate` command line and
//! `litestream.yml`, many databases in one process, one replicator thread each
//! (docs/plans/phase-4-sidecar.md).
//!
//! [`run`] reads the arguments after `replicate` ([`flags`]), installs the signal
//! handlers ([`signals`]), loads the config (`-config`, `LITESTREAM_CONFIG`, else
//! `/etc/litestream.yml`; `-` is standard input) or builds Go's one-database
//! config from `DB_PATH REPLICA_URL`, maps it onto replicators (`lsr_config::plan`,
//! which scans `dir` entries once), starts the listener on `addr` ([`http`]), and
//! runs the [`daemon`] until a signal or the `-exec` child's exit ([`supervise`],
//! [`exec`]), or, with `-once`, until every database has synced and uploaded what
//! it had.
//!
//! `-once` starts every database and drains it at once: everything committed is
//! synced and uploaded, as Go's `runOnce` syncs the database and then the replica.
//!
//! What this build refuses rather than ignores, since ignoring would change what
//! runs: `-force-snapshot` (Go writes a level-9 snapshot; the replicator's API can
//! only make the next sync start from page 1 at level 0) and `-enforce-retention`
//! (no one-off retention pass exists in the replicator's API). An `exec` line Go
//! would run differently from what it says, and an `addr` Go cannot bind, are
//! refused before any database is touched.
//!
//! Lines are logged in Go's slog formats ([`logging`]). Nothing in this crate
//! opens a database file: replicators open theirs through SQLite, and the
//! directory scan's header read happens in `lsr-config`, before any starts.

pub mod daemon;
mod error;
pub mod exec;
pub mod flags;
pub mod health;
pub mod http;
pub mod logging;
pub mod metrics;
pub mod signals;
pub mod supervise;

use std::io::Read as _;
use std::sync::Arc;
use std::sync::mpsc;

use lsr_config::{Expand, GoConfig, Loaded, LogLevel, ProcessEnv, expand_path};
use lsr_replica::Runtime;

pub use daemon::{End, Finished};
pub use error::SidecarError;
pub use exec::{ChildEnd, During, ExecCommand, ExecError};
pub use flags::{Parsed, UsageError};
pub use http::AddrError;

use crate::daemon::Daemon;
use crate::exec::Wake;
use crate::flags::{Flags, Source};
use crate::health::Board;
use crate::logging::Logger;
use crate::supervise::Control;

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
    // One wake for the first signal and one for the child's exit: never full.
    let (send, wake) = mpsc::sync_channel(2);
    let control = Arc::new(Control::new(send));
    let on_signal = Arc::clone(&control);
    let _watch = signals::Watch::install(move |signal| on_signal.on_signal(signal))
        .map_err(SidecarError::Signals)?;
    let loaded = load(&flags)?;
    let config = &loaded.config;
    let level = flags.log_level.unwrap_or(config.logging().level());
    let log = Arc::new(Logger::new(
        level,
        config.logging().format(),
        config.logging().stderr(),
        Runtime::system().clock,
    ));
    refuse_unbuilt(&flags)?;
    let command = exec_command(&flags, config)?;
    let addr = config.addr().map(http::target).transpose()?;
    let plan = lsr_config::plan(config, &ProcessEnv)?;
    announce(&log, level, &loaded, &plan);
    let board = Board::default();
    if let Some(target) = &addr {
        listen(&log, target, &board);
    }
    let restore = flags.restore_if_db_not_exists;
    let daemon = Arc::new(Daemon::start(
        &plan,
        restore,
        Arc::clone(&log),
        control.started(),
        board,
    ));
    let finished = if flags.once {
        daemon.stop(None, daemon.now())
    } else {
        supervise::supervise(&daemon, command.as_ref(), &control, &wake)
    };
    if let Ok(Wake::Signal(signal)) = wake.try_recv() {
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

fn refuse_unbuilt(flags: &Flags) -> Result<(), SidecarError> {
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

/// `-exec`, else the `exec` key (Go: the flag overrides the key when set, `replicate.go:242`),
/// split as Go splits it. Go refuses `-once` with either (`replicate.go:260`).
fn exec_command(flags: &Flags, config: &GoConfig) -> Result<Option<ExecCommand>, SidecarError> {
    let line = flags
        .exec
        .as_deref()
        .or_else(|| config.exec().filter(|line| !line.is_empty()));
    let Some(line) = line else {
        return Ok(None);
    };
    if flags.once {
        return Err(UsageError::OnceWithExec.into());
    }
    Ok(Some(ExecCommand::parse(line)?))
}

fn listen(log: &Logger, target: &http::Target, board: &Board) {
    match http::serve(target, board.clone(), Runtime::system().clock) {
        Ok(local) => {
            let fields = [
                ("addr", local.to_string().into()),
                ("url", format!("http://{local}/metrics").into()),
                ("health", format!("http://{local}/healthz").into()),
            ];
            log.info("serving metrics on", &fields);
        }
        Err(error) => {
            let fields = [("error", error.to_string().into())];
            log.error("cannot start metrics server", &fields);
        }
    }
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
}
