//! `lsr replicate <db> <replica>`: one replicator, in the foreground, until a signal.
//!
//! The signal handlers are installed before the replicator starts, so a signal never finds
//! the default action (which would end the process with nothing synced). Once the replicator
//! runs, one line goes to stdout (`replicating <db>`), which is what a supervisor or a test
//! waits for. SIGINT, SIGTERM and SIGHUP stop it with a final sync, within the shutdown grace
//! (30 s, Go's); a second signal shortens the drain to nothing. The outcome is printed and
//! sets the exit code: everything committed on the replica is success.
//!
//! Signals arrive through `signal-hook` (MIT or Apache-2.0): installing a handler needs
//! `unsafe`, which only the SQLite crate may write in this workspace.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use lsr_replica::{Config, DynStore, Replicator, Stopped};
use lsr_store_fs::FsStore;
use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::iterator::Signals;

use crate::args::{Replica, ReplicateArgs};
use crate::replica;
use crate::run::{CliError, Done};

fn store(replica: &Replica) -> Result<Arc<dyn DynStore>, CliError> {
    Ok(match replica {
        Replica::Dir(path) => Arc::new(FsStore::new(path)),
        Replica::S3(config) => Arc::new(replica::open_s3((**config).clone())?),
    })
}

/// Replicates until a signal, then drains.
///
/// # Errors
/// [`CliError`] when the replicator cannot start or the signals cannot be watched.
pub fn replicate(args: ReplicateArgs) -> Result<Done, CliError> {
    let config = Config::default();
    let grace = config.shutdown_grace;
    let mut signals = Signals::new([SIGINT, SIGTERM, SIGHUP]).map_err(CliError::Signals)?;
    let replicator = Replicator::start(&args.database, store(&args.replica)?, config)?;
    let mut out = io::stdout().lock();
    writeln!(out, "replicating {}", args.database.display()).map_err(CliError::Output)?;
    out.flush().map_err(CliError::Output)?;
    drop(out);
    let handle = signals.handle();
    let (first, signalled) = mpsc::sync_channel(1);
    let watcher = replicator.clone();
    let watching = std::thread::Builder::new()
        .name("lsr-signals".to_owned())
        .spawn(move || {
            let mut seen = signals.forever();
            if seen.next().is_some() && first.send(()).is_ok() {
                // A second signal: stop now, whatever is left.
                for _ in seen {
                    drop(watcher.stop(Duration::ZERO));
                }
            }
        })
        .map_err(CliError::Signals)?;
    // The watcher holds the only sender, so this returns at the first signal, or when the
    // watcher is gone (it cannot be: it ends only on a closed handle).
    let _signal: Result<(), _> = signalled.recv();
    let stopped = replicator.stop(grace).wait();
    handle.close();
    drop(watching.join());
    Ok(Done::Replicated {
        database: args.database,
        stopped,
    })
}

/// Whether a stop left everything committed on the replica.
pub const fn drained(stopped: &Stopped) -> bool {
    matches!(stopped, Stopped::Drained { .. })
}
