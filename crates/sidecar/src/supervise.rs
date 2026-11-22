//! What ends a run, and in what order: the first signal, or the `-exec` child exiting.
//!
//! **Without `-exec`**, the first SIGINT, SIGTERM or SIGHUP begins the drain.
//!
//! **With `-exec`**, the child starts once [`Daemon::start`] has returned, so every database
//! that exists has its read lock held (a signal that came during the start means no child at all,
//! as Go cancels its start). Then, as Go's `runUntilStopped` (`replicate.go:74-143`):
//! - the child exiting begins the drain, with the whole shutdown deadline from that moment;
//! - the first signal is forwarded to the child, and the drain waits for the child to exit,
//!   within the shutdown deadline begun at the signal; a child still running at the deadline
//!   is killed (Go's `Process.Kill` after "shutdown timed out"). The databases keep
//!   replicating while the child stops, so its last writes are drained too.
//!
//! **Once shutdown has begun**, a further signal cuts every drain to nothing and kills the
//! child, as Go's "shutdown interrupted" does.
//!
//! **Fail-stop** (`crate::failstop`): the child starts only once every database's gate is
//! open; until then the daemon waits, still taking signals. Under `stop-app`, a gate held
//! while the child runs stops it with SIGTERM, the shutdown grace and SIGKILL, and the run
//! drains and ends with the CLI's own exit code.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use lsr_replica::{Gate, Trip};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::daemon::{Daemon, Finished, Started};
use crate::exec::{Child, ChildEnd, ExecCommand, Wake};
use crate::failstop;
use crate::logging::Value;
use crate::signals;

/// What the signal thread and the main thread share.
#[derive(Debug)]
pub(crate) struct Control {
    started: Started,
    child: Mutex<Option<Child>>,
    begun: AtomicBool,
    wake: SyncSender<Wake>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Each critical section leaves plain data behind.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Control {
    /// Wakes the main thread through `wake`.
    #[must_use]
    pub(crate) fn new(wake: SyncSender<Wake>) -> Self {
        Self {
            started: Started::default(),
            child: Mutex::new(None),
            begun: AtomicBool::new(false),
            wake,
        }
    }

    /// The replicators a further signal cuts.
    #[must_use]
    pub(crate) fn started(&self) -> Started {
        self.started.clone()
    }

    /// One signal: the first wakes the main thread, any after shutdown began cuts it short.
    pub(crate) fn on_signal(&self, signal: i32) {
        if self.begun.swap(true, Ordering::SeqCst) {
            self.started.cut();
            if let Some(child) = &*lock(&self.child) {
                child.kill();
            }
        } else if self.wake.try_send(Wake::Signal(signal)).is_err() {
            // The main thread is already awake.
        }
    }

    /// Shutdown has begun without a signal: the next one cuts it short.
    fn begin(&self) {
        self.begun.store(true, Ordering::SeqCst);
    }
}

/// The `-exec` child process, if one was asked for.
enum ChildProcess {
    None,
    Running(Child),
    DidNotStart,
}

fn spawn(daemon: &Daemon, control: &Control, command: &ExecCommand) -> ChildProcess {
    match Child::spawn(command, control.wake.clone()) {
        Ok(child) => {
            *lock(&control.child) = Some(child.clone());
            ChildProcess::Running(child)
        }
        Err(error) => {
            let fields = [
                ("exec", command.program().into()),
                ("error", error.to_string().into()),
            ];
            daemon.log().error("cannot start exec command", &fields);
            ChildProcess::DidNotStart
        }
    }
}

/// How often the gates are read while the `-exec` child waits to start or runs under
/// `stop-app` (`crate::failstop`).
const GATE_POLL: Duration = Duration::from_secs(1);

/// Runs until a signal or the child's exit, then drains every database.
#[must_use]
pub(crate) fn supervise(
    daemon: &Arc<Daemon>,
    exec: Option<&ExecCommand>,
    control: &Control,
    wake: &Receiver<Wake>,
) -> Finished {
    let waiter = daemon.wait_for_files();
    let mut early = wake.try_recv().ok();
    let mut held = None;
    if let (Some(_), None) = (exec, early) {
        (early, held) = hold_start(daemon, wake);
    }
    let child_process = match (exec, early) {
        (Some(command), None) => spawn(daemon, control, command),
        (Some(_) | None, _) => ChildProcess::None,
    };
    let first = match (&child_process, early) {
        (_, Some(wake)) => Some(wake),
        (ChildProcess::DidNotStart, None) => Some(Wake::ChildExited),
        (ChildProcess::Running(child), None) => match watch(daemon, wake) {
            Watched::Wake(wake) => wake,
            Watched::Held(path, trip) => {
                stop_held(daemon, control, child, wake, &path, trip);
                held = Some(trip);
                None
            }
        },
        (ChildProcess::None, None) => wake.recv().ok(),
    };
    let begun = daemon.now();
    match first {
        Some(Wake::Signal(signal)) => {
            daemon.signalled(signal);
            if let ChildProcess::Running(child) = &child_process {
                child.forward(signal);
                wait_for_child(daemon, child, wake, begun);
            }
        }
        Some(Wake::ChildExited) => {
            control.begin();
            if let ChildProcess::Running(_) = child_process {
                daemon
                    .log()
                    .info("subprocess exited, litestream shutting down", &[]);
            }
        }
        None => {}
    }
    let end = match &child_process {
        ChildProcess::None => None,
        ChildProcess::Running(child) => child.end(),
        ChildProcess::DidNotStart => Some(ChildEnd::DidNotStart),
    };
    if let Some(end) = end {
        ended(daemon, end);
    }
    let mut finished = daemon.stop(waiter, begun);
    finished.child = end;
    finished.app_held = held;
    finished
}

/// The first held gate, with its database.
fn first_held(daemon: &Daemon) -> Option<(PathBuf, Trip)> {
    daemon
        .gates()
        .into_iter()
        .find_map(|(path, gate)| match gate {
            Gate::Held(trip) => Some((path, trip)),
            Gate::Pending | Gate::Allowed => None,
        })
}

/// Waits until every database's gate is open before the `-exec` child starts: a pending gate
/// (the boot not judged yet) waits, a held one keeps the child from starting while the daemon
/// stays up answering 503. Returns the signal that ended the wait, if one did, and the trip
/// that held the start then.
fn hold_start(daemon: &Daemon, wake: &Receiver<Wake>) -> (Option<Wake>, Option<Trip>) {
    let mut said = None;
    loop {
        let held = first_held(daemon);
        let pending = daemon
            .gates()
            .iter()
            .any(|(_, gate)| *gate == Gate::Pending);
        if held.is_none() && !pending {
            return (None, None);
        }
        if let Some((path, trip)) = &held
            && said != Some(*trip)
        {
            let fields = [
                ("db", path.display().to_string().into()),
                ("why", failstop::trip_name(*trip).into()),
            ];
            daemon.log().warn(failstop::START_REFUSED, &fields);
            said = Some(*trip);
        }
        match wake.recv_timeout(GATE_POLL) {
            Ok(signal @ Wake::Signal(_)) => return (Some(signal), held.map(|(_, trip)| trip)),
            Ok(Wake::ChildExited) | Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return (None, None),
        }
    }
}

/// What ended the watch over a running child.
enum Watched {
    /// A signal, the child's exit, or (`None`) nothing left to wake the daemon.
    Wake(Option<Wake>),
    /// Under `stop-app`, this database's gate is held.
    Held(PathBuf, Trip),
}

/// Waits for a signal or the child's exit; under `stop-app`, also for a held gate.
fn watch(daemon: &Daemon, wake: &Receiver<Wake>) -> Watched {
    loop {
        match wake.recv_timeout(GATE_POLL) {
            Ok(wake) => return Watched::Wake(Some(wake)),
            Err(RecvTimeoutError::Disconnected) => return Watched::Wake(None),
            Err(RecvTimeoutError::Timeout) => {}
        }
        if daemon.stop_app()
            && let Some((path, trip)) = first_held(daemon)
        {
            return Watched::Held(path, trip);
        }
    }
}

/// Stops the running child for a held gate: SIGTERM, the shutdown grace, SIGKILL. The drain
/// that follows is the run's own (decision D19).
fn stop_held(
    daemon: &Daemon,
    control: &Control,
    child: &Child,
    wake: &Receiver<Wake>,
    path: &std::path::Path,
    trip: Trip,
) {
    let fields = [
        ("db", path.display().to_string().into()),
        ("why", failstop::trip_name(trip).into()),
    ];
    daemon.log().error(failstop::APP_STOPPED, &fields);
    control.begin();
    child.forward(signal_hook::consts::SIGTERM);
    wait_for_child(daemon, child, wake, daemon.now());
}

/// Waits for the child to exit within the deadline begun at `begun`, then kills it.
fn wait_for_child(
    daemon: &Daemon,
    child: &Child,
    wake: &Receiver<Wake>,
    begun: lsr_ltx::Timestamp,
) {
    loop {
        match wake.recv_timeout(daemon.left(begun)) {
            Ok(Wake::ChildExited) | Err(RecvTimeoutError::Disconnected) => return,
            // Only the first signal is sent here, and it has been read.
            Ok(Wake::Signal(_)) => {}
            Err(RecvTimeoutError::Timeout) => {
                daemon.log().error(
                    "subprocess still running at the shutdown deadline; killing it",
                    &[],
                );
                child.kill();
                if wake.recv().is_err() {
                    // No sender left: nothing more will come.
                }
                return;
            }
        }
    }
}

fn ended(daemon: &Daemon, end: ChildEnd) {
    let clean = ("clean", Value::from(end.clean()));
    match end {
        ChildEnd::Exited(code) => {
            let code = ("code", Value::Int(i64::from(code)));
            daemon.log().info("subprocess ended", &[code, clean]);
        }
        ChildEnd::Signalled { signal, during } => {
            let fields = [
                ("signal", signals::name(signal).into()),
                ("number", Value::Int(i64::from(signal))),
                ("during", during.word().into()),
                clean,
            ];
            daemon.log().info("subprocess ended", &fields);
        }
        ChildEnd::DidNotStart => {}
    }
}
