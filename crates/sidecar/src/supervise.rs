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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::daemon::{Daemon, Finished, Started};
use crate::exec::{Child, ChildEnd, ExecCommand, Wake};
use crate::logging::Value;
use crate::signals;

/// What the signal thread and the main thread share.
#[derive(Debug)]
pub struct Control {
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
    pub fn new(wake: SyncSender<Wake>) -> Self {
        Self {
            started: Started::default(),
            child: Mutex::new(None),
            begun: AtomicBool::new(false),
            wake,
        }
    }

    /// The replicators a further signal cuts.
    #[must_use]
    pub fn started(&self) -> Started {
        self.started.clone()
    }

    /// One signal: the first wakes the main thread, any after shutdown began cuts it short.
    pub fn on_signal(&self, signal: i32) {
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

/// Runs until a signal or the child's exit, then drains every database.
#[must_use]
pub fn supervise(
    daemon: &Arc<Daemon>,
    exec: Option<&ExecCommand>,
    control: &Control,
    wake: &Receiver<Wake>,
) -> Finished {
    let waiter = daemon.wait_for_files();
    let early = wake.try_recv().ok();
    let child_process = match (exec, early) {
        (Some(command), None) => spawn(daemon, control, command),
        (Some(_) | None, _) => ChildProcess::None,
    };
    let first = match (&child_process, early) {
        (_, Some(wake)) => Some(wake),
        (ChildProcess::DidNotStart, None) => Some(Wake::ChildExited),
        (ChildProcess::None | ChildProcess::Running(_), None) => wake.recv().ok(),
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
    finished
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
