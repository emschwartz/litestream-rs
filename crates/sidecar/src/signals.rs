//! SIGINT, SIGTERM and SIGHUP, as the CLI's `replicate` already takes them:
//! through `signal-hook` (MIT or Apache-2.0), because installing a handler needs
//! `unsafe` and this workspace keeps that in `lsr-sqlite`.
//!
//! The handlers are installed before any replicator starts, so a signal never
//! meets the default action (which ends the process with nothing synced). One
//! thread reads them: the first goes to the daemon through a channel of one,
//! every later one calls the daemon's `on_repeat` (which cuts the drain short),
//! as Go's `runUntilStopped` ends its shutdown on a second signal.

use std::io;
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;

use signal_hook::consts::{SIGHUP, SIGINT, SIGTERM};
use signal_hook::iterator::{Handle, Signals};

/// Go's `os.Signal` names, for the log.
#[must_use]
pub fn name(signal: i32) -> &'static str {
    match signal {
        SIGINT => "interrupt",
        SIGTERM => "terminated",
        SIGHUP => "hangup",
        _ => "signal",
    }
}

/// The installed handlers and the thread reading them.
#[derive(Debug)]
pub struct Watch {
    handle: Handle,
    first: Receiver<i32>,
    thread: Option<JoinHandle<()>>,
}

impl Watch {
    /// Installs the handlers; `on_repeat` runs on the reading thread for every signal after
    /// the first.
    ///
    /// # Errors
    /// The OS refused a handler or the thread.
    pub fn install(on_repeat: impl Fn(i32) + Send + 'static) -> io::Result<Self> {
        let mut signals = Signals::new([SIGINT, SIGTERM, SIGHUP])?;
        let handle = signals.handle();
        let (send, first) = mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("lsr-signals".to_owned())
            .spawn(move || {
                let mut seen = signals.forever();
                if let Some(signal) = seen.next()
                    && send.send(signal).is_ok()
                {
                    for signal in seen {
                        on_repeat(signal);
                    }
                }
            })?;
        Ok(Self {
            handle,
            first,
            thread: Some(thread),
        })
    }

    /// Blocks until the first signal.
    #[must_use]
    pub fn first(&self) -> Option<i32> {
        self.first.recv().ok()
    }

    /// Whether a signal has come, without waiting.
    #[must_use]
    pub fn try_first(&self) -> Option<i32> {
        self.first.try_recv().ok()
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.handle.close();
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            // The reading thread panicked; the handlers are closed either way.
        }
    }
}
