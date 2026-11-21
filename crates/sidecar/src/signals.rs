//! SIGINT, SIGTERM and SIGHUP, as the CLI's `replicate` already takes them:
//! through `signal-hook` (MIT or Apache-2.0), because installing a handler needs
//! `unsafe` and this workspace keeps that in `lsr-sqlite`.
//!
//! The handlers are installed before any replicator starts, so a signal never
//! meets the default action (which ends the process with nothing synced). One
//! thread reads them and hands each to the daemon's callback, which wakes the
//! main thread on the first and, once shutdown has begun, cuts it short, as Go's
//! `runUntilStopped` ends its shutdown on a second signal.

use lsr_replica::stack;
use std::io;
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
    thread: Option<JoinHandle<()>>,
}

impl Watch {
    /// Installs the handlers; `on_signal` runs on the reading thread for every signal.
    ///
    /// # Errors
    /// The OS refused a handler or the thread.
    pub fn install(on_signal: impl Fn(i32) + Send + 'static) -> io::Result<Self> {
        let mut signals = Signals::new([SIGINT, SIGTERM, SIGHUP])?;
        let handle = signals.handle();
        let thread = stack::spawn("lsr-signals", stack::WAIT_STACK, move || {
            for signal in signals.forever() {
                on_signal(signal);
            }
        })?;
        Ok(Self {
            handle,
            thread: Some(thread),
        })
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
