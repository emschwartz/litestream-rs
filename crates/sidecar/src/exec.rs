//! `-exec` (and the `exec` key): the application as the daemon's child.
//!
//! **The command line** is split as Go splits it, by go-shellwords v1.0.12's `Parse` with its
//! defaults (`replicate.go:467`): no environment or backtick expansion, single quotes keep
//! everything, a backslash escapes the next character outside single quotes, and an unquoted
//! `(` not after `$` or an unbalanced quote is an error. [`split`] is a port of that function,
//! tested against a table of its outputs. Two departures, both refusals before any database
//! is touched: go-shellwords stops at an unquoted `;`, `&`, `|`, `<` or `>` and Go runs only
//! the words before it, silently dropping the rest; and an empty command, which Go indexes
//! anyway. [`ExecCommand::parse`] refuses both.
//!
//! **The child** runs with the parent's environment, standard input from `/dev/null`, and the
//! parent's standard output and error (`replicate.go:472-475`). It starts only after every
//! database that exists has its read transaction held (Go starts it after `Store.Open`, which
//! takes no lock, so Go's child can write before the read lock exists). One thread waits for it and
//! wakes the daemon when it exits.
//!
//! **Signals** reach the child through [`Child::forward`]. The waiting thread first waits for
//! the exit without reaping (`waitid` with `WNOWAIT`), marks the child gone, and only then
//! reaps it, so a signal is never sent to a process id that may have been reused. Both calls
//! are `rustix`'s (Apache-2.0 WITH LLVM-exception, Apache-2.0 or MIT), already in the lock file
//! through `tempfile`, because sending a signal to another process needs `unsafe` otherwise.
//!
//! **How the child ended** ([`ChildEnd`]) decides whether the run is clean, as Go decides it:
//! an exit of 0 is clean; so is a death by any signal once a shutdown begun by a signal has
//! forwarded it ([`During::Shutdown`]; Go `replicate.go:152` only checks for "signal:"); any
//! other exit status, a death by a signal outside a shutdown, and a child the daemon had to
//! kill are not. Go's `replicate` then returns an error and its `main` exits 1
//! (`main.go:97-103`), whatever the child's own code, so the CLI exits 1 too; the code and the
//! signal stay in the "subprocess ended" log line. Passing the child's code on, as a wrapper
//! such as `env` or `timeout` does, is the alternative recorded in issue #41.

use lsr_replica::stack;
use std::io;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use rustix::process::{Pid, Signal, WaitId, WaitIdOptions};

/// What wakes the daemon's main thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wake {
    /// The first SIGINT, SIGTERM or SIGHUP.
    Signal(i32),
    /// The `-exec` child exited (or could not be started).
    ChildExited,
}

/// Where go-shellwords stopped reading: an unquoted shell operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stop {
    /// The operator.
    pub operator: char,
    /// Go's `Parser.Position`: the operator's index in characters, one less after a word that
    /// starts with a digit before `>` (Go drops that word as a redirection).
    pub position: usize,
}

/// What go-shellwords' `Parse` returns for a line it accepts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Words {
    /// The words.
    pub args: Vec<String>,
    /// Where it stopped, if it did before the end.
    pub stop: Option<Stop>,
}

/// go-shellwords' one error, "invalid command line string": a quote, backtick, `$(` or
/// escape left open, or an unquoted `(` that does not follow `$`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid command line string")]
pub struct Unbalanced;

/// Why an `-exec` command line is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ExecError {
    /// go-shellwords refuses it (Go: "cannot parse exec command").
    #[error("cannot parse exec command {line:?}: {source}")]
    Unbalanced {
        /// The command line.
        line: String,
        /// The parser's error.
        source: Unbalanced,
    },
    /// An unquoted shell operator: Go would run only the words before it.
    #[error(
        "exec command {line:?} has an unquoted {operator:?} at character {position}: Go would \
         run only the words before it; quote it, or run a shell (sh -c '...')"
    )]
    Operator {
        /// The command line.
        line: String,
        /// The operator.
        operator: char,
        /// Where, in characters.
        position: usize,
    },
    /// No words at all.
    #[error("exec command {line:?} names no program")]
    Empty {
        /// The command line.
        line: String,
    },
}

/// The parser's state between characters.
#[derive(Default)]
struct State {
    buf: String,
    escaped: bool,
    double: bool,
    single: bool,
    back: bool,
    dollar: bool,
    /// Go's `got != argNo`: a word has begun, possibly empty (`""`).
    got: bool,
}

impl State {
    const fn quoted(&self) -> bool {
        self.single || self.double || self.back || self.dollar
    }

    const fn open(&self) -> bool {
        self.escaped || self.quoted()
    }

    /// One character that is not escaped, a backslash or a space; [`Flow::Stop`] at an
    /// unquoted operator.
    fn other(&mut self, c: char) -> Result<Flow, Unbalanced> {
        match c {
            '`' if !self.single && !self.double && !self.dollar => self.back = !self.back,
            ')' if !self.single && !self.double && !self.back => self.dollar = !self.dollar,
            '(' if !self.single && !self.double && !self.back => {
                if self.dollar || !self.buf.ends_with('$') {
                    return Err(Unbalanced);
                }
                self.dollar = true;
                self.buf.push('(');
                return Ok(Flow::Next);
            }
            '"' if !self.single && !self.dollar => {
                if self.double {
                    self.got = true;
                }
                self.double = !self.double;
                return Ok(Flow::Next);
            }
            '\'' if !self.double && !self.dollar => {
                if self.single {
                    self.got = true;
                }
                self.single = !self.single;
                return Ok(Flow::Next);
            }
            ';' | '&' | '|' | '<' | '>' if !self.quoted() => return Ok(Flow::Stop),
            _ => {}
        }
        self.got = true;
        self.buf.push(c);
        Ok(Flow::Next)
    }
}

enum Flow {
    Next,
    Stop,
}

/// go-shellwords v1.0.12's `Parser.Parse` with `ParseEnv` and `ParseBacktick` off, its
/// defaults and Go's call: backticks and `$(...)` are kept as written.
///
/// # Errors
/// [`Unbalanced`], where go-shellwords returns its error.
pub(crate) fn split(line: &str) -> Result<Words, Unbalanced> {
    let mut args = Vec::new();
    let mut s = State::default();
    let mut stop = None;
    for (i, c) in line.chars().enumerate() {
        if s.escaped {
            s.buf.push(c);
            s.escaped = false;
            s.got = true;
            continue;
        }
        if c == '\\' {
            if s.single {
                s.buf.push(c);
            } else {
                s.escaped = true;
            }
            continue;
        }
        if matches!(c, ' ' | '\t' | '\r' | '\n') {
            if s.quoted() {
                s.buf.push(c);
            } else if s.got {
                args.push(std::mem::take(&mut s.buf));
                s.got = false;
            }
            continue;
        }
        if let Flow::Stop = s.other(c)? {
            let mut position = i;
            if c == '>' && s.buf.starts_with(|d: char| d.is_ascii_digit()) {
                // Go reads `2>` as a redirection of descriptor 2 and drops the word.
                position = i.saturating_sub(1);
                s.got = false;
            }
            stop = Some(Stop {
                operator: c,
                position,
            });
            break;
        }
    }
    if s.got {
        args.push(s.buf.clone());
    }
    if s.open() {
        return Err(Unbalanced);
    }
    Ok(Words { args, stop })
}

/// A command line `-exec` can run: a program and its arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecCommand {
    program: String,
    args: Vec<String>,
}

impl ExecCommand {
    /// Splits `line` as Go does, refusing what Go would run differently from what it says.
    ///
    /// # Errors
    /// [`ExecError`].
    pub fn parse(line: &str) -> Result<Self, ExecError> {
        let words = split(line).map_err(|source| ExecError::Unbalanced {
            line: line.to_owned(),
            source,
        })?;
        if let Some(stop) = words.stop {
            return Err(ExecError::Operator {
                line: line.to_owned(),
                operator: stop.operator,
                position: stop.position,
            });
        }
        let mut words = words.args.into_iter();
        let program = words.next().ok_or_else(|| ExecError::Empty {
            line: line.to_owned(),
        })?;
        Ok(Self {
            program,
            args: words.collect(),
        })
    }

    /// The program.
    #[must_use]
    pub fn program(&self) -> &str {
        &self.program
    }

    /// Its arguments.
    #[must_use]
    pub fn args(&self) -> &[String] {
        &self.args
    }
}

/// What the daemon was doing when a signal ended the child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum During {
    /// Running: no shutdown signal had reached the daemon (the child crashed, or something
    /// else killed it). Go's `result` is the child's error and the run fails.
    Run,
    /// A shutdown begun by a signal, which the daemon forwarded. Go counts any death by a
    /// signal here as clean (`replicate.go:152` checks only for the "signal:" prefix).
    Shutdown,
    /// The daemon killed it: at the shutdown deadline, or on a second signal. Go fails the
    /// run with "shutdown timed out" or "shutdown interrupted".
    Kill,
}

impl During {
    /// The word for the log.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Shutdown => "shutdown",
            Self::Kill => "kill",
        }
    }
}

/// How the child ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildEnd {
    /// It exited with this status.
    Exited(i32),
    /// A signal ended it.
    Signalled {
        /// The signal.
        signal: i32,
        /// What the daemon was doing then.
        during: During,
    },
    /// It could not be started.
    DidNotStart,
}

impl ChildEnd {
    /// Whether the child ended as Go counts a success: an exit of 0, or a death by any
    /// signal during a shutdown begun by a signal. Anything else makes Go's `replicate`
    /// return an error, which its `main` prints and exits 1 on (`main.go:97-103`).
    #[must_use]
    pub const fn clean(self) -> bool {
        match self {
            Self::Exited(code) => code == 0,
            Self::Signalled { during, .. } => match during {
                During::Shutdown => true,
                During::Run | During::Kill => false,
            },
            Self::DidNotStart => false,
        }
    }
}

/// How the process ended, before the daemon's part in it is known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Status {
    Exited(i32),
    Signalled(i32),
}

/// What the waiting thread and the signal forwarder share.
#[derive(Debug, Default)]
struct Shared {
    /// Set once the child has exited, before it is reaped: no signal is sent after.
    exited: bool,
    /// A shutdown begun by a signal: the signal was forwarded, or was due when the child had
    /// just died (Go's select may take the signal first; its `shutdown` then reads the death
    /// as "signal:" and counts it clean).
    shutdown: bool,
    /// The daemon sent it SIGKILL.
    killed: bool,
    /// How it ended, once reaped.
    status: Option<Status>,
}

fn lock(shared: &Mutex<Shared>) -> MutexGuard<'_, Shared> {
    // Each critical section leaves plain data behind.
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A running (or ended) child.
#[derive(Clone, Debug)]
pub(crate) struct Child {
    pid: Pid,
    shared: Arc<Mutex<Shared>>,
}

impl Child {
    /// Starts `command` and a thread that waits for it and sends [`Wake::ChildExited`].
    ///
    /// # Errors
    /// The program could not be started, or the waiting thread could not be.
    pub(crate) fn spawn(command: &ExecCommand, wake: SyncSender<Wake>) -> io::Result<Self> {
        let mut child = Command::new(command.program())
            .args(command.args())
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        let pid = i32::try_from(child.id())
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| io::Error::other("the child's process id is out of range"))?;
        let shared = Arc::new(Mutex::new(Shared::default()));
        let ours = Arc::clone(&shared);
        let spawned = stack::spawn("lsr-exec", stack::WAIT_STACK, move || {
            exited(pid);
            lock(&ours).exited = true;
            let status = child.wait();
            lock(&ours).status = Some(match status {
                Ok(status) => status_of(status),
                // Reaping failed after the exit was seen: nothing says it was clean.
                Err(_) => Status::Exited(1),
            });
            if wake.try_send(Wake::ChildExited).is_err() {
                // The daemon is already stopping and no longer listens.
            }
        });
        match spawned {
            Ok(_thread) => Ok(Self { pid, shared }),
            Err(error) => Err(error),
        }
    }

    /// Begins a shutdown begun by `signal`: sends it to the child if it has not exited, and
    /// remembers that a death by a signal from now on is clean.
    pub(crate) fn forward(&self, signal: i32) {
        let mut state = lock(&self.shared);
        state.shutdown = true;
        let Some(sig) = Signal::from_named_raw(signal) else {
            return;
        };
        if !state.exited && rustix::process::kill_process(self.pid, sig).is_err() {
            // It exited between the check and the call; the waiting thread sees it.
        }
    }

    /// Kills the child (Go's `Process.Kill` when its shutdown is cut short).
    pub(crate) fn kill(&self) {
        let mut state = lock(&self.shared);
        if !state.exited && rustix::process::kill_process(self.pid, Signal::KILL).is_ok() {
            state.killed = true;
        }
    }

    /// How it ended, once it has.
    #[must_use]
    pub(crate) fn end(&self) -> Option<ChildEnd> {
        let state = lock(&self.shared);
        let during = if state.killed {
            During::Kill
        } else if state.shutdown {
            During::Shutdown
        } else {
            During::Run
        };
        state.status.map(|status| match status {
            Status::Exited(code) => ChildEnd::Exited(code),
            Status::Signalled(signal) => ChildEnd::Signalled { signal, during },
        })
    }
}

/// Blocks until the child has exited, leaving it unreaped so its process id stays its own.
fn exited(pid: Pid) {
    loop {
        match rustix::process::waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOWAIT,
        ) {
            Err(rustix::io::Errno::INTR) => {}
            // Exited, or there is nothing left to wait for: either way it is gone.
            Ok(_) | Err(_) => return,
        }
    }
}

fn status_of(status: ExitStatus) -> Status {
    use std::os::unix::process::ExitStatusExt as _;
    match (status.code(), status.signal()) {
        (Some(code), _) => Status::Exited(code),
        (None, Some(signal)) => Status::Signalled(signal),
        // Stopped or continued are not exits; `wait` reports only exits.
        (None, None) => Status::Exited(1),
    }
}

#[cfg(test)]
mod tests;
