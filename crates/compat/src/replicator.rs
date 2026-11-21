//! A running replicator process, Go's `litestream replicate` or Rust's `lsr replicate`, and
//! the one way the harness waits: polling a condition.
//!
//! Both kinds write their output to one log file. A test knows a process is up when its log
//! says so ([`Replicator::wait_up`]: Go logs `replicating to` once its store runs, `lsr` prints
//! `replicating <db>` once its read transaction is held). [`Replicator::stop`] is the
//! operator's SIGINT, after which both drain and exit; [`Replicator::kill`] is SIGKILL, the
//! crash the alternation test ends each turn with.
//!
//! [`lsr_binary`] builds `lsr` with cargo before a test runs it: a test of one crate cannot
//! name another crate's binary, and building every time keeps a stale binary out of the run
//! (cargo returns at once when nothing changed).

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::binaries::workspace_root;
use crate::error::CompatError;

const POLL: Duration = Duration::from_millis(20);

/// Polls `cond` until it holds or `timeout` passes.
///
/// # Errors
/// [`CompatError::Timeout`] naming `what`, or whatever `cond` returned.
#[expect(
    clippy::disallowed_methods,
    reason = "waits on a real Go process, which keeps real time"
)]
pub fn wait_until(
    what: &str,
    timeout: Duration,
    mut cond: impl FnMut() -> Result<bool, CompatError>,
) -> Result<(), CompatError> {
    let deadline = Instant::now().checked_add(timeout);
    loop {
        if cond()? {
            return Ok(());
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(CompatError::Timeout(what.to_owned()));
        }
        std::thread::sleep(POLL);
    }
}

/// Which program a [`Replicator`] runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Go's `litestream replicate -config <file>`.
    Go,
    /// Rust's `lsr replicate <db> <replica>`.
    Rust,
}

impl Kind {
    /// The command, for messages.
    pub const fn command(self) -> &'static str {
        match self {
            Self::Go => "litestream replicate",
            Self::Rust => "lsr replicate",
        }
    }

    /// What the process writes to its log once it runs.
    const fn up_line(self) -> &'static str {
        match self {
            Self::Go => "replicating to",
            Self::Rust => "replicating ",
        }
    }
}

/// A running replicator process, killed if dropped.
#[derive(Debug)]
pub struct Replicator {
    child: Child,
    log: PathBuf,
    kind: Kind,
}

/// Where cargo puts the `lsr` binary of a debug build.
fn lsr_path() -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || workspace_root().join("target"),
        |dir| workspace_root().join(PathBuf::from(dir)),
    );
    target.join("debug").join("lsr")
}

/// Builds the `lsr` binary of this workspace (a debug build, `--locked`) and returns its path.
///
/// # Errors
/// [`CompatError::Process`] when cargo fails, [`CompatError::MissingBinary`] when the build
/// left no binary where it should be.
pub fn lsr_binary() -> Result<PathBuf, CompatError> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| OsString::from("cargo"));
    let output = Command::new(&cargo)
        .args(["build", "--locked", "-p", "lsr-cli", "--bin", "lsr"])
        .current_dir(workspace_root())
        .output()
        .map_err(CompatError::io("cargo"))?;
    if !output.status.success() {
        return Err(CompatError::Process {
            command: "cargo build -p lsr-cli --bin lsr".to_owned(),
            detail: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    let path = lsr_path();
    if path.is_file() {
        Ok(path)
    } else {
        Err(CompatError::MissingBinary { path })
    }
}

fn spawn(command: &mut Command, binary: &Path, log: &Path) -> Result<Child, CompatError> {
    let out = fs::File::create(log).map_err(CompatError::io(log))?;
    let err = out.try_clone().map_err(CompatError::io(log))?;
    command
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .spawn()
        .map_err(CompatError::io(binary))
}

impl Replicator {
    /// Starts Go's `binary replicate -config config`, output to `log`.
    ///
    /// # Errors
    /// [`CompatError`] when the log or the process cannot be created.
    pub fn start(binary: &Path, config: &Path, log: &Path) -> Result<Self, CompatError> {
        let mut command = Command::new(binary);
        command.arg("replicate").arg("-config").arg(config);
        Ok(Self {
            child: spawn(&mut command, binary, log)?,
            log: log.to_path_buf(),
            kind: Kind::Go,
        })
    }

    /// Starts Rust's `binary replicate db replica` (from [`lsr_binary`]), output to `log`.
    /// It stages in Go's meta directory beside the database, as Go does.
    ///
    /// # Errors
    /// [`CompatError`] when the log or the process cannot be created.
    pub fn start_lsr(
        binary: &Path,
        db: &Path,
        replica: &Path,
        log: &Path,
    ) -> Result<Self, CompatError> {
        let mut command = Command::new(binary);
        command.arg("replicate").arg(db).arg(replica);
        Ok(Self {
            child: spawn(&mut command, binary, log)?,
            log: log.to_path_buf(),
            kind: Kind::Rust,
        })
    }

    /// Which program runs.
    pub const fn kind(&self) -> Kind {
        self.kind
    }

    /// The last lines of the process output.
    pub fn log_tail(&self) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().rev().take(8).collect();
        lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
    }

    /// Fails with the log tail when the process has exited.
    ///
    /// # Errors
    /// [`CompatError::Process`] when it exited.
    pub fn ensure_running(&mut self) -> Result<(), CompatError> {
        match self.child.try_wait().map_err(CompatError::io(&self.log))? {
            None => Ok(()),
            Some(status) => Err(CompatError::Process {
                command: self.kind.command().to_owned(),
                detail: format!("exited early with {status}: {}", self.log_tail()),
            }),
        }
    }

    /// Waits until the log says the process runs.
    ///
    /// # Errors
    /// [`CompatError::Process`] when it exited first, [`CompatError::Timeout`] when it said
    /// nothing within `timeout`.
    pub fn wait_up(&mut self, timeout: Duration) -> Result<(), CompatError> {
        let line = self.kind.up_line();
        let log = self.log.clone();
        wait_until(&format!("{} to run", self.kind.command()), timeout, || {
            self.ensure_running()?;
            Ok(fs::read_to_string(&log).is_ok_and(|text| text.contains(line)))
        })
    }

    /// SIGKILL, then reap the process: a crash at whatever it was doing.
    ///
    /// # Errors
    /// [`CompatError::Process`] when it had already exited, since the kill would then
    /// prove nothing.
    pub fn kill(mut self) -> Result<(), CompatError> {
        self.ensure_running()?;
        self.child.kill().map_err(CompatError::io(&self.log))?;
        self.child.wait().map_err(CompatError::io(&self.log))?;
        Ok(())
    }

    /// SIGINT, the way an operator stops it, then wait for it to exit. Both kinds drain
    /// first. Fails when it does not stop, and for Rust when it exits non-zero: `lsr` exits
    /// zero exactly when everything committed reached the replica.
    ///
    /// # Errors
    /// [`CompatError::Process`] when it did not exit on the signal, or `lsr` did not drain.
    pub fn stop(mut self) -> Result<(), CompatError> {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-INT", &pid])
            .status()
            .map_err(CompatError::io("kill"))?;
        let mut status = None;
        let result = wait_until("replicate to exit", Duration::from_secs(40), || {
            status = self.child.try_wait().map_err(CompatError::io(&self.log))?;
            Ok(status.is_some())
        });
        let command = self.kind.command().to_owned();
        if !sent.success() || result.is_err() {
            return Err(CompatError::Process {
                command,
                detail: format!("did not stop on SIGINT: {}", self.log_tail()),
            });
        }
        match (self.kind, status) {
            (Kind::Rust, Some(code)) if !code.success() => Err(CompatError::Process {
                command,
                detail: format!("stopped with {code}: {}", self.log_tail()),
            }),
            (Kind::Go | Kind::Rust, _) => Ok(()),
        }
    }
}

impl Drop for Replicator {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            drop(self.child.kill());
            drop(self.child.wait());
        }
    }
}
