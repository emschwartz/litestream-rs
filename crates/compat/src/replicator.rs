//! A running `litestream replicate` and the one way the harness waits: polling a condition.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::error::CompatError;

const POLL: Duration = Duration::from_millis(20);

/// Polls `cond` until it holds or `timeout` passes.
///
/// # Errors
/// [`CompatError::Timeout`] naming `what`, or whatever `cond` returned.
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

/// A running `litestream replicate`, killed if dropped.
#[derive(Debug)]
pub struct Replicator {
    child: Child,
    log: PathBuf,
}

impl Replicator {
    /// Starts `binary replicate -config config`, output to `log`.
    ///
    /// # Errors
    /// [`CompatError`] when the log or the process cannot be created.
    pub fn start(binary: &Path, config: &Path, log: &Path) -> Result<Self, CompatError> {
        let out = fs::File::create(log).map_err(CompatError::io(log))?;
        let err = out.try_clone().map_err(CompatError::io(log))?;
        let child = Command::new(binary)
            .arg("replicate")
            .arg("-config")
            .arg(config)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .map_err(CompatError::io(binary))?;
        Ok(Self {
            child,
            log: log.to_path_buf(),
        })
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
                command: "litestream replicate".to_owned(),
                detail: format!("exited early with {status}: {}", self.log_tail()),
            }),
        }
    }

    /// SIGINT, the way an operator stops it, then wait for a clean exit.
    /// Fails when it does not stop.
    ///
    /// # Errors
    /// [`CompatError::Process`] when it did not exit on the signal.
    pub fn stop(mut self) -> Result<(), CompatError> {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .args(["-INT", &pid])
            .status()
            .map_err(CompatError::io("kill"))?;
        let mut exited = false;
        let result = wait_until("replicate to exit", Duration::from_secs(30), || {
            exited = self
                .child
                .try_wait()
                .map_err(CompatError::io(&self.log))?
                .is_some();
            Ok(exited)
        });
        if !sent.success() || result.is_err() {
            return Err(CompatError::Process {
                command: "litestream replicate".to_owned(),
                detail: format!("did not stop on SIGINT: {}", self.log_tail()),
            });
        }
        Ok(())
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
