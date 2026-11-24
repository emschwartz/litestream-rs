//! Running a program and reading what it cost.
//!
//! A child's CPU time and peak resident memory reach only its parent, and the peak is the
//! largest of every child the parent ever reaped. So each measured program runs under a
//! wrapper: this binary started again as `exec`, which runs the program under `/usr/bin/time`
//! (the one tool that reports both without unsafe code) and, once it exits, writes the wall
//! time, user plus system CPU time (10 ms resolution) and peak resident memory to a file. The harness stops a replicator with SIGINT, the way an
//! operator does, and reads the file.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use super::clock::now;
use crate::error::CompatError;
use crate::replicator::{UP_LINE, wait_until};
use crate::s3::{home_beside, scrub_env};

/// The first argument that makes the binary run as the wrapper.
pub(super) const EXEC: &str = "exec";

/// What one run of a program cost.
#[derive(Debug, Clone, Copy)]
pub(super) struct Usage {
    /// From just before the spawn to the exit.
    pub wall: Duration,
    /// User plus system CPU time.
    pub cpu: Duration,
    /// Peak resident set size in bytes.
    pub peak_rss: u64,
    /// Whether the program exited zero.
    pub success: bool,
}

/// `time`'s report of its child: user plus system seconds and peak resident bytes. macOS's
/// `time -l` prints a `real user sys` line and a `maximum resident set size` line in bytes;
/// GNU `time -f '%e %U %S %M'` prints one line, the last, with the size in KiB.
fn parse_time(text: &str) -> Option<(Duration, u64)> {
    let secs_before = |line: &str, word: &str| -> Option<f64> {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let at = tokens.iter().position(|t| *t == word)?;
        tokens.get(at.checked_sub(1)?)?.parse().ok()
    };
    if cfg!(target_os = "macos") {
        let times = text.lines().find(|l| l.contains(" real"))?;
        let rss = text
            .lines()
            .find(|l| l.contains("maximum resident set size"))?;
        let cpu = secs_before(times, "user")? + secs_before(times, "sys")?;
        let bytes = rss.split_whitespace().next()?.parse().ok()?;
        Some((Duration::try_from_secs_f64(cpu).ok()?, bytes))
    } else {
        let mut parts = text.lines().last()?.split_whitespace();
        let _real: f64 = parts.next()?.parse().ok()?;
        let user: f64 = parts.next()?.parse().ok()?;
        let sys: f64 = parts.next()?.parse().ok()?;
        let kib: u64 = parts.next()?.parse().ok()?;
        Some((
            Duration::try_from_secs_f64(user + sys).ok()?,
            kib.saturating_mul(1024),
        ))
    }
}

/// The wrapper: `exec STATS PIDFILE PROGRAM ARGS...`. Runs the program under `time` without the
/// machine's `AWS_*` and `LITESTREAM_*` variables, writes the program's pid (a child of
/// `time`), waits, writes its usage.
pub(super) fn exec_wrapper(args: &[OsString]) -> Result<(), CompatError> {
    let [stats, pidfile, program, rest @ ..] = args else {
        return Err(CompatError::Fixture(
            "exec needs STATS PIDFILE PROGRAM".to_owned(),
        ));
    };
    let stats = Path::new(stats);
    let raw = stats.with_extension("time");
    let mut command = Command::new("/usr/bin/time");
    if cfg!(target_os = "macos") {
        command.arg("-l");
    } else {
        command.args(["-f", "%e %U %S %M"]);
    }
    command.arg("-o").arg(&raw).arg(program).args(rest);
    scrub_env(&mut command, &home_beside(stats))?;
    let start = now();
    let mut timer = command.spawn().map_err(CompatError::io("/usr/bin/time"))?;
    let mut child_pid = None;
    wait_until("time to start the program", Duration::from_secs(10), || {
        let out = Command::new("pgrep")
            .args(["-P", &timer.id().to_string()])
            .output()
            .map_err(CompatError::io("pgrep"))?;
        child_pid = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .and_then(|l| l.trim().parse::<u32>().ok());
        Ok(child_pid.is_some() || timer.try_wait().map_err(CompatError::io("time"))?.is_some())
    })?;
    fs::write(pidfile, child_pid.unwrap_or(0).to_string()).map_err(CompatError::io(pidfile))?;
    let status = timer.wait().map_err(CompatError::io(program))?;
    let wall = now().duration_since(start);
    let report = fs::read_to_string(&raw).map_err(CompatError::io(&raw))?;
    let (cpu, rss) = parse_time(&report)
        .ok_or_else(|| CompatError::Fixture(format!("unreadable time report: {report:?}")))?;
    let line = format!(
        "{} {} {rss} {}",
        wall.as_nanos(),
        cpu.as_nanos(),
        status.success()
    );
    fs::write(stats, line).map_err(CompatError::io(stats))
}

fn parse_usage(text: &str) -> Option<Usage> {
    let mut parts = text.split_whitespace();
    let nanos = |p: Option<&str>| p?.parse::<u64>().ok().map(Duration::from_nanos);
    Some(Usage {
        wall: nanos(parts.next())?,
        cpu: nanos(parts.next())?,
        peak_rss: parts.next()?.parse().ok()?,
        success: parts.next()?.parse().ok()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_reports_parse_on_this_platform() {
        let (text, cpu_ms, bytes) = if cfg!(target_os = "macos") {
            (
                "        1.50 real         1.20 user         0.30 sys\n  52428800  maximum resident set size\n   100  page reclaims\n",
                1500,
                52_428_800,
            )
        } else {
            (
                "Command exited with non-zero status 1\n1.50 1.20 0.30 51200\n",
                1500,
                52_428_800,
            )
        };
        let (cpu, rss) = parse_time(text).expect("parses");
        assert_eq!(cpu.as_millis(), cpu_ms);
        assert_eq!(rss, bytes);
        assert!(parse_time("garbage").is_none());
    }

    #[test]
    fn usage_lines_round_trip() {
        let usage = parse_usage("2000000000 1500000000 52428800 true").expect("parses");
        assert_eq!(usage.wall, Duration::from_secs(2));
        assert_eq!(usage.cpu, Duration::from_millis(1500));
        assert_eq!(usage.peak_rss, 52_428_800);
        assert!(usage.success);
        assert!(parse_usage("2 1 x true").is_none());
    }
}

/// A program running under the wrapper; killed if dropped.
#[derive(Debug)]
pub(super) struct Managed {
    wrapper: Child,
    pid: u32,
    stats: PathBuf,
    log: PathBuf,
    what: String,
}

impl Managed {
    /// Starts `program args` under the wrapper (`exe` is this binary), its output in
    /// `dir/tag.log`, and waits until the wrapper has started it.
    pub(super) fn start(
        exe: &Path,
        program: &Path,
        args: &[&OsStr],
        dir: &Path,
        tag: &str,
    ) -> Result<Self, CompatError> {
        Self::start_with_env(exe, program, args, dir, tag, &[])
    }

    /// As [`Managed::start`], with these variables added to the program's environment.
    pub(super) fn start_with_env(
        exe: &Path,
        program: &Path,
        args: &[&OsStr],
        dir: &Path,
        tag: &str,
        env: &[(&str, String)],
    ) -> Result<Self, CompatError> {
        let stats = dir.join(format!("{tag}.usage"));
        let pidfile = dir.join(format!("{tag}.pid"));
        let log = dir.join(format!("{tag}.log"));
        for stale in [&stats, &pidfile] {
            drop(fs::remove_file(stale));
        }
        let out = fs::File::create(&log).map_err(CompatError::io(&log))?;
        let err = out.try_clone().map_err(CompatError::io(&log))?;
        let wrapper = Command::new(exe)
            .arg(EXEC)
            .arg(&stats)
            .arg(&pidfile)
            .arg(program)
            .args(args)
            .envs(env.iter().map(|(k, v)| (*k, v)))
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .map_err(CompatError::io(exe))?;
        let mut me = Self {
            wrapper,
            pid: 0,
            stats,
            log,
            what: program.display().to_string(),
        };
        let mut pid = None;
        wait_until(
            "the wrapper to start the program",
            Duration::from_secs(10),
            || {
                pid = fs::read_to_string(&pidfile)
                    .ok()
                    .and_then(|t| t.trim().parse::<u32>().ok());
                Ok(pid.is_some())
            },
        )?;
        me.pid = pid.unwrap_or(0);
        Ok(me)
    }

    /// The program's own process id (the wrapper's child), for `/proc/<pid>`.
    pub(super) const fn pid(&self) -> u32 {
        self.pid
    }

    /// The last lines of the program's output.
    pub(super) fn tail(&self) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().rev().take(8).collect();
        lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
    }

    /// Fails with the log tail when the program has exited.
    pub(super) fn ensure_running(&mut self) -> Result<(), CompatError> {
        match self
            .wrapper
            .try_wait()
            .map_err(CompatError::io(&self.log))?
        {
            None => Ok(()),
            Some(status) => Err(CompatError::Process {
                command: self.what.clone(),
                detail: format!("exited early with {status}: {}", self.tail()),
            }),
        }
    }

    /// Waits until the log says the replicator runs.
    pub(super) fn wait_up(&mut self, timeout: Duration) -> Result<(), CompatError> {
        let log = self.log.clone();
        wait_until("the replicator to run", timeout, || {
            self.ensure_running()?;
            Ok(fs::read_to_string(&log).is_ok_and(|t| t.contains(UP_LINE)))
        })
        .map_err(|e| self.explain(e))
    }

    fn explain(&self, err: CompatError) -> CompatError {
        match err {
            CompatError::Timeout(what) => {
                CompatError::Timeout(format!("{what}; the log ends: {}", self.tail()))
            }
            other @ (CompatError::Io { .. }
            | CompatError::Sqlite(_)
            | CompatError::Open(_)
            | CompatError::Json { .. }
            | CompatError::BadKey(_)
            | CompatError::MissingBinary { .. }
            | CompatError::Process { .. }
            | CompatError::Fixture(_)
            | CompatError::Integrity { .. }
            | CompatError::NotPages { .. }) => other,
        }
    }

    fn usage(&mut self, timeout: Duration) -> Result<Usage, CompatError> {
        let mut usage = None;
        let wrapper = &mut self.wrapper;
        let stats = &self.stats;
        wait_until("the program to exit", timeout, || {
            if wrapper
                .try_wait()
                .map_err(CompatError::io(stats))?
                .is_none()
            {
                return Ok(false);
            }
            usage = fs::read_to_string(stats).ok().and_then(|t| parse_usage(&t));
            Ok(true)
        })
        .map_err(|e| self.explain(e))?;
        usage.ok_or_else(|| CompatError::Process {
            command: self.what.clone(),
            detail: "the wrapper left no usage".to_owned(),
        })
    }

    /// SIGINT, the way an operator stops a replicator, then its usage. A non-zero exit is an
    /// error: both programs exit zero once everything committed is in the replica.
    pub(super) fn stop(mut self) -> Result<Usage, CompatError> {
        let sent = Command::new("kill")
            .args(["-INT", &self.pid.to_string()])
            .status()
            .map_err(CompatError::io("kill"))?;
        let usage = self.usage(Duration::from_secs(120))?;
        if !sent.success() || !usage.success {
            return Err(CompatError::Process {
                command: self.what.clone(),
                detail: format!("did not stop cleanly on SIGINT: {}", self.tail()),
            });
        }
        Ok(usage)
    }

    /// Waits for a program that ends on its own (a restore) and returns its usage.
    pub(super) fn finish(mut self, timeout: Duration) -> Result<Usage, CompatError> {
        let usage = self.usage(timeout)?;
        if !usage.success {
            return Err(CompatError::Process {
                command: self.what.clone(),
                detail: format!("failed: {}", self.tail()),
            });
        }
        Ok(usage)
    }
}

impl Drop for Managed {
    fn drop(&mut self) {
        if matches!(self.wrapper.try_wait(), Ok(None)) {
            drop(
                Command::new("kill")
                    .args(["-KILL", &self.pid.to_string()])
                    .status(),
            );
            drop(self.wrapper.wait());
        }
    }
}
