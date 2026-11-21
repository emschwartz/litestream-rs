//! Running the pinned Go binaries: configuration, restore, and time formatting.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use rusqlite::Connection;

use crate::error::CompatError;
use crate::manifest::Target;
use crate::remote::Remote;
use crate::s3::{home_beside, scrub_env};

/// The configuration both `replicate` and `restore` read: short intervals so a replica holds
/// every level and several snapshots within seconds. The keys exist, with the same meaning,
/// from v0.5.0 to 041a8c3 (read in each tag's `cmd/litestream/main.go`).
pub fn write_config(path: &Path, db: &Path, replica: &Path) -> Result<(), CompatError> {
    write_config_snapshotting(path, db, replica, "3s")
}

/// [`write_config`] with a chosen snapshot interval (Go's duration syntax), for a run that
/// must not take a snapshot of its own while a check looks at what the replica holds.
pub fn write_config_snapshotting(
    path: &Path,
    db: &Path,
    replica: &Path,
    snapshot_interval: &str,
) -> Result<(), CompatError> {
    write_config_with(path, db, replica, snapshot_interval, "1h")
}

/// [`write_config`] whose snapshot window is a day, as Rust's default, so neither Go nor Rust
/// takes a scheduled snapshot while a test runs once one exists (both snapshot at once when
/// the replica has none). A new snapshot during such a run is then a forced one, never the
/// schedule's, unless the run crosses midnight UTC, where both windows start.
pub fn write_config_daily_snapshots(
    path: &Path,
    db: &Path,
    replica: &Path,
) -> Result<(), CompatError> {
    write_config_with(path, db, replica, "24h", "24h")
}

fn write_config_with(
    path: &Path,
    db: &Path,
    replica: &Path,
    snapshot_interval: &str,
    snapshot_retention: &str,
) -> Result<(), CompatError> {
    let schedule = Schedule {
        levels: Schedule::FAST.levels,
        snapshot_interval,
        snapshot_retention,
    };
    write_config_for(path, db, &Remote::Dir(replica.to_path_buf()), schedule)
}

/// How often a replicator compacts and snapshots, in Go's duration syntax: the `levels`
/// intervals and the `snapshot` section of the config file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule<'a> {
    /// The compaction level intervals, level 1 first.
    pub levels: &'a [&'a str],
    /// `snapshot.interval`.
    pub snapshot_interval: &'a str,
    /// `snapshot.retention`.
    pub snapshot_retention: &'a str,
}

impl Schedule<'static> {
    /// Short intervals, so a replica holds every level and several snapshots within seconds
    /// ([`write_config`]).
    pub const FAST: Self = Self {
        levels: &["1s", "2s", "4s"],
        snapshot_interval: "3s",
        snapshot_retention: "1h",
    };

    /// [`Schedule::FAST`]'s levels with a day-long snapshot window
    /// ([`write_config_daily_snapshots`]).
    pub const DAILY_SNAPSHOTS: Self = Self {
        levels: &["1s", "2s", "4s"],
        snapshot_interval: "24h",
        snapshot_retention: "24h",
    };

    /// No compaction and no scheduled snapshot within a run: the replica holds the boot's
    /// snapshot and one level-0 file per sync, each stamped when it was written, so a restore
    /// at an instant between two syncs has exactly one right answer, whichever stamp a reader
    /// takes (Go before v0.5.16 reads only the object's `LastModified`).
    pub const LEVEL_ZERO_ONLY: Self = Self {
        levels: &["1h"],
        snapshot_interval: "24h",
        snapshot_retention: "24h",
    };
}

/// The config file for `db` replicating to `remote` on `schedule`. For an S3 remote it names
/// the endpoint, path style, region and credentials explicitly (`s3.rs`); Go v0.5.0 and later
/// read every key it writes, and so does `lsr replicate -config`.
pub fn write_config_for(
    path: &Path,
    db: &Path,
    remote: &Remote,
    schedule: Schedule<'_>,
) -> Result<(), CompatError> {
    let levels: String = schedule
        .levels
        .iter()
        .map(|interval| format!("  - interval: {interval}\n"))
        .collect();
    let text = format!(
        "levels:
{levels}snapshot:
  interval: {snapshot_interval}
  retention: {snapshot_retention}
dbs:
  - path: {db}
    monitor-interval: 100ms
    checkpoint-interval: 1h
{replica}",
        db = db.display(),
        snapshot_interval = schedule.snapshot_interval,
        snapshot_retention = schedule.snapshot_retention,
        replica = remote.yaml(),
    );
    fs::write(path, text).map_err(CompatError::io(path))
}

/// The outcome of one Go restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GoRestore {
    /// Go exited zero and wrote the output database.
    Restored,
    /// Go exited non-zero; its status and the tail of its output.
    Refused {
        /// What Go said.
        detail: String,
    },
}

/// `litestream restore` of `db_path`'s replica from `config` into `out` for a target, with
/// the given pinned binary, in an environment without `AWS_*` or `LITESTREAM_*` variables
/// ([`scrub_env`]).
pub fn restore_with_go(
    binary: &Path,
    config: &Path,
    db_path: &Path,
    out: &Path,
    target: Target,
) -> Result<GoRestore, CompatError> {
    let mut cmd = Command::new(binary);
    cmd.arg("restore")
        .arg("-config")
        .arg(config)
        .arg("-o")
        .arg(out);
    match target {
        Target::Latest => {}
        Target::Txid { txid } => {
            cmd.arg("-txid").arg(format!("{txid:016x}"));
        }
        Target::AsOf { unix_ms } => {
            cmd.arg("-timestamp").arg(rfc3339_millis(unix_ms)?);
        }
    }
    cmd.arg(db_path);
    scrub_env(&mut cmd, &home_beside(out))?;
    let output = cmd.output().map_err(CompatError::io(binary))?;
    if output.status.success() {
        return Ok(GoRestore::Restored);
    }
    let text = String::from_utf8_lossy(&output.stderr);
    let tail: Vec<&str> = text.lines().rev().take(3).collect();
    Ok(GoRestore::Refused {
        detail: format!("{}: {}", output.status, tail.join(" | ")),
    })
}

/// Builds the Go helper `go/verify` (the pinned ltx library's `Decoder.Verify`) into `into`.
/// Needs the Go toolchain and the ltx module in Go's module cache or network access.
pub fn build_verify_helper(into: &Path) -> Result<PathBuf, CompatError> {
    let source = crate::workspace_root()
        .join("crates")
        .join("compat")
        .join("go")
        .join("verify");
    let binary = into.join("ltx-verify");
    let output = Command::new("go")
        .args(["build", "-trimpath", "-o"])
        .arg(&binary)
        .arg(".")
        .current_dir(&source)
        .output()
        .map_err(CompatError::io("go"))?;
    if output.status.success() {
        Ok(binary)
    } else {
        Err(CompatError::Process {
            command: format!("go build in {}", source.display()),
            detail: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

/// Runs the verify helper over `files`; `Ok(())` when Go's decoder accepted every one,
/// otherwise the helper's report of which failed and why.
pub fn go_verify(helper: &Path, files: &[PathBuf]) -> Result<(), CompatError> {
    let output = Command::new(helper)
        .args(files)
        .output()
        .map_err(CompatError::io(helper))?;
    if output.status.success() {
        return Ok(());
    }
    Err(CompatError::Process {
        command: format!("{} ({} files)", helper.display(), files.len()),
        detail: String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|l| l.starts_with("FAIL"))
            .take(5)
            .collect::<Vec<_>>()
            .join(" | "),
    })
}

/// Unix milliseconds as an RFC 3339 UTC time with millisecond precision, the form Go's
/// `-timestamp` flag parses. SQLite does the calendar arithmetic.
pub fn rfc3339_millis(unix_ms: i64) -> Result<String, CompatError> {
    let conn = Connection::open_in_memory()?;
    Ok(conn.query_row(
        "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', ?1 / 1000.0, 'unixepoch')",
        [unix_ms],
        |r| r.get(0),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_known_instants() {
        assert_eq!(rfc3339_millis(0).expect("fmt"), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            rfc3339_millis(1_700_000_000_123).expect("fmt"),
            "2023-11-14T22:13:20.123Z"
        );
    }
}
