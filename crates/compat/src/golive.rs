//! Running the pinned Go binaries: configuration, restore, and time formatting.

use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use rusqlite::Connection;

use crate::error::CompatError;
use crate::manifest::Target;

/// The configuration both `replicate` and `restore` read: short intervals so a replica holds
/// every level and several snapshots within seconds. The keys exist, with the same meaning,
/// from v0.5.0 to 041a8c3 (read in each tag's `cmd/litestream/main.go`).
pub fn write_config(path: &Path, db: &Path, replica: &Path) -> Result<(), CompatError> {
    let text = format!(
        "levels:
  - interval: 1s
  - interval: 2s
  - interval: 4s
snapshot:
  interval: 3s
  retention: 1h
dbs:
  - path: {db}
    monitor-interval: 100ms
    checkpoint-interval: 1h
    replica:
      type: file
      path: {replica}
      sync-interval: 100ms
",
        db = db.display(),
        replica = replica.display()
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
/// the given pinned binary.
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
