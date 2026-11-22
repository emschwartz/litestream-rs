//! The effective-config differential: what Go Litestream and litestream-rs would replicate
//! from one config file, printed in one shape so a test can compare them.
//!
//! Go has no command that prints the configuration it would run (`litestream databases` does
//! not expand `dir` entries), so `scripts/build-go-litestream.sh` builds
//! `litestream-dump-<name>`: the pinned `cmd/litestream` package plus one file,
//! `go/dump/lsrdump.go`, which calls Go's own `ReadConfig`, `NewDBsFromDirectoryConfig` and
//! `NewDBFromConfig` (the calls `replicate` makes before it starts) and prints the result as
//! JSON. [`rust_dump`] prints the same shape from `lsr_config::plan`, the function the daemon
//! starts from. Both run against one [`DumpEnv`]: the Go child gets exactly its variables,
//! home and current directory, and nothing else from this process.
//!
//! Equality is on the databases found, their meta directories and their replicas. A refusal is
//! compared as a refusal only: the two error texts differ by design (litestream-rs names the
//! key and line).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use lsr_config::{Env, Expand, GoConfig, ReplicaPlan};
use serde::Deserialize;

use crate::error::CompatError;

/// One replica as both sides print it.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
pub struct DumpReplica {
    /// Go's `ReplicaClient.Type()`: `file` or `s3`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The directory of a file replica, or the key prefix of an S3 one.
    pub path: String,
    /// The bucket of an S3 replica, empty for a file replica.
    #[serde(default)]
    pub bucket: String,
}

/// One database as both sides print it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DumpDb {
    /// The database file, absolute.
    pub path: String,
    /// The meta directory.
    pub meta: String,
    /// Where it replicates to.
    pub replica: DumpReplica,
}

/// What one side would run from a config file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Dump {
    /// The databases, in start order.
    Runs(Vec<DumpDb>),
    /// The config is refused; the text is that side's first error.
    Refused(String),
}

impl Dump {
    /// Whether both sides would do the same: the same databases, or both refuse.
    #[must_use]
    pub fn agrees_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Runs(a), Self::Runs(b)) => a == b,
            (Self::Refused(_), Self::Refused(_)) => true,
            (Self::Runs(_), Self::Refused(_)) | (Self::Refused(_), Self::Runs(_)) => false,
        }
    }
}

#[derive(Deserialize)]
struct GoOut {
    #[serde(default)]
    error: String,
    dbs: Vec<DumpDb>,
}

/// The environment both sides see: variables, home and current directory.
#[derive(Clone, Debug)]
pub struct DumpEnv {
    /// The variables, and nothing else.
    pub vars: BTreeMap<String, String>,
    /// `HOME`, for `~`.
    pub home: PathBuf,
    /// The current directory, for relative paths.
    pub cwd: PathBuf,
}

impl Env for DumpEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.vars.get(name).cloned()
    }
    fn pid(&self) -> u32 {
        std::process::id()
    }
    fn home_dir(&self) -> Option<PathBuf> {
        Some(self.home.clone())
    }
    fn current_dir(&self) -> Option<PathBuf> {
        Some(self.cwd.clone())
    }
}

/// Runs the Go dumper on `config`.
///
/// # Errors
/// The dumper could not run or printed something that is not its JSON.
pub fn go_dump(binary: &Path, config: &Path, env: &DumpEnv) -> Result<Dump, CompatError> {
    let output = Command::new(binary)
        .arg(config)
        .env_clear()
        .envs(&env.vars)
        .env("HOME", &env.home)
        .env("LSR_DUMP", "1")
        .current_dir(&env.cwd)
        .output()
        .map_err(CompatError::io(binary))?;
    let process = |detail: String| CompatError::Process {
        command: format!("{} {}", binary.display(), config.display()),
        detail,
    };
    if !output.status.success() {
        return Err(process(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    let out: GoOut = serde_json::from_slice(&output.stdout)
        .map_err(|e| process(format!("{e}: {}", String::from_utf8_lossy(&output.stdout))))?;
    Ok(if out.error.is_empty() {
        Dump::Runs(out.dbs)
    } else {
        Dump::Refused(out.error)
    })
}

/// What `lsr replicate -config <config>` would run: the load and the plan the daemon starts from.
#[must_use]
pub fn rust_dump(config: &Path, env: &DumpEnv) -> Dump {
    let loaded = match GoConfig::load(config, Expand::Env, env) {
        Ok(loaded) => loaded,
        Err(error) => return Dump::Refused(error.to_string()),
    };
    let plan = match lsr_config::plan(&loaded.config, env) {
        Ok(plan) => plan,
        Err(error) => return Dump::Refused(error.to_string()),
    };
    Dump::Runs(
        plan.dbs()
            .iter()
            .map(|db| DumpDb {
                path: db.path().display().to_string(),
                meta: db.meta_dir().display().to_string(),
                replica: match db.replica() {
                    ReplicaPlan::File(path) => DumpReplica {
                        kind: "file".to_owned(),
                        path: path.display().to_string(),
                        bucket: String::new(),
                    },
                    ReplicaPlan::S3 { config, .. } => DumpReplica {
                        kind: "s3".to_owned(),
                        path: config.prefix().as_str().to_owned(),
                        bucket: config.bucket().as_str().to_owned(),
                    },
                },
            })
            .collect(),
    )
}
