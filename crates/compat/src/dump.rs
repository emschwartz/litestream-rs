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
//!
//! With [`DumpEnv::credentials`], each S3 replica also carries the credentials it would sign
//! with: Go's from `config.LoadDefaultConfig` with the options its S3 client's `Init` passes,
//! Rust's from the plan's [`Credentials`] and `lsr_store_s3::CredentialChain` over the same
//! environment. Only fake keys ever reach these tests. Go's chain would go on to ECS and the
//! EC2 metadata service, which litestream-rs does not build, so [`DumpEnv::credentials`]
//! also sets `AWS_EC2_METADATA_DISABLED` for both sides.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use lsr_config::{Credentials, Env, Expand, GoConfig, ReplicaPlan};
use lsr_store_s3::{CredentialChain, S3Credentials};
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
    /// What an S3 replica would sign with, when asked for.
    #[serde(default)]
    pub credentials: Option<DumpCredentials>,
}

/// The credentials a replica would sign with, or a refusal.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct DumpCredentials {
    /// Why there are none; empty when resolved. Compared as present or absent only.
    #[serde(default)]
    pub error: String,
    /// The key id.
    #[serde(default)]
    pub access_key_id: String,
    /// The secret (fake in every test).
    #[serde(default)]
    pub secret_access_key: String,
    /// The session token, empty for none.
    #[serde(default)]
    pub session_token: String,
}

impl DumpCredentials {
    fn agrees_with(&self, other: &Self) -> bool {
        match (self.error.is_empty(), other.error.is_empty()) {
            (true, true) => self == other,
            (false, false) => true,
            (true, false) | (false, true) => false,
        }
    }

    fn of(credentials: &S3Credentials) -> Self {
        let (id, secret, token) = credentials.exposed();
        Self {
            error: String::new(),
            access_key_id: id.to_owned(),
            secret_access_key: secret.to_owned(),
            session_token: token.unwrap_or_default().to_owned(),
        }
    }
}

impl DumpReplica {
    fn agrees_with(&self, other: &Self) -> bool {
        let credentials = match (&self.credentials, &other.credentials) {
            (Some(a), Some(b)) => a.agrees_with(b),
            (None, None) => true,
            (Some(_), None) | (None, Some(_)) => false,
        };
        credentials
            && self.kind == other.kind
            && self.path == other.path
            && self.bucket == other.bucket
    }
}

impl DumpDb {
    fn agrees_with(&self, other: &Self) -> bool {
        self.path == other.path
            && self.meta == other.meta
            && self.replica.agrees_with(&other.replica)
    }
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
            (Self::Runs(a), Self::Runs(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| a.agrees_with(b))
            }
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
    /// Whether S3 replicas carry their resolved credentials.
    pub credentials: bool,
}

/// What both sides get for the metadata service: off, so Go's chain ends where Rust's does.
const NO_IMDS: (&str, &str) = ("AWS_EC2_METADATA_DISABLED", "true");

impl DumpEnv {
    fn var_or_home(&self, name: &str) -> Option<String> {
        if name == "HOME" {
            return Some(self.home.display().to_string());
        }
        if self.credentials && name == NO_IMDS.0 {
            return Some(NO_IMDS.1.to_owned());
        }
        self.vars.get(name).cloned()
    }
}

impl Env for DumpEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.var_or_home(name)
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
    let mut command = Command::new(binary);
    command
        .arg(config)
        .env_clear()
        .envs(&env.vars)
        .env("HOME", &env.home)
        .env("LSR_DUMP", "1")
        .current_dir(&env.cwd);
    if env.credentials {
        command
            .env("LSR_DUMP_CREDENTIALS", "1")
            .env(NO_IMDS.0, NO_IMDS.1);
    }
    let output = command.output().map_err(CompatError::io(binary))?;
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
                        credentials: None,
                    },
                    ReplicaPlan::S3 {
                        config,
                        credentials,
                    } => DumpReplica {
                        kind: "s3".to_owned(),
                        path: config.prefix().as_str().to_owned(),
                        bucket: config.bucket().as_str().to_owned(),
                        credentials: env.credentials.then(|| resolve(credentials, env)),
                    },
                },
            })
            .collect(),
    )
}

/// What the store would sign with: the config's keys, else the chain over `env`.
fn resolve(credentials: &Credentials, env: &DumpEnv) -> DumpCredentials {
    let env = env.clone();
    let chain = CredentialChain::with_env(move |name| env.var_or_home(name));
    // The daemon's own construction (`sidecar/src/daemon.rs`): config keys are explicit
    // values on the same chain, so the shared files are still read first.
    let chain = match credentials {
        Credentials::FromConfig(keys) => chain.with_explicit(keys.clone()),
        Credentials::FromEnvironment => chain,
    };
    match chain.resolve() {
        Ok(resolved) => DumpCredentials::of(resolved.credentials()),
        Err(error) => DumpCredentials {
            error: error.to_string(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: String::new(),
        },
    }
}
