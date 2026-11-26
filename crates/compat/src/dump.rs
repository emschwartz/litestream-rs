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
//! environment. Only fake keys ever reach these tests. Both chains end at the EC2 metadata
//! service, which a test host may or may not answer, so [`DumpEnv::credentials`] also sets
//! `AWS_EC2_METADATA_DISABLED` for both sides unless the variables point the service at a
//! fake (`AWS_EC2_METADATA_SERVICE_ENDPOINT`, see [`FakeAws`](crate::FakeAws)).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use lsr_config::{
    Credentials, DbConfig, DbSource, Env, Expand, GoConfig, GoDuration, ReplicaConfig, ReplicaKind,
    ReplicaPlan,
};
use lsr_store_s3::{CredentialChain, S3Credentials, SystemClock, Transport, UreqTransport};
use serde::{Deserialize, Serialize};

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
    #[serde(default)]
    config: Option<DumpConfig>,
}

/// The parsed configuration both sides keep, before any database is built
/// ([`DumpEnv::config`]). Durations are nanoseconds.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DumpConfig {
    /// `addr`.
    pub addr: String,
    /// `exec`.
    pub exec: String,
    /// The compaction level intervals.
    pub levels: Vec<i64>,
    /// `snapshot.interval`.
    pub snapshot_interval: Option<i64>,
    /// `snapshot.retention`.
    pub snapshot_retention: Option<i64>,
    /// `l0-retention`.
    pub l0_retention: Option<i64>,
    /// `l0-retention-check-interval`.
    pub l0_retention_check_interval: Option<i64>,
    /// `retention.enabled`; Go's unset is enabled, and is read as `Some(true)`.
    pub retention_enabled: Option<bool>,
    /// `shutdown-sync-timeout`.
    pub shutdown_sync_timeout: Option<i64>,
    /// The `dbs` entries.
    pub dbs: Vec<DumpDbConfig>,
}

/// One `dbs` entry as parsed.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[expect(missing_docs, reason = "each field is the Go key of the same name")]
pub struct DumpDbConfig {
    pub path: String,
    pub dir: String,
    pub pattern: String,
    pub recursive: bool,
    pub meta_path: Option<String>,
    pub meta_dir: Option<String>,
    pub monitor_interval: Option<i64>,
    pub checkpoint_interval: Option<i64>,
    pub min_checkpoint_page_count: Option<i64>,
    pub truncate_page_n: Option<i64>,
    pub max_sync_wal_bytes: Option<i64>,
    pub restore_if_db_not_exists: bool,
    pub snapshot_interval: Option<i64>,
    pub snapshot_retention: Option<i64>,
    pub replica: Option<DumpReplicaConfig>,
}

/// One replica entry as parsed, global settings copied down.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[expect(missing_docs, reason = "each field is the Go key of the same name")]
pub struct DumpReplicaConfig {
    #[serde(rename = "type")]
    pub kind: String,
    pub url: String,
    pub path: String,
    pub sync_interval: Option<i64>,
    pub max_sync_ltx_files: Option<i64>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub bucket: String,
    pub endpoint: String,
    pub force_path_style: Option<bool>,
    pub part_size: Option<i64>,
    pub concurrency: Option<i64>,
}

/// What the parse of a config gives each side: the parsed config or a refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    /// The parsed configuration.
    Config(Box<DumpConfig>),
    /// The file is refused at load; that side's first error.
    Refused(String),
}

/// The dumper's JSON: the last line of its output, since Go logs config warnings to standard
/// output before it.
fn last_json_line(stdout: &[u8]) -> Result<GoOut, String> {
    let text = String::from_utf8_lossy(stdout);
    let line = text
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("");
    serde_json::from_str(line).map_err(|e| format!("{e}: {text}"))
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

/// Go's parse of `config` (environment expansion on): the parsed config, or Go's refusal.
///
/// # Errors
/// The dumper could not run or printed something that is not its JSON.
pub fn go_parse(binary: &Path, config: &Path, env: &DumpEnv) -> Result<Parsed, CompatError> {
    let output = Command::new(binary)
        .arg(config)
        .env_clear()
        .envs(&env.vars)
        .env("HOME", &env.home)
        .env("LSR_DUMP", "1")
        .env("LSR_DUMP_CONFIG", "1")
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
    let out: GoOut = last_json_line(&output.stdout).map_err(process)?;
    Ok(match out.config {
        Some(mut config) => {
            config.retention_enabled = Some(config.retention_enabled.unwrap_or(true));
            Parsed::Config(Box::new(config))
        }
        None => Parsed::Refused(out.error),
    })
}

/// litestream-rs's load of `config`, environment expansion on.
#[must_use]
pub fn rust_parse(config: &Path, env: &DumpEnv) -> Parsed {
    match GoConfig::load(config, Expand::Env, env) {
        Ok(loaded) => Parsed::Config(Box::new(dump_config(&loaded.config))),
        Err(error) => Parsed::Refused(error.to_string()),
    }
}

fn nanos(duration: GoDuration) -> i64 {
    duration.nanos()
}

fn dump_config(config: &GoConfig) -> DumpConfig {
    DumpConfig {
        addr: config.addr().unwrap_or_default().to_owned(),
        exec: config.exec().unwrap_or_default().to_owned(),
        levels: config.levels().iter().copied().map(nanos).collect(),
        snapshot_interval: Some(nanos(config.snapshot().interval())),
        snapshot_retention: Some(nanos(config.snapshot().retention())),
        l0_retention: Some(nanos(config.l0_retention())),
        l0_retention_check_interval: Some(nanos(
            config
                .l0_retention_check_interval()
                .unwrap_or(lsr_config::defaults::L0_RETENTION_CHECK_INTERVAL),
        )),
        retention_enabled: Some(config.retention_enabled()),
        shutdown_sync_timeout: Some(nanos(config.shutdown_sync_timeout())),
        dbs: config.dbs().iter().map(dump_db_config).collect(),
    }
}

fn dump_db_config(db: &DbConfig) -> DumpDbConfig {
    let (path, dir, pattern, recursive, meta_dir) = match db.source() {
        DbSource::Path(path) => (path.display().to_string(), "", "", false, None),
        DbSource::Dir(source) => (
            String::new(),
            source.dir(),
            source.pattern(),
            source.recursive(),
            source.meta_dir().map(str::to_owned),
        ),
    };
    DumpDbConfig {
        path,
        dir: dir.to_owned(),
        pattern: pattern.to_owned(),
        recursive,
        meta_path: db.meta_path().map(str::to_owned),
        meta_dir,
        monitor_interval: db.monitor_interval().map(nanos),
        checkpoint_interval: db.checkpoint_interval().map(nanos),
        min_checkpoint_page_count: db.min_checkpoint_page_count(),
        truncate_page_n: db.truncate_page_n(),
        max_sync_wal_bytes: db.max_sync_wal_bytes(),
        restore_if_db_not_exists: db.restore_if_db_not_exists(),
        snapshot_interval: db.snapshot().interval().map(nanos),
        snapshot_retention: db.snapshot().retention().map(nanos),
        replica: db.replica().map(dump_replica_config),
    }
}

fn dump_replica_config(replica: &ReplicaConfig) -> DumpReplicaConfig {
    let s3 = replica.s3();
    let text = |value: Option<&str>| value.unwrap_or_default().to_owned();
    DumpReplicaConfig {
        kind: match replica.kind() {
            ReplicaKind::File => "file",
            ReplicaKind::S3 => "s3",
        }
        .to_owned(),
        url: text(replica.url()),
        path: text(replica.path()),
        sync_interval: replica.sync_interval().map(nanos),
        max_sync_ltx_files: replica.max_sync_ltx_files(),
        access_key_id: text(s3.access_key_id().map(|s| s.expose())),
        secret_access_key: text(s3.secret_access_key().map(|s| s.expose())),
        region: text(s3.region()),
        bucket: text(s3.bucket()),
        endpoint: text(s3.endpoint()),
        force_path_style: s3.force_path_style(),
        part_size: s3.part_size().and_then(|n| i64::try_from(n).ok()),
        concurrency: s3.concurrency().and_then(|n| i64::try_from(n).ok()),
    }
}

/// What both sides get for the metadata service unless a case names a fake one: off, so
/// neither side asks whatever answers on the test host.
const NO_IMDS: (&str, &str) = ("AWS_EC2_METADATA_DISABLED", "true");
const IMDS_ENDPOINT: &str = "AWS_EC2_METADATA_SERVICE_ENDPOINT";

impl DumpEnv {
    fn var_or_home(&self, name: &str) -> Option<String> {
        if name == "HOME" {
            return Some(self.home.display().to_string());
        }
        if self.imds_off() && name == NO_IMDS.0 {
            return Some(NO_IMDS.1.to_owned());
        }
        self.vars.get(name).cloned()
    }

    fn imds_off(&self) -> bool {
        self.credentials && !self.vars.contains_key(IMDS_ENDPOINT)
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
        command.env("LSR_DUMP_CREDENTIALS", "1");
    }
    if env.imds_off() {
        command.env(NO_IMDS.0, NO_IMDS.1);
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
    let out: GoOut = last_json_line(&output.stdout).map_err(process)?;
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
                        credentials: env
                            .credentials
                            .then(|| resolve(credentials, config.region().as_str(), env)),
                    },
                },
            })
            .collect(),
    )
}

/// What the store would sign with: the config's keys, else the chain over `env`, with the
/// daemon's HTTP client for the sources that fetch.
fn resolve(credentials: &Credentials, region: &str, env: &DumpEnv) -> DumpCredentials {
    let env = env.clone();
    let chain = CredentialChain::with_env(move |name| env.var_or_home(name));
    // No TLS provider: the sources that fetch over HTTP are not built, as in the daemon.
    let chain = match UreqTransport::new(Duration::from_secs(5)) {
        Ok(transport) => {
            let transport: Arc<dyn Transport> = Arc::new(transport);
            chain.with_transport(transport, Arc::new(SystemClock))
        }
        Err(_no_provider) => chain,
    }
    .with_region(region);
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
