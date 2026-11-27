//! The typed config: what litestream-rs does with a Go file, Go's defaults applied.
//!
//! Only implemented keys appear here; off-only keys were checked and no-effect
//! keys warned about during the load. `Option` stands where Go uses a pointer
//! and applies its default later, when it builds the database or replica
//! (the `map` module turns those into the replicator's own config). Plain types only: no
//! dependency on the replicator.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::duration::GoDuration;

/// Go's `DefaultConfig` values (`cmd/litestream/main.go`, `store.go`, `db.go`).
pub mod defaults {
    use crate::duration::GoDuration;

    /// `levels`: L1 30s, L2 5m, L3 1h (`DefaultCompactionLevels`).
    pub const LEVELS: [GoDuration; 3] = [
        GoDuration::from_secs(30),
        GoDuration::from_secs(300),
        GoDuration::from_secs(3600),
    ];
    /// `snapshot.interval`: 24h.
    pub const SNAPSHOT_INTERVAL: GoDuration = GoDuration::from_secs(86_400);
    /// `snapshot.retention`: 24h.
    pub const SNAPSHOT_RETENTION: GoDuration = GoDuration::from_secs(86_400);
    /// `l0-retention`: 5m (`DefaultL0Retention`).
    pub const L0_RETENTION: GoDuration = GoDuration::from_secs(300);
    /// `l0-retention-check-interval`: 15s (`DefaultL0RetentionCheckInterval`).
    pub const L0_RETENTION_CHECK_INTERVAL: GoDuration = GoDuration::from_secs(15);
    /// `shutdown-sync-timeout`: 30s (`DefaultShutdownSyncTimeout`).
    pub const SHUTDOWN_SYNC_TIMEOUT: GoDuration = GoDuration::from_secs(30);
}

/// A Go config file as litestream-rs runs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoConfig {
    pub(crate) addr: Option<String>,
    pub(crate) exec: Option<String>,
    pub(crate) levels: Vec<GoDuration>,
    pub(crate) snapshot: Snapshot,
    pub(crate) l0_retention: GoDuration,
    /// `None` when the file leaves it unset: a set value is the interval the check runs at,
    /// exactly; only the unset default is stretched (D193).
    pub(crate) l0_retention_check_interval: Option<GoDuration>,
    pub(crate) retention_enabled: bool,
    pub(crate) shutdown_sync_timeout: GoDuration,
    pub(crate) validation_interval: Option<GoDuration>,
    pub(crate) logging: Logging,
    pub(crate) dbs: Vec<DbConfig>,
    pub(crate) rust: RustSettings,
}

impl GoConfig {
    /// The config narrowed to the one `path` entry for `path`, as Go's `Config.DBConfig`
    /// finds it for its single-database commands (`restore`, `ltx`): `dir` entries are not
    /// scanned and other entries are not built. `None` when no `path` entry names it.
    #[must_use]
    pub fn only_database(&self, path: &Path) -> Option<Self> {
        let db = self
            .dbs
            .iter()
            .find(|db| matches!(db.source(), DbSource::Path(p) if p == path))?;
        Some(Self {
            dbs: vec![db.clone()],
            ..self.clone()
        })
    }

    /// The config with only the entry at `index`, for a command that plans one `dbs` entry (Go's
    /// `newDBsFromConfig`, which builds a `dir` entry's databases and no other entry's). `None`
    /// when there is no such entry.
    #[must_use]
    pub fn only_entry(&self, index: usize) -> Option<Self> {
        let db = self.dbs.get(index)?;
        Some(Self {
            dbs: vec![db.clone()],
            ..self.clone()
        })
    }

    /// `addr`: the metrics listener; `None` serves none.
    #[must_use]
    pub fn addr(&self) -> Option<&str> {
        self.addr.as_deref()
    }
    /// `exec`: the child command line, unsplit.
    #[must_use]
    pub fn exec(&self) -> Option<&str> {
        self.exec.as_deref()
    }
    /// `levels`: L1 onwards. Empty when the file has `levels:` with nothing, as in Go.
    #[must_use]
    pub fn levels(&self) -> &[GoDuration] {
        &self.levels
    }
    /// `snapshot`, after Go promotes a database-level setting when no global one is set.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.snapshot
    }
    /// `l0-retention`; zero keeps no L0 files past compaction.
    #[must_use]
    pub fn l0_retention(&self) -> GoDuration {
        self.l0_retention
    }
    /// `l0-retention-check-interval`: `None` when the file leaves it unset (Go's default is
    /// `defaults::L0_RETENTION_CHECK_INTERVAL`).
    #[must_use]
    pub fn l0_retention_check_interval(&self) -> Option<GoDuration> {
        self.l0_retention_check_interval
    }
    /// `retention.enabled`: whether litestream-rs deletes what retention expires (Go's default, true).
    #[must_use]
    pub fn retention_enabled(&self) -> bool {
        self.retention_enabled
    }
    /// `shutdown-sync-timeout`: the one deadline for every database's final sync.
    #[must_use]
    pub fn shutdown_sync_timeout(&self) -> GoDuration {
        self.shutdown_sync_timeout
    }
    /// `validation.interval`: how often the scheduled verify runs. `None` when unset, as Go
    /// leaves it; Go runs no validation at zero or less either.
    #[must_use]
    pub fn validation_interval(&self) -> Option<GoDuration> {
        self.validation_interval
    }
    /// `logging`, with `LOG_LEVEL` applied.
    #[must_use]
    pub fn logging(&self) -> Logging {
        self.logging
    }
    /// `dbs`, in file order.
    #[must_use]
    pub fn dbs(&self) -> &[DbConfig] {
        &self.dbs
    }
    /// The `litestream-rs` section.
    #[must_use]
    pub fn rust(&self) -> RustSettings {
        self.rust
    }
}

/// Snapshot interval and retention.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub(crate) interval: GoDuration,
    pub(crate) retention: GoDuration,
}

impl Snapshot {
    /// How often a snapshot is taken.
    #[must_use]
    pub fn interval(self) -> GoDuration {
        self.interval
    }
    /// How long snapshots are kept.
    #[must_use]
    pub fn retention(self) -> GoDuration {
        self.retention
    }
}

/// A log level; Go's names, any case, `warning` as `warn`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    /// `trace`.
    Trace,
    /// `debug`.
    Debug,
    /// `info` (the default).
    Info,
    /// `warn` or `warning`.
    Warn,
    /// `error`.
    Error,
}

/// A log line format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    /// logfmt (Go's `text`, the default).
    Text,
    /// One JSON object per line.
    Json,
}

/// The `logging` section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Logging {
    pub(crate) level: LogLevel,
    pub(crate) format: LogFormat,
    pub(crate) stderr: bool,
}

impl Logging {
    /// The level, `LOG_LEVEL` first as in Go.
    #[must_use]
    pub fn level(self) -> LogLevel {
        self.level
    }
    /// The format.
    #[must_use]
    pub fn format(self) -> LogFormat {
        self.format
    }
    /// Whether logs go to stderr rather than stdout.
    #[must_use]
    pub fn stderr(self) -> bool {
        self.stderr
    }
}

/// Where a `dbs` entry's databases are.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbSource {
    /// One database; the path is expanded (`~`, `sqlite://`) and absolute, as Go's parse leaves it.
    Path(PathBuf),
    /// Every database a directory scan finds, once, at start.
    Dir(DirSource),
}

/// A `dir` entry, as written (Go expands its paths when it scans).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirSource {
    pub(crate) dir: String,
    pub(crate) pattern: String,
    pub(crate) recursive: bool,
    pub(crate) meta_dir: Option<String>,
}

impl DirSource {
    /// `dir`.
    #[must_use]
    pub fn dir(&self) -> &str {
        &self.dir
    }
    /// `pattern`, a `filepath.Match` glob on file names.
    #[must_use]
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
    /// `recursive`.
    #[must_use]
    pub fn recursive(&self) -> bool {
        self.recursive
    }
    /// `meta-dir`.
    #[must_use]
    pub fn meta_dir(&self) -> Option<&str> {
        self.meta_dir.as_deref()
    }
}

/// Database-level snapshot settings, before Go promotes them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotOverride {
    pub(crate) interval: Option<GoDuration>,
    pub(crate) retention: Option<GoDuration>,
}

impl SnapshotOverride {
    /// `snapshot.interval` on the entry.
    #[must_use]
    pub fn interval(self) -> Option<GoDuration> {
        self.interval
    }
    /// `snapshot.retention` on the entry.
    #[must_use]
    pub fn retention(self) -> Option<GoDuration> {
        self.retention
    }
}

/// One `dbs` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DbConfig {
    pub(crate) source: DbSource,
    pub(crate) meta_path: Option<String>,
    pub(crate) snapshot: SnapshotOverride,
    pub(crate) monitor_interval: Option<GoDuration>,
    pub(crate) checkpoint_interval: Option<GoDuration>,
    pub(crate) min_checkpoint_page_count: Option<i64>,
    pub(crate) truncate_page_n: Option<i64>,
    pub(crate) max_sync_wal_bytes: Option<i64>,
    pub(crate) restore_if_db_not_exists: bool,
    pub(crate) replica: Option<ReplicaConfig>,
}

impl DbConfig {
    /// `path` or `dir`.
    #[must_use]
    pub fn source(&self) -> &DbSource {
        &self.source
    }
    /// The database path, for a `path` entry.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        match &self.source {
            DbSource::Path(path) => Some(path),
            DbSource::Dir(_) => None,
        }
    }
    /// `meta-path`, as written.
    #[must_use]
    pub fn meta_path(&self) -> Option<&str> {
        self.meta_path.as_deref()
    }
    /// The entry's own `snapshot`.
    #[must_use]
    pub fn snapshot(&self) -> SnapshotOverride {
        self.snapshot
    }
    /// `monitor-interval` (Go's default 1s).
    #[must_use]
    pub fn monitor_interval(&self) -> Option<GoDuration> {
        self.monitor_interval
    }
    /// `checkpoint-interval` (Go's default 1m).
    #[must_use]
    pub fn checkpoint_interval(&self) -> Option<GoDuration> {
        self.checkpoint_interval
    }
    /// `min-checkpoint-page-count` (Go's default 1000).
    #[must_use]
    pub fn min_checkpoint_page_count(&self) -> Option<i64> {
        self.min_checkpoint_page_count
    }
    /// `truncate-page-n`; unset for Go's zero, which is Go's default (121359).
    #[must_use]
    pub fn truncate_page_n(&self) -> Option<i64> {
        self.truncate_page_n
    }
    /// `max-sync-wal-bytes` (Go's default 64 MiB).
    #[must_use]
    pub fn max_sync_wal_bytes(&self) -> Option<i64> {
        self.max_sync_wal_bytes
    }
    /// `restore-if-db-not-exists`.
    #[must_use]
    pub fn restore_if_db_not_exists(&self) -> bool {
        self.restore_if_db_not_exists
    }
    /// `replica`, or the one item of the deprecated `replicas`; `None` when
    /// neither is given (Go refuses that when it starts the database).
    #[must_use]
    pub fn replica(&self) -> Option<&ReplicaConfig> {
        self.replica.as_ref()
    }
}

/// The two replica types litestream-rs has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaKind {
    /// An S3-compatible bucket.
    S3,
    /// A local directory.
    File,
}

impl ReplicaKind {
    /// Go's `ReplicaClient.Type()` word, which its JSON output and the `type` key use.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::S3 => "s3",
        }
    }
}

/// A replica, top-level settings copied in where it does not set them (Go's `SetDefaults`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaConfig {
    pub(crate) kind: ReplicaKind,
    pub(crate) url: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) sync_interval: Option<GoDuration>,
    pub(crate) max_sync_ltx_files: Option<i64>,
    pub(crate) s3: S3Settings,
}

impl ReplicaConfig {
    /// From the URL's scheme, else `type`, else `file`, as Go's `ReplicaType`.
    #[must_use]
    pub fn kind(&self) -> ReplicaKind {
        self.kind
    }
    /// `url`.
    #[must_use]
    pub fn url(&self) -> Option<&str> {
        self.url.as_deref()
    }
    /// `path`.
    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
    /// `sync-interval` (Go's default 1s).
    #[must_use]
    pub fn sync_interval(&self) -> Option<GoDuration> {
        self.sync_interval
    }
    /// `max-sync-ltx-files` (Go's default 256; zero is all).
    #[must_use]
    pub fn max_sync_ltx_files(&self) -> Option<i64> {
        self.max_sync_ltx_files
    }
    /// The S3 keys.
    #[must_use]
    pub fn s3(&self) -> &S3Settings {
        &self.s3
    }
}

/// A secret read from the file; never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }
    /// The value, for handing to the store's credentials.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

/// The S3 keys litestream-rs implements.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct S3Settings {
    pub(crate) access_key_id: Option<Secret>,
    pub(crate) secret_access_key: Option<Secret>,
    pub(crate) region: Option<String>,
    pub(crate) bucket: Option<String>,
    pub(crate) endpoint: Option<String>,
    pub(crate) force_path_style: Option<bool>,
    pub(crate) part_size: Option<u64>,
    pub(crate) concurrency: Option<u64>,
}

impl S3Settings {
    /// `access-key-id`.
    #[must_use]
    pub fn access_key_id(&self) -> Option<&Secret> {
        self.access_key_id.as_ref()
    }
    /// `secret-access-key`.
    #[must_use]
    pub fn secret_access_key(&self) -> Option<&Secret> {
        self.secret_access_key.as_ref()
    }
    /// `region`.
    #[must_use]
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }
    /// `bucket`.
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }
    /// `endpoint`.
    #[must_use]
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }
    /// `force-path-style`.
    #[must_use]
    pub fn force_path_style(&self) -> Option<bool> {
        self.force_path_style
    }
    /// `part-size`, in bytes.
    #[must_use]
    pub fn part_size(&self) -> Option<u64> {
        self.part_size
    }
    /// `concurrency`: multipart parts in flight, as in Go. Zero or less is
    /// unset, as Go then keeps its default.
    #[must_use]
    pub fn concurrency(&self) -> Option<u64> {
        self.concurrency
    }
}

/// The `litestream-rs` section, which Go ignores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RustSettings {
    pub(crate) app_autocheckpoint: Option<std::num::NonZeroU32>,
    pub(crate) responses: lsr_core::Responses,
    pub(crate) if_replication_stops: lsr_replica::IfReplicationStops,
    pub(crate) max_unreplicated_age: lsr_replica::MaxUnreplicatedAge,
    pub(crate) snapshot_read_limit: Option<std::num::NonZeroU64>,
}

impl RustSettings {
    /// `app-autocheckpoint`: the `wal_autocheckpoint` the application's connections run, in
    /// frames; `None` for `0` or `off`. SQLite's default, 1000, when the key is absent, since
    /// the replicator cannot read another process's setting.
    #[must_use]
    pub fn app_autocheckpoint(self) -> Option<std::num::NonZeroU32> {
        self.app_autocheckpoint
    }
    /// `responses`.
    #[must_use]
    pub fn responses(self) -> lsr_core::Responses {
        self.responses
    }
    /// `if-replication-stops`: `report`, `hold-start` (the default) or `stop-app`.
    #[must_use]
    pub fn if_replication_stops(self) -> lsr_replica::IfReplicationStops {
        self.if_replication_stops
    }
    /// `max-unreplicated-age`: 10 minutes when absent, at least 5.
    #[must_use]
    pub fn max_unreplicated_age(self) -> lsr_replica::MaxUnreplicatedAge {
        self.max_unreplicated_age
    }
    /// `snapshot-read-limit`: the most bytes a second the engine reads from the database file
    /// for a whole-database read (the boot copy, a level-9 snapshot, the scheduled verify), as a
    /// byte size (`30MB`); `None` when absent, as in Go. Zero is refused.
    #[must_use]
    pub fn snapshot_read_limit(self) -> Option<std::num::NonZeroU64> {
        self.snapshot_read_limit
    }
}
