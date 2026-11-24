//! From a loaded [`GoConfig`] to what the daemon starts: one [`DbPlan`] per
//! database, each with the replicator's [`Config`] and its store, as Go 041a8c3
//! builds a `DB` and its `Replica` (`cmd/litestream/main.go`, `NewDBFromConfig`,
//! `NewReplicaFromConfig`).
//!
//! Every setting carries Go's value, default included, rather than the
//! library's own default, so the replicator does what Go would:
//! - `meta-path`, else Go's `.<name>-litestream` beside the database (`db.go:342`),
//!   always written into [`Config::meta_dir`], so a Go meta directory is the one used.
//! - `monitor-interval` (Go 1 s) is the WAL poll. Go's zero turns its monitor off
//!   and nothing is ever synced; litestream-rs refuses it rather than replicate nothing.
//! - `sync-interval` (Go 1 s) is the first retry wait, doubling to 5 minutes, as
//!   Go's replica backoff (`replica.go:449-456`). Go also uploads only once per
//!   sync interval; litestream-rs uploads on the poll that staged the file.
//! - `checkpoint-interval` (Go 1 m; zero or less is off, `db.go:1571`),
//!   `min-checkpoint-page-count` (Go 1000; zero or less Go refuses at open,
//!   `db.go:872`), `truncate-page-n` (only with `app-readers: none`; Go's zero is
//!   121359 and less than zero is off, `db.go:1599-1608`).
//! - `max-sync-wal-bytes` (Go 64 MiB; zero or less is no cap, `wal_reader.go:241`)
//!   and `max-sync-ltx-files` (Go 256; zero or less is all, `replica.go:208`).
//! - `levels`, `snapshot`, `l0-retention` (zero keeps level 0),
//!   `l0-retention-check-interval` (Go 15 s; zero or less Go refuses at load) and
//!   `retention.enabled` become core's `Maintenance`, which refuses some shapes Go
//!   loads (levels not increasing, snapshots not above the top level, retention
//!   below the interval); each refusal names the setting. The check interval is
//!   how long an upload waits for the next maintenance listing when the next
//!   compaction window is further away, as Go's level-0 retention ticker.
//! - `shutdown-sync-timeout` (Go 30 s) is each replicator's drain grace, and the
//!   daemon's one shutdown deadline.
//! - `validation.interval` (Go unset: off) is the scheduled verify's interval for every
//!   database; zero or less is off, as Go starts its monitor only above zero (`store.go:268`).
//! - `litestream-rs.responses` over `Responses::GO_LIKE`, and
//!   `litestream-rs.app-autocheckpoint` (SQLite's 1000 unless stated; `0` or `off` for a
//!   application at `wal_autocheckpoint = 0`), which only lets health infer application checkpoint
//!   attempts the wal-index cannot show.
//!
//! A `dir` entry is scanned once, here (`dir.rs`, a static scan at start, as Go derives the
//! paths): every database it finds gets Go's replica path and meta
//! path for its relative path. The scan reads each candidate's 16-byte header,
//! before any replicator opens it.
//!
//! Then the whole set is checked: two entries naming one database, two databases
//! on one replica (bucket and prefix, or directory), or two on one meta directory
//! are refused. Go keeps the first of a duplicate database and lets the rest share
//! a replica, which is another writer on one history.

mod dir;
mod pattern;
mod replica;
mod s3;

use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::{
    CheckpointConfig, ConfigError as MaintenanceError, Interval, L0Retention, Maintenance,
    OtherWriter, Response, Responses, RetentionMode,
};
use lsr_replica::{Config, NonZeroDuration, ReaderTracking, SyncLimits};
use lsr_store_s3::{S3Config, S3Credentials};

use crate::config::{
    AppReaders, DbConfig, DbSource, GoConfig, OtherWriterSetting, ResponseSetting, ResponseSettings,
};
use crate::duration::GoDuration;
use crate::error::KeyPath;
use crate::expand::{Env, PathError, expand_path};

/// Go's `DefaultMonitorInterval`, `DefaultCheckpointInterval` and the rest (`db.go:32-38`,
/// `replica.go:26-27`).
mod go {
    use std::time::Duration;

    pub(super) const MONITOR_INTERVAL: Duration = Duration::from_secs(1);
    pub(super) const SYNC_INTERVAL: Duration = Duration::from_secs(1);
    pub(super) const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(60);
    pub(super) const MIN_CHECKPOINT_PAGES: u32 = 1000;
    pub(super) const TRUNCATE_PAGES: u32 = 121_359;
    pub(super) const MAX_SYNC_WAL_BYTES: u64 = 64 << 20;
    pub(super) const MAX_SYNC_LTX_FILES: u32 = 256;
}

/// Everything the daemon starts, in config order.
#[derive(Clone, Debug)]
pub struct Plan {
    dbs: Vec<DbPlan>,
    shutdown_sync_timeout: Duration,
    notes: Vec<Note>,
}

impl Plan {
    /// One entry per database, `dir` entries expanded in Go's walk order.
    #[must_use]
    pub fn dbs(&self) -> &[DbPlan] {
        &self.dbs
    }

    /// The one deadline every database's final sync shares.
    #[must_use]
    pub fn shutdown_sync_timeout(&self) -> Duration {
        self.shutdown_sync_timeout
    }

    /// Settings accepted with no effect, to log once at start.
    #[must_use]
    pub fn notes(&self) -> &[Note] {
        &self.notes
    }
}

/// One database.
#[derive(Clone, Debug)]
pub struct DbPlan {
    entry: KeyPath,
    path: PathBuf,
    config: Config,
    replica: ReplicaPlan,
    restore_if_missing: bool,
    from_dir: bool,
}

impl DbPlan {
    /// The `dbs` entry it came from (`dbs[2]`).
    #[must_use]
    pub fn entry(&self) -> &KeyPath {
        &self.entry
    }
    /// The database file, absolute.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// The replicator's configuration; its `meta_dir` is always set.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }
    /// The meta directory.
    #[must_use]
    pub fn meta_dir(&self) -> &Path {
        self.config.meta_dir.as_deref().unwrap_or(Path::new(""))
    }
    /// Where it replicates to.
    #[must_use]
    pub fn replica(&self) -> &ReplicaPlan {
        &self.replica
    }
    /// `restore-if-db-not-exists` (only on a `path` entry, as Go).
    #[must_use]
    pub fn restore_if_missing(&self) -> bool {
        self.restore_if_missing
    }
    /// Whether a `dir` scan found it (Go restores only `path` entries, even under the flag).
    #[must_use]
    pub fn from_dir(&self) -> bool {
        self.from_dir
    }
}

/// A replica's store.
#[derive(Clone, Debug)]
pub enum ReplicaPlan {
    /// A directory, absolute.
    File(PathBuf),
    /// A bucket prefix.
    S3 {
        /// Where.
        config: Box<S3Config>,
        /// With what.
        credentials: Credentials,
    },
}

impl ReplicaPlan {
    /// Go's `ReplicaClient.Type()`.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::File(_) => "file",
            Self::S3 { .. } => "s3",
        }
    }
}

/// Where an S3 replica's credentials come from.
#[derive(Clone, Debug)]
pub enum Credentials {
    /// `access-key-id` and `secret-access-key` from the file.
    FromConfig(S3Credentials),
    /// The AWS environment variables (`LITESTREAM_*` after `AWS_*`, as Go copies them) and
    /// shared files, resolved when the store opens.
    FromEnvironment,
}

/// A setting accepted that changes nothing, said once at start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Note {
    /// Where.
    pub at: KeyPath,
    /// What and why.
    pub text: &'static str,
}

impl fmt::Display for Note {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.at, self.text)
    }
}

/// Why a config cannot be started: every problem found, never empty.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}", first(.0))]
pub struct MapError(Vec<MapProblem>);

fn first(problems: &[MapProblem]) -> String {
    match problems {
        [] => "config cannot be started".to_owned(),
        [only] => only.to_string(),
        [one, rest @ ..] => format!("{one} (and {} more)", rest.len()),
    }
}

impl MapError {
    /// Every problem.
    #[must_use]
    pub fn problems(&self) -> &[MapProblem] {
        &self.0
    }
}

/// One problem, at the key that caused it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{at}: {kind}")]
pub struct MapProblem {
    /// The key, or the `dbs` entry.
    pub at: KeyPath,
    /// What.
    pub kind: MapProblemKind,
}

/// What is wrong. No variant holds a credential.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MapProblemKind {
    /// `monitor-interval` of zero or less: Go would never sync this database.
    #[error(
        "{value} would never poll the database (Go turns its monitor off); give a positive interval"
    )]
    MonitorOff {
        /// The value.
        value: GoDuration,
    },
    /// `min-checkpoint-page-count` of zero or less (Go refuses it when the database opens).
    #[error("minimum checkpoint page count must be above zero, got {value}")]
    MinCheckpointPages {
        /// The value.
        value: i64,
    },
    /// A duration litestream-rs cannot represent (negative where it must not be).
    #[error("{value} is not a usable duration here")]
    Duration {
        /// The value.
        value: GoDuration,
    },
    /// The maintenance schedule core refuses.
    #[error("compaction and snapshot schedule: {0}")]
    Maintenance(MaintenanceError),
    /// Neither `replica` nor `replicas` (Go: "must specify replica for database").
    #[error("must specify replica for database")]
    NoReplica,
    /// A path that cannot be made absolute.
    #[error("{0}")]
    Path(PathError),
    /// The replica is refused; see the reason.
    #[error("{0}")]
    Replica(replica::ReplicaProblem),
    /// A `dir` scan failed.
    #[error("{0}")]
    Dir(dir::DirProblem),
    /// Two entries name one database.
    #[error("{} is already replicated by {first}", .path.display())]
    DuplicateDatabase {
        /// The database.
        path: PathBuf,
        /// The entry that named it first.
        first: KeyPath,
    },
    /// Two databases on one replica: another writer on one history.
    #[error("{} replicates to the same place as {first}: {replica}", .path.display())]
    SharedReplica {
        /// The second database.
        path: PathBuf,
        /// The replica, without credentials.
        replica: String,
        /// The entry of the first.
        first: KeyPath,
    },
    /// Two databases on one meta directory (Go refuses this within a `dir` entry).
    #[error("{} shares meta directory {} with {first}", .path.display(), .meta.display())]
    SharedMetaDir {
        /// The second database.
        path: PathBuf,
        /// The directory.
        meta: PathBuf,
        /// The entry of the first.
        first: KeyPath,
    },
}

pub use dir::DirProblem;
pub use replica::ReplicaProblem;
pub use s3::{S3Overrides, S3Url, s3_config};

/// Collects problems and notes during one mapping.
#[derive(Default)]
struct Found {
    problems: Vec<MapProblem>,
    notes: Vec<Note>,
}

impl Found {
    fn problem(&mut self, at: &KeyPath, kind: MapProblemKind) {
        self.problems.push(MapProblem {
            at: at.clone(),
            kind,
        });
    }
}

/// Maps a loaded config onto replicators and stores, scanning `dir` entries once.
///
/// # Errors
/// [`MapError`] with every problem found; nothing has been opened or started.
pub fn plan(config: &GoConfig, env: &dyn Env) -> Result<Plan, MapError> {
    let mut found = Found::default();
    let maintenance = maintenance(config, &mut found);
    let mut dbs = Vec::new();
    for (index, db) in config.dbs().iter().enumerate() {
        let entry = KeyPath::root().key("dbs").index(index);
        let base = settings(config, db, &entry, maintenance.as_ref(), &mut found);
        if db.replica().is_none() {
            found.problem(&entry, MapProblemKind::NoReplica);
        }
        match db.source() {
            DbSource::Path(path) => {
                let meta = path_meta(&entry, db, path, env, &mut found);
                let replica = db
                    .replica()
                    .and_then(|r| replica::map(&entry.key("replica"), r, env, &mut found));
                dbs.extend(one(&entry, path.clone(), meta, replica, db, base.as_ref()));
            }
            DbSource::Dir(source) => {
                for scanned in dir::scan(&entry, source, db, env, &mut found) {
                    let dir::Found {
                        path,
                        meta,
                        replica,
                    } = scanned;
                    dbs.extend(one(&entry, path, meta, replica, db, base.as_ref()));
                }
            }
        }
    }
    distinct(&dbs, &mut found);
    if found.problems.is_empty() {
        Ok(Plan {
            dbs,
            shutdown_sync_timeout: config.shutdown_sync_timeout().to_std().unwrap_or_default(),
            notes: found.notes,
        })
    } else {
        Err(MapError(found.problems))
    }
}

/// A `path` entry's meta directory: `meta-path` expanded, else Go's default.
fn path_meta(
    entry: &KeyPath,
    db: &DbConfig,
    path: &Path,
    env: &dyn Env,
    found: &mut Found,
) -> Option<PathBuf> {
    let Some(written) = db.meta_path() else {
        return default_meta(path);
    };
    match expand_path(written, env) {
        Ok(expanded) => Some(expanded),
        Err(error) => {
            found.problem(&entry.key("meta-path"), MapProblemKind::Path(error));
            None
        }
    }
}

/// One database's plan, once its meta directory and replica are known.
fn one(
    entry: &KeyPath,
    path: PathBuf,
    meta: Option<PathBuf>,
    replica: Option<ReplicaPlan>,
    db: &DbConfig,
    base: Option<&Config>,
) -> Option<DbPlan> {
    let mut config = base?.clone();
    config.meta_dir = Some(meta?);
    Some(DbPlan {
        entry: entry.clone(),
        path,
        config,
        replica: replica?,
        restore_if_missing: db.restore_if_db_not_exists()
            && matches!(db.source(), DbSource::Path(_)),
        from_dir: matches!(db.source(), DbSource::Dir(_)),
    })
}

/// Go's `.<name>-litestream` beside the database (`db.go:342`, `litestream.go:20`).
#[must_use]
pub fn default_meta(database: &Path) -> Option<PathBuf> {
    let name = database.file_name()?.to_str()?;
    let parent = database.parent().unwrap_or_else(|| Path::new(""));
    Some(parent.join(format!(".{name}-litestream")))
}

/// The replicator settings an entry gives every database it names.
fn settings(
    config: &GoConfig,
    db: &DbConfig,
    entry: &KeyPath,
    maintenance: Option<&Maintenance>,
    found: &mut Found,
) -> Option<Config> {
    let poll = poll_interval(db, entry, found);
    let retry = retry_interval(db, entry, found);
    let checkpoint = checkpoint(config, db, entry, found);
    let sync_limits = sync_limits(db);
    let shutdown_grace = config.shutdown_sync_timeout().to_std().unwrap_or_default();
    Some(Config {
        meta_dir: None,
        poll_interval: poll?,
        retry_interval: retry?,
        checkpoint: checkpoint?,
        maintenance: maintenance?.clone(),
        sync_limits: sync_limits?,
        responses: responses(config.rust().responses()),
        readers: match config.rust().app_readers() {
            AppReaders::Unknown => ReaderTracking::Untracked,
            AppReaders::None => ReaderTracking::Tracked,
        },
        shutdown_grace,
        app_autocheckpoint: config.rust().app_autocheckpoint(),
        verify_interval: config.validation_interval().and_then(positive),
        if_replication_stops: config.rust().if_replication_stops(),
        max_unreplicated_age: config.rust().max_unreplicated_age(),
    })
}

fn positive(value: GoDuration) -> Option<NonZeroDuration> {
    value.to_std().and_then(NonZeroDuration::new)
}

/// `monitor-interval`: Go's 1 s; zero or less is refused (Go never syncs).
fn poll_interval(db: &DbConfig, entry: &KeyPath, found: &mut Found) -> Option<NonZeroDuration> {
    match db.monitor_interval() {
        None => NonZeroDuration::new(go::MONITOR_INTERVAL),
        Some(value) => positive(value).or_else(|| {
            found.problem(
                &entry.key("monitor-interval"),
                MapProblemKind::MonitorOff { value },
            );
            None
        }),
    }
}

/// `sync-interval`: Go's 1 s, the first backoff wait; the load already refused zero or less.
fn retry_interval(db: &DbConfig, entry: &KeyPath, found: &mut Found) -> Option<NonZeroDuration> {
    match db.replica().and_then(|r| r.sync_interval()) {
        None => NonZeroDuration::new(go::SYNC_INTERVAL),
        Some(value) => positive(value).or_else(|| {
            let at = entry.key("replica").key("sync-interval");
            found.problem(&at, MapProblemKind::Duration { value });
            None
        }),
    }
}

fn checkpoint(
    config: &GoConfig,
    db: &DbConfig,
    entry: &KeyPath,
    found: &mut Found,
) -> Option<CheckpointConfig> {
    let interval = match db.checkpoint_interval() {
        None => Some(go::CHECKPOINT_INTERVAL),
        Some(value) => value.to_std().filter(|d| !d.is_zero()),
    };
    let min_pages = match db.min_checkpoint_page_count() {
        None => NonZeroU32::new(go::MIN_CHECKPOINT_PAGES),
        Some(value) if value <= 0 => {
            let at = entry.key("min-checkpoint-page-count");
            found.problem(&at, MapProblemKind::MinCheckpointPages { value });
            return None;
        }
        Some(value) => count(value),
    };
    let truncate_pages = match (config.rust().app_readers(), db.truncate_page_n()) {
        (AppReaders::Unknown, _) => None,
        (AppReaders::None, None) => NonZeroU32::new(go::TRUNCATE_PAGES),
        (AppReaders::None, Some(value)) if value < 0 => None,
        (AppReaders::None, Some(value)) => count(value),
    };
    Some(CheckpointConfig {
        interval,
        min_pages,
        truncate_pages,
    })
}

/// A positive count as a `NonZeroU32`, saturating. Go holds these in an `int`; every
/// count they bound (WAL frames, database pages, files per upload) is itself at most
/// `u32::MAX`, so a larger threshold behaves exactly as `u32::MAX` does.
fn count(value: i64) -> Option<NonZeroU32> {
    NonZeroU32::new(u32::try_from(value).unwrap_or(u32::MAX))
}

fn sync_limits(db: &DbConfig) -> Option<SyncLimits> {
    let wal_bytes_per_step = match db.max_sync_wal_bytes() {
        None => NonZeroU64::new(go::MAX_SYNC_WAL_BYTES),
        Some(value) => u64::try_from(value).ok().and_then(NonZeroU64::new),
    };
    let files_per_upload = match db.replica().and_then(|r| r.max_sync_ltx_files()) {
        None => NonZeroU32::new(go::MAX_SYNC_LTX_FILES),
        Some(value) if value <= 0 => None,
        Some(value) => count(value),
    };
    Some(SyncLimits {
        wal_bytes_per_step,
        files_per_upload,
    })
}

/// Go's schedule as core's `Maintenance`, which refuses some shapes Go accepts.
fn maintenance(config: &GoConfig, found: &mut Found) -> Option<Maintenance> {
    let duration = |value: GoDuration, at: &str, found: &mut Found| {
        value.to_std().or_else(|| {
            found.problem(&KeyPath::root().key(at), MapProblemKind::Duration { value });
            None
        })
    };
    let mut levels = Vec::new();
    for level in config.levels() {
        levels.push(duration(*level, "levels", found)?);
    }
    let snapshot = config.snapshot();
    let interval = duration(snapshot.interval(), "snapshot", found)?;
    let retention = duration(snapshot.retention(), "snapshot", found)?;
    let l0 = duration(config.l0_retention(), "l0-retention", found)?;
    let l0 = if l0.is_zero() {
        L0Retention::Keep
    } else {
        L0Retention::After(l0)
    };
    let check = l0_check(config.l0_retention_check_interval(), found)?;
    match Maintenance::new(levels, interval, retention, l0) {
        Ok(schedule) => {
            let schedule = if config.l0_retention_check_interval_is_set() {
                schedule.with_l0_check_interval(check)
            } else {
                schedule
            };
            Some(schedule.with_retention(if config.retention_enabled() {
                RetentionMode::Enforce
            } else {
                RetentionMode::Off
            }))
        }
        Err(error) => {
            found.problem(&KeyPath::root(), MapProblemKind::Maintenance(error));
            None
        }
    }
}

/// `l0-retention-check-interval` as core's interval: whole milliseconds above
/// zero (load already refused zero and less, as Go's `Validate` does).
fn l0_check(value: GoDuration, found: &mut Found) -> Option<Interval> {
    let every = value.to_std().and_then(|d| Interval::new(d).ok());
    if every.is_none() {
        let at = KeyPath::root().key("l0-retention-check-interval");
        found.problem(&at, MapProblemKind::Duration { value });
    }
    every
}

/// `litestream-rs.responses` over Go's.
fn responses(settings: ResponseSettings) -> Responses {
    let base = Responses::GO_LIKE;
    let one = |setting: Option<ResponseSetting>, default: Response| match setting {
        None => default,
        Some(ResponseSetting::KeepTrying) => Response::KeepTrying,
        Some(ResponseSetting::Stop) => Response::Stop,
    };
    Responses {
        credentials: one(settings.credentials, base.credentials),
        permission_denied: one(settings.permission_denied, base.permission_denied),
        clock_skew: one(settings.clock_skew, base.clock_skew),
        bucket_missing: one(settings.bucket_missing, base.bucket_missing),
        tls: one(settings.tls, base.tls),
        store_refused: one(settings.store_refused, base.store_refused),
        remote_corrupt: one(settings.remote_corrupt, base.remote_corrupt),
        remote_regressed: one(settings.remote_regressed, base.remote_regressed),
        wal_corrupt: one(settings.wal_corrupt, base.wal_corrupt),
        wal_truncated: one(settings.wal_truncated, base.wal_truncated),
        wal_unlinked: one(settings.wal_unlinked, base.wal_unlinked),
        wal_sync_failed: one(settings.wal_sync_failed, base.wal_sync_failed),
        page_size_changed: one(settings.page_size_changed, base.page_size_changed),
        database_page_mismatch: base.database_page_mismatch,
        conditional_writes_unsupported: one(
            settings.conditional_writes_unsupported,
            base.conditional_writes_unsupported,
        ),
        bug: one(settings.bug, base.bug),
        other_writer: match settings.other_writer {
            None => base.other_writer,
            Some(OtherWriterSetting::Stop) => OtherWriter::Stop,
            Some(OtherWriterSetting::ContinueAndInterleaveHistories) => {
                OtherWriter::ContinueAndInterleaveHistories
            }
        },
    }
}

/// Refuses two plans on one database, one replica or one meta directory.
fn distinct(dbs: &[DbPlan], found: &mut Found) {
    for (index, db) in dbs.iter().enumerate() {
        let earlier = dbs.iter().take(index);
        for before in earlier {
            if before.path == db.path {
                let first = before.entry.clone();
                let path = db.path.clone();
                found.problem(&db.entry, MapProblemKind::DuplicateDatabase { path, first });
                break;
            }
            if let Some(replica) = replica::same(&before.replica, &db.replica) {
                let first = before.entry.clone();
                let path = db.path.clone();
                let kind = MapProblemKind::SharedReplica {
                    path,
                    replica,
                    first,
                };
                found.problem(&db.entry, kind);
                break;
            }
            if before.config.meta_dir == db.config.meta_dir {
                let kind = MapProblemKind::SharedMetaDir {
                    path: db.path.clone(),
                    meta: db.meta_dir().to_owned(),
                    first: before.entry.clone(),
                };
                found.problem(&db.entry, kind);
                break;
            }
        }
    }
}
