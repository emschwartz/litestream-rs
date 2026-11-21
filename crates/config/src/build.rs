//! From the walked [`Fields`] to [`GoConfig`]: Go's `ParseConfig` after its
//! `yaml.Unmarshal`, in Go's order.
//!
//! 1. Defaults (`DefaultConfig`), restored where an empty section left them
//!    null, as Go restores them; `levels:` with nothing stays empty, as in Go.
//! 2. Database-level `snapshot` promoted to the global one when the file sets
//!    no global value, refusing entries that disagree (`applyDBSnapshotConfig`).
//! 3. Database paths expanded (`expand`), then top-level replica settings
//!    copied into each replica that does not set them (`SetDefaults`; Go does
//!    not copy `part-size`).
//! 4. `Validate`, every rule, collecting all failures rather than the first.
//!    Then what Go refuses only when it starts a database (`replica` with
//!    `replicas`, two replicas, a URL in `path`, an unknown replica type) is
//!    refused here too, before anything runs; and `truncate-page-n` without
//!    the `host-readers: none` claim.
//! 5. `LOG_LEVEL` over `logging.level`; the snapshot-retention warning.

use std::num::NonZeroU32;

use crate::config::{
    DbConfig, DbSource, DirSource, ForeignWriterSetting, GoConfig, HostReaders, LogFormat,
    LogLevel, Logging, ReplicaConfig, ReplicaKind, ResponseSetting, ResponseSettings, RustSettings,
    S3Settings, Secret, Snapshot, SnapshotOverride, defaults,
};
use crate::duration::GoDuration;
use crate::error::{KeyPath, ProblemKind, WarningKind};
use crate::expand::{Env, clean, expand_path};
use crate::walk::{Fields, Listed, Walk};

const MIN_HEARTBEAT: GoDuration = GoDuration::from_secs(60);

/// SQLite's default `wal_autocheckpoint`, in frames.
const SQLITE_AUTOCHECKPOINT: u32 = 1000;

pub(crate) fn build(top: &Fields, env: &dyn Env, walk: &mut Walk) -> GoConfig {
    let rust = rust_settings(top.map("litestream-rs"), walk);
    global_checks(top, walk);
    let levels = levels(top, walk);
    let mut dbs = Vec::new();
    let mut seen: Vec<(String, usize)> = Vec::new();
    if let Listed::Items(items) = top.maps("dbs") {
        for (index, fields) in items.iter().enumerate() {
            if let Some(db) = db(fields, index, top, rust, &mut seen, env, walk) {
                dbs.push((index, fields, db));
            }
        }
    }
    let global = top.map("snapshot");
    let promoted = promote(&dbs, walk);
    let snapshot = Snapshot {
        interval: global
            .and_then(|s| s.duration("interval"))
            .or(promoted.interval)
            .unwrap_or(defaults::SNAPSHOT_INTERVAL),
        retention: global
            .and_then(|s| s.duration("retention"))
            .or(promoted.retention)
            .unwrap_or(defaults::SNAPSHOT_RETENTION),
    };
    snapshot_checks(snapshot, &KeyPath::root().key("snapshot"), global, walk);
    if rust.host_readers != HostReaders::None && !dbs.is_empty() {
        walk.warn(KeyPath::root(), None, WarningKind::NoTruncateWithoutClaim);
    }
    GoConfig {
        addr: top.string("addr"),
        exec: top.string("exec"),
        levels,
        snapshot,
        l0_retention: top
            .duration("l0-retention")
            .unwrap_or(defaults::L0_RETENTION),
        l0_retention_check_interval: top
            .duration("l0-retention-check-interval")
            .unwrap_or(defaults::L0_RETENTION_CHECK_INTERVAL),
        retention_enabled: top
            .map("retention")
            .and_then(|r| r.boolean("enabled"))
            .unwrap_or(true),
        shutdown_sync_timeout: top
            .duration("shutdown-sync-timeout")
            .unwrap_or(defaults::SHUTDOWN_SYNC_TIMEOUT),
        validation_interval: top.map("validation").and_then(|v| v.duration("interval")),
        logging: logging(top.map("logging"), env, walk),
        dbs: dbs.into_iter().map(|(_, _, db)| db).collect(),
        rust,
    }
}

/// A duration that must be above zero, or at least zero.
fn positive(fields: &Fields, name: &str, walk: &mut Walk) {
    if let Some(value) = fields.duration(name).filter(|d| d.nanos() <= 0) {
        walk.problem(
            fields.path.key(name),
            fields.line(name),
            ProblemKind::NotPositive { value },
        );
    }
}

fn not_negative(fields: &Fields, name: &str, walk: &mut Walk) {
    if let Some(value) = fields.duration(name).filter(|d| d.nanos() < 0) {
        walk.problem(
            fields.path.key(name),
            fields.line(name),
            ProblemKind::Negative { value },
        );
    }
}

/// Go's `Validate` for the top-level durations.
fn global_checks(top: &Fields, walk: &mut Walk) {
    if let Some(snapshot) = top.map("snapshot") {
        positive(snapshot, "interval", walk);
        positive(snapshot, "retention", walk);
    }
    not_negative(top, "l0-retention", walk);
    positive(top, "l0-retention-check-interval", walk);
    not_negative(top, "shutdown-sync-timeout", walk);
    positive(top, "shutdown-sync-interval", walk);
    if let Some(value) = top
        .duration("heartbeat-interval")
        .filter(|d| *d < MIN_HEARTBEAT)
    {
        walk.problem(
            KeyPath::root().key("heartbeat-interval"),
            top.line("heartbeat-interval"),
            ProblemKind::HeartbeatIntervalTooShort { value },
        );
    }
}

fn levels(top: &Fields, walk: &mut Walk) -> Vec<GoDuration> {
    match top.maps("levels") {
        Listed::Absent => defaults::LEVELS.to_vec(),
        Listed::Null => Vec::new(),
        Listed::Items(items) => items
            .iter()
            .map(|level| {
                let interval = level.duration("interval").unwrap_or(GoDuration::ZERO);
                if interval.nanos() <= 0 {
                    let line = level.line("interval");
                    walk.problem(
                        level.path.key("interval"),
                        line,
                        ProblemKind::NotPositive { value: interval },
                    );
                }
                interval
            })
            .collect(),
    }
}

/// Go's warning when retention is shorter than the interval.
fn snapshot_checks(snapshot: Snapshot, path: &KeyPath, fields: Option<&Fields>, walk: &mut Walk) {
    if snapshot.retention < snapshot.interval {
        walk.warn(
            path.key("retention"),
            fields.and_then(|f| f.line("retention")),
            WarningKind::RetentionShorterThanInterval {
                retention: snapshot.retention,
                interval: snapshot.interval,
            },
        );
    }
}

/// Go's `applyDBSnapshotConfig`: the database-level values, which must agree.
fn promote(dbs: &[(usize, &Fields, DbConfig)], walk: &mut Walk) -> SnapshotOverride {
    let mut interval: Option<(usize, GoDuration)> = None;
    let mut retention: Option<(usize, GoDuration)> = None;
    for (index, fields, db) in dbs {
        for (what, name, own, slot) in [
            ("intervals", "interval", db.snapshot.interval, &mut interval),
            (
                "retentions",
                "retention",
                db.snapshot.retention,
                &mut retention,
            ),
        ] {
            let Some(value) = own else { continue };
            match slot {
                Some((first, first_value)) if *first_value != value => {
                    let snapshot = fields.map("snapshot");
                    walk.problem(
                        fields.path.key("snapshot").key(name),
                        snapshot.and_then(|s| s.line(name)),
                        ProblemKind::ConflictingSnapshot {
                            what,
                            first: *first,
                            first_value: *first_value,
                            value,
                        },
                    );
                }
                Some(_) => {}
                None => *slot = Some((*index, value)),
            }
        }
    }
    SnapshotOverride {
        interval: interval.map(|(_, d)| d),
        retention: retention.map(|(_, d)| d),
    }
}

fn db(
    fields: &Fields,
    index: usize,
    top: &Fields,
    rust: RustSettings,
    seen: &mut Vec<(String, usize)>,
    env: &dyn Env,
    walk: &mut Walk,
) -> Option<DbConfig> {
    let at = fields.path.clone();
    let line = fields.line;
    let source = source(fields, index, seen, env, walk)?;
    if fields.boolean("watch") == Some(true) && !matches!(source, DbSource::Dir(_)) {
        walk.problem(at.clone(), line, ProblemKind::WatchWithoutDir);
    }
    if fields.string("meta-path").is_some() && fields.string("meta-dir").is_some() {
        walk.problem(at.clone(), line, ProblemKind::MetaPathAndMetaDir);
    }
    let snapshot_fields = fields.map("snapshot");
    if let Some(snapshot) = snapshot_fields {
        positive(snapshot, "interval", walk);
        positive(snapshot, "retention", walk);
    }
    let snapshot = SnapshotOverride {
        interval: snapshot_fields.and_then(|s| s.duration("interval")),
        retention: snapshot_fields.and_then(|s| s.duration("retention")),
    };
    if let (Some(interval), Some(retention)) = (snapshot.interval, snapshot.retention) {
        snapshot_checks(
            Snapshot {
                interval,
                retention,
            },
            &at.key("snapshot"),
            snapshot_fields,
            walk,
        );
    }
    let truncate_page_n = fields.int("truncate-page-n").filter(|&n| n != 0);
    if truncate_page_n.is_some() && rust.host_readers != HostReaders::None {
        walk.problem(
            at.key("truncate-page-n"),
            fields.line("truncate-page-n"),
            ProblemKind::TruncateNeedsClaim,
        );
    }
    Some(DbConfig {
        source,
        meta_path: fields.string("meta-path"),
        snapshot,
        monitor_interval: fields.duration("monitor-interval"),
        checkpoint_interval: fields.duration("checkpoint-interval"),
        min_checkpoint_page_count: fields.int("min-checkpoint-page-count"),
        truncate_page_n,
        max_sync_wal_bytes: fields.int("max-sync-wal-bytes"),
        restore_if_db_not_exists: fields.boolean("restore-if-db-not-exists").unwrap_or(false),
        replica: db_replica(fields, top, walk),
    })
}

/// `path` or `dir`, checked as Go's `Validate` checks them.
fn source(
    fields: &Fields,
    index: usize,
    seen: &mut Vec<(String, usize)>,
    env: &dyn Env,
    walk: &mut Walk,
) -> Option<DbSource> {
    let (at, line) = (fields.path.clone(), fields.line);
    let meta_dir = fields.string("meta-dir");
    match (fields.string("path"), fields.string("dir")) {
        (Some(_), Some(_)) => {
            walk.problem(at, line, ProblemKind::PathAndDir);
            None
        }
        (None, None) => {
            walk.problem(at, line, ProblemKind::NeitherPathNorDir);
            None
        }
        (Some(path), None) => {
            if meta_dir.is_some() {
                walk.problem(at.clone(), line, ProblemKind::MetaDirWithoutDir);
            }
            let path_line = fields.line("path");
            let expanded = match expand_path(&path, env) {
                Ok(expanded) => expanded,
                Err(error) => {
                    walk.problem(at.key("path"), path_line, ProblemKind::Path(error));
                    return None;
                }
            };
            let key = clean(&expanded.to_string_lossy());
            match seen.iter().find(|(seen, _)| *seen == key) {
                Some(&(_, first)) => walk.problem(
                    at.key("path"),
                    path_line,
                    ProblemKind::DuplicatePath { first },
                ),
                None => seen.push((key, index)),
            }
            Some(DbSource::Path(expanded))
        }
        (None, Some(dir)) => {
            let Some(pattern) = fields.string("pattern") else {
                walk.problem(at, line, ProblemKind::PatternRequired);
                return None;
            };
            let recursive = fields.boolean("recursive").unwrap_or(false);
            Some(DbSource::Dir(DirSource {
                dir,
                pattern,
                recursive,
                meta_dir,
            }))
        }
    }
}

/// `replica`, or the one item of `replicas`.
fn db_replica(fields: &Fields, top: &Fields, walk: &mut Walk) -> Option<ReplicaConfig> {
    let single = fields.map("replica");
    let list = match fields.maps("replicas") {
        Listed::Items(items) => items,
        Listed::Absent | Listed::Null => &[],
    };
    for item in list {
        positive(item, "sync-interval", walk);
    }
    if let Some(single) = single {
        positive(single, "sync-interval", walk);
    }
    if single.is_some() && !list.is_empty() {
        walk.problem(
            fields.path.clone(),
            fields.line,
            ProblemKind::ReplicaAndReplicas,
        );
        return None;
    }
    if list.len() > 1 {
        walk.problem(
            fields.path.key("replicas"),
            fields.line("replicas"),
            ProblemKind::MultipleReplicas,
        );
        return None;
    }
    single
        .or(list.first())
        .and_then(|replica| build_replica(replica, top, walk))
}

/// Go's `^\w+://`.
fn is_url(text: &str) -> bool {
    text.split_once("://").is_some_and(|(scheme, _)| {
        !scheme.is_empty()
            && scheme
                .bytes()
                .all(|b| b == b'_' || b.is_ascii_alphanumeric())
    })
}

/// Go's `ReplicaType`: the URL's scheme, else `type`, else `file`.
fn replica_kind(fields: &Fields, walk: &mut Walk) -> Option<ReplicaKind> {
    let url = fields.string("url").filter(|u| is_url(u));
    let (name, key) = match (&url, fields.string("type")) {
        (Some(url), _) => {
            let scheme = url
                .split_once("://")
                .map_or("", |(s, _)| s)
                .to_ascii_lowercase();
            (
                if scheme == "webdavs" {
                    "webdav".to_owned()
                } else {
                    scheme
                },
                "url",
            )
        }
        (None, Some(kind)) => (kind, "type"),
        (None, None) => ("file".to_owned(), "type"),
    };
    let kind = match name.as_str() {
        "s3" => return Some(ReplicaKind::S3),
        "file" => return Some(ReplicaKind::File),
        "gs" | "abs" | "sftp" | "webdav" | "nats" | "oss" => {
            ProblemKind::UnsupportedReplicaType { name }
        }
        _ => ProblemKind::UnknownReplicaType { name },
    };
    walk.problem(fields.path.key(key), fields.line(key), kind);
    None
}

fn build_replica(fields: &Fields, top: &Fields, walk: &mut Walk) -> Option<ReplicaConfig> {
    let path = fields.string("path");
    if path.as_deref().is_some_and(is_url) {
        walk.problem(
            fields.path.key("path"),
            fields.line("path"),
            ProblemKind::ReplicaPathIsUrl,
        );
    }
    let kind = replica_kind(fields, walk)?;
    let secret = |name: &str| {
        fields
            .string(name)
            .or_else(|| top.string(name))
            .map(Secret::new)
    };
    let text = |name: &str| fields.string(name).or_else(|| top.string(name));
    Some(ReplicaConfig {
        kind,
        url: fields.string("url"),
        path,
        sync_interval: fields
            .duration("sync-interval")
            .or_else(|| top.duration("sync-interval")),
        max_sync_ltx_files: fields
            .int("max-sync-ltx-files")
            .or_else(|| top.int("max-sync-ltx-files")),
        s3: S3Settings {
            access_key_id: secret("access-key-id"),
            secret_access_key: secret("secret-access-key"),
            region: text("region"),
            bucket: text("bucket"),
            endpoint: text("endpoint"),
            force_path_style: fields
                .boolean("force-path-style")
                .or_else(|| top.boolean("force-path-style")),
            part_size: fields.bytes("part-size"),
        },
    })
}

/// `logging`, with `LOG_LEVEL` over `logging.level` as in Go.
fn logging(fields: Option<&Fields>, env: &dyn Env, walk: &mut Walk) -> Logging {
    let path = KeyPath::root().key("logging");
    let from_env = env.var("LOG_LEVEL").filter(|v| !v.is_empty());
    let written = fields.and_then(|f| f.string("level"));
    let level = match (&from_env, &written) {
        (Some(value), _) => parse_level(value).unwrap_or_else(|| {
            walk.problem(
                path.key("level"),
                None,
                ProblemKind::LogLevelFromEnv {
                    value: value.clone(),
                },
            );
            LogLevel::Info
        }),
        (None, Some(value)) => parse_level(value).unwrap_or_else(|| {
            let line = fields.and_then(|f| f.line("level"));
            walk.problem(
                path.key("level"),
                line,
                ProblemKind::UnsupportedValue {
                    value: value.clone(),
                    allowed: "trace, debug, info, warn, error",
                },
            );
            LogLevel::Info
        }),
        (None, None) => LogLevel::Info,
    };
    let line = fields.and_then(|f| f.line("type"));
    let format = match fields.and_then(|f| f.string("type")).as_deref() {
        None | Some("text") => LogFormat::Text,
        Some("json") => LogFormat::Json,
        Some("pretty") => {
            let feature = "the pretty log format";
            walk.problem(path.key("type"), line, ProblemKind::NotInV1 { feature });
            LogFormat::Text
        }
        Some(other) => {
            let (value, allowed) = (other.to_owned(), "text, json");
            walk.problem(
                path.key("type"),
                line,
                ProblemKind::UnsupportedValue { value, allowed },
            );
            LogFormat::Text
        }
    };
    let stderr = fields.and_then(|f| f.boolean("stderr")).unwrap_or(false);
    Logging {
        level,
        format,
        stderr,
    }
}

fn parse_level(value: &str) -> Option<LogLevel> {
    match value.to_ascii_uppercase().as_str() {
        "TRACE" => Some(LogLevel::Trace),
        "DEBUG" => Some(LogLevel::Debug),
        "INFO" => Some(LogLevel::Info),
        "WARN" | "WARNING" => Some(LogLevel::Warn),
        "ERROR" => Some(LogLevel::Error),
        _ => None,
    }
}

fn rust_settings(fields: Option<&Fields>, walk: &mut Walk) -> RustSettings {
    let host_readers = match fields.and_then(|f| f.string("host-readers")).as_deref() {
        None | Some("unknown") => HostReaders::Unknown,
        Some("none") => HostReaders::None,
        Some(other) => {
            let path = KeyPath::root().key("litestream-rs").key("host-readers");
            let line = fields.and_then(|f| f.line("host-readers"));
            let (value, allowed) = (other.to_owned(), "none, unknown");
            walk.problem(path, line, ProblemKind::UnsupportedValue { value, allowed });
            HostReaders::Unknown
        }
    };
    let responses = fields
        .and_then(|f| f.map("responses"))
        .map(|r| responses(r, walk))
        .unwrap_or_default();
    RustSettings {
        host_readers,
        host_autocheckpoint: host_autocheckpoint(fields, walk),
        responses,
    }
}

/// `host-autocheckpoint`: frames, or `0` and `off` for none; SQLite's 1000 when absent.
fn host_autocheckpoint(fields: Option<&Fields>, walk: &mut Walk) -> Option<NonZeroU32> {
    let Some(text) = fields.and_then(|f| f.string("host-autocheckpoint")) else {
        return NonZeroU32::new(SQLITE_AUTOCHECKPOINT);
    };
    if text == "off" {
        return None;
    }
    match text.parse::<u32>() {
        Ok(frames) => NonZeroU32::new(frames),
        Err(_) => {
            let path = KeyPath::root()
                .key("litestream-rs")
                .key("host-autocheckpoint");
            let line = fields.and_then(|f| f.line("host-autocheckpoint"));
            let allowed = "a page count, 0 or off";
            walk.problem(
                path,
                line,
                ProblemKind::UnsupportedValue {
                    value: text,
                    allowed,
                },
            );
            NonZeroU32::new(SQLITE_AUTOCHECKPOINT)
        }
    }
}

fn responses(fields: &Fields, walk: &mut Walk) -> ResponseSettings {
    let mut one = |name: &str| match fields.string(name).as_deref() {
        None => None,
        Some("keep-trying") => Some(ResponseSetting::KeepTrying),
        Some("stop") => Some(ResponseSetting::Stop),
        Some(other) => {
            let (value, allowed) = (other.to_owned(), "keep-trying, stop");
            walk.problem(
                fields.path.key(name),
                fields.line(name),
                ProblemKind::UnsupportedValue { value, allowed },
            );
            None
        }
    };
    let mut settings = ResponseSettings {
        credentials: one("credentials"),
        store_refused: one("store-refused"),
        remote_corrupt: one("remote-corrupt"),
        remote_regressed: one("remote-regressed"),
        wal_corrupt: one("wal-corrupt"),
        wal_truncated: one("wal-truncated"),
        wal_unlinked: one("wal-unlinked"),
        page_size_changed: one("page-size-changed"),
        bug: one("bug"),
        foreign_writer: None,
    };
    settings.foreign_writer = match fields.string("foreign-writer").as_deref() {
        None => None,
        Some("stop") => Some(ForeignWriterSetting::Stop),
        Some("continue-and-interleave-histories") => {
            Some(ForeignWriterSetting::ContinueAndInterleaveHistories)
        }
        Some(other) => {
            let (value, allowed) = (other.to_owned(), "stop, continue-and-interleave-histories");
            let name = "foreign-writer";
            walk.problem(
                fields.path.key(name),
                fields.line(name),
                ProblemKind::UnsupportedValue { value, allowed },
            );
            None
        }
    };
    settings
}
