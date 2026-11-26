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
//!    refused here too, before anything runs.
//! 5. `LOG_LEVEL` over `logging.level`; the snapshot-retention warning.

use std::num::NonZeroU32;

use lsr_core::{OtherWriter, Response, Responses};
use lsr_replica::{IfReplicationStops, MaxUnreplicatedAge};

use crate::config::{
    DbConfig, DbSource, DirSource, GoConfig, LogFormat, LogLevel, Logging, ReplicaConfig,
    ReplicaKind, RustSettings, S3Settings, Secret, Snapshot, SnapshotOverride, defaults,
};
use crate::duration::GoDuration;
use crate::error::{KeyPath, ProblemKind, WarningKind};
use crate::expand::{Env, clean, expand_path};
use crate::walk::{Fields, Listed, Walk};

const MIN_HEARTBEAT: GoDuration = GoDuration::from_secs(60);

/// SQLite's default `wal_autocheckpoint`, in frames.
const SQLITE_AUTOCHECKPOINT: u32 = 1000;

pub(crate) fn build(top: &Fields, env: &dyn Env, walk: &mut Walk) -> GoConfig {
    let rust = rust_settings(top.map("litestream-rs", walk), walk);
    global_checks(top, walk);
    let levels = levels(top, walk);
    let mut dbs = Vec::new();
    let mut seen: Vec<(String, usize)> = Vec::new();
    if let Listed::Items(items) = top.maps("dbs", walk) {
        for (index, fields) in items.iter().enumerate() {
            if let Some(db) = db(fields, index, top, &mut seen, env, walk) {
                dbs.push((index, fields, db));
            }
        }
    }
    let global = top.map("snapshot", walk);
    let promoted = promote(&dbs, walk);
    let snapshot = Snapshot {
        interval: global
            .and_then(|s| s.duration("interval", walk))
            .or(promoted.interval)
            .unwrap_or(defaults::SNAPSHOT_INTERVAL),
        retention: global
            .and_then(|s| s.duration("retention", walk))
            .or(promoted.retention)
            .unwrap_or(defaults::SNAPSHOT_RETENTION),
    };
    snapshot_checks(snapshot, &KeyPath::root().key("snapshot"), global, walk);
    GoConfig {
        addr: top.string("addr", walk),
        exec: top.string("exec", walk),
        levels,
        snapshot,
        l0_retention: top
            .duration("l0-retention", walk)
            .unwrap_or(defaults::L0_RETENTION),
        l0_retention_check_interval: top.duration("l0-retention-check-interval", walk),
        retention_enabled: top
            .map("retention", walk)
            .and_then(|r| r.boolean("enabled", walk))
            .unwrap_or(true),
        shutdown_sync_timeout: top
            .duration("shutdown-sync-timeout", walk)
            .unwrap_or(defaults::SHUTDOWN_SYNC_TIMEOUT),
        validation_interval: top
            .map("validation", walk)
            .and_then(|v| v.duration("interval", walk)),
        logging: logging(top.map("logging", walk), env, walk),
        dbs: dbs.into_iter().map(|(_, _, db)| db).collect(),
        rust,
    }
}

/// A duration that must be above zero, or at least zero.
fn positive(fields: &Fields, name: &'static str, walk: &mut Walk) {
    if let Some(value) = fields.duration(name, walk).filter(|d| d.nanos() <= 0) {
        walk.problem(
            fields.path.key(name),
            fields.line(name),
            ProblemKind::NotPositive { value },
        );
    }
}

fn not_negative(fields: &Fields, name: &'static str, walk: &mut Walk) {
    if let Some(value) = fields.duration(name, walk).filter(|d| d.nanos() < 0) {
        walk.problem(
            fields.path.key(name),
            fields.line(name),
            ProblemKind::Negative { value },
        );
    }
}

/// Go's `Validate` for the top-level durations.
fn global_checks(top: &Fields, walk: &mut Walk) {
    if let Some(snapshot) = top.map("snapshot", walk) {
        positive(snapshot, "interval", walk);
        positive(snapshot, "retention", walk);
    }
    not_negative(top, "l0-retention", walk);
    positive(top, "l0-retention-check-interval", walk);
    not_negative(top, "shutdown-sync-timeout", walk);
    positive(top, "shutdown-sync-interval", walk);
    if let Some(value) = top
        .duration("heartbeat-interval", walk)
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
    match top.maps("levels", walk) {
        Listed::Absent => defaults::LEVELS.to_vec(),
        Listed::Null => Vec::new(),
        Listed::Items(items) => items
            .iter()
            .map(|level| {
                let interval = level.duration("interval", walk).unwrap_or(GoDuration::ZERO);
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
                    let snapshot = fields.map("snapshot", walk);
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
    seen: &mut Vec<(String, usize)>,
    env: &dyn Env,
    walk: &mut Walk,
) -> Option<DbConfig> {
    let at = fields.path.clone();
    let line = fields.line;
    let source = source(fields, index, seen, env, walk)?;
    if fields.boolean("watch", walk) == Some(true) && !matches!(source, DbSource::Dir(_)) {
        walk.problem(at.clone(), line, ProblemKind::WatchWithoutDir);
    }
    if fields.string("meta-path", walk).is_some() && fields.string("meta-dir", walk).is_some() {
        walk.problem(at.clone(), line, ProblemKind::MetaPathAndMetaDir);
    }
    let snapshot_fields = fields.map("snapshot", walk);
    if let Some(snapshot) = snapshot_fields {
        positive(snapshot, "interval", walk);
        positive(snapshot, "retention", walk);
    }
    let snapshot = SnapshotOverride {
        interval: snapshot_fields.and_then(|s| s.duration("interval", walk)),
        retention: snapshot_fields.and_then(|s| s.duration("retention", walk)),
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
    let truncate_page_n = fields.int("truncate-page-n", walk).filter(|&n| n != 0);
    Some(DbConfig {
        source,
        meta_path: fields.string("meta-path", walk),
        snapshot,
        monitor_interval: fields.duration("monitor-interval", walk),
        checkpoint_interval: fields.duration("checkpoint-interval", walk),
        min_checkpoint_page_count: fields.int("min-checkpoint-page-count", walk),
        truncate_page_n,
        max_sync_wal_bytes: fields.int("max-sync-wal-bytes", walk),
        restore_if_db_not_exists: fields
            .boolean("restore-if-db-not-exists", walk)
            .unwrap_or(false),
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
    let meta_dir = fields.string("meta-dir", walk);
    match (fields.string("path", walk), fields.string("dir", walk)) {
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
            let Some(pattern) = fields.string("pattern", walk) else {
                walk.problem(at, line, ProblemKind::PatternRequired);
                return None;
            };
            let recursive = fields.boolean("recursive", walk).unwrap_or(false);
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
    let single = fields.map("replica", walk);
    let list = match fields.maps("replicas", walk) {
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

/// Go's `litestream.IsURL`: `^\w+://`, where Go's `\w` is ASCII only (RE2), so a Unicode
/// letter in the scheme makes the text a path.
#[must_use]
pub fn is_url(text: &str) -> bool {
    text.split_once("://").is_some_and(|(scheme, _)| {
        !scheme.is_empty()
            && scheme
                .bytes()
                .all(|b| b == b'_' || b.is_ascii_alphanumeric())
    })
}

/// Go's `ReplicaType`: the URL's scheme, else `type`, else `file`.
fn replica_kind(fields: &Fields, walk: &mut Walk) -> Option<ReplicaKind> {
    let url = fields.string("url", walk).filter(|u| is_url(u));
    let (name, key) = match (&url, fields.string("type", walk)) {
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

/// A replica's string key, else the top level's (Go copies the global value in).
fn either(fields: &Fields, top: &Fields, name: &'static str, walk: &mut Walk) -> Option<String> {
    match fields.string(name, walk) {
        Some(value) => Some(value),
        None => top.string(name, walk),
    }
}

fn build_replica(fields: &Fields, top: &Fields, walk: &mut Walk) -> Option<ReplicaConfig> {
    let path = fields.string("path", walk);
    if path.as_deref().is_some_and(is_url) {
        walk.problem(
            fields.path.key("path"),
            fields.line("path"),
            ProblemKind::ReplicaPathIsUrl,
        );
    }
    let kind = replica_kind(fields, walk)?;
    Some(ReplicaConfig {
        kind,
        url: fields.string("url", walk),
        path,
        sync_interval: fields
            .duration("sync-interval", walk)
            .or_else(|| top.duration("sync-interval", walk)),
        max_sync_ltx_files: fields
            .int("max-sync-ltx-files", walk)
            .or_else(|| top.int("max-sync-ltx-files", walk)),
        s3: S3Settings {
            access_key_id: either(fields, top, "access-key-id", walk).map(Secret::new),
            secret_access_key: either(fields, top, "secret-access-key", walk).map(Secret::new),
            region: either(fields, top, "region", walk),
            bucket: either(fields, top, "bucket", walk),
            endpoint: either(fields, top, "endpoint", walk),
            force_path_style: fields
                .boolean("force-path-style", walk)
                .or_else(|| top.boolean("force-path-style", walk)),
            part_size: fields.bytes("part-size", walk),
            concurrency: fields
                .int("concurrency", walk)
                .and_then(|n| u64::try_from(n).ok())
                .filter(|n| *n > 0),
        },
    })
}

/// `logging`, with `LOG_LEVEL` over `logging.level` as in Go.
fn logging(fields: Option<&Fields>, env: &dyn Env, walk: &mut Walk) -> Logging {
    let path = KeyPath::root().key("logging");
    let from_env = env.var("LOG_LEVEL").filter(|v| !v.is_empty());
    let written = fields.and_then(|f| f.string("level", walk));
    // Go's `InitLog` switches on the upper-cased level and keeps INFO for anything else,
    // and builds a text handler for any type but json and pretty (internal/log.go).
    let level = match from_env.as_ref().or(written.as_ref()) {
        None => LogLevel::Info,
        Some(value) => parse_level(value).unwrap_or_else(|| {
            let line = fields.and_then(|f| f.line("level"));
            let value = value.clone();
            let kind = WarningKind::UnknownLogSetting {
                value,
                what: "level",
                used: "info",
            };
            walk.warn(path.key("level"), line, kind);
            LogLevel::Info
        }),
    };
    let line = fields.and_then(|f| f.line("type"));
    let format = match fields.and_then(|f| f.string("type", walk)).as_deref() {
        None | Some("text") => LogFormat::Text,
        Some("json") => LogFormat::Json,
        Some("pretty") => {
            let why = "the pretty log format; logs are plain text";
            walk.warn(path.key("type"), line, WarningKind::NoEffect { why });
            LogFormat::Text
        }
        Some(other) => {
            let value = other.to_owned();
            let kind = WarningKind::UnknownLogSetting {
                value,
                what: "format",
                used: "text",
            };
            walk.warn(path.key("type"), line, kind);
            LogFormat::Text
        }
    };
    let stderr = fields
        .and_then(|f| f.boolean("stderr", walk))
        .unwrap_or(false);
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

/// A `litestream-rs` key that takes one of a few words: the word's value, or `None` (with a
/// problem listing the words, in table order) when the key is absent or not one of them.
fn choice<T: Copy>(
    fields: Option<&Fields>,
    name: &'static str,
    table: &[(&str, T)],
    walk: &mut Walk,
) -> Option<T> {
    let fields = fields?;
    let text = fields.string(name, walk)?;
    if let Some((_, value)) = table.iter().find(|(word, _)| *word == text) {
        return Some(*value);
    }
    let words: Vec<&str> = table.iter().map(|(word, _)| *word).collect();
    unsupported(fields, name, text, words.join(", "), walk);
    None
}

/// The `UnsupportedValue` problem for `value` at key `name` of `fields`.
fn unsupported(fields: &Fields, name: &str, value: String, allowed: String, walk: &mut Walk) {
    let kind = ProblemKind::UnsupportedValue { value, allowed };
    walk.problem(fields.path.key(name), fields.line(name), kind);
}

fn rust_settings(fields: Option<&Fields>, walk: &mut Walk) -> RustSettings {
    let responses = fields
        .and_then(|f| f.map("responses", walk))
        .map(|r| responses(r, walk))
        .unwrap_or(Responses::GO_LIKE);
    RustSettings {
        app_autocheckpoint: app_autocheckpoint(fields, walk),
        responses,
        if_replication_stops: if_replication_stops(fields, walk),
        max_unreplicated_age: max_unreplicated_age(fields, walk),
    }
}

/// `if-replication-stops` (decisions D13, D14): `hold-start` when absent.
fn if_replication_stops(fields: Option<&Fields>, walk: &mut Walk) -> IfReplicationStops {
    let table = [
        ("report", IfReplicationStops::Report),
        ("hold-start", IfReplicationStops::HoldStart),
        ("stop-app", IfReplicationStops::StopApp),
    ];
    choice(fields, "if-replication-stops", &table, walk).unwrap_or(IfReplicationStops::HoldStart)
}

/// `max-unreplicated-age` (decision D22): 10 minutes when absent; below 5 minutes is refused,
/// so one retry at the backoff cap always fits inside it.
fn max_unreplicated_age(fields: Option<&Fields>, walk: &mut Walk) -> MaxUnreplicatedAge {
    let name = "max-unreplicated-age";
    let Some((fields, value)) = fields.and_then(|f| Some((f, f.duration(name, walk)?))) else {
        return MaxUnreplicatedAge::DEFAULT;
    };
    if let Some(bound) = value.to_std().and_then(MaxUnreplicatedAge::new) {
        return bound;
    }
    let allowed = "a duration of at least 5m".to_owned();
    unsupported(fields, name, value.to_string(), allowed, walk);
    MaxUnreplicatedAge::DEFAULT
}

/// `app-autocheckpoint`: frames, or `0` and `off` for none; SQLite's 1000 when absent.
fn app_autocheckpoint(fields: Option<&Fields>, walk: &mut Walk) -> Option<NonZeroU32> {
    let name = "app-autocheckpoint";
    let Some((fields, text)) = fields.and_then(|f| Some((f, f.string(name, walk)?))) else {
        return NonZeroU32::new(SQLITE_AUTOCHECKPOINT);
    };
    if text == "off" {
        return None;
    }
    if let Ok(frames) = text.parse::<u32>() {
        return NonZeroU32::new(frames);
    }
    unsupported(
        fields,
        name,
        text,
        "a page count, 0 or off".to_owned(),
        walk,
    );
    NonZeroU32::new(SQLITE_AUTOCHECKPOINT)
}

fn responses(fields: &Fields, walk: &mut Walk) -> Responses {
    let base = Responses::GO_LIKE;
    let mut one = |name: &'static str, default: Response| {
        let table = [
            ("keep-trying", Response::KeepTrying),
            ("stop", Response::Stop),
        ];
        choice(Some(fields), name, &table, walk).unwrap_or(default)
    };
    let credentials = one("credentials", base.credentials);
    let permission_denied = one("permission-denied", base.permission_denied);
    let clock_skew = one("clock-skew", base.clock_skew);
    let bucket_missing = one("bucket-missing", base.bucket_missing);
    let tls = one("tls", base.tls);
    let store_refused = one("store-refused", base.store_refused);
    let remote_corrupt = one("remote-corrupt", base.remote_corrupt);
    let remote_regressed = one("remote-regressed", base.remote_regressed);
    let wal_corrupt = one("wal-corrupt", base.wal_corrupt);
    let wal_truncated = one("wal-truncated", base.wal_truncated);
    let wal_unlinked = one("wal-unlinked", base.wal_unlinked);
    let wal_sync_failed = one("wal-sync-failed", base.wal_sync_failed);
    let page_size_changed = one("page-size-changed", base.page_size_changed);
    let conditional_writes_unsupported = one(
        "conditional-writes-unsupported",
        base.conditional_writes_unsupported,
    );
    let bug = one("bug", base.bug);
    let other_writer = [
        ("stop", OtherWriter::Stop),
        (
            "continue-and-interleave-histories",
            OtherWriter::ContinueAndInterleaveHistories,
        ),
    ];
    Responses {
        credentials,
        permission_denied,
        clock_skew,
        bucket_missing,
        tls,
        store_refused,
        remote_corrupt,
        remote_regressed,
        wal_corrupt,
        wal_truncated,
        wal_unlinked,
        wal_sync_failed,
        page_size_changed,
        database_page_mismatch: base.database_page_mismatch,
        conditional_writes_unsupported,
        bug,
        other_writer: choice(Some(fields), "other-writer", &other_writer, walk)
            .unwrap_or(base.other_writer),
    }
}

#[cfg(test)]
mod tests;
