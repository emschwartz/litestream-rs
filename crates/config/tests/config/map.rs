//! From a loaded config to replicators: each key litestream-rs honours against the Go line
//! it copies, the refusals, and `dir` expansion (Go's directory tests in
//! `cmd/litestream/main_test.go`, ported by scenario).

use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_config::{
    Credentials, DbPlan, Env, Expand, GoConfig, MapProblemKind, Plan, ReplicaPlan, ReplicaProblem,
    plan,
};
use lsr_core::{
    Interval, L0Retention, Maintenance, OtherWriter, Response, Responses, RetentionMode,
};
use lsr_replica::{IfReplicationStops, MaxUnreplicatedAge, NonZeroDuration};
use lsr_store_s3::{Provider, UrlStyle};
use tempfile::TempDir;

use crate::common::{TestEnv, parse, refused};

fn planned_with(text: &str, env: &dyn Env) -> Plan {
    let loaded = match GoConfig::parse(text, Expand::Off, env) {
        Ok(loaded) => loaded,
        Err(error) => panic!("refused at load:\n{}", error.report()),
    };
    match plan(&loaded.config, env) {
        Ok(plan) => plan,
        Err(error) => panic!("refused at mapping: {:#?}", error.problems()),
    }
}

fn planned(text: &str) -> Plan {
    planned_with(text, &TestEnv::default())
}

fn one(text: &str) -> DbPlan {
    let plan = planned(text);
    assert_eq!(plan.dbs().len(), 1, "{plan:#?}");
    plan.dbs().first().unwrap().clone()
}

/// The mapping problems of a file that loads.
fn unmapped(text: &str) -> Vec<(String, MapProblemKind)> {
    let env = TestEnv::default();
    let config = parse(text).config;
    match plan(&config, &env) {
        Ok(plan) => panic!("mapped: {plan:#?}"),
        Err(error) => error
            .problems()
            .iter()
            .map(|p| (p.at.to_string(), p.kind.clone()))
            .collect(),
    }
}

fn db_with(keys: &str) -> String {
    format!("dbs:\n  - path: /data/app.db\n{keys}    replica:\n      path: /backup\n")
}

fn s3_with(replica: &str) -> DbPlan {
    one(&format!(
        "dbs:\n  - path: /data/app.db\n    replica:\n{replica}"
    ))
}

fn s3_config(db: &DbPlan) -> &lsr_store_s3::S3Config {
    match db.replica() {
        ReplicaPlan::S3 { config, .. } => config,
        ReplicaPlan::File(path) => panic!("a file replica at {}", path.display()),
    }
}

fn s3_refusal(replica: &str) -> ReplicaProblem {
    let problems = unmapped(&format!(
        "dbs:\n  - path: /data/app.db\n    replica:\n{replica}"
    ));
    match problems.as_slice() {
        [(_, MapProblemKind::Replica(problem))] => problem.clone(),
        other => panic!("{other:#?}"),
    }
}

fn secs(n: u64) -> NonZeroDuration {
    NonZeroDuration::new(Duration::from_secs(n)).unwrap()
}

/// Go's `NewDB` defaults (`db.go:32-38`, `db.go:342`) and `NewReplica`'s
/// (`replica.go:26-27`), and the store defaults of `DefaultConfig`.
#[test]
fn an_entry_with_no_settings_gets_go_s_defaults() {
    let db = one(&db_with(""));
    assert_eq!(db.path(), Path::new("/data/app.db"));
    assert_eq!(db.meta_dir(), Path::new("/data/.app.db-litestream"));
    let config = db.config();
    assert_eq!(config.poll_interval, secs(1));
    assert_eq!(config.retry_interval, secs(1));
    assert_eq!(config.checkpoint.interval, Some(Duration::from_secs(60)));
    assert_eq!(config.checkpoint.min_pages, NonZeroU32::new(1000));
    assert_eq!(config.checkpoint.truncate_pages, NonZeroU32::new(121_359));
    assert_eq!(config.maintenance, Maintenance::go_default());
    assert_eq!(
        config.sync_limits.wal_bytes_per_step,
        NonZeroU64::new(64 << 20)
    );
    assert_eq!(config.sync_limits.files_per_upload, NonZeroU32::new(256));
    assert_eq!(config.responses, Responses::GO_LIKE);
    assert_eq!(config.shutdown_grace, Duration::from_secs(30));
    assert_eq!(
        config.verify_interval, None,
        "Go runs no validation unless asked"
    );
    assert!(matches!(db.replica(), ReplicaPlan::File(p) if p == Path::new("/backup")));
    assert!(!db.restore_if_missing());
}

#[test]
fn monitor_interval_is_the_poll_and_zero_is_refused() {
    let db = one(&db_with("    monitor-interval: 250ms\n"));
    assert_eq!(
        db.config().poll_interval,
        NonZeroDuration::new(Duration::from_millis(250)).unwrap()
    );
    let problems = unmapped(&db_with("    monitor-interval: 0s\n"));
    assert!(
        matches!(problems.as_slice(), [(at, MapProblemKind::MonitorOff { .. })] if at == "dbs[0].monitor-interval"),
        "{problems:#?}"
    );
}

/// Go's `CheckpointInterval > 0` check (`db.go:1571`).
#[test]
fn checkpoint_interval_of_zero_turns_the_timed_checkpoint_off() {
    let db = one(&db_with("    checkpoint-interval: 2m\n"));
    assert_eq!(
        db.config().checkpoint.interval,
        Some(Duration::from_secs(120))
    );
    let db = one(&db_with("    checkpoint-interval: 0s\n"));
    assert_eq!(db.config().checkpoint.interval, None);
}

/// Go refuses a non-positive count when the database opens (`db.go:872`).
#[test]
fn min_checkpoint_page_count_must_be_above_zero() {
    let db = one(&db_with("    min-checkpoint-page-count: 500\n"));
    assert_eq!(db.config().checkpoint.min_pages, NonZeroU32::new(500));
    let problems = unmapped(&db_with("    min-checkpoint-page-count: 0\n"));
    assert!(
        matches!(
            problems.as_slice(),
            [(_, MapProblemKind::MinCheckpointPages { value: 0 })]
        ),
        "{problems:#?}"
    );
    // Go holds it in an `int`; a WAL never holds more than `u32::MAX` frames, so a
    // larger count is the same threshold.
    let db = one(&db_with("    min-checkpoint-page-count: 9999999999\n"));
    assert_eq!(db.config().checkpoint.min_pages, NonZeroU32::new(u32::MAX));
}

/// Go's `effectiveTruncatePageN` (`db.go:1599-1608`); the
/// library's defaults and the file's agree.
#[test]
fn truncate_page_n_acts_as_go() {
    {
        let at = |key: &str| one(&db_with(key)).config().checkpoint;
        assert_eq!(at("").truncate_pages, NonZeroU32::new(121_359));
        assert_eq!(
            at("    truncate-page-n: 0\n").truncate_pages,
            NonZeroU32::new(121_359)
        );
        assert_eq!(
            at("    truncate-page-n: 5000\n").truncate_pages,
            NonZeroU32::new(5000)
        );
        assert_eq!(at("    truncate-page-n: -1\n").truncate_pages, None);
    }
    assert_eq!(
        one(&db_with("")).config().checkpoint.truncate_pages,
        lsr_replica::Config::default().checkpoint.truncate_pages
    );
}

/// `wal_reader.go:241` and `replica.go:208`: zero or less is no limit.
#[test]
fn sync_limits_follow_go_with_zero_as_no_limit() {
    let db = one(
        "dbs:\n  - path: /data/app.db\n    max-sync-wal-bytes: 1024\n    replica:\n      path: /backup\n      max-sync-ltx-files: 10\n",
    );
    assert_eq!(
        db.config().sync_limits.wal_bytes_per_step,
        NonZeroU64::new(1024)
    );
    assert_eq!(
        db.config().sync_limits.files_per_upload,
        NonZeroU32::new(10)
    );
    let db = one(
        "dbs:\n  - path: /data/app.db\n    max-sync-wal-bytes: 0\n    replica:\n      path: /backup\n      max-sync-ltx-files: 0\n",
    );
    assert_eq!(db.config().sync_limits.wal_bytes_per_step, None);
    assert_eq!(db.config().sync_limits.files_per_upload, None);
}

/// Go's replica backoff starts at the sync interval (`replica.go:449-456`); the
/// top-level setting is copied into the replica as Go's `SetDefaults` does.
#[test]
fn sync_interval_is_the_first_retry_wait() {
    let db = one(&format!("sync-interval: 10s\n{}", db_with("")));
    assert_eq!(db.config().retry_interval, secs(10));
}

#[test]
fn schedule_settings_become_core_s_maintenance() {
    let db = one(&format!(
        "levels:\n  - interval: 1m\n  - interval: 10m\nsnapshot:\n  interval: 2h\n  retention: 48h\nl0-retention: 0s\nretention:\n  enabled: false\n{}",
        db_with("")
    ));
    let want = Maintenance::new(
        vec![Duration::from_secs(60), Duration::from_secs(600)],
        Duration::from_secs(7200),
        Duration::from_secs(172_800),
        L0Retention::Keep,
    )
    .unwrap()
    .with_retention(RetentionMode::Off);
    assert_eq!(db.config().maintenance, want);
}

/// Go's `l0-retention-check-interval` (`store.go:92`, 15 s) is how long an upload waits for
/// the next maintenance listing; before it was mapped, every upload listed the replica.
#[test]
fn l0_retention_check_interval_maps_and_defaults_to_go_s_15s() {
    let every = |text: &str| {
        one(&format!("{text}{}", db_with("")))
            .config()
            .maintenance
            .l0_check_interval()
    };
    let iv = |secs: u64| Interval::new(Duration::from_secs(secs)).unwrap();
    assert_eq!(every(""), iv(15));
    assert_eq!(every("l0-retention-check-interval: 1m\n"), iv(60));
    assert_eq!(Maintenance::go_default().l0_check_interval(), iv(15));
    // The check runs at what the file sets, exactly (also when it sets Go's 15 s); only the
    // unset default runs it at 120 s (docs/decisions/D193.md).
    let runs = |text: &str| {
        one(&format!("{text}{}", db_with("")))
            .config()
            .maintenance
            .retention_check_every()
    };
    assert_eq!(runs("l0-retention-check-interval: 1m\n"), iv(60));
    assert_eq!(runs("l0-retention-check-interval: 15s\n"), iv(15));
    assert_eq!(runs(""), iv(120));
    let problems = unmapped(&format!(
        "l0-retention-check-interval: 1500us\n{}",
        db_with("")
    ));
    assert!(
        matches!(
            problems.as_slice(),
            [(at, MapProblemKind::Duration { .. })] if at == "l0-retention-check-interval"
        ),
        "{problems:#?}"
    );
}

/// Core refuses levels that are not increasing; Go loads them.
#[test]
fn a_schedule_core_refuses_is_refused_naming_it() {
    let problems = unmapped(&format!(
        "levels:\n  - interval: 10m\n  - interval: 1m\n{}",
        db_with("")
    ));
    assert!(
        matches!(problems.as_slice(), [(_, MapProblemKind::Maintenance(_))]),
        "{problems:#?}"
    );
}

#[test]
fn shutdown_sync_timeout_is_the_drain_grace_and_the_shared_deadline() {
    let plan = planned(&format!("shutdown-sync-timeout: 5s\n{}", db_with("")));
    assert_eq!(plan.shutdown_sync_timeout(), Duration::from_secs(5));
    assert_eq!(
        plan.dbs().first().unwrap().config().shutdown_grace,
        Duration::from_secs(5)
    );
}

/// Go's `validation.interval` is the scheduled verify's interval for every database; at zero
/// or less Go starts no validation monitor (`store.go:268`), and neither does litestream-rs. The
/// replica-level `validation-interval` Go parses and never reads, so it changes nothing.
#[test]
fn validation_interval_is_the_scheduled_verify_s_and_zero_is_off() {
    let every = |text: &str| {
        one(&format!("{text}\n{}", db_with("")))
            .config()
            .verify_interval
    };
    assert_eq!(every("validation:\n  interval: 10m"), Some(secs(600)));
    assert_eq!(every("validation:\n  interval: 0s"), None);
    assert_eq!(every("validation:\n  interval: -1s"), None);
    assert_eq!(every("validation-interval: 10m"), None);
}

#[test]
fn responses_override_go_s_one_by_one() {
    let db = one(&format!(
        "litestream-rs:\n  responses:\n    credentials: stop\n    other-writer: continue-and-interleave-histories\n{}",
        db_with("")
    ));
    let responses = db.config().responses;
    assert_eq!(responses.credentials, Response::Stop);
    assert_eq!(responses.store_refused, Response::KeepTrying);
    assert_eq!(
        responses.other_writer,
        OtherWriter::ContinueAndInterleaveHistories
    );
}

/// Each kind of access refusal has its own key, which sets its field and no other.
#[test]
fn each_kind_of_access_refusal_has_its_own_response() {
    let keys = [
        "credentials",
        "permission-denied",
        "clock-skew",
        "bucket-missing",
        "tls",
    ];
    for key in keys {
        let db = one(&format!(
            "litestream-rs:\n  responses:\n    {key}: stop\n{}",
            db_with("")
        ));
        let r = db.config().responses;
        let stops = [
            r.credentials,
            r.permission_denied,
            r.clock_skew,
            r.bucket_missing,
            r.tls,
        ]
        .map(|response| response == Response::Stop);
        let want = keys.map(|other| other == key);
        assert_eq!(stops, want, "{key}");
    }
}

/// A Rust-only setting: the application's `wal_autocheckpoint`, SQLite's 1000 unless stated.
#[test]
fn app_autocheckpoint_defaults_to_sqlite_s_and_can_be_off() {
    let frames = |head: &str| {
        one(&format!("{head}{}", db_with("")))
            .config()
            .app_autocheckpoint
    };
    assert_eq!(frames(""), NonZeroU32::new(1000));
    assert_eq!(
        frames("litestream-rs:\n  app-autocheckpoint: 500\n"),
        NonZeroU32::new(500)
    );
    assert_eq!(frames("litestream-rs:\n  app-autocheckpoint: 0\n"), None);
    assert_eq!(frames("litestream-rs:\n  app-autocheckpoint: off\n"), None);
    let problems = refused(&format!(
        "litestream-rs:\n  app-autocheckpoint: lots\n{}",
        db_with("")
    ));
    assert!(
        problems
            .iter()
            .any(|p| p.path.to_string() == "litestream-rs.app-autocheckpoint"),
        "{problems:#?}"
    );
}

/// The fail-stop setting: `hold-start` and 10 minutes unless
/// stated; an unknown value, or a bound below five minutes, is refused.
#[test]
fn if_replication_stops_defaults_to_hold_start_and_the_bound_to_ten_minutes() {
    let config = |head: &str| one(&format!("{head}{}", db_with(""))).config().clone();
    let default = config("");
    assert_eq!(default.if_replication_stops, IfReplicationStops::HoldStart);
    assert_eq!(default.max_unreplicated_age, MaxUnreplicatedAge::DEFAULT);
    for (value, want) in [
        ("report", IfReplicationStops::Report),
        ("hold-start", IfReplicationStops::HoldStart),
        ("stop-app", IfReplicationStops::StopApp),
    ] {
        let got = config(&format!(
            "litestream-rs:\n  if-replication-stops: {value}\n"
        ));
        assert_eq!(got.if_replication_stops, want, "{value}");
    }
    let bound = config("litestream-rs:\n  max-unreplicated-age: 1h\n").max_unreplicated_age;
    assert_eq!(bound.get(), std::time::Duration::from_secs(3600));
    for (head, key) in [
        (
            "litestream-rs:\n  if-replication-stops: always\n",
            "if-replication-stops",
        ),
        (
            "litestream-rs:\n  max-unreplicated-age: 4m59s\n",
            "max-unreplicated-age",
        ),
    ] {
        let problems = refused(&format!("{head}{}", db_with("")));
        let path = format!("litestream-rs.{key}");
        assert!(
            problems.iter().any(|p| p.path.to_string() == path),
            "{head}: {problems:#?}"
        );
    }
}

/// `snapshot-read-limit` (issue 315): a byte size a second, no limit unless stated, never
/// zero; Go ignores the section, so the key is read only here.
#[test]
fn snapshot_read_limit_is_a_byte_size_off_by_default() {
    let config = |head: &str| one(&format!("{head}{}", db_with(""))).config().clone();
    assert_eq!(config("").snapshot_read_limit, None);
    for (value, want) in [
        ("30MB", 30_000_000),
        ("512KiB", 524_288),
        ("1048576", 1_048_576),
    ] {
        let got = config(&format!("litestream-rs:\n  snapshot-read-limit: {value}\n"));
        assert_eq!(
            got.snapshot_read_limit.map(|l| l.get()),
            Some(want),
            "{value}"
        );
    }
    for value in ["0", "fast", "-1MB"] {
        let problems = refused(&format!(
            "litestream-rs:\n  snapshot-read-limit: {value}\n{}",
            db_with("")
        ));
        assert!(
            problems
                .iter()
                .any(|p| p.path.to_string() == "litestream-rs.snapshot-read-limit"),
            "{value}: {problems:#?}"
        );
    }
}

/// Go's `TestNewDBFromConfig_MetaPathExpansion`.
#[test]
fn meta_path_is_expanded_and_used_as_is() {
    let db = one(&db_with("    meta-path: ~/litestream-meta\n"));
    assert_eq!(db.meta_dir(), Path::new("/home/u/litestream-meta"));
}

#[test]
fn relative_paths_are_made_absolute_as_go_does() {
    let db = one("dbs:\n  - path: app.db\n    replica:\n      path: backup\n");
    assert_eq!(db.path(), Path::new("/work/app.db"));
    assert_eq!(db.meta_dir(), Path::new("/work/.app.db-litestream"));
    assert!(matches!(db.replica(), ReplicaPlan::File(p) if p == Path::new("/work/backup")));
}

#[test]
fn restore_if_db_not_exists_is_carried() {
    let db = one(&db_with("    restore-if-db-not-exists: true\n"));
    assert!(db.restore_if_missing());
}

/// Go's `TestNewFileReplicaFromConfig` and its file URL form.
#[test]
fn a_file_replica_from_path_or_url() {
    let db = s3_with("      url: file:///backup/app\n");
    assert!(matches!(db.replica(), ReplicaPlan::File(p) if p == Path::new("/backup/app")));
    let problem = s3_refusal("      url: file:///backup\n      path: /other\n");
    assert_eq!(problem, ReplicaProblem::FileUrlAndPath);
    let problem = s3_refusal("      type: file\n");
    assert_eq!(problem, ReplicaProblem::FilePathRequired);
}

/// Go's `TestNewFileReplicaFromConfig_LegacyEscapedURLPath`.
#[test]
fn an_escaped_file_url_uses_the_legacy_directory_only_when_it_alone_exists() {
    let dir = TempDir::new().unwrap();
    let decoded = dir.path().join("daily backup");
    let legacy = dir.path().join("daily%20backup");
    let url = format!(
        "      url: file://{}/daily%20backup\n",
        dir.path().display()
    );
    let db = s3_with(&url);
    assert!(matches!(db.replica(), ReplicaPlan::File(p) if *p == decoded));
    std::fs::create_dir(&legacy).unwrap();
    let db = s3_with(&url);
    assert!(matches!(db.replica(), ReplicaPlan::File(p) if *p == legacy));
    std::fs::create_dir(&decoded).unwrap();
    assert!(matches!(
        s3_refusal(&url),
        ReplicaProblem::LegacyBoth { .. }
    ));
}

/// Go's `TestNewS3ReplicaFromConfig/URL` and `URLWithoutQuery`: AWS, virtual host.
#[test]
fn an_s3_url_names_the_bucket_and_prefix() {
    let db = s3_with("      url: s3://my-bucket/db/app\n");
    let config = s3_config(&db);
    assert_eq!(config.bucket().as_str(), "my-bucket");
    assert_eq!(config.prefix().as_str(), "db/app");
    assert_eq!(config.region().as_str(), "us-east-1");
    assert_eq!(
        config.endpoint().url().as_str(),
        "https://s3.us-east-1.amazonaws.com/"
    );
    assert_eq!(config.style(), UrlStyle::VirtualHost);
    assert!(matches!(
        db.replica(),
        ReplicaPlan::S3 {
            credentials: Credentials::FromEnvironment,
            ..
        }
    ));
}

/// A bucket with no path inside it is a replica at the root of the bucket, as under Go, however
/// it is written: a URL with or without the slash, or `type: s3` with a missing or empty `path`.
#[test]
fn a_bucket_with_no_path_is_the_whole_bucket() {
    for replica in [
        "      url: s3://my-bucket/\n",
        "      type: s3\n      bucket: my-bucket\n",
        "      type: s3\n      bucket: my-bucket\n      path: \"\"\n",
    ] {
        let db = s3_with(replica);
        let config = s3_config(&db);
        assert_eq!(config.bucket().as_str(), "my-bucket", "{replica}");
        assert_eq!(config.prefix().as_str(), "", "{replica}");
    }
    // Go's prefix for `s3://my-bucket` is `.`, which no URL can address: refused, never the bare bucket.
    assert!(matches!(
        s3_refusal("      url: s3://my-bucket\n"),
        ReplicaProblem::NoSlashAfterBucket
    ));
}

/// Go's `URLWithEndpointQuery` and `ConfigOverridesQuery`: an endpoint turns path
/// style on, the file's keys win over the query.
#[test]
fn query_settings_apply_and_the_file_s_keys_win() {
    let db =
        s3_with("      url: s3://b/p?endpoint=localhost:9000&region=eu-west-1&partSize=10485760\n");
    let config = s3_config(&db);
    assert_eq!(config.endpoint().url().as_str(), "http://localhost:9000/");
    assert_eq!(config.region().as_str(), "eu-west-1");
    assert_eq!(config.style(), UrlStyle::Path);
    assert_eq!(config.part_size().get(), 10 << 20);
    let db = s3_with(
        "      url: s3://b/p?endpoint=https://example.com&forcePathStyle=false\n      region: us-west-2\n      part-size: 6MiB\n",
    );
    let config = s3_config(&db);
    assert_eq!(config.style(), UrlStyle::VirtualHost);
    assert_eq!(config.region().as_str(), "us-west-2");
    assert_eq!(config.part_size().get(), 6 << 20);
}

/// Go's `TestNewS3ReplicaClientFromConfig/HTTPSEndpoint` and the bucket and path keys.
#[test]
fn bucket_path_and_endpoint_keys_without_a_url() {
    let db = s3_with(
        "      type: s3\n      bucket: b\n      path: x/y\n      endpoint: fly.storage.tigris.dev\n      region: auto\n",
    );
    let config = s3_config(&db);
    assert_eq!(
        config.endpoint().url().as_str(),
        "https://fly.storage.tigris.dev/"
    );
    assert_eq!(config.provider(), Provider::Tigris);
    assert_eq!(config.style(), UrlStyle::Path);
    assert_eq!(config.prefix().as_str(), "x/y");
}

/// Go's `s3.ParseHost` for AWS virtual hosts and MinIO's `bucket.host:port`
/// (`TestNewS3ReplicaFromConfig/MinIO`).
#[test]
fn provider_hosts_go_splits() {
    let db = s3_with("      url: s3://my-bucket.s3.eu-west-1.amazonaws.com/db\n");
    let config = s3_config(&db);
    assert_eq!(config.bucket().as_str(), "my-bucket");
    assert_eq!(config.region().as_str(), "eu-west-1");
    let db = s3_with("      url: s3://my-bucket.localhost:9000/db\n");
    let config = s3_config(&db);
    assert_eq!(config.bucket().as_str(), "my-bucket");
    assert_eq!(config.endpoint().url().as_str(), "http://localhost:9000/");
    assert_eq!(config.style(), UrlStyle::Path);
    assert!(matches!(
        s3_refusal("      url: s3://b.nyc3.digitaloceanspaces.com/db\n"),
        ReplicaProblem::ProviderHost { .. }
    ));
}

#[test]
fn s3_refusals() {
    assert_eq!(
        s3_refusal("      url: s3://b/p\n      bucket: c\n"),
        ReplicaProblem::S3UrlAndBucket
    );
    assert_eq!(
        s3_refusal("      url: s3://b/p\n      path: q\n"),
        ReplicaProblem::S3UrlAndPath
    );
    assert_eq!(
        s3_refusal("      type: s3\n      path: q\n"),
        ReplicaProblem::BucketRequired
    );
    assert!(matches!(
        s3_refusal("      url: s3://b/p?skipVerify=true\n"),
        ReplicaProblem::QueryNotInV1 { .. }
    ));
    assert!(matches!(
        s3_refusal("      url: s3://b/p?storageClass=GLACIER\n"),
        ReplicaProblem::QueryNotInV1 { .. }
    ));
    assert!(matches!(
        s3_refusal("      url: s3://b/p?forcePathstyle=true\n"),
        ReplicaProblem::QueryUnknown { .. }
    ));
    assert_eq!(
        s3_refusal("      url: s3://b/p\n      access-key-id: AKIA\n"),
        ReplicaProblem::HalfCredentials
    );
    assert!(matches!(
        s3_refusal("      url: s3://arn:aws:s3:us-east-1:123:accesspoint/ap/db\n"),
        ReplicaProblem::AccessPoint
    ));
    assert!(matches!(
        s3_refusal("      url: s3://b/p?partSize=big\n"),
        ReplicaProblem::QueryNotNumber { .. }
    ));
    // Go's `ExplicitTypeWithEndpointLikeURL`.
    assert_eq!(
        s3_refusal("      type: s3\n      url: rook-ceph-rgw:8080\n      path: path\n"),
        ReplicaProblem::S3UrlAndPath
    );
}

/// Go accepts the signing and MD5 query keys; litestream-rs notes they change nothing.
/// `concurrency` is honoured, so it adds no note.
#[test]
fn no_effect_query_keys_are_noted() {
    let plan = planned(
        "dbs:\n  - path: /data/app.db\n    replica:\n      url: s3://b/p?signPayload=false&concurrency=8\n",
    );
    assert_eq!(plan.notes().len(), 1, "{:#?}", plan.notes());
}

/// The replica's `concurrency` wins over the URL's; zero leaves Go's default
/// of 5 parts (R2 2, see `S3Config::concurrency`).
#[test]
fn concurrency_reaches_the_store_config() {
    let of = |text: &str| match planned(text).dbs()[0].replica() {
        ReplicaPlan::S3 { config, .. } => config.concurrency().get(),
        other @ ReplicaPlan::File(_) => panic!("not S3: {other:?}"),
    };
    let base = "dbs:\n  - path: /data/app.db\n    replica:\n      url: s3://b/p";
    assert_eq!(of(&format!("{base}\n")), 5);
    assert_eq!(of(&format!("{base}?concurrency=3\n")), 3);
    assert_eq!(of(&format!("{base}?concurrency=0\n")), 5);
    assert_eq!(
        of(&format!("{base}?concurrency=3\n      concurrency: 5\n")),
        5
    );
}

#[test]
fn credentials_from_the_file() {
    let db = s3_with(
        "      url: s3://b/p\n      access-key-id: AKIA\n      secret-access-key: s3cr3t\n",
    );
    assert!(matches!(
        db.replica(),
        ReplicaPlan::S3 {
            credentials: Credentials::FromConfig(_),
            ..
        }
    ));
    let debug = format!("{db:?}");
    assert!(!debug.contains("s3cr3t"), "{debug}");
}

/// Go keeps the first of a duplicate and lets two databases share a replica.
#[test]
fn one_database_one_replica_one_meta_directory() {
    let problems = unmapped(
        "dbs:\n  - path: /a.db\n    replica:\n      path: /r/a\n  - path: /b.db\n    replica:\n      path: /r/a\n",
    );
    assert!(
        matches!(problems.as_slice(), [(at, MapProblemKind::SharedReplica { .. })] if at == "dbs[1]"),
        "{problems:#?}"
    );
    let problems = unmapped(
        "dbs:\n  - path: /a.db\n    replica:\n      url: s3://b/p\n  - path: /b.db\n    replica:\n      url: s3://b/p\n",
    );
    assert!(
        matches!(
            problems.as_slice(),
            [(_, MapProblemKind::SharedReplica { .. })]
        ),
        "{problems:#?}"
    );
    let problems = unmapped(
        "dbs:\n  - path: /a.db\n    meta-path: /m\n    replica:\n      path: /r/a\n  - path: /b.db\n    meta-path: /m\n    replica:\n      path: /r/b\n",
    );
    assert!(
        matches!(
            problems.as_slice(),
            [(_, MapProblemKind::SharedMetaDir { .. })]
        ),
        "{problems:#?}"
    );
}

#[test]
fn a_database_named_by_a_path_and_a_dir_is_refused() {
    let tree = Tree::new(&["app.db"]);
    let problems = unmapped(&format!(
        "dbs:\n  - path: {root}/app.db\n    replica:\n      path: /r/one\n  - dir: {root}\n    pattern: '*.db'\n    replica:\n      path: /r/dir\n",
        root = tree.root().display()
    ));
    assert!(
        matches!(problems.as_slice(), [(at, MapProblemKind::DuplicateDatabase { .. })] if at == "dbs[1]"),
        "{problems:#?}"
    );
}

#[test]
fn an_entry_without_a_replica_is_refused() {
    let problems = unmapped("dbs:\n  - path: /a.db\n");
    assert!(
        matches!(problems.as_slice(), [(_, MapProblemKind::NoReplica)]),
        "{problems:#?}"
    );
}

/// Go's positional `replicate DB_PATH REPLICA_URL`.
#[test]
fn the_positional_form_is_one_database() {
    let env = TestEnv::default();
    let loaded = GoConfig::for_one_database("app \"x\".db", "s3://b/p", &env).unwrap();
    let plan = plan(&loaded.config, &env).unwrap();
    let db = plan.dbs().first().unwrap();
    assert_eq!(db.path(), Path::new("/work/app \"x\".db"));
    assert_eq!(s3_config(db).prefix().as_str(), "p");
    GoConfig::for_one_database("a.db", "gs://b/p", &env).unwrap_err();
}

/// A directory of synthetic database files: a SQLite header is all the scan reads.
struct Tree(TempDir);

impl Tree {
    fn new(files: &[&str]) -> Self {
        let dir = TempDir::new().unwrap();
        for file in files {
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut bytes = b"SQLite format 3\0".to_vec();
            bytes.resize(4096, 0);
            std::fs::write(path, bytes).unwrap();
        }
        Self(dir)
    }

    fn root(&self) -> &Path {
        self.0.path()
    }

    fn plan(&self, entry: &str) -> Plan {
        planned(&format!(
            "dbs:\n  - dir: {}\n{entry}",
            self.root().display()
        ))
    }
}

fn replicas(plan: &Plan, root: &Path) -> Vec<(PathBuf, String)> {
    plan.dbs()
        .iter()
        .map(|db| {
            let replica = match db.replica() {
                ReplicaPlan::File(path) => path.display().to_string(),
                ReplicaPlan::S3 { config, .. } => {
                    format!("{}/{}", config.bucket().as_str(), config.prefix().as_str())
                }
            };
            (db.path().strip_prefix(root).unwrap().to_owned(), replica)
        })
        .collect()
}

/// Go's `TestNewDBsFromDirectoryConfig_SubdirectoryPaths` and `_UniquePaths`.
#[test]
fn a_recursive_dir_keeps_relative_paths_under_a_file_replica() {
    let tree = Tree::new(&[
        "db1.db",
        "team-a/db2.db",
        "team-b/nested/db3.db",
        "notes.txt",
    ]);
    let plan =
        tree.plan("    pattern: '*.db'\n    recursive: true\n    replica:\n      path: /backup\n");
    assert_eq!(
        replicas(&plan, tree.root()),
        [
            (PathBuf::from("db1.db"), "/backup/db1.db".to_owned()),
            (
                PathBuf::from("team-a/db2.db"),
                "/backup/team-a/db2.db".to_owned()
            ),
            (
                PathBuf::from("team-b/nested/db3.db"),
                "/backup/team-b/nested/db3.db".to_owned()
            ),
        ]
    );
    let db = plan.dbs().get(1).unwrap();
    assert_eq!(db.meta_dir(), tree.root().join("team-a/.db2.db-litestream"));
}

/// `FindSQLiteDatabases` without `recursive`, and `IsSQLiteDatabase`.
#[test]
fn a_flat_dir_skips_subdirectories_and_files_that_are_not_databases() {
    let tree = Tree::new(&["a.db", "sub/b.db"]);
    std::fs::write(tree.root().join("fake.db"), b"not a database").unwrap();
    let plan = tree.plan("    pattern: '*.db'\n    replica:\n      path: /backup\n");
    assert_eq!(
        replicas(&plan, tree.root()),
        [(PathBuf::from("a.db"), "/backup/a.db".to_owned())]
    );
}

/// Go's `TestNewDBsFromDirectoryConfig_S3URL` and `_DuplicateFilenames`.
#[test]
fn a_dir_under_an_s3_url_appends_the_relative_path() {
    let tree = Tree::new(&["team-a/db.sqlite", "team-b/nested/db.sqlite"]);
    let plan = tree.plan(
        "    pattern: '*.sqlite'\n    recursive: true\n    replica:\n      url: s3://test-bucket/backups\n",
    );
    assert_eq!(
        replicas(&plan, tree.root()),
        [
            (
                PathBuf::from("team-a/db.sqlite"),
                "test-bucket/backups/team-a/db.sqlite".to_owned()
            ),
            (
                PathBuf::from("team-b/nested/db.sqlite"),
                "test-bucket/backups/team-b/nested/db.sqlite".to_owned()
            ),
        ]
    );
    let plan = tree.plan(
        "    pattern: '*.sqlite'\n    recursive: true\n    replica:\n      type: s3\n      bucket: test-bucket\n      path: backups\n",
    );
    assert_eq!(replicas(&plan, tree.root()).len(), 2);
}

/// Go's `TestNewDBsFromDirectoryConfig_ReplicasArrayURL`.
#[test]
fn a_dir_with_the_deprecated_replicas_list() {
    let tree = Tree::new(&["db1.sqlite", "subs/db2.sqlite"]);
    let plan = tree.plan(
        "    pattern: '*.sqlite'\n    recursive: true\n    replicas:\n      - url: s3://legacy-bucket/base\n",
    );
    assert_eq!(
        replicas(&plan, tree.root()),
        [
            (
                PathBuf::from("db1.sqlite"),
                "legacy-bucket/base/db1.sqlite".to_owned()
            ),
            (
                PathBuf::from("subs/db2.sqlite"),
                "legacy-bucket/base/subs/db2.sqlite".to_owned()
            ),
        ]
    );
}

/// Go's `TestNewDBsFromDirectoryConfig_MetaPathPerDatabase`.
#[test]
fn a_dir_with_meta_path_gives_each_database_its_own() {
    let tree = Tree::new(&["primary.db", "team/nested/analytics.db"]);
    let plan = tree.plan(
        "    pattern: '*.db'\n    recursive: true\n    meta-path: ~/meta-root\n    replica:\n      path: /backup\n",
    );
    let metas: Vec<_> = plan
        .dbs()
        .iter()
        .map(|db| db.meta_dir().to_owned())
        .collect();
    assert_eq!(
        metas,
        [
            PathBuf::from("/home/u/meta-root/.primary.db-litestream"),
            PathBuf::from("/home/u/meta-root/team/nested/.analytics.db-litestream"),
        ]
    );
}

/// Go's `TestNewDBsFromDirectoryConfig_MetaDir`: no leading dot under `meta-dir`.
#[test]
fn a_dir_with_meta_dir_mirrors_the_tree() {
    let tree = Tree::new(&["tenant-a/data.metadata", "tenant-b/nested/data.metadata"]);
    let plan = tree.plan(
        "    pattern: '*.metadata'\n    recursive: true\n    meta-dir: /state\n    replica:\n      path: /replicas\n",
    );
    let metas: Vec<_> = plan
        .dbs()
        .iter()
        .map(|db| db.meta_dir().to_owned())
        .collect();
    assert_eq!(
        metas,
        [
            PathBuf::from("/state/tenant-a/data.metadata-litestream"),
            PathBuf::from("/state/tenant-b/nested/data.metadata-litestream"),
        ]
    );
}

/// Go's `TestNewDBsFromDirectoryConfig_EmptyBasePath` and `_SpecialCharacters`.
#[test]
fn a_dir_under_a_file_replica_with_no_path_and_odd_names() {
    let tree = Tree::new(&["my database.db", "tenant#1.db"]);
    let plan = tree.plan("    pattern: '*.db'\n    replica:\n      type: file\n");
    assert_eq!(
        replicas(&plan, tree.root()),
        [
            (
                PathBuf::from("my database.db"),
                "/work/my database.db".to_owned()
            ),
            (PathBuf::from("tenant#1.db"), "/work/tenant#1.db".to_owned()),
        ]
    );
}

/// Go's `TestNewDBsFromDirectoryConfig_EmptyDirectoryRequiresDatabases`.
#[test]
fn a_dir_that_finds_nothing_is_refused() {
    let tree = Tree::new(&[]);
    let problems = unmapped(&format!(
        "dbs:\n  - dir: {}\n    pattern: '*.db'\n    replica:\n      path: /backup\n",
        tree.root().display()
    ));
    assert!(
        matches!(
            problems.as_slice(),
            [(
                _,
                MapProblemKind::Dir(lsr_config::DirProblem::NoDatabases { .. })
            )]
        ),
        "{problems:#?}"
    );
}

/// Go's `TestNewDBsFromDirectoryConfig_EmptyDirectoryWithWatch`: litestream-rs refuses `watch`.
#[test]
fn watching_a_dir_is_refused_at_load() {
    let problems = refused(
        "dbs:\n  - dir: /data\n    pattern: '*.db'\n    watch: true\n    replica:\n      path: /backup\n",
    );
    assert!(
        problems
            .iter()
            .any(|p| p.path.to_string() == "dbs[0].watch"),
        "{problems:#?}"
    );
}

/// Go's `filepath.Match` on the file name.
#[test]
fn patterns_are_go_s_glob() {
    let tree = Tree::new(&["a1.db", "b2.db", "c.db", "a1.sqlite"]);
    let plan = tree.plan("    pattern: '[ab]?.db'\n    replica:\n      path: /backup\n");
    let names: Vec<_> = replicas(&plan, tree.root())
        .into_iter()
        .map(|(p, _)| p)
        .collect();
    assert_eq!(names, [PathBuf::from("a1.db"), PathBuf::from("b2.db")]);
    let problems = unmapped(&format!(
        "dbs:\n  - dir: {}\n    pattern: '[a'\n    replica:\n      path: /backup\n",
        tree.root().display()
    ));
    assert!(
        matches!(
            problems.as_slice(),
            [(
                _,
                MapProblemKind::Dir(lsr_config::DirProblem::BadPattern { .. })
            )]
        ),
        "{problems:#?}"
    );
}

fn dir_problems(dir: &Path, pattern: &str) -> Vec<lsr_config::DirProblem> {
    unmapped(&format!(
        "dbs:\n  - dir: {}\n    pattern: '{pattern}'\n    replica:\n      path: /backup\n",
        dir.display()
    ))
    .into_iter()
    .map(|(_, kind)| {
        let MapProblemKind::Dir(problem) = kind else {
            panic!("not a dir problem: {kind:?}")
        };
        problem
    })
    .collect()
}

/// Go's `filepath.Walk` looks at its root with `lstat`: a `dir` that is a symbolic link to a
/// directory is matched as a file, its header read fails, and Go finds nothing.
#[test]
fn a_dir_that_is_a_symbolic_link_finds_nothing_as_go() {
    let tree = Tree::new(&["a.db"]);
    let outer = TempDir::new().unwrap();
    let link = outer.path().join("link");
    std::os::unix::fs::symlink(tree.root(), &link).unwrap();
    assert!(
        matches!(
            dir_problems(&link, "*").as_slice(),
            [lsr_config::DirProblem::NoDatabases { .. }]
        ),
        "a linked dir must find nothing"
    );
}

/// A `dir` naming a database file is that database at relative path `.`: the replica path
/// is used as written and the meta directory is Go's default beside the file.
#[test]
fn a_dir_that_is_a_database_file_is_that_one_database() {
    let tree = Tree::new(&["lone.db"]);
    let file = tree.root().join("lone.db");
    let plan = planned(&format!(
        "dbs:\n  - dir: {}\n    pattern: '*.db'\n    replica:\n      path: /backup\n",
        file.display()
    ));
    let [db] = plan.dbs() else {
        panic!("{plan:#?}")
    };
    assert_eq!(db.path(), file);
    assert_eq!(db.meta_dir(), tree.root().join(".lone.db-litestream"));
    assert!(matches!(db.replica(), ReplicaPlan::File(path) if path == Path::new("/backup")));
}

/// Go's `IsSQLiteDatabase` makes one `read` into a zeroed buffer: a 15-byte file holding
/// `SQLite format 3` matches (its 16th byte counts as the NUL), an empty one does not.
#[test]
fn a_short_header_read_counts_the_missing_bytes_as_zero() {
    let tree = Tree::new(&[]);
    std::fs::write(tree.root().join("short.db"), b"SQLite format 3").unwrap();
    std::fs::write(tree.root().join("empty.db"), b"").unwrap();
    std::fs::write(tree.root().join("fourteen.db"), b"SQLite format ").unwrap();
    let plan = tree.plan("    pattern: '*.db'\n    replica:\n      path: /backup\n");
    let names: Vec<_> = replicas(&plan, tree.root())
        .into_iter()
        .map(|(p, _)| p)
        .collect();
    assert_eq!(names, [PathBuf::from("short.db")]);
}

/// Go's `filepath.Match` reports a malformed pattern only for a name that reaches the bad
/// part, and its walk fails at that file: `a*[` scans a directory of `b.db` normally (and
/// finds nothing) but fails on `ab.db`; `[-a]*` is malformed for every name.
#[test]
fn a_malformed_pattern_fails_the_scan_only_when_a_name_reaches_it() {
    let quiet = Tree::new(&["b.db"]);
    assert!(
        matches!(
            dir_problems(quiet.root(), "a*[").as_slice(),
            [lsr_config::DirProblem::NoDatabases { .. }]
        ),
        "no name reaches the bad part"
    );
    let loud = Tree::new(&["ab.db"]);
    assert!(matches!(
        dir_problems(loud.root(), "a*[").as_slice(),
        [lsr_config::DirProblem::BadPattern { path, .. }] if path.ends_with("ab.db")
    ));
    let dash = Tree::new(&["-x.db"]);
    assert!(matches!(
        dir_problems(dash.root(), "[-a]*").as_slice(),
        [lsr_config::DirProblem::BadPattern { .. }]
    ));
}

/// A database's relative path reaches a URL replica as Go's `url.URL` carries it: a
/// backslash stays a backslash (the `url` crate reads it as `/` in a `file` URL) and `%41`
/// stays three characters.
#[test]
fn odd_names_reach_a_url_replica_unchanged() {
    let tree = Tree::new(&["a\\b.db", "a%41.db"]);
    let plan = tree.plan("    pattern: '*.db'\n    replica:\n      url: file:///backup\n");
    let got: Vec<_> = replicas(&plan, tree.root())
        .into_iter()
        .map(|(_, replica)| replica)
        .collect();
    assert_eq!(got, ["/backup/a%41.db", "/backup/a\\b.db"]);
}

/// Go's `meta-path` is a `*string`: `""` is a value, and `NewDBFromConfig` expands it with
/// `filepath.Abs("")`, the current directory. Null leaves the default beside the database.
#[test]
fn an_empty_meta_path_is_the_current_directory_as_go() {
    let db = one(&db_with("    meta-path: \"\"\n"));
    assert_eq!(db.meta_dir(), Path::new("/work"));
    let db = one(&db_with("    meta-path: ~\n"));
    assert_eq!(db.meta_dir(), Path::new("/data/.app.db-litestream"));
}
