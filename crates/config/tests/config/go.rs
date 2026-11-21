//! Go's config tests in `cmd/litestream/main_test.go` at 041a8c3, one Rust test
//! per Go scenario, each with Go's expected values or Go's error. Where v1
//! refuses at load what Go refuses only when it starts the database, or what
//! Go accepts and ignores, the test says so.

use std::path::Path;

use lsr_config::{
    AppReaders, DbSource, Expand, GoConfig, GoDuration, KeyClass, ProblemKind, ReplicaKind,
    WarningKind, defaults,
};

use crate::common::{TestEnv, config, parse, problem_at, refused, refused_with, warnings_at};

fn secs(s: i64) -> GoDuration {
    GoDuration::from_secs(s)
}

const MIN: i64 = 60;
const HOUR: i64 = 3600;

// TestOpenConfigFile, TestReadConfig/ReadsFromFile

#[test]
fn load_reads_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("litestream.yml");
    std::fs::write(&path, "dbs:\n  - path: /tmp/db\n").unwrap();
    let loaded = GoConfig::load(&path, Expand::Env, &TestEnv::default()).unwrap();
    assert_eq!(loaded.config.dbs()[0].path(), Some(Path::new("/tmp/db")));
}

#[test]
fn a_missing_file_is_not_found() {
    let error = GoConfig::load(
        Path::new("/nonexistent/file.yml"),
        Expand::Env,
        &TestEnv::default(),
    )
    .unwrap_err();
    assert!(error.is_not_found(), "{error}");
}

// TestReadConfigFile

#[test]
fn global_credentials_propagate_into_replicas() {
    let config = config(
        "access-key-id: XXX\nsecret-access-key: YYY\n\ndbs:\n  - path: /path/to/db\n    replicas:\n      - url: s3://foo/bar\n",
    );
    let s3 = config.dbs()[0].replica().unwrap().s3();
    assert_eq!(s3.access_key_id().unwrap().expose(), "XXX");
    assert_eq!(s3.secret_access_key().unwrap().expose(), "YYY");
}

#[test]
fn variables_expand_and_unset_ones_are_empty() {
    let env = TestEnv::with(&[
        ("LITESTREAM_TEST_0129380", "/path/to/db"),
        ("LITESTREAM_TEST_1872363", "s3://foo/bar"),
    ]);
    let text = "dbs:\n  - path: $LITESTREAM_TEST_0129380\n    replica:\n      url: ${LITESTREAM_TEST_1872363}\n      bucket: ${LITESTREAM_TEST_NO_SUCH_ENV}\n";
    let loaded = GoConfig::parse(text, Expand::Env, &env).unwrap();
    let db = &loaded.config.dbs()[0];
    assert_eq!(db.path(), Some(Path::new("/path/to/db")));
    let replica = db.replica().unwrap();
    assert_eq!(replica.url(), Some("s3://foo/bar"));
    assert_eq!(replica.s3().bucket(), None);
    let unset = WarningKind::UnsetVariable {
        name: "LITESTREAM_TEST_NO_SUCH_ENV".to_owned(),
    };
    assert!(loaded.report.warnings.iter().any(|w| w.kind == unset));
}

#[test]
fn no_expand_env_keeps_the_text() {
    let env = TestEnv::with(&[("LITESTREAM_TEST_9847533", "s3://foo/bar")]);
    let text = "dbs:\n  - path: /path/to/db\n    replica:\n      url: ${LITESTREAM_TEST_9847533}\n";
    let loaded = GoConfig::parse(text, Expand::Off, &env).unwrap();
    assert_eq!(
        loaded.config.dbs()[0].replica().unwrap().url(),
        Some("${LITESTREAM_TEST_9847533}")
    );
}

#[test]
fn directory_entry_keeps_its_meta_dir() {
    let config = config(
        "dbs:\n  - dir: /path/to/dbs\n    pattern: \"*.db\"\n    recursive: true\n    meta-dir: /path/to/litestream-state\n    replica:\n      url: file:///path/to/replicas\n",
    );
    let DbSource::Dir(dir) = config.dbs()[0].source() else {
        panic!("not a dir")
    };
    assert_eq!(dir.dir(), "/path/to/dbs");
    assert_eq!(dir.pattern(), "*.db");
    assert!(dir.recursive());
    assert_eq!(dir.meta_dir(), Some("/path/to/litestream-state"));
}

// TestParseConfig_StrictWarnings: Go warns, v1 refuses, and suggests.

#[test]
fn keys_go_does_not_know_are_refused_where_go_warns() {
    type Expected<'a> = &'a [(&'a str, Option<&'a str>)];
    let cases: &[(&str, Expected<'_>)] = &[
        (
            "definitely-not-a-setting: true\ndbs:\n  - path: /tmp/x.db\n    replica:\n      path: /tmp/xrep\n",
            &[("definitely-not-a-setting", None)],
        ),
        (
            "dbs:\n  - path: /tmp/x.db\n    replica:\n      path: /tmp/xrep\n      retention: 72h\n",
            &[("dbs[0].replica.retention", None)],
        ),
        (
            "levels:\n  - interval: 5m\n    retention: 1h\n  - interval: 1h\n",
            &[("levels[0].retention", None)],
        ),
        (
            "dbs:\n  - path: /tmp/x.db\n    snapshot:\n      intervall: 1h\n    replica:\n      path: /tmp/xrep\n  - path: /tmp/y.db\n    replicas:\n      - path: /tmp/yrep\n        typo-here: 1\nlogging:\n  lvl: debug\n",
            &[
                ("dbs[0].snapshot.intervall", Some("interval")),
                ("dbs[1].replicas[0].typo-here", None),
                ("logging.lvl", Some("level")),
            ],
        ),
    ];
    for (text, expected) in cases {
        let problems = refused(text);
        assert_eq!(problems.len(), expected.len(), "{problems:#?}");
        for (path, suggestion) in *expected {
            assert_eq!(
                problem_at(&problems, path),
                ProblemKind::Unknown {
                    suggestion: *suggestion
                },
                "{path}"
            );
        }
    }
}

#[test]
fn a_duplicate_key_is_refused_where_go_warns() {
    let problems = refused(
        "dbs:\n  - path: /tmp/x.db\n    replica:\n      path: /tmp/xrep\n      path: /tmp/other\n",
    );
    assert_eq!(problems[0].kind, ProblemKind::Duplicate { first_line: 4 });
    assert_eq!(problems[0].line, Some(5));
}

#[test]
fn the_strict_warnings_valid_case_has_no_unknown_keys() {
    let loaded = parse(
        "access-key-id: XXX\nsync-interval: 5s\nlevels:\n  - interval: 5m\nsnapshot:\n  interval: 1h\n  retention: 24h\nlogging:\n  level: info\ndbs:\n  - path: /tmp/x.db\n    snapshot:\n      interval: 2h\n    replica:\n      url: s3://foo/bar\n      part-size: 5MB\n      age:\n        identities: []\n",
    );
    assert!(
        loaded
            .report
            .keys
            .iter()
            .all(|(_, class)| *class != KeyClass::Unknown)
    );
    assert_eq!(
        loaded.config.dbs()[0].replica().unwrap().s3().part_size(),
        Some(5_000_000)
    );
}

// TestNewDBFromConfig_MetaDir, TestDBConfigValidation

#[test]
fn database_entry_validation() {
    let cases = [
        (
            "dbs:\n  - path: /path/to/db.sqlite\n    dir: /path/to/dir\n",
            "dbs[0]",
            ProblemKind::PathAndDir,
        ),
        ("dbs:\n  - {}\n", "dbs[0]", ProblemKind::NeitherPathNorDir),
        (
            "dbs:\n  - dir: /path/to/dir\n",
            "dbs[0]",
            ProblemKind::PatternRequired,
        ),
        (
            "dbs:\n  - path: /path/to/db.sqlite\n    meta-dir: /path/to/meta\n",
            "dbs[0]",
            ProblemKind::MetaDirWithoutDir,
        ),
        (
            "dbs:\n  - dir: /path/to/dir\n    pattern: \"*.db\"\n    meta-path: /m\n    meta-dir: /d\n",
            "dbs[0]",
            ProblemKind::MetaPathAndMetaDir,
        ),
        (
            "dbs:\n  - path: /path/to/db.sqlite\n  - path: /path/to/db.sqlite\n",
            "dbs[1].path",
            ProblemKind::DuplicatePath { first: 0 },
        ),
        (
            "dbs:\n  - path: /path/to/db.sqlite\n  - path: /path/to/../to/db.sqlite\n",
            "dbs[1].path",
            ProblemKind::DuplicatePath { first: 0 },
        ),
    ];
    for (text, path, kind) in cases {
        assert_eq!(problem_at(&refused(text), path), kind, "{text}");
    }
}

#[test]
fn distinct_paths_and_directories_are_valid() {
    let config = config(
        "dbs:\n  - path: /path/to/a.sqlite\n  - path: /path/to/b.sqlite\n  - dir: /path/to/dir\n    pattern: \"*.db\"\n  - dir: /path/to/dir\n    pattern: \"*.sqlite\"\n    recursive: true\n",
    );
    assert_eq!(config.dbs().len(), 4);
}

// TestNewReplicaFromConfig_AgeEncryption

#[test]
fn age_settings_are_refused_and_an_empty_age_is_accepted() {
    for text in [
        "dbs:\n  - path: /db\n    replica:\n      url: s3://foo/bar\n      age:\n        identities: [AGE-SECRET-KEY-1EXAMPLE]\n",
        "dbs:\n  - path: /db\n    replica:\n      url: s3://foo/bar\n      age:\n        recipients: [age1example]\n",
    ] {
        let problems = refused(text);
        assert!(
            matches!(
                problems[0].kind,
                ProblemKind::NotInV1 {
                    feature: "age encryption"
                }
            ),
            "{problems:?}"
        );
        assert!(!problems[0].to_string().contains("AGE-SECRET"));
    }
    let both = refused(
        "dbs:\n  - path: /db\n    replica:\n      url: s3://foo/bar\n      age:\n        identities: [a]\n        recipients: [b]\n",
    );
    assert_eq!(both.len(), 2);
    config("dbs:\n  - path: /db\n    replica:\n      url: s3://foo/bar\n");
}

// TestConfig_Validate_SnapshotIntervals

#[test]
fn snapshot_settings_parse() {
    let snapshot = config("snapshot:\n  interval: 1h\n  retention: 24h\n").snapshot();
    assert_eq!(
        (snapshot.interval(), snapshot.retention()),
        (secs(HOUR), secs(24 * HOUR))
    );
}

#[test]
fn a_database_snapshot_is_promoted_when_no_global_one_is_set() {
    let config = config(
        "dbs:\n  - path: /tmp/test.db\n    snapshot:\n      interval: 10m\n      retention: 2h\n",
    );
    let own = config.dbs()[0].snapshot();
    assert_eq!(
        (own.interval(), own.retention()),
        (Some(secs(10 * MIN)), Some(secs(2 * HOUR)))
    );
    assert_eq!(
        (config.snapshot().interval(), config.snapshot().retention()),
        (secs(10 * MIN), secs(2 * HOUR))
    );
}

#[test]
fn the_global_snapshot_takes_precedence() {
    let config = config(
        "snapshot:\n  interval: 1h\n  retention: 24h\ndbs:\n  - path: /tmp/test.db\n    snapshot:\n      interval: 10m\n      retention: 2h\n",
    );
    assert_eq!(
        (config.snapshot().interval(), config.snapshot().retention()),
        (secs(HOUR), secs(24 * HOUR))
    );
}

#[test]
fn snapshot_durations_must_be_positive() {
    let cases = [
        (
            "snapshot:\n  interval: 0s\n  retention: 24h\n",
            "snapshot.interval",
        ),
        (
            "snapshot:\n  interval: 1h\n  retention: 0s\n",
            "snapshot.retention",
        ),
        (
            "snapshot:\n  interval: -1h\n  retention: 24h\n",
            "snapshot.interval",
        ),
        (
            "dbs:\n  - path: /tmp/test.db\n    snapshot:\n      interval: 0s\n",
            "dbs[0].snapshot.interval",
        ),
    ];
    for (text, path) in cases {
        assert!(
            matches!(
                problem_at(&refused(text), path),
                ProblemKind::NotPositive { .. }
            ),
            "{text}"
        );
    }
}

#[test]
fn retention_shorter_than_interval_warns() {
    let loaded = parse("snapshot:\n  interval: 1h\n  retention: 30m\n");
    assert_eq!(loaded.config.snapshot().retention(), secs(30 * MIN));
    let want = WarningKind::RetentionShorterThanInterval {
        retention: secs(30 * MIN),
        interval: secs(HOUR),
    };
    assert_eq!(
        warnings_at(&loaded.report.warnings, "snapshot.retention"),
        [&want]
    );
    // Go's own case starts the file with one space before `dbs:`.
    let loaded = parse(
        " dbs:\n  - path: /tmp/test.db\n    snapshot:\n      interval: 1h\n      retention: 30m\n",
    );
    assert_eq!(
        loaded.config.dbs()[0].snapshot().retention(),
        Some(secs(30 * MIN))
    );
    assert_eq!(
        warnings_at(&loaded.report.warnings, "dbs[0].snapshot.retention"),
        [&want]
    );
}

#[test]
fn conflicting_database_snapshot_intervals_are_refused() {
    let problems = refused(
        "dbs:\n  - path: /tmp/a.db\n    snapshot:\n      interval: 10m\n  - path: /tmp/b.db\n    snapshot:\n      interval: 20m\n",
    );
    let want = ProblemKind::ConflictingSnapshot {
        what: "intervals",
        first: 0,
        first_value: secs(10 * MIN),
        value: secs(20 * MIN),
    };
    assert_eq!(problem_at(&problems, "dbs[1].snapshot.interval"), want);
    assert!(
        problems[0]
            .to_string()
            .contains("conflicting database snapshot intervals")
    );
}

#[test]
fn snapshot_defaults_apply_when_absent_or_empty() {
    for text in [
        "# snapshot section not specified\ndbs:\n  - path: /tmp/test.db\n",
        "snapshot:\n",
        "",
    ] {
        let snapshot = config(text).snapshot();
        assert_eq!(
            (snapshot.interval(), snapshot.retention()),
            (secs(24 * HOUR), secs(24 * HOUR)),
            "{text:?}"
        );
    }
}

// TestConfig_Validate_ValidationInterval: the scheduled verify's interval.

#[test]
fn a_validation_interval_is_read_and_its_absence_is_none() {
    assert_eq!(
        config("validation:\n  interval: 5m\n").validation_interval(),
        Some(secs(5 * 60))
    );
    assert_eq!(
        config("dbs:\n  - path: /tmp/test.db\n").validation_interval(),
        None
    );
    assert_eq!(config("validation: {}\n").validation_interval(), None);
}

// TestConfig_Validate_L0Retention

#[test]
fn l0_retention_rules() {
    assert_eq!(
        config("l0-retention: 0s\n").l0_retention(),
        GoDuration::ZERO
    );
    let problems = refused("l0-retention: -1s\n");
    assert!(matches!(
        problem_at(&problems, "l0-retention"),
        ProblemKind::Negative { .. }
    ));
    let problems = refused("l0-retention-check-interval: 0s\n");
    assert!(matches!(
        problem_at(&problems, "l0-retention-check-interval"),
        ProblemKind::NotPositive { .. }
    ));
    let defaulted =
        config("dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n");
    assert_eq!(defaulted.l0_retention(), defaults::L0_RETENTION);
    assert_eq!(
        defaulted.l0_retention_check_interval(),
        defaults::L0_RETENTION_CHECK_INTERVAL
    );
}

// TestConfig_Validate_SyncIntervals

#[test]
fn sync_interval_rules() {
    let config = config(
        "dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n      sync-interval: 30s\n",
    );
    assert_eq!(
        config.dbs()[0].replica().unwrap().sync_interval(),
        Some(secs(30))
    );
    for value in ["0s", "-30s"] {
        let problems = refused(&format!(
            "dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n      sync-interval: {value}\n"
        ));
        assert!(matches!(
            problem_at(&problems, "dbs[0].replica.sync-interval"),
            ProblemKind::NotPositive { .. }
        ));
    }
    let unset = crate::common::config(
        "dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n",
    );
    assert_eq!(unset.dbs()[0].replica().unwrap().sync_interval(), None);
}

#[test]
fn two_replicas_are_refused_and_each_sync_interval_is_checked() {
    // Go's parse accepts two replicas and refuses them when it starts the database.
    let problems = refused(
        "dbs:\n  - path: /tmp/test.db\n    replicas:\n      - url: file:///tmp/replica1\n        sync-interval: 30s\n      - url: file:///tmp/replica2\n        sync-interval: 0s\n",
    );
    assert!(matches!(
        problem_at(&problems, "dbs[0].replicas[1].sync-interval"),
        ProblemKind::NotPositive { .. }
    ));
    assert_eq!(
        problem_at(&problems, "dbs[0].replicas"),
        ProblemKind::MultipleReplicas
    );
    let problems = refused(
        "dbs:\n  - path: /tmp/test.db\n    replicas:\n      - url: file:///tmp/replica1\n        sync-interval: 30s\n      - url: file:///tmp/replica2\n        sync-interval: 1m\n",
    );
    assert_eq!(problems.len(), 1);
    assert_eq!(
        problem_at(&problems, "dbs[0].replicas"),
        ProblemKind::MultipleReplicas
    );
}

// TestReplicaConfig_MaxSyncLTXFiles

#[test]
fn max_sync_ltx_files_is_read_and_inherited() {
    let replica = |text: &str| {
        config(text).dbs()[0]
            .replica()
            .unwrap()
            .max_sync_ltx_files()
    };
    assert_eq!(
        replica(
            "dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n      max-sync-ltx-files: 32\n"
        ),
        Some(32)
    );
    assert_eq!(
        replica("dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n"),
        None
    );
    assert_eq!(
        replica(
            "max-sync-ltx-files: 16\ndbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n"
        ),
        Some(16)
    );
}

// TestConfig_Validate_CompactionLevels, TestConfig_DefaultValues

#[test]
fn compaction_levels() {
    assert_eq!(
        config("levels:\n  - interval: 5m\n  - interval: 1h\n").levels(),
        [secs(5 * MIN), secs(HOUR)]
    );
    assert_eq!(
        config("levels:\n  - interval: 10m\n  - interval: 30m\n  - interval: 2h\n").levels(),
        [secs(10 * MIN), secs(30 * MIN), secs(2 * HOUR)]
    );
    assert_eq!(
        config("# levels section not specified\ndbs:\n  - path: /tmp/test.db\n").levels(),
        defaults::LEVELS
    );
    assert!(matches!(
        problem_at(
            &refused("levels:\n  - interval: 0s\n  - interval: 1h\n"),
            "levels[0].interval"
        ),
        ProblemKind::NotPositive { .. }
    ));
    assert!(matches!(
        problem_at(
            &refused("levels:\n  - interval: 5m\n  - interval: -1h\n"),
            "levels[1].interval"
        ),
        ProblemKind::NotPositive { .. }
    ));
}

#[test]
fn an_empty_file_has_go_defaults() {
    let config = config("");
    assert_eq!(config.snapshot().interval(), defaults::SNAPSHOT_INTERVAL);
    assert_eq!(config.snapshot().retention(), defaults::SNAPSHOT_RETENTION);
    assert_eq!(config.levels(), defaults::LEVELS);
    assert_eq!(
        config.shutdown_sync_timeout(),
        defaults::SHUTDOWN_SYNC_TIMEOUT
    );
    assert!(config.retention_enabled());
    assert_eq!(config.addr(), None);
    assert_eq!(config.rust().app_readers(), AppReaders::Unknown);
    assert!(config.dbs().is_empty());
}

// TestS3ReplicaConfig_PartSizeAndConcurrency

#[test]
fn part_size_in_iec_and_si_units_and_concurrency() {
    let base = "dbs:\n  - path: /path/to/db\n    replicas:\n      - type: s3\n        bucket: mybucket\n        path: mypath\n        region: us-east-1\n";
    let part = |extra: &str| {
        parse(&format!("{base}{extra}")).config.dbs()[0]
            .replica()
            .unwrap()
            .s3()
            .part_size()
    };
    assert_eq!(part("        part-size: 5MiB\n"), Some(5 * 1024 * 1024));
    assert_eq!(part("        part-size: 10MB\n"), Some(10_000_000));
    assert_eq!(part(""), None);
    let loaded = parse(&format!(
        "{base}        part-size: 5MiB\n        concurrency: 10\n"
    ));
    let replica = loaded.config.dbs()[0].replica().unwrap();
    assert_eq!(replica.kind(), ReplicaKind::S3);
    assert_eq!(replica.s3().bucket(), Some("mybucket"));
    assert_eq!(replica.s3().concurrency(), Some(10));
    assert!(warnings_at(&loaded.report.warnings, "dbs[0].replicas[0].concurrency").is_empty());
    let zero = parse(&format!(
        "{base}        concurrency: 0
"
    ));
    assert_eq!(
        zero.config.dbs()[0].replica().unwrap().s3().concurrency(),
        None
    );
}

// TestDBConfig_CheckpointFields, TestDBConfig_MaxSyncWALBytes

#[test]
fn checkpoint_fields() {
    let db = |text: &str| config(text).dbs()[0].clone();
    let both = db(
        "dbs:\n  - path: /tmp/test.db\n    min-checkpoint-page-count: 2000\n    truncate-page-n: 100000\n    replica:\n      url: file:///tmp/replica\nlitestream-rs:\n  app-readers: none\n",
    );
    assert_eq!(
        (both.min_checkpoint_page_count(), both.truncate_page_n()),
        (Some(2000), Some(100_000))
    );
    let unset = db("dbs:\n  - path: /tmp/test.db\n    replica:\n      url: file:///tmp/replica\n");
    assert_eq!(
        (unset.min_checkpoint_page_count(), unset.truncate_page_n()),
        (None, None)
    );
    // Without the claim that the application holds no long readers, v1 never truncates.
    let problems = refused(
        "dbs:\n  - path: /tmp/test.db\n    truncate-page-n: 100000\n    replica:\n      url: file:///tmp/replica\n",
    );
    assert_eq!(
        problem_at(&problems, "dbs[0].truncate-page-n"),
        ProblemKind::TruncateNeedsClaim
    );
}

#[test]
fn max_sync_wal_bytes() {
    let db = |text: &str| config(text).dbs()[0].max_sync_wal_bytes();
    assert_eq!(
        db("dbs:\n  - path: /tmp/test.db\n    max-sync-wal-bytes: 16777216\n"),
        Some(16_777_216)
    );
    assert_eq!(db("dbs:\n  - path: /tmp/test.db\n"), None);
}

// TestGlobalDefaults

#[test]
fn global_replica_defaults() {
    let config = config(
        "# Global defaults for all replicas\naccess-key-id: GLOBAL_ACCESS_KEY\nsecret-access-key: GLOBAL_SECRET_KEY\nregion: us-west-2\nendpoint: custom.s3.endpoint.com\nsync-interval: 30s\n\ndbs:\n  - path: /tmp/db1.sqlite\n    replica:\n      type: s3\n      bucket: my-bucket-1\n\n  - path: /tmp/db2.sqlite\n    replica:\n      type: s3\n      bucket: my-bucket-2\n      region: us-east-1           # Override global region\n      access-key-id: CUSTOM_KEY   # Override global access key\n\n  - path: /tmp/db3.sqlite\n    replicas:\n      - type: s3\n        bucket: my-bucket-3\n",
    );
    let expect = |index: usize, key: &str, secret: &str, region: &str, bucket: &str| {
        let replica = config.dbs()[index].replica().unwrap();
        let s3 = replica.s3();
        assert_eq!(s3.access_key_id().unwrap().expose(), key);
        assert_eq!(s3.secret_access_key().unwrap().expose(), secret);
        assert_eq!(s3.region(), Some(region));
        assert_eq!(s3.endpoint(), Some("custom.s3.endpoint.com"));
        assert_eq!(s3.bucket(), Some(bucket));
        assert_eq!(replica.sync_interval(), Some(secs(30)));
    };
    expect(
        0,
        "GLOBAL_ACCESS_KEY",
        "GLOBAL_SECRET_KEY",
        "us-west-2",
        "my-bucket-1",
    );
    expect(
        1,
        "CUSTOM_KEY",
        "GLOBAL_SECRET_KEY",
        "us-east-1",
        "my-bucket-2",
    );
    expect(
        2,
        "GLOBAL_ACCESS_KEY",
        "GLOBAL_SECRET_KEY",
        "us-west-2",
        "my-bucket-3",
    );
}

#[test]
fn global_settings_of_other_backends_are_accepted_but_their_replica_types_are_not() {
    let text = "access-key-id: GLOBAL_S3_KEY\nsecret-access-key: GLOBAL_S3_SECRET\nregion: global-region\nendpoint: global.endpoint.com\naccount-name: global-abs-account\naccount-key: global-abs-key\nhost: global.sftp.host\nuser: global-sftp-user\npassword: global-sftp-pass\nsync-interval: 45s\n\ndbs:\n  - path: /tmp/s3.sqlite\n    replica:\n      type: s3\n      bucket: s3-bucket\n\n  - path: /tmp/abs.sqlite\n    replica:\n      type: abs\n      bucket: abs-container\n\n  - path: /tmp/sftp.sqlite\n    replica:\n      type: sftp\n      path: /backup/path\n";
    let problems = refused(text);
    assert_eq!(problems.len(), 2, "{problems:#?}");
    assert_eq!(
        problem_at(&problems, "dbs[1].replica.type"),
        ProblemKind::UnsupportedReplicaType {
            name: "abs".to_owned()
        }
    );
    assert_eq!(
        problem_at(&problems, "dbs[2].replica.type"),
        ProblemKind::UnsupportedReplicaType {
            name: "sftp".to_owned()
        }
    );
    let config = config(
        &text
            .replace("      type: abs\n", "      type: s3\n")
            .replace("      type: sftp\n", "      type: file\n"),
    );
    assert_eq!(
        config.dbs()[0].replica().unwrap().sync_interval(),
        Some(secs(45))
    );
}

// TestReadConfigFile_SQLiteConnectionString

#[test]
fn sqlite_connection_string_prefixes_are_stripped() {
    for prefix in ["sqlite3://", "sqlite://"] {
        let config = config(&format!(
            "dbs:\n  - path: {prefix}/tmp/dir/test.db\n    replicas:\n      - url: file:///tmp/dir/replica\n"
        ));
        assert_eq!(config.dbs()[0].path(), Some(Path::new("/tmp/dir/test.db")));
    }
}

#[test]
fn database_paths_expand_home_and_become_absolute() {
    let config = config("dbs:\n  - path: ~/db.sqlite\n  - path: data/app.db\n");
    assert_eq!(config.dbs()[0].path(), Some(Path::new("/home/u/db.sqlite")));
    assert_eq!(config.dbs()[1].path(), Some(Path::new("/work/data/app.db")));
}

#[test]
fn log_level_from_the_environment_wins() {
    let env = TestEnv::with(&[("LOG_LEVEL", "debug")]);
    let loaded = GoConfig::parse(
        "logging:\n  level: error\n  type: json\n  stderr: true\n",
        Expand::Off,
        &env,
    )
    .unwrap();
    let logging = loaded.config.logging();
    assert_eq!(logging.level(), lsr_config::LogLevel::Debug);
    assert_eq!(logging.format(), lsr_config::LogFormat::Json);
    assert!(logging.stderr());
    let problems = refused_with("", Expand::Off, &TestEnv::with(&[("LOG_LEVEL", "loud")]));
    assert_eq!(
        problem_at(&problems, "logging.level"),
        ProblemKind::LogLevelFromEnv {
            value: "loud".to_owned()
        }
    );
}
