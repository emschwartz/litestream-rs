//! The four key classes,
//! every out-of-scope Go feature at its off value and at its on value, and
//! secrets kept out of every message.

use lsr_config::{Expand, GoConfig, KeyClass, ProblemKind, ReplicaKind, WarningKind};
use proptest::prelude::*;

use crate::common::{TestEnv, config, parse, problem_at, refused, warnings_at};

const DB: &str = "dbs:\n  - path: /db\n    replica:\n      url: s3://bucket/db\n";

/// A file with `line` at the top level, then one database.
fn top(line: &str) -> String {
    format!("{line}\n{DB}")
}

/// One database whose replica also has `line` (indented under the replica).
fn in_replica(line: &str) -> String {
    format!("{DB}      {line}\n")
}

/// One database that also has `line` (indented under the database).
fn in_db(line: &str) -> String {
    format!("dbs:\n  - path: /db\n    {line}\n    replica:\n      url: s3://bucket/db\n")
}

#[test]
fn one_key_of_each_class() {
    // Implemented.
    let loaded = parse(&in_replica("type: s3"));
    assert_eq!(
        loaded.config.dbs()[0].replica().unwrap().kind(),
        ReplicaKind::S3
    );
    assert!(
        loaded
            .report
            .keys
            .iter()
            .any(|(p, c)| p.to_string() == "dbs[0].replica.type" && *c == KeyClass::Implemented)
    );
    // Off-only: accepted at its off value, refused naming the key otherwise.
    let loaded = parse(&top("socket:\n  enabled: false"));
    assert!(
        loaded
            .report
            .keys
            .iter()
            .any(|(p, c)| p.to_string() == "socket.enabled" && *c == KeyClass::OffOnly)
    );
    assert_eq!(
        problem_at(
            &refused("dbs:\n  - path: /db\n    replica:\n      type: gs\n      bucket: b\n"),
            "dbs[0].replica.type"
        ),
        ProblemKind::UnsupportedReplicaType {
            name: "gs".to_owned()
        }
    );
    // No effect: one warning.
    let loaded = parse(&in_db("busy-timeout: 5s"));
    assert!(matches!(
        warnings_at(&loaded.report.warnings, "dbs[0].busy-timeout")[..],
        [WarningKind::NoEffect { .. }]
    ));
    // Unknown: refused with the suggestion.
    let problems = refused(&in_replica("sync_interval: 1s"));
    assert_eq!(
        problem_at(&problems, "dbs[0].replica.sync_interval"),
        ProblemKind::Unknown {
            suggestion: Some("sync-interval")
        }
    );
    assert!(
        problems[0]
            .to_string()
            .contains("did you mean `sync-interval`?")
    );
}

/// Every Go feature litestream-rs lacks, its key path, a line at its off value and a line at its on value.
type Place = fn(&str) -> String;

const OUT_OF_SCOPE: &[(Place, &str, &str, &str)] = &[
    (
        top,
        "socket.enabled",
        "socket:\n  enabled: false\n  path: /var/run/litestream.sock\n  permissions: 0600",
        "socket:\n  enabled: true",
    ),
    (top, "mcp-addr", "mcp-addr: \"\"", "mcp-addr: \":3001\""),
    (
        top,
        "heartbeat-url",
        "heartbeat-url:",
        "heartbeat-url: https://hc.example/ping",
    ),
    (
        top,
        "verify-compaction",
        "verify-compaction: false",
        "verify-compaction: true",
    ),
    (
        top,
        "auto-recover",
        "auto-recover: false",
        "auto-recover: true",
    ),
    (
        top,
        "storage-class",
        "storage-class: \"\"",
        "storage-class: GLACIER_IR",
    ),
    (
        in_replica,
        "dbs[0].replica.auto-recover",
        "auto-recover: no",
        "auto-recover: yes",
    ),
    (
        in_replica,
        "dbs[0].replica.storage-class",
        "storage-class:",
        "storage-class: STANDARD_IA",
    ),
    (
        in_replica,
        "dbs[0].replica.sse-customer-algorithm",
        "sse-customer-algorithm: \"\"",
        "sse-customer-algorithm: AES256",
    ),
    (
        in_replica,
        "dbs[0].replica.sse-customer-key",
        "sse-customer-key:",
        "sse-customer-key: c2VjcmV0",
    ),
    (
        in_replica,
        "dbs[0].replica.sse-customer-key-path",
        "sse-customer-key-path:",
        "sse-customer-key-path: /k",
    ),
    (
        in_replica,
        "dbs[0].replica.sse-kms-key-id",
        "sse-kms-key-id:",
        "sse-kms-key-id: alias/k",
    ),
    (
        in_replica,
        "dbs[0].replica.age.identities",
        "age:\n        identities: []",
        "age:\n        identities: [k]",
    ),
    (in_db, "dbs[0].watch", "watch: false", "watch: true"),
];

#[test]
fn every_out_of_scope_feature_is_accepted_off_and_refused_on() {
    for (place, path, off, on) in OUT_OF_SCOPE {
        let off_text = place(off);
        if let Err(error) = GoConfig::parse(&off_text, Expand::Off, &TestEnv::default()) {
            panic!("{path} off refused:\n{off_text}\n{}", error.report());
        }
        let problems = refused(&place(on));
        assert!(
            matches!(problem_at(&problems, path), ProblemKind::NotInV1 { .. }),
            "{path}: {problems:#?}"
        );
    }
}

#[test]
fn every_other_replica_type_is_refused_by_type_and_by_url() {
    for name in ["gs", "abs", "sftp", "webdav", "nats", "oss"] {
        let by_type =
            format!("dbs:\n  - path: /db\n    replica:\n      type: {name}\n      bucket: b\n");
        let unsupported = ProblemKind::UnsupportedReplicaType {
            name: name.to_owned(),
        };
        assert_eq!(
            problem_at(&refused(&by_type), "dbs[0].replica.type"),
            unsupported
        );
        let by_url = format!("dbs:\n  - path: /db\n    replica:\n      url: {name}://host/path\n");
        assert_eq!(
            problem_at(&refused(&by_url), "dbs[0].replica.url"),
            unsupported
        );
    }
    let webdavs = refused("dbs:\n  - path: /db\n    replica:\n      url: webdavs://host/path\n");
    assert_eq!(
        problem_at(&webdavs, "dbs[0].replica.url"),
        ProblemKind::UnsupportedReplicaType {
            name: "webdav".to_owned()
        }
    );
    let unknown = refused("dbs:\n  - path: /db\n    replica:\n      type: ftp\n");
    assert_eq!(
        problem_at(&unknown, "dbs[0].replica.type"),
        ProblemKind::UnknownReplicaType {
            name: "ftp".to_owned()
        }
    );
    // The URL's scheme wins over `type`, as Go's `ReplicaType`.
    let kind =
        config("dbs:\n  - path: /db\n    replica:\n      type: gs\n      url: S3://bucket/db\n")
            .dbs()[0]
            .replica()
            .unwrap()
            .kind();
    assert_eq!(kind, ReplicaKind::S3);
    let kind = config("dbs:\n  - path: /db\n    replica:\n      path: /backups/db\n").dbs()[0]
        .replica()
        .unwrap()
        .kind();
    assert_eq!(kind, ReplicaKind::File);
}

#[test]
fn no_effect_knobs_warn_once_each() {
    let text = "shutdown-sync-interval: 1s\nshutdown-timeout: 10s\npart-size: 5MB\nvalidation-interval: 1h\nlogging:\n  source: true\ndbs:\n  - path: /db\n    busy-timeout: 5s\n    replica:\n      url: s3://bucket/db\n      name: primary\n      validation-interval: 10m\n      sign-payload: true\n      sign-accept-encoding: false\n      require-content-md5: false\n      put-object-content-md5: true\n      skip-verify: true\n";
    let loaded = parse(text);
    let paths = [
        "shutdown-sync-interval",
        "shutdown-timeout",
        "part-size",
        "validation-interval",
        "logging.source",
        "dbs[0].busy-timeout",
        "dbs[0].replica.name",
        "dbs[0].replica.validation-interval",
        "dbs[0].replica.sign-payload",
        "dbs[0].replica.sign-accept-encoding",
        "dbs[0].replica.require-content-md5",
        "dbs[0].replica.put-object-content-md5",
        "dbs[0].replica.skip-verify",
    ];
    for path in paths {
        let warnings = warnings_at(&loaded.report.warnings, path);
        assert!(
            matches!(warnings[..], [WarningKind::NoEffect { .. }]),
            "{path}: {warnings:?}"
        );
        assert!(
            loaded
                .report
                .keys
                .iter()
                .any(|(p, c)| p.to_string() == path && *c == KeyClass::NoEffect)
        );
    }
}

#[test]
fn other_backends_keys_do_nothing_on_an_s3_replica() {
    let loaded = parse(&in_replica(
        "password: x\n      host-key: ssh-ed25519 AAAA\n      root-cas: [/ca.pem]\n      timeout: 5s\n      jwt: j",
    ));
    assert!(
        loaded
            .report
            .warnings
            .iter()
            .all(|w| !matches!(w.kind, WarningKind::NoEffect { .. }))
    );
}

#[test]
fn retention_enabled_is_honoured() {
    assert!(!config("retention:\n  enabled: false\n").retention_enabled());
    assert!(config("retention:\n  enabled: true\n").retention_enabled());
    assert!(config("retention: {}\n").retention_enabled());
}

#[test]
fn rust_settings_live_under_one_key() {
    let config = config(
        "litestream-rs:\n  app-readers: none\n  responses:\n    credentials: stop\n    other-writer: continue-and-interleave-histories\n",
    );
    let rust = config.rust();
    assert_eq!(rust.app_readers(), lsr_config::AppReaders::None);
    assert_eq!(
        rust.responses().credentials,
        Some(lsr_config::ResponseSetting::Stop)
    );
    assert_eq!(rust.responses().wal_corrupt, None);
    assert_eq!(
        rust.responses().other_writer,
        Some(lsr_config::OtherWriterSetting::ContinueAndInterleaveHistories)
    );
    let problems = refused("litestream-rs:\n  app-reader: none\n  responses:\n    bug: maybe\n");
    assert_eq!(
        problem_at(&problems, "litestream-rs.app-reader"),
        ProblemKind::Unknown {
            suggestion: Some("app-readers")
        }
    );
    assert!(matches!(
        problem_at(&problems, "litestream-rs.responses.bug"),
        ProblemKind::UnsupportedValue { .. }
    ));
}

#[test]
fn without_the_app_readers_claim_the_truncate_divergence_is_reported() {
    let loaded = parse(DB);
    assert!(
        warnings_at(&loaded.report.warnings, "the file")
            .contains(&&WarningKind::NoTruncateWithoutClaim)
    );
    let loaded = parse(&format!("{DB}litestream-rs:\n  app-readers: none\n"));
    assert!(
        !loaded
            .report
            .warnings
            .iter()
            .any(|w| w.kind == WarningKind::NoTruncateWithoutClaim)
    );
}

const SECRET_KEYS: &[&str] = &[
    "secret-access-key",
    "access-key-id",
    "sse-customer-key",
    "password",
    "token",
];

proptest! {
    /// A secret never appears in the report, whatever else is wrong with the file.
    #[test]
    fn secrets_never_appear_in_messages(secret in "[A-Za-z0-9/+]{12,40}", key in 0..SECRET_KEYS.len()) {
        let name = SECRET_KEYS[key];
        let texts = [
            format!("{name}: {secret}\nsync_interval: 1s\ndbs:\n  - path: /db\n    replica:\n      url: gs://b/{name}\n      sync-interval: soon\n"),
            format!("dbs:\n  - path: /db\n    replica:\n      url: s3://b/db\n      {name}: {secret}\n      {name}: {secret}x\n"),
            format!("dbs:\n  - path: /db\n    replica:\n      url: s3://b/db\n      {name}:\n        nested: {secret}\n"),
            format!("{name}: {secret}\nlevels: [{{interval: 0s}}]\nsnapshot: {{interval: -1h}}\n"),
            format!("{name}: \"{secret}\nunclosed: [\n"),
        ];
        for text in texts {
            let report = lsr_config::check_text(&text, Expand::Off, &TestEnv::default());
            prop_assert!(!report.is_ok());
            let printed = format!("{report}{report:?}");
            prop_assert!(!printed.contains(&secret), "{}", printed);
        }
    }
}
