//! yaml.v2's readings, measured: each expected value below is what Go 1.24
//! with gopkg.in/yaml.v2 v2.4.0 decoded the same YAML into a field of the
//! same Go type (the probe and its output are in docs/spikes.md, "S6"). Where
//! v1 refuses what Go reads, the case says so.

use lsr_config::{Expected, GoDuration, ProblemKind, ReplicaConfig, WarningKind};

use crate::common::{parse, problem_at, refused};

/// A replica with `key: value`.
fn replica_text(key: &str, value: &str) -> String {
    format!("dbs:\n  - path: /db\n    replica:\n      url: s3://bucket/db\n      {key}: {value}\n")
}

fn replica(key: &str, value: &str) -> ReplicaConfig {
    parse(&replica_text(key, value)).config.dbs()[0]
        .replica()
        .unwrap()
        .clone()
}

fn refused_at(key: &str, value: &str) -> ProblemKind {
    problem_at(
        &refused(&replica_text(key, value)),
        &format!("dbs[0].replica.{key}"),
    )
}

fn nanos(n: i64) -> Option<GoDuration> {
    Some(GoDuration::from_nanos(n))
}

#[test]
fn durations() {
    let cases = [
        ("10s", nanos(10_000_000_000)),
        ("\"10s\"", nanos(10_000_000_000)),
        ("1h30m", nanos(5_400_000_000_000)),
        ("1.5h", nanos(5_400_000_000_000)),
        ("300ms", nanos(300_000_000)),
        ("3\u{b5}s", nanos(3_000)),
        ("+5s", nanos(5_000_000_000)),
        ("5", nanos(5)),
        ("", None),
        ("null", None),
        ("~", None),
    ];
    for (text, want) in cases {
        assert_eq!(
            replica("sync-interval", text).sync_interval(),
            want,
            "{text:?}"
        );
    }
    let loaded = parse(&replica_text("sync-interval", "5"));
    let bare = WarningKind::BareNumberDuration {
        value: GoDuration::from_nanos(5),
    };
    assert!(loaded.report.warnings.iter().any(|w| w.kind == bare));
    for text in ["\"5\"", "10 s", "10S", "1d", "yes"] {
        let kind = refused_at("sync-interval", text);
        assert!(
            matches!(
                kind,
                ProblemKind::WrongType {
                    expected: Expected::Duration,
                    ..
                }
            ),
            "{text}: {kind:?}"
        );
    }
    // Go truncates 1.5 to 1ns; v1 refuses the fraction.
    assert_eq!(refused_at("sync-interval", "1.5"), ProblemKind::Fractional);
}

#[test]
fn booleans() {
    for (text, want) in [
        ("true", true),
        ("yes", true),
        ("Yes", true),
        ("on", true),
        ("y", true),
        ("TRUE", true),
        ("off", false),
        ("no", false),
        ("n", false),
        ("False", false),
    ] {
        assert_eq!(
            replica("force-path-style", text).s3().force_path_style(),
            Some(want),
            "{text}"
        );
    }
    assert_eq!(
        replica("force-path-style", "").s3().force_path_style(),
        None
    );
    for text in ["tRue", "\"true\"", "1"] {
        let kind = refused_at("force-path-style", text);
        assert!(
            matches!(
                kind,
                ProblemKind::WrongType {
                    expected: Expected::Boolean,
                    ..
                }
            ),
            "{text}"
        );
    }
}

#[test]
fn strings_keep_their_text() {
    for (text, want) in [
        ("yes", Some("yes")),
        ("0600", Some("0600")),
        ("1.0", Some("1.0")),
        ("123", Some("123")),
        ("2001-01-01", Some("2001-01-01")),
        ("null", None),
        ("~", None),
        ("\"\"", None),
        ("abc # note", Some("abc")),
        ("abc#note", Some("abc#note")),
        ("a:b", Some("a:b")),
        ("\"a\\tb\\u00e9\"", Some("a\tb\u{e9}")),
        ("'it''s'", Some("it's")),
    ] {
        assert_eq!(replica("region", text).s3().region(), want, "{text}");
    }
    assert_eq!(
        replica("region", "|\n        a\n        b").s3().region(),
        Some("a\nb\n")
    );
    assert_eq!(
        replica("region", ">\n        a\n        b").s3().region(),
        Some("a b\n")
    );
    let kind = refused_at("region", "\n        a: b");
    assert!(matches!(
        kind,
        ProblemKind::WrongType {
            expected: Expected::String,
            ..
        }
    ));
}

#[test]
fn integers() {
    for (text, want) in [
        ("0600", 384),
        ("0o600", 384),
        ("08", 8),
        ("0x10", 16),
        ("0b101", 5),
        ("1_000", 1000),
        ("1e3", 1000),
        ("-5", -5),
        ("+5", 5),
    ] {
        assert_eq!(
            replica("max-sync-ltx-files", text).max_sync_ltx_files(),
            Some(want),
            "{text}"
        );
    }
    assert!(matches!(
        refused_at("max-sync-ltx-files", "\"5\""),
        ProblemKind::WrongType {
            expected: Expected::Integer,
            ..
        }
    ));
    assert!(matches!(
        refused_at("max-sync-ltx-files", "99999999999999999999"),
        ProblemKind::OutOfRange { .. }
    ));
    assert!(matches!(
        refused_at("max-sync-ltx-files", ".inf"),
        ProblemKind::OutOfRange { .. }
    ));
    // Go truncates 2.7 to 2; v1 refuses the fraction.
    assert_eq!(
        refused_at("max-sync-ltx-files", "2.7"),
        ProblemKind::Fractional
    );
}

#[test]
fn anchors_aliases_and_merges() {
    let config = parse("dbs:\n  - path: /a\n    replica: &r\n      url: s3://bucket/a\n      sync-interval: 5s\n  - path: /b\n    replica:\n      <<: *r\n      url: s3://bucket/b\n").config;
    let b = config.dbs()[1].replica().unwrap();
    assert_eq!(
        (b.url(), b.sync_interval()),
        (Some("s3://bucket/b"), nanos(5_000_000_000))
    );
    let problems = refused(
        "dbs:\n  - path: /a\n    replica: &r\n      url: s3://bucket/a\n  - path: /b\n    replica:\n      url: s3://bucket/b\n      <<: *r\n",
    );
    assert!(matches!(
        problems[0].kind,
        ProblemKind::MergeOverridesKey { key_line: 7 }
    ));
}

#[test]
fn documents_tabs_and_layout() {
    assert!(matches!(
        refused("exec: a\n---\nexec: b\n")[0].kind,
        ProblemKind::MultipleDocuments
    ));
    assert_eq!(parse("exec: a\n...\n").config.exec(), Some("a"));
    assert_eq!(parse("%YAML 1.1\n---\nexec: a\n").config.exec(), Some("a"));
    assert!(matches!(
        refused("exec:\ta\n")[0].kind,
        ProblemKind::TabAfterColon { .. }
    ));
    assert!(matches!(
        refused("logging:\n\tlevel: info\n")[0].kind,
        ProblemKind::Syntax { .. }
    ));
    assert_eq!(parse("exec: a\tb\n").config.exec(), Some("a\tb"));
    assert_eq!(parse("exec: a\r\naddr: b\r\n").config.addr(), Some("b"));
    assert_eq!(parse("\u{feff}exec: a\n").config.exec(), Some("a"));
    assert_eq!(parse("\"exec\": a\n").config.exec(), Some("a"));
    assert_eq!(
        parse("logging: {level: debug}\n").config.logging().level(),
        lsr_config::LogLevel::Debug
    );
    assert!(matches!(
        refused("- a\n")[0].kind,
        ProblemKind::WrongType {
            expected: Expected::Mapping,
            ..
        }
    ));
    assert!(matches!(
        refused("exec: !!str a\n")[0].kind,
        ProblemKind::Tag
    ));
    assert!(matches!(
        refused("dbs:\n  -\n  - path: /db\n")[0].kind,
        ProblemKind::EmptyItem
    ));
    // `levels:` with nothing is no levels in Go (the defaults are not restored).
    assert!(parse("levels:\n").config.levels().is_empty());
    assert_eq!(
        problem_at(&refused("EXEC: a\n"), "EXEC"),
        ProblemKind::Unknown {
            suggestion: Some("exec")
        }
    );
}
