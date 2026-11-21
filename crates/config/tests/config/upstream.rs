//! Upstream's example file, `etc/litestream.yml` at 041a8c3, copied unchanged
//! to `tests/data/litestream.yml` (Apache-2.0, see NOTICE). It is all
//! comments; each of its sections is uncommented in turn and loaded.

use lsr_config::{GoDuration, KeyClass, LogFormat, LogLevel, ProblemKind, WarningKind, defaults};

use crate::common::{parse, problem_at, refused, warnings_at};

const FILE: &str = include_str!("../data/litestream.yml");

/// The file with the YAML lines between the two markers uncommented: a line
/// `# key: ...`, `#   key: ...` or `#  - key: ...`, never prose or a nested comment.
fn uncomment(from: &str, to: &str) -> String {
    let mut inside = false;
    let mut out = String::new();
    for line in FILE.lines() {
        if line.starts_with(from) {
            inside = true;
        } else if line.starts_with(to) {
            inside = false;
        }
        let yaml = line.strip_prefix("# ").filter(|rest| {
            let body = rest.trim_start().trim_start_matches("- ");
            let key = body.split_once(':').map_or("", |(k, _)| k);
            !key.is_empty()
                && key
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        });
        match yaml {
            Some(rest) if inside => out.push_str(rest),
            _ => out.push_str(line),
        }
        out.push('\n');
    }
    out
}

#[test]
fn the_file_as_shipped_loads_with_go_defaults() {
    let config = parse(FILE).config;
    assert_eq!(config.levels(), defaults::LEVELS);
    assert!(config.dbs().is_empty());
}

#[test]
fn the_global_options_uncommented_load_with_the_defaults_they_document() {
    let text = uncomment("# Global options", "\u{0}");
    let loaded = parse(&text);
    let config = &loaded.config;
    assert_eq!(config.levels(), defaults::LEVELS);
    assert_eq!(config.snapshot().interval(), GoDuration::from_secs(86_400));
    assert_eq!(config.snapshot().retention(), GoDuration::from_secs(86_400));
    assert_eq!(config.l0_retention(), GoDuration::from_secs(300));
    assert_eq!(
        config.l0_retention_check_interval(),
        GoDuration::from_secs(15)
    );
    assert!(config.retention_enabled());
    assert_eq!(config.addr(), Some(":9090"));
    assert_eq!(config.logging().level(), LogLevel::Info);
    assert_eq!(config.logging().format(), LogFormat::Text);
    assert!(!config.logging().stderr());
    assert!(matches!(
        warnings_at(&loaded.report.warnings, "logging.source")[..],
        [WarningKind::NoEffect { .. }]
    ));
    let classes: Vec<_> = loaded.report.keys.iter().map(|(_, class)| *class).collect();
    assert!(!classes.contains(&KeyClass::Unknown));
}

#[test]
fn the_credentials_uncommented_load() {
    parse(&uncomment("# AWS credentials", "# Control socket"));
}

#[test]
fn the_socket_example_enables_the_socket_which_litestream_rs_refuses() {
    let problems = refused(&uncomment("# Control socket", "# dbs:"));
    assert_eq!(problems.len(), 1, "{problems:#?}");
    assert!(matches!(
        problem_at(&problems, "socket.enabled"),
        ProblemKind::NotInV1 { .. }
    ));
}

#[test]
fn the_database_examples_name_backends_litestream_rs_refuses() {
    let text = uncomment("# dbs:", "# ----");
    let problems = refused(&text);
    // The example sets `url` twice on one replica (s3, then nats) and uses sftp on the other.
    assert!(
        problems
            .iter()
            .any(|p| matches!(p.kind, ProblemKind::Duplicate { .. })),
        "{problems:#?}"
    );
    let sftp = ProblemKind::UnsupportedReplicaType {
        name: "sftp".to_owned(),
    };
    assert_eq!(problem_at(&problems, "dbs[1].replica.url"), sftp);
}
