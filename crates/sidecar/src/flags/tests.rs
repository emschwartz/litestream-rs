//! Go's `replicate_test.go` flag cases, ported by scenario.

use super::*;

fn args(line: &[&str]) -> Vec<String> {
    line.iter().map(|s| (*s).to_owned()).collect()
}

fn run(line: &[&str]) -> Flags {
    match parse(&args(line)) {
        Ok(Parsed::Run(flags)) => flags,
        other => panic!("{line:?}: {other:?}"),
    }
}

fn refused(line: &[&str]) -> UsageError {
    match parse(&args(line)) {
        Err(error) => error,
        Ok(other) => panic!("{line:?} accepted: {other:?}"),
    }
}

const POSITIONAL: [&str; 2] = ["test.db", "s3://bucket/test.db"];

fn with_positional<'a>(flags: &[&'a str]) -> Vec<&'a str> {
    let mut line = flags.to_vec();
    line.extend(POSITIONAL);
    line
}

/// `TestReplicateCommand_ParseFlags_OnceFlags`.
#[test]
fn once_and_its_companions() {
    let flags = run(&with_positional(&["-once"]));
    assert!(flags.once && !flags.force_snapshot && !flags.enforce_retention);
    let flags = run(&with_positional(&["-once", "-force-snapshot"]));
    assert!(flags.once && flags.force_snapshot);
    let flags = run(&with_positional(&["-once", "-enforce-retention"]));
    assert!(flags.once && flags.enforce_retention);
    let flags = run(&with_positional(&[
        "-once",
        "-force-snapshot",
        "-enforce-retention",
    ]));
    assert!(flags.once && flags.force_snapshot && flags.enforce_retention);
    // Checked after the config is read, as Go does.
    let combination = |args: &[&str]| check(&run(&with_positional(args))).unwrap_err();
    assert_eq!(
        combination(&["-force-snapshot"]),
        UsageError::ForceSnapshotWithoutOnce
    );
    assert_eq!(
        combination(&["-enforce-retention"]),
        UsageError::EnforceRetentionWithoutOnce
    );
    assert_eq!(
        combination(&["-once", "-exec", "echo test"]),
        UsageError::OnceWithExec
    );
}

/// `TestReplicateCommand_ParseFlags_FlagPositioning`.
#[test]
fn flags_come_before_the_positional_arguments() {
    assert_eq!(
        refused(&["test.db", "s3://bucket/test.db", "-exec", "echo test"]),
        UsageError::FlagAfterArguments {
            flag: "-exec".to_owned()
        }
    );
    assert_eq!(
        refused(&["test.db", "s3://bucket/test.db", "-config", "/c"]),
        UsageError::FlagAfterArguments {
            flag: "-config".to_owned()
        }
    );
    let flags = run(&with_positional(&["-exec", "echo test", "-no-expand-env"]));
    assert_eq!(flags.exec.as_deref(), Some("echo test"));
    assert!(flags.no_expand_env);
    assert_eq!(
        flags.source,
        Source::OneDatabase {
            database: "test.db".to_owned(),
            replica: "s3://bucket/test.db".to_owned(),
        }
    );
    assert_eq!(
        refused(&["test.db"]),
        UsageError::OnlyDatabase {
            database: "test.db".to_owned()
        }
    );
}

/// `TestReplicateCommand_ParseFlags_LogLevel`; the environment's part is the daemon's.
#[test]
fn log_level_flag() {
    assert_eq!(
        run(&with_positional(&["-log-level", "debug"])).log_level,
        Some(LogLevel::Debug)
    );
    assert_eq!(
        run(&with_positional(&["-log-level=TRACE"])).log_level,
        Some(LogLevel::Trace)
    );
    assert_eq!(
        run(&with_positional(&["--log-level", "error"])).log_level,
        Some(LogLevel::Error)
    );
    assert_eq!(run(&with_positional(&[])).log_level, None);
    let flags = run(&with_positional(&["-log-level", "warn", "-once"]));
    assert_eq!(flags.log_level, Some(LogLevel::Warn));
    assert!(flags.once);
    assert_eq!(
        refused(&["test.db", "s3://bucket/test.db", "-log-level", "debug"]),
        UsageError::FlagAfterArguments {
            flag: "-log-level".to_owned()
        }
    );
    // Go's logger keeps INFO for a level it does not know.
    let flags = run(&with_positional(&["-log-level", "loud"]));
    assert_eq!(flags.log_level, Some(LogLevel::Info));
}

#[test]
fn go_s_flag_syntax() {
    let flags = run(&["-config=/a.yml", "--config", "/b.yml"]);
    assert_eq!(flags.source, Source::Config(Some("/b.yml".to_owned())));
    assert!(!run(&["-once=false"]).once);
    assert_eq!(
        refused(&["-once=maybe"]),
        UsageError::BadBool {
            name: "once",
            value: "maybe".to_owned()
        }
    );
    assert_eq!(
        refused(&["-config"]),
        UsageError::MissingValue { name: "config" }
    );
    assert_eq!(
        refused(&["-bogus"]),
        UsageError::UnknownFlag {
            name: "bogus".to_owned()
        }
    );
    assert_eq!(parse(&args(&["-h"])), Ok(Parsed::Help));
    assert_eq!(parse(&args(&["--help"])), Ok(Parsed::Help));
    assert_eq!(run(&[]).source, Source::Config(None));
    let flags = run(&["--", "-odd.db", "/replica"]);
    assert_eq!(
        flags.source,
        Source::OneDatabase {
            database: "-odd.db".to_owned(),
            replica: "/replica".to_owned()
        }
    );
    assert_eq!(
        refused(&["-config", "/c.yml", "a.db", "/r"]),
        UsageError::ReplicaAndConfig
    );
    assert_eq!(refused(&["a.db", "/r", "/s"]), UsageError::MultipleReplicas);
}

/// Go's `resolveConfigPath`.
#[test]
fn the_config_path_is_the_flag_then_the_environment_then_the_default() {
    let env = |name: &str| (name == "LITESTREAM_CONFIG").then(|| "/env.yml".to_owned());
    assert_eq!(
        config_path(Some("/flag.yml"), env),
        PathBuf::from("/flag.yml")
    );
    assert_eq!(config_path(None, env), PathBuf::from("/env.yml"));
    assert_eq!(config_path(None, |_| None), PathBuf::from(DEFAULT_CONFIG));
}

/// Go's `flag` package sets each flag as it reads it, so a bad boolean before `-help`
/// is the error; help only stops the scan where it is reached.
#[test]
fn a_bad_boolean_before_help_is_the_error() {
    assert!(matches!(
        refused(&["-once=maybe", "-help"]),
        UsageError::BadBool { name: "once", .. }
    ));
    assert!(matches!(
        parse(&["-help".to_owned(), "-once=maybe".to_owned()]),
        Ok(Parsed::Help)
    ));
}

/// Go's `main` exits 0 for help only when an argument is literally `-h`, `-help` or
/// `--help`; `-help=1` and `--h` print help and fail.
#[test]
fn help_succeeds_only_when_asked_for_literally() {
    for literal in ["-h", "-help", "--help"] {
        assert!(
            matches!(parse(&[literal.to_owned()]), Ok(Parsed::Help)),
            "{literal}"
        );
    }
    for other in ["-help=1", "--h", "-h=x"] {
        assert_eq!(refused(&[other]), UsageError::HelpNotLiteral, "{other}");
    }
}
