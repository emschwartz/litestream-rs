use lsr_store_s3::{Provider, UrlStyle};

use super::*;
use lsr_core::{AsOfInside, RestoreTarget};
use lsr_ltx::{Timestamp, Txid};
use lsr_replica::local::IntegrityCheck;

fn words(line: &str) -> Vec<String> {
    line.split_whitespace().map(str::to_owned).collect()
}

fn parsed(line: &str) -> Result<Command, UsageError> {
    parse(&words(line))
}

fn usage(line: &str) -> UsageError {
    parsed(line).unwrap_err()
}

fn txid(raw: u64) -> Txid {
    Txid::new(raw).unwrap()
}

#[test]
fn restore_latest_is_the_default_target() {
    assert_eq!(
        parsed("restore --replica r --output o"),
        Ok(Command::Restore(Box::new(RestoreArgs {
            from: RestoreFrom::Replica(Replica::Dir("r".into())),
            output: Some("o".into()),
            target: RestoreTarget::Latest,
            as_of_inside: AsOfInside::Refuse,
            clear_journal_files: false,
            integrity_check: IntegrityCheck::Off,
            if_db_not_exists: false,
            if_replica_exists: false,
            dry_run: false,
            json: false,
            force: false,
            parallelism: false,
            txid_and_timestamp: false,
            timestamp_given: false,
        })))
    );
}

/// Go's restore switches, under Go's names with this CLI's two dashes.
#[test]
fn restore_takes_the_skip_dry_run_and_json_switches() {
    for (flag, pick) in [
        ("--if-db-not-exists", 0_usize),
        ("--if-replica-exists", 1),
        ("--dry-run", 2),
        ("--json", 3),
    ] {
        let Ok(Command::Restore(args)) = parsed(&format!("restore --replica r {flag} --output o"))
        else {
            panic!("{flag} did not parse");
        };
        let got = [
            args.if_db_not_exists,
            args.if_replica_exists,
            args.dry_run,
            args.json,
        ];
        let mut want = [false; 4];
        want[pick] = true;
        assert_eq!(got, want, "{flag}");
    }
    let Ok(Command::Restore(args)) = parsed(
        "restore --replica r --output o --dry-run --json --if-db-not-exists --if-replica-exists",
    ) else {
        panic!("all four did not parse together");
    };
    assert!(args.dry_run && args.json && args.if_db_not_exists && args.if_replica_exists);
}

#[test]
fn restore_reads_every_flag_in_any_order() {
    let Ok(Command::Restore(args)) =
        parsed("restore --clear-journal-files --txid 000000000000002a --output o --replica r")
    else {
        panic!("did not parse");
    };
    assert_eq!(args.target, RestoreTarget::Txid(txid(42)));
    assert!(args.clear_journal_files);
    let Ok(Command::Restore(args)) =
        parsed("restore --replica r --output o --timestamp 2026-11-20T10:00:00.5+01:00")
    else {
        panic!("did not parse");
    };
    let at = Timestamp::parse_rfc3339("2026-11-20T09:00:00.500Z").unwrap();
    assert_eq!(args.target, RestoreTarget::AsOf(at));
}

#[test]
fn verify_defaults_to_contents_and_takes_depth_and_json() {
    assert_eq!(
        parsed("verify --replica r"),
        Ok(Command::Verify(VerifyArgs {
            replica: Replica::Dir("r".into()),
            depth: Depth::Contents,
            json: false,
        }))
    );
    assert_eq!(
        parsed("verify --json --depth names --replica r"),
        Ok(Command::Verify(VerifyArgs {
            replica: Replica::Dir("r".into()),
            depth: Depth::Names,
            json: true,
        }))
    );
}

#[test]
fn files_takes_replica_and_json() {
    assert_eq!(
        parsed("files --replica r --json"),
        Ok(Command::Files(FilesArgs {
            replica: Replica::Dir("r".into()),
            json: true,
            resolved: false,
        }))
    );
    let Ok(Command::Files(args)) = parsed("files --replica s3://b/p --created-at") else {
        panic!("--created-at is refused beside an s3 replica");
    };
    assert!(args.resolved);
    // A directory has no per-file reads to ask for, so the flag would change nothing there.
    assert_eq!(
        usage("files --replica r --created-at"),
        UsageError::S3FlagForDirectory {
            flag: "--created-at"
        }
    );
}

#[test]
fn help_and_version_in_every_spelling() {
    for line in [
        "--help",
        "-h",
        "help",
        "restore --help",
        "verify -h",
        "files --replica r --help",
    ] {
        assert_eq!(parsed(line), Ok(Command::Help), "{line}");
    }
    for line in ["--version", "-V"] {
        assert_eq!(parsed(line), Ok(Command::Version), "{line}");
    }
}

/// `version` is Go's command, not `--version`: Go's flag grammar, no positional needed, and a
/// stray argument ignored as Go ignores it (`cmd/litestream/version.go`).
#[test]
fn version_is_a_command_with_go_s_one_flag() {
    assert_eq!(
        parsed("version"),
        Ok(Command::VersionLine(VersionArgs { verbose: false }))
    );
    for line in [
        "version -verbose",
        "version --verbose",
        "version -verbose=true",
    ] {
        assert_eq!(
            parsed(line),
            Ok(Command::VersionLine(VersionArgs { verbose: true })),
            "{line}"
        );
    }
    assert_eq!(
        parsed("version stray"),
        Ok(Command::VersionLine(VersionArgs { verbose: false }))
    );
    assert_eq!(parsed("version -h"), Ok(Command::Help));
    assert!(matches!(usage("version -json"), UsageError::GoFlag(_)));
}

#[test]
fn no_command() {
    assert_eq!(parse(&[]), Err(UsageError::NoCommand));
}

#[test]
fn unknown_command() {
    assert_eq!(
        usage("frobnicate"),
        UsageError::UnknownCommand {
            name: "frobnicate".into()
        }
    );
}

/// A flag before the command is the Go habit `lsr -config c.yml replicate`; the message says where
/// flags go and names every command, and no other first word gets it.
#[test]
fn a_flag_before_the_command_says_flags_follow_it() {
    for line in ["-config c.yml replicate", "--config=c.yml replicate", "-x"] {
        let error = usage(line);
        let first = line.split_whitespace().next().unwrap().to_owned();
        assert_eq!(
            error,
            UsageError::FlagBeforeCommand { flag: first },
            "{line}"
        );
        let message = error.to_string();
        assert!(
            message.starts_with("flags go after the command"),
            "{message}"
        );
        for word in ["replicate", "config"] {
            assert!(message.contains(word), "{message}");
        }
        for command in CommandName::ALL {
            assert!(message.contains(command.word()), "{message}");
        }
    }
    // The help and version flags still answer, and a word that is not a command is still unknown.
    assert_eq!(parsed("-h"), Ok(Command::Help));
    assert_eq!(parsed("--version"), Ok(Command::Version));
    assert_eq!(
        usage("frobnicate -config c.yml"),
        UsageError::UnknownCommand {
            name: "frobnicate".into()
        }
    );
}

#[test]
fn unknown_flag_including_one_another_command_takes() {
    assert_eq!(
        usage("verify --replica r --bogus"),
        UsageError::UnknownFlag {
            command: "verify",
            flag: "--bogus".into()
        }
    );
    assert_eq!(
        usage("files --replica r --output o"),
        UsageError::UnknownFlag {
            command: "files",
            flag: "--output".into()
        }
    );
    assert_eq!(
        usage("verify --replica r -x"),
        UsageError::UnknownFlag {
            command: "verify",
            flag: "-x".into()
        }
    );
}

#[test]
fn duplicate_flag_even_a_switch() {
    assert_eq!(
        usage("verify --replica r --replica s"),
        UsageError::DuplicateFlag { flag: "--replica" }
    );
    assert_eq!(
        usage("files --replica r --json --json"),
        UsageError::DuplicateFlag { flag: "--json" }
    );
}

#[test]
fn missing_value_at_the_end_and_before_another_flag() {
    assert_eq!(
        usage("verify --replica"),
        UsageError::MissingValue { flag: "--replica" }
    );
    assert_eq!(
        usage("verify --replica --json"),
        UsageError::MissingValue { flag: "--replica" }
    );
}

#[test]
fn missing_required_flag() {
    for (line, command) in [
        ("verify", "verify"),
        ("files --json", "files"),
        ("trust-local app.db", "trust-local"),
    ] {
        assert_eq!(
            usage(line),
            UsageError::MissingFlag {
                command,
                flag: "--replica"
            },
            "{line}"
        );
    }
    // restore reads Go's grammar: no source at all is Go's own refusal.
    assert_eq!(usage("restore --output o"), UsageError::GoSourceMissing);
}

/// Go refuses a txid and a timestamp together only when it plans or restores, so the parse
/// keeps both and marks the line; a zero txid is no txid.
#[test]
fn two_targets_at_once() {
    let marked = |line: &str| match parsed(line) {
        Ok(Command::Restore(args)) => args.txid_and_timestamp,
        other => panic!("{line}: {other:?}"),
    };
    assert!(marked(
        "restore --replica r --output o --txid 0000000000000001 --timestamp 2026-01-01T00:00:00Z"
    ));
    assert!(!marked(
        "restore --replica r --output o --txid 0000000000000000 --timestamp 2026-01-01T00:00:00Z"
    ));
}

#[test]
fn allow_earlier_takes_the_earlier_state_for_a_timestamp() {
    let line = "restore --replica r --output o --timestamp 2026-01-01T00:00:00Z";
    let Ok(Command::Restore(plain)) = parsed(line) else {
        panic!("not a restore");
    };
    assert_eq!(plain.as_of_inside, AsOfInside::Refuse);
    let Ok(Command::Restore(accepting)) = parsed(&format!("{line} --allow-earlier")) else {
        panic!("not a restore");
    };
    assert_eq!(accepting.as_of_inside, AsOfInside::TakeEarlier);
}

#[test]
fn allow_earlier_without_a_timestamp_is_a_usage_error() {
    for target in ["", "--txid 0000000000000001"] {
        assert_eq!(
            usage(&format!(
                "restore --replica r --output o {target} --allow-earlier"
            )),
            UsageError::AllowEarlierWithoutTimestamp
        );
    }
    assert_eq!(
        UsageError::AllowEarlierWithoutTimestamp.to_string(),
        "--allow-earlier needs --timestamp"
    );
    assert!(matches!(
        parsed("verify --replica r --allow-earlier"),
        Err(UsageError::UnknownFlag { .. })
    ));
}

#[test]
fn bare_word_where_a_flag_belongs() {
    assert_eq!(
        usage("verify --replica r extra"),
        UsageError::UnexpectedArgument {
            argument: "extra".into()
        }
    );
}

/// Go's `txidVar`: sixteen hex digits in any case, refused as the flag is read; zero is
/// Go's "latest".
#[test]
fn txid_as_go_reads_it() {
    for bad in ["2a", "000000000000002g", "0x0000000000002a"] {
        let line = format!("restore --replica r --output o --txid {bad}");
        assert_eq!(
            usage(&line).to_string(),
            format!("invalid value {bad:?} for flag -txid: invalid txid format")
        );
    }
    let target = |line: &str| match parsed(line) {
        Ok(Command::Restore(args)) => args.target,
        other => panic!("{line}: {other:?}"),
    };
    assert_eq!(
        target("restore --replica r --output o -txid 000000000000002A"),
        RestoreTarget::Txid(txid(42))
    );
    assert_eq!(
        target("restore --replica r --output o -txid 0000000000000000"),
        RestoreTarget::Latest
    );
}

#[test]
fn bad_timestamp() {
    assert_eq!(
        usage("restore --replica r --output o --timestamp yesterday"),
        UsageError::GoTimestamp
    );
}

#[test]
fn bad_depth() {
    assert_eq!(
        usage("verify --replica r --depth deep"),
        UsageError::BadDepth {
            value: "deep".into()
        }
    );
}

/// Go's `-integrity-check` words, and its default of none.
#[test]
fn integrity_check_takes_go_s_three_words() {
    for (word, want) in [
        ("none", IntegrityCheck::Off),
        ("quick", IntegrityCheck::Quick),
        ("full", IntegrityCheck::Full),
    ] {
        let Ok(Command::Restore(args)) = parsed(&format!(
            "restore --replica r --output o --integrity-check {word}"
        )) else {
            panic!("{word} did not parse");
        };
        assert_eq!(args.integrity_check, want, "{word}");
    }
}

#[test]
fn bad_integrity_check() {
    for word in ["Quick", "integrity", "off", "true", "quick_check"] {
        assert_eq!(
            usage(&format!(
                "restore --replica r --output o --integrity-check {word}"
            )),
            UsageError::GoIntegrityCheck { value: word.into() },
            "{word}"
        );
    }
    assert_eq!(
        usage("restore --replica r --output o --integrity-check").to_string(),
        "flag needs an argument: -integrity-check"
    );
    assert!(matches!(
        usage("verify --replica r --integrity-check quick"),
        UsageError::UnknownFlag { .. }
    ));
}

#[test]
fn a_url_replica_is_refused_by_scheme_only() {
    let https = usage("files --replica https://x/y");
    assert_eq!(
        https,
        UsageError::UnsupportedReplica {
            scheme: "https".into()
        }
    );
    assert!(!https.to_string().contains("x/y"));
}

fn s3(line: &str) -> S3Config {
    match parsed(line) {
        Ok(Command::Files(FilesArgs {
            replica: Replica::S3(config),
            ..
        })) => *config,
        other => panic!("not an s3 files command: {other:?}"),
    }
}

/// The table of Go's `TestParseURL` (s3/replica_client_test.go:3448), each row
/// read through `--replica`.
#[test]
fn parse_s3_forms() {
    let config = s3("files --replica s3://bucket/path");
    assert_eq!(config.bucket().as_str(), "bucket");
    assert_eq!(config.prefix().as_str(), "path");
    assert_eq!(config.region().as_str(), "us-east-1");
    assert_eq!(
        config.endpoint().url().as_str(),
        "https://s3.us-east-1.amazonaws.com/"
    );
    assert_eq!(config.style(), UrlStyle::VirtualHost);
    assert_eq!(config.provider(), Provider::Aws);
    assert_eq!(
        s3("files --replica s3://bucket/a/b/c").prefix().as_str(),
        "a/b/c"
    );
    // A bucket with no path is the whole bucket, as under Go: keys start with a slash. Without
    // the slash Go's prefix is `.` (keys `./0000/...`), which the `url` type cannot carry: refused.
    let bare = s3("files --replica s3://bucket/");
    assert_eq!(bare.bucket().as_str(), "bucket");
    assert_eq!(bare.prefix().as_str(), "");
    assert_eq!(
        usage("files --replica s3://bucket"),
        UsageError::BadReplicaUrl {
            reason: ReplicaProblem::NoSlashAfterBucket
        }
    );
    assert!(matches!(
        usage("files --replica s3:///p"),
        UsageError::BadReplicaUrl {
            reason: ReplicaProblem::BucketRequired
        }
    ));
}

/// The hosts Go's `s3.ParseHost` takes apart, read as the daemon reads them: an AWS
/// virtual host gives the bucket and region, MinIO's `bucket.host:port` the bucket and an
/// http endpoint in path style; a provider whose endpoint litestream-rs does not derive is refused.
#[test]
fn go_s_host_spellings_read_as_the_daemon_reads_them() {
    let aws = s3("files --replica s3://my-bucket.s3.us-west-2.amazonaws.com/db");
    assert_eq!(aws.bucket().as_str(), "my-bucket");
    assert_eq!(aws.region().as_str(), "us-west-2");
    assert_eq!(
        aws.endpoint().url().as_str(),
        "https://s3.us-west-2.amazonaws.com/"
    );
    let minio = s3("files --replica s3://bucket.localhost:9000/p");
    assert_eq!(minio.bucket().as_str(), "bucket");
    assert_eq!(minio.endpoint().url().as_str(), "http://localhost:9000/");
    assert_eq!(minio.style(), UrlStyle::Path);
    for host in [
        "bucket.nyc3.digitaloceanspaces.com",
        "bucket.s3.us-west-000.backblazeb2.com",
        "bucket.s3.fr-par.scw.cloud",
    ] {
        assert!(
            matches!(
                usage(&format!("files --replica s3://{host}/p")),
                UsageError::BadReplicaUrl {
                    reason: ReplicaProblem::ProviderHost { .. }
                }
            ),
            "{host}"
        );
    }
}

/// `lsr restore --replica <url>` names the same bucket, prefix, endpoint, region, style
/// and part size the daemon's `lsr replicate DB_PATH <url>` replicates to.
#[test]
fn restore_accepts_every_url_the_daemon_accepts() {
    for url in [
        "s3://bucket/path",
        "s3://my-bucket.s3.amazonaws.com/db/app",
        "s3://my-bucket.s3.eu-west-1.amazonaws.com/db",
        "s3://bucket.localhost:9000/p",
        "s3://b/p?endpoint=localhost:9000&region=eu-west-1",
        "s3://b/p?endpoint=https://fly.storage.tigris.dev&region=auto",
        "s3://b/p?endpoint=https://example.com&forcePathStyle=false",
        "s3://b/p?partSize=10485760",
        "s3://b/p?signPayload=false&concurrency=8",
    ] {
        let loaded =
            lsr_config::GoConfig::for_one_database("/data/app.db", url, &lsr_config::ProcessEnv)
                .unwrap();
        let plan = lsr_config::plan(&loaded.config, &lsr_config::ProcessEnv).unwrap();
        let lsr_config::ReplicaPlan::S3 { config: daemon, .. } = plan.dbs()[0].replica() else {
            panic!("{url}: not an S3 replica");
        };
        let restore = match parsed(&format!("restore --replica {url} --output o")) {
            Ok(Command::Restore(args)) => match args.from {
                RestoreFrom::Replica(Replica::S3(config)) => config,
                other @ (RestoreFrom::Replica(Replica::Dir(_))
                | RestoreFrom::Url(_)
                | RestoreFrom::Database { .. }) => panic!("{url}: {other:?}"),
            },
            other => panic!("{url}: {other:?}"),
        };
        assert_eq!(restore, *daemon, "{url}");
    }
}

#[test]
fn s3_flags_set_endpoint_region_and_style() {
    let config = s3(
        "files --replica s3://b/p --endpoint http://127.0.0.1:9000 --region eu-west-1 --force-path-style",
    );
    assert_eq!(config.endpoint().url().as_str(), "http://127.0.0.1:9000/");
    assert_eq!(config.region().as_str(), "eu-west-1");
    assert_eq!(config.style(), UrlStyle::Path);
    assert_eq!(config.provider(), Provider::Minio);
    // As Go's `endpoint` key: an endpoint turns path style on.
    let tigris = s3("files --replica s3://b/p --endpoint https://fly.storage.tigris.dev");
    assert_eq!(tigris.provider(), Provider::Tigris);
    assert_eq!(tigris.style(), UrlStyle::Path);
}

#[test]
fn a_flag_overrides_the_url_query() {
    let config = s3(
        "files --replica s3://b/p?region=us-west-2&forcePathStyle=false&endpoint=http://a:1 --region eu-west-1 --force-path-style --endpoint http://b:2",
    );
    assert_eq!(config.region().as_str(), "eu-west-1");
    assert_eq!(config.style(), UrlStyle::Path);
    assert_eq!(config.endpoint().url().as_str(), "http://b:2/");
    // The query alone still works, and an endpoint with a query of its own survives.
    let alone = s3("files --replica s3://b/p?endpoint=http%3A%2F%2Fa%3A1");
    assert_eq!(alone.endpoint().url().as_str(), "http://a:1/");
}

#[test]
fn bad_s3_flags_name_the_flag_and_not_the_value() {
    for (line, flag, reason) in [
        (
            "files --replica s3://b/p --endpoint ftp://h",
            "--endpoint",
            ConfigError::UnsupportedScheme,
        ),
        (
            "files --replica s3://b/p --endpoint http://user:hunter2@h",
            "--endpoint",
            ConfigError::BadUrl,
        ),
        (
            "files --replica s3://b/p --region no/good",
            "--region",
            ConfigError::BadRegion,
        ),
    ] {
        let err = usage(line);
        assert_eq!(err, UsageError::BadS3Flag { flag, reason }, "{line}");
        assert!(!err.to_string().contains("hunter2"));
    }
}

#[test]
fn a_url_with_userinfo_or_a_secret_query_never_prints() {
    let err = usage("files --replica s3://AK:hunter2@b/p");
    assert_eq!(
        err,
        UsageError::BadReplicaUrl {
            reason: ReplicaProblem::UserInfo
        }
    );
    assert!(!err.to_string().contains("hunter2"));
    let err = usage("files --replica s3://b/p?secret=hunter2");
    assert_eq!(
        err,
        UsageError::BadReplicaUrl {
            reason: ReplicaProblem::QueryUnknown {
                name: "secret".to_owned()
            }
        }
    );
    assert!(!err.to_string().contains("hunter2"));
}

#[test]
fn s3_flags_with_a_directory_replica_are_refused() {
    for flag in [
        "--endpoint http://h",
        "--region eu-west-1",
        "--force-path-style",
    ] {
        let name = flag.split(' ').next().unwrap();
        let err = usage(&format!("files --replica dir {flag}"));
        assert!(
            matches!(&err, UsageError::S3FlagForDirectory { flag } if *flag == name),
            "{err:?}"
        );
    }
}

#[test]
fn s3_flags_are_taken_by_every_command() {
    for line in [
        "restore --replica s3://b/p --output o --region eu-west-1",
        "verify --replica s3://b/p --endpoint http://h:9 --force-path-style",
        "files --replica s3://b/p --region eu-west-1",
    ] {
        assert!(parsed(line).is_ok(), "{line}");
    }
}

#[test]
fn help_documents_every_flag_command_and_exit_code() {
    let help = crate::render::help();
    for command in CommandName::ALL {
        for flag in help_flags(command) {
            assert!(
                help.contains(&flag.name) && help.contains(&flag.help),
                "{}",
                flag.name
            );
        }
        assert!(
            help.contains(&format!("lsr {}", command.word())),
            "{}",
            command.word()
        );
    }
    for exit in crate::run::Exit::ALL {
        assert!(help.contains(&format!("{:<3}{}", exit.code(), exit.meaning())));
    }
}

/// Go's follow mode is not built: refused by name, never read as a one-shot restore.
#[test]
fn follow_mode_is_refused_as_not_built() {
    for line in ["restore -f -o o s3://b/p", "restore --f=true /data/db"] {
        assert_eq!(usage(line), UsageError::FollowNotBuilt, "{line}");
    }
    assert!(UsageError::FollowNotBuilt.to_string().contains("#177"));
}

#[test]
fn trust_local_takes_the_database_first_then_its_flags() {
    assert_eq!(
        parsed("trust-local app.db --replica r --meta-dir m"),
        Ok(Command::TrustLocal(TrustLocalArgs {
            database: PathBuf::from("app.db"),
            replica: Replica::Dir(PathBuf::from("r")),
            meta_dir: Some(PathBuf::from("m")),
        }))
    );
    assert_eq!(
        parsed("trust-local --replica r"),
        Err(UsageError::MissingDatabase)
    );
    assert_eq!(parsed("trust-local"), Err(UsageError::MissingDatabase));
}
/// The words after `lsr` that the dispatcher takes are `CommandName::ALL`, and the error that
/// names them lists the same ones, so no command can be accepted and left out of the help.
#[test]
fn every_command_the_dispatcher_accepts_is_in_the_top_level_help() {
    let help = crate::render::help();
    let listed: std::collections::BTreeSet<&str> = COMMAND_WORDS.split(", ").collect();
    let all: std::collections::BTreeSet<&str> = CommandName::ALL
        .into_iter()
        .map(CommandName::word)
        .collect();
    assert_eq!(listed, all, "COMMAND_WORDS drifted from CommandName::ALL");
    assert_eq!(all.len(), CommandName::ALL.len(), "a word is listed twice");
    for command in CommandName::ALL {
        let word = command.word();
        assert_eq!(CommandName::from_word(word), Some(command));
        assert!(
            !matches!(
                parsed(&format!("{word} -h")),
                Err(UsageError::UnknownCommand { .. } | UsageError::FlagBeforeCommand { .. })
            ),
            "{word} is listed but the dispatcher does not take it"
        );
        let usage = command
            .usage_line()
            .map_or_else(|| format!("\nlsr {word}"), |line| format!("\n{line}"));
        assert!(
            help.contains(&usage),
            "{word} has no usage line in the help"
        );
        assert!(help.contains(command.summary()), "{word} has no summary");
    }
    assert_eq!(
        usage("frobnicate"),
        UsageError::UnknownCommand {
            name: "frobnicate".into()
        }
    );
}

/// The flags each Go-grammar command's help lists are exactly the flags its parser defines,
/// with the same arity, so help text and parser cannot drift apart.
#[test]
fn help_lists_exactly_the_flags_each_go_grammar_parser_defines() {
    let tables = [
        (CommandName::Restore, &restore::DEFINED[..]),
        (CommandName::Databases, &inspect::DATABASES[..]),
        (CommandName::Ltx, &inspect::LTX[..]),
        (CommandName::Wal, &inspect::LTX[..]),
        (CommandName::Version, &inspect::VERSION[..]),
    ];
    for (command, defined) in tables {
        let listed = help_flags(command);
        let mut want: Vec<(&str, bool)> = defined.iter().map(|d| (d.name, d.value)).collect();
        let mut have: Vec<(&str, bool)> = listed
            .iter()
            .map(|f| (f.name.trim_start_matches('-'), f.value.is_some()))
            .collect();
        want.sort_unstable();
        have.sort_unstable();
        assert_eq!(have, want, "{}: help and parser disagree", command.word());
    }
}

/// How a flag is written in the walk below: its value, or none for a switch.
fn sample(flag: &str) -> &'static [&'static str] {
    match flag {
        "--replica" => &["r2"],
        "--endpoint" => &["http://h:9"],
        "--region" => &["eu-west-1"],
        "--output" | "-o" => &["p"],
        "--txid" => &["000000000000002a"],
        "--timestamp" => &["2025-01-01T00:00:00Z"],
        "--integrity-check" => &["quick"],
        "-config" => &["c.yml"],
        "-parallelism" => &["4"],
        "-follow-interval" => &["2s"],
        "-level" => &["all"],
        "--depth" => &["names"],
        "--meta-dir" => &["m"],
        "--force-path-style"
        | "--allow-earlier"
        | "--clear-journal-files"
        | "--if-db-not-exists"
        | "--if-replica-exists"
        | "--dry-run"
        | "-no-expand-env"
        | "-force"
        | "-f"
        | "--json"
        | "-json"
        | "-verbose"
        | "--created-at" => &[],
        other => {
            panic!("{other} has no sample: give a new flag one, and so a proof it does something")
        }
    }
}

/// One way of naming a replica, its base command line, and the flags that line already uses.
struct Form {
    label: &'static str,
    command: CommandName,
    base: &'static str,
    in_base: &'static [&'static str],
    /// Flags that change nothing here, and why. Each is held to that: it must change nothing.
    inert: &'static [(&'static str, &'static str)],
}

const GO_IGNORES_IT_BESIDE_A_URL: &str = "Go reads it only for a config; beside a URL it takes the flag and does nothing, and a URL needs no expansion";
const FOLLOW_ONLY: &str =
    "Go reads it only with -f, which this build refuses, so it is accepted and unused as in Go";

const FORMS: &[Form] = &[
    Form {
        label: "restore --replica dir",
        command: CommandName::Restore,
        base: "restore --replica r --output o",
        in_base: &["--replica", "--output"],
        inert: &[("-follow-interval", FOLLOW_ONLY)],
    },
    Form {
        label: "restore --replica s3",
        command: CommandName::Restore,
        base: "restore --replica s3://b/p --output o",
        in_base: &["--replica", "--output"],
        inert: &[("-follow-interval", FOLLOW_ONLY)],
    },
    Form {
        label: "restore REPLICA_URL",
        command: CommandName::Restore,
        base: "restore -o o s3://b/p",
        in_base: &["-o"],
        inert: &[
            ("-no-expand-env", GO_IGNORES_IT_BESIDE_A_URL),
            ("-follow-interval", FOLLOW_ONLY),
        ],
    },
    Form {
        label: "restore DB_PATH",
        command: CommandName::Restore,
        base: "restore /data/db",
        in_base: &[],
        inert: &[("-follow-interval", FOLLOW_ONLY)],
    },
    Form {
        label: "databases",
        command: CommandName::Databases,
        base: "databases",
        in_base: &[],
        inert: &[],
    },
    Form {
        label: "version",
        command: CommandName::Version,
        base: "version",
        in_base: &[],
        inert: &[],
    },
    Form {
        label: "ltx REPLICA_URL",
        command: CommandName::Ltx,
        base: "ltx s3://b/p",
        in_base: &[],
        inert: &[("-no-expand-env", GO_IGNORES_IT_BESIDE_A_URL)],
    },
    Form {
        label: "ltx DB_PATH",
        command: CommandName::Ltx,
        base: "ltx /data/db",
        in_base: &[],
        inert: &[],
    },
    Form {
        label: "wal REPLICA_URL",
        command: CommandName::Wal,
        base: "wal s3://b/p",
        in_base: &[],
        inert: &[("-no-expand-env", GO_IGNORES_IT_BESIDE_A_URL)],
    },
    Form {
        label: "wal DB_PATH",
        command: CommandName::Wal,
        base: "wal /data/db",
        in_base: &[],
        inert: &[],
    },
    Form {
        label: "verify dir",
        command: CommandName::Verify,
        base: "verify --replica r",
        in_base: &["--replica"],
        inert: &[],
    },
    Form {
        label: "verify s3",
        command: CommandName::Verify,
        base: "verify --replica s3://b/p",
        in_base: &["--replica"],
        inert: &[],
    },
    Form {
        label: "files dir",
        command: CommandName::Files,
        base: "files --replica r",
        in_base: &["--replica"],
        inert: &[],
    },
    Form {
        label: "files s3",
        command: CommandName::Files,
        base: "files --replica s3://b/p",
        in_base: &["--replica"],
        inert: &[],
    },
    Form {
        label: "trust-local dir",
        command: CommandName::TrustLocal,
        base: "trust-local app.db --replica r",
        in_base: &["--replica"],
        inert: &[],
    },
    Form {
        label: "trust-local s3",
        command: CommandName::TrustLocal,
        base: "trust-local app.db --replica s3://b/p",
        in_base: &["--replica"],
        inert: &[],
    },
];

/// Every flag a command's help lists, beside every way of naming a replica, either changes
/// what the command line means or is refused; a flag that is read and dropped is a request
/// the binary does not meet, and it must be an error instead (the owner's rule for
/// `--endpoint` beside a bare URL). The few flags Go itself takes and drops are named, with
/// why, and held to changing nothing.
#[test]
fn every_flag_changes_the_command_or_is_refused() {
    let mut walked = 0_usize;
    for form in FORMS {
        let flags = help_flags(form.command);
        assert!(!flags.is_empty(), "{}", form.label);
        for flag in flags {
            let name = flag.name.as_str();
            let inert = form.inert.iter().any(|(inert, _)| *inert == name);
            let base: Vec<String> = words(form.base);
            let (with, without) = if form.in_base.contains(&name) {
                // The line already uses it: dropping it, and its value, is the comparison.
                let mut dropped = Vec::new();
                let mut skip = 0;
                for word in &base {
                    if skip > 0 {
                        skip -= 1;
                    } else if word == name {
                        skip = usize::from(flag.value.is_some());
                    } else {
                        dropped.push(word.clone());
                    }
                }
                (base, dropped)
            } else {
                let mut with = base.clone();
                let mut add = vec![name.to_owned()];
                add.extend(sample(name).iter().map(|s| (*s).to_owned()));
                // Go's flag package stops at the first positional argument, so a Go-grammar
                // line takes its flags right after the command word; `lsr`'s own commands
                // take them anywhere after it.
                let at = match form.command {
                    CommandName::Restore
                    | CommandName::Databases
                    | CommandName::Ltx
                    | CommandName::Wal
                    | CommandName::Version => 1,
                    CommandName::Verify
                    | CommandName::Files
                    | CommandName::TrustLocal
                    | CommandName::Replicate
                    | CommandName::Config => with.len(),
                };
                with.splice(at..at, add);
                (with, base)
            };
            let after = parse(&with);
            let before = parse(&without);
            if inert {
                assert_eq!(
                    after, before,
                    "{}: {name} is listed as inert but is not",
                    form.label
                );
            } else {
                assert!(
                    after.is_err() || after != before,
                    "{}: {name} is read and dropped: {with:?} parses as it does without",
                    form.label
                );
            }
            walked += 1;
        }
    }
    assert!(walked > 100, "walked {walked} flags");
}

/// A flag name never runs into its help text: the column is as wide as the longest name
/// (`--clear-journal-files` once printed as `--clear-journal-filesfirst remove ...`).
#[test]
fn help_columns_leave_a_gap_after_every_flag() {
    for line in crate::render::help().lines() {
        let Some(row) = line.strip_prefix("  -") else {
            continue;
        };
        let name_end = row.find(' ').unwrap_or(row.len());
        assert!(
            row.get(name_end..)
                .is_some_and(|rest| rest.starts_with("  ")),
            "{line}"
        );
    }
}

/// Go has no `--endpoint`: a bare replica URL carries its endpoint in the query, so the flag
/// beside one (or beside a DB_PATH, whose config sets it) was read and dropped, and a restore
/// then went to AWS. It is refused now, with the query form named.
#[test]
fn an_s3_flag_beside_a_bare_url_or_database_is_refused_and_the_query_form_is_kept() {
    for flag in [
        "--endpoint http://h:9",
        "--region eu-west-1",
        "--force-path-style",
    ] {
        let name = flag.split(' ').next().unwrap_or_default();
        let line = format!("restore {flag} -o o s3://b/p");
        assert_eq!(
            usage(&line),
            UsageError::S3FlagWithUrl {
                flag: name.to_owned().leak()
            },
            "{line}"
        );
        let line = format!("restore {flag} /data/db");
        assert_eq!(
            usage(&line),
            UsageError::S3FlagWithDatabase {
                flag: name.to_owned().leak()
            },
            "{line}"
        );
    }
    let message = usage("restore --endpoint http://h:9 -o o s3://b/p").to_string();
    assert!(message.contains("?endpoint="), "{message}");
    // Go's form: the query rides on the URL, untouched, for the daemon's reader.
    assert_eq!(
        parsed("restore -o o s3://b/p?endpoint=http://h:9"),
        Ok(Command::Restore(Box::new(RestoreArgs {
            from: RestoreFrom::Url("s3://b/p?endpoint=http://h:9".into()),
            output: Some("o".into()),
            target: RestoreTarget::Latest,
            as_of_inside: AsOfInside::Refuse,
            clear_journal_files: false,
            integrity_check: IntegrityCheck::Off,
            if_db_not_exists: false,
            if_replica_exists: false,
            dry_run: false,
            json: false,
            force: false,
            parallelism: false,
            txid_and_timestamp: false,
            timestamp_given: false,
        })))
    );
    // `lsr`'s own `--replica` form still takes them, and `-o` with `--output` is one flag twice.
    parsed("restore --replica s3://b/p --endpoint http://h:9 -o o").unwrap();
    assert_eq!(
        usage("restore --replica r -o a --output b"),
        UsageError::DuplicateFlag { flag: "--output" }
    );
    assert_eq!(
        usage("restore --replica r --output o -config c.yml"),
        UsageError::ConfigFlagWithReplica { flag: "-config" }
    );
}
