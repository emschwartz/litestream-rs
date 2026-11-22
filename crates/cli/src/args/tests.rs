use lsr_store_s3::{Provider, UrlStyle};

use super::*;
use lsr_core::AsOfInside;

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
        Ok(Command::Restore(RestoreArgs {
            replica: Replica::Dir("r".into()),
            output: "o".into(),
            target: RestoreTarget::Latest,
            as_of_inside: AsOfInside::Refuse,
            clear_journal_files: false,
            integrity_check: IntegrityCheck::Off,
            if_db_not_exists: false,
            if_replica_exists: false,
            dry_run: false,
            json: false,
        }))
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
    assert_eq!(
        parsed("files --replica r --created-at"),
        Ok(Command::Files(FilesArgs {
            replica: Replica::Dir("r".into()),
            json: false,
            resolved: true,
        }))
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
fn missing_required_flag_for_every_required_flag() {
    let valid = [
        (CommandName::Restore, "restore --replica r --output o"),
        (CommandName::Verify, "verify --replica r"),
        (CommandName::Files, "files --replica r"),
        (CommandName::TakeOver, "take-over app.db --replica r"),
    ];
    let mut checked = 0;
    for (command, line) in valid {
        for spec in FLAGS
            .iter()
            .filter(|s| s.required && s.commands.contains(&command))
        {
            let mut args = words(line);
            let at = args.iter().position(|a| a == spec.name).unwrap();
            args.drain(at..=at + 1);
            assert_eq!(
                parse(&args),
                Err(UsageError::MissingFlag {
                    command: command.word(),
                    flag: spec.name
                })
            );
            checked += 1;
        }
    }
    assert_eq!(
        checked,
        FLAGS
            .iter()
            .filter(|s| s.required)
            .map(|s| s.commands.len())
            .sum::<usize>()
    );
}

#[test]
fn two_targets_at_once() {
    assert_eq!(
        usage(
            "restore --replica r --output o --txid 0000000000000001 --timestamp 2026-01-01T00:00:00Z"
        ),
        UsageError::ConflictingTargets
    );
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

#[test]
fn bad_txid() {
    assert_eq!(
        usage("restore --replica r --output o --txid 2a"),
        UsageError::BadTxid {
            value: "2a".into(),
            source: ParseTxidError::Length { found: 2 }
        }
    );
    assert!(matches!(
        usage("restore --replica r --output o --txid 0000000000000000"),
        UsageError::BadTxid {
            source: ParseTxidError::Zero,
            ..
        }
    ));
}

#[test]
fn bad_timestamp() {
    assert!(matches!(
        usage("restore --replica r --output o --timestamp yesterday"),
        UsageError::BadTimestamp { .. }
    ));
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
            UsageError::BadIntegrityCheck { value: word.into() },
            "{word}"
        );
    }
    assert_eq!(
        usage("restore --replica r --output o --integrity-check"),
        UsageError::MissingValue {
            flag: "--integrity-check"
        }
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
    // Departure: Go accepts a bucket with no path and builds keys with a leading
    // slash; an empty prefix is refused.
    assert_eq!(
        usage("files --replica s3://bucket"),
        UsageError::BadReplicaUrl {
            reason: ReplicaProblem::S3(ConfigError::EmptyPrefix)
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
            Ok(Command::Restore(RestoreArgs {
                replica: Replica::S3(config),
                ..
            })) => config,
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
            "files --replica s3://b/p --region No_Good",
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
fn every_flag_in_the_table_is_accepted_by_its_commands() {
    for spec in FLAGS {
        for command in spec.commands {
            let replica = if S3_ONLY_FLAGS.contains(&spec.name) {
                "s3://b/p"
            } else {
                "r"
            };
            let base = match command {
                CommandName::Restore if spec.name == "--allow-earlier" => {
                    format!(
                        "restore --replica {replica} --output o --timestamp 2026-01-01T00:00:00Z"
                    )
                }
                CommandName::Restore => format!("restore --replica {replica} --output o"),
                CommandName::Verify => format!("verify --replica {replica}"),
                CommandName::Files => format!("files --replica {replica}"),
                CommandName::TakeOver => format!("take-over app.db --replica {replica}"),
            };
            let value = match spec.name {
                "--endpoint" => "http://h",
                "--region" => "eu-west-1",
                "--txid" => "0000000000000001",
                "--timestamp" => "2026-01-01T00:00:00Z",
                "--depth" => "names",
                "--integrity-check" => "full",
                _ => "x",
            };
            let extra = spec.value.map_or(String::new(), |_| format!(" {value}"));
            let line = if spec.required {
                base
            } else {
                format!("{base} {}{extra}", spec.name)
            };
            assert!(parsed(&line).is_ok(), "{line}");
            let other: Vec<_> = CommandName::ALL
                .iter()
                .filter(|c| !spec.commands.contains(c))
                .collect();
            for command in other {
                let db = if *command == CommandName::TakeOver {
                    " app.db"
                } else {
                    ""
                };
                let line = format!(
                    "{}{db} --replica {replica} {}{extra}",
                    command.word(),
                    spec.name
                );
                assert!(
                    matches!(usage(&line), UsageError::UnknownFlag { .. }),
                    "{line}"
                );
            }
        }
    }
}

#[test]
fn help_documents_every_flag_command_and_exit_code() {
    let help = crate::render::help();
    for spec in FLAGS {
        assert!(
            help.contains(spec.name) && help.contains(spec.help),
            "{}",
            spec.name
        );
    }
    for command in CommandName::ALL {
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

#[test]
fn take_over_takes_the_database_first_then_its_flags() {
    assert_eq!(
        parsed("take-over app.db --replica r --meta-dir m"),
        Ok(Command::TakeOver(TakeOverArgs {
            database: PathBuf::from("app.db"),
            replica: Replica::Dir(PathBuf::from("r")),
            meta_dir: Some(PathBuf::from("m")),
        }))
    );
    assert_eq!(
        parsed("take-over --replica r"),
        Err(UsageError::MissingDatabase)
    );
    assert_eq!(parsed("take-over"), Err(UsageError::MissingDatabase));
}
