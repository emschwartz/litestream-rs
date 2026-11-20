use super::*;

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
            replica: "r".into(),
            output: "o".into(),
            target: RestoreTarget::Latest,
            clear_sidecars: false,
        }))
    );
}

#[test]
fn restore_reads_every_flag_in_any_order() {
    let Ok(Command::Restore(args)) =
        parsed("restore --clear-sidecars --txid 000000000000002a --output o --replica r")
    else {
        panic!("did not parse");
    };
    assert_eq!(args.target, RestoreTarget::Txid(txid(42)));
    assert!(args.clear_sidecars);
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
            replica: "r".into(),
            depth: Depth::Contents,
            json: false,
        }))
    );
    assert_eq!(
        parsed("verify --json --depth names --replica r"),
        Ok(Command::Verify(VerifyArgs {
            replica: "r".into(),
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
            replica: "r".into(),
            json: true,
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

#[test]
fn a_url_replica_is_refused_by_scheme_only() {
    assert_eq!(
        usage("verify --replica s3://key:secret@bucket/path"),
        UsageError::UnsupportedReplica {
            scheme: "s3".into()
        }
    );
    assert!(
        !usage("files --replica https://x/y")
            .to_string()
            .contains("x/y")
    );
}

#[test]
fn every_flag_in_the_table_is_accepted_by_its_commands() {
    for spec in FLAGS {
        for command in spec.commands {
            let base = match command {
                CommandName::Restore => "restore --replica r --output o",
                CommandName::Verify => "verify --replica r",
                CommandName::Files => "files --replica r",
            };
            let value = match spec.name {
                "--txid" => "0000000000000001",
                "--timestamp" => "2026-01-01T00:00:00Z",
                "--depth" => "names",
                _ => "x",
            };
            let extra = spec.value.map_or(String::new(), |_| format!(" {value}"));
            let line = if spec.required {
                base.to_owned()
            } else {
                format!("{base} {}{extra}", spec.name)
            };
            assert!(parsed(&line).is_ok(), "{line}");
            let other: Vec<_> = CommandName::ALL
                .iter()
                .filter(|c| !spec.commands.contains(c))
                .collect();
            for command in other {
                let line = format!("{} --replica r {}{extra}", command.word(), spec.name);
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
