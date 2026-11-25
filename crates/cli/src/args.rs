//! Argument parsing: pure, no I/O, no process exit.
//!
//! `restore`, `databases` and `ltx` read Go's flag grammar (`restore.rs`, `inspect.rs`,
//! `lsr-sidecar`'s `goflag`), and each lists its flags for the help text in a table beside
//! the table of flags it defines; a test holds the two together. `lsr`'s own commands
//! (`verify`, `files`, `trust-local`, `config check`) are read by clap, built from
//! the [`Command`](clap::Command)s below; the help text lists their flags from the
//! same definitions, so it cannot document a flag the parser does not know. A
//! flag takes its value as the next argument (`--replica dir`); a value that
//! starts with `--` is read as a missing value, so a path like that is written
//! `./--name`. Clap's own help, version and error output are off: `main` prints
//! `lsr`'s help and one-line errors.
//!
//! `--replica` is a directory or an `s3://` URL, read by the daemon's own parser
//! (`lsr_config::S3Url`), so `lsr restore` accepts every URL a `replica: url:`
//! entry does, Go's host spellings included. The S3 flags (`--endpoint`,
//! `--region`, `--force-path-style`) override the URL as a replica entry's keys
//! do. Credentials are not parsed here (this module reads no environment);
//! `replica.rs` takes them from the AWS variables and shared files.
//!
//! `lsr replicate` is the daemon's command line, read by `lsr-sidecar`, Go's
//! two-argument form `lsr replicate DB_PATH REPLICA_URL` included.

use std::path::PathBuf;

use clap::error::{ContextKind, ErrorKind};
use clap::{Arg, ArgAction, ArgMatches};

use lsr_config::{ReplicaProblem, S3Overrides, S3Url, s3_config};
use lsr_core::Depth;
use lsr_store_s3::{ConfigError, Endpoint, Region, S3Config};
use thiserror::Error;

mod inspect;
mod restore;

pub use inspect::{DatabasesArgs, Inspect, LtxArgs, LtxFrom, VersionArgs};
pub use restore::{Restore, RestoreArgs, RestoreFrom};

/// Every word `lsr` takes first, in help order. The dispatcher in [`parse`] matches on it
/// exhaustively and the help text lists every one, so a command cannot be accepted and left
/// out of the help.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandName {
    /// `lsr restore`.
    Restore,
    /// `lsr databases`.
    Databases,
    /// `lsr ltx`.
    Ltx,
    /// `lsr wal`, Go's deprecated alias of `ltx`.
    Wal,
    /// `lsr verify`.
    Verify,
    /// `lsr files`.
    Files,
    /// `lsr trust-local <db>`.
    TrustLocal,
    /// `lsr replicate`.
    Replicate,
    /// `lsr config check <file>`.
    Config,
    /// `lsr version`, Go's `litestream version`.
    Version,
}

impl CommandName {
    /// Every command, in help order.
    pub const ALL: [Self; 10] = [
        Self::Restore,
        Self::Databases,
        Self::Ltx,
        Self::Wal,
        Self::Verify,
        Self::Files,
        Self::TrustLocal,
        Self::Replicate,
        Self::Config,
        Self::Version,
    ];

    /// The word typed after `lsr`.
    pub const fn word(self) -> &'static str {
        match self {
            Self::Restore => "restore",
            Self::Databases => "databases",
            Self::Ltx => "ltx",
            Self::Wal => "wal",
            Self::Verify => "verify",
            Self::Files => "files",
            Self::TrustLocal => "trust-local",
            Self::Replicate => "replicate",
            Self::Config => "config",
            Self::Version => "version",
        }
    }

    /// The command a first argument names, if it names one.
    pub fn from_word(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|command| command.word() == word)
    }

    /// The positional argument after the word, for the help text.
    pub const fn positional(self) -> Option<&'static str> {
        match self {
            Self::Restore => Some("[DB_PATH | REPLICA_URL]"),
            Self::Ltx | Self::Wal => Some("<DB_PATH | REPLICA_URL>"),
            Self::TrustLocal => Some("<db>"),
            Self::Databases
            | Self::Verify
            | Self::Files
            | Self::Replicate
            | Self::Config
            | Self::Version => None,
        }
    }

    /// The whole usage line of a command whose flags the help text does not list: the
    /// daemon's (`lsr replicate -h` lists them) and `config check`.
    pub const fn usage_line(self) -> Option<&'static str> {
        match self {
            Self::Replicate => Some(DAEMON_USAGE),
            Self::Config => Some(CONFIG_CHECK_USAGE),
            Self::Restore
            | Self::Databases
            | Self::Ltx
            | Self::Wal
            | Self::Verify
            | Self::Files
            | Self::TrustLocal
            | Self::Version => None,
        }
    }

    /// One line for the help text.
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Restore => {
                "rebuild a database from a replica, Go's way (a REPLICA_URL, or a DB_PATH the config names) or with --replica; never touches an existing file without -force"
            }
            Self::Databases => "list the databases of a config and the type of each one's replica",
            Self::Ltx => {
                "list the LTX files of a replica (a REPLICA_URL, or a DB_PATH the config names), level 0 unless -level says otherwise"
            }
            Self::Wal => "Go's deprecated name for ltx: warns, then does the same",
            Self::Verify => "check a replica for damage and gaps without restoring",
            Self::Files => "list every file of a replica with its size and stored time",
            Self::TrustLocal => {
                "trust the local database <db> over what the replica holds now, after a replicator refused replica files this database has no record of writing: records a takeover through the replica's newest transaction in <db>'s meta directory, so the next start replicates above them. If the replica is the right one, run lsr restore instead"
            }
            Self::Replicate => DAEMON_SUMMARY,
            Self::Config => CONFIG_CHECK_SUMMARY,
            Self::Version => {
                "print the version, as Go's version command does (a bare line); -verbose adds the build details lsr has"
            }
        }
    }
}

/// Every word `lsr` takes first, for the error that names them; a test holds it to
/// [`CommandName::ALL`].
const COMMAND_WORDS: &str =
    "replicate, restore, databases, ltx, wal, config, verify, files, trust-local, version";

/// One flag of a command's help text.
#[derive(Debug)]
pub struct HelpFlag {
    /// The flag with its dashes.
    pub name: String,
    /// The placeholder of its value, or `None` for a switch.
    pub value: Option<String>,
    /// Whether the command refuses to run without it.
    pub required: bool,
    /// One help line.
    pub help: String,
}

/// One flag of a Go-grammar command's help text, as a constant row. A test holds each table
/// to the flags its parser defines, both ways.
pub(super) struct FlagRow {
    /// The flag with its dashes.
    pub(super) name: &'static str,
    /// The placeholder of its value, or `None` for a switch.
    pub(super) value: Option<&'static str>,
    /// One help line.
    pub(super) help: &'static str,
}

impl FlagRow {
    fn help_flag(&self) -> HelpFlag {
        HelpFlag {
            name: self.name.to_owned(),
            value: self.value.map(str::to_owned),
            required: false,
            help: self.help.to_owned(),
        }
    }
}

/// Where the replica lives.
#[derive(Debug, PartialEq, Eq)]
pub enum Replica {
    /// A directory that holds `ltx/`.
    Dir(PathBuf),
    /// A bucket prefix; credentials come from the AWS environment variables or
    /// shared files at run time.
    S3(Box<S3Config>),
}

/// A parsed command line.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Rebuild a database.
    Restore(Box<RestoreArgs>),
    /// Go's `databases`: the config's databases and their replica types.
    Databases(DatabasesArgs),
    /// Go's `ltx` (or `wal`): a replica's LTX files.
    Ltx(LtxArgs),
    /// Check a replica.
    Verify(VerifyArgs),
    /// List a replica's files.
    Files(FilesArgs),
    /// Trust the local database over the replica: record a takeover.
    TrustLocal(TrustLocalArgs),
    /// Go's `replicate` daemon: the arguments after `replicate`, read by `lsr-sidecar`.
    Daemon(Vec<String>),
    /// `lsr config check <file>`.
    ConfigCheck(PathBuf),
    /// Print the help text.
    Help,
    /// Print the version, as `--version` does.
    Version,
    /// Go's `version` command: a bare version line, or `-verbose` details.
    VersionLine(VersionArgs),
}

/// The daemon's usage line, for the help text.
pub const DAEMON_USAGE: &str =
    "lsr replicate [-config <file>] [Go's replicate flags] [DB_PATH REPLICA_URL]";

/// One line for the help text.
pub const DAEMON_SUMMARY: &str = "Go's replicate daemon: every database in litestream.yml (default $LITESTREAM_CONFIG, else /etc/litestream.yml), or DB_PATH alone to REPLICA_URL (file:// or s3://, as Go), until a signal; lsr replicate -h lists the flags";

/// `lsr config check`'s usage line.
pub const CONFIG_CHECK_USAGE: &str = "lsr config check <file>";

/// One line for the help text.
pub const CONFIG_CHECK_SUMMARY: &str = "read a Go litestream.yml as the daemon would and print every refusal and warning a start would; writes nothing and touches no bucket, but scans a dir entry as a start does";

/// `lsr trust-local`.
#[derive(Debug, PartialEq, Eq)]
pub struct TrustLocalArgs {
    /// The database that may replicate over the replica.
    pub database: PathBuf,
    /// The replica.
    pub replica: Replica,
    /// Its meta directory, when not the default.
    pub meta_dir: Option<PathBuf>,
}

/// `lsr verify`.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifyArgs {
    /// The replica.
    pub replica: Replica,
    /// How much to read.
    pub depth: Depth,
    /// JSON instead of text.
    pub json: bool,
}

/// `lsr files`.
#[derive(Debug, PartialEq, Eq)]
pub struct FilesArgs {
    /// The replica.
    pub replica: Replica,
    /// JSON instead of text.
    pub json: bool,
    /// Read every file's stored time instead of printing the listed one.
    pub resolved: bool,
}

/// Why the command line is not a command.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum UsageError {
    /// Nothing after `lsr`.
    #[error("no command given")]
    NoCommand,
    /// `lsr trust-local` with no database path before its flags.
    #[error("trust-local needs the database path first: lsr trust-local <db> --replica <dir>")]
    MissingDatabase,
    /// The first argument is not a subcommand.
    #[error("unknown command {name:?}")]
    UnknownCommand {
        /// What was typed.
        name: String,
    },
    /// A flag before the command: `lsr -config c.yml replicate`.
    #[error(
        "flags go after the command, not before it: {flag:?} came first; write lsr <command> [flags], where <command> is one of {COMMAND_WORDS}"
    )]
    FlagBeforeCommand {
        /// What was typed first.
        flag: String,
    },
    /// A flag the command does not take.
    #[error("unknown flag {flag} for {command}")]
    UnknownFlag {
        /// The subcommand.
        command: &'static str,
        /// What was typed.
        flag: String,
    },
    /// A flag given twice.
    #[error("{flag} given more than once")]
    DuplicateFlag {
        /// The flag.
        flag: &'static str,
    },
    /// A flag with no value after it.
    #[error("{flag} needs a value")]
    MissingValue {
        /// The flag.
        flag: &'static str,
    },
    /// A required flag that was not given.
    #[error("{command} needs {flag}")]
    MissingFlag {
        /// The subcommand.
        command: &'static str,
        /// The flag.
        flag: &'static str,
    },
    /// An S3 flag next to a REPLICA_URL. Go has no such flag: its URL carries them as query
    /// keys, so the flag would change nothing.
    #[error(
        "{flag} does not apply to a REPLICA_URL: write it in the URL's query (s3://bucket/path?endpoint=http://host:9000, ?region=, ?forcePathStyle=true), as Go does, or give the replica with --replica"
    )]
    S3FlagWithUrl {
        /// The flag.
        flag: &'static str,
    },
    /// An S3 flag next to a DB_PATH, whose replica the config describes.
    #[error("{flag} does not apply to a DB_PATH: the config's replica sets it")]
    S3FlagWithDatabase {
        /// The flag.
        flag: &'static str,
    },
    /// `-config` or `-no-expand-env` next to `--replica`, which reads no config.
    #[error("{flag} reads the config that names a DB_PATH and does not go with --replica")]
    ConfigFlagWithReplica {
        /// The flag.
        flag: &'static str,
    },
    /// `--allow-earlier` without `--timestamp`.
    #[error("--allow-earlier needs --timestamp")]
    AllowEarlierWithoutTimestamp,
    /// A bare word where a flag belongs.
    #[error("unexpected argument {argument:?}")]
    UnexpectedArgument {
        /// What was typed.
        argument: String,
    },
    /// `--depth` is neither `names` nor `contents`.
    #[error("--depth {value:?}: expected names or contents")]
    BadDepth {
        /// What was typed.
        value: String,
    },
    /// `--replica` is a URL whose scheme is not `s3`.
    #[error("--replica {scheme}:// replicas are not supported; give a directory or an s3:// URL")]
    UnsupportedReplica {
        /// The scheme, never the rest of the URL (it may hold credentials).
        scheme: String,
    },
    /// `--replica s3://...` is not a usable bucket and prefix, as the daemon reads it.
    /// The reason names the shape that is wrong, never any part of the URL but a
    /// query key.
    #[error("--replica: {reason}")]
    BadReplicaUrl {
        /// What is wrong.
        reason: ReplicaProblem,
    },
    /// `--endpoint` or `--region` is not usable.
    #[error("{flag}: {reason}")]
    BadS3Flag {
        /// The flag.
        flag: &'static str,
        /// What is wrong.
        reason: ConfigError,
    },
    /// An S3 flag next to a directory replica.
    #[error("{flag} applies to s3:// replicas only")]
    S3FlagForDirectory {
        /// The flag.
        flag: &'static str,
    },
    /// `lsr config check` takes exactly one file.
    #[error(
        "config check takes one file, not {found} arguments: {}",
        CONFIG_CHECK_USAGE
    )]
    ConfigCheckArguments {
        /// How many were given.
        found: usize,
    },
    /// Go's flag package refused a `restore` argument.
    #[error("{0}")]
    GoFlag(lsr_sidecar::goflag::FlagError),
    /// `-help=<value>` or `--h`: help printed, and Go exits non-zero.
    #[error("help requested")]
    HelpNotLiteral,
    /// `restore` with neither a positional argument nor `--replica`.
    #[error("database path or replica URL required")]
    GoSourceMissing,
    /// More than one positional argument.
    #[error("too many arguments")]
    GoTooManyArguments,
    /// A positional argument and `--replica`.
    #[error("give a DB_PATH or REPLICA_URL, or --replica, not both")]
    ReplicaAndPositional,
    /// A replica URL and no `-o`.
    #[error("-o is required when restoring from a replica URL")]
    GoOutputRequired,
    /// A replica URL and `-config`.
    #[error("cannot specify a replica URL and the -config flag")]
    GoUrlAndConfig,
    /// Go's `-integrity-check` refusal.
    #[error("invalid -integrity-check value: {value}")]
    GoIntegrityCheck {
        /// What was typed.
        value: String,
    },
    /// Go's `-timestamp` refusal.
    #[error("invalid -timestamp, must specify in ISO 8601 format (e.g. 2000-01-01T00:00:00Z)")]
    GoTimestamp,
    /// A non-zero `-txid` and a `-timestamp`.
    #[error("cannot specify index & timestamp to restore")]
    GoTxidAndTimestamp,
    /// `-f`, Go's follow mode, which is not built.
    #[error("-f (follow mode) is not built in litestream-rs yet (issue #177)")]
    FollowNotBuilt,
    /// An argument that is not UTF-8, which a flag value cannot be.
    #[error("argument {position} is not valid UTF-8")]
    NotUtf8 {
        /// 1-based position after `lsr`.
        position: usize,
    },
}

fn is_help(arg: &str) -> bool {
    matches!(arg, "-h" | "--help")
}

/// A flag's value is the next argument, unless that is another `--flag`.
fn flag_value(raw: &str) -> Result<String, String> {
    if raw.starts_with("--") {
        Err("a flag, not a value".to_owned())
    } else {
        Ok(raw.to_owned())
    }
}

fn value_flag(name: &'static str, value: &'static str, help: &'static str) -> Arg {
    Arg::new(name)
        .long(name)
        .value_name(value)
        .help(help)
        .num_args(1)
        .allow_hyphen_values(true)
        .value_parser(flag_value)
}

fn switch(name: &'static str, help: &'static str) -> Arg {
    Arg::new(name)
        .long(name)
        .help(help)
        .action(ArgAction::SetTrue)
}

/// The flags every command that names a replica takes.
fn replica_flags(command: clap::Command) -> clap::Command {
    command
        .arg(
            value_flag(
                "replica",
                "<dir>",
                "the replica: a directory that holds ltx/, or s3://<bucket>/<prefix>",
            )
            .required(true),
        )
        .arg(value_flag(
            "endpoint",
            "<url>",
            "s3 only: the service endpoint (http or https), for anything but AWS",
        ))
        .arg(value_flag(
            "region",
            "<name>",
            "s3 only: the signing region (default us-east-1)",
        ))
        .arg(switch(
            "force-path-style",
            "s3 only: address the bucket as endpoint/bucket instead of bucket.endpoint",
        ))
}

/// The clap definition of one of `lsr`'s own commands; `None` for `restore`, which reads
/// Go's grammar.
fn own_command(command: CommandName) -> Option<clap::Command> {
    let base = clap::Command::new(command.word())
        .no_binary_name(true)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .disable_help_subcommand(true);
    match command {
        CommandName::Restore
        | CommandName::Databases
        | CommandName::Ltx
        | CommandName::Wal
        | CommandName::Replicate
        | CommandName::Config
        | CommandName::Version => None,
        CommandName::Verify => Some(
            replica_flags(base)
                .arg(value_flag(
                    "depth",
                    "names|contents",
                    "names lists only; contents (the default) reads and decodes every file",
                ))
                .arg(switch("json", "print one JSON object instead of text")),
        ),
        CommandName::Files => Some(
            replica_flags(base)
                .arg(switch(
                    "created-at",
                    "s3 only: read each file's stored time with one HEAD per file (default: the listed times, no HEADs)",
                ))
                .arg(switch("json", "print one JSON object instead of text")),
        ),
        CommandName::TrustLocal => Some(replica_flags(base).arg(value_flag(
            "meta-dir",
            "<dir>",
            "the database's meta directory when its config sets one (default .<name>-litestream beside it)",
        ))),
    }
}

/// The flags of `command` for the help text, in the order they are listed.
pub fn help_flags(command: CommandName) -> Vec<HelpFlag> {
    let Some(own) = own_command(command) else {
        let rows = match command {
            CommandName::Restore => restore::HELP,
            CommandName::Databases => inspect::DATABASES_HELP,
            CommandName::Ltx | CommandName::Wal => inspect::LTX_HELP,
            CommandName::Version => inspect::VERSION_HELP,
            CommandName::Replicate | CommandName::Config => &[],
            CommandName::Verify | CommandName::Files | CommandName::TrustLocal => &[],
        };
        return rows.iter().map(FlagRow::help_flag).collect();
    };
    own.get_arguments()
        .map(|arg| HelpFlag {
            name: format!("--{}", arg.get_id()),
            value: arg
                .get_value_names()
                .and_then(<[_]>::first)
                .map(ToString::to_string),
            required: arg.is_required_set(),
            help: arg.get_help().map(ToString::to_string).unwrap_or_default(),
        })
        .collect()
}

/// The names of `lsr`'s own flags, to give a clap error the `&'static str` a [`UsageError`] holds.
const OWN_FLAGS: [&str; 8] = [
    "--replica",
    "--endpoint",
    "--region",
    "--force-path-style",
    "--meta-dir",
    "--depth",
    "--created-at",
    "--json",
];

/// The `UsageError` for a clap refusal, by the kind of refusal and the argument it names.
fn usage_error(command: CommandName, error: &clap::Error) -> UsageError {
    let arg = error
        .get(ContextKind::InvalidArg)
        .map(ToString::to_string)
        .unwrap_or_default();
    let flag = OWN_FLAGS
        .into_iter()
        .find(|known| arg.split_whitespace().next() == Some(known));
    match (error.kind(), flag) {
        (ErrorKind::ArgumentConflict, Some(flag)) => UsageError::DuplicateFlag { flag },
        (
            ErrorKind::InvalidValue | ErrorKind::ValueValidation | ErrorKind::WrongNumberOfValues,
            Some(flag),
        ) => UsageError::MissingValue { flag },
        (ErrorKind::MissingRequiredArgument, Some(flag)) => UsageError::MissingFlag {
            command: command.word(),
            flag,
        },
        (ErrorKind::UnknownArgument, _) if arg.starts_with('-') && arg != "-" => {
            UsageError::UnknownFlag {
                command: command.word(),
                flag: arg,
            }
        }
        _ => UsageError::UnexpectedArgument { argument: arg },
    }
}

/// Reads the arguments after a command's word, or `None` when help was asked for first.
/// A `-h` or `--help` in flag position (not another flag's value) wins over what follows
/// it, and over a missing required flag, but not over a refusal before it.
fn read_own(
    command: CommandName,
    cli: clap::Command,
    rest: &[String],
) -> Result<Option<ArgMatches>, UsageError> {
    let value_flags: Vec<String> = cli
        .get_arguments()
        .filter(|arg| arg.get_action().takes_values())
        .map(|arg| format!("--{}", arg.get_id()))
        .collect();
    let help_at = rest.iter().enumerate().position(|(at, arg)| {
        is_help(arg)
            && !at
                .checked_sub(1)
                .is_some_and(|b| rest.get(b).is_some_and(|p| value_flags.contains(p)))
    });
    let (read, cli) = match help_at {
        Some(at) => (
            rest.get(..at).unwrap_or_default(),
            cli.mut_args(|arg| arg.required(false)),
        ),
        None => (rest, cli),
    };
    let matches = cli
        .try_get_matches_from(read)
        .map_err(|error| usage_error(command, &error))?;
    Ok(help_at.is_none().then_some(matches))
}

fn replica_of_matches(matches: &ArgMatches) -> Result<Replica, UsageError> {
    let value = |name: &str| matches.get_one::<String>(name).cloned();
    replica_of(
        &value("replica").unwrap_or_default(),
        value("endpoint"),
        value("region"),
        matches.get_flag("force-path-style"),
    )
}

fn depth_of(matches: &ArgMatches) -> Result<Depth, UsageError> {
    match matches.get_one::<String>("depth").map(String::as_str) {
        None | Some("contents") => Ok(Depth::Contents),
        Some("names") => Ok(Depth::Names),
        Some(other) => Err(UsageError::BadDepth {
            value: other.to_owned(),
        }),
    }
}

/// A `--replica` value: a directory, or an `s3://` URL as the daemon reads it with the S3
/// flags in place of a replica entry's keys, so a flag overrides the URL's host and query.
fn replica_of(
    value: &str,
    endpoint: Option<String>,
    region: Option<String>,
    force_path_style: bool,
) -> Result<Replica, UsageError> {
    match value.split_once("://") {
        None => {
            let given = [
                ("--endpoint", endpoint.is_some()),
                ("--region", region.is_some()),
                ("--force-path-style", force_path_style),
            ];
            if let Some((flag, _)) = given.iter().find(|(_, set)| *set) {
                return Err(UsageError::S3FlagForDirectory { flag });
            }
            Ok(Replica::Dir(PathBuf::from(value)))
        }
        Some((scheme, _)) if scheme.eq_ignore_ascii_case("s3") => s3_replica(
            value,
            endpoint.as_deref(),
            region.as_deref(),
            force_path_style,
        ),
        Some((scheme, _)) => Err(UsageError::UnsupportedReplica {
            scheme: scheme.to_owned(),
        }),
    }
}

fn s3_replica(
    url: &str,
    endpoint: Option<&str>,
    region: Option<&str>,
    force_path_style: bool,
) -> Result<Replica, UsageError> {
    if let Some(endpoint) = endpoint {
        Endpoint::new(endpoint).map_err(|reason| UsageError::BadS3Flag {
            flag: "--endpoint",
            reason,
        })?;
    }
    if let Some(region) = region {
        Region::new(region).map_err(|reason| UsageError::BadS3Flag {
            flag: "--region",
            reason,
        })?;
    }
    let over = S3Overrides {
        region,
        endpoint,
        force_path_style: force_path_style.then_some(true),
        ..S3Overrides::default()
    };
    let bad = |reason| UsageError::BadReplicaUrl { reason };
    let parsed = S3Url::parse(url).map_err(bad)?;
    let config = s3_config(Some(parsed), &over).map_err(bad)?;
    Ok(Replica::S3(Box::new(config)))
}

/// Reads a command line (without the program name).
///
/// # Errors
/// [`UsageError`] naming the first thing wrong.
pub fn parse(args: &[String]) -> Result<Command, UsageError> {
    let Some(first) = args.first() else {
        return Err(UsageError::NoCommand);
    };
    let rest = args.get(1..).unwrap_or_default();
    match first.as_str() {
        "-h" | "--help" | "help" => return Ok(Command::Help),
        "-V" | "--version" => return Ok(Command::Version),
        _ => {}
    }
    let Some(command) = CommandName::from_word(first) else {
        return Err(if first.starts_with('-') {
            UsageError::FlagBeforeCommand {
                flag: first.clone(),
            }
        } else {
            UsageError::UnknownCommand {
                name: first.clone(),
            }
        });
    };
    match command {
        CommandName::Restore => match restore::parse(rest)? {
            Restore::Run(args) => Ok(Command::Restore(args)),
            Restore::Help => Ok(Command::Help),
        },
        CommandName::Databases => match inspect::parse_databases(rest)? {
            Inspect::Run(args) => Ok(Command::Databases(args)),
            Inspect::Help => Ok(Command::Help),
        },
        CommandName::Ltx | CommandName::Wal => {
            match inspect::parse_ltx(rest, command == CommandName::Wal)? {
                Inspect::Run(args) => Ok(Command::Ltx(args)),
                Inspect::Help => Ok(Command::Help),
            }
        }
        CommandName::Verify | CommandName::Files | CommandName::TrustLocal => {
            parse_own(command, rest)
        }
        CommandName::Version => match inspect::parse_version(rest)? {
            Inspect::Run(args) => Ok(Command::VersionLine(args)),
            Inspect::Help => Ok(Command::Help),
        },
        CommandName::Replicate => Ok(Command::Daemon(rest.to_vec())),
        CommandName::Config => parse_config(rest),
    }
}

/// `verify`, `files` and `trust-local <db>`: read by clap. `trust-local` takes the database path
/// first, then its flags.
fn parse_own(command: CommandName, rest: &[String]) -> Result<Command, UsageError> {
    let (database, flags) = match (command, rest.split_first()) {
        (CommandName::TrustLocal, Some((first, _))) if is_help(first) => return Ok(Command::Help),
        (CommandName::TrustLocal, Some((first, flags))) if !first.starts_with('-') => {
            (PathBuf::from(first), flags)
        }
        (CommandName::TrustLocal, _) => return Err(UsageError::MissingDatabase),
        (_, _) => (PathBuf::new(), rest),
    };
    let Some(cli) = own_command(command) else {
        return Err(UsageError::NoCommand);
    };
    let Some(matches) = read_own(command, cli, flags)? else {
        return Ok(Command::Help);
    };
    let json = matches.try_get_one::<bool>("json").ok().flatten().copied() == Some(true);
    Ok(match command {
        CommandName::Verify => Command::Verify(VerifyArgs {
            replica: replica_of_matches(&matches)?,
            depth: depth_of(&matches)?,
            json,
        }),
        CommandName::Files => {
            let replica = replica_of_matches(&matches)?;
            let resolved = matches.get_flag("created-at");
            // A directory lists the times it has and there is nothing to read one by one, so
            // the flag would change nothing there: refused, as the S3 flags are.
            if resolved && matches!(replica, Replica::Dir(_)) {
                return Err(UsageError::S3FlagForDirectory {
                    flag: "--created-at",
                });
            }
            Command::Files(FilesArgs {
                replica,
                json,
                resolved,
            })
        }
        CommandName::TrustLocal => Command::TrustLocal(TrustLocalArgs {
            database,
            replica: replica_of_matches(&matches)?,
            meta_dir: matches.get_one::<String>("meta-dir").map(PathBuf::from),
        }),
        CommandName::Restore
        | CommandName::Databases
        | CommandName::Ltx
        | CommandName::Wal
        | CommandName::Replicate
        | CommandName::Config
        | CommandName::Version => return Err(UsageError::NoCommand),
    })
}

/// `config check <file>`.
fn parse_config(rest: &[String]) -> Result<Command, UsageError> {
    let check = clap::Command::new("check")
        .no_binary_name(true)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .disable_help_subcommand(true)
        .arg(Arg::new("file").required(true));
    match rest.split_first() {
        None => Err(UsageError::NoCommand),
        Some((word, _)) if is_help(word) => Ok(Command::Help),
        Some((word, args)) if word == "check" => {
            if args.iter().any(|arg| is_help(arg)) {
                return Ok(Command::Help);
            }
            let found = args.len();
            let matches = check
                .try_get_matches_from(args)
                .map_err(|_refused| UsageError::ConfigCheckArguments { found })?;
            let file = matches
                .get_one::<String>("file")
                .cloned()
                .unwrap_or_default();
            Ok(Command::ConfigCheck(PathBuf::from(file)))
        }
        Some((other, _)) => Err(UsageError::UnknownCommand {
            name: format!("config {other}"),
        }),
    }
}

#[cfg(test)]
mod tests;
