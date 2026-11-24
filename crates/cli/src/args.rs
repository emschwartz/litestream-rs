//! Argument parsing: pure, no I/O, no process exit.
//!
//! `restore` reads Go's flag grammar (`restore.rs`, `lsr-sidecar`'s `goflag`), and its
//! flags are listed for the help text in [`RESTORE_FLAGS`]. `lsr`'s own commands
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

pub use inspect::{DatabasesArgs, Inspect, LtxArgs, LtxFrom};
pub use restore::{Restore, RestoreArgs, RestoreFrom};

/// The three subcommands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandName {
    /// `lsr restore`.
    Restore,
    /// `lsr verify`.
    Verify,
    /// `lsr files`.
    Files,
    /// `lsr trust-local <db>`.
    TrustLocal,
}

impl CommandName {
    /// Every subcommand, in help order.
    pub const ALL: [Self; 4] = [Self::Restore, Self::Verify, Self::Files, Self::TrustLocal];

    /// The word typed after `lsr`.
    pub const fn word(self) -> &'static str {
        match self {
            Self::Restore => "restore",
            Self::Verify => "verify",
            Self::Files => "files",
            Self::TrustLocal => "trust-local",
        }
    }

    /// The positional argument after the word, for the help text.
    pub const fn positional(self) -> Option<&'static str> {
        match self {
            Self::Restore | Self::Verify | Self::Files => None,
            Self::TrustLocal => Some("<db>"),
        }
    }

    /// One line for the help text.
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Restore => "rebuild a database from a replica; never touches an existing file",
            Self::Verify => "check a replica for damage and gaps without restoring",
            Self::Files => "list every file of a replica with its size and stored time",
            Self::TrustLocal => {
                "trust the local database <db> over what the replica holds now, after a replicator refused replica files this database has no record of writing: records a takeover through the replica's newest transaction in <db>'s meta directory, so the next start replicates above them. If the replica is the right one, run lsr restore instead"
            }
        }
    }
}

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

/// The flag of `restore`'s help text, as a constant row.
struct RestoreFlag {
    name: &'static str,
    value: Option<&'static str>,
    required: bool,
    help: &'static str,
}

/// Every flag `restore` accepts, for the help text; the parser is Go's flag grammar.
const RESTORE_FLAGS: &[RestoreFlag] = &[
    RestoreFlag {
        name: "--replica",
        value: Some("<dir>"),
        required: true,
        help: "the replica: a directory that holds ltx/, or s3://<bucket>/<prefix>",
    },
    RestoreFlag {
        name: "--endpoint",
        value: Some("<url>"),
        required: false,
        help: "s3 only: the service endpoint (http or https), for anything but AWS",
    },
    RestoreFlag {
        name: "--region",
        value: Some("<name>"),
        required: false,
        help: "s3 only: the signing region (default us-east-1)",
    },
    RestoreFlag {
        name: "--force-path-style",
        value: None,
        required: false,
        help: "s3 only: address the bucket as endpoint/bucket instead of bucket.endpoint",
    },
    RestoreFlag {
        name: "--output",
        value: Some("<path>"),
        required: false,
        help: "the database to create, as Go's -o; with a DB_PATH the default is DB_PATH itself; an existing empty file is replaced, any other refused without -force",
    },
    RestoreFlag {
        name: "--txid",
        value: Some("<16 hex digits>"),
        required: false,
        help: "restore the state right after this transaction (not with --timestamp)",
    },
    RestoreFlag {
        name: "--timestamp",
        value: Some("<rfc3339>"),
        required: false,
        help: "restore the newest state created before this instant (not with --txid)",
    },
    RestoreFlag {
        name: "--allow-earlier",
        value: None,
        required: false,
        help: "With --timestamp, restore the newest state provably before the instant when the instant falls inside a compacted file, instead of refusing.",
    },
    RestoreFlag {
        name: "--clear-journal-files",
        value: None,
        required: false,
        help: "first remove a stale -wal, -shm or -journal beside the output",
    },
    RestoreFlag {
        name: "--integrity-check",
        value: Some("none|quick|full"),
        required: false,
        help: "run SQLite's quick_check or integrity_check on the result before publishing it; a failure publishes nothing (default none)",
    },
    RestoreFlag {
        name: "--if-db-not-exists",
        value: None,
        required: false,
        help: "exit 0 without restoring when something already exists at the output",
    },
    RestoreFlag {
        name: "--if-replica-exists",
        value: None,
        required: false,
        help: "exit 0 without restoring when the replica holds no LTX file (a directory with no ltx/ included, as in Go); a target a replica with files cannot meet is still refused; --dry-run ignores it, as Go's does",
    },
    RestoreFlag {
        name: "--dry-run",
        value: None,
        required: false,
        help: "check the output and print the files a restore would apply, writing and removing nothing",
    },
    RestoreFlag {
        name: "-o",
        value: Some("<path>"),
        required: false,
        help: "Go's spelling of --output",
    },
    RestoreFlag {
        name: "-config",
        value: Some("<file>"),
        required: false,
        help: "with a DB_PATH, the config that names it (default $LITESTREAM_CONFIG, else /etc/litestream.yml; - reads standard input); not with a REPLICA_URL",
    },
    RestoreFlag {
        name: "-no-expand-env",
        value: None,
        required: false,
        help: "read the config without expanding environment variables",
    },
    RestoreFlag {
        name: "-force",
        value: None,
        required: false,
        help: "replace an existing output and remove its -wal, -shm and -journal; the old file stays until the restored one is complete and checked, then is replaced in one rename",
    },
    RestoreFlag {
        name: "-parallelism",
        value: Some("<n>"),
        required: false,
        help: "accepted with a warning and no effect, as Go",
    },
    RestoreFlag {
        name: "-follow-interval",
        value: Some("<duration>"),
        required: false,
        help: "accepted for Go's -f, which is not built yet (#177); -f itself is refused",
    },
    RestoreFlag {
        name: "--json",
        value: None,
        required: false,
        help: "print one JSON object instead of text",
    },
];

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
    /// Print the version.
    Version,
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
        CommandName::Restore => None,
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
        return RESTORE_FLAGS
            .iter()
            .map(|flag| HelpFlag {
                name: flag.name.to_owned(),
                value: flag.value.map(str::to_owned),
                required: flag.required,
                help: flag.help.to_owned(),
            })
            .collect();
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
        "-h" | "--help" | "help" => Ok(Command::Help),
        "-V" | "--version" => Ok(Command::Version),
        "restore" => match restore::parse(rest)? {
            Restore::Run(args) => Ok(Command::Restore(args)),
            Restore::Help => Ok(Command::Help),
        },
        "databases" => match inspect::parse_databases(rest)? {
            Inspect::Run(args) => Ok(Command::Databases(args)),
            Inspect::Help => Ok(Command::Help),
        },
        word @ ("ltx" | "wal") => match inspect::parse_ltx(rest, word == "wal")? {
            Inspect::Run(args) => Ok(Command::Ltx(args)),
            Inspect::Help => Ok(Command::Help),
        },
        "verify" => parse_own(CommandName::Verify, rest),
        "files" => parse_own(CommandName::Files, rest),
        "trust-local" => parse_own(CommandName::TrustLocal, rest),
        "replicate" => Ok(Command::Daemon(rest.to_vec())),
        "config" => parse_config(rest),
        other => Err(UsageError::UnknownCommand {
            name: other.to_owned(),
        }),
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
        CommandName::Files => Command::Files(FilesArgs {
            replica: replica_of_matches(&matches)?,
            json,
            resolved: matches.get_flag("created-at"),
        }),
        CommandName::TrustLocal => Command::TrustLocal(TrustLocalArgs {
            database,
            replica: replica_of_matches(&matches)?,
            meta_dir: matches.get_one::<String>("meta-dir").map(PathBuf::from),
        }),
        CommandName::Restore => return Err(UsageError::NoCommand),
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
