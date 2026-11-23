//! Argument parsing: pure, no I/O, no process exit.
//!
//! One table, [`FLAGS`], names every flag with the commands that take it, its
//! value placeholder, whether it is required and its help line. The parser
//! reads the table and the help text is generated from it, so neither can
//! accept or document a flag the other does not know. A flag takes its value
//! as the next argument (`--replica dir`); a value that starts with `--` is
//! read as a missing value, so a path like that is written `./--name`.
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
    /// `lsr take-over <db>`.
    TakeOver,
}

impl CommandName {
    /// Every subcommand, in help order.
    pub const ALL: [Self; 4] = [Self::Restore, Self::Verify, Self::Files, Self::TakeOver];

    /// The word typed after `lsr`.
    pub const fn word(self) -> &'static str {
        match self {
            Self::Restore => "restore",
            Self::Verify => "verify",
            Self::Files => "files",
            Self::TakeOver => "take-over",
        }
    }

    /// The positional argument after the word, for the help text.
    pub const fn positional(self) -> Option<&'static str> {
        match self {
            Self::Restore | Self::Verify | Self::Files => None,
            Self::TakeOver => Some("<db>"),
        }
    }

    /// One line for the help text.
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Restore => "rebuild a database from a replica; never touches an existing file",
            Self::Verify => "check a replica for damage and gaps without restoring",
            Self::Files => "list every file of a replica with its size and stored time",
            Self::TakeOver => {
                "let <db> replicate over what the replica holds now, after a replicator refused files it has no record of writing: records a takeover through the replica's newest transaction in <db>'s meta directory"
            }
        }
    }
}

/// One flag of the command line.
#[derive(Debug)]
pub struct FlagSpec {
    /// The flag with its dashes.
    pub name: &'static str,
    /// The placeholder of its value, or `None` for a switch.
    pub value: Option<&'static str>,
    /// Whether the command refuses to run without it.
    pub required: bool,
    /// The commands that take it.
    pub commands: &'static [CommandName],
    /// One help line.
    pub help: &'static str,
}

/// Every flag the parser accepts.
pub const FLAGS: &[FlagSpec] = &[
    FlagSpec {
        name: "--replica",
        value: Some("<dir>"),
        required: true,
        commands: &[
            CommandName::Restore,
            CommandName::Verify,
            CommandName::Files,
            CommandName::TakeOver,
        ],
        help: "the replica: a directory that holds ltx/, or s3://<bucket>/<prefix>",
    },
    FlagSpec {
        name: "--endpoint",
        value: Some("<url>"),
        required: false,
        commands: &[
            CommandName::Restore,
            CommandName::Verify,
            CommandName::Files,
            CommandName::TakeOver,
        ],
        help: "s3 only: the service endpoint (http or https), for anything but AWS",
    },
    FlagSpec {
        name: "--region",
        value: Some("<name>"),
        required: false,
        commands: &[
            CommandName::Restore,
            CommandName::Verify,
            CommandName::Files,
            CommandName::TakeOver,
        ],
        help: "s3 only: the signing region (default us-east-1)",
    },
    FlagSpec {
        name: "--force-path-style",
        value: None,
        required: false,
        commands: &[
            CommandName::Restore,
            CommandName::Verify,
            CommandName::Files,
            CommandName::TakeOver,
        ],
        help: "s3 only: address the bucket as endpoint/bucket instead of bucket.endpoint",
    },
    FlagSpec {
        name: "--meta-dir",
        value: Some("<dir>"),
        required: false,
        commands: &[CommandName::TakeOver],
        help: "the database's meta directory when its config sets one (default .<name>-litestream beside it)",
    },
    FlagSpec {
        name: "--output",
        value: Some("<path>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "the database to create, as Go's -o; with a DB_PATH the default is DB_PATH itself; an existing empty file is replaced, any other refused without -force",
    },
    FlagSpec {
        name: "--txid",
        value: Some("<16 hex digits>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "restore the state right after this transaction (not with --timestamp)",
    },
    FlagSpec {
        name: "--timestamp",
        value: Some("<rfc3339>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "restore the newest state created before this instant (not with --txid)",
    },
    FlagSpec {
        name: "--allow-earlier",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "With --timestamp, restore the newest state provably before the instant when the instant falls inside a compacted file, instead of refusing.",
    },
    FlagSpec {
        name: "--clear-journal-files",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "first remove a stale -wal, -shm or -journal beside the output",
    },
    FlagSpec {
        name: "--integrity-check",
        value: Some("none|quick|full"),
        required: false,
        commands: &[CommandName::Restore],
        help: "run SQLite's quick_check or integrity_check on the result before publishing it; a failure publishes nothing (default none)",
    },
    FlagSpec {
        name: "--if-db-not-exists",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "exit 0 without restoring when something already exists at the output",
    },
    FlagSpec {
        name: "--if-replica-exists",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "exit 0 without restoring when the replica holds no LTX file (a directory with no ltx/ included, as in Go); a target a replica with files cannot meet is still refused; --dry-run ignores it, as Go's does",
    },
    FlagSpec {
        name: "--dry-run",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "check the output and print the files a restore would apply, writing and removing nothing",
    },
    FlagSpec {
        name: "-o",
        value: Some("<path>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "Go's spelling of --output",
    },
    FlagSpec {
        name: "-config",
        value: Some("<file>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "with a DB_PATH, the config that names it (default $LITESTREAM_CONFIG, else /etc/litestream.yml; - reads standard input); not with a REPLICA_URL",
    },
    FlagSpec {
        name: "-no-expand-env",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "read the config without expanding environment variables",
    },
    FlagSpec {
        name: "-force",
        value: None,
        required: false,
        commands: &[CommandName::Restore],
        help: "replace an existing output and remove its -wal, -shm and -journal; the old file stays until the restored one is complete and checked, then is replaced in one rename",
    },
    FlagSpec {
        name: "-parallelism",
        value: Some("<n>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "accepted with a warning and no effect, as Go",
    },
    FlagSpec {
        name: "-follow-interval",
        value: Some("<duration>"),
        required: false,
        commands: &[CommandName::Restore],
        help: "accepted for Go's -f, which is not built yet (#177); -f itself is refused",
    },
    FlagSpec {
        name: "--depth",
        value: Some("names|contents"),
        required: false,
        commands: &[CommandName::Verify],
        help: "names lists only; contents (the default) reads and decodes every file",
    },
    FlagSpec {
        name: "--created-at",
        value: None,
        required: false,
        commands: &[CommandName::Files],
        help: "s3 only: read each file's stored time with one HEAD per file (default: the listed times, no HEADs)",
    },
    FlagSpec {
        name: "--json",
        value: None,
        required: false,
        commands: &[
            CommandName::Restore,
            CommandName::Verify,
            CommandName::Files,
        ],
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

/// The flags that only mean something for an S3 replica.
#[cfg(test)]
const S3_ONLY_FLAGS: [&str; 3] = ["--endpoint", "--region", "--force-path-style"];

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
    /// Record a takeover.
    TakeOver(TakeOverArgs),
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

/// `config check <file>`.
fn parse_config(rest: &[String]) -> Result<Command, UsageError> {
    match rest {
        [check, file] if check == "check" && !is_help(file) => {
            Ok(Command::ConfigCheck(PathBuf::from(file)))
        }
        [check, ..] if check == "check" && rest.iter().any(|a| is_help(a)) => Ok(Command::Help),
        [check, ..] if check == "check" => Err(UsageError::ConfigCheckArguments {
            found: rest.len().saturating_sub(1),
        }),
        [] => Err(UsageError::NoCommand),
        [other, ..] if is_help(other) => Ok(Command::Help),
        [other, ..] => Err(UsageError::UnknownCommand {
            name: format!("config {other}"),
        }),
    }
}

/// `lsr take-over`.
#[derive(Debug, PartialEq, Eq)]
pub struct TakeOverArgs {
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
    /// `lsr take-over` with no database path before its flags.
    #[error("take-over needs the database path first: lsr take-over <db> --replica <dir>")]
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

/// What the scan of one subcommand's arguments found.
enum Scan {
    Help,
    Flags(Vec<(&'static str, Option<String>)>),
}

fn is_help(arg: &str) -> bool {
    matches!(arg, "-h" | "--help")
}

fn scan(command: CommandName, rest: &[String]) -> Result<Scan, UsageError> {
    let mut seen: Vec<(&'static str, Option<String>)> = Vec::new();
    let mut args = rest.iter();
    while let Some(arg) = args.next() {
        if is_help(arg) {
            return Ok(Scan::Help);
        }
        if !arg.starts_with('-') || arg == "-" {
            return Err(UsageError::UnexpectedArgument {
                argument: arg.clone(),
            });
        }
        let spec = FLAGS
            .iter()
            .find(|spec| spec.name == arg && spec.commands.contains(&command))
            .ok_or_else(|| UsageError::UnknownFlag {
                command: command.word(),
                flag: arg.clone(),
            })?;
        if seen.iter().any(|(name, _)| *name == spec.name) {
            return Err(UsageError::DuplicateFlag { flag: spec.name });
        }
        let value = match spec.value {
            None => None,
            Some(_) => Some(
                args.next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or(UsageError::MissingValue { flag: spec.name })?
                    .clone(),
            ),
        };
        seen.push((spec.name, value));
    }
    Ok(Scan::Flags(seen))
}

/// The flags of one command line, looked up by name.
struct Flags {
    command: CommandName,
    seen: Vec<(&'static str, Option<String>)>,
}

impl Flags {
    fn value(&self, flag: &str) -> Option<&str> {
        self.seen
            .iter()
            .find(|(name, _)| *name == flag)
            .and_then(|(_, value)| value.as_deref())
    }

    fn has(&self, flag: &str) -> bool {
        self.seen.iter().any(|(name, _)| *name == flag)
    }

    fn require(&self, flag: &'static str) -> Result<&str, UsageError> {
        self.value(flag).ok_or(UsageError::MissingFlag {
            command: self.command.word(),
            flag,
        })
    }

    fn replica(&self) -> Result<Replica, UsageError> {
        let value = self.require("--replica")?;
        replica_of(
            value,
            self.value("--endpoint").map(str::to_owned),
            self.value("--region").map(str::to_owned),
            self.has("--force-path-style"),
        )
    }

    fn depth(&self) -> Result<Depth, UsageError> {
        match self.value("--depth") {
            None | Some("contents") => Ok(Depth::Contents),
            Some("names") => Ok(Depth::Names),
            Some(other) => Err(UsageError::BadDepth {
                value: other.to_owned(),
            }),
        }
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
    let command = match first.as_str() {
        "-h" | "--help" | "help" => return Ok(Command::Help),
        "-V" | "--version" => return Ok(Command::Version),
        "restore" => {
            return match restore::parse(args.get(1..).unwrap_or_default())? {
                Restore::Run(args) => Ok(Command::Restore(args)),
                Restore::Help => Ok(Command::Help),
            };
        }
        "databases" => {
            return match inspect::parse_databases(args.get(1..).unwrap_or_default())? {
                Inspect::Run(args) => Ok(Command::Databases(args)),
                Inspect::Help => Ok(Command::Help),
            };
        }
        word @ ("ltx" | "wal") => {
            let as_wal = word == "wal";
            return match inspect::parse_ltx(args.get(1..).unwrap_or_default(), as_wal)? {
                Inspect::Run(args) => Ok(Command::Ltx(args)),
                Inspect::Help => Ok(Command::Help),
            };
        }
        "verify" => CommandName::Verify,
        "files" => CommandName::Files,
        "take-over" => return parse_take_over(args.get(1..).unwrap_or_default()),
        "replicate" => {
            return Ok(Command::Daemon(args.get(1..).unwrap_or_default().to_vec()));
        }
        "config" => return parse_config(args.get(1..).unwrap_or_default()),
        other => {
            return Err(UsageError::UnknownCommand {
                name: other.to_owned(),
            });
        }
    };
    let rest = args.get(1..).unwrap_or_default();
    let flags = match scan(command, rest)? {
        Scan::Help => return Ok(Command::Help),
        Scan::Flags(seen) => Flags { command, seen },
    };
    Ok(match command {
        CommandName::Restore => return Err(UsageError::NoCommand),
        CommandName::Verify => Command::Verify(VerifyArgs {
            replica: flags.replica()?,
            depth: flags.depth()?,
            json: flags.has("--json"),
        }),
        CommandName::Files => Command::Files(FilesArgs {
            replica: flags.replica()?,
            json: flags.has("--json"),
            resolved: flags.has("--created-at"),
        }),
        CommandName::TakeOver => return Err(UsageError::MissingDatabase),
    })
}

/// `take-over <db> [flags]`: the database path, then the flags.
fn parse_take_over(rest: &[String]) -> Result<Command, UsageError> {
    let command = CommandName::TakeOver;
    let (database, flags) = match rest.split_first() {
        Some((first, _)) if is_help(first) => return Ok(Command::Help),
        Some((first, flags)) if !first.starts_with('-') => (PathBuf::from(first), flags),
        Some(_) | None => return Err(UsageError::MissingDatabase),
    };
    let flags = match scan(command, flags)? {
        Scan::Help => return Ok(Command::Help),
        Scan::Flags(seen) => Flags { command, seen },
    };
    Ok(Command::TakeOver(TakeOverArgs {
        database,
        replica: flags.replica()?,
        meta_dir: flags.value("--meta-dir").map(PathBuf::from),
    }))
}

#[cfg(test)]
mod tests;
