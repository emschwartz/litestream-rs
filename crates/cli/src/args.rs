//! Argument parsing: pure, no I/O, no process exit.
//!
//! One table, [`FLAGS`], names every flag with the commands that take it, its
//! value placeholder, whether it is required and its help line. The parser
//! reads the table and the help text is generated from it, so neither can
//! accept or document a flag the other does not know. A flag takes its value
//! as the next argument (`--replica dir`); a value that starts with `--` is
//! read as a missing value, so a path like that is written `./--name`.
//!
//! `--replica` is a directory or an `s3://<bucket>/<prefix>` URL. The S3 flags
//! (`--endpoint`, `--region`, `--force-path-style`) are folded into the URL's
//! own query and parsed once by `S3Config::from_url`, so a flag overrides the
//! query and the provider defaults follow the final endpoint. Credentials are
//! not parsed here (this module reads no environment); `replica.rs` takes them
//! from the AWS variables and shared files.

use std::path::PathBuf;

use lsr_core::{Depth, RestoreTarget};
use lsr_local::IntegrityCheck;
use lsr_ltx::{ParseTxidError, Timestamp, TimestampParseError, Txid};
use lsr_store_s3::{ConfigError, Endpoint, Region, S3Config};
use thiserror::Error;

/// The three subcommands.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandName {
    /// `lsr restore`.
    Restore,
    /// `lsr verify`.
    Verify,
    /// `lsr files`.
    Files,
}

impl CommandName {
    /// Every subcommand, in help order.
    pub const ALL: [Self; 3] = [Self::Restore, Self::Verify, Self::Files];

    /// The word typed after `lsr`.
    pub const fn word(self) -> &'static str {
        match self {
            Self::Restore => "restore",
            Self::Verify => "verify",
            Self::Files => "files",
        }
    }

    /// One line for the help text.
    pub const fn summary(self) -> &'static str {
        match self {
            Self::Restore => "rebuild a database from a replica; never touches an existing file",
            Self::Verify => "check a replica for damage and gaps without restoring",
            Self::Files => "list every file of a replica with its size and stored time",
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
        ],
        help: "s3 only: address the bucket as endpoint/bucket instead of bucket.endpoint",
    },
    FlagSpec {
        name: "--output",
        value: Some("<path>"),
        required: true,
        commands: &[CommandName::Restore],
        help: "the database to create; must not exist",
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
        name: "--clear-sidecars",
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
        name: "--depth",
        value: Some("names|contents"),
        required: false,
        commands: &[CommandName::Verify],
        help: "names lists only; contents (the default) reads and decodes every file",
    },
    FlagSpec {
        name: "--stamps",
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
const S3_ONLY_FLAGS: [&str; 3] = ["--endpoint", "--region", "--force-path-style"];

/// A parsed command line.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Rebuild a database.
    Restore(RestoreArgs),
    /// Check a replica.
    Verify(VerifyArgs),
    /// List a replica's files.
    Files(FilesArgs),
    /// Replicate a live database until a signal stops it.
    Replicate(ReplicateArgs),
    /// Go's `replicate` daemon: the arguments after `replicate`, read by `lsr-sidecar`.
    Daemon(Vec<String>),
    /// `lsr config check <file>`.
    ConfigCheck(PathBuf),
    /// Print the help text.
    Help,
    /// Print the version.
    Version,
}

/// `lsr replicate <db> <replica>`.
#[derive(Debug, PartialEq, Eq)]
pub struct ReplicateArgs {
    /// The database, which must exist.
    pub database: PathBuf,
    /// Where it goes: a directory (created if missing) or an `s3://` URL, whose query takes
    /// the S3 settings (`endpoint`, `region`, `forcePathStyle`).
    pub replica: Replica,
}

/// The usage line of `lsr replicate`, for the help text.
pub const REPLICATE_USAGE: &str = "lsr replicate <db> <replica>";

/// One line for the help text.
pub const REPLICATE_SUMMARY: &str = "replicate a live database to a directory or an s3:// URL until SIGINT, SIGTERM or SIGHUP, then sync what is committed and stop";

/// The daemon's usage line, for the help text.
pub const DAEMON_USAGE: &str =
    "lsr replicate [-config <file>] [Go's replicate flags] [DB_PATH REPLICA_URL]";

/// One line for the help text.
pub const DAEMON_SUMMARY: &str = "Go's replicate daemon: every database in litestream.yml (default $LITESTREAM_CONFIG, else /etc/litestream.yml), until a signal; lsr replicate -h lists the flags";

/// `lsr config check`'s usage line.
pub const CONFIG_CHECK_USAGE: &str = "lsr config check <file>";

/// One line for the help text.
pub const CONFIG_CHECK_SUMMARY: &str = "read a Go litestream.yml as the daemon would and print every refusal and warning; touches no database or bucket";

/// `lsr replicate`: two plain arguments are the one-database form; anything else, flags or
/// no arguments, is the daemon.
fn parse_replicate(rest: &[String]) -> Result<Command, UsageError> {
    let flagless = !rest.iter().any(|arg| arg.starts_with('-') && *arg != "-");
    if !flagless || rest.is_empty() {
        return Ok(Command::Daemon(rest.to_vec()));
    }
    let [database, replica] = rest else {
        return Err(UsageError::ReplicateArguments { found: rest.len() });
    };
    Ok(Command::Replicate(ReplicateArgs {
        database: PathBuf::from(database),
        replica: replica_of(replica)?,
    }))
}

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

/// A replica named on its own: a directory, or an `s3://` URL with its settings in the query.
fn replica_of(value: &str) -> Result<Replica, UsageError> {
    match value.split_once("://") {
        None => Ok(Replica::Dir(PathBuf::from(value))),
        Some((scheme, _)) if scheme.eq_ignore_ascii_case("s3") => {
            let config =
                S3Config::from_url(value).map_err(|reason| UsageError::BadReplicaUrl { reason })?;
            if names_a_provider_endpoint(config.bucket().as_str()) {
                return Err(UsageError::EndpointInReplicaHost);
            }
            Ok(Replica::S3(Box::new(config)))
        }
        Some((scheme, _)) => Err(UsageError::UnsupportedReplica {
            scheme: scheme.to_owned(),
        }),
    }
}

/// `lsr restore`.
#[derive(Debug, PartialEq, Eq)]
pub struct RestoreArgs {
    /// The replica.
    pub replica: Replica,
    /// The database to create.
    pub output: PathBuf,
    /// The state to rebuild.
    pub target: RestoreTarget,
    /// Whether to remove stale sidecars first.
    pub clear_sidecars: bool,
    /// SQLite's check of the result before it is published.
    pub integrity_check: IntegrityCheck,
    /// Succeed without restoring when the output exists.
    pub if_db_not_exists: bool,
    /// Succeed without restoring when the replica holds no LTX file.
    pub if_replica_exists: bool,
    /// Plan and check only; write nothing.
    pub dry_run: bool,
    /// JSON instead of text.
    pub json: bool,
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
    pub stamps: bool,
}

/// Why the command line is not a command.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum UsageError {
    /// Nothing after `lsr`.
    #[error("no command given")]
    NoCommand,
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
    /// Both `--txid` and `--timestamp`.
    #[error("give --txid or --timestamp, not both")]
    ConflictingTargets,
    /// A bare word where a flag belongs.
    #[error("unexpected argument {argument:?}")]
    UnexpectedArgument {
        /// What was typed.
        argument: String,
    },
    /// `--txid` is not sixteen lowercase hex digits.
    #[error("--txid {value:?}: {source}")]
    BadTxid {
        /// What was typed.
        value: String,
        /// Why.
        source: ParseTxidError,
    },
    /// `--timestamp` is not an RFC 3339 instant.
    #[error("--timestamp {value:?}: {source}")]
    BadTimestamp {
        /// What was typed.
        value: String,
        /// Why.
        source: TimestampParseError,
    },
    /// `--depth` is neither `names` nor `contents`.
    #[error("--depth {value:?}: expected names or contents")]
    BadDepth {
        /// What was typed.
        value: String,
    },
    /// `--integrity-check` is not `none`, `quick` or `full`.
    #[error("--integrity-check {value:?}: expected none, quick or full")]
    BadIntegrityCheck {
        /// What was typed.
        value: String,
    },
    /// `--replica` is a URL whose scheme is not `s3`.
    #[error("--replica {scheme}:// replicas are not supported; give a directory or an s3:// URL")]
    UnsupportedReplica {
        /// The scheme, never the rest of the URL (it may hold credentials).
        scheme: String,
    },
    /// `--replica s3://...` is not a usable bucket and prefix. The reason names
    /// the shape that is wrong, never any part of the URL.
    #[error("--replica: {reason}")]
    BadReplicaUrl {
        /// What is wrong.
        reason: ConfigError,
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
    /// The URL's host is a provider endpoint (or a `host:port`), which Go splits
    /// into bucket and endpoint; here the host is the bucket, verbatim.
    #[error(
        "--replica host names a provider endpoint, not a bucket; write s3://<bucket>/<prefix> and pass the endpoint with --endpoint"
    )]
    EndpointInReplicaHost,
    /// `lsr replicate` takes exactly a database and a replica.
    #[error(
        "replicate takes a database and a replica, not {found} arguments: {}",
        REPLICATE_USAGE
    )]
    ReplicateArguments {
        /// How many were given.
        found: usize,
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
    /// An argument that is not UTF-8, which a flag value cannot be.
    #[error("argument {position} is not valid UTF-8")]
    NotUtf8 {
        /// 1-based position after `lsr`.
        position: usize,
    },
}

/// Percent-encodes everything but the unreserved characters.
fn query_escape(raw: &str) -> String {
    raw.bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

/// Hosts Go's `ParseHost` takes apart into bucket, region and endpoint
/// (rc.go:1820-1871); here the host is the bucket, so these would name the wrong one.
fn names_a_provider_endpoint(host: &str) -> bool {
    const SUFFIXES: [&str; 6] = [
        ".amazonaws.com",
        ".digitaloceanspaces.com",
        ".backblazeb2.com",
        "filebase.com",
        ".scw.cloud",
        ".neon.tech",
    ];
    SUFFIXES.iter().any(|suffix| host.ends_with(suffix))
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
        match value.split_once("://") {
            None => {
                if let Some(flag) = S3_ONLY_FLAGS.iter().find(|flag| self.has(flag)) {
                    return Err(UsageError::S3FlagForDirectory { flag });
                }
                Ok(Replica::Dir(PathBuf::from(value)))
            }
            Some((scheme, _)) if scheme.eq_ignore_ascii_case("s3") => self.s3(value),
            Some((scheme, _)) => Err(UsageError::UnsupportedReplica {
                scheme: scheme.to_owned(),
            }),
        }
    }

    /// The URL with the S3 flags appended to its query (the last value of a key
    /// wins in `from_url`), so the flags override the query.
    fn s3(&self, url: &str) -> Result<Replica, UsageError> {
        let mut extra = Vec::new();
        if let Some(endpoint) = self.value("--endpoint") {
            Endpoint::new(endpoint).map_err(|reason| UsageError::BadS3Flag {
                flag: "--endpoint",
                reason,
            })?;
            extra.push(format!("endpoint={}", query_escape(endpoint)));
        }
        if let Some(region) = self.value("--region") {
            Region::new(region).map_err(|reason| UsageError::BadS3Flag {
                flag: "--region",
                reason,
            })?;
            extra.push(format!("region={}", query_escape(region)));
        }
        if self.has("--force-path-style") {
            extra.push("forcePathStyle=true".to_owned());
        }
        let base = url.split_once('#').map_or(url, |(base, _fragment)| base);
        let joined = match (extra.is_empty(), base.contains('?')) {
            (true, _) => base.to_owned(),
            (false, true) => format!("{base}&{}", extra.join("&")),
            (false, false) => format!("{base}?{}", extra.join("&")),
        };
        let config =
            S3Config::from_url(&joined).map_err(|reason| UsageError::BadReplicaUrl { reason })?;
        if names_a_provider_endpoint(config.bucket().as_str()) {
            return Err(UsageError::EndpointInReplicaHost);
        }
        Ok(Replica::S3(Box::new(config)))
    }

    fn target(&self) -> Result<RestoreTarget, UsageError> {
        match (self.value("--txid"), self.value("--timestamp")) {
            (Some(_), Some(_)) => Err(UsageError::ConflictingTargets),
            (Some(value), None) => {
                value
                    .parse::<Txid>()
                    .map(RestoreTarget::Txid)
                    .map_err(|source| UsageError::BadTxid {
                        value: value.to_owned(),
                        source,
                    })
            }
            (None, Some(value)) => Timestamp::parse_rfc3339(value)
                .map(RestoreTarget::AsOf)
                .map_err(|source| UsageError::BadTimestamp {
                    value: value.to_owned(),
                    source,
                }),
            (None, None) => Ok(RestoreTarget::Latest),
        }
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

    /// Go's `-integrity-check` words, exactly (cmd/litestream/restore.go).
    fn integrity_check(&self) -> Result<IntegrityCheck, UsageError> {
        match self.value("--integrity-check") {
            None | Some("none") => Ok(IntegrityCheck::Off),
            Some("quick") => Ok(IntegrityCheck::Quick),
            Some("full") => Ok(IntegrityCheck::Full),
            Some(other) => Err(UsageError::BadIntegrityCheck {
                value: other.to_owned(),
            }),
        }
    }
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
        "restore" => CommandName::Restore,
        "verify" => CommandName::Verify,
        "files" => CommandName::Files,
        "replicate" => return parse_replicate(args.get(1..).unwrap_or_default()),
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
        CommandName::Restore => Command::Restore(RestoreArgs {
            replica: flags.replica()?,
            output: PathBuf::from(flags.require("--output")?),
            target: flags.target()?,
            clear_sidecars: flags.has("--clear-sidecars"),
            integrity_check: flags.integrity_check()?,
            if_db_not_exists: flags.has("--if-db-not-exists"),
            if_replica_exists: flags.has("--if-replica-exists"),
            dry_run: flags.has("--dry-run"),
            json: flags.has("--json"),
        }),
        CommandName::Verify => Command::Verify(VerifyArgs {
            replica: flags.replica()?,
            depth: flags.depth()?,
            json: flags.has("--json"),
        }),
        CommandName::Files => Command::Files(FilesArgs {
            replica: flags.replica()?,
            json: flags.has("--json"),
            stamps: flags.has("--stamps"),
        }),
    })
}

#[cfg(test)]
mod tests;
