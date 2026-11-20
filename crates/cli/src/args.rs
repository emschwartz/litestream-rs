//! Argument parsing: pure, no I/O, no process exit.
//!
//! One table, [`FLAGS`], names every flag with the commands that take it, its
//! value placeholder, whether it is required and its help line. The parser
//! reads the table and the help text is generated from it, so neither can
//! accept or document a flag the other does not know. A flag takes its value
//! as the next argument (`--replica dir`); a value that starts with `--` is
//! read as a missing value, so a path like that is written `./--name`.

use std::path::PathBuf;

use lsr_core::{Depth, RestoreTarget};
use lsr_ltx::{ParseTxidError, Timestamp, TimestampParseError, Txid};
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
        help: "the file replica: the directory that holds ltx/",
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
        name: "--depth",
        value: Some("names|contents"),
        required: false,
        commands: &[CommandName::Verify],
        help: "names lists only; contents (the default) reads and decodes every file",
    },
    FlagSpec {
        name: "--json",
        value: None,
        required: false,
        commands: &[CommandName::Verify, CommandName::Files],
        help: "print one JSON object instead of text",
    },
];

/// A parsed command line.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Rebuild a database.
    Restore(RestoreArgs),
    /// Check a replica.
    Verify(VerifyArgs),
    /// List a replica's files.
    Files(FilesArgs),
    /// Print the help text.
    Help,
    /// Print the version.
    Version,
}

/// `lsr restore`.
#[derive(Debug, PartialEq, Eq)]
pub struct RestoreArgs {
    /// The replica directory.
    pub replica: PathBuf,
    /// The database to create.
    pub output: PathBuf,
    /// The state to rebuild.
    pub target: RestoreTarget,
    /// Whether to remove stale sidecars first.
    pub clear_sidecars: bool,
}

/// `lsr verify`.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifyArgs {
    /// The replica directory.
    pub replica: PathBuf,
    /// How much to read.
    pub depth: Depth,
    /// JSON instead of text.
    pub json: bool,
}

/// `lsr files`.
#[derive(Debug, PartialEq, Eq)]
pub struct FilesArgs {
    /// The replica directory.
    pub replica: PathBuf,
    /// JSON instead of text.
    pub json: bool,
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
    /// `--replica` is a URL; only directories work yet.
    #[error("--replica {scheme}:// replicas are not supported yet; give a directory")]
    UnsupportedReplica {
        /// The scheme, never the rest of the URL (it may hold credentials).
        scheme: String,
    },
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

    fn replica(&self) -> Result<PathBuf, UsageError> {
        let value = self.require("--replica")?;
        if let Some((scheme, _)) = value.split_once("://") {
            return Err(UsageError::UnsupportedReplica {
                scheme: scheme.to_owned(),
            });
        }
        Ok(PathBuf::from(value))
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
        }),
        CommandName::Verify => Command::Verify(VerifyArgs {
            replica: flags.replica()?,
            depth: flags.depth()?,
            json: flags.has("--json"),
        }),
        CommandName::Files => Command::Files(FilesArgs {
            replica: flags.replica()?,
            json: flags.has("--json"),
        }),
    })
}

#[cfg(test)]
mod tests;
