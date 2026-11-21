//! Go's `replicate` command line (`cmd/litestream/replicate.go:145-260`), read
//! as Go's `flag` package reads it: `-flag` or `--flag`, a value as
//! `-flag=value` or the next argument, a boolean only as `-flag` or
//! `-flag=<bool>`, flags before the first positional argument (or `--`), the
//! last of a repeated flag winning. Then Go's checks, in Go's order.

use std::path::PathBuf;

use lsr_config::LogLevel;

/// Why the command line is not a run.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum UsageError {
    /// A flag `replicate` does not define.
    #[error("flag provided but not defined: -{name}")]
    UnknownFlag {
        /// The name, without dashes.
        name: String,
    },
    /// A value flag at the end of the line.
    #[error("flag needs an argument: -{name}")]
    MissingValue {
        /// The name.
        name: &'static str,
    },
    /// A boolean flag with a value that is not a boolean.
    #[error("invalid boolean value {value:?} for -{name}")]
    BadBool {
        /// The name.
        name: &'static str,
        /// What was given.
        value: String,
    },
    /// `-log-level` that is not a level.
    #[error("invalid -log-level {value:?}: expected trace, debug, info, warn or error")]
    BadLogLevel {
        /// What was given.
        value: String,
    },
    /// A database and no replica.
    #[error("must specify at least one replica URL for {database}")]
    OnlyDatabase {
        /// The database given.
        database: String,
    },
    /// More than one replica URL (Go refuses it when the database opens).
    #[error("multiple replicas on a single database are no longer supported")]
    MultipleReplicas,
    /// Positional arguments and `-config`.
    #[error("cannot specify a replica URL and the -config flag")]
    ReplicaAndConfig,
    /// A flag after the positional arguments.
    #[error("flag {flag:?} must be positioned before DB_PATH and REPLICA_URL arguments")]
    FlagAfterArguments {
        /// The flag.
        flag: String,
    },
    /// `-once` with `-exec`.
    #[error("cannot specify -once flag with -exec")]
    OnceWithExec,
    /// `-force-snapshot` without `-once`.
    #[error("cannot specify -force-snapshot flag without -once")]
    ForceSnapshotWithoutOnce,
    /// `-enforce-retention` without `-once`.
    #[error("cannot specify -enforce-retention flag without -once")]
    EnforceRetentionWithoutOnce,
}

/// Where the databases come from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// A config file; `None` is Go's default (`LITESTREAM_CONFIG`, else `/etc/litestream.yml`).
    Config(Option<String>),
    /// `DB_PATH REPLICA_URL`.
    OneDatabase {
        /// The database.
        database: String,
        /// The replica URL.
        replica: String,
    },
}

/// A parsed `replicate` command line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flags {
    /// Where the databases come from.
    pub source: Source,
    /// `-no-expand-env`.
    pub no_expand_env: bool,
    /// `-exec`.
    pub exec: Option<String>,
    /// `-log-level`.
    pub log_level: Option<LogLevel>,
    /// `-restore-if-db-not-exists`.
    pub restore_if_db_not_exists: bool,
    /// `-once`.
    pub once: bool,
    /// `-force-snapshot`.
    pub force_snapshot: bool,
    /// `-enforce-retention`.
    pub enforce_retention: bool,
}

/// What the command line asks for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Parsed {
    /// Run.
    Run(Flags),
    /// `-h` or `-help`: print the usage.
    Help,
}

/// The flags `replicate` defines: name and whether it takes a value.
const DEFINED: [(&str, bool); 9] = [
    ("config", true),
    ("no-expand-env", false),
    ("exec", true),
    ("log-level", true),
    ("restore-if-db-not-exists", false),
    ("once", false),
    ("force-snapshot", false),
    ("enforce-retention", false),
    ("help", false),
];

/// Go's `strconv.ParseBool`.
fn parse_bool(name: &'static str, value: &str) -> Result<bool, UsageError> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(UsageError::BadBool {
            name,
            value: value.to_owned(),
        }),
    }
}

/// Go's level names, any case.
pub fn parse_level(value: &str) -> Option<LogLevel> {
    match value.to_ascii_uppercase().as_str() {
        "TRACE" => Some(LogLevel::Trace),
        "DEBUG" => Some(LogLevel::Debug),
        "INFO" => Some(LogLevel::Info),
        "WARN" | "WARNING" => Some(LogLevel::Warn),
        "ERROR" => Some(LogLevel::Error),
        _ => None,
    }
}

/// The flags as Go's `FlagSet.Parse` leaves them, and the positional arguments.
type Scanned = (Vec<(&'static str, String)>, Vec<String>);

fn scan(args: &[String]) -> Result<Option<Scanned>, UsageError> {
    let mut set = Vec::new();
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let Some(body) = arg.strip_prefix('-').filter(|b| !b.is_empty()) else {
            let mut positional = vec![arg.clone()];
            positional.extend(rest.cloned());
            return Ok(Some((set, positional)));
        };
        let body = body.strip_prefix('-').unwrap_or(body);
        if body.is_empty() {
            return Ok(Some((set, rest.cloned().collect())));
        }
        let (name, inline) = match body.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned())),
            None => (body, None),
        };
        let Some(&(name, takes_value)) = DEFINED.iter().find(|(n, _)| *n == name) else {
            if name == "h" {
                return Ok(None);
            }
            return Err(UsageError::UnknownFlag {
                name: name.to_owned(),
            });
        };
        let value = match (takes_value, inline) {
            (_, Some(value)) => value,
            (true, None) => rest
                .next()
                .cloned()
                .ok_or(UsageError::MissingValue { name })?,
            (false, None) => "true".to_owned(),
        };
        if name == "help" {
            return Ok(None);
        }
        set.push((name, value));
    }
    Ok(Some((set, Vec::new())))
}

/// Parses the arguments after `replicate`.
///
/// # Errors
/// [`UsageError`], the first Go would report.
pub fn parse(args: &[String]) -> Result<Parsed, UsageError> {
    let Some((set, positional)) = scan(args)? else {
        return Ok(Parsed::Help);
    };
    let value = |name: &str| {
        set.iter()
            .rev()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.clone())
    };
    let boolean = |name: &'static str| value(name).map_or(Ok(false), |v| parse_bool(name, &v));
    let config = value("config").filter(|c| !c.is_empty());
    let source = match positional.as_slice() {
        [] => Source::Config(config),
        [database] => {
            return Err(UsageError::OnlyDatabase {
                database: database.clone(),
            });
        }
        [database, replicas @ ..] => {
            if config.is_some() {
                return Err(UsageError::ReplicaAndConfig);
            }
            if let Some(flag) = replicas.iter().find(|r| r.starts_with('-')) {
                let flag = flag.clone();
                return Err(UsageError::FlagAfterArguments { flag });
            }
            let [replica] = replicas else {
                return Err(UsageError::MultipleReplicas);
            };
            Source::OneDatabase {
                database: database.clone(),
                replica: replica.clone(),
            }
        }
    };
    let log_level = match value("log-level").filter(|l| !l.is_empty()) {
        None => None,
        Some(text) => Some(parse_level(&text).ok_or(UsageError::BadLogLevel { value: text })?),
    };
    let flags = Flags {
        source,
        no_expand_env: boolean("no-expand-env")?,
        exec: value("exec").filter(|e| !e.is_empty()),
        log_level,
        restore_if_db_not_exists: boolean("restore-if-db-not-exists")?,
        once: boolean("once")?,
        force_snapshot: boolean("force-snapshot")?,
        enforce_retention: boolean("enforce-retention")?,
    };
    check(&flags)?;
    Ok(Parsed::Run(flags))
}

/// Go's flag combinations (`replicate.go:248-257`); `exec` here is only the flag's,
/// the config file's is checked once it is read.
fn check(flags: &Flags) -> Result<(), UsageError> {
    if flags.once && flags.exec.is_some() {
        return Err(UsageError::OnceWithExec);
    }
    if flags.force_snapshot && !flags.once {
        return Err(UsageError::ForceSnapshotWithoutOnce);
    }
    if flags.enforce_retention && !flags.once {
        return Err(UsageError::EnforceRetentionWithoutOnce);
    }
    Ok(())
}

/// Go's default config path when neither `-config` nor `LITESTREAM_CONFIG` is set.
pub const DEFAULT_CONFIG: &str = "/etc/litestream.yml";

/// Go's `resolveConfigPath`: the flag, else `LITESTREAM_CONFIG`, else the default.
#[must_use]
pub fn config_path(flag: Option<&str>, env: impl Fn(&str) -> Option<String>) -> PathBuf {
    flag.map(str::to_owned)
        .or_else(|| env("LITESTREAM_CONFIG").filter(|v| !v.is_empty()))
        .map_or_else(|| PathBuf::from(DEFAULT_CONFIG), PathBuf::from)
}

/// The usage text, after Go's.
pub const USAGE: &str = "\
The replicate command starts a server to monitor & replicate databases.
You can specify your database & replicas in a configuration file or you can
replicate a single database file by specifying its path and its replicas in the
command line arguments.

Usage:

\tlsr replicate [arguments]

\tlsr replicate [arguments] DB_PATH REPLICA_URL

Arguments:

\t-config PATH
\t    Specifies the configuration file. Use \"-\" to read from standard input.
\t    Defaults to $LITESTREAM_CONFIG, else /etc/litestream.yml

\t-exec CMD
\t    Executes a subcommand once every database is pinned. Litestream will exit
\t    when the child process exits: 0 if it exited 0 and every database drained,
\t    else 1, as Go does (the child's own code is logged). Signals are forwarded
\t    to it, and a child that then dies by a signal counts as clean. Overrides
\t    the config file's exec. Cannot be used with -once.

\t-once
\t    Replicate once and exit: sync and upload every database, then stop.

\t-force-snapshot
\t    Not in this build: refused. Requires -once.

\t-enforce-retention
\t    Not in this build: refused. Requires -once.

\t-log-level LEVEL
\t    Sets the log level. Overrides the config file and LOG_LEVEL.
\t    Valid values: trace, debug, info, warn, error

\t-no-expand-env
\t    Disables environment variable expansion in configuration file.

\t-restore-if-db-not-exists
\t    Restores the database from the replica if it doesn't exist.
\t    On first start with no backup, proceeds normally.

SIGINT, SIGTERM and SIGHUP stop replication after a final sync, bounded by
shutdown-sync-timeout for all databases together; a second signal cuts it short.
With -exec the signal goes to the child first, and the drain waits for it.

The config's addr serves /metrics (Prometheus) and /healthz (JSON) on exactly
the address given; an empty host is every interface.
";

#[cfg(test)]
mod tests;
