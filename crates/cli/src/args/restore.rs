//! `restore`'s command line, read as Go's (`cmd/litestream/restore.go:24-117`): Go's
//! flag package rules ([`lsr_sidecar::goflag`]), Go's flags, and one positional argument,
//! a replica URL or a database path looked up in the config. `lsr`'s own spellings are
//! further flags in the same table (`--replica`, `--output`, the S3 flags,
//! `--allow-earlier`, `--clear-journal-files`); Go's `flag` package reads `--x` as `-x`, so
//! `lsr restore --replica dir --output db` still reads as before, and every line Go
//! accepts reads as Go reads it. Checks run in Go's order; the ones Go makes after it has
//! opened the replica or the config are the run's (`run.rs`).

use std::path::PathBuf;

use lsr_config::is_url;
use lsr_core::{AsOfInside, RestoreTarget};
use lsr_ltx::{Timestamp, Txid};
use lsr_replica::local::IntegrityCheck;
use lsr_sidecar::goflag::{self, Def, FlagError, Set};

use super::inspect::{self, Inspect};
use super::{FlagRow, Replica, UsageError, replica_of};

/// Where a restore reads from.
#[derive(Debug, PartialEq, Eq)]
pub enum RestoreFrom {
    /// `lsr`'s `--replica`: a directory or an `s3://` URL, with the S3 flags.
    Replica(Replica),
    /// Go's `REPLICA_URL` (`scheme://...`), read as a config's `url:`.
    Url(String),
    /// Go's `DB_PATH`, found among the config's `path` entries.
    Database {
        /// The path as given.
        path: String,
        /// `-config`, when given.
        config: Option<String>,
        /// `-no-expand-env`: read the config without expanding variables. Only a config is
        /// read, so only this source carries it; a URL needs no expansion, and Go takes the
        /// flag there and does nothing with it.
        no_expand_env: bool,
    },
}

/// `restore`, as parsed.
#[derive(Debug, PartialEq, Eq)]
pub struct RestoreArgs {
    /// Where from.
    pub from: RestoreFrom,
    /// `-o` (or `--output`); Go's default is the database's own path.
    pub output: Option<PathBuf>,
    /// The state to rebuild.
    pub target: RestoreTarget,
    /// What to do when `--timestamp` falls inside a compacted file.
    pub as_of_inside: AsOfInside,
    /// Whether to remove stale journal files first (`lsr`'s `--clear-journal-files`).
    pub clear_journal_files: bool,
    /// SQLite's check of the result before it is published.
    pub integrity_check: IntegrityCheck,
    /// Succeed without restoring when something exists at the output.
    pub if_db_not_exists: bool,
    /// Succeed without restoring when the replica holds no LTX file.
    pub if_replica_exists: bool,
    /// Plan and check only; write nothing.
    pub dry_run: bool,
    /// JSON instead of text.
    pub json: bool,
    /// `-force`: replace an existing output and its journal files.
    pub force: bool,
    /// `-parallelism` was given; Go warns that it has no effect.
    pub parallelism: bool,
    /// A non-zero `-txid` and a `-timestamp` both: Go refuses them only when it plans or
    /// restores, after the config, the skip and the output checks, so the run does too.
    pub txid_and_timestamp: bool,
    /// `-timestamp` was given, with or without a `-txid`.
    pub timestamp_given: bool,
}

/// Go's `-txid` (`txidVar.Set`): sixteen hex digits, any case, zero included.
fn go_txid(text: &str) -> bool {
    text.len() == 16 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Go's `-parallelism` (`strconv.ParseInt(s, 0, 64)`): a sign, then decimal, or `0x`,
/// `0o`, `0b` or a leading `0` (octal); underscores as Go's literal syntax allows them,
/// between digits, the prefix counting as a digit.
fn go_int(text: &str) -> bool {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (digits, radix) = match body.get(..2) {
        Some("0x" | "0X") => (body.get(2..).unwrap_or_default(), 16),
        Some("0o" | "0O") => (body.get(2..).unwrap_or_default(), 8),
        Some("0b" | "0B") => (body.get(2..).unwrap_or_default(), 2),
        _ if body.len() > 1 && body.starts_with('0') => (body.get(1..).unwrap_or_default(), 8),
        _ => (body, 10),
    };
    let underscores_ok = !body.starts_with('_') && !body.ends_with('_') && !body.contains("__");
    let clean: String = digits.chars().filter(|c| *c != '_').collect();
    let limit = i64::MAX.unsigned_abs().saturating_add(u64::from(negative));
    !clean.is_empty()
        && underscores_ok
        && u64::from_str_radix(&clean, radix).is_ok_and(|magnitude| magnitude <= limit)
}

/// Go's `-follow-interval` (`time.ParseDuration`).
fn go_duration(text: &str) -> bool {
    lsr_config::GoDuration::parse(text).is_ok()
}

/// The flags `restore` defines: Go's, then `lsr`'s.
pub(super) const DEFINED: [Def; 21] = [
    goflag::value("config"),
    goflag::boolean("no-expand-env"),
    goflag::value("o"),
    goflag::checked("txid", |v| {
        go_txid(v).then_some(()).ok_or("invalid txid format")
    }),
    goflag::checked("parallelism", |v| {
        go_int(v).then_some(()).ok_or("parse error")
    }),
    goflag::boolean("if-db-not-exists"),
    goflag::boolean("if-replica-exists"),
    goflag::value("timestamp"),
    goflag::boolean("dry-run"),
    goflag::boolean("force"),
    goflag::boolean("json"),
    goflag::boolean("f"),
    goflag::checked("follow-interval", |v| {
        go_duration(v).then_some(()).ok_or("parse error")
    }),
    goflag::value("integrity-check"),
    goflag::value("replica"),
    goflag::value("output"),
    goflag::value("endpoint"),
    goflag::value("region"),
    goflag::boolean("force-path-style"),
    goflag::boolean("allow-earlier"),
    goflag::boolean("clear-journal-files"),
];

/// Every flag `restore` accepts, for the help text; a test holds it to [`DEFINED`], both ways.
pub(super) const HELP: &[FlagRow] = &[
    FlagRow {
        name: "--replica",
        value: Some("<dir>"),
        help: "the replica: a directory that holds ltx/, or s3://<bucket>/<prefix>",
    },
    FlagRow {
        name: "--endpoint",
        value: Some("<url>"),
        help: "s3 only: the service endpoint (http or https), for anything but AWS",
    },
    FlagRow {
        name: "--region",
        value: Some("<name>"),
        help: "s3 only: the signing region (default us-east-1)",
    },
    FlagRow {
        name: "--force-path-style",
        value: None,
        help: "s3 only: address the bucket as endpoint/bucket instead of bucket.endpoint",
    },
    FlagRow {
        name: "--output",
        value: Some("<path>"),
        help: "the database to create, as Go's -o; with a DB_PATH the default is DB_PATH itself; an existing empty file is replaced, any other refused without -force",
    },
    FlagRow {
        name: "--txid",
        value: Some("<16 hex digits>"),
        help: "restore the state right after this transaction (not with --timestamp)",
    },
    FlagRow {
        name: "--timestamp",
        value: Some("<rfc3339>"),
        help: "restore the newest state created before this instant (not with --txid)",
    },
    FlagRow {
        name: "--allow-earlier",
        value: None,
        help: "With --timestamp, restore the newest state provably before the instant when the instant falls inside a compacted file, instead of refusing.",
    },
    FlagRow {
        name: "--clear-journal-files",
        value: None,
        help: "first remove a stale -wal, -shm or -journal beside the output",
    },
    FlagRow {
        name: "--integrity-check",
        value: Some("none|quick|full"),
        help: "run SQLite's quick_check or integrity_check on the result before publishing it; a failure publishes nothing (default none)",
    },
    FlagRow {
        name: "--if-db-not-exists",
        value: None,
        help: "exit 0 without restoring when something already exists at the output",
    },
    FlagRow {
        name: "--if-replica-exists",
        value: None,
        help: "exit 0 without restoring when the replica holds no LTX file (a directory with no ltx/ included, as in Go); a target a replica with files cannot meet is still refused; --dry-run ignores it, as Go's does",
    },
    FlagRow {
        name: "--dry-run",
        value: None,
        help: "check the output and print the files a restore would apply, writing and removing nothing",
    },
    FlagRow {
        name: "-o",
        value: Some("<path>"),
        help: "Go's spelling of --output",
    },
    FlagRow {
        name: "-config",
        value: Some("<file>"),
        help: "with a DB_PATH, the config that names it (default $LITESTREAM_CONFIG, else /etc/litestream.yml; - reads standard input); not with a REPLICA_URL",
    },
    FlagRow {
        name: "-no-expand-env",
        value: None,
        help: "read the config without expanding environment variables",
    },
    FlagRow {
        name: "-force",
        value: None,
        help: "replace an existing output and remove its -wal, -shm and -journal; the old file stays until the restored one is complete and checked, then is replaced in one rename",
    },
    FlagRow {
        name: "-parallelism",
        value: Some("<n>"),
        help: "accepted with a warning and no effect, as Go",
    },
    FlagRow {
        name: "-f",
        value: None,
        help: "Go's follow mode, which is not built yet (#177): refused",
    },
    FlagRow {
        name: "-follow-interval",
        value: Some("<duration>"),
        help: "accepted for Go's -f, which is not built yet (#177); -f itself is refused",
    },
    FlagRow {
        name: "--json",
        value: None,
        help: "print one JSON object instead of text",
    },
];

/// Parses the arguments after `restore`.
///
/// # Errors
/// [`UsageError`], the first Go would report.
pub fn parse(args: &[String]) -> Result<Inspect<Box<RestoreArgs>>, UsageError> {
    let Some(set) = inspect::scan(&DEFINED, args)? else {
        return Ok(Inspect::Help);
    };
    let from = from(&set)?;
    if set.boolean("f")? {
        return Err(UsageError::FollowNotBuilt);
    }
    let integrity_check = match set.value("integrity-check").as_deref() {
        None | Some("none") => IntegrityCheck::Off,
        Some("quick") => IntegrityCheck::Quick,
        Some("full") => IntegrityCheck::Full,
        Some(other) => {
            return Err(UsageError::GoIntegrityCheck {
                value: other.to_owned(),
            });
        }
    };
    let target = target(&set)?;
    let txid_and_timestamp = matches!(target, RestoreTarget::Txid(_))
        && set.value("timestamp").is_some_and(|t| !t.is_empty());
    let as_of_inside = match (set.boolean("allow-earlier")?, target) {
        (false, _) => AsOfInside::Refuse,
        (true, RestoreTarget::AsOf(_)) => AsOfInside::TakeEarlier,
        (true, RestoreTarget::Latest | RestoreTarget::Txid(_)) => {
            return Err(UsageError::AllowEarlierWithoutTimestamp);
        }
    };
    // `-o` and `--output` are one flag; two of them cannot both be honoured.
    if set.value("o").is_some() && set.value("output").is_some() {
        return Err(UsageError::DuplicateFlag { flag: "--output" });
    }
    let output = set
        .value("o")
        .or_else(|| set.value("output"))
        .filter(|o| !o.is_empty())
        .map(PathBuf::from);
    Ok(Inspect::Run(Box::new(RestoreArgs {
        from,
        output,
        target,
        as_of_inside,
        clear_journal_files: set.boolean("clear-journal-files")?,
        integrity_check,
        if_db_not_exists: set.boolean("if-db-not-exists")?,
        if_replica_exists: set.boolean("if-replica-exists")?,
        dry_run: set.boolean("dry-run")?,
        json: set.boolean("json")?,
        force: set.boolean("force")?,
        parallelism: set.visited("parallelism"),
        txid_and_timestamp,
        timestamp_given: set.value("timestamp").is_some_and(|t| !t.is_empty()),
    })))
}

/// The source: Go's one positional argument, or `lsr`'s `--replica`.
fn from(set: &Set) -> Result<RestoreFrom, UsageError> {
    let replica = set.value("replica").filter(|r| !r.is_empty());
    let config = set.value("config").filter(|c| !c.is_empty());
    match (set.positional(), replica) {
        ([], None) => Err(UsageError::GoSourceMissing),
        ([first, ..], _) if first.is_empty() => Err(UsageError::GoSourceMissing),
        ([_, _, ..], _) => Err(UsageError::GoTooManyArguments),
        ([_], Some(_)) => Err(UsageError::ReplicaAndPositional),
        ([], Some(replica)) => {
            if config.is_some() {
                return Err(UsageError::ConfigFlagWithReplica { flag: "-config" });
            }
            if set.boolean("no-expand-env")? {
                return Err(UsageError::ConfigFlagWithReplica {
                    flag: "-no-expand-env",
                });
            }
            let over = |name: &str| set.value(name).filter(|v| !v.is_empty());
            let style = set.boolean("force-path-style")?;
            replica_of(&replica, over("endpoint"), over("region"), style).map(RestoreFrom::Replica)
        }
        ([url], None) if is_url(url) => {
            if config.is_some() {
                return Err(UsageError::GoUrlAndConfig);
            }
            if let Some(flag) = s3_flag_given(set)? {
                return Err(UsageError::S3FlagWithUrl { flag });
            }
            Ok(RestoreFrom::Url(url.clone()))
        }
        ([path], None) => {
            if let Some(flag) = s3_flag_given(set)? {
                return Err(UsageError::S3FlagWithDatabase { flag });
            }
            Ok(RestoreFrom::Database {
                path: path.clone(),
                config,
                no_expand_env: set.boolean("no-expand-env")?,
            })
        }
    }
}

/// The first of `lsr`'s S3 flags that was given with a value. Only `--replica` can honour
/// them: a URL carries its endpoint, region and path style in its query (Go's form), and a
/// config carries them in its replica, so beside either they would change nothing.
pub(super) fn s3_flag_given(set: &Set) -> Result<Option<&'static str>, UsageError> {
    for (name, flag) in [("endpoint", "--endpoint"), ("region", "--region")] {
        if set.value(name).is_some_and(|v| !v.is_empty()) {
            return Ok(Some(flag));
        }
    }
    let style = set.boolean("force-path-style")?;
    Ok(style.then_some("--force-path-style"))
}

/// `-txid` and `-timestamp`. Go's zero txid means the latest; with both, the txid is kept
/// and [`RestoreArgs::txid_and_timestamp`] says the run must refuse.
fn target(set: &Set) -> Result<RestoreTarget, UsageError> {
    let timestamp = match set.value("timestamp").filter(|t| !t.is_empty()) {
        Some(text) => {
            Some(Timestamp::parse_rfc3339(&text).map_err(|_unparsed| UsageError::GoTimestamp)?)
        }
        None => None,
    };
    let txid = set
        .value("txid")
        .and_then(|text| u64::from_str_radix(&text, 16).ok())
        .and_then(Txid::new);
    match (txid, timestamp) {
        (Some(txid), _) => Ok(RestoreTarget::Txid(txid)),
        (None, Some(at)) => Ok(RestoreTarget::AsOf(at)),
        (None, None) => Ok(RestoreTarget::Latest),
    }
}

impl From<FlagError> for UsageError {
    fn from(error: FlagError) -> Self {
        Self::GoFlag(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_ints_are_read_as_strconv_reads_them() {
        for (text, ok) in [
            ("8", true),
            ("-3", true),
            ("+3", true),
            ("0x1f", true),
            ("0o17", true),
            ("017", true),
            ("0b101", true),
            ("0x_1f", true),
            ("1_000", true),
            ("_1", false),
            ("1_", false),
            ("-9223372036854775808", true),
            ("0x__1", false),
            ("", false),
            ("x", false),
            ("08", false),
            ("9223372036854775808", false),
        ] {
            assert_eq!(go_int(text), ok, "{text:?}");
        }
    }

    /// Go's `litestream.IsURL` is `^\w+://` (`replica_url.go`, used by `restore.go:100`),
    /// and RE2's `\w` is ASCII only: a Unicode letter in the scheme makes a database path.
    #[test]
    fn urls_are_what_go_calls_urls() {
        for (text, url) in [
            ("s3://b/p", true),
            ("file:///d", true),
            ("x_1://y", true),
            ("/data/db", false),
            ("://x", false),
            ("a-b://x", false),
            ("é://x", false),
            ("s3é://x", false),
        ] {
            let args = [text.to_owned()];
            let Ok(Inspect::Run(run)) = parse(&args) else {
                panic!("{text:?} does not parse");
            };
            assert_eq!(matches!(run.from, RestoreFrom::Url(_)), url, "{text:?}");
            if !url {
                assert!(matches!(run.from, RestoreFrom::Database { .. }), "{text:?}");
            }
        }
    }
}
