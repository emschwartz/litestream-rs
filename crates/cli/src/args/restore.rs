//! `restore`'s command line, read as Go's (`cmd/litestream/restore.go:24-117`): Go's
//! flag package rules ([`lsr_sidecar::goflag`]), Go's flags, and one positional argument,
//! a replica URL or a database path looked up in the config. `lsr`'s own spellings are
//! further flags in the same table (`--replica`, `--output`, the S3 flags,
//! `--allow-earlier`, `--clear-journal-files`); Go's `flag` package reads `--x` as `-x`, so
//! `lsr restore --replica dir --output db` still reads as before, and every line Go
//! accepts reads as Go reads it. Checks run in Go's order; the ones Go makes after it has
//! opened the replica or the config are the run's (`run.rs`).

use std::path::PathBuf;

use lsr_core::{AsOfInside, RestoreTarget};
use lsr_local::IntegrityCheck;
use lsr_ltx::{Timestamp, Txid};
use lsr_sidecar::goflag::{self, Def, FlagError, Scan, Set};

use super::{Replica, UsageError, replica_of};

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
    },
}

/// `restore`, as parsed.
#[derive(Debug, PartialEq, Eq)]
pub struct RestoreArgs {
    /// Where from.
    pub from: RestoreFrom,
    /// `-o` (or `--output`); Go's default is the database's own path.
    pub output: Option<PathBuf>,
    /// `-no-expand-env`.
    pub no_expand_env: bool,
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
const DEFINED: [Def; 21] = [
    goflag::value("config"),
    goflag::boolean("no-expand-env"),
    goflag::value("o"),
    goflag::checked("txid", go_txid, "invalid txid format"),
    goflag::checked("parallelism", go_int, "parse error"),
    goflag::boolean("if-db-not-exists"),
    goflag::boolean("if-replica-exists"),
    goflag::value("timestamp"),
    goflag::boolean("dry-run"),
    goflag::boolean("force"),
    goflag::boolean("json"),
    goflag::boolean("f"),
    goflag::checked("follow-interval", go_duration, "parse error"),
    goflag::value("integrity-check"),
    goflag::value("replica"),
    goflag::value("output"),
    goflag::value("endpoint"),
    goflag::value("region"),
    goflag::boolean("force-path-style"),
    goflag::boolean("allow-earlier"),
    goflag::boolean("clear-journal-files"),
];

/// Go's `litestream.IsURL`: `^\w+://`.
fn is_url(text: &str) -> bool {
    text.split_once("://").is_some_and(|(scheme, _)| {
        !scheme.is_empty() && scheme.chars().all(|c| c.is_alphanumeric() || c == '_')
    })
}

/// What a `restore` command line asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Restore {
    /// Run it.
    Run(Box<RestoreArgs>),
    /// `-h` or `-help`, literally.
    Help,
}

/// Parses the arguments after `restore`.
///
/// # Errors
/// [`UsageError`], the first Go would report.
pub fn parse(args: &[String]) -> Result<Restore, UsageError> {
    let set = match goflag::scan(&DEFINED, args).map_err(UsageError::GoFlag)? {
        Scan::Set(set) => set,
        Scan::Help if goflag::literal_help(args) => return Ok(Restore::Help),
        Scan::Help => return Err(UsageError::HelpNotLiteral),
    };
    let from = from(&set)?;
    let boolean = |name| set.boolean(name).map_err(UsageError::GoFlag);
    if boolean("f")? {
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
    let as_of_inside = match (boolean("allow-earlier")?, target) {
        (false, _) => AsOfInside::Refuse,
        (true, RestoreTarget::AsOf(_)) => AsOfInside::TakeEarlier,
        (true, RestoreTarget::Latest | RestoreTarget::Txid(_)) => {
            return Err(UsageError::AllowEarlierWithoutTimestamp);
        }
    };
    let output = set
        .value("o")
        .or_else(|| set.value("output"))
        .filter(|o| !o.is_empty())
        .map(PathBuf::from);
    Ok(Restore::Run(Box::new(RestoreArgs {
        from,
        output,
        no_expand_env: boolean("no-expand-env")?,
        target,
        as_of_inside,
        clear_journal_files: boolean("clear-journal-files")?,
        integrity_check,
        if_db_not_exists: boolean("if-db-not-exists")?,
        if_replica_exists: boolean("if-replica-exists")?,
        dry_run: boolean("dry-run")?,
        json: boolean("json")?,
        force: boolean("force")?,
        parallelism: set.visited("parallelism"),
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
            let over = |name: &str| set.value(name).filter(|v| !v.is_empty());
            let style = set
                .boolean("force-path-style")
                .map_err(UsageError::GoFlag)?;
            replica_of(&replica, over("endpoint"), over("region"), style).map(RestoreFrom::Replica)
        }
        ([url], None) if is_url(url) => {
            if config.is_some() {
                return Err(UsageError::GoUrlAndConfig);
            }
            Ok(RestoreFrom::Url(url.clone()))
        }
        ([path], None) => Ok(RestoreFrom::Database {
            path: path.clone(),
            config,
        }),
    }
}

/// `-txid` and `-timestamp`. Go's zero txid means the latest.
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
        (Some(_), Some(_)) => Err(UsageError::GoTxidAndTimestamp),
        (Some(txid), None) => Ok(RestoreTarget::Txid(txid)),
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

    #[test]
    fn urls_are_what_go_calls_urls() {
        for (text, url) in [
            ("s3://b/p", true),
            ("file:///d", true),
            ("x_1://y", true),
            ("/data/db", false),
            ("://x", false),
            ("a-b://x", false),
        ] {
            assert_eq!(is_url(text), url, "{text:?}");
        }
    }
}
