//! Go's read-only commands that need no running daemon, read as Go reads them:
//! `databases` (`cmd/litestream/databases.go`), `ltx` (`ltx.go`) and its deprecated alias
//! `wal` (`main.go:175-178`, a warning and then `ltx`). Go's flag package rules
//! ([`lsr_sidecar::goflag`]), Go's flags, Go's messages, Go's order.

use lsr_config::is_url;
use lsr_core::Level;
use lsr_sidecar::goflag::{self, Def, Scan};

use super::{FlagRow, UsageError};

/// `databases`.
#[derive(Debug, PartialEq, Eq)]
pub struct DatabasesArgs {
    /// `-config`, when given.
    pub config: Option<String>,
    /// `-no-expand-env`.
    pub no_expand_env: bool,
    /// `-json`.
    pub json: bool,
}

/// Where `ltx` lists from.
#[derive(Debug, PartialEq, Eq)]
pub enum LtxFrom {
    /// A replica URL, read as a config's `url:`.
    Url(String),
    /// A database path found among the config's `path` entries.
    Database {
        /// The path as given.
        path: String,
        /// `-config`, when given.
        config: Option<String>,
        /// `-no-expand-env`. Only a config is read, so only this source carries it; Go takes
        /// the flag beside a URL and does nothing with it.
        no_expand_env: bool,
    },
}

/// `ltx` (and `wal`).
#[derive(Debug, PartialEq, Eq)]
pub struct LtxArgs {
    /// Where from.
    pub from: LtxFrom,
    /// `-json`.
    pub json: bool,
    /// `-level`: one level, or `None` for `all`. Go's default is level 0.
    pub level: Option<Level>,
    /// Run as `wal`: Go warns that the command is deprecated first.
    pub as_wal: bool,
}

/// `version`.
#[derive(Debug, PartialEq, Eq)]
pub struct VersionArgs {
    /// `-verbose`.
    pub verbose: bool,
}

/// What a read-only command line asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Inspect<T> {
    /// Run it.
    Run(T),
    /// `-h` or `-help`, literally.
    Help,
}

fn scan(defs: &[Def], args: &[String]) -> Result<Option<goflag::Set>, UsageError> {
    match goflag::scan(defs, args).map_err(UsageError::GoFlag)? {
        Scan::Set(set) => Ok(Some(set)),
        Scan::Help if goflag::literal_help(args) => Ok(None),
        Scan::Help => Err(UsageError::HelpNotLiteral),
    }
}

pub(super) const DATABASES: [Def; 3] = [
    goflag::value("config"),
    goflag::boolean("no-expand-env"),
    goflag::boolean("json"),
];

/// The help rows of `databases`; a test holds them to [`DATABASES`], both ways.
pub(super) const DATABASES_HELP: &[FlagRow] = &[
    FlagRow {
        name: "-config",
        value: Some("<file>"),
        help: "the config to read (default $LITESTREAM_CONFIG, else /etc/litestream.yml; - reads standard input)",
    },
    FlagRow {
        name: "-no-expand-env",
        value: None,
        help: "read the config without expanding environment variables",
    },
    FlagRow {
        name: "-json",
        value: None,
        help: "print JSON instead of a table",
    },
];

/// Parses the arguments after `databases`.
///
/// # Errors
/// [`UsageError`], the first Go would report.
pub fn parse_databases(args: &[String]) -> Result<Inspect<DatabasesArgs>, UsageError> {
    let Some(set) = scan(&DATABASES, args)? else {
        return Ok(Inspect::Help);
    };
    if !set.positional().is_empty() {
        return Err(UsageError::GoTooManyArguments);
    }
    let boolean = |name| set.boolean(name).map_err(UsageError::GoFlag);
    Ok(Inspect::Run(DatabasesArgs {
        config: set.value("config").filter(|c| !c.is_empty()),
        no_expand_env: boolean("no-expand-env")?,
        json: boolean("json")?,
    }))
}

pub(super) const VERSION: [Def; 1] = [goflag::boolean("verbose")];

/// The help rows of `version`; a test holds them to [`VERSION`], both ways.
pub(super) const VERSION_HELP: &[FlagRow] = &[FlagRow {
    name: "-verbose",
    value: None,
    help: "print the version, the OS and the architecture on labelled lines, as Go's -verbose does (no Go version or commit: this is not a Go build)",
}];

/// Parses the arguments after `version`. Go reads only its flag and ignores any other
/// argument (`cmd/litestream/version.go`), so does this.
///
/// # Errors
/// [`UsageError`], the first Go would report.
pub fn parse_version(args: &[String]) -> Result<Inspect<VersionArgs>, UsageError> {
    let Some(set) = scan(&VERSION, args)? else {
        return Ok(Inspect::Help);
    };
    Ok(Inspect::Run(VersionArgs {
        verbose: set.boolean("verbose").map_err(UsageError::GoFlag)?,
    }))
}

/// Go's `levelVar.Set`: `all`, or an integer 0 to 9 (`strconv.Atoi`: an optional sign and
/// decimal digits).
fn go_level(text: &str) -> Result<(), &'static str> {
    if text == "all" {
        return Ok(());
    }
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid level: must be 0-9 or \"all\"");
    }
    match text.parse::<i64>() {
        Ok(n) if (0..=9).contains(&n) => Ok(()),
        Ok(_) => Err("level must be between 0 and 9"),
        // More digits than an int holds: Atoi's range error, reported as Go's first message.
        Err(_) => Err("invalid level: must be 0-9 or \"all\""),
    }
}

pub(super) const LTX: [Def; 4] = [
    goflag::value("config"),
    goflag::boolean("no-expand-env"),
    goflag::boolean("json"),
    goflag::checked("level", go_level),
];

/// The help rows of `ltx` and `wal`; a test holds them to [`LTX`], both ways.
pub(super) const LTX_HELP: &[FlagRow] = &[
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
        name: "-level",
        value: Some("<0-9|all>"),
        help: "the compaction level to list (default 0)",
    },
    FlagRow {
        name: "-json",
        value: None,
        help: "print JSON instead of a table",
    },
];

/// Parses the arguments after `ltx`, or after `wal` with `as_wal`.
///
/// # Errors
/// [`UsageError`], the first Go would report.
pub fn parse_ltx(args: &[String], as_wal: bool) -> Result<Inspect<LtxArgs>, UsageError> {
    let Some(set) = scan(&LTX, args)? else {
        return Ok(Inspect::Help);
    };
    let config = set.value("config").filter(|c| !c.is_empty());
    let from = match set.positional() {
        [] => return Err(UsageError::GoSourceMissing),
        [first, ..] if first.is_empty() => return Err(UsageError::GoSourceMissing),
        [_, _, ..] => return Err(UsageError::GoTooManyArguments),
        [url] if is_url(url) => {
            if config.is_some() {
                return Err(UsageError::GoUrlAndConfig);
            }
            LtxFrom::Url(url.clone())
        }
        [path] => LtxFrom::Database {
            path: path.clone(),
            config,
            no_expand_env: set.boolean("no-expand-env").map_err(UsageError::GoFlag)?,
        },
    };
    let level = match set.value("level").as_deref() {
        None => Level::new(0),
        Some("all") => None,
        Some(text) => text
            .trim_start_matches('+')
            .parse::<u8>()
            .ok()
            .and_then(Level::new),
    };
    let boolean = |name| set.boolean(name).map_err(UsageError::GoFlag);
    Ok(Inspect::Run(LtxArgs {
        from,
        json: boolean("json")?,
        level,
        as_wal,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_as_go_reads_them() {
        for (text, want) in [
            ("all", Ok(())),
            ("0", Ok(())),
            ("9", Ok(())),
            ("+3", Ok(())),
            ("10", Err("level must be between 0 and 9")),
            ("-1", Err("level must be between 0 and 9")),
            ("x", Err("invalid level: must be 0-9 or \"all\"")),
            ("", Err("invalid level: must be 0-9 or \"all\"")),
            ("ALL", Err("invalid level: must be 0-9 or \"all\"")),
        ] {
            assert_eq!(go_level(text), want, "{text:?}");
        }
    }

    #[test]
    fn ltx_defaults_to_level_zero_and_all_is_every_level() {
        let words = |line: &str| {
            line.split_whitespace()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let level = |line: &str| match parse_ltx(&words(line), false) {
            Ok(Inspect::Run(args)) => args.level,
            other => panic!("{line}: {other:?}"),
        };
        assert_eq!(level("/data/db"), Level::new(0));
        assert_eq!(level("-level all /data/db"), None);
        assert_eq!(level("-level=9 s3://b/p"), Level::new(9));
    }
}
