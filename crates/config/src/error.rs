//! What a load found: problems that refuse the file, warnings that do not, and
//! the class of every key the file sets.
//!
//! No problem or warning holds a value read from a key that holds a secret
//! (`secret-access-key`, `password` and the rest marked secret in
//! `keys.rs`); values from other keys are shown to say what was wrong. The
//! YAML parser's own messages hold at most one character of the input, from a
//! fixed set (`%`, `@`, a backquote).

use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::duration::GoDuration;
use crate::expand::PathError;
use crate::size::SizeError;

/// Where a key sits in the file, written as Go's own messages write it:
/// `dbs[0].replica.type`. The empty path is the whole file.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyPath(Vec<Segment>);

/// One step of a [`KeyPath`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Segment {
    Key(String),
    Index(usize),
}

impl KeyPath {
    /// The whole file.
    #[must_use]
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// This path's child key `key`.
    #[must_use]
    pub fn key(&self, key: &str) -> Self {
        let mut segments = self.0.clone();
        segments.push(Segment::Key(key.to_owned()));
        Self(segments)
    }

    /// This path's list item `index`.
    #[must_use]
    pub fn index(&self, index: usize) -> Self {
        let mut segments = self.0.clone();
        segments.push(Segment::Index(index));
        Self(segments)
    }

    /// The last key named on the path, if any.
    #[must_use]
    pub fn last_key(&self) -> Option<&str> {
        self.0.iter().rev().find_map(|segment| match segment {
            Segment::Key(key) => Some(key.as_str()),
            Segment::Index(_) => None,
        })
    }
}

impl fmt::Display for KeyPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str("the file");
        }
        for (at, segment) in self.0.iter().enumerate() {
            match segment {
                Segment::Key(key) if at == 0 => f.write_str(key)?,
                Segment::Key(key) => write!(f, ".{key}")?,
                Segment::Index(index) => write!(f, "[{index}]")?,
            }
        }
        Ok(())
    }
}

/// How litestream-rs treats a key Go defines: implemented, accepted only at its off value, a knob with no
/// effect, or refused (docs/plans/phase-4-sidecar.md, the config section).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum KeyClass {
    /// Go knows it and litestream-rs does what it says.
    Implemented,
    /// Go knows it for a feature litestream-rs lacks: accepted only at the value that
    /// asks for nothing (`enabled: false`, empty), refused otherwise.
    OffOnly,
    /// Go knows it as a tuning knob with no counterpart in litestream-rs: accepted, with
    /// one warning.
    NoEffect,
    /// Go does not know it: refused, with the nearest Go key when one is close.
    Unknown,
}

/// What Go reads a key as, named in type errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expected {
    /// Any scalar.
    String,
    /// `true`/`false`, `yes`/`no`, `on`/`off`, `y`/`n` in yaml.v2's spellings.
    Boolean,
    /// A whole number (decimal, `0x`, `0o`, `0b`, or a leading `0` for octal).
    Integer,
    /// A whole number that is not negative and fits 32 bits.
    Unsigned32,
    /// A Go duration (`10s`, `1h30m`), or a bare integer read as nanoseconds.
    Duration,
    /// A byte size (`5MiB`, `5MB`, `5242880`).
    ByteSize,
    /// A list of scalars.
    List,
    /// A mapping of keys.
    Mapping,
    /// A list of mappings.
    ListOfMappings,
}

impl fmt::Display for Expected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::String => "a string",
            Self::Boolean => "a boolean (true or false)",
            Self::Integer => "a whole number",
            Self::Unsigned32 => "a whole number from 0 to 4294967295",
            Self::Duration => "a duration such as 10s or 1h30m",
            Self::ByteSize => "a byte size such as 5MiB",
            Self::List => "a list",
            Self::Mapping => "a mapping of keys",
            Self::ListOfMappings => "a list of mappings",
        })
    }
}

/// What a type error found instead. `Scalar` holds the text, or `None` when the
/// key holds a secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Found {
    /// A scalar, with its text unless the key is secret.
    Scalar(Option<String>),
    /// A mapping.
    Mapping,
    /// A list.
    List,
}

impl fmt::Display for Found {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(Some(text)) => write!(f, "`{text}`"),
            Self::Scalar(None) => f.write_str("a value (not shown: the key holds a secret)"),
            Self::Mapping => f.write_str("a mapping"),
            Self::List => f.write_str("a list"),
        }
    }
}

/// Why the file is refused. Each variant says what to change.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProblemKind {
    /// The file does not exist (Go's `ErrConfigFileNotFound`).
    #[error("config file not found: {}", file.display())]
    NotFound {
        /// The file asked for.
        file: PathBuf,
    },
    /// The file could not be read.
    #[error("cannot read config file {}: {kind}", file.display())]
    Unreadable {
        /// The file asked for.
        file: PathBuf,
        /// What the operating system said.
        kind: io::ErrorKind,
    },
    /// The file is larger than any config needs to be.
    #[error("config file is {bytes} bytes, more than the {limit} read")]
    TooLarge {
        /// Its size.
        bytes: u64,
        /// The most that is read.
        limit: u64,
    },
    /// The file is not UTF-8.
    #[error("config file is not UTF-8 text")]
    NotUtf8,
    /// Not YAML.
    #[error("YAML syntax at column {column}: {message}")]
    Syntax {
        /// The parser's message.
        message: String,
        /// The column, counted from 1.
        column: usize,
    },
    /// A tab after `:`, which yaml.v2 reads as a space and the parser refuses.
    #[error("a tab follows ':' at column {column}; Go reads it as a space, write a space")]
    TabAfterColon {
        /// The column of the tab, counted from 1.
        column: usize,
    },
    /// A second YAML document, which Go ignores.
    #[error(
        "a second YAML document starts here; Go reads only the first and ignores the rest, so remove it"
    )]
    MultipleDocuments,
    /// An explicit tag (`!!str`, `!foo`).
    #[error("explicit YAML tags are not read; remove the tag")]
    Tag,
    /// Nesting deeper than any config needs.
    #[error("nested more than {limit} levels deep")]
    TooDeep {
        /// The deepest nesting read.
        limit: usize,
    },
    /// Aliases that expand to more nodes than any config needs.
    #[error("aliases expand to more than {limit} nodes")]
    AliasExpansion {
        /// The most nodes aliases may add.
        limit: usize,
    },
    /// A key that is a mapping or a list.
    #[error("a key must be a scalar")]
    ComplexKey,
    /// A key Go does not define here.
    #[error("unknown key{}", suggestion.map(|s| format!("; did you mean `{s}`?")).unwrap_or_default())]
    Unknown {
        /// The Go key it is closest to, within two edits.
        suggestion: Option<&'static str>,
    },
    /// A key set twice in one mapping.
    #[error(
        "key set twice (first on line {first_line}); Go keeps the last and warns, so remove one"
    )]
    Duplicate {
        /// Where it was first set.
        first_line: usize,
    },
    /// A key set before a `<<` merge that sets it again.
    #[error(
        "set on line {key_line} and again by a later `<<` merge: yaml.v2 lets the merge win, the YAML spec lets the key win; move the key after the merge"
    )]
    MergeOverridesKey {
        /// Where the key was set.
        key_line: usize,
    },
    /// A `<<` merge of something that is not a mapping.
    #[error("a `<<` merge must name a mapping or a list of mappings")]
    MergeNotMapping,
    /// A value of the wrong type.
    #[error("expected {expected}, found {found}")]
    WrongType {
        /// What Go reads this key as.
        expected: Expected,
        /// What the file has.
        found: Found,
    },
    /// A number with a fractional part where Go wants a whole number.
    #[error("a whole number is needed; Go would drop the fraction, so write the value you mean")]
    Fractional,
    /// A number too large or too small for the key.
    #[error("out of range: expected {expected}")]
    OutOfRange {
        /// What fits.
        expected: Expected,
    },
    /// A byte size Go cannot parse.
    #[error("invalid byte size: {0}")]
    ByteSize(SizeError),
    /// An empty item in a list of mappings, which Go would crash on.
    #[error("an empty list item")]
    EmptyItem,
    /// A Go feature litestream-rs does not have, asked for.
    #[error("{feature} is not in litestream-rs; remove the key or set it to its off value")]
    NotInV1 {
        /// The feature, in words.
        feature: &'static str,
    },
    /// A replica type Go has and litestream-rs does not.
    #[error("replica type `{name}` is not in litestream-rs, which replicates to s3 and file only")]
    UnsupportedReplicaType {
        /// The type, from `type:` or the URL scheme.
        name: String,
    },
    /// A replica type Go does not have.
    #[error("unknown replica type `{name}`")]
    UnknownReplicaType {
        /// The type, from `type:` or the URL scheme.
        name: String,
    },
    /// A value outside the ones this key takes.
    #[error("`{value}` is not one of: {allowed}")]
    UnsupportedValue {
        /// The value.
        value: String,
        /// The values it may take.
        allowed: &'static str,
    },
    /// A duration Go requires to be above zero.
    #[error("must be greater than 0 (got {value})")]
    NotPositive {
        /// The duration.
        value: GoDuration,
    },
    /// A duration Go requires to be zero or more.
    #[error("must not be negative (got {value})")]
    Negative {
        /// The duration.
        value: GoDuration,
    },
    /// A heartbeat interval under Go's minute.
    #[error("heartbeat interval must be at least 1 minute (got {value})")]
    HeartbeatIntervalTooShort {
        /// The interval.
        value: GoDuration,
    },
    /// Both `path` and `dir`.
    #[error("cannot specify both 'path' and 'dir'")]
    PathAndDir,
    /// Neither `path` nor `dir`.
    #[error("must specify either 'path' or 'dir'")]
    NeitherPathNorDir,
    /// `dir` without `pattern`.
    #[error("'pattern' is required when using 'dir'")]
    PatternRequired,
    /// `watch` on a single database.
    #[error("'watch' can only be enabled with a directory")]
    WatchWithoutDir,
    /// `meta-dir` on a single database.
    #[error("'meta-dir' can only be used with a directory")]
    MetaDirWithoutDir,
    /// Both `meta-path` and `meta-dir`.
    #[error("cannot specify both 'meta-path' and 'meta-dir'")]
    MetaPathAndMetaDir,
    /// The same database twice.
    #[error("duplicate path (already used by dbs[{first}]); each database can be listed only once")]
    DuplicatePath {
        /// The entry that has it first.
        first: usize,
    },
    /// Database-level snapshot settings that disagree, with no global one.
    #[error(
        "conflicting database snapshot {what}: dbs[{first}] has {first_value}, this one has {value}"
    )]
    ConflictingSnapshot {
        /// `intervals` or `retentions`.
        what: &'static str,
        /// The entry that set it first.
        first: usize,
        /// Its value.
        first_value: GoDuration,
        /// This entry's value.
        value: GoDuration,
    },
    /// Both `replica` and `replicas`.
    #[error("cannot specify 'replica' and 'replicas' on a database")]
    ReplicaAndReplicas,
    /// More than one replica.
    #[error("multiple replicas on a single database are no longer supported")]
    MultipleReplicas,
    /// A URL in `path`.
    #[error("replica path cannot be a url, please use the 'url' field instead")]
    ReplicaPathIsUrl,
    /// `truncate-page-n` without the operator's claim that the application holds no long readers.
    #[error(
        "TRUNCATE checkpoints need `litestream-rs: {{app-readers: none}}`: without that claim litestream-rs never truncates (an application reader would make every attempt a wasted snapshot)"
    )]
    TruncateNeedsClaim,
    /// A path that cannot be made absolute.
    #[error("{0}")]
    Path(PathError),
}

/// One reason the file is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Problem {
    /// The key it is about.
    pub path: KeyPath,
    /// The line, counted from 1, where known.
    pub line: Option<usize>,
    /// What is wrong.
    pub kind: ProblemKind,
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(line) => write!(f, "{} (line {line}): {}", self.path, self.kind),
            None => write!(f, "{}: {}", self.path, self.kind),
        }
    }
}

/// Something the file asks that litestream-rs reads but does not fully do, or that reads
/// oddly. Never a refusal.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum WarningKind {
    /// A log level or format Go does not know: Go's logger keeps its default, and so does
    /// litestream-rs.
    #[error("`{value}` is not a log {what} Go knows; it logs at {used}, as Go does")]
    UnknownLogSetting {
        /// What was given.
        value: String,
        /// `level` or `format`.
        what: &'static str,
        /// What is used instead.
        used: &'static str,
    },
    /// A Go tuning knob with no counterpart in litestream-rs.
    #[error("has no effect in litestream-rs: {why}")]
    NoEffect {
        /// Why.
        why: &'static str,
    },
    /// A duration written as a bare number, which Go reads as nanoseconds.
    #[error(
        "a bare number is a duration in nanoseconds ({value}), as Go reads it; add a unit if that is not what you mean"
    )]
    BareNumberDuration {
        /// What it was read as.
        value: GoDuration,
    },
    /// `$NAME` or `${NAME}` with no such variable, expanded to nothing as Go does.
    #[error("environment variable {name} is not set; it expands to nothing, as in Go")]
    UnsetVariable {
        /// The variable.
        name: String,
    },
    /// Snapshot retention below the interval (Go warns too).
    #[error("snapshot retention {retention} is shorter than snapshot interval {interval}")]
    RetentionShorterThanInterval {
        /// The retention.
        retention: GoDuration,
        /// The interval.
        interval: GoDuration,
    },
    /// Go would TRUNCATE the WAL at 121359 pages; litestream-rs does not without the claim.
    #[error(
        "Go checkpoints with TRUNCATE at truncate-page-n (121359 pages by default); litestream-rs does not unless `litestream-rs: {{app-readers: none}}` states the application holds no long readers"
    )]
    NoTruncateWithoutClaim,
}

/// A warning about one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Warning {
    /// The key it is about.
    pub path: KeyPath,
    /// The line, counted from 1, where known.
    pub line: Option<usize>,
    /// What it says.
    pub kind: WarningKind,
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(line) => write!(f, "{} (line {line}): {}", self.path, self.kind),
            None => write!(f, "{}: {}", self.path, self.kind),
        }
    }
}

/// Everything a load found, for `lsr config check`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Report {
    /// Every key the file sets, with its class, in file order.
    pub keys: Vec<(KeyPath, KeyClass)>,
    /// Warnings.
    pub warnings: Vec<Warning>,
    /// Problems; the file is refused when there is any.
    pub errors: Vec<Problem>,
}

impl Report {
    /// Whether the file loads.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for problem in &self.errors {
            writeln!(f, "error: {problem}")?;
        }
        for warning in &self.warnings {
            writeln!(f, "warning: {warning}")?;
        }
        if self.errors.is_empty() {
            writeln!(f, "ok: {} keys read", self.keys.len())
        } else {
            writeln!(f, "refused: {} errors", self.errors.len())
        }
    }
}

/// A refused file: the report, with at least one problem.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{}", first_line(.0))]
pub struct ConfigError(Report);

fn first_line(report: &Report) -> String {
    match report.errors.as_slice() {
        [] => "config refused".to_owned(),
        [only] => only.to_string(),
        [first, rest @ ..] => format!("{first} (and {} more)", rest.len()),
    }
}

impl ConfigError {
    pub(crate) fn new(report: Report) -> Self {
        Self(report)
    }

    /// Everything the load found.
    #[must_use]
    pub fn report(&self) -> &Report {
        &self.0
    }

    /// The problems; never empty.
    #[must_use]
    pub fn problems(&self) -> &[Problem] {
        &self.0.errors
    }

    /// Whether the file does not exist (Go's `ErrConfigFileNotFound`).
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.0
            .errors
            .iter()
            .any(|p| matches!(p.kind, ProblemKind::NotFound { .. }))
    }
}
