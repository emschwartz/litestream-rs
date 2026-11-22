//! Go's `flag` package, as Litestream's commands use it (`flag.ContinueOnError`): a command
//! line read against a table of the flags a command defines.
//!
//! `-flag` or `--flag`; a value as `-flag=value` or the next argument; a boolean only as
//! `-flag` or `-flag=<bool>` (Go's `strconv.ParseBool` spellings), set as it is read, so a
//! bad value stops the scan there; flags end at the first argument that does not start with
//! `-` (a lone `-` included) or at `--`, which is dropped; the last of a repeated flag wins.
//! `-h` and `-help`, when the command does not define them, stop the scan as a request for
//! help, which Go's `main` then treats as success only when an argument is literally `-h`,
//! `-help` or `--help` (`cmd/litestream/main.go:89-96`).

/// One flag a command defines.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Def {
    /// The name, without dashes.
    pub name: &'static str,
    /// Whether it takes a value; `false` for a boolean.
    pub value: bool,
}

/// A value flag.
#[must_use]
pub const fn value(name: &'static str) -> Def {
    Def { name, value: true }
}

/// A boolean flag.
#[must_use]
pub const fn boolean(name: &'static str) -> Def {
    Def { name, value: false }
}

/// Why Go's `flag` package stops.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum FlagError {
    /// A flag the command does not define.
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
}

/// What a scan found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scan {
    /// `-h` or `-help`, which the command does not define.
    Help,
    /// The flags set, in order, and the positional arguments.
    Set(Set),
}

/// The flags set and the positional arguments after them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Set {
    flags: Vec<(&'static str, String)>,
    positional: Vec<String>,
}

impl Set {
    /// The last value given for `name`.
    #[must_use]
    pub fn value(&self, name: &str) -> Option<String> {
        self.flags
            .iter()
            .rev()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.clone())
    }

    /// Whether `name` was given at all (Go's `FlagSet.Visit`).
    #[must_use]
    pub fn visited(&self, name: &str) -> bool {
        self.flags.iter().any(|(n, _)| *n == name)
    }

    /// A boolean's value; `false` when not given.
    ///
    /// # Errors
    /// [`FlagError::BadBool`]; the scan has already refused one, so only a caller that names
    /// a value flag here sees it.
    pub fn boolean(&self, name: &'static str) -> Result<bool, FlagError> {
        self.value(name).map_or(Ok(false), |v| parse_bool(name, &v))
    }

    /// The positional arguments.
    #[must_use]
    pub fn positional(&self) -> &[String] {
        &self.positional
    }
}

/// Go's `strconv.ParseBool`.
///
/// # Errors
/// [`FlagError::BadBool`].
pub fn parse_bool(name: &'static str, value: &str) -> Result<bool, FlagError> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(FlagError::BadBool {
            name,
            value: value.to_owned(),
        }),
    }
}

/// Whether an argument is literally `-h`, `-help` or `--help`, the only help Go's `main`
/// exits 0 for.
#[must_use]
pub fn literal_help(args: &[String]) -> bool {
    args.iter()
        .any(|a| matches!(a.as_str(), "-h" | "-help" | "--help"))
}

/// Reads `args` against `defs`.
///
/// # Errors
/// [`FlagError`], the first Go would report.
pub fn scan(defs: &[Def], args: &[String]) -> Result<Scan, FlagError> {
    let mut flags = Vec::new();
    let mut rest = args.iter();
    while let Some(arg) = rest.next() {
        let Some(body) = arg.strip_prefix('-').filter(|b| !b.is_empty()) else {
            let mut positional = vec![arg.clone()];
            positional.extend(rest.cloned());
            return Ok(Scan::Set(Set { flags, positional }));
        };
        let body = body.strip_prefix('-').unwrap_or(body);
        if body.is_empty() {
            let positional = rest.cloned().collect();
            return Ok(Scan::Set(Set { flags, positional }));
        }
        let (name, inline) = match body.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned())),
            None => (body, None),
        };
        let Some(def) = defs.iter().find(|d| d.name == name) else {
            if name == "h" || name == "help" {
                return Ok(Scan::Help);
            }
            return Err(FlagError::UnknownFlag {
                name: name.to_owned(),
            });
        };
        let name = def.name;
        let value = match (def.value, inline) {
            (false, Some(value)) => {
                parse_bool(name, &value)?;
                value
            }
            (true, Some(value)) => value,
            (true, None) => rest
                .next()
                .cloned()
                .ok_or(FlagError::MissingValue { name })?,
            (false, None) => "true".to_owned(),
        };
        flags.push((name, value));
    }
    Ok(Scan::Set(Set {
        flags,
        positional: Vec::new(),
    }))
}
