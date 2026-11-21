//! Go Litestream's `litestream.yml`, read as Go 041a8c3 reads it, and held to
//! v1's rule that a file never means less than it says
//! (docs/plans/phase-4-sidecar.md, the config ruling and task 1).
//!
//! A load runs, in order:
//! 1. **Environment expansion** over the raw text (`expand.rs`), unless
//!    [`Expand::Off`] (Go's `-no-expand-env`).
//! 2. **YAML** into a node tree (`yaml.rs`, on `saphyr-parser`), settling the
//!    places yaml.v2 and the spec disagree (merges, duplicates, documents).
//! 3. **The walk** (`walk.rs`): every key looked up in Go's tables
//!    (`keys.rs`) and read as yaml.v2 reads that Go type (`value.rs`), so
//!    `yes` is a boolean, `0600` is octal, and a bare `5` duration is 5ns. Each
//!    key's class applies: implemented keys are kept, inert keys are refused
//!    unless at their off value, no-effect keys warn, unknown keys are refused
//!    with a "did you mean".
//! 4. **The build** (`build.rs`): Go's defaults, snapshot promotion, path
//!    expansion, top-level replica settings copied down, and Go's validation.
//!
//! Every problem is collected, not just the first, and names its key path and
//! line. Rust-only settings live under the one top-level key `litestream-rs`,
//! which Go ignores, so the same file still starts Go.

mod build;
mod config;
mod duration;
mod error;
mod expand;
mod keys;
mod map;
mod size;
mod value;
mod walk;
mod yaml;

use std::io::Read as _;
use std::path::Path;

pub use config::{
    DbConfig, DbSource, DirSource, ForeignWriterSetting, GoConfig, HostReaders, LogFormat,
    LogLevel, Logging, ReplicaConfig, ReplicaKind, ResponseSetting, ResponseSettings, RustSettings,
    S3Settings, Secret, Snapshot, SnapshotOverride, defaults,
};
pub use duration::{DurationError, GoDuration};
pub use error::{
    ConfigError, Expected, Found, KeyClass, KeyPath, Problem, ProblemKind, Report, Warning,
    WarningKind,
};
pub use expand::{Env, Expand, PathError, ProcessEnv, clean, expand_path, strip_sqlite_prefix};
pub use map::{
    Credentials, DbPlan, DirProblem, MapError, MapProblem, MapProblemKind, Note, Plan, ReplicaPlan,
    ReplicaProblem, S3Overrides, S3Url, default_meta, plan, s3_config,
};
pub use size::{SizeError, parse_byte_size};

use error::Problem as P;
use walk::Walk;

/// The most of a config file that is read.
pub const MAX_FILE_BYTES: u64 = 1 << 20;

/// A loaded config and what the load found (warnings and key classes; no errors).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Loaded {
    /// The config.
    pub config: GoConfig,
    /// The warnings and every key's class.
    pub report: Report,
}

impl GoConfig {
    /// Reads and parses a config file.
    ///
    /// # Errors
    /// A [`ConfigError`] listing every problem, the missing file included
    /// ([`ConfigError::is_not_found`]).
    pub fn load(path: &Path, expand: Expand, env: &dyn Env) -> Result<Loaded, ConfigError> {
        let text = read_file(path).map_err(|problem| ConfigError::new(report_of(problem)))?;
        Self::parse(&text, expand, env)
    }

    /// Parses config text as Go's `ParseConfig`.
    ///
    /// # Errors
    /// A [`ConfigError`] listing every problem.
    pub fn parse(text: &str, expand: Expand, env: &dyn Env) -> Result<Loaded, ConfigError> {
        let mut walk = Walk::default();
        let text = match expand {
            Expand::Off => text.to_owned(),
            Expand::Env => {
                let expanded = expand::expand_env(text, env);
                for name in expanded.unset {
                    walk.warn(KeyPath::root(), None, WarningKind::UnsetVariable { name });
                }
                expanded.text
            }
        };
        let (root, problems) = match yaml::parse(&text) {
            Ok(parsed) => parsed,
            Err(problem) => {
                let mut report = report_of(problem);
                report.warnings = walk.warnings;
                return Err(ConfigError::new(report));
            }
        };
        walk.problems.extend(problems);
        let top = walk.top(root.as_ref());
        let config = build::build(&top, env, &mut walk);
        let report = Report {
            keys: walk.keys,
            warnings: walk.warnings,
            errors: walk.problems,
        };
        if report.is_ok() {
            Ok(Loaded { config, report })
        } else {
            Err(ConfigError::new(report))
        }
    }
}

impl GoConfig {
    /// The config Go's `replicate DB_PATH REPLICA_URL` builds: Go's defaults and one
    /// database with one replica, read through the same checks as a file.
    ///
    /// # Errors
    /// A [`ConfigError`] when the URL names a replica type v1 does not have.
    pub fn for_one_database(
        database: &str,
        replica_url: &str,
        env: &dyn Env,
    ) -> Result<Loaded, ConfigError> {
        let text = format!(
            "dbs:\n  - path: {}\n    replica:\n      url: {}\n",
            quoted(database),
            quoted(replica_url)
        );
        Self::parse(&text, Expand::Off, env)
    }
}

/// A YAML double-quoted scalar holding `text` exactly.
fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len().saturating_add(2));
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn report_of(problem: P) -> Report {
    Report {
        errors: vec![problem],
        ..Report::default()
    }
}

fn read_file(path: &Path) -> Result<String, P> {
    let file_problem = |kind| P {
        path: KeyPath::root(),
        line: None,
        kind,
    };
    let file = std::fs::File::open(path).map_err(|error| {
        let kind = error.kind();
        file_problem(if kind == std::io::ErrorKind::NotFound {
            ProblemKind::NotFound {
                file: path.to_owned(),
            }
        } else {
            ProblemKind::Unreadable {
                file: path.to_owned(),
                kind,
            }
        })
    })?;
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| {
            file_problem(ProblemKind::Unreadable {
                file: path.to_owned(),
                kind: error.kind(),
            })
        })?;
    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if len > MAX_FILE_BYTES {
        return Err(file_problem(ProblemKind::TooLarge {
            bytes: len,
            limit: MAX_FILE_BYTES,
        }));
    }
    String::from_utf8(bytes).map_err(|_invalid| file_problem(ProblemKind::NotUtf8))
}

/// What loading config text found, refused or not: every key, warning and problem of
/// the load alone. `lsr config check` also runs the daemon's start checks
/// (`lsr_sidecar::check`), which this does not.
#[must_use]
pub fn check_text(text: &str, expand: Expand, env: &dyn Env) -> Report {
    match GoConfig::parse(text, expand, env) {
        Ok(loaded) => loaded.report,
        Err(error) => error.report().clone(),
    }
}
