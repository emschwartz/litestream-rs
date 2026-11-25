//! Go Litestream's `litestream.yml`, read as Go 041a8c3 reads it, and held to
//! litestream-rs's rule that a file never means less than it says.
//!
//! A load runs, in order:
//! 1. **Environment expansion** over the raw text (`expand.rs`), unless
//!    [`Expand::Off`] (Go's `-no-expand-env`).
//! 2. **YAML** into a node tree (`yaml.rs`, on `saphyr-parser`), settling the
//!    places yaml.v2 and the spec disagree (merges, duplicates, documents).
//! 3. **The walk** (`walk.rs`): every key looked up in Go's tables
//!    (`keys.rs`) and read as yaml.v2 reads that Go type (`value.rs`), so
//!    `yes` is a boolean, `0600` is octal, and a bare `5` duration is 5ns. Each
//!    key's class applies: implemented keys are kept, off-only keys are refused
//!    unless at their off value, no-effect keys warn, unknown keys are refused
//!    with a "did you mean".
//! 4. **The build** (`build.rs`): Go's defaults, snapshot promotion, path
//!    expansion, top-level replica settings copied down, and Go's validation.
//!
//! Every problem is collected, not just the first, and names its key path and
//! line. Rust-only settings live under the one top-level key `litestream-rs`,
//! which Go ignores, so the same file still starts Go.
//!
//! # Starting the library from the same file
//!
//! An application that replicates in its own process reads the file the daemon would read:
//! [`load_plan`] gives one [`DbPlan`] per database, [`open_store`] opens its replica, and
//! `lsr_replica::Replicator::start` takes the path and the [`DbPlan::config`]. `docs/config-keys.md`
//! lists what every key sets (or that it is not supported or has no effect). The daemon-only keys
//! (`addr`, `exec`, `logging`) are loaded and ignored here.
//!
//! ```no_run
//! use lsr_config::{Expand, ProcessEnv, load_plan, open_store};
//! use lsr_replica::Replicator;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let plan = load_plan("litestream.yml".as_ref(), Expand::Env, &ProcessEnv)?;
//! let mut replicators = Vec::new();
//! for db in plan.dbs() {
//!     let store = open_store(db.replica())?;
//!     // With `db.restore_if_missing()`, call `lsr_replica::restore_if_missing` first.
//!     replicators.push(Replicator::start(db.path(), store, db.config().clone())?);
//! }
//! # Ok(())
//! # }
//! ```

mod build;
mod config;
mod duration;
mod error;
mod expand;
mod keys;
mod load;
mod map;
mod size;
mod store;
mod value;
mod walk;
mod yaml;

use std::io::Read as _;
use std::path::Path;

pub use config::{
    AppReaders, DbConfig, DbSource, DirSource, GoConfig, LogFormat, LogLevel, Logging,
    OtherWriterSetting, ReplicaConfig, ReplicaKind, ResponseSetting, ResponseSettings,
    RustSettings, S3Settings, Secret, Snapshot, SnapshotOverride, defaults,
};
pub use duration::{DurationError, GoDuration};
pub use error::{
    ConfigError, Expected, Found, KeyClass, KeyPath, Problem, ProblemKind, Report, Warning,
    WarningKind,
};
pub use expand::{Env, Expand, PathError, ProcessEnv, clean, expand_path, strip_sqlite_prefix};
pub use load::{LoadPlanError, load_plan};
pub use map::{
    Credentials, DbPlan, DirProblem, MapError, MapProblem, MapProblemKind, Note, Plan, ReplicaPlan,
    ReplicaProblem, S3Overrides, S3Url, default_meta, plan, s3_config,
};
pub use size::{SizeError, parse_byte_size};
pub use store::{OpenStoreError, open_store};

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
                for name in read_by_a_value(text, env, expanded.unset) {
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
    /// A [`ConfigError`] when the URL names a replica type litestream-rs does not have.
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

/// The names of `unset` that some key or value reads. Go expands the whole file and warns of
/// nothing, so a reference in a comment, which changes nothing, is not worth a warning: each
/// unset name is expanded to a marker of its own and the marker looked for in the parsed tree.
/// A file that does not parse with the markers is refused later anyway; its names all stay.
fn read_by_a_value(text: &str, env: &dyn Env, unset: Vec<String>) -> Vec<String> {
    if unset.is_empty() {
        return unset;
    }
    let mut seen = String::new();
    match yaml::parse(&expand::expand_probe(text, env)) {
        Ok((root, _)) => {
            if let Some(root) = root {
                root.push_text(&mut seen);
            }
        }
        Err(_) => return unset,
    }
    unset
        .into_iter()
        .enumerate()
        .filter(|(index, _)| seen.contains(&expand::probe_sentinel(*index)))
        .map(|(_, name)| name)
        .collect()
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
