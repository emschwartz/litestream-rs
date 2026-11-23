//! The commands: parse result in, a [`Done`] or a [`CliError`] out, and the one
//! place that maps either to an [`Exit`] code.
//!
//! A restore never touches an existing target: [`FileSink::create`] refuses it
//! and any stale journal file, and the executor publishes by hard link. With
//! `--clear-journal-files` the stale files are removed first and a failure after that
//! still reports what was removed. With `--integrity-check` the sink runs
//! SQLite's check on the scratch before publishing; a database SQLite finds
//! invalid is a refused restore ([`CliError::Integrity`]), and nothing is left
//! at the output.
//!
//! Go's restore switches, with Go's names: `--if-db-not-exists` looks at the
//! output before the replica is opened and ends with [`Skip::OutputExists`];
//! `--if-replica-exists` turns [`PlanOutcome::EmptyReplica`], and only that, into
//! [`Skip::EmptyReplica`], because a replica with files that cannot meet the
//! target is a refusal, never a quiet success. A directory with no `ltx/` folder,
//! or none at all, is an empty replica to a restore, as Go's file replica lists it
//! (it ends as [`Skip::NoLtxFolder`] or [`CliError::NoLtxFolder`], so the reason is
//! named); `verify` and `files` still refuse it as a wrong path. `--dry-run` runs
//! the restore's own checks of the output read-only (`preflight`), lists and plans
//! exactly as the restore does, and stops before the first write; as in Go
//! (`cmd/litestream/restore.go:119-122`) it ignores `--if-replica-exists`, so an
//! empty replica is refused there. Every way a restore ends
//! is one [`Finished`] value, so the text and the JSON describe the same thing.

use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lsr_config::{
    Credentials, Expand, GoConfig, ProcessEnv, ReplicaKind, ReplicaPlan, expand_path,
};
use lsr_core::{
    AsOfInside, AsOfPlanner, ContiguousChain, Depth, Inventory, InventoryError, KeyTarget,
    MAX_ATTEMPTS, ObjectStore, PlanOutcome, Report, ResolveError, RestoreError, RestoreOptions,
    RestoreTarget, Restored, Severity, VerifyError, listed_past, plan, resolve, restore_with,
    verify,
};
use lsr_replica::local::{
    ClearError, CreateError, Existing, FileSink, IntegrityCheck, IntegrityFailure, PublishError,
    clear_journal_files, preflight,
};
use lsr_store_fs::FsStore;
use lsr_store_s3::{ConfigError, CredentialError, S3Credentials};
use thiserror::Error;

use crate::args::{
    Command, FilesArgs, Replica, RestoreArgs, RestoreFrom, TakeOverArgs, UsageError, VerifyArgs,
};
use crate::render;
use crate::replica::{self, Remote};

/// How the process ends. One closed enum, mapped from results in
/// [`exit_for_done`] and [`exit_for_error`] only.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Exit {
    /// Done; for verify, nothing worse than a note.
    Success,
    /// Verify found something degraded.
    Degraded,
    /// Verify found the replica unrestorable, or a restore was refused.
    Refused,
    /// The environment failed: store, sink, I/O.
    Environment,
    /// The command line is wrong.
    Usage,
    /// `replicate -exec`: the run did not end clean, for any reason (the child failed or did
    /// not start, or a database did not start or drain). Go's `main` exits 1 on every error
    /// (`main.go:97-103`), so the child's own code is not passed on; it is in the log.
    ExecFailed,
    /// `replicate -exec`: fail-stop held the application (`if-replication-stops`, decision
    /// D19): the child was stopped for safety, or never started. A code of its own, so a
    /// supervisor can tell a stop for safety from a crash. The code and its line were approved by
    /// the owner (decision D84).
    AppStopped,
}

impl Exit {
    /// Every code, for the help text.
    pub const ALL: [Self; 7] = [
        Self::Success,
        Self::Degraded,
        Self::ExecFailed,
        Self::Refused,
        Self::Environment,
        Self::AppStopped,
        Self::Usage,
    ];

    /// The process exit status.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::Degraded | Self::ExecFailed => 1,
            Self::Refused => 2,
            Self::Environment => 3,
            Self::AppStopped => 4,
            Self::Usage => 64,
        }
    }

    /// What the code means, for the help text.
    #[must_use]
    pub const fn meaning(self) -> &'static str {
        match self {
            Self::Success => "success (verify: nothing worse than a note)",
            Self::Degraded => "verify found a degraded replica",
            Self::Refused => {
                "verify found it unrestorable, the restore was refused, or a config was refused"
            }
            Self::Environment => {
                "store, destination or I/O failure; for replicate without -exec, a database that did not start or drain"
            }
            Self::Usage => "usage error",
            Self::AppStopped => {
                "replicate -exec: replication stopped, so the application was stopped or never started (if-replication-stops)"
            }
            Self::ExecFailed => {
                "replicate -exec: the run failed, as Go exits 1 (the child exited non-zero, died by a signal outside a shutdown, was killed or did not start, or a database did not start or drain); the child's own code is in the log"
            }
        }
    }
}

/// Where a restore reads from, named without anything the URL may hide: an
/// S3 replica is `s3://<bucket>/<prefix>`, never the typed URL with its query.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Source {
    kind: ReplicaKind,
    name: String,
}

impl Source {
    fn of(replica: &Replica) -> Self {
        match replica {
            Replica::Dir(path) => Self {
                kind: ReplicaKind::File,
                name: path.display().to_string(),
            },
            Replica::S3(config) => Self {
                kind: ReplicaKind::S3,
                name: format!(
                    "s3://{}/{}",
                    config.bucket().as_str(),
                    config.prefix().as_str()
                ),
            },
        }
    }

    /// A replica URL not opened (Go's early skip): its scheme and nothing else, since the
    /// rest may hold credentials.
    fn of_url(url: &str) -> Self {
        let scheme = url.split_once("://").map_or("", |(scheme, _)| scheme);
        let kind = if scheme.eq_ignore_ascii_case("s3") {
            ReplicaKind::S3
        } else {
            ReplicaKind::File
        };
        Self {
            kind,
            name: format!("{scheme}://"),
        }
    }

    /// The store's kind.
    #[must_use]
    pub const fn kind(&self) -> ReplicaKind {
        self.kind
    }

    /// The replica's printable name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Why a restore succeeded without restoring.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Skip {
    /// `--if-db-not-exists`: something exists at the output.
    OutputExists,
    /// `--if-replica-exists`: the replica holds no LTX file.
    EmptyReplica,
    /// `--if-replica-exists`: the directory has no `ltx/` folder, or does not exist, which
    /// Go lists as an empty replica.
    NoLtxFolder,
}

/// How a restore that did not fail ended.
#[derive(Debug)]
pub enum Finished {
    /// A database was published.
    Restored {
        /// What the executor did.
        restored: Restored,
        /// Stale journal files removed first.
        cleared: Vec<PathBuf>,
        /// The SQLite check the database passed before it was published.
        integrity_check: IntegrityCheck,
        /// Wall time of the clearing and the restore, for Go's `duration_ms`.
        elapsed: Duration,
    },
    /// `--dry-run`: the files a restore would apply; nothing was written.
    Planned {
        /// The chain, in the order it would be applied.
        chain: ContiguousChain,
        /// Exact created-at times read to plan an as-of target.
        created_at_read: u64,
        /// Stale journal files `--clear-journal-files` would remove first.
        journal_files_to_remove: Vec<PathBuf>,
    },
    /// Nothing was restored, by request.
    Skipped {
        /// Why.
        skip: Skip,
        /// Stale journal files removed before the replica turned out empty.
        cleared: Vec<PathBuf>,
    },
}

/// What `lsr restore` produced.
#[derive(Debug)]
pub struct RestoreDone {
    /// The database path given.
    pub output: PathBuf,
    /// Where from.
    pub source: Source,
    /// Whether to print JSON.
    pub json: bool,
    /// How it ended.
    pub finished: Finished,
    /// `-parallelism` was given: Go warns that it has no effect.
    pub parallelism: bool,
}

/// What a command produced.
#[derive(Debug)]
pub enum Done {
    /// A restore ran to an end: restored, planned or skipped.
    Restore(Box<RestoreDone>),
    /// A replica was checked.
    Verified {
        /// How much was read.
        depth: Depth,
        /// What was found.
        report: Box<Report>,
        /// Whether to print JSON.
        json: bool,
    },
    /// A replica was listed.
    Files {
        /// Every file.
        inventory: Inventory,
        /// For an S3 replica listed with `--created-at`, how many files had no stored
        /// created-at time and show the object's `LastModified` instead (replicas from Go
        /// v0.5.0 have no created-at times).
        created_at_fallbacks: Option<u64>,
        /// Whether to print JSON.
        json: bool,
    },
    /// A takeover was recorded, through this transaction (`None`: the replica is empty and
    /// nothing was recorded), in this meta directory.
    TakenOver {
        /// The replica's newest transaction.
        through: Option<lsr_ltx::Txid>,
        /// Where the record went.
        meta: PathBuf,
    },
    /// The daemon ran and stopped; it logged everything as it went.
    Daemon(lsr_sidecar::Finished),
    /// The daemon's own usage text.
    DaemonHelp(&'static str),
    /// A config file was read and put through the daemon's checks; the result says
    /// whether it would start.
    ConfigChecked(Box<lsr_sidecar::Checked>),
    /// The help text.
    Help,
    /// The version.
    Version,
}

/// Why a command failed.
#[derive(Debug, Error)]
pub enum CliError {
    /// The command line.
    #[error("{0}")]
    Usage(#[from] UsageError),
    /// `--replica` is not a directory holding `ltx/`.
    #[error("{} is not a file replica: no ltx/ directory inside it", .path.display())]
    NoReplica {
        /// The path given.
        path: PathBuf,
    },
    /// An S3 replica needs credentials and no source had them, or a shared
    /// credentials or config file could not be used.
    #[error("S3 credentials: {0}")]
    Credentials(CredentialError),
    /// The S3 endpoint cannot address the bucket.
    #[error("S3 replica: {0}")]
    S3Config(ConfigError),
    /// The replica could not be listed completely.
    #[error("listing the replica: {0}")]
    Inventory(#[from] InventoryError),
    /// The replica has no exact answer for the target.
    #[error("no exact restore: {}", render::plan_outcome(.0))]
    NoPlan(PlanOutcome),
    /// A restore from a directory with no `ltx/` folder (or no directory at all), which Go
    /// reads as an empty replica, without `--if-replica-exists` or with `--dry-run`.
    #[error("no exact restore: {} holds no backup: no ltx/ folder inside it", .path.display())]
    NoLtxFolder {
        /// The path given.
        path: PathBuf,
    },
    /// SQLite found the restored database invalid; nothing was published.
    #[error("{0}; nothing was published")]
    Integrity(IntegrityFailure),
    /// Publishing the restored database failed; the error says whether it is visible at
    /// the output (complete and verified, but maybe not durable) or not there at all.
    #[error("{0}")]
    Publish(PublishError),
    /// The restore failed for another reason.
    #[error("{0}")]
    Restore(RestoreError),
    /// The verify walk stopped.
    #[error("{0}")]
    Verify(#[from] VerifyError),
    /// The takeover record could not be written: by `lsr take-over`, or after a restore that
    /// was published (a replicator started on it then refuses until `lsr take-over` runs).
    #[error("the takeover record cannot be written in {}: {kind:?}", .meta.display())]
    Takeover {
        /// The meta directory.
        meta: PathBuf,
        /// What the file system said.
        kind: std::io::ErrorKind,
    },
    /// The database path names no file, so it has no meta directory.
    #[error("{} names no file, so it has no meta directory for the takeover record (lsr take-over accepts --meta-dir)", .path.display())]
    NoMetaDir {
        /// The path given.
        path: PathBuf,
    },
    /// A stale SQLite journal file sits beside the output.
    #[error("{} exists beside the output and SQLite would replay it over the restored database; remove it or pass --clear-journal-files", .path.display())]
    JournalFile {
        /// The journal file.
        path: PathBuf,
    },
    /// The destination cannot take a restore.
    #[error("{0}")]
    Create(CreateError),
    /// Stale journal files could not be removed.
    #[error("{0}")]
    Clear(#[from] ClearError),
    /// Something failed after stale journal files were already removed.
    #[error("{source}; the stale files {} were removed first", render::paths(.cleared))]
    AfterClearing {
        /// What was removed.
        cleared: Vec<PathBuf>,
        /// What failed.
        source: Box<CliError>,
    },
    /// Writing the result failed.
    #[error("writing the result: {0}")]
    Output(#[source] io::Error),
    /// A replica URL `restore` was given is not a usable replica (Go's `NewReplicaFromConfig`).
    #[error("{0}")]
    Config(lsr_config::ConfigError),
    /// The config, or the replica a URL names, cannot be planned.
    #[error("{0}")]
    Plan(lsr_config::MapError),
    /// The planned config holds no database (a URL plan always holds one).
    #[error("no database to restore")]
    NoDatabase,
    /// A `DB_PATH` that cannot be made absolute.
    #[error("{0}")]
    DatabasePath(lsr_config::PathError),
    /// A `DB_PATH` no `path` entry of the config names (Go's `DBConfig`).
    #[error("database not found in config: {}", .path.display())]
    NotInConfig {
        /// The path, expanded.
        path: PathBuf,
    },
    /// A replica URL with a timestamp and a replica that holds nothing (Go's
    /// `CalcRestoreTarget`, which `-if-replica-exists` does not excuse).
    #[error("no backups found")]
    NoBackupsFound,
    /// The output's parent directory could not be created.
    #[error("create parent directory: {0}")]
    Parent(#[source] io::Error),
    /// The daemon did not start: its command line, its config, or its signal handlers.
    #[error("replicate: {0}")]
    Daemon(#[from] lsr_sidecar::SidecarError),
}

impl From<RestoreError> for CliError {
    fn from(err: RestoreError) -> Self {
        match err {
            RestoreError::NoPlan(none) => Self::NoPlan(none.outcome().clone()),
            RestoreError::Publish { left, source } => match source.downcast::<PublishError>() {
                Ok(publish) => match *publish {
                    PublishError::Integrity(failure) => Self::Integrity(failure),
                    other @ (PublishError::NotVisible { .. }
                    | PublishError::VisibleNotDurable { .. }) => Self::Publish(other),
                },
                Err(source) => Self::Restore(RestoreError::Publish { left, source }),
            },
            other @ (RestoreError::Inventory(_)
            | RestoreError::Store(_)
            | RestoreError::File { .. }
            | RestoreError::Unstable { .. }
            | RestoreError::ListingBehind { .. }
            | RestoreError::DatabaseChecksum { .. }
            | RestoreError::CreatedAtRepeated { .. }
            | RestoreError::Sink { .. }) => Self::Restore(other),
        }
    }
}

impl From<CreateError> for CliError {
    fn from(err: CreateError) -> Self {
        match err {
            CreateError::JournalFile { path } => Self::JournalFile { path },
            other @ (CreateError::TargetExists(_)
            | CreateError::RestoreInProgress(_)
            | CreateError::Inspect { .. }
            | CreateError::Scratch { .. }) => Self::Create(other),
        }
    }
}

/// The exit code of a finished command.
#[must_use]
pub fn exit_for_done(done: &Done) -> Exit {
    match done {
        Done::Verified { report, .. } => match report.worst() {
            None | Some(Severity::Note) => Exit::Success,
            Some(Severity::Degraded) => Exit::Degraded,
            Some(Severity::Unrestorable) => Exit::Refused,
        },
        Done::Daemon(finished) if finished.app_held.is_some() => Exit::AppStopped,
        Done::Daemon(finished) => match (finished.clean(), finished.ran_exec()) {
            (true, _) => Exit::Success,
            (false, true) => Exit::ExecFailed,
            (false, false) => Exit::Environment,
        },
        Done::ConfigChecked(checked) => config_exit(checked),
        Done::Restore(_)
        | Done::TakenOver { .. }
        | Done::Files { .. }
        | Done::DaemonHelp(_)
        | Done::Help
        | Done::Version => Exit::Success,
    }
}

/// A config that would start is success. One the load refuses is a refusal, unless the file
/// could not be read at all; one the daemon's checks refuse exits as the daemon would.
fn config_exit(checked: &lsr_sidecar::Checked) -> Exit {
    let report = checked.report();
    use lsr_config::ProblemKind;
    let unreadable = report.errors.iter().any(|problem| {
        matches!(
            problem.kind,
            ProblemKind::NotFound { .. } | ProblemKind::Unreadable { .. }
        )
    });
    if let Some(first) = checked.refusals().first() {
        daemon_exit(first)
    } else if report.is_ok() {
        Exit::Success
    } else if unreadable {
        Exit::Environment
    } else {
        Exit::Refused
    }
}

/// The daemon's errors, on the one table.
fn daemon_exit(error: &lsr_sidecar::SidecarError) -> Exit {
    use lsr_sidecar::SidecarError;
    match error {
        SidecarError::Usage(_) => Exit::Usage,
        SidecarError::Config(_) if error.is_missing_config() => Exit::Environment,
        SidecarError::Config(_)
        | SidecarError::Map(_)
        | SidecarError::NotBuilt { .. }
        | SidecarError::Exec(_)
        | SidecarError::StopAppWithoutExec
        | SidecarError::Addr(_) => Exit::Refused,
        SidecarError::Stdin(_) | SidecarError::Signals(_) => Exit::Environment,
    }
}

/// The exit code of a failed command.
#[must_use]
pub fn exit_for_error(error: &CliError) -> Exit {
    match error {
        CliError::Usage(_) => Exit::Usage,
        CliError::NoPlan(_) | CliError::NoLtxFolder { .. } | CliError::Integrity(_) => {
            Exit::Refused
        }
        CliError::Config(_)
        | CliError::Plan(_)
        | CliError::NoDatabase
        | CliError::DatabasePath(_)
        | CliError::NotInConfig { .. } => Exit::Refused,
        CliError::Parent(_) => Exit::Environment,
        CliError::NoBackupsFound => Exit::Refused,
        CliError::Restore(err) => match err {
            RestoreError::NoPlan(_)
            | RestoreError::File { .. }
            | RestoreError::DatabaseChecksum { .. } => Exit::Refused,
            RestoreError::Inventory(_)
            | RestoreError::Store(_)
            | RestoreError::Unstable { .. }
            | RestoreError::ListingBehind { .. }
            | RestoreError::CreatedAtRepeated { .. }
            | RestoreError::Sink { .. }
            | RestoreError::Publish { .. } => Exit::Environment,
        },
        CliError::AfterClearing { source, .. } => exit_for_error(source),
        CliError::Daemon(error) => daemon_exit(error),
        CliError::NoReplica { .. }
        | CliError::Credentials(_)
        | CliError::S3Config(_)
        | CliError::Inventory(_)
        | CliError::Verify(_)
        | CliError::JournalFile { .. }
        | CliError::Create(_)
        | CliError::Clear(_)
        | CliError::Publish(_)
        | CliError::Takeover { .. }
        | CliError::NoMetaDir { .. }
        | CliError::Output(_) => Exit::Environment,
    }
}

/// Runs one parsed command.
///
/// # Errors
/// [`CliError`]; nothing is published unless it is `Ok(Done::Restored)`.
pub async fn run(command: Command) -> Result<Done, CliError> {
    match command {
        Command::Restore(args) => restore_command(args).await,
        Command::Verify(args) => verify_command(args).await,
        Command::Files(args) => files_command(args).await,
        Command::TakeOver(args) => take_over_command(args).await,
        Command::Daemon(args) => match lsr_sidecar::run(&args)? {
            lsr_sidecar::Outcome::Help(text) => Ok(Done::DaemonHelp(text)),
            lsr_sidecar::Outcome::Ran(finished) => Ok(Done::Daemon(finished)),
        },
        Command::ConfigCheck(path) => Ok(Done::ConfigChecked(Box::new(lsr_sidecar::check(&path)))),
        Command::Help => Ok(Done::Help),
        Command::Version => Ok(Done::Version),
    }
}

/// The store behind a replica.
enum Opened {
    Dir(FsStore),
    S3(Remote),
}

fn open(replica: &Replica) -> Result<Opened, CliError> {
    match replica {
        Replica::Dir(path) if path.join("ltx").is_dir() => Ok(Opened::Dir(FsStore::new(path))),
        Replica::Dir(path) => Err(CliError::NoReplica { path: path.clone() }),
        Replica::S3(config) => replica::open_s3((**config).clone()).map(Opened::S3),
    }
}

async fn restore_command(args: Box<RestoreArgs>) -> Result<Done, CliError> {
    let RestoreArgs {
        from,
        output,
        no_expand_env,
        target,
        as_of_inside,
        clear_journal_files,
        integrity_check,
        if_db_not_exists,
        if_replica_exists,
        dry_run,
        json,
        force,
        parallelism,
        txid_and_timestamp,
        timestamp_given,
    } = *args;
    let url_timestamp = timestamp_given && matches!(from, RestoreFrom::Url(_));
    let how = How {
        target,
        as_of_inside,
        clear_journal_files_first: clear_journal_files,
        integrity_check,
        if_replica_exists,
        existing: if force {
            Existing::Any
        } else {
            Existing::Empty
        },
        txid_and_timestamp,
        url_timestamp,
    };
    let (opening, output) = match resolve_from(from, output, no_expand_env, if_db_not_exists)? {
        Resolution::Open(opening, output) => (opening, output),
        Resolution::OutputExists(output, source) => {
            return Ok(Done::Restore(Box::new(RestoreDone {
                output,
                source,
                json,
                finished: Finished::Skipped {
                    skip: Skip::OutputExists,
                    cleared: Vec::new(),
                },
                parallelism,
            })));
        }
    };
    let source = Source::of(&opening.replica);
    let finished = if if_db_not_exists && output_exists(&output, how.existing)? {
        Finished::Skipped {
            skip: Skip::OutputExists,
            cleared: Vec::new(),
        }
    } else {
        restore_or_plan(&opening, &output, how, dry_run).await?
    };
    Ok(Done::Restore(Box::new(RestoreDone {
        output,
        source,
        json,
        finished,
        parallelism,
    })))
}

/// A replica to restore from, with the keys its config gave, if any.
struct Opening {
    replica: Replica,
    keys: Option<S3Credentials>,
}

impl Opening {
    fn of_plan(plan: &ReplicaPlan) -> Self {
        match plan {
            ReplicaPlan::File(dir) => Self {
                replica: Replica::Dir(dir.clone()),
                keys: None,
            },
            ReplicaPlan::S3 {
                config,
                credentials,
            } => Self {
                replica: Replica::S3(config.clone()),
                keys: match credentials {
                    Credentials::FromConfig(keys) => Some(keys.clone()),
                    Credentials::FromEnvironment => None,
                },
            },
        }
    }
}

/// Where a restore reads from and writes to, or Go's early skip of a URL restore whose
/// output exists (`loadFromURL` checks `-if-db-not-exists` before it builds the replica).
enum Resolution {
    Open(Opening, PathBuf),
    OutputExists(PathBuf, Source),
}

/// Go's `loadFromURL` and `loadFromConfig` (`cmd/litestream/restore.go:302-361`), in order.
fn resolve_from(
    from: RestoreFrom,
    output: Option<PathBuf>,
    no_expand_env: bool,
    if_db_not_exists: bool,
) -> Result<Resolution, CliError> {
    let expand = if no_expand_env {
        Expand::Off
    } else {
        Expand::Env
    };
    match from {
        RestoreFrom::Replica(replica) => {
            let output = output.ok_or(UsageError::MissingFlag {
                command: "restore",
                flag: "--output",
            })?;
            Ok(Resolution::Open(
                Opening {
                    replica,
                    keys: None,
                },
                output,
            ))
        }
        RestoreFrom::Url(url) => {
            let output = output.ok_or(UsageError::GoOutputRequired)?;
            if if_db_not_exists && output.symlink_metadata().is_ok() {
                let source = Source::of_url(&url);
                return Ok(Resolution::OutputExists(output, source));
            }
            let loaded = GoConfig::for_one_database(&output.to_string_lossy(), &url, &ProcessEnv)
                .map_err(CliError::Config)?;
            let plan = lsr_config::plan(&loaded.config, &ProcessEnv).map_err(CliError::Plan)?;
            let db = plan.dbs().first().ok_or(CliError::NoDatabase)?;
            Ok(Resolution::Open(Opening::of_plan(db.replica()), output))
        }
        RestoreFrom::Database { path, config } => {
            let loaded = lsr_sidecar::load_config(config.as_deref(), expand)?;
            let want = expand_path(&path, &ProcessEnv).map_err(CliError::DatabasePath)?;
            // Go's `DBConfig` reads only the `path` entries: `dir` entries are not scanned
            // and the other entries are not built.
            let narrowed = loaded
                .config
                .only_database(&want)
                .ok_or_else(|| CliError::NotInConfig { path: want.clone() })?;
            let plan = lsr_config::plan(&narrowed, &ProcessEnv).map_err(CliError::Plan)?;
            let db = plan.dbs().first().ok_or(CliError::NoDatabase)?;
            let output = output.unwrap_or_else(|| db.path().to_path_buf());
            Ok(Resolution::Open(Opening::of_plan(db.replica()), output))
        }
    }
}

/// The restore, or with `dry_run` its plan. A directory with no `ltx/` folder is opened
/// all the same, because Go's file replica lists a missing level directory as empty
/// (`file/replica_client.go:98-100`): the output is checked as for any replica, and the
/// empty-replica ending then names the missing folder.
async fn restore_or_plan(
    opening: &Opening,
    output: &Path,
    how: How,
    dry_run: bool,
) -> Result<Finished, CliError> {
    let (opened, bare) = match &opening.replica {
        Replica::Dir(path) => {
            let bare = (!path.join("ltx").is_dir()).then(|| path.clone());
            (Opened::Dir(FsStore::new(path)), bare)
        }
        Replica::S3(config) => (
            Opened::S3(replica::open_s3_with(
                (**config).clone(),
                opening.keys.clone(),
            )?),
            None,
        ),
    };
    let finished = match (opened, dry_run) {
        (Opened::Dir(store), false) => restore_on(&store, output, how).await,
        (Opened::Dir(store), true) => plan_on(&store, output, how).await,
        (Opened::S3(store), false) => restore_on(&store, output, how).await,
        (Opened::S3(store), true) => plan_on(&store, output, how).await,
    };
    let Some(path) = bare else {
        return finished;
    };
    match finished {
        Ok(Finished::Skipped {
            skip: Skip::EmptyReplica,
            cleared,
        }) => Ok(Finished::Skipped {
            skip: Skip::NoLtxFolder,
            cleared,
        }),
        Ok(other) => Ok(other),
        Err(error) => Err(no_ltx_folder(error, path)),
    }
}

/// An empty-replica refusal from a directory without `ltx/`, named as that.
fn no_ltx_folder(error: CliError, path: PathBuf) -> CliError {
    match error {
        CliError::NoPlan(PlanOutcome::EmptyReplica) => CliError::NoLtxFolder { path },
        CliError::AfterClearing { cleared, source } => CliError::AfterClearing {
            cleared,
            source: Box::new(no_ltx_folder(*source, path)),
        },
        other @ (CliError::NoPlan(_)
        | CliError::Parent(_)
        | CliError::NoBackupsFound
        | CliError::Config(_)
        | CliError::Plan(_)
        | CliError::NoDatabase
        | CliError::DatabasePath(_)
        | CliError::NotInConfig { .. }
        | CliError::Usage(_)
        | CliError::NoReplica { .. }
        | CliError::Credentials(_)
        | CliError::S3Config(_)
        | CliError::Inventory(_)
        | CliError::NoLtxFolder { .. }
        | CliError::Integrity(_)
        | CliError::Publish(_)
        | CliError::Restore(_)
        | CliError::Verify(_)
        | CliError::JournalFile { .. }
        | CliError::Create(_)
        | CliError::Clear(_)
        | CliError::Output(_)
        | CliError::Takeover { .. }
        | CliError::NoMetaDir { .. }
        | CliError::Daemon(_)) => other,
    }
}

/// Whether anything is at the output, as the sink judges it: a path that
/// cannot be inspected is an error, not a reason to skip.
fn output_exists(output: &Path, existing: Existing) -> Result<bool, CliError> {
    if existing != Existing::Refuse && output.symlink_metadata().is_ok() {
        return Ok(true);
    }
    match preflight(output) {
        Ok(_) => Ok(false),
        Err(CreateError::TargetExists(_)) => Ok(true),
        Err(other) => Err(other.into()),
    }
}

/// How a restore runs, apart from where from and where to.
struct How {
    target: RestoreTarget,
    as_of_inside: AsOfInside,
    clear_journal_files_first: bool,
    integrity_check: IntegrityCheck,
    if_replica_exists: bool,
    existing: Existing,
    txid_and_timestamp: bool,
    /// A `-timestamp` with a replica URL: Go's `loadFromURL` then refuses an empty replica
    /// first (`CalcRestoreTarget`, "no backups found"), whatever `-if-replica-exists` says.
    url_timestamp: bool,
}

#[expect(
    clippy::disallowed_methods,
    reason = "Go's duration_ms is wall time the user sees; it decides nothing"
)]
fn started() -> Instant {
    Instant::now()
}

async fn restore_on<S: ObjectStore>(
    store: &S,
    output: &Path,
    how: How,
) -> Result<Finished, CliError> {
    let How {
        target,
        as_of_inside,
        clear_journal_files_first,
        integrity_check,
        if_replica_exists,
        existing,
        txid_and_timestamp,
        url_timestamp,
    } = how;
    refuse_empty_for_timestamp(store, url_timestamp).await?;
    let start = started();
    let cleared = if clear_journal_files_first {
        clear_journal_files(output)?
    } else {
        Vec::new()
    };
    let created = create_parents(output).map_err(CliError::Parent)?;
    let attempt = async {
        // A replicator started on the restored database builds on the replica it came from,
        // through the takeover record: an output with no meta directory is refused before
        // anything is written, never restored without one.
        let meta = meta_dir_of(output)?;
        let sink = FileSink::create_over(output, existing)?.with_integrity_check(integrity_check);
        if txid_and_timestamp {
            return Err(UsageError::GoTxidAndTimestamp.into());
        }
        let options = RestoreOptions { as_of_inside };
        let restored = restore_with(store, target, options, sink).await?;
        lsr_replica::record_takeover(&meta, restored.reached()).map_err(|err| {
            CliError::Takeover {
                meta,
                kind: err.kind(),
            }
        })?;
        Ok::<_, CliError>(restored)
    }
    .await;
    if attempt.is_err() {
        remove_created(&created);
    }
    match attempt {
        Ok(restored) => Ok(Finished::Restored {
            restored,
            cleared,
            integrity_check,
            elapsed: start.elapsed(),
        }),
        Err(CliError::NoPlan(PlanOutcome::EmptyReplica)) if if_replica_exists => {
            Ok(Finished::Skipped {
                skip: Skip::EmptyReplica,
                cleared,
            })
        }
        Err(source) if cleared.is_empty() => Err(source),
        Err(source) => Err(CliError::AfterClearing {
            cleared,
            source: Box::new(source),
        }),
    }
}

/// Go's refusal of an empty replica when a URL restore names a timestamp.
async fn refuse_empty_for_timestamp<S: ObjectStore>(
    store: &S,
    asked: bool,
) -> Result<(), CliError> {
    if asked && Inventory::load(store).await?.newest().is_none() {
        return Err(CliError::NoBackupsFound);
    }
    Ok(())
}

/// Creates the output's missing parent directories, as Go's restore does (mode 0700,
/// `replica.go:728`), and fsyncs the directory that holds each new one, so the output's own
/// directory sync later makes a durable path. Returns what it created, outermost first.
fn create_parents(output: &Path) -> io::Result<Vec<PathBuf>> {
    use std::os::unix::fs::DirBuilderExt as _;
    let mut missing = Vec::new();
    let mut at = output.parent();
    while let Some(dir) = at.filter(|d| !d.as_os_str().is_empty()) {
        if dir.symlink_metadata().is_ok() {
            break;
        }
        missing.push(dir.to_path_buf());
        at = dir.parent();
    }
    missing.reverse();
    let mut created = Vec::new();
    for dir in missing {
        std::fs::DirBuilder::new().mode(0o700).create(&dir)?;
        created.push(dir.clone());
        if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::File::open(parent)?.sync_all()?;
        }
    }
    Ok(created)
}

/// Removes the directories [`create_parents`] made for a restore that failed, innermost
/// first; a directory something else has written into since stays.
fn remove_created(created: &[PathBuf]) {
    for dir in created.iter().rev() {
        if std::fs::remove_dir(dir).is_err() {
            break;
        }
    }
}

/// The restore's checks and plan, without its writes: the output is checked
/// as [`FileSink::create`] would, journal files are listed instead of removed, and
/// the replica is listed and planned as [`restore`] plans its first attempt.
async fn plan_on<S: ObjectStore>(store: &S, output: &Path, how: How) -> Result<Finished, CliError> {
    // Go's dry run plans and prints without looking at the output (`restore.go:119-136`):
    // an output a restore would refuse is not a reason to refuse the plan. The journal files
    // are listed for --clear-journal-files, when they can be.
    refuse_empty_for_timestamp(store, how.url_timestamp).await?;
    if how.txid_and_timestamp {
        return Err(UsageError::GoTxidAndTimestamp.into());
    }
    let journal_files_to_remove = preflight(output).unwrap_or_default();
    // As the restore does: a listing that shows a gap may have raced a writer's rename (a
    // directory read can miss the new file and see a later one), so the plan is made again
    // on a fresh listing, and a gap every listing shows is the answer.
    let mut attempt = 1_u8;
    let (inventory, outcome, created_at_read) = loop {
        let inventory = Inventory::load(store).await?;
        let (outcome, created_at_read) = plan_target(store, &inventory, how.target).await?;
        if !matches!(outcome, PlanOutcome::Gap { .. }) || attempt >= MAX_ATTEMPTS {
            break (inventory, outcome, created_at_read);
        }
        attempt = attempt.saturating_add(1);
    };
    // The real restore reads past the listing before it trusts it for the newest state, or an
    // empty one for any target; a plan printed from a lagging listing would name an older chain
    // as the answer, or skip a replica that exists.
    let empty = inventory.newest().is_none();
    if (empty || !matches!(how.target, RestoreTarget::Txid(_)))
        && let Some(found) = listed_past(store, &inventory)
            .await
            .map_err(CliError::Restore)?
    {
        return Err(CliError::Restore(RestoreError::ListingBehind {
            attempts: 1,
            found,
        }));
    }
    match outcome {
        PlanOutcome::AsOfInsideFile { chain, .. }
            if how.as_of_inside == AsOfInside::TakeEarlier =>
        {
            Ok(Finished::Planned {
                chain,
                created_at_read,
                journal_files_to_remove,
            })
        }
        PlanOutcome::Plan(chain) => Ok(Finished::Planned {
            chain,
            created_at_read,
            journal_files_to_remove,
        }),
        other @ (PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::Gap { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. }) => Err(CliError::NoPlan(other)),
    }
}

/// The planner the restore uses for [target], and the created-at times it read. A store
/// failure is the restore's own error for it, so the two exit alike.
async fn plan_target<S: ObjectStore>(
    store: &S,
    inventory: &Inventory,
    target: RestoreTarget,
) -> Result<(PlanOutcome, u64), CliError> {
    let instant = match target {
        RestoreTarget::Latest => return Ok((plan(inventory, KeyTarget::Latest), 0)),
        RestoreTarget::Txid(txid) => return Ok((plan(inventory, KeyTarget::Txid(txid)), 0)),
        RestoreTarget::AsOf(instant) => instant,
    };
    let mut planner = AsOfPlanner::new(inventory, instant);
    match resolve(store, &mut planner).await {
        Ok(resolved) => Ok((resolved.value, resolved.created_at_read)),
        Err(ResolveError::Store(err)) => Err(CliError::Restore(RestoreError::Store(err))),
        Err(ResolveError::Repeated { key }) => {
            Err(CliError::Restore(RestoreError::CreatedAtRepeated { key }))
        }
    }
}

async fn verify_command(args: VerifyArgs) -> Result<Done, CliError> {
    let report = match open(&args.replica)? {
        Opened::Dir(store) => verify(&store, args.depth).await?,
        Opened::S3(store) => verify(&store, args.depth).await?,
    };
    Ok(Done::Verified {
        depth: args.depth,
        report: Box::new(report),
        json: args.json,
    })
}

/// `lsr take-over`: a takeover record through the replica's newest transaction in the
/// database's meta directory (`lsr_core::TakeoverRecord`).
async fn take_over_command(args: TakeOverArgs) -> Result<Done, CliError> {
    let meta = match args.meta_dir {
        Some(meta) => meta,
        None => meta_dir_of(&args.database)?,
    };
    let newest = match open(&args.replica)? {
        Opened::Dir(store) => Inventory::load(&store).await?.newest(),
        Opened::S3(store) => Inventory::load(&store).await?.newest(),
    };
    if let Some(through) = newest {
        lsr_replica::record_takeover(&meta, through).map_err(|err| CliError::Takeover {
            meta: meta.clone(),
            kind: err.kind(),
        })?;
    }
    Ok(Done::TakenOver {
        through: newest,
        meta,
    })
}

/// A database's default meta directory, Go's `.<name>-litestream` beside it.
fn meta_dir_of(database: &Path) -> Result<PathBuf, CliError> {
    lsr_replica::Config::default()
        .meta_dir_for(database)
        .ok_or_else(|| CliError::NoMetaDir {
            path: database.to_owned(),
        })
}

async fn files_command(args: FilesArgs) -> Result<Done, CliError> {
    match open(&args.replica)? {
        Opened::Dir(store) => Ok(Done::Files {
            inventory: Inventory::load(&store).await?,
            created_at_fallbacks: None,
            json: args.json,
        }),
        Opened::S3(store) => files_on_s3(&store, args.json, args.resolved).await,
    }
}

async fn files_on_s3<T: lsr_store_s3::Transport>(
    store: &Remote<T>,
    json: bool,
    resolved: bool,
) -> Result<Done, CliError> {
    let inventory = Inventory::load(store).await?;
    if !resolved {
        return Ok(Done::Files {
            inventory,
            created_at_fallbacks: None,
            json,
        });
    }
    let inventory = inventory
        .with_created_at(store)
        .await
        .map_err(InventoryError::Store)?;
    Ok(Done::Files {
        inventory,
        created_at_fallbacks: Some(store.inner().created_at_stats().fell_back),
        json,
    })
}

#[cfg(test)]
mod tests;
