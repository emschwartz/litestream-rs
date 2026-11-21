//! The commands: parse result in, a [`Done`] or a [`CliError`] out, and the one
//! place that maps either to an [`Exit`] code.
//!
//! A restore never touches an existing target: [`FileSink::create`] refuses it
//! and any stale sidecar, and the executor publishes by hard link. With
//! `--clear-sidecars` the stale files are removed first and a failure after that
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

use lsr_core::{
    AsOfPlanner, ContiguousChain, Depth, Inventory, InventoryError, KeyTarget, ObjectStore,
    PlanOutcome, Report, ResolveError, RestoreError, RestoreTarget, Restored, Severity,
    VerifyError, plan, resolve, restore, verify,
};
use lsr_local::{
    ClearError, CreateError, FileSink, IntegrityCheck, IntegrityFailure, PublishError,
    clear_sidecars, preflight,
};
use lsr_store_fs::FsStore;
use lsr_store_s3::{ConfigError, CredentialError};
use thiserror::Error;

use crate::args::{Command, FilesArgs, Replica, RestoreArgs, UsageError, VerifyArgs};
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
}

impl Exit {
    /// Every code, for the help text.
    pub const ALL: [Self; 6] = [
        Self::Success,
        Self::Degraded,
        Self::ExecFailed,
        Self::Refused,
        Self::Environment,
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
            Self::ExecFailed => {
                "replicate -exec: the run failed, as Go exits 1 (the child exited non-zero, died by a signal outside a shutdown, was killed or did not start, or a database did not start or drain); the child's own code is in the log"
            }
        }
    }
}

/// The kind of store a replica lives in, as Go names it in its JSON.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReplicaKind {
    /// A directory.
    File,
    /// A bucket prefix.
    S3,
}

impl ReplicaKind {
    /// Go's `ReplicaClient.Type()` word.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::S3 => "s3",
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
        /// Stale sidecars removed first.
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
        /// Exact stamps read to plan an as-of target.
        stamps_read: u64,
        /// Stale sidecars `--clear-sidecars` would remove first.
        sidecars_to_remove: Vec<PathBuf>,
    },
    /// Nothing was restored, by request.
    Skipped {
        /// Why.
        skip: Skip,
        /// Stale sidecars removed before the replica turned out empty.
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
        /// For an S3 replica listed with `--stamps`, how many files had no stored
        /// stamp and show the object's `LastModified` instead (replicas from Go
        /// v0.5.0 have no stamps).
        stamp_fallbacks: Option<u64>,
        /// Whether to print JSON.
        json: bool,
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
    /// A stale SQLite sidecar sits beside the output.
    #[error("{} exists beside the output and SQLite would replay it over the restored database; remove it or pass --clear-sidecars", .path.display())]
    Sidecar {
        /// The sidecar.
        path: PathBuf,
    },
    /// The destination cannot take a restore.
    #[error("{0}")]
    Create(CreateError),
    /// Stale sidecars could not be removed.
    #[error("{0}")]
    Clear(#[from] ClearError),
    /// Something failed after stale sidecars were already removed.
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
            | RestoreError::DatabaseChecksum { .. }
            | RestoreError::StampRepeated { .. }
            | RestoreError::Sink { .. }) => Self::Restore(other),
        }
    }
}

impl From<CreateError> for CliError {
    fn from(err: CreateError) -> Self {
        match err {
            CreateError::Sidecar { path } => Self::Sidecar { path },
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
        Done::Daemon(finished) => match (finished.clean(), finished.ran_exec()) {
            (true, _) => Exit::Success,
            (false, true) => Exit::ExecFailed,
            (false, false) => Exit::Environment,
        },
        Done::ConfigChecked(checked) => config_exit(checked),
        Done::Restore(_)
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
        CliError::Restore(err) => match err {
            RestoreError::NoPlan(_)
            | RestoreError::File { .. }
            | RestoreError::DatabaseChecksum { .. } => Exit::Refused,
            RestoreError::Inventory(_)
            | RestoreError::Store(_)
            | RestoreError::Unstable { .. }
            | RestoreError::StampRepeated { .. }
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
        | CliError::Sidecar { .. }
        | CliError::Create(_)
        | CliError::Clear(_)
        | CliError::Publish(_)
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

async fn restore_command(args: RestoreArgs) -> Result<Done, CliError> {
    let RestoreArgs {
        replica,
        output,
        target,
        clear_sidecars,
        integrity_check,
        if_db_not_exists,
        if_replica_exists,
        dry_run,
        json,
    } = args;
    let source = Source::of(&replica);
    let how = How {
        target,
        clear_sidecars_first: clear_sidecars,
        integrity_check,
        if_replica_exists,
    };
    let finished = if if_db_not_exists && output_exists(&output)? {
        Finished::Skipped {
            skip: Skip::OutputExists,
            cleared: Vec::new(),
        }
    } else {
        restore_or_plan(&replica, &output, how, dry_run).await?
    };
    Ok(Done::Restore(Box::new(RestoreDone {
        output,
        source,
        json,
        finished,
    })))
}

/// The restore, or with `dry_run` its plan. A directory with no `ltx/` folder is opened
/// all the same, because Go's file replica lists a missing level directory as empty
/// (`file/replica_client.go:98-100`): the output is checked as for any replica, and the
/// empty-replica ending then names the missing folder.
async fn restore_or_plan(
    replica: &Replica,
    output: &Path,
    how: How,
    dry_run: bool,
) -> Result<Finished, CliError> {
    let (opened, bare) = match replica {
        Replica::Dir(path) => {
            let bare = (!path.join("ltx").is_dir()).then(|| path.clone());
            (Opened::Dir(FsStore::new(path)), bare)
        }
        Replica::S3(_) => (open(replica)?, None),
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
        | CliError::Sidecar { .. }
        | CliError::Create(_)
        | CliError::Clear(_)
        | CliError::Output(_)
        | CliError::Daemon(_)) => other,
    }
}

/// Whether anything is at the output, as the sink judges it: a path that
/// cannot be inspected is an error, not a reason to skip.
fn output_exists(output: &Path) -> Result<bool, CliError> {
    match preflight(output) {
        Ok(_) => Ok(false),
        Err(CreateError::TargetExists(_)) => Ok(true),
        Err(other) => Err(other.into()),
    }
}

/// How a restore runs, apart from where from and where to.
struct How {
    target: RestoreTarget,
    clear_sidecars_first: bool,
    integrity_check: IntegrityCheck,
    if_replica_exists: bool,
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
        clear_sidecars_first,
        integrity_check,
        if_replica_exists,
    } = how;
    let start = started();
    let cleared = if clear_sidecars_first {
        clear_sidecars(output)?
    } else {
        Vec::new()
    };
    let attempt = async {
        let sink = FileSink::create(output)?.with_integrity_check(integrity_check);
        let restored = restore(store, target, sink).await?;
        Ok::<_, CliError>(restored)
    }
    .await;
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

/// The restore's checks and plan, without its writes: the output is checked
/// as [`FileSink::create`] would, sidecars are listed instead of removed, and
/// the replica is listed and planned as [`restore`] plans its first attempt.
async fn plan_on<S: ObjectStore>(store: &S, output: &Path, how: How) -> Result<Finished, CliError> {
    let sidecars_to_remove = preflight(output)?;
    if let Some(path) = sidecars_to_remove
        .first()
        .filter(|_| !how.clear_sidecars_first)
    {
        return Err(CliError::Sidecar { path: path.clone() });
    }
    let inventory = Inventory::load(store).await?;
    let (outcome, stamps_read) = plan_target(store, &inventory, how.target).await?;
    match outcome {
        PlanOutcome::Plan(chain) => Ok(Finished::Planned {
            chain,
            stamps_read,
            sidecars_to_remove,
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

/// The planner the restore uses for [target], and the stamps it read. A store
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
        Ok(resolved) => Ok((resolved.value, resolved.stamps_read)),
        Err(ResolveError::Store(err)) => Err(CliError::Restore(RestoreError::Store(err))),
        Err(ResolveError::Repeated { key }) => {
            Err(CliError::Restore(RestoreError::StampRepeated { key }))
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

async fn files_command(args: FilesArgs) -> Result<Done, CliError> {
    match open(&args.replica)? {
        Opened::Dir(store) => Ok(Done::Files {
            inventory: Inventory::load(&store).await?,
            stamp_fallbacks: None,
            json: args.json,
        }),
        Opened::S3(store) => files_on_s3(&store, args.json, args.stamps).await,
    }
}

async fn files_on_s3<T: lsr_store_s3::Transport>(
    store: &Remote<T>,
    json: bool,
    stamps: bool,
) -> Result<Done, CliError> {
    let inventory = Inventory::load(store).await?;
    if !stamps {
        return Ok(Done::Files {
            inventory,
            stamp_fallbacks: None,
            json,
        });
    }
    let inventory = inventory
        .stamped(store)
        .await
        .map_err(InventoryError::Store)?;
    Ok(Done::Files {
        inventory,
        stamp_fallbacks: Some(store.inner().stamp_stats().fell_back),
        json,
    })
}

#[cfg(test)]
mod tests;
