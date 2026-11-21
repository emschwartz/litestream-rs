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

use std::io;
use std::path::PathBuf;

use lsr_core::{
    Depth, Inventory, InventoryError, ObjectStore, PlanOutcome, Report, RestoreError,
    RestoreTarget, Restored, Severity, SinkOp, VerifyError, restore, verify,
};
use lsr_local::{
    ClearError, CreateError, FileSink, IntegrityCheck, IntegrityFailure, clear_sidecars,
};
use lsr_store_fs::FsStore;
use lsr_store_s3::ConfigError;
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
}

impl Exit {
    /// Every code, for the help text.
    pub const ALL: [Self; 5] = [
        Self::Success,
        Self::Degraded,
        Self::Refused,
        Self::Environment,
        Self::Usage,
    ];

    /// The process exit status.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::Degraded => 1,
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
            Self::Refused => "verify found it unrestorable, or the restore was refused",
            Self::Environment => "store, destination or I/O failure",
            Self::Usage => "usage error",
        }
    }
}

/// What a command produced.
#[derive(Debug)]
pub enum Done {
    /// A database was published.
    Restored {
        /// The database.
        output: PathBuf,
        /// What the executor did.
        restored: Restored,
        /// Stale sidecars removed first.
        cleared: Vec<PathBuf>,
        /// The SQLite check the database passed before it was published.
        integrity_check: IntegrityCheck,
    },
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
    /// An S3 replica needs credentials and the environment has none.
    #[error(
        "S3 credentials missing: {var} is not set (credentials come from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and optionally AWS_SESSION_TOKEN)"
    )]
    MissingCredentials {
        /// The variable that is unset or empty.
        var: &'static str,
    },
    /// The S3 endpoint cannot address the bucket.
    #[error("S3 replica: {0}")]
    S3Config(ConfigError),
    /// The replica could not be listed completely.
    #[error("listing the replica: {0}")]
    Inventory(#[from] InventoryError),
    /// The replica has no exact answer for the target.
    #[error("no exact restore: {}", render::plan_outcome(.0))]
    NoPlan(PlanOutcome),
    /// SQLite found the restored database invalid; nothing was published.
    #[error("{0}; nothing was published")]
    Integrity(IntegrityFailure),
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
}

impl From<RestoreError> for CliError {
    fn from(err: RestoreError) -> Self {
        match err {
            RestoreError::NoPlan(none) => Self::NoPlan(none.outcome().clone()),
            RestoreError::Sink {
                op: SinkOp::Publish,
                source,
            } => match IntegrityFailure::in_io(&source) {
                Some(failure) => Self::Integrity(failure.clone()),
                None => Self::Restore(RestoreError::Sink {
                    op: SinkOp::Publish,
                    source,
                }),
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
        Done::Restored { .. } | Done::Files { .. } | Done::Help | Done::Version => Exit::Success,
    }
}

/// The exit code of a failed command.
#[must_use]
pub fn exit_for_error(error: &CliError) -> Exit {
    match error {
        CliError::Usage(_) => Exit::Usage,
        CliError::NoPlan(_) | CliError::Integrity(_) => Exit::Refused,
        CliError::Restore(err) => match err {
            RestoreError::NoPlan(_)
            | RestoreError::File { .. }
            | RestoreError::DatabaseChecksum { .. } => Exit::Refused,
            RestoreError::Inventory(_)
            | RestoreError::Store(_)
            | RestoreError::Unstable { .. }
            | RestoreError::StampRepeated { .. }
            | RestoreError::Sink { .. } => Exit::Environment,
        },
        CliError::AfterClearing { source, .. } => exit_for_error(source),
        CliError::NoReplica { .. }
        | CliError::MissingCredentials { .. }
        | CliError::S3Config(_)
        | CliError::Inventory(_)
        | CliError::Verify(_)
        | CliError::Sidecar { .. }
        | CliError::Create(_)
        | CliError::Clear(_)
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
    } = args;
    let how = How {
        target,
        clear_sidecars_first: clear_sidecars,
        integrity_check,
    };
    match open(&replica)? {
        Opened::Dir(store) => restore_on(&store, output, how).await,
        Opened::S3(store) => restore_on(&store, output, how).await,
    }
}

/// How a restore runs, apart from where from and where to.
struct How {
    target: RestoreTarget,
    clear_sidecars_first: bool,
    integrity_check: IntegrityCheck,
}

async fn restore_on<S: ObjectStore>(
    store: &S,
    output: PathBuf,
    how: How,
) -> Result<Done, CliError> {
    let How {
        target,
        clear_sidecars_first,
        integrity_check,
    } = how;
    let cleared = if clear_sidecars_first {
        clear_sidecars(&output)?
    } else {
        Vec::new()
    };
    let attempt = async {
        let sink = FileSink::create(&output)?.with_integrity_check(integrity_check);
        let restored = restore(store, target, sink).await?;
        Ok::<_, CliError>(restored)
    }
    .await;
    match attempt {
        Ok(restored) => Ok(Done::Restored {
            output,
            restored,
            cleared,
            integrity_check,
        }),
        Err(source) if cleared.is_empty() => Err(source),
        Err(source) => Err(CliError::AfterClearing {
            cleared,
            source: Box::new(source),
        }),
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
        stamp_fallbacks: Some(store.inner().fell_back()),
        json,
    })
}

#[cfg(test)]
mod tests;
