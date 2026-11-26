//! Restore and verify over the store the application also replicates with, as free functions: restoring
//! over a live database must not look as easy as a method on the replicator would make it.
//!
//! Each runs on the caller's thread and blocks until done. The restore writes a scratch file
//! beside the target and publishes it by hard link only after every file verified
//! ([`crate::local::FileSink`]); an existing target or a stale sidecar is refused, never replaced.
//!
//! A restore then records a trust record through the transaction it restored, in the target's
//! meta directory as the replicator's [`Config`] names it (`Restore::config`; the default
//! is `.<name>-litestream` beside the target, see `crate::meta`), so a replicator started on
//! the restored database builds on the replica it came from instead of refusing it.

use std::io;
use std::path::Path;

use crate::local::{CreateError, FileSink, IntegrityCheck};
use lsr_core::{
    Depth, Report, RestoreOptions, RestoreTarget, Restored, TakeoverRecord, VerifyError,
};
use lsr_ltx::Txid;

use crate::config::Config;
use crate::error::{RestoreError, TrustLocalError};
use crate::exec::block_on;
use crate::meta::write_takeover;
use crate::store::{DynStore, Erased};

/// What [`restore`] restores and how. Each field's default is the one a restore without it
/// gets: `Restore { target, ..Restore::default() }`.
#[derive(Clone, Debug, Default)]
pub struct Restore {
    /// The state to restore; the newest the replica holds by default.
    pub target: RestoreTarget,
    /// The options for the cases a restore can either refuse or accept, such as an instant
    /// inside a compacted file.
    pub options: RestoreOptions,
    /// The SQLite check run on the restored file before it is published (Go's
    /// `-integrity-check`); off by default, as in Go.
    pub integrity: IntegrityCheck,
    /// The configuration the database will be replicated with: the trust record goes in its
    /// meta directory (`Config::meta_dir`), where the replicator's start reads it.
    pub config: Config,
}

/// Restores into a new database at `to` what `how` asks for from `store`, then records the
/// trust record that lets a replicator build on the replica it came from.
///
/// # Errors
/// [`RestoreError::Destination`] when `to` (or a sidecar beside it) exists or the scratch file
/// cannot be made; [`RestoreError::Restore`] when the replica has no exact answer or a read,
/// a file or the publish fails; [`RestoreError::NoMetaDir`]
/// when `to` names no file and no meta directory was configured. Nothing is published on these
/// errors, except a publish failure that says it left the database visible, and
/// [`RestoreError::TrustRecord`], which carries the restore that succeeded.
pub fn restore(store: &dyn DynStore, to: &Path, how: &Restore) -> Result<Restored, RestoreError> {
    // A path that names no file has no default meta directory, and is refused before
    // anything is written rather than restored without a trust record.
    let meta = how
        .config
        .meta_dir_for(to)
        .ok_or_else(|| RestoreError::NoMetaDir {
            path: to.to_owned(),
        })?;
    let sink = FileSink::create(to)?.with_integrity_check(how.integrity);
    let restored = block_on(lsr_core::restore_with(
        &Erased(store),
        how.target,
        how.options,
        sink,
    ))?;
    write_takeover(&meta, TakeoverRecord::through(restored.reached())).map_err(|err| {
        RestoreError::TrustRecord {
            restored,
            kind: err.kind(),
        }
    })?;
    Ok(restored)
}

/// Records a trust record through `through` in the meta directory `meta` (created if
/// missing): the database there may replicate over the replica's files through `through`.
/// Every restore records one itself; this is for a caller retrying a
/// [`RestoreError::TrustRecord`] with `restored.reached()`, which trusts exactly what was
/// restored, where [`trust_local`] would trust the replica's newest transaction.
///
/// # Errors
/// The first failed file operation.
pub fn record_trust(meta: &Path, through: Txid) -> io::Result<()> {
    write_takeover(meta, TakeoverRecord::through(through))
}

/// `lsr trust-local`: the caller trusts the local database over the replica. Records that the
/// database whose meta directory is `meta` may replicate over everything `store` holds now,
/// through its newest transaction (a trust record). Returns that transaction, or `None` for
/// an empty replica (nothing is written). A replicator's next start then builds on those files
/// (a snapshot above them) instead of refusing them; a file another writer puts above them
/// later is refused again. When the replica is the right one instead, [`restore`] from it.
///
/// # Errors
/// [`TrustLocalError`]: the replica cannot be listed, or the record cannot be written.
pub fn trust_local(store: &dyn DynStore, meta: &Path) -> Result<Option<Txid>, TrustLocalError> {
    let inventory = block_on(lsr_core::Inventory::load(&Erased(store)))?;
    let Some(through) = inventory.newest() else {
        return Ok(None);
    };
    write_takeover(meta, TakeoverRecord::through(through))
        .map_err(|err| TrustLocalError::Write { kind: err.kind() })?;
    Ok(Some(through))
}

/// Checks every file of the replica, to `depth`, without restoring.
///
/// # Errors
/// [`VerifyError`] when the walk itself stops (the listing fails); damage is in the report.
pub fn verify(store: &dyn DynStore, depth: Depth) -> Result<Report, VerifyError> {
    block_on(lsr_core::verify(&Erased(store), depth))
}

/// What [`restore_if_missing`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum IfMissing {
    /// Nothing was at the path, and the replica's newest state was restored there.
    Restored(Restored),
    /// A database is already at the path; nothing was done.
    Exists,
    /// Nothing was at the path and the replica holds nothing: a first start. Nothing was
    /// written. Start the replicator with `StartOptions::create_if_missing`,
    /// which creates the empty database as SQLite would, or create it before the start, and
    /// replicate from scratch (Go's `replicate -restore-if-db-not-exists` starts fresh the
    /// same way).
    EmptyReplica,
}

/// Go's `restore -if-db-not-exists`: restores the newest state into `to` only when nothing is
/// there, recording the trust record in `config`'s meta directory.
///
/// # Errors
/// As [`restore`], except that an existing target is [`IfMissing::Exists`] and an empty
/// replica is [`IfMissing::EmptyReplica`].
pub fn restore_if_missing(
    store: &dyn DynStore,
    to: &Path,
    config: &Config,
) -> Result<IfMissing, RestoreError> {
    match std::fs::symlink_metadata(to) {
        Ok(_) => return Ok(IfMissing::Exists),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(RestoreError::Destination(CreateError::Inspect {
                path: to.to_owned(),
                source,
            }));
        }
    }
    let how = Restore {
        config: config.clone(),
        ..Restore::default()
    };
    match restore(store, to, &how) {
        Ok(restored) => Ok(IfMissing::Restored(restored)),
        // Another process restored it between the look and the create.
        Err(RestoreError::Destination(CreateError::TargetExists(_))) => Ok(IfMissing::Exists),
        Err(RestoreError::Restore(lsr_core::RestoreError::NoPlan(none)))
            if *none.outcome() == lsr_core::PlanOutcome::EmptyReplica =>
        {
            Ok(IfMissing::EmptyReplica)
        }
        Err(other) => Err(other),
    }
}
