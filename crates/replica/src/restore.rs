//! Restore and verify over the store the application also replicates with, as free functions: restoring
//! over a live database must not look as easy as a method on the replicator would make it.
//!
//! Each runs on the caller's thread and blocks until done. The restore writes a scratch file
//! beside the target and publishes it by hard link only after every file verified
//! ([`crate::local::FileSink`]); an existing target or a stale sidecar is refused, never replaced.
//!
//! A restore then records a takeover through the transaction it restored, in the target's
//! default meta directory (`.<name>-litestream` beside it; see `crate::meta`), so a
//! replicator started on the restored database builds on the replica it came from instead of
//! refusing it. An application with its own `Config::meta_dir` calls [`trust_local`] for that
//! directory instead.

use std::io;
use std::path::Path;

use crate::local::{CreateError, FileSink};
use lsr_core::{
    Depth, Report, RestoreOptions, RestoreTarget, Restored, TakeoverRecord, VerifyError,
};
use lsr_ltx::Txid;

use crate::config::Config;
use crate::error::{RestoreError, TrustLocalError};
use crate::exec::block_on;
use crate::meta::write_takeover;
use crate::store::{DynStore, Erased};

/// Restores `target` from `store` into a new database at `to`.
///
/// # Errors
/// [`RestoreError::Destination`] when `to` (or a sidecar beside it) exists or the scratch file
/// cannot be made; [`RestoreError::Restore`] when the replica has no exact answer for the
/// target or a file fails verification; [`RestoreError::NoMetaDir`] when `to` names no file,
/// so has no meta directory for the takeover record. Nothing is published on these errors.
pub fn restore(
    store: &dyn DynStore,
    target: RestoreTarget,
    to: &Path,
) -> Result<Restored, RestoreError> {
    restore_with(store, target, RestoreOptions::default(), to)
}

/// [`restore`] with `options` for the cases it can either refuse or accept, such as an instant
/// inside a compacted file.
///
/// # Errors
/// As [`restore`].
pub fn restore_with(
    store: &dyn DynStore,
    target: RestoreTarget,
    options: RestoreOptions,
    to: &Path,
) -> Result<Restored, RestoreError> {
    // The takeover record goes in the meta directory beside `to`: a path that names no file
    // has none, and is refused before anything is written rather than restored without one.
    let meta = Config::default()
        .meta_dir_for(to)
        .ok_or_else(|| RestoreError::NoMetaDir {
            path: to.to_owned(),
        })?;
    let sink = FileSink::create(to)?;
    let restored = block_on(lsr_core::restore_with(
        &Erased(store),
        target,
        options,
        sink,
    ))?;
    write_takeover(&meta, TakeoverRecord::through(restored.reached()))
        .map_err(|err| RestoreError::TakeoverRecord { kind: err.kind() })?;
    Ok(restored)
}

/// Records a takeover through `through` in the meta directory `meta` (created if missing).
/// Every restore records one in the target's
/// default meta directory itself; a driver whose database has its own `Config::meta_dir`
/// records it there.
///
/// # Errors
/// The first failed file operation.
pub fn record_takeover(meta: &Path, through: Txid) -> io::Result<()> {
    write_takeover(meta, TakeoverRecord::through(through))
}

/// `lsr trust-local`: the caller trusts the local database over the replica. Records that the
/// database whose meta directory is `meta` may replicate over everything `store` holds now,
/// through its newest transaction (a takeover record, `lsr_core::TakeoverRecord`). Returns
/// that transaction, or `None` for an empty replica (nothing is written). A replicator's next
/// start then builds on those files (a snapshot above them) instead of refusing them; a file
/// another writer puts above them later is refused again. When the replica is the right one
/// instead, [`restore`] from it.
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

/// Go's `restore -if-db-not-exists`: restores the newest state into `to` only when nothing is
/// there. `Ok(None)` when a database is already at `to`.
///
/// # Errors
/// As [`restore`], except that an existing target is `Ok(None)`.
pub fn restore_if_missing(
    store: &dyn DynStore,
    to: &Path,
) -> Result<Option<Restored>, RestoreError> {
    match std::fs::symlink_metadata(to) {
        Ok(_) => return Ok(None),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(RestoreError::Destination(CreateError::Inspect {
                path: to.to_owned(),
                source,
            }));
        }
    }
    match restore(store, RestoreTarget::Latest, to) {
        // Another process restored it between the look and the create.
        Err(RestoreError::Destination(CreateError::TargetExists(_))) => Ok(None),
        other => other.map(Some),
    }
}
