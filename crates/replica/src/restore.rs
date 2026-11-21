//! Restore and verify over the store the host also replicates with, as free functions: restoring
//! over a live database must not look as easy as a method on the replicator would make it.
//!
//! Each runs on the caller's thread and blocks until done. The restore writes a scratch file
//! beside the target and publishes it by hard link only after every file verified
//! (`lsr_local::FileSink`); an existing target or a stale sidecar is refused, never replaced.

use std::io;
use std::path::Path;

use lsr_core::{Depth, Report, RestoreOptions, RestoreTarget, Restored, VerifyError};
use lsr_local::{CreateError, FileSink};

use crate::error::RestoreError;
use crate::exec::block_on;
use crate::store::{DynStore, Erased};

/// Restores `target` from `store` into a new database at `to`.
///
/// # Errors
/// [`RestoreError::Destination`] when `to` (or a sidecar beside it) exists or the scratch file
/// cannot be made; [`RestoreError::Restore`] when the replica has no exact answer for the
/// target or a file fails verification. Nothing is published on any error.
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
    let sink = FileSink::create(to)?;
    Ok(block_on(lsr_core::restore_with(
        &Erased(store),
        target,
        options,
        sink,
    ))?)
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
