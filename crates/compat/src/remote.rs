//! Where a [`crate::AppFixture`]'s replica lives: a directory, as the committed fixtures and most live
//! checks use, or a prefix of a local MinIO bucket ([`S3Replica`]). The watchers of the
//! handover module, the restores and the config files go through this, so one test body runs
//! against either, and what a check sees on S3 is read through the S3 store, never a copy.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use lsr_core::{RestoreError, RestoreTarget, Restored};
use lsr_ltx::Txid;

use crate::error::CompatError;
use crate::handover;
use crate::replica::{Summary, l0_starting_at, newest_txid};
use crate::rsrestore::restore_replica;
use crate::s3::S3Replica;

/// A fixture's replica.
#[derive(Debug)]
pub enum Remote {
    /// A file replica directory.
    Dir(PathBuf),
    /// A prefix of a local MinIO bucket.
    S3(Box<S3Replica>),
}

impl Remote {
    /// The newest transaction any replica file covers.
    ///
    /// # Errors
    /// [`CompatError`] when the listing fails.
    pub fn newest_txid(&self) -> Result<Option<Txid>, CompatError> {
        match self {
            Self::Dir(dir) => newest_txid(&[dir.as_path()]),
            Self::S3(s3) => s3.newest_txid(),
        }
    }

    /// The level-9 files, by path (`ltx/9/<name>`).
    ///
    /// # Errors
    /// [`CompatError`] when the listing fails.
    pub fn snapshots(&self) -> Result<BTreeSet<String>, CompatError> {
        match self {
            Self::Dir(dir) => handover::snapshots(dir),
            Self::S3(s3) => s3.snapshots(),
        }
    }

    /// The level-0 file whose transactions start at `min`, read whole.
    ///
    /// # Errors
    /// [`CompatError`] when the listing fails or the file does not decode.
    pub fn l0_starting_at(&self, min: Txid) -> Result<Option<Summary>, CompatError> {
        match self {
            Self::Dir(dir) => l0_starting_at(&[dir.as_path()], min),
            Self::S3(s3) => s3.l0_starting_at(min),
        }
    }

    /// Restores `target` with the Rust executor into a new database at `out`.
    ///
    /// # Errors
    /// [`CompatError`] when the sink cannot be created; the executor's refusal is the inner `Err`.
    pub fn restore(
        &self,
        target: RestoreTarget,
        out: &Path,
    ) -> Result<Result<Restored, RestoreError>, CompatError> {
        match self {
            Self::Dir(dir) => restore_replica(dir, target, out),
            Self::S3(s3) => s3.restore(target, out),
        }
    }

    /// The `replica:` block of a `dbs` entry in the config both programs read.
    pub(crate) fn yaml(&self) -> String {
        match self {
            Self::Dir(dir) => format!(
                "    replica:
      type: file
      path: {}
      sync-interval: 100ms
",
                dir.display()
            ),
            Self::S3(s3) => s3.yaml(),
        }
    }
}
