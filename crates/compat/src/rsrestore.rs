//! Restoring a fixture replica with the Rust executor, the Rust half of the matrix.
//!
//! A fixture is read-only and git resets file times, so a restore works on a staged copy:
//! [`stage`] copies the fixture into a temporary directory and restores the recorded
//! mtimes, which is where [`lsr_store_fs::FsStore`] reads each file's creation time from.
//! [`restore_replica`] then runs the real executor over that copy into a
//! [`lsr_replica::local::FileSink`]. The executor's own failure comes back as a value, because the
//! matrix asserts on it (a gap is a correct answer); only a harness failure is an error.

use std::fs;
use std::path::{Path, PathBuf};

use lsr_core::{
    FileKey, Inventory, PlanOutcome, RestoreError, RestoreOptions, RestoreTarget, Restored,
    plan_reference, restore, restore_with,
};
use lsr_ltx::{Encoder, Event, ReadDecoder};
use lsr_replica::local::FileSink;
use lsr_store_fs::FsStore;

use crate::error::CompatError;
use crate::manifest::Manifest;

/// A staged copy of one fixture; the directory goes away when this is dropped.
#[derive(Debug)]
pub struct Staged {
    dir: tempfile::TempDir,
}

impl Staged {
    /// The fixture's replica directory inside the copy.
    pub fn replica(&self) -> PathBuf {
        Manifest::replica_dir(self.dir.path())
    }

    /// A path inside the staging directory, outside the replica: where a restore writes.
    pub fn output(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Rewrites every replica file with `lsr_ltx::Encoder`: same header, same pages, the
    /// block encoding, then restores each file's recorded mtime. What Go then reads is Rust's
    /// output, not Go's.
    pub fn reencode(&self, manifest: &Manifest) -> Result<(), CompatError> {
        for entry in &manifest.files {
            let path = self.replica().join(&entry.key);
            let old = fs::read(&path).map_err(CompatError::io(&path))?;
            let new = reencode_file(&old)
                .map_err(|why| CompatError::Fixture(format!("{}: {why}", entry.key)))?;
            fs::write(&path, new).map_err(CompatError::io(&path))?;
        }
        manifest.apply_mtimes(self.dir.path())
    }

    /// Everything in the staging directory outside the replica and the manifest, by name.
    /// A restore that published nothing and cleaned up leaves this empty.
    pub fn leftovers(&self) -> Result<Vec<String>, CompatError> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.dir.path()).map_err(CompatError::io(self.dir.path()))? {
            let name = entry
                .map_err(CompatError::io(self.dir.path()))?
                .file_name()
                .to_string_lossy()
                .into_owned();
            if name != "replica" && name != crate::manifest::MANIFEST_FILE {
                out.push(name);
            }
        }
        out.sort();
        Ok(out)
    }
}

/// One LTX file decoded and written again by the strict encoder.
fn reencode_file(old: &[u8]) -> Result<Vec<u8>, String> {
    let mut decoder = ReadDecoder::new(old);
    let mut encoder: Option<Encoder<Vec<u8>>> = None;
    while let Some(event) = decoder.next_event().map_err(|e| format!("decode: {e}"))? {
        match event {
            Event::Header(header) => {
                encoder =
                    Some(Encoder::new(Vec::new(), header).map_err(|e| format!("encode: {e}"))?);
            }
            Event::Page(page) => {
                let (pgno, data) = (page.pgno(), page.data());
                let enc = encoder.as_mut().ok_or("a page before the header")?;
                enc.write_page(pgno, data)
                    .map_err(|e| format!("encode: {e}"))?;
            }
        }
    }
    let verified = decoder.finish().map_err(|e| format!("verify: {e}"))?;
    let (bytes, _trailer) = encoder
        .ok_or("no header")?
        .finish(verified.trailer().post_apply())
        .map_err(|e| format!("encode: {e}"))?;
    Ok(bytes)
}

/// Copies a directory tree (files and directories only).
pub fn copy_tree(from: &Path, to: &Path) -> Result<(), CompatError> {
    fs::create_dir_all(to).map_err(CompatError::io(to))?;
    for entry in fs::read_dir(from).map_err(CompatError::io(from))? {
        let entry = entry.map_err(CompatError::io(from))?;
        let dest = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &dest)?;
        } else {
            fs::copy(entry.path(), &dest).map_err(CompatError::io(&dest))?;
        }
    }
    Ok(())
}

/// Copies a fixture into a temporary directory and sets every file's recorded mtime.
pub fn stage(fixture: &Path, manifest: &Manifest) -> Result<Staged, CompatError> {
    let dir = tempfile::tempdir().map_err(CompatError::io(std::env::temp_dir()))?;
    copy_tree(fixture, dir.path())?;
    manifest.apply_mtimes(dir.path())?;
    Ok(Staged { dir })
}

/// The keys the planner would apply for `target`, oldest first; an error when it has no plan.
pub fn chain_files(replica: &Path, target: RestoreTarget) -> Result<Vec<FileKey>, CompatError> {
    let store = FsStore::new(replica);
    let inventory = lsr_core::conformance::block_on(Inventory::load(&store))
        .map_err(|e| CompatError::Fixture(format!("inventory: {e}")))?;
    let outcome = plan_reference(&inventory, target);
    let PlanOutcome::Plan(chain) = outcome else {
        return Err(CompatError::Fixture(format!(
            "no plan for {target:?}: {outcome:?}"
        )));
    };
    Ok(chain.files().iter().map(|f| f.key).collect())
}

/// Restores `target` from the file replica at `replica` into a new database at `out`.
///
/// # Errors
/// [`CompatError`] when the sink cannot be created; the executor's own refusal is the inner
/// `Err`.
pub fn restore_replica(
    replica: &Path,
    target: RestoreTarget,
    out: &Path,
) -> Result<Result<Restored, RestoreError>, CompatError> {
    let sink = FileSink::create(out)
        .map_err(|e| CompatError::Fixture(format!("creating the restore sink: {e}")))?;
    let store = FsStore::new(replica);
    Ok(lsr_core::conformance::block_on(restore(
        &store, target, sink,
    )))
}

/// [`restore_replica`] with `options` for the cases the executor can either refuse or accept.
///
/// # Errors
/// [`CompatError`] when the sink cannot be created; the executor's own refusal is the inner
/// `Err`.
pub fn restore_replica_with(
    replica: &Path,
    target: RestoreTarget,
    options: RestoreOptions,
    out: &Path,
) -> Result<Result<Restored, RestoreError>, CompatError> {
    let sink = FileSink::create(out)
        .map_err(|e| CompatError::Fixture(format!("creating the restore sink: {e}")))?;
    let store = FsStore::new(replica);
    Ok(lsr_core::conformance::block_on(restore_with(
        &store, target, options, sink,
    )))
}
