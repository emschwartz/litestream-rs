//! The restore-and-compare helpers the live handover, S3 and sidecar tests share: restore a
//! fixture's replica with Rust or with Go into a fresh path and read back the pages.

#![expect(
    clippy::expect_used,
    reason = "test helpers: a failure here is a failed test, reported with its label"
)]

use std::fs;
use std::path::{Path, PathBuf};

use lsr_core::RestoreTarget;

use crate::chain::AppFixture;
use crate::golive::{GoRestore, restore_with_go};
use crate::manifest::Target;
use crate::pages::{hash_pages, integrity_check};

/// The page hashes of the database at `path`, after SQLite's integrity check passes.
///
/// # Panics
/// When the check or the hashing fails: a test helper, a failure there is a failed test.
#[must_use]
pub fn pages_of(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

/// `name` in the fixture's directory, with any earlier database, WAL and shared-memory file
/// of that name removed.
#[must_use]
pub fn fresh(fixture: &AppFixture, name: &str) -> PathBuf {
    let out = fixture.dir().join(name);
    for suffix in ["", "-wal", "-shm"] {
        drop(fs::remove_file(format!("{}{suffix}", out.display())));
    }
    out
}

/// Rust restores `target` from the fixture's replica, a directory or a MinIO prefix, and the
/// pages of the result come back.
///
/// # Panics
/// When the restore is refused.
#[must_use]
pub fn rust_restore(fixture: &AppFixture, target: RestoreTarget, name: &str) -> Vec<String> {
    let out = fresh(fixture, name);
    let restored = fixture.remote().restore(target, &out).expect("sink");
    assert!(
        restored.is_ok(),
        "{name}: Rust cannot restore: {restored:?}"
    );
    pages_of(&out)
}

/// Go's `restore` (the pinned `bin`, reading `config`) of `target`, and the pages of the result.
///
/// # Panics
/// When Go does not run or does not restore.
#[must_use]
pub fn go_restore(
    bin: &Path,
    config: &Path,
    fixture: &AppFixture,
    target: Target,
    name: &str,
) -> Vec<String> {
    let out = fresh(fixture, name);
    let result = restore_with_go(bin, config, &fixture.db, &out, target).expect("go runs");
    assert_eq!(result, GoRestore::Restored, "{name}: Go restore");
    pages_of(&out)
}
