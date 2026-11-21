//! Watching a handover between two replicator processes on one [`AppFixture`]: what each process
//! staged and uploaded, read from the meta directory and the replica, never from timing.
//!
//! A process's position is the newest transaction any file covers in the meta directory or
//! the replica, a directory or a MinIO prefix ([`crate::Remote`]) ([`position`]): a process stages a level-0 file in the meta directory before it
//! uploads it, so a file staged by a process killed before its upload still counts, and the
//! next process uploads it. A boot that continues writes its first file at `position + 1`, holding
//! only the pages its transactions wrote; a boot that snapshots writes every page of the
//! database at `position + 1` (Go's `writeLTXFromDB`, Rust's snapshot step), or at transaction 1.
//! [`wait_boot_file`] finds that file and [`crate::Summary::holds_every_page`] tells the two
//! apart. A new level-9 file ([`snapshots`]) is the other kind of snapshot a boot can take.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use lsr_core::Level;
use lsr_ltx::Txid;

use crate::chain::AppFixture;
use crate::error::CompatError;
use crate::replica::{Summary, l0_starting_at, list, newest_txid};
use crate::replicator::{Replicator, wait_until};

/// How long a process may take to stage, upload or boot before the wait is a failure: far
/// above Rust's one-second poll and Go's 100 ms monitor.
pub const PROCESS_STEP: Duration = Duration::from_secs(30);

/// The newest transaction staged or uploaded on `fixture`.
///
/// # Errors
/// [`CompatError`] when a listing fails.
pub fn position(fixture: &AppFixture) -> Result<Option<Txid>, CompatError> {
    Ok(newest_txid(&[fixture.meta.as_path()])?.max(fixture.remote().newest_txid()?))
}

/// The newest transaction the replica holds.
///
/// # Errors
/// [`CompatError`] when the listing fails.
pub fn uploaded(fixture: &AppFixture) -> Result<Option<Txid>, CompatError> {
    fixture.remote().newest_txid()
}

/// The replica's level-9 files, by path.
///
/// # Errors
/// [`CompatError`] when the listing fails.
pub fn snapshots(replica: &Path) -> Result<BTreeSet<String>, CompatError> {
    Ok(list(replica)?
        .files
        .into_iter()
        .filter(|f| f.key.level() == Level::SNAPSHOT)
        .map(|f| f.rel)
        .collect())
}

/// Waits until `process` staged a transaction past `after`; returns the new position.
///
/// # Errors
/// [`CompatError`] when the process exits or stages nothing within [`PROCESS_STEP`].
pub fn wait_staged_past(
    process: &mut Replicator,
    fixture: &AppFixture,
    after: Option<Txid>,
) -> Result<Txid, CompatError> {
    let mut now = None;
    wait_until("a newer staged transaction", PROCESS_STEP, || {
        process.ensure_running()?;
        now = position(fixture)?;
        Ok(now > after)
    })?;
    now.ok_or_else(|| CompatError::Fixture("nothing was staged".to_owned()))
}

/// Waits until the replica holds everything `process` staged.
///
/// # Errors
/// [`CompatError`] when the process exits or does not upload within [`PROCESS_STEP`].
pub fn wait_uploaded(process: &mut Replicator, fixture: &AppFixture) -> Result<Txid, CompatError> {
    let mut now = None;
    wait_until(
        "the replica to hold every staged file",
        PROCESS_STEP,
        || {
            process.ensure_running()?;
            let staged = position(fixture)?;
            now = uploaded(fixture)?;
            Ok(staged.is_some() && now == staged)
        },
    )?;
    now.ok_or_else(|| CompatError::Fixture("nothing was uploaded".to_owned()))
}

/// Waits for the level-0 file a booting `process` writes first, the one that starts at
/// `after + 1`, and reads it whole. The caller commits first, so there is something to write.
///
/// # Errors
/// [`CompatError`] when the process exits, writes no such file within [`PROCESS_STEP`], or
/// the file does not decode.
pub fn wait_boot_file(
    process: &mut Replicator,
    fixture: &AppFixture,
    after: Txid,
) -> Result<Summary, CompatError> {
    let next = after
        .get()
        .checked_add(1)
        .and_then(Txid::new)
        .ok_or_else(|| CompatError::Fixture(format!("no transaction follows {after}")))?;
    let mut found = None;
    let what = format!("the boot's first file at {next}");
    wait_until(&what, PROCESS_STEP, || {
        process.ensure_running()?;
        found = match l0_starting_at(&[fixture.meta.as_path()], next)? {
            Some(staged) => Some(staged),
            None => fixture.remote().l0_starting_at(next)?,
        };
        Ok(found.is_some())
    })?;
    found.ok_or_else(|| CompatError::Timeout(what.clone()))
}
