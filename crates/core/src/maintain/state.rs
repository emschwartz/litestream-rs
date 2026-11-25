//! What the maintenance loop remembers between calls to [`super::due`].
//!
//! The planner reads a listing, not history, so two facts that a listing
//! cannot carry live here, each bounded by the eight compaction levels:
//!
//! - Backoff. A job that failed is not planned again at its level until a
//!   doubling wait has passed ([`backoff_wait`]: at most [`MAX_BACKOFF`],
//!   unless a throttled store asks for longer), so one failing level cannot
//!   starve the snapshot and the deletes behind it. A success at the level
//!   clears it. Go runs each level on its own ticker; this
//!   is the same independence without a thread.
//! - Distrusted outputs. A compaction output or a snapshot the store rejected
//!   (size or checksum mismatch) is deleted at once; when that delete fails the key is
//!   remembered here, [`super::due`] plans its delete before anything else at the
//!   level, and retention holds until it is gone. A listing cannot tell such an
//!   object from a good one, and the inputs it "covers" would be deleted.
//!
//! - The kept restore point. While a task of the actor is degraded, the newest
//!   transaction the replica held when the degradation began is pinned: the
//!   files of its restore chain are not deleted, and the rest of the replica
//!   is retained as usual (`retain`). The actor sets it and clears it.
//!
//! Nothing here can make retention delete more than a fresh state would: a
//! remembered fact only holds or delays. (The retention anchor is not kept
//! for that reason; see `retain`.)
//!
//! None of it survives a restart. That is safe for backoff (one extra
//! attempt). A distrusted key forgotten by a crash between the corrupt publish
//! and its delete is found again by the read-back the next boot owes
//! (`audit`), before anything is planned from the listing.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{Timestamp, Txid};

use super::{CompactJob, DeleteJob, Job, MaintainError, Outcome};
use crate::compact::CompactError;
use crate::fault::{Backoff, Fault, backoff_wait};
use crate::layout::{FileKey, Level};
use crate::store::StoreError;

/// The wait a throttled store asked for, if the job failed on one.
const fn retry_after(err: &MaintainError) -> Option<Duration> {
    let store = match err {
        MaintainError::Compact(err) | MaintainError::OutputDistrusted { source: err, .. } => {
            match err {
                CompactError::Source(err) | CompactError::Dest(err) => err,
                // A local store names no wait.
                CompactError::Local(_)
                | CompactError::InputVanished { .. }
                | CompactError::Input { .. }
                | CompactError::Encode(_)
                | CompactError::Inputs(_)
                | CompactError::SnapshotLevel { .. } => return None,
            }
        }
        MaintainError::Delete(err) => err,
    };
    if let StoreError::Throttled { retry_after, .. } = store {
        *retry_after
    } else {
        None
    }
}

/// The first wait after a failure; each further failure doubles it.
const RETRY_BASE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
struct Retry {
    failures: NonZeroU32,
    until: Timestamp,
}

/// Maintenance memory. Start with [`Default`]; feed every job's outcome to
/// [`Self::record`] (and [`Self::record_snapshot`] for the snapshot).
#[derive(Clone, Debug, Default)]
pub struct MaintainState {
    /// By destination level; [`Level::SNAPSHOT`] is the snapshot's slot.
    retries: BTreeMap<Level, Retry>,
    /// At most one per level above 0: a level's delete runs first.
    distrusted: BTreeMap<Level, FileKey>,
    /// The restore point retention keeps with its chain.
    kept_restore_point: Option<Txid>,
    /// A snapshot built from level-0 files failed: until a restart, the first snapshot is read
    /// from the database, which does not depend on those files.
    seed_refused: bool,
}

impl MaintainState {
    /// Keeps the restore chain of `txid` from retention, or (`None`) lets the
    /// last kept restore point go.
    pub const fn set_kept_restore_point(&mut self, txid: Option<Txid>) {
        self.kept_restore_point = txid;
    }

    /// The restore point retention keeps, if one is pinned.
    #[must_use]
    pub const fn kept_restore_point(&self) -> Option<Txid> {
        self.kept_restore_point
    }

    /// Whether the first snapshot must be read from the database: one built from the
    /// replica's level-0 files failed.
    #[must_use]
    pub(crate) const fn seed_refused(&self) -> bool {
        self.seed_refused
    }

    /// Makes the first snapshot a read of the database.
    pub(crate) const fn refuse_seed(&mut self) {
        self.seed_refused = true;
    }

    /// Notes how `job` ended at `now`.
    pub fn record(&mut self, now: Timestamp, job: &Job, outcome: &Outcome) {
        match job {
            Job::Compact(job) => self.record_compact(now, job, outcome),
            Job::Delete(job) => self.record_delete(now, job, outcome),
            // The actor's loop runs it and reports through `record_snapshot`.
            Job::Snapshot(_) => {}
        }
    }

    /// Notes how a snapshot job ended at `now`: `None` for a success, else
    /// the fault it failed with.
    pub fn record_snapshot(&mut self, now: Timestamp, failed: Option<&Fault>) {
        match failed {
            None => {
                self.retries.remove(&Level::SNAPSHOT);
            }
            Some(fault) => {
                let hint = if let Fault::StoreThrottled { retry_after } = fault {
                    *retry_after
                } else {
                    None
                };
                self.fail(Level::SNAPSHOT, now, hint);
            }
        }
    }

    /// Remembers a snapshot the store rejected and that could not be deleted:
    /// its delete is planned before the next snapshot, and retention holds
    /// until it is gone.
    pub fn distrust_snapshot(&mut self, key: FileKey) {
        // Its one caller is the rejected snapshot whose delete failed.
        self.distrusted.insert(Level::SNAPSHOT, key);
    }

    fn record_compact(&mut self, now: Timestamp, job: &CompactJob, outcome: &Outcome) {
        let level = job.level();
        match outcome {
            Outcome::Compacted { .. } => {
                self.retries.remove(&level);
            }
            // The listing changed under the job; asking again is the answer.
            Outcome::Replan { .. } => {}
            Outcome::Failed(err @ MaintainError::OutputDistrusted { key, .. }) => {
                self.distrusted.insert(level, *key);
                self.fail(level, now, retry_after(err));
            }
            Outcome::Failed(err @ (MaintainError::Compact(_) | MaintainError::Delete(_))) => {
                self.fail(level, now, retry_after(err));
            }
            Outcome::Deleted { .. } => self.fail(level, now, None),
        }
    }

    fn record_delete(&mut self, now: Timestamp, job: &DeleteJob, outcome: &Outcome) {
        let hit: Vec<Level> = self
            .distrusted
            .iter()
            .filter(|(_, key)| job.keys().binary_search(key).is_ok())
            .map(|(level, _)| *level)
            .collect();
        for level in hit {
            match outcome {
                Outcome::Deleted { .. } => {
                    self.distrusted.remove(&level);
                    self.retries.remove(&level);
                }
                Outcome::Failed(err) => self.fail(level, now, retry_after(err)),
                Outcome::Compacted { .. } | Outcome::Replan { .. } => {
                    self.fail(level, now, None);
                }
            }
        }
    }

    /// Counts a failure at `level` and waits the later of its doubling
    /// backoff and the store's `retry_after`: never shorter than either. The
    /// count, not the last wait, drives the doubling.
    fn fail(&mut self, level: Level, now: Timestamp, retry_after: Option<Duration>) {
        let failures = self
            .retries
            .get(&level)
            .map_or(NonZeroU32::MIN, |retry| retry.failures.saturating_add(1));
        let wait = backoff_wait(
            retry_after.map_or(Backoff::Doubling, Backoff::After),
            failures,
            RETRY_BASE,
        );
        let until = now.saturating_add(wait);
        self.retries.insert(level, Retry { failures, until });
    }

    /// Whether any level's failed job is waiting out its backoff at `now`.
    pub(crate) fn backing_off(&self, now: Timestamp) -> bool {
        self.retries
            .keys()
            .any(|level| self.backing_off_until(*level, now).is_some())
    }

    /// When the level may be planned again, if it is backing off at `now`.
    pub(super) fn backing_off_until(&self, level: Level, now: Timestamp) -> Option<Timestamp> {
        self.retries
            .get(&level)
            .map(|retry| retry.until)
            .filter(|until| *until > now)
    }

    /// When a failed `job` may be planned again, if its level keeps a wait:
    /// a compaction's destination, the snapshot's slot, or the level of a
    /// distrusted key the delete names. A retention delete keeps none.
    pub(crate) fn retry_at(&self, job: &Job, now: Timestamp) -> Option<Timestamp> {
        match job {
            Job::Compact(job) => self.backing_off_until(job.level(), now),
            Job::Snapshot(_) => self.backing_off_until(Level::SNAPSHOT, now),
            Job::Delete(job) => job
                .keys()
                .iter()
                .filter(|key| self.distrusted_at(key.level()) == Some(**key))
                .filter_map(|key| self.backing_off_until(key.level(), now))
                .min(),
        }
    }

    /// The level's distrusted output.
    pub(super) fn distrusted_at(&self, level: Level) -> Option<FileKey> {
        self.distrusted.get(&level).copied()
    }

    /// Whether any level holds a rejected output that could not be deleted.
    pub(crate) fn has_distrusted(&self) -> bool {
        !self.distrusted.is_empty()
    }

    /// Forgets distrusted keys that `listed` no longer holds.
    pub(super) fn forget_unlisted(&mut self, listed: impl Fn(&FileKey) -> bool) {
        self.distrusted.retain(|_, key| listed(key));
    }
}
