//! What the maintenance loop remembers between calls to [`super::due`].
//!
//! The planner reads a listing, not history, so three facts that a listing
//! cannot carry live here, each bounded by the eight compaction levels:
//!
//! - Backoff. A job that failed is not planned again at its level until a
//!   doubling wait has passed ([`retry_wait`], at most [`MAX_BACKOFF`]), so one
//!   failing level cannot starve the snapshot and the deletes behind it. A
//!   success at the level clears it. Go runs each level on its own ticker; this
//!   is the same independence without a thread.
//! - Distrusted outputs. A compaction output the store rejected (size or
//!   checksum mismatch) is deleted at once; when that delete fails the key is
//!   remembered here, [`super::due`] plans its delete before anything else at the
//!   level, and retention holds until it is gone. A listing cannot tell such an
//!   object from a good one, and the inputs it "covers" would be deleted.
//! - The retention anchor's high-water mark, so a clock that steps backwards
//!   cannot make a snapshot retention already passed look young again.
//!
//! None of it survives a restart. That is safe for backoff (one extra attempt)
//! and for the anchor (the listing's own stamps rebuild it). For a distrusted
//! key it is the open case recorded in the plan: a crash between a corrupt
//! publish and its delete forgets the key.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::Timestamp;

use super::{CompactJob, DeleteJob, Job, MaintainError, Outcome};
use crate::fault::retry_wait;
use crate::layout::{FileKey, Level};

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
    anchor: Option<i64>,
    /// By destination level; [`Level::SNAPSHOT`] is the snapshot's slot.
    retries: BTreeMap<Level, Retry>,
    /// At most one per compaction level: a level's delete runs first.
    distrusted: BTreeMap<Level, FileKey>,
}

impl MaintainState {
    /// Notes how `job` ended at `now`.
    pub fn record(&mut self, now: Timestamp, job: &Job, outcome: &Outcome) {
        match job {
            Job::Compact(job) => self.record_compact(now, job, outcome),
            Job::Delete(job) => self.record_delete(now, job, outcome),
            // Task 4's loop runs it and reports through `record_snapshot`.
            Job::Snapshot(_) => {}
        }
    }

    /// Notes how a snapshot job ended at `now`.
    pub fn record_snapshot(&mut self, now: Timestamp, succeeded: bool) {
        if succeeded {
            self.retries.remove(&Level::SNAPSHOT);
        } else {
            self.fail(Level::SNAPSHOT, now);
        }
    }

    fn record_compact(&mut self, now: Timestamp, job: &CompactJob, outcome: &Outcome) {
        let level = job.level();
        match outcome {
            Outcome::Compacted { .. } => {
                self.retries.remove(&level);
            }
            // The listing changed under the job; asking again is the answer.
            Outcome::Replan { .. } => {}
            Outcome::Failed(MaintainError::OutputDistrusted { key, .. }) => {
                self.distrusted.insert(level, *key);
                self.fail(level, now);
            }
            Outcome::Failed(MaintainError::Compact(_) | MaintainError::Delete(_))
            | Outcome::Deleted { .. } => self.fail(level, now),
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
                Outcome::Failed(_) | Outcome::Compacted { .. } | Outcome::Replan { .. } => {
                    self.fail(level, now);
                }
            }
        }
    }

    fn fail(&mut self, level: Level, now: Timestamp) {
        let failures = self
            .retries
            .get(&level)
            .map_or(NonZeroU32::MIN, |retry| retry.failures.saturating_add(1));
        let wait = retry_wait(failures, RETRY_BASE);
        let until = Timestamp::from_unix_millis(
            now.unix_millis()
                .saturating_add(i64::try_from(wait.as_millis()).unwrap_or(i64::MAX)),
        );
        self.retries.insert(level, Retry { failures, until });
    }

    /// When the level may be planned again, if it is backing off at `now`.
    pub(super) fn backing_off_until(&self, level: Level, now: Timestamp) -> Option<Timestamp> {
        self.retries
            .get(&level)
            .map(|retry| retry.until)
            .filter(|until| *until > now)
    }

    /// The level's distrusted output.
    pub(super) fn distrusted_at(&self, level: Level) -> Option<FileKey> {
        self.distrusted.get(&level).copied()
    }

    /// Forgets distrusted keys that `listed` no longer holds.
    pub(super) fn forget_unlisted(&mut self, listed: impl Fn(&FileKey) -> bool) {
        self.distrusted.retain(|_, key| listed(key));
    }

    /// The raised anchor: never lower than any earlier one.
    pub(super) fn raise_anchor(&mut self, candidate: i64) -> i64 {
        let raised = self.anchor.map_or(candidate, |seen| seen.max(candidate));
        self.anchor = Some(raised);
        raised
    }
}
