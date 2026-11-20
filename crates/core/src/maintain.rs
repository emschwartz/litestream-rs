//! The maintenance planner and its executor.
//!
//! [`due`] is a pure function of a listing, an instant and a schedule: it
//! reads nothing else and keeps nothing between calls, so a restarted process
//! or a second process reading the same listing plans the same job. It returns
//! one job; the caller runs it, lists again and asks again.
//!
//! Order: compaction of level 1, then 2, up to the top level, then the
//! snapshot. Lower levels first so a higher level compacts what the one below
//! just produced.
//!
//! A level compacts when its destination's newest file was not created inside
//! the current window (`go_truncate(now, interval)`), and then only the source
//! files above the destination's max, as one strictly contiguous run of at
//! most [`MAX_INPUTS`]. The output carries the last input's timestamp
//! (task 2), so a backlog keeps passing the window test until a run's last
//! input is newer than the window start: 100 source files drain as four jobs
//! in one window and the last one closes it. A hole in the source ends the run
//! and the next job starts after it, so a hole one level down is the same hole
//! here and no file claims transactions nobody wrote (Go merges across it).
//! When the source has nothing above the destination the level is idle: no
//! write, no job, no wake time.
//!
//! The snapshot is written by task 4's loop, which owns the database position;
//! [`SnapshotJob`] only says when. Compaction never deletes its inputs, so a
//! crash at any point leaves a replica that restores: the inputs are still
//! there and the output is either visible or absent. Deletion is a separate
//! [`DeleteJob`], built by retention.

mod config;

use lsr_ltx::{Timestamp, Txid};
use thiserror::Error;

use crate::compact::{CompactError, CompactionInputs, MAX_INPUTS, compact};
use crate::inventory::{Inventory, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};
use crate::store::{ObjectStore, StoreError};

pub use config::{ConfigError, Interval, IntervalError, L0Retention, Maintenance, Setting};

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and time arithmetic on small bounded values"
)]
mod tests;

/// Most keys one `DeleteObjects` call takes on S3 (`s3/replica_client.go:58`).
pub const MAX_DELETE: usize = 1000;

/// Go's zero time, 0001-01-01 UTC, in Unix milliseconds.
const GO_ZERO_UNIX_MILLIS: i128 = -62_135_596_800_000;

/// Why a job cannot be built.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum JobError {
    /// A compaction destination outside levels 1 to 8.
    #[error("{level} is not a compaction destination")]
    LevelOutOfRange {
        /// The level given.
        level: Level,
    },
    /// An input not at the level directly below the destination.
    #[error("{key} is not at level {expected}")]
    InputAtWrongLevel {
        /// The input.
        key: FileKey,
        /// The level every input must be at.
        expected: Level,
    },
    /// A delete job with no keys.
    #[error("no keys to delete")]
    NoKeys,
    /// More keys than one delete call takes.
    #[error("{found} keys, at most {MAX_DELETE}")]
    TooManyKeys {
        /// How many were given.
        found: usize,
    },
    /// Keys not strictly ascending (a repeated key counts).
    #[error("{next} does not come after {after}")]
    KeysNotAscending {
        /// The earlier key.
        after: FileKey,
        /// The key that does not follow it.
        next: FileKey,
    },
}

/// One compaction: inputs at `level - 1` become one file at `level`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CompactJob {
    inputs: CompactionInputs,
    level: Level,
}

impl CompactJob {
    /// Checks the destination and the inputs' level. Also guarantees the
    /// output level is strictly above every input's.
    ///
    /// # Errors
    /// [`JobError::LevelOutOfRange`] unless `level` is 1 to 8;
    /// [`JobError::InputAtWrongLevel`] for the first input not at `level - 1`.
    pub fn new(inputs: CompactionInputs, level: Level) -> Result<Self, JobError> {
        let expected = Level::new(level.get().saturating_sub(1))
            .filter(|_| (1..=8).contains(&level.get()))
            .ok_or(JobError::LevelOutOfRange { level })?;
        if let Some(wrong) = inputs
            .files()
            .iter()
            .find(|info| info.key.level() != expected)
        {
            return Err(JobError::InputAtWrongLevel {
                key: wrong.key,
                expected,
            });
        }
        Ok(Self { inputs, level })
    }

    /// The files to merge, oldest first.
    #[must_use]
    pub const fn inputs(&self) -> &CompactionInputs {
        &self.inputs
    }

    /// The destination level.
    #[must_use]
    pub const fn level(&self) -> Level {
        self.level
    }

    /// The key the output is published under.
    #[must_use]
    pub fn output_key(&self) -> FileKey {
        FileKey::new(self.level, self.inputs.range())
    }
}

/// Write a snapshot when the database position is above `after`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SnapshotJob {
    /// The newest snapshot's last transaction; `None` when there is none.
    pub after: Option<Txid>,
    /// The newest position the planner saw. Task 4 snapshots its own
    /// position, which is at least this.
    pub through: Txid,
}

/// One delete call's keys.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DeleteJob {
    keys: Vec<FileKey>,
}

impl DeleteJob {
    /// Checks the keys.
    ///
    /// # Errors
    /// [`JobError::NoKeys`], [`JobError::TooManyKeys`] above [`MAX_DELETE`],
    /// or [`JobError::KeysNotAscending`].
    pub fn new(keys: Vec<FileKey>) -> Result<Self, JobError> {
        if keys.is_empty() {
            return Err(JobError::NoKeys);
        }
        if keys.len() > MAX_DELETE {
            return Err(JobError::TooManyKeys { found: keys.len() });
        }
        for pair in keys.windows(2) {
            if let [after, next] = pair
                && after >= next
            {
                return Err(JobError::KeysNotAscending {
                    after: *after,
                    next: *next,
                });
            }
        }
        Ok(Self { keys })
    }

    /// The keys, ascending.
    #[must_use]
    pub fn keys(&self) -> &[FileKey] {
        &self.keys
    }
}

/// One unit of maintenance work.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Job {
    /// Merge a run of files into the next level.
    Compact(CompactJob),
    /// Write a snapshot (task 4's job to run).
    Snapshot(SnapshotJob),
    /// Delete files.
    Delete(DeleteJob),
}

/// What to do now.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Due {
    /// Run this job, list again, ask again.
    Run(Job),
    /// Nothing is ready.
    Wait {
        /// The earliest instant something becomes due; `None` when nothing
        /// does without a new write.
        until: Option<Timestamp>,
    },
}

/// Go's `Time.Truncate` on milliseconds: the start of the interval-sized
/// window holding `now`, counted from Go's zero time (0001-01-01 UTC), not the
/// Unix epoch (`compaction_level.go:32-40`). The two agree for every interval
/// dividing a day. Rounds toward the past for every input and saturates at the
/// smallest instant.
#[must_use]
pub fn go_truncate(now: Timestamp, interval: Interval) -> Timestamp {
    let now = i128::from(now.unix_millis());
    let since_zero = now.saturating_sub(GO_ZERO_UNIX_MILLIS);
    // The interval is at least 1 by construction.
    let into_window = since_zero.rem_euclid(i128::from(interval.millis()));
    let start = now.saturating_sub(into_window);
    Timestamp::from_unix_millis(i64::try_from(start).unwrap_or(i64::MIN))
}

fn next_window(now: Timestamp, interval: Interval) -> (Timestamp, Timestamp) {
    let start = go_truncate(now, interval);
    let step = i64::try_from(interval.millis()).unwrap_or(i64::MAX);
    let next = Timestamp::from_unix_millis(start.unix_millis().saturating_add(step));
    (start, next)
}

/// What one level, or the snapshot, wants.
enum Step {
    Run(Job),
    Wait(Timestamp),
    Idle,
}

/// The next job, or when to ask again. See the module docs for the rules.
#[must_use]
pub fn due(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
) -> Due {
    let mut until: Option<Timestamp> = None;
    let steps = Level::ALL
        .iter()
        .skip(1)
        .take(usize::from(config.top().get()))
        .map(|level| plan_level(inventory, now, config, *level))
        .chain(std::iter::once_with(|| {
            plan_snapshot(inventory, now, config, published)
        }));
    for step in steps {
        match step {
            Step::Run(job) => return Due::Run(job),
            Step::Wait(at) => until = Some(until.map_or(at, |soonest| soonest.min(at))),
            Step::Idle => {}
        }
    }
    Due::Wait { until }
}

fn plan_level(inventory: &Inventory, now: Timestamp, config: &Maintenance, dst: Level) -> Step {
    let (Some(src_level), Some(interval)) = (
        Level::new(dst.get().saturating_sub(1)),
        config.interval(dst),
    ) else {
        return Step::Idle;
    };
    let dst_files = inventory.level(dst);
    let dst_last = dst_files.last();
    let dst_max = dst_last.map(|info| info.key.range().max());
    let mut above = inventory
        .level(src_level)
        .iter()
        .filter(|info| dst_max.is_none_or(|max| info.key.range().min() > max))
        .peekable();
    if above.peek().is_none() {
        return Step::Idle;
    }
    let (start, next) = next_window(now, interval);
    if dst_last.is_some_and(|info| info.created_at > start) {
        return Step::Wait(next);
    }
    let run = contiguous_run(above);
    match CompactionInputs::new(run)
        .ok()
        .and_then(|inputs| CompactJob::new(inputs, dst).ok())
    {
        Some(job) => Step::Run(Job::Compact(job)),
        // Unreachable for a run built above; a property test pins it.
        None => Step::Idle,
    }
}

/// The longest strictly contiguous prefix, at most [`MAX_INPUTS`] files.
fn contiguous_run<'a>(files: impl Iterator<Item = &'a FileInfo>) -> Vec<FileInfo> {
    let mut run: Vec<FileInfo> = Vec::new();
    for info in files {
        if run.len() == MAX_INPUTS {
            break;
        }
        if let Some(prev) = run.last()
            && prev.key.range().max().next() != Some(info.key.range().min())
        {
            break;
        }
        run.push(*info);
    }
    run
}

fn plan_snapshot(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
) -> Step {
    let Some(pos) = inventory.newest().max(published) else {
        return Step::Idle;
    };
    let Some(snap) = inventory
        .level(Level::SNAPSHOT)
        .iter()
        .rev()
        .find(|info| is_snapshot_start(info))
    else {
        return Step::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: pos,
        }));
    };
    let after = snap.key.range().max();
    if pos <= after {
        return Step::Idle;
    }
    let (start, next) = next_window(now, config.snapshot_interval());
    if snap.created_at > start {
        return Step::Wait(next);
    }
    Step::Run(Job::Snapshot(SnapshotJob {
        after: Some(after),
        through: pos,
    }))
}

/// Why a job did not finish.
#[derive(Debug, Error)]
pub enum MaintainError {
    /// The compaction failed. After a failed destination the output may be
    /// visible; the next [`due`] on a fresh listing settles it.
    #[error("compaction: {0}")]
    Compact(#[source] CompactError),
    /// The delete call failed.
    #[error("delete: {0}")]
    Delete(#[source] StoreError),
}

/// How a job ended.
#[derive(Debug)]
pub enum Outcome {
    /// The output is published.
    Compacted {
        /// The published file.
        info: FileInfo,
        /// Bytes read from the source.
        bytes_read: u64,
    },
    /// The keys are gone (or already were).
    Deleted {
        /// How many keys the call named.
        count: usize,
    },
    /// An input is gone from the replica: list again and plan again.
    Replan {
        /// The missing input.
        key: FileKey,
    },
    /// The job failed; nothing was retried here.
    Failed(MaintainError),
}

/// Runs one compaction. Level-1 jobs read `local` first and the replica once
/// when an input is missing there (the local copy may be gone after a
/// restart); every other level reads the replica. The output always goes to
/// the replica. Inputs are never deleted.
pub async fn run_compact<L: ObjectStore, R: ObjectStore>(
    job: &CompactJob,
    local: &L,
    replica: &R,
) -> Outcome {
    let result = if job.level().get() == 1 {
        match compact(local, replica, job.inputs(), job.level()).await {
            Err(CompactError::InputVanished { .. }) => {
                compact(replica, replica, job.inputs(), job.level()).await
            }
            other => other,
        }
    } else {
        compact(replica, replica, job.inputs(), job.level()).await
    };
    match result {
        Ok(done) => Outcome::Compacted {
            info: done.info(),
            bytes_read: done.bytes_read(),
        },
        Err(CompactError::InputVanished { key }) => Outcome::Replan { key },
        Err(
            err @ (CompactError::Source(_)
            | CompactError::Dest(_)
            | CompactError::Input { .. }
            | CompactError::Encode(_)
            | CompactError::SnapshotLevel { .. }
            | CompactError::Inputs(_)),
        ) => Outcome::Failed(MaintainError::Compact(err)),
    }
}

/// Runs one delete call. A key already gone is not an error.
pub async fn run_delete<R: ObjectStore>(job: &DeleteJob, replica: &R) -> Outcome {
    match replica.delete(job.keys()).await {
        Ok(()) => Outcome::Deleted {
            count: job.keys().len(),
        },
        Err(err) => Outcome::Failed(MaintainError::Delete(err)),
    }
}
