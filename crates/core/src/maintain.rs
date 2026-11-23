//! The maintenance planner and its executor.
//!
//! [`due`] is a function of a listing, an instant, a schedule and a small
//! [`MaintainState`] (backoff and distrusted outputs; see `state`). A fresh
//! state plans from the listing alone,
//! so a restarted process reading the same listing plans the same job. It
//! returns one job; the caller runs it, records the outcome, lists again and
//! asks again.
//!
//! Created-at times are read lazily. The schedule asks for the destination level's last
//! file only when the source has files above it, and for the newest snapshot
//! only when one is otherwise due; retention's reads are listed in its module
//! doc. Each ask is a [`Resolve::Need`], so a listing that carries no exact
//! created-at times costs a request per ask and the job sequence is the one a listing that
//! carries every created-at time gives.
//!
//! A level whose last job failed is skipped until its retry time, so one
//! failing level never keeps the snapshot or the deletes from being planned.
//!
//! Order: compaction of level 1, then 2, up to the top level, then the
//! snapshot. Lower levels first so a higher level compacts what the one below
//! just produced. A level holding a distrusted output (one the store rejected
//! and that could not be removed) plans that key's delete before anything else;
//! so does the snapshot, whose rejected file would otherwise stay listed as
//! the base of every restore between it and the next one.
//!
//! A level compacts when its destination's newest file was not created inside
//! the current window (`go_truncate(now, interval)`), and then only the source
//! files above the destination's max, as one strictly contiguous run of at
//! most [`MAX_INPUTS`]. The output carries the last input's timestamp
//! (so a compaction never makes its output look older than its inputs), so a backlog keeps passing the window test until a run's last
//! input is newer than the window start: 100 source files drain as four jobs
//! in one window and the last one closes it. A hole in the source ends the run
//! and the next job starts after it, so a hole one level down is the same hole
//! here and no file claims transactions nobody wrote (Go merges across it).
//! When the source has nothing above the destination the level is idle: no
//! write, no job, no wake time.
//!
//! The first listing after a boot is read back before anything is planned
//! from it (`audit`): the newest file of each level above 0 must verify, so
//! an output that a crashed process published with wrong bytes is neither an
//! input nor cover.
//!
//! Deletes come last: when no compaction or snapshot is ready, `retain` plans
//! one delete job against a listing that already holds the newest coverage, or
//! says why it refuses (`Due::Wait { held }`). With retention switched off
//! ([`RetentionMode::Off`]) that step is skipped: it reads no created-at time, plans no
//! delete and reports no hold, and the wait is the schedule's alone.
//!
//! The snapshot is written by the actor's loop, which owns the database position;
//! [`SnapshotJob`] only says when. Compaction never deletes its inputs, so a
//! crash at any point leaves a replica that restores: the inputs are still
//! there and the output is either visible or absent. Deletion is a separate
//! [`DeleteJob`], built by retention.

mod audit;
mod config;
mod retain;
mod state;

use lsr_ltx::{TRAILER_LEN, Timestamp, Txid};
use thiserror::Error;

use crate::compact::{CompactError, Compacted, CompactionInputs, MAX_INPUTS, compact_in};
use crate::created_at::{Need, Resolve, ResolvedCreatedAt};
use crate::fault::role::Role;
use crate::inventory::{Inventory, is_snapshot_start};
use crate::layout::{FileInfo, FileKey, Level};
use crate::plan::PlanOutcome;
use crate::store::{CorruptCause, ObjectStore, ReadLen, StoreError};
use retain::{Retention, retention};

pub use config::{
    ConfigError, Interval, IntervalError, L0Retention, Maintenance, RetentionMode, Setting,
};
pub use state::MaintainState;

#[cfg(test)]
pub(crate) use audit::restores_without;
pub(crate) use audit::{ReadBack, read_back};

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "test fixtures do index and time arithmetic on small bounded values"
)]
mod tests;

/// Most keys one `DeleteObjects` call takes on S3 (`s3/replica_client.go:58`).
pub const MAX_DELETE: usize = 1000;

// A job must fit one DeleteObjects call, or a retried job would not converge on one request.
const _: () = assert!(MAX_DELETE >= 1 && MAX_DELETE <= 1000);

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
    /// The newest position the planner saw. The actor snapshots its own
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
    /// Write a snapshot (the actor's loop runs it).
    Snapshot(SnapshotJob),
    /// Delete files.
    Delete(DeleteJob),
}

/// Why deletions are refused right now. Compaction and snapshots still run.
/// Every variant is a listing that cannot safely be read as a history; the
/// rules and the gate's order are in the `retain` module docs.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum RetentionHold {
    /// A name in the listing could not be parsed; it may be a file that
    /// covers something.
    #[error("{count} unparsable names at {level}")]
    UnrecognizedNames {
        /// The level.
        level: Level,
        /// How many.
        count: usize,
    },
    /// A level-9 file that does not start at transaction 1.
    #[error("{key} is a snapshot that does not start at transaction 1")]
    MalformedSnapshot {
        /// The file.
        key: FileKey,
    },
    /// The listing misses a file this process knows it wrote: stale or partial.
    #[error("listing ends at {listed:?} but {published} was published")]
    ListingBehind {
        /// The newest transaction listed.
        listed: Option<Txid>,
        /// The newest transaction this process committed.
        published: Txid,
    },
    /// A transaction after the newest snapshot that no file covers.
    #[error("no file covers the transactions after {after}, up to {next:?}")]
    Hole {
        /// The last covered transaction before the hole.
        after: Txid,
        /// The first covered transaction after it; `None` when the tail is missing.
        next: Option<Txid>,
    },
    /// The newest state cannot be planned.
    #[error("the newest state does not plan: {outcome:?}")]
    LatestUnplannable {
        /// Why; never a plan.
        outcome: PlanOutcome,
    },
    /// A compaction output the store rejected and that could not be deleted:
    /// until it is gone the listing holds a file that may not decode.
    #[error("{key} was rejected by the store and is still listed")]
    OutputDistrusted {
        /// The output.
        key: FileKey,
    },
    /// A snapshot or level-0 file with no usable created-at time.
    #[error("{key} has no creation time")]
    NoCreatedAt {
        /// The file.
        key: FileKey,
    },
    /// Snapshots whose created-at times run backwards, so age means nothing. A copied
    /// replica whose stored times were reset lands here: the stored times no
    /// longer follow the headers.
    #[error(
        "{later} is recorded as created before {earlier}: the stored times no longer follow the headers"
    )]
    SnapshotCreatedAtBackwards {
        /// The snapshot with the lower transactions.
        earlier: FileKey,
        /// The snapshot with the higher transactions and the older created-at time.
        later: FileKey,
    },
    /// The restore point pinned while a task is degraded does not plan, on
    /// the listing or without the planned delete.
    #[error("the pinned restore point {restore_point} does not plan: {outcome:?}")]
    RestorePointUnplannable {
        /// The pinned transaction.
        restore_point: Txid,
        /// What the planner said; never a plan that reaches the kept restore point.
        outcome: PlanOutcome,
    },
    /// The planned delete would have broken the newest restore. A bug in the
    /// rules, caught before any key was named.
    #[error("the planned delete would break the newest restore: {outcome:?}")]
    SelfCheck {
        /// What the planner said without the keys; never a plan.
        outcome: PlanOutcome,
    },
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
        /// Why deletions are refused right now, if they are.
        held: Option<RetentionHold>,
    },
}

/// Go's `Time.Truncate` on milliseconds: the start of the interval-sized
/// window holding `now`, counted from Go's zero time (0001-01-01 UTC), not the
/// Unix epoch (`compaction_level.go:32-40`). The two agree for every interval
/// dividing a day. Rounds toward the past for every input and saturates at the
/// smallest instant.
#[must_use]
pub(crate) fn go_truncate(now: Timestamp, interval: Interval) -> Timestamp {
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

/// When maintenance next lists the replica after an upload at `now`: the
/// earliest of the next window of every compaction level and of the snapshot,
/// and, while retention deletes level 0, `now` plus the level-0 retention
/// check interval. Always after `now`.
///
/// An upload gives a level something to compact, but no level compacts twice
/// inside one window, so listing before the next window opens can only find
/// level-0 files to delete; Go looks for those on its check interval
/// (`store.go:658-680`) and for compactions on each level's window
/// (`store.go:591-656`). Listing at once after every upload instead lists the
/// whole replica once a second for a database that writes once a second.
#[must_use]
pub(crate) fn listing_after_upload(now: Timestamp, config: &Maintenance) -> Timestamp {
    let check = match (config.retention(), config.l0_retention()) {
        (RetentionMode::Enforce, L0Retention::After(_)) => {
            let step = i64::try_from(config.l0_check_interval().millis()).unwrap_or(i64::MAX);
            Some(Timestamp::from_unix_millis(
                now.unix_millis().saturating_add(step),
            ))
        }
        (RetentionMode::Enforce, L0Retention::Keep)
        | (RetentionMode::Off, L0Retention::Keep | L0Retention::After(_)) => None,
    };
    let levels = Level::ALL
        .iter()
        .filter_map(|level| config.interval(*level));
    levels
        .chain([config.snapshot_interval()])
        .map(|interval| next_window(now, interval).1)
        .chain(check)
        .min()
        .unwrap_or(now)
}

/// What one level, or the snapshot, wants.
enum Step {
    Run(Job),
    Wait(Timestamp),
    Idle,
}

/// The next job, or when to ask again, or the one created-at time needed first. See the
/// module docs for the rules.
///
/// A created-at time the listing does not carry is asked for through [`Resolve::Need`];
/// the caller resolves it and calls again from the start, so everything here is
/// restartable. Writes to the state only to forget distrusted keys the listing
/// no longer holds, which is idempotent.
#[must_use]
pub fn due(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &mut MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Resolve<Due> {
    match decide(inventory, now, config, published, state, resolved) {
        Ok(due) => Resolve::Done(due),
        Err(file) => Resolve::Need(file),
    }
}

fn decide(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &mut MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Result<Due, Need> {
    state.forget_unlisted(|key| inventory.level(key.level()).iter().any(|f| f.key == *key));
    let mut until: Option<Timestamp> = None;
    let levels = Level::ALL
        .iter()
        .skip(1)
        .take(usize::from(config.top().get()));
    for level in levels {
        let step = plan_level(inventory, now, config, *level, state, resolved)?;
        if let Some(job) = absorb(step, &mut until) {
            return Ok(Due::Run(job));
        }
    }
    let waits = state.backing_off_until(Level::SNAPSHOT, now);
    let step = match (waits, state.distrusted_at(Level::SNAPSHOT)) {
        (Some(retry), _) => Step::Wait(retry),
        // A rejected snapshot goes before the next one is written.
        (None, Some(bad)) => delete_first(bad),
        (None, None) => plan_snapshot(inventory, now, config, published, resolved)?,
    };
    if let Some(job) = absorb(step, &mut until) {
        return Ok(Due::Run(job));
    }
    let retained = match config.retention() {
        RetentionMode::Off => return Ok(Due::Wait { until, held: None }),
        RetentionMode::Enforce => {
            match retention(inventory, now, config, published, state, resolved) {
                Resolve::Done(retained) => retained,
                Resolve::Need(file) => return Err(file),
            }
        }
    };
    Ok(match retained {
        Retention::Delete(job) => Due::Run(Job::Delete(job)),
        Retention::Wait { until: wake } => Due::Wait {
            until: soonest(until, wake),
            held: None,
        },
        Retention::Held(hold) => Due::Wait {
            until,
            held: Some(hold),
        },
    })
}

/// The job a step wants, or folds its wake time into `until`.
fn absorb(step: Step, until: &mut Option<Timestamp>) -> Option<Job> {
    match step {
        Step::Run(job) => Some(job),
        Step::Wait(at) => {
            *until = Some(until.map_or(at, |soonest| soonest.min(at)));
            None
        }
        Step::Idle => None,
    }
}

fn soonest(a: Option<Timestamp>, b: Option<Timestamp>) -> Option<Timestamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn plan_level(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    dst: Level,
    state: &MaintainState,
    resolved: &ResolvedCreatedAt,
) -> Result<Step, Need> {
    let (Some(src_level), Some(interval)) = (
        Level::new(dst.get().saturating_sub(1)),
        config.interval(dst),
    ) else {
        return Ok(Step::Idle);
    };
    if let Some(retry) = state.backing_off_until(dst, now) {
        return Ok(Step::Wait(retry));
    }
    // A rejected output is never an input: its own level deletes it first.
    if state.distrusted_at(src_level).is_some() {
        return Ok(Step::Idle);
    }
    if let Some(bad) = state.distrusted_at(dst) {
        return Ok(delete_first(bad));
    }
    let dst_files = inventory.level(dst);
    let dst_last = dst_files.last();
    let dst_max = dst_last.map(|info| info.key.range().max());
    let mut above = inventory
        .level(src_level)
        .iter()
        .filter(|info| dst_max.is_none_or(|max| info.key.range().min() > max))
        .peekable();
    if above.peek().is_none() {
        return Ok(Step::Idle);
    }
    let (start, next) = next_window(now, interval);
    if let Some(info) = dst_last
        && created_after(resolved, info, start)?
    {
        return Ok(Step::Wait(next));
    }
    let run = contiguous_run(above);
    Ok(
        match CompactionInputs::new(run)
            .ok()
            .and_then(|inputs| CompactJob::new(inputs, dst).ok())
        {
            Some(job) => Step::Run(Job::Compact(job)),
            // Unreachable for a run built above; a property test pins it.
            None => Step::Idle,
        },
    )
}

/// The delete of a distrusted output, alone.
fn delete_first(bad: FileKey) -> Step {
    DeleteJob::new(vec![bad]).map_or(Step::Idle, |job| Step::Run(Job::Delete(job)))
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
    resolved: &ResolvedCreatedAt,
) -> Result<Step, Need> {
    let Some(pos) = inventory.newest().max(published) else {
        return Ok(Step::Idle);
    };
    let Some(snap) = inventory
        .level(Level::SNAPSHOT)
        .iter()
        .rev()
        .find(|info| is_snapshot_start(info))
    else {
        return Ok(Step::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: pos,
        })));
    };
    let after = snap.key.range().max();
    if pos <= after {
        return Ok(Step::Idle);
    }
    let (start, next) = next_window(now, config.snapshot_interval());
    if created_after(resolved, snap, start)? {
        return Ok(Step::Wait(next));
    }
    Ok(Step::Run(Job::Snapshot(SnapshotJob {
        after: Some(after),
        through: pos,
    })))
}

/// Whether the file was recorded as created after `start`, or the request for its created-at time.
fn created_after(
    resolved: &ResolvedCreatedAt,
    info: &FileInfo,
    start: Timestamp,
) -> Result<bool, Need> {
    resolved
        .exact(info)
        .map(|created_at| created_at.at() > start)
        .ok_or_else(|| Need::one(*info))
}

/// Why a job did not finish.
#[derive(Debug, Error)]
pub enum MaintainError {
    /// The compaction failed. After a failed destination the output may be
    /// visible; the next [`due`] on a fresh listing settles it.
    #[error("compaction: {0}")]
    Compact(#[source] CompactError),
    /// The store rejected the output (size or checksum mismatch) and deleting
    /// it failed too: it may be listed and undecodable. [`MaintainState`]
    /// remembers the key.
    #[error("compaction output {key} is corrupt and could not be removed: {source}")]
    OutputDistrusted {
        /// The output key.
        key: FileKey,
        /// The commit error.
        #[source]
        source: CompactError,
    },
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
    /// An input is gone from the replica, or the output's key already holds
    /// an object the listing did not show: list again and plan again.
    Replan {
        /// The missing input, or the output found already there.
        key: FileKey,
    },
    /// The job failed; nothing was retried here.
    Failed(MaintainError),
}

/// Runs one compaction. Level-1 jobs read `local` when every input's local
/// copy is the replica's object ([`local_is_replica`]), and the replica
/// otherwise or once when an input is missing or does not decode locally (the
/// local copy may be gone after a restart, or rotten); every other level
/// reads the replica. The output always goes to the replica. Inputs are never
/// deleted.
///
/// When the store rejected the output's commit, the output key is deleted
/// before returning: a listing would otherwise show it as coverage. That
/// delete may only take the object this attempt wrote, never one the key held
/// before (an earlier run's output that retention may already count as
/// cover): an output key a read finds taken before the write is not written
/// over, and the job is planned again from a fresh listing.
pub(crate) async fn run_compact<L: Role, R: Role>(
    job: &CompactJob,
    local: &L,
    replica: &R,
) -> Outcome {
    let output = job.output_key();
    match crate::probe::exists(replica, &output).await {
        Ok(true) => return Outcome::Replan { key: output },
        Ok(false) => {}
        Err(err) => return Outcome::Failed(MaintainError::Compact(CompactError::Dest(err))),
    }
    let result = if job.level().get() == 1 {
        compact_level_one(job, local, replica).await
    } else {
        compact_in(replica, replica, job.inputs(), job.level()).await
    };
    match result {
        Ok(done) => Outcome::Compacted {
            info: done.info(),
            bytes_read: done.bytes_read(),
        },
        Err(CompactError::InputVanished { key }) => Outcome::Replan { key },
        Err(CompactError::Dest(err)) if leaves_bad_output(&err) => {
            remove_rejected(replica, job.output_key(), err).await
        }
        Err(
            err @ (CompactError::Source(_)
            | CompactError::Local(_)
            | CompactError::Dest(_)
            | CompactError::Input { .. }
            | CompactError::Encode(_)
            | CompactError::SnapshotLevel { .. }
            | CompactError::Inputs(_)),
        ) => Outcome::Failed(MaintainError::Compact(err)),
    }
}

/// Level 1 from the local copies of level 0 when they are the replica's
/// objects, from the replica otherwise.
async fn compact_level_one<L: Role, R: Role>(
    job: &CompactJob,
    local: &L,
    replica: &R,
) -> Result<Compacted, CompactError> {
    if !local_is_replica(local, replica, job.inputs()).await? {
        return compact_in(replica, replica, job.inputs(), job.level()).await;
    }
    // The source is staging here, so a failed read is staging's error
    // (`CompactError::Local`), by its role.
    match compact_in(local, replica, job.inputs(), job.level()).await {
        Err(CompactError::InputVanished { .. } | CompactError::Input { .. }) => {
            compact_in(replica, replica, job.inputs(), job.level()).await
        }
        other => other,
    }
}

/// Bytes read from the end of each copy: the trailer, and one more to see
/// that the local copy ends where the replica's does.
const TAIL: ReadLen = ReadLen::or_chunk(17);
const _: () = assert!(matches!(TRAILER_LEN.checked_add(1), Some(17)));

/// Whether every input's local copy is the replica's object under the same
/// key: the same trailer (which holds the checksum of the whole file) at the
/// listed size, and nothing after it. A staged file can verify and still
/// differ (a staging backup put back, a file staged again after the replica
/// took the first): building level 1 from it would leave the replica with two
/// contents for one transaction, since restores read level 0 from the
/// replica. One read of 17 bytes per input from each store; a local copy that
/// is gone or differs answers false, and a replica object that is gone is
/// [`CompactError::InputVanished`].
async fn local_is_replica<L: Role, R: Role>(
    local: &L,
    replica: &R,
    inputs: &CompactionInputs,
) -> Result<bool, CompactError> {
    let trailer = u64::try_from(TRAILER_LEN).unwrap_or(u64::MAX);
    for info in inputs.files() {
        let Some(at) = info.size.checked_sub(trailer) else {
            return Ok(false);
        };
        let ours = match local.read(&info.key, at, TAIL).await {
            Ok(bytes) => bytes,
            Err(StoreError::NotFound { .. }) => return Ok(false),
            Err(err) => return Err(L::compact_input(err)),
        };
        if ours.len() != TRAILER_LEN {
            return Ok(false);
        }
        let theirs = match replica.read(&info.key, at, TAIL).await {
            Ok(bytes) => bytes,
            Err(StoreError::NotFound { .. }) => {
                return Err(CompactError::InputVanished { key: info.key });
            }
            Err(err) => return Err(R::compact_input(err)),
        };
        if ours != theirs {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether a failed destination commit may have left an object under the output
/// key that a listing would show and that does not decode.
const fn leaves_bad_output(err: &StoreError) -> bool {
    match err {
        StoreError::Corrupt { cause, .. } => match cause {
            CorruptCause::CommitSizeMismatch { removed, .. } => !*removed,
            CorruptCause::ChecksumMismatch { .. } => true,
            CorruptCause::NotARegularFile
            | CorruptCause::TimestampOutOfRange
            | CorruptCause::BadCreatedAt => false,
        },
        StoreError::Transient { .. }
        | StoreError::Throttled { .. }
        | StoreError::Denied { .. }
        | StoreError::NotFound { .. }
        | StoreError::AlreadyExists { .. }
        | StoreError::ScratchExhausted { .. }
        | StoreError::Refused { .. } => false,
    }
}

async fn remove_rejected<R: ObjectStore>(replica: &R, key: FileKey, err: StoreError) -> Outcome {
    match crate::publish::remove_one(replica, key, crate::publish::Why::RejectedOutput).await {
        Ok(()) => Outcome::Failed(MaintainError::Compact(CompactError::Dest(err))),
        Err(_unremoved) => Outcome::Failed(MaintainError::OutputDistrusted {
            key,
            source: CompactError::Dest(err),
        }),
    }
}

/// Runs one delete call. A key already gone is not an error.
pub async fn run_delete<R: ObjectStore>(job: &DeleteJob, replica: &R) -> Outcome {
    let removal = crate::publish::Removal::new(job.keys(), crate::publish::Why::Retention);
    match crate::publish::remove(replica, &removal).await {
        Ok(()) => Outcome::Deleted {
            count: job.keys().len(),
        },
        Err(err) => Outcome::Failed(MaintainError::Delete(err)),
    }
}
