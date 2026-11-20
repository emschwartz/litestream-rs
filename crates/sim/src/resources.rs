//! The resource properties: what the engine may spend, and when.
//!
//! Each is a bound on something the simulator can count: checkpoints by mode,
//! bytes staged, listing calls, the WAL's length, the size of one write. The
//! counts come from the taps (ground truth) and from [`CountingCheckpointer`],
//! never from the engine's own reports.

use std::cell::Cell;
use std::rc::Rc;

use lsr_core::{
    Barrier, CheckpointMode, CheckpointRun, Checkpointer, Event, SourceError, WriteLock,
};

use crate::oracle::{Bound, Violation, within};
use crate::store::TapCounts;

/// The largest piece the engine hands a store at once: its flush threshold
/// (1 MiB) and a page of slack.
pub const WRITE_CHUNK_LIMIT: u64 = (1 << 20) + (1 << 16);

/// Listing calls one tick may make: a listing of every level, twice over.
pub const LIST_CALLS_PER_TICK: u64 = 24;

/// What the checkpointer was asked, by mode.
#[derive(Clone, Debug, Default)]
pub struct CheckpointTally {
    passive: Rc<Cell<u32>>,
    truncate: Rc<Cell<u32>>,
}

impl CheckpointTally {
    /// Checkpoints asked in `mode` so far.
    #[must_use]
    pub fn count(&self, mode: CheckpointMode) -> u32 {
        match mode {
            CheckpointMode::Passive => self.passive.get(),
            CheckpointMode::Truncate => self.truncate.get(),
        }
    }

    /// Every checkpoint asked so far.
    #[must_use]
    pub fn total(&self) -> u32 {
        self.passive.get().saturating_add(self.truncate.get())
    }
}

/// A checkpointer that counts what it is asked and forwards everything.
#[derive(Debug)]
pub struct CountingCheckpointer<C> {
    inner: C,
    tally: CheckpointTally,
}

impl<C> CountingCheckpointer<C> {
    /// Wraps `inner`, counting into `tally`.
    #[must_use]
    pub const fn new(inner: C, tally: CheckpointTally) -> Self {
        Self { inner, tally }
    }
}

impl<C: Checkpointer> Checkpointer for CountingCheckpointer<C> {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.inner.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.inner.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let cell = match mode {
            CheckpointMode::Passive => &self.tally.passive,
            CheckpointMode::Truncate => &self.tally.truncate,
        };
        cell.set(cell.get().saturating_add(1));
        self.inner.checkpoint(mode)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.inner.bump_seq()
    }

    fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError> {
        self.inner.begin_write_lock()
    }

    fn end_write_lock(&mut self, lock: WriteLock) -> Result<(), SourceError> {
        self.inner.end_write_lock(lock)
    }
}

/// What one tick did, counted from outside.
#[derive(Clone, Debug)]
pub struct TickStats {
    /// What the tick reported; `None` when it was cut short.
    pub event: Option<Event>,
    /// The replica's calls during the tick.
    pub remote: TapCounts,
    /// Staging's calls during the tick.
    pub staging: TapCounts,
    /// Passive checkpoints asked.
    pub passive: u32,
    /// All checkpoints asked.
    pub checkpoints: u32,
}

/// What the run lets one tick spend.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// The most bytes one tick may stage.
    pub staged_bytes: u64,
}

/// `after - before`, field by field, for the counters (the maxima stay).
#[must_use]
pub fn delta(before: TapCounts, after: TapCounts) -> TapCounts {
    TapCounts {
        lists: after.lists.saturating_sub(before.lists),
        reads: after.reads.saturating_sub(before.reads),
        begins: after.begins.saturating_sub(before.begins),
        writes: after.writes.saturating_sub(before.writes),
        commits: after.commits.saturating_sub(before.commits),
        deletes: after.deletes.saturating_sub(before.deletes),
        written_bytes: after.written_bytes.saturating_sub(before.written_bytes),
        max_upload_bytes: after.max_upload_bytes,
        max_write_bytes: after.max_write_bytes,
    }
}

const fn lists_allowed(event: &Event) -> bool {
    match event {
        Event::Booted { .. } | Event::Retrying { .. } => true,
        Event::Maintained(_) | Event::Uploaded(_) => true,
        Event::Synced { .. }
        | Event::Checkpointed { .. }
        | Event::Idle
        | Event::Resnapshot(_)
        | Event::Clamped(_) => false,
    }
}

fn calls(counts: &TapCounts) -> u64 {
    [
        counts.lists,
        counts.reads,
        counts.begins,
        counts.commits,
        counts.deletes,
    ]
    .iter()
    .fold(0, |sum, n| sum.saturating_add(*n))
}

/// The bounds that hold of every tick.
///
/// # Errors
/// The first [`Violation::ResourceBound`].
pub fn per_tick(stats: &TickStats, limits: Limits) -> Result<(), Violation> {
    within(Bound::PassivePerTick, u64::from(stats.passive), 1)?;
    within(
        Bound::StagedBytesPerTick,
        stats.staging.written_bytes,
        limits.staged_bytes,
    )?;
    within(Bound::ListCalls, stats.remote.lists, LIST_CALLS_PER_TICK)?;
    if matches!(stats.event, Some(Event::Idle)) {
        let spent = calls(&stats.remote);
        within(Bound::IdleLists, spent, 0)?;
    }
    Ok(())
}

/// The bounds on a stretch of ticks in which the app did nothing.
#[derive(Debug, Default)]
pub struct IdleWindow {
    staged: u64,
    checkpoints: u64,
    lists: u64,
}

impl IdleWindow {
    /// Adds one idle tick.
    pub fn add(&mut self, stats: &TickStats) {
        self.staged = self.staged.saturating_add(stats.staging.commits);
        self.checkpoints = self
            .checkpoints
            .saturating_add(u64::from(stats.checkpoints));
        let listing_is_due = stats.event.as_ref().is_some_and(lists_allowed);
        if !listing_is_due {
            self.lists = self.lists.saturating_add(stats.remote.lists);
        }
    }

    /// Judges the window.
    ///
    /// # Errors
    /// The first [`Violation::ResourceBound`].
    pub fn check(&self) -> Result<(), Violation> {
        within(Bound::IdleFiles, self.staged, 0)?;
        within(Bound::IdleCheckpoints, self.checkpoints, 0)?;
        within(Bound::IdleLists, self.lists, 0)
    }
}

/// The bounds on what the whole run wrote to the replica.
///
/// # Errors
/// [`Violation::ResourceBound`] when one write held more than the engine's flush
/// size.
pub fn memory(remote: TapCounts, staging: TapCounts) -> Result<(), Violation> {
    within(
        Bound::UploadBuffer,
        remote.max_write_bytes,
        WRITE_CHUNK_LIMIT,
    )?;
    within(
        Bound::UploadBuffer,
        staging.max_write_bytes,
        WRITE_CHUNK_LIMIT,
    )
}

#[cfg(test)]
mod tests;
