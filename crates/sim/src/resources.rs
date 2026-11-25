//! The resource properties: what the engine may spend, and when.
//!
//! Each is a bound on something the simulator can count: checkpoints by mode,
//! bytes staged, listing calls, the WAL's length, the size of one write. The
//! counts come from the recorders (ground truth) and from [`CountingCheckpointer`],
//! never from the engine's own reports.

use std::cell::Cell;
use std::rc::Rc;

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, Event, SourceError};
use lsr_ltx::Timestamp;

use crate::oracle::{Bound, Violation, within};
use crate::reach::Reach;
use crate::store::RecorderCounts;

/// Bytes read on earlier ticks that an upload tick may hand the replica with its own: a file's
/// header, which the upload is begun from, held until it is whole.
const HELD_HEADER: u64 = 100;

/// Slices that may run after the application commits before a tick ends in anything else:
/// with a file in flight the WAL step and the slices take turns (`docs/decisions/D205.md`).
pub const SLICES_AHEAD: u64 = 1;

/// The largest piece the engine hands a store at once: its flush threshold
/// (1 MiB) and a page of slack.
pub const WRITE_CHUNK_LIMIT: u64 = (1 << 20) + (1 << 16);

/// Listing calls one tick may make: ten levels (`Level::ALL`) listed once each, three times
/// over, so 30. A boot into a brand-new meta directory makes the three: its own listing, the page
/// size's prefix, and the identity check's restore, which makes one attempt and lists once
/// (`docs/decisions/D176.md`). Every other boot makes two (the first two). A restore that listed
/// again on each retry made up to five.
pub const LIST_CALLS_PER_TICK: u64 = 30;

/// What the checkpointer was asked, by mode.
#[derive(Clone, Debug, Default)]
pub struct CheckpointTally {
    passive: Rc<Cell<u32>>,
    truncate: Rc<Cell<u32>>,
    full: Rc<Cell<u32>>,
}

impl CheckpointTally {
    /// Checkpoints asked in `mode` so far.
    #[must_use]
    pub fn count(&self, mode: CheckpointMode) -> u32 {
        match mode {
            CheckpointMode::Passive => self.passive.get(),
            CheckpointMode::Truncate => self.truncate.get(),
            CheckpointMode::Full => self.full.get(),
        }
    }

    /// Every checkpoint asked so far.
    #[must_use]
    pub fn total(&self) -> u32 {
        self.passive
            .get()
            .saturating_add(self.truncate.get())
            .saturating_add(self.full.get())
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
            CheckpointMode::Full => &self.tally.full,
        };
        cell.set(cell.get().saturating_add(1));
        self.inner.checkpoint(mode)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.inner.bump_seq()
    }
}

/// What one tick did, counted from outside.
#[derive(Clone, Debug)]
pub struct TickStats {
    /// What the tick reported; `None` when it was cut short.
    pub event: Option<Event>,
    /// The replica's calls during the tick.
    pub remote: RecorderCounts,
    /// Staging's calls during the tick.
    pub staging: RecorderCounts,
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
    /// The most bytes of staged files one tick may read for the replica
    /// (`UploadConfig::bytes_per_tick`).
    pub slice_bytes: u64,
}

/// `after - before`, field by field, for the counters (the maxima stay).
#[must_use]
pub fn delta(before: RecorderCounts, after: RecorderCounts) -> RecorderCounts {
    RecorderCounts {
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
        Event::Booted { .. }
        | Event::Retrying { .. }
        | Event::Degraded { .. }
        | Event::WritesHeld { .. } => true,
        Event::Maintained { .. } | Event::Uploaded(_) => true,
        Event::Synced { .. }
        | Event::Sending { .. }
        | Event::Checkpointed { .. }
        | Event::Idle
        | Event::Resnapshot(_)
        | Event::Clamped(_)
        | Event::TakeoverWanted(_) => false,
    }
}

fn calls(counts: &RecorderCounts) -> u64 {
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
    if matches!(
        stats.event,
        Some(Event::Sending { .. } | Event::Uploaded(_))
    ) {
        let slice = limits.slice_bytes.saturating_add(HELD_HEADER);
        within(Bound::SliceBytes, stats.remote.written_bytes, slice)?;
    }
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

/// Whether the slices of a file in flight leave the WAL step its turns. Once the application
/// has committed, at most [`SLICES_AHEAD`] ticks may end in a slice before one ends in
/// something else. While a task waits out a retry the WAL step may be the one waiting, so
/// those slices are counted as excused, never judged: the wait is the engine's own report.
#[derive(Debug, Default)]
pub struct SendTurns {
    /// The application committed since the last tick was taken in.
    landed: bool,
    /// A commit landed and every tick since ended in a slice.
    waiting: bool,
    ahead: u64,
    /// The last tick ended in a slice.
    in_flight: bool,
    waits_until: Option<Timestamp>,
}

impl SendTurns {
    /// A commit by the application, or by another connection, landed in the WAL.
    pub const fn committed(&mut self) {
        self.landed = true;
    }

    /// Takes in one tick: `ended` is its event (`None` for a tick that crashed, stopped or
    /// ended a drain). A commit that landed inside the tick is told after it.
    ///
    /// # Errors
    /// [`Violation::ResourceBound`] of [`Bound::SlicesAhead`].
    pub fn tick(
        &mut self,
        ended: Option<&Event>,
        began: Timestamp,
        reach: &mut Reach,
    ) -> Result<(), Violation> {
        let landed = std::mem::take(&mut self.landed);
        if self.waits_until.is_some_and(|until| began >= until) {
            self.waits_until = None;
        }
        if landed {
            self.waiting = true;
            if self.in_flight {
                reach.bump("send.commit_between_slices");
            }
        }
        let was_in_flight = self.in_flight;
        match ended {
            Some(Event::Sending { .. }) => {
                self.in_flight = true;
                if !self.waiting {
                    return Ok(());
                }
                if self.waits_until.is_some() {
                    reach.bump("send.turns.excused");
                    return Ok(());
                }
                self.ahead = self.ahead.saturating_add(1);
                reach.bump("send.turns.judged");
                return within(Bound::SlicesAhead, self.ahead, SLICES_AHEAD);
            }
            Some(
                Event::Retrying { until, .. }
                | Event::Degraded { until, .. }
                | Event::WritesHeld { until },
            ) => {
                self.waits_until = self.waits_until.max(Some(*until));
                if was_in_flight {
                    reach.bump("send.then_a_fault");
                }
            }
            None => {
                if was_in_flight {
                    reach.bump("send.then_the_process_ended");
                }
            }
            Some(
                Event::Booted { .. }
                | Event::Synced { .. }
                | Event::Checkpointed { .. }
                | Event::Uploaded(_)
                | Event::Maintained { .. }
                | Event::Idle
                | Event::Resnapshot(_)
                | Event::Clamped(_)
                | Event::TakeoverWanted(_),
            ) => {}
        }
        self.waiting = false;
        self.ahead = 0;
        self.in_flight = false;
        Ok(())
    }
}

/// The bounds on what the whole run wrote to the replica.
///
/// # Errors
/// [`Violation::ResourceBound`] when one write held more than the engine's flush
/// size.
pub fn memory(remote: RecorderCounts, staging: RecorderCounts) -> Result<(), Violation> {
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
