//! Running totals of the engine's own work: its sync steps and the SQLite checkpoints it
//! asks for. Go's `litestream_sync_*`, `litestream_total_wal_bytes`, `litestream_disk_full`
//! and `litestream_checkpoint_*` series are read from them (`db.go:1359-1376`,
//! `db.go:2256`, `db.go:2808-2820`). A report: nothing decides on it.
//!
//! A sync step is counted as Go counts `syncLocked`: once per attempt, an error when it
//! fails, and the time it took on the engine's clock. A checkpoint is counted at the
//! `Checkpointer::checkpoint` call, as Go counts `execCheckpoint`: the call, whether it
//! returned an error, and its time. A step that waits for room in staging, or a TRUNCATE
//! refused because readers are held, is a call too: the engine asked for it.

use std::time::Duration;

use lsr_wal::HEADER_LEN;

use crate::env::Clock;
use crate::fault::Fault;
use crate::position::{Position, ReadLockHold};
use crate::source::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, SourceError};

/// One kind of call's totals.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CallTotals {
    calls: u64,
    errors: u64,
    millis: u64,
}

impl CallTotals {
    /// No call yet.
    pub const ZERO: Self = Self {
        calls: 0,
        errors: 0,
        millis: 0,
    };

    /// Calls made.
    #[must_use]
    pub const fn calls(&self) -> u64 {
        self.calls
    }

    /// Calls that failed.
    #[must_use]
    pub const fn errors(&self) -> u64 {
        self.errors
    }

    /// Time spent in them on the engine's clock, in milliseconds.
    #[must_use]
    pub const fn millis(&self) -> u64 {
        self.millis
    }

    pub(crate) fn record(&mut self, failed: bool, took: Duration) {
        self.calls = self.calls.saturating_add(1);
        if failed {
            self.errors = self.errors.saturating_add(1);
        }
        let millis = u64::try_from(took.as_millis()).unwrap_or(u64::MAX);
        self.millis = self.millis.saturating_add(millis);
    }

    fn add(&mut self, other: Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.errors = self.errors.saturating_add(other.errors);
        self.millis = self.millis.saturating_add(other.millis);
    }
}

/// The checkpoints one tick asked SQLite for, per mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CheckpointTotals {
    passive: CallTotals,
    full: CallTotals,
    truncate: CallTotals,
}

impl CheckpointTotals {
    pub(crate) fn record(&mut self, mode: CheckpointMode, failed: bool, took: Duration) {
        self.get_mut(mode).record(failed, took);
    }

    const fn get(&self, mode: CheckpointMode) -> CallTotals {
        match mode {
            // A backlog copy is a PASSIVE to SQLite, from the other connection.
            CheckpointMode::Passive | CheckpointMode::Backlog => self.passive,
            CheckpointMode::Full => self.full,
            CheckpointMode::Truncate => self.truncate,
        }
    }

    const fn get_mut(&mut self, mode: CheckpointMode) -> &mut CallTotals {
        match mode {
            CheckpointMode::Passive | CheckpointMode::Backlog => &mut self.passive,
            CheckpointMode::Full => &mut self.full,
            CheckpointMode::Truncate => &mut self.truncate,
        }
    }
}

/// A checkpointer that counts each `checkpoint` call it passes on, for one tick.
pub(crate) struct Counted<'a, C> {
    inner: &'a mut C,
    clock: &'a dyn Clock,
    totals: CheckpointTotals,
}

impl<'a, C: Checkpointer> Counted<'a, C> {
    pub(crate) fn new(inner: &'a mut C, clock: &'a dyn Clock) -> Self {
        Self {
            inner,
            clock,
            totals: CheckpointTotals::default(),
        }
    }

    pub(crate) const fn totals(&self) -> CheckpointTotals {
        self.totals
    }
}

impl<C: Checkpointer> Checkpointer for Counted<'_, C> {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        self.inner.begin_barrier()
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        self.inner.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        let began = self.clock.now();
        let run = self.inner.checkpoint(mode);
        let took = self.clock.now().saturating_duration_since(began);
        self.totals.record(mode, run.is_err(), took);
        run
    }

    fn renew_read(&mut self) -> Result<ReadLockHold, SourceError> {
        self.inner.renew_read()
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.inner.bump_seq()
    }
}

/// What the engine's sync steps and checkpoints did since the actor began.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkCounts {
    sync: CallTotals,
    wal_bytes: u64,
    disk_full: bool,
    checkpoints: CheckpointTotals,
}

impl WorkCounts {
    /// Nothing done yet.
    pub const ZERO: Self = Self {
        sync: CallTotals::ZERO,
        wal_bytes: 0,
        disk_full: false,
        checkpoints: CheckpointTotals {
            passive: CallTotals::ZERO,
            full: CallTotals::ZERO,
            truncate: CallTotals::ZERO,
        },
    };

    /// Sync steps: attempts, failures and time (Go's `litestream_sync_count`,
    /// `_sync_error_count`, `_sync_seconds`).
    #[must_use]
    pub const fn sync(&self) -> CallTotals {
        self.sync
    }

    /// WAL bytes the sync steps staged (Go's `litestream_total_wal_bytes`).
    #[must_use]
    pub const fn wal_bytes(&self) -> u64 {
        self.wal_bytes
    }

    /// Whether the last sync step failed because the local disk is full (Go's
    /// `litestream_disk_full`).
    #[must_use]
    pub const fn disk_full(&self) -> bool {
        self.disk_full
    }

    /// The checkpoints asked of SQLite in `mode` (Go's `litestream_checkpoint_count`,
    /// `_checkpoint_error_count`, `_checkpoint_seconds`).
    #[must_use]
    pub const fn checkpoints(&self, mode: CheckpointMode) -> CallTotals {
        self.checkpoints.get(mode)
    }

    /// Counts one sync step that moved the position from `from` to `to`.
    pub(crate) fn sync_step(
        &mut self,
        fault: Option<Fault>,
        took: Duration,
        from: Option<&Position>,
        to: Option<&Position>,
    ) {
        self.sync.record(fault.is_some(), took);
        self.disk_full = fault == Some(Fault::LocalDiskFull);
        self.wal_bytes = self.wal_bytes.saturating_add(wal_bytes(from, to));
    }

    pub(crate) fn add_checkpoints(&mut self, tick: CheckpointTotals) {
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Truncate,
        ] {
            self.checkpoints.get_mut(mode).add(tick.get(mode));
        }
    }
}

/// The WAL bytes between two synced positions: the difference in one WAL generation, all of
/// the new generation's frames after a restart.
fn wal_bytes(from: Option<&Position>, to: Option<&Position>) -> u64 {
    let Some(to) = to else {
        return 0;
    };
    let end = to.synced().offset().get();
    match from {
        Some(from) if from.cursor().salts() == to.cursor().salts() => {
            end.saturating_sub(from.synced().offset().get())
        }
        Some(_) | None => end.saturating_sub(u64::try_from(HEADER_LEN).unwrap_or(u64::MAX)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_call_counts_its_failure_and_time() {
        let mut totals = CallTotals::default();
        totals.record(false, Duration::from_millis(5));
        totals.record(true, Duration::from_millis(7));
        assert_eq!(
            (totals.calls(), totals.errors(), totals.millis()),
            (2, 1, 12)
        );
    }

    #[test]
    fn disk_full_follows_the_last_step() {
        let mut work = WorkCounts::default();
        work.sync_step(Some(Fault::LocalDiskFull), Duration::ZERO, None, None);
        assert!(work.disk_full());
        work.sync_step(Some(Fault::WalSyncFailed), Duration::ZERO, None, None);
        assert!(!work.disk_full());
        work.sync_step(Some(Fault::LocalDiskFull), Duration::ZERO, None, None);
        work.sync_step(None, Duration::ZERO, None, None);
        assert!(!work.disk_full());
        assert_eq!((work.sync().calls(), work.sync().errors()), (4, 3));
    }

    #[test]
    fn checkpoints_are_kept_per_mode() {
        let mut tick = CheckpointTotals::default();
        tick.record(CheckpointMode::Truncate, true, Duration::from_millis(3));
        let mut work = WorkCounts::default();
        work.add_checkpoints(tick);
        work.add_checkpoints(tick);
        assert_eq!(
            work.checkpoints(CheckpointMode::Passive),
            CallTotals::default()
        );
        let truncate = work.checkpoints(CheckpointMode::Truncate);
        assert_eq!(
            (truncate.calls(), truncate.errors(), truncate.millis()),
            (2, 2, 6)
        );
    }
}
