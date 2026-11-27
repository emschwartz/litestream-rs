//! The application's wait on the engine's write barrier.
//!
//! A checkpoint takes the barrier (`BEGIN; INSERT INTO _litestream_lock`) so no commit lands
//! between the seal that stages the WAL's tail and the PASSIVE that copies it back: every
//! application writer waits for it. Go holds it for its `verifyAndSync` to the local level 0
//! and the checkpoint, local disk work only (`db.go` `checkpoint`). [`Watching`] wraps the
//! engine's checkpointer and, from the recorders' counts, prices what each tick did on each
//! store while it held the barrier at [`Profile::DEFAULT`]: the application's wait. The
//! runner judges it after every tick: no replica request under the barrier, the wait within
//! one staging tick's bound, and no barrier still held when a tick ends without a crash.
//!
//! The store calls are not all of the wait: the PASSIVE under the barrier copies frames into the
//! database file, and a WAL that a level-9 snapshot held back for its whole upload makes that
//! copy as long as the snapshot (a drill's writer waited 5.9 s on one). So the wrapper also
//! counts the frames the engine's own checkpoints copy while it holds the barrier, read from
//! the model's `nBackfill` around each call, and the runner bounds them per barrier.

use std::cell::RefCell;
use std::rc::Rc;

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, ReadLockHold, SourceError};

use crate::cost::Profile;
use crate::resources::delta;
use crate::store::{RecorderCounts, RecorderLog};

/// What the barrier saw since the runner last took it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Held {
    /// Barriers released.
    pub holds: u64,
    /// Replica requests made while one was held.
    pub remote_requests: u64,
    /// Microseconds of store calls made while one was held, at [`Profile::DEFAULT`].
    pub wait_micros: u64,
    /// Frames the engine's checkpoints copied into the database file while one was held.
    pub copied_frames: u64,
    /// A barrier is held now.
    pub open: bool,
}

#[derive(Debug, Default)]
struct State {
    /// Both stores' counts when the barrier held now was taken.
    open: Option<(RecorderCounts, RecorderCounts)>,
    held: Held,
}

/// The record a run keeps across its ticks' checkpointers.
#[derive(Clone, Debug, Default)]
pub struct BarrierWatch(Rc<RefCell<State>>);

impl BarrierWatch {
    /// What was seen since the last call, which starts the count again; a barrier still held
    /// stays open.
    pub fn take(&self) -> Held {
        let mut state = self.0.borrow_mut();
        let held = Held {
            open: state.open.is_some(),
            ..state.held
        };
        state.held = Held::default();
        held
    }

    /// The process died: whatever it held went with it.
    pub fn process_died(&self) {
        *self.0.borrow_mut() = State::default();
    }
}

/// The model's `nBackfill` now.
pub type Backfilled = Box<dyn Fn() -> u32>;

/// A checkpointer that records what each barrier it takes is held across.
pub struct Watching<C> {
    inner: C,
    watch: BarrierWatch,
    remote: RecorderLog,
    staging: RecorderLog,
    backfilled: Backfilled,
}

impl<C> std::fmt::Debug for Watching<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Watching").finish_non_exhaustive()
    }
}

impl<C> Watching<C> {
    /// Wraps `inner`, recording into `watch` from the two stores' recorders and the model's
    /// `backfilled`.
    #[must_use]
    pub fn new(
        inner: C,
        watch: BarrierWatch,
        (remote, staging): (RecorderLog, RecorderLog),
        backfilled: Backfilled,
    ) -> Self {
        Self {
            inner,
            watch,
            remote,
            staging,
            backfilled,
        }
    }
}

impl<C: Checkpointer> Checkpointer for Watching<C> {
    fn begin_barrier(&mut self) -> Result<Barrier, SourceError> {
        let barrier = self.inner.begin_barrier()?;
        self.watch.0.borrow_mut().open = Some((self.remote.counts(), self.staging.counts()));
        Ok(barrier)
    }

    fn end_barrier(&mut self, barrier: Barrier) -> Result<(), SourceError> {
        let open = self.watch.0.borrow_mut().open.take();
        if let Some((remote0, staging0)) = open {
            let remote = delta(remote0, self.remote.counts());
            let staging = delta(staging0, self.staging.counts());
            let mut state = self.watch.0.borrow_mut();
            let held = &mut state.held;
            held.holds = held.holds.saturating_add(1);
            held.remote_requests = held
                .remote_requests
                .saturating_add(crate::cost::requests(&remote));
            held.wait_micros = held.wait_micros.saturating_add(
                u64::try_from(Profile::DEFAULT.tick(&remote, &staging).as_micros())
                    .unwrap_or(u64::MAX),
            );
        }
        self.inner.end_barrier(barrier)
    }

    fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError> {
        if self.watch.0.borrow().open.is_none() {
            return self.inner.checkpoint(mode);
        }
        let before = (self.backfilled)();
        let run = self.inner.checkpoint(mode);
        // Nothing restarts the WAL under the barrier, so the count only grows.
        let copied = (self.backfilled)().saturating_sub(before);
        let held = &mut self.watch.0.borrow_mut().held;
        held.copied_frames = held.copied_frames.saturating_add(u64::from(copied));
        run
    }

    fn renew_read(&mut self) -> Result<ReadLockHold, SourceError> {
        self.inner.renew_read()
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.inner.bump_seq()
    }
}

#[cfg(test)]
mod tests;
