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

use std::cell::RefCell;
use std::rc::Rc;

use lsr_core::{Barrier, CheckpointMode, CheckpointRun, Checkpointer, SourceError};

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

/// A checkpointer that records what each barrier it takes is held across.
#[derive(Debug)]
pub struct Watching<C> {
    inner: C,
    watch: BarrierWatch,
    remote: RecorderLog,
    staging: RecorderLog,
}

impl<C> Watching<C> {
    /// Wraps `inner`, recording into `watch` from the two stores' recorders.
    #[must_use]
    pub const fn new(
        inner: C,
        watch: BarrierWatch,
        remote: RecorderLog,
        staging: RecorderLog,
    ) -> Self {
        Self {
            inner,
            watch,
            remote,
            staging,
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
        self.inner.checkpoint(mode)
    }

    fn bump_seq(&mut self) -> Result<(), SourceError> {
        self.inner.bump_seq()
    }
}

#[cfg(test)]
mod tests;
