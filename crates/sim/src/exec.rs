//! The executor: runs a future by hand and crashes it on request.
//!
//! A crash is dropping the future at one of its `Pending` results. Every
//! borrow, read-transaction handle and in-flight upload the future held dies with
//! it, exactly as a process death takes them, while the stores and the SQLite
//! model it touched stay. `YieldEachCall` (in `store`) makes every store call return
//! `Pending` once, so every store call is a crash point and nothing else is.
//! The waker is a no-op: a future that is not ready is simply polled again.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// How many `Pending` results a run may survive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Polls(u32);

impl Polls {
    /// No crash: the run is polled until it is done.
    pub const UNBOUNDED: Self = Self(u32::MAX);

    /// Survives `survived` `Pending` results and is dropped at the next one.
    #[must_use]
    pub const fn new(survived: u32) -> Self {
        Self(survived)
    }

    /// The count.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// How a run ended.
#[derive(Debug, PartialEq, Eq)]
pub enum RunEnd<T> {
    /// The future finished.
    Done(T),
    /// The future was dropped at the next `Pending` after `polls` of them.
    Crashed {
        /// The `Pending` results it survived.
        polls: u32,
    },
}

/// Polls `fut` until it is done or would return its (`budget` + 1)th `Pending`,
/// when it is dropped instead.
pub fn run_until<F: Future>(fut: F, budget: Polls) -> RunEnd<F::Output> {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    let mut pending = 0_u32;
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return RunEnd::Done(out),
            Poll::Pending => {
                if pending >= budget.get() {
                    return RunEnd::Crashed { polls: pending };
                }
                pending = pending.saturating_add(1);
            }
        }
    }
}

/// Runs `fut` to the end and returns its output with how many `Pending` results
/// it produced: the number of crash points a tick has.
pub fn count_polls<F: Future>(fut: F) -> (F::Output, u32) {
    let mut fut = pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    let mut pending = 0_u32;
    loop {
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(out) => return (out, pending),
            Poll::Pending => pending = pending.saturating_add(1),
        }
    }
}

#[cfg(test)]
mod tests;
