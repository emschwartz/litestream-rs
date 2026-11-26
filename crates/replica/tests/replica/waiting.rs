//! Waiting on a replicator that runs on the fake clock, for tests.
//!
//! On [`lsr_replica::Runtime::fake`] the clock moves only when the test moves it, so anything the
//! engine has timed on it (a retry's backoff after a busy database or a failed store call, the
//! next poll, a scheduled verify) never comes while a test only watches health. A test that
//! moves the clock once and then watches stalls whenever the engine needs more clock than that
//! one move. [`moved_until`] is the one way to wait on such a replicator: it moves the clock a
//! step at a time, each time only once the replica thread's park has read the clock as it
//! reads now (the thread is waiting on that clock, not working), until a condition on the
//! health holds. Every wait is bounded twice: by `steps` moves of the clock, and by [`LOOKS`]
//! looks at the health, so a thread that never changes anything fails the test in about a
//! minute instead of hanging it.

use std::cell::Cell;
use std::time::Duration;

use lsr_replica::{FakeClockHandle, Health, Replicator};

/// How long a single look may wait on the replicator's clock; on the fake clock this deadline
/// is never the bound, the look count is.
pub const PATIENCE: Duration = Duration::from_secs(600);

/// How many times a wait looks at the health before it gives up. The replicator wakes a waiter
/// on every change and at least every 50 ms of real time, so a wait that sees nothing
/// change gives up after about a minute.
pub const LOOKS: u32 = 1_200;

/// The health a wait ended with when its condition never held.
#[derive(Clone, Debug)]
pub struct NeverGotThere(pub Box<Health>);

impl std::fmt::Display for NeverGotThere {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the replicator never got there: {:?}", self.0)
    }
}

/// Moves `clock` `step` at a time, at most `steps` times, until `done` holds, and returns that
/// health. Each move waits first for `done` or for the thread's park to have read the clock as
/// it reads now, so a slow thread is never outrun by the clock.
///
/// # Errors
/// [`NeverGotThere`] with the last health seen when `done` never held.
pub fn moved_until(
    replicator: &Replicator,
    clock: &FakeClockHandle,
    (step, steps): (Duration, u32),
    done: impl Fn(&Health) -> bool,
) -> Result<Health, NeverGotThere> {
    for _ in 0..steps {
        if let Some(health) = seen_or(replicator, clock, &done)? {
            return Ok(health);
        }
        clock.advance(step);
    }
    seen_or(replicator, clock, &done)?.ok_or_else(|| NeverGotThere(Box::new(replicator.health())))
}

/// Waits until `done` holds, returning that health, or until the thread's park has read the
/// clock as it reads now, returning `None`. Equal, not at least: after a step back the park's
/// last reading is later than the clock.
///
/// # Errors
/// [`NeverGotThere`] after [`LOOKS`] looks with neither.
pub fn seen_or(
    replicator: &Replicator,
    clock: &FakeClockHandle,
    done: &impl Fn(&Health) -> bool,
) -> Result<Option<Health>, NeverGotThere> {
    let now = clock.now();
    let (looks, met) = (Cell::new(0_u32), Cell::new(false));
    let seen = replicator.wait_until(PATIENCE, |health| {
        looks.set(looks.get().saturating_add(1));
        met.set(done(health));
        met.get() || clock.seen_by_park() == Some(now) || looks.get() >= LOOKS
    });
    match seen {
        Some(health) if met.get() => Ok(Some(health)),
        Some(_) if clock.seen_by_park() == Some(now) => Ok(None),
        Some(health) => Err(NeverGotThere(Box::new(health))),
        None => Err(NeverGotThere(Box::new(replicator.health()))),
    }
}
