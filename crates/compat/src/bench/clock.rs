//! The real clock, in one place. A benchmark measures real time, so these are the only
//! sites in the module that read it or wait on it.

use std::time::{Duration, Instant};

/// The monotonic clock.
#[expect(
    clippy::disallowed_methods,
    reason = "the benchmark measures real time; there is no simulated clock to take"
)]
pub(super) fn now() -> Instant {
    Instant::now()
}

/// Blocks the calling thread for `pause`: the paced writers wait out their schedule.
#[expect(
    clippy::disallowed_methods,
    reason = "a paced writer keeps real time, as the processes it feeds do"
)]
pub(super) fn pause(pause: Duration) {
    std::thread::sleep(pause);
}

/// Blocks until `at`; returns at once when it has passed.
pub(super) fn pause_until(at: Instant) {
    if let Some(left) = at.checked_duration_since(now()) {
        pause(left);
    }
}

/// `start` plus `step` times `n`: the n-th slot of a schedule that does not drift.
pub(super) fn slot(start: Instant, step: Duration, n: u32) -> Instant {
    step.checked_mul(n)
        .and_then(|d| start.checked_add(d))
        .unwrap_or(start)
}
