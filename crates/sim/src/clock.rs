//! The clock the engine and the application read inside a tick.
//!
//! The world keeps one [`FakeClock`], the driver's: it moves between ticks (a retry wait, a
//! jump, an idle second) and the recorders read it as the floor of every stamp. What the code
//! under test reads goes through [`SimClock`], under a [`ClockLaw`]:
//!
//! - [`ClockLaw::PerTick`]: every read inside a tick returns the driver's clock, which does not
//!   move until the tick ends. One reading per tick, as the simulator always had: a stamp taken
//!   before a commit and one taken after it are equal, so no ordering of clock reads against
//!   commits can be wrong. Every recorded history keeps this law.
//! - [`ClockLaw::EachRead`]: every read returns a later time than the read before it, by a step
//!   of 1 to [`MAX_STEP_MS`] ms drawn from the run's own stream, so code that reads its clock
//!   at the wrong moment stamps a time the history can tell from the right one. When the tick
//!   ends the driver's clock catches up with the last read ([`SimClock::settle`]), so time never
//!   runs back across ticks unless a jump moves it.
//!
//! Every read is noted in the [`HighWater`] the recorders keep, so the oracle's bound on a
//! stamp (between the driver's clock when the store took the file and the latest time the
//! code under test read) holds under either law.

use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::{Clock, Entropy, FakeClock, SeededEntropy};
use lsr_ltx::Timestamp;

use crate::store::HighWater;

/// The largest step, in milliseconds, between two reads under [`ClockLaw::EachRead`].
pub const MAX_STEP_MS: u64 = 40;

/// How the clock moves between reads inside a tick. Not an operation: it changes what every
/// read of the run returns, so a history's draws are the same under either law.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClockLaw {
    /// Every read in a tick returns the same time.
    #[default]
    PerTick,
    /// Every read returns a later time than the one before.
    EachRead,
}

/// The engine's and the application's clock.
#[derive(Debug)]
pub struct SimClock {
    driver: Arc<FakeClock>,
    high: HighWater,
    law: ClockLaw,
    steps: SeededEntropy,
    // The last time read in this tick under `EachRead`; cleared by `settle`.
    last: Mutex<Option<Timestamp>>,
}

impl SimClock {
    /// A clock over the driver's `driver`, noting reads in `high`, stepping under `law` with
    /// steps drawn from `seed`.
    #[must_use]
    pub fn new(driver: Arc<FakeClock>, high: HighWater, law: ClockLaw, seed: u64) -> Self {
        Self {
            driver,
            high,
            law,
            steps: SeededEntropy::new(seed),
            last: Mutex::new(None),
        }
    }

    /// The law it moves by.
    #[must_use]
    pub const fn law(&self) -> ClockLaw {
        self.law
    }

    /// The tick ended: the driver's clock moves up to the last time read, and the next tick's
    /// reads start from the driver's clock again (which a jump may have moved either way).
    pub fn settle(&self) {
        let last = self
            .last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(last) = last.filter(|last| *last > self.driver.now()) {
            self.driver.set(last);
        }
    }

    fn step(&self) -> i64 {
        let drawn = self.steps.below(MAX_STEP_MS).saturating_add(1);
        i64::try_from(drawn).unwrap_or(1)
    }
}

impl Clock for SimClock {
    fn now(&self) -> Timestamp {
        let now = match self.law {
            ClockLaw::PerTick => self.driver.now(),
            ClockLaw::EachRead => {
                let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
                let base = last.map_or(self.driver.now(), |last| last.max(self.driver.now()));
                let now =
                    Timestamp::from_unix_millis(base.unix_millis().saturating_add(self.step()));
                *last = Some(now);
                now
            }
        };
        self.high.note(now);
        now
    }
}

#[cfg(test)]
mod tests;
