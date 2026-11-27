//! The clock the engine and the application read inside a tick.
//!
//! The world keeps one [`FakeClock`], the driver's: it moves between ticks (a retry wait, a
//! jump, an idle second) and the recorders read it as the floor of every stamp. What the code
//! under test reads goes through [`SimClock`], under a [`ClockLaw`]:
//!
//! - [`ClockLaw::PerTick`]: every read inside a tick returns the driver's clock, which does not
//!   move until the tick ends (and what the tick has stalled, below). A stamp taken before a
//!   commit and one taken after it are equal, so no ordering of clock reads against commits can
//!   be wrong. Every recorded history keeps this law.
//! - [`ClockLaw::EachRead`]: every read returns a later time than the read before it, by a step
//!   of 1 to [`MAX_STEP_MS`] ms drawn from the run's own stream, so code that reads its clock
//!   at the wrong moment stamps a time the history can tell from the right one. When the tick
//!   ends the driver's clock catches up with the last read ([`SimClock::settle`]), so time never
//!   runs back across ticks unless a jump moves it.
//!
//! Every read is noted in the [`HighWater`] the recorders keep, so the oracle's bound on a
//! stamp (between the driver's clock when the store took the file and the latest time the
//! code under test read) holds under either law.
//!
//! Time can also pass inside a tick, under either law ([`Stall`]): a store call that hangs
//! until the client gives up, or a pause of the whole process ([`SimClock::pause_at`]), puts
//! every later read of the tick that much further on. Only the code under test sees it: the
//! driver's clock, which the stores and the recorders read, stays where the tick began, so a
//! file the engine stamped before the stall and a store took after it still lies inside the
//! oracle's bound, as the stamp lies inside the real store's own time. `settle` moves the
//! driver on by what the tick stalled, so the next tick begins after it.

use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::{Clock, Entropy, FakeClock, SeededEntropy};
use lsr_ltx::Timestamp;

use crate::store::HighWater;

/// Time spent inside the tick under way, shared by the clock and whatever stalls it (a hung
/// store call): every read after a stall is that much later. Cleared when the tick settles.
#[derive(Clone, Debug, Default)]
pub struct Stall(Arc<Mutex<u64>>);

impl Stall {
    /// The tick spends `millis` more milliseconds before its next read.
    pub fn add(&self, millis: u64) {
        let mut spent = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        *spent = spent.saturating_add(millis);
    }

    /// Milliseconds the tick has stalled so far.
    #[must_use]
    pub fn spent(&self) -> u64 {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn take(&self) -> u64 {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

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
    stall: Stall,
    // A pause armed for this tick: the reads it still waits, and its length in milliseconds.
    pause: Mutex<Option<(u8, u64)>>,
    // Pauses that fired since the clock was made.
    paused: Mutex<u32>,
}

impl SimClock {
    /// A clock over the driver's `driver`, noting reads in `high`, stepping under `law` with
    /// steps drawn from `seed`, and later by whatever `stall` adds inside a tick.
    #[must_use]
    pub fn new(
        driver: Arc<FakeClock>,
        high: HighWater,
        law: ClockLaw,
        seed: u64,
        stall: Stall,
    ) -> Self {
        Self {
            driver,
            high,
            law,
            steps: SeededEntropy::new(seed),
            last: Mutex::new(None),
            stall,
            pause: Mutex::new(None),
            paused: Mutex::new(0),
        }
    }

    /// The process stops for `millis` milliseconds just before the tick's `read`th clock
    /// read after its first (1: the first read the code under test makes), as an OS that
    /// suspends it would. A tick with fewer reads is not paused; the pause is dropped when it
    /// settles.
    pub fn pause_at(&self, read: u8, millis: u64) {
        // The tick's own reading comes first, and is not one of the code's reads.
        *self.pause.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((read.saturating_add(1), millis));
    }

    /// Pauses that fired so far.
    #[must_use]
    pub fn paused(&self) -> u32 {
        *self.paused.lock().unwrap_or_else(PoisonError::into_inner)
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
        *self.pause.lock().unwrap_or_else(PoisonError::into_inner) = None;
        let stalled = self.stalled_now();
        self.stall.take();
        let end = last.map_or(stalled, |last| last.max(stalled));
        if end > self.driver.now() {
            self.driver.set(end);
        }
    }

    /// The driver's clock and what the tick has stalled so far.
    fn stalled_now(&self) -> Timestamp {
        let spent = i64::try_from(self.stall.spent()).unwrap_or(i64::MAX);
        Timestamp::from_unix_millis(self.driver.now().unix_millis().saturating_add(spent))
    }

    /// Counts a read against an armed pause; stalls the tick when it is the one.
    fn count_read(&self) {
        let mut pause = self.pause.lock().unwrap_or_else(PoisonError::into_inner);
        let Some((left, millis)) = *pause else {
            return;
        };
        if left <= 1 {
            *pause = None;
            self.stall.add(millis);
            let mut paused = self.paused.lock().unwrap_or_else(PoisonError::into_inner);
            *paused = paused.saturating_add(1);
        } else {
            *pause = Some((left.saturating_sub(1), millis));
        }
    }

    fn step(&self) -> i64 {
        let drawn = self.steps.below(MAX_STEP_MS).saturating_add(1);
        i64::try_from(drawn).unwrap_or(1)
    }
}

impl Clock for SimClock {
    fn now(&self) -> Timestamp {
        self.count_read();
        let now = match self.law {
            ClockLaw::PerTick => self.stalled_now(),
            ClockLaw::EachRead => {
                let mut last = self.last.lock().unwrap_or_else(PoisonError::into_inner);
                let stalled = self.stalled_now();
                let base = last.map_or(stalled, |last| last.max(stalled));
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
