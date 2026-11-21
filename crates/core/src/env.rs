//! The only doors to time and chance in core.
//!
//! Code in this crate reads no clock, sleeps on no timer and draws no random
//! number except through these traits, so the simulator can replay any run from
//! its seed. Production implementations live beside the runtime that provides
//! them; the fakes here (behind `testing`) are what tests and the simulator
//! build on.

use std::future::Future;
use std::time::Duration;

use lsr_ltx::Timestamp;

/// Wall-clock time.
pub trait Clock: Send + Sync {
    /// The current time.
    fn now(&self) -> Timestamp;
}

/// Suspends the calling future. Retry waits are the only caller in core.
pub trait Sleeper: Send + Sync {
    /// Completes after `wait`.
    fn sleep(&self, wait: Duration) -> impl Future<Output = ()> + Send;
}

/// A source of uniform random numbers.
pub trait Entropy: Send + Sync {
    /// A number uniform in `0..upper`. `upper` is above zero; an
    /// implementation may return zero for zero.
    fn below(&self, upper: u64) -> u64;
}

#[cfg(any(test, feature = "testing"))]
pub use fakes::{FakeClock, RecordingSleeper, SeededEntropy};

#[cfg(any(test, feature = "testing"))]
mod fakes {
    use std::future::Future;
    use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;

    use lsr_ltx::Timestamp;

    use super::{Clock, Entropy, Sleeper};

    /// A clock that moves only when told to.
    #[derive(Debug)]
    pub struct FakeClock {
        millis: AtomicI64,
    }

    impl FakeClock {
        /// A clock reading `start`.
        #[must_use]
        pub fn new(start: Timestamp) -> Self {
            Self {
                millis: AtomicI64::new(start.unix_millis()),
            }
        }

        /// Moves the clock forward, saturating at the largest timestamp.
        pub fn advance(&self, by: Duration) {
            let step = i64::try_from(by.as_millis()).unwrap_or(i64::MAX);
            self.millis
                .update(Ordering::Relaxed, Ordering::Relaxed, |now| {
                    now.saturating_add(step)
                });
        }

        /// Sets the clock.
        pub fn set(&self, to: Timestamp) {
            self.millis.store(to.unix_millis(), Ordering::Relaxed);
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_unix_millis(self.millis.load(Ordering::Relaxed))
        }
    }

    /// A sleeper that returns at once and records every wait it was asked for.
    /// Clones share one record, so a test keeps a handle while the code under
    /// test owns another.
    #[derive(Clone, Debug, Default)]
    pub struct RecordingSleeper {
        waits: Arc<Mutex<Vec<Duration>>>,
    }

    impl RecordingSleeper {
        /// A sleeper with an empty record.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Every wait asked for so far, in order.
        #[must_use]
        pub fn waits(&self) -> Vec<Duration> {
            self.waits
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }

        /// The sum of every wait asked for so far.
        #[must_use]
        pub fn total(&self) -> Duration {
            self.waits()
                .into_iter()
                .fold(Duration::ZERO, Duration::saturating_add)
        }
    }

    impl Sleeper for RecordingSleeper {
        fn sleep(&self, wait: Duration) -> impl Future<Output = ()> + Send {
            self.waits
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(wait);
            std::future::ready(())
        }
    }

    const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

    /// A small seeded generator (splitmix64), here so tests need no
    /// dependency. The same seed gives the same sequence for any one caller.
    #[derive(Debug)]
    pub struct SeededEntropy {
        state: AtomicU64,
    }

    impl SeededEntropy {
        /// A generator for `seed`.
        #[must_use]
        pub fn new(seed: u64) -> Self {
            Self {
                state: AtomicU64::new(seed),
            }
        }

        fn next_u64(&self) -> u64 {
            let step = self
                .state
                .fetch_add(GAMMA, Ordering::Relaxed)
                .wrapping_add(GAMMA);
            let mixed = (step ^ (step >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            mixed ^ (mixed >> 31)
        }
    }

    impl Entropy for SeededEntropy {
        fn below(&self, upper: u64) -> u64 {
            // Multiply-shift range reduction: the bias is under 2^-32 for
            // the bounds used here, which a fake can live with.
            let wide = u128::from(self.next_u64()).wrapping_mul(u128::from(upper));
            u64::try_from(wide >> 64).unwrap_or(0)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn clock_moves_only_when_told() {
            let clock = FakeClock::new(Timestamp::from_unix_millis(10));
            assert_eq!(clock.now().unix_millis(), 10);
            clock.advance(Duration::from_millis(5));
            assert_eq!(clock.now().unix_millis(), 15);
            clock.set(Timestamp::from_unix_millis(-3));
            assert_eq!(clock.now().unix_millis(), -3);
        }

        #[test]
        fn entropy_is_below_the_bound_and_repeats_per_seed() {
            let (a, b) = (SeededEntropy::new(7), SeededEntropy::new(7));
            for upper in [1_u64, 2, 3, 1000, u64::MAX] {
                let (x, y) = (a.below(upper), b.below(upper));
                assert_eq!(x, y);
                assert!(x < upper);
            }
            assert_eq!(a.below(0), 0);
        }

        /// The generator is splitmix64: its first outputs for seed 0 are
        /// published, so any change to a shift, a mask or the step shows.
        #[test]
        fn entropy_is_splitmix64() {
            let rng = SeededEntropy::new(0);
            assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
            assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
            assert_eq!(rng.next_u64(), 0x06c4_5d18_8009_454f);
        }

        /// `below` is the high half of the 128-bit product: for a bound of
        /// 2^32 that is the output's top 32 bits.
        #[test]
        fn below_scales_the_whole_output() {
            let rng = SeededEntropy::new(0);
            assert_eq!(rng.below(1 << 32), 0xe220_a839);
            assert_eq!(rng.below(1 << 32), 0x6e78_9e6a);
            assert_eq!(rng.below(1 << 8), 0x06);
        }

        #[test]
        fn sleeper_records_in_order() {
            let sleeper = RecordingSleeper::new();
            let handle = sleeper.clone();
            drop(sleeper.sleep(Duration::from_millis(1)));
            drop(sleeper.sleep(Duration::from_millis(2)));
            assert_eq!(
                handle.waits(),
                [Duration::from_millis(1), Duration::from_millis(2)]
            );
            assert_eq!(handle.total(), Duration::from_millis(3));
        }
    }
}
