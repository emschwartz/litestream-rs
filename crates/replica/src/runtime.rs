//! Time, waiting and chance, as the replicator thread and its callers see them.
//!
//! The thread waits in one place only: [`Park::park`], until the tick's next wake, the next
//! poll, or a command (which calls [`Park::unpark`]). Every deadline is a [`Timestamp`] read
//! from the injected [`Clock`]. So a test that injects [`Runtime::fake`] drives the poll
//! interval, the backoff and the stop deadline by moving the clock, and nothing sleeps.
//!
//! A caller that waits ([`crate::Replicator::wait_booted`], [`crate::FinalSync::wait`]) waits
//! on the replicator's condition variable, which the thread signals after every tick. Its
//! deadline is on the injected clock too; since a fake clock signals nobody when it moves, the
//! caller reads the clock again at least every [`CALLER_SLICE`] of real time.
//!
//! Not covered here: an `S3Store` signs with its own clock (`S3Store::with_transport` takes
//! it), and HTTP request deadlines run on real time inside the HTTP client.

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsr_core::{Clock, Entropy};
use lsr_ltx::Timestamp;

/// How long a caller's wait goes without reading the clock again.
pub const CALLER_SLICE: Duration = Duration::from_millis(50);

/// Everything the replicator thread waits on.
pub trait Park: Send + Sync {
    /// Returns once `until` has passed on the runtime's clock, or [`Park::unpark`] was called
    /// since the last return, whichever is first. `None` waits for an unpark alone. An unpark
    /// that comes before the park is not lost: the next park returns at once.
    fn park(&self, until: Option<Timestamp>);

    /// Ends the current park, or the next one if none is running.
    fn unpark(&self);
}

/// The clock, the park and the randomness one replicator runs on.
#[derive(Clone)]
pub struct Runtime {
    /// The time every deadline is read on.
    pub clock: Arc<dyn Clock>,
    /// Where the thread waits.
    pub park: Arc<dyn Park>,
    /// Chance, for whatever the host's store layers want jittered.
    pub entropy: Arc<dyn Entropy>,
}

impl fmt::Debug for Runtime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Runtime")
            .field("now", &self.clock.now())
            .finish_non_exhaustive()
    }
}

impl Runtime {
    /// The wall clock, a condition-variable park on it, and the standard library's randomly
    /// keyed hasher for chance.
    #[must_use]
    pub fn system() -> Self {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        Self {
            park: Arc::new(SystemPark::new(Arc::clone(&clock))),
            clock,
            entropy: Arc::new(OsEntropy::default()),
        }
    }

    /// A clock that moves only when the test says so ([`FakeClockHandle`]), a park that waits
    /// on it, and seeded chance: the deterministic test setup.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn fake(start: Timestamp, seed: u64) -> (Self, FakeClockHandle) {
        let time = Arc::new(fake::FakeTime::new(start));
        let clock: Arc<dyn Clock> = Arc::<fake::FakeTime>::clone(&time);
        let park: Arc<dyn Park> = Arc::<fake::FakeTime>::clone(&time);
        let runtime = Self {
            clock,
            park,
            entropy: Arc::new(lsr_core::SeededEntropy::new(seed)),
        };
        (runtime, FakeClockHandle { time })
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Every critical section here leaves plain data behind, so a poisoned lock is usable.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Milliseconds from `now` to `until`, zero when it has passed.
pub(crate) fn millis_until(now: Timestamp, until: Timestamp) -> u64 {
    u64::try_from(until.unix_millis().saturating_sub(now.unix_millis())).unwrap_or(0)
}

/// `at` plus `wait`, saturating.
pub(crate) fn later(at: Timestamp, wait: Duration) -> Timestamp {
    at.saturating_add(wait)
}

/// The wall clock.
#[derive(Debug)]
struct SystemClock;

impl Clock for SystemClock {
    #[expect(
        clippy::disallowed_methods,
        reason = "this is the real clock every other site takes as a `Clock`"
    )]
    fn now(&self) -> Timestamp {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        Timestamp::from_unix_millis(i64::try_from(since.as_millis()).unwrap_or(i64::MAX))
    }
}

/// A park on a condition variable, timed by its clock.
struct SystemPark {
    clock: Arc<dyn Clock>,
    woken: Mutex<bool>,
    signal: Condvar,
}

impl SystemPark {
    fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            woken: Mutex::new(false),
            signal: Condvar::new(),
        }
    }
}

impl Park for SystemPark {
    fn park(&self, until: Option<Timestamp>) {
        let mut woken = lock(&self.woken);
        loop {
            if std::mem::take(&mut *woken) {
                return;
            }
            woken = match until {
                None => self
                    .signal
                    .wait(woken)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(until) => {
                    let left = millis_until(self.clock.now(), until);
                    if left == 0 {
                        return;
                    }
                    self.signal
                        .wait_timeout(woken, Duration::from_millis(left))
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }

    fn unpark(&self) {
        *lock(&self.woken) = true;
        self.signal.notify_all();
    }
}

/// Chance from the standard library's randomly keyed hasher; jitter needs spread, not secrecy.
/// The entropy [`Runtime::system`] uses, and the `lsr` commands' retry layer.
#[derive(Debug, Default)]
pub struct OsEntropy {
    state: RandomState,
    draws: AtomicU64,
}

impl Entropy for OsEntropy {
    fn below(&self, upper: u64) -> u64 {
        let draw = self.draws.fetch_add(1, Ordering::Relaxed);
        self.state.hash_one(draw).checked_rem(upper).unwrap_or(0)
    }
}

#[cfg(feature = "testing")]
pub use fake::FakeClockHandle;

#[cfg(feature = "testing")]
mod fake {
    use std::sync::{Arc, Condvar, Mutex, PoisonError};
    use std::time::Duration;

    use lsr_core::{Clock, FakeClock};
    use lsr_ltx::Timestamp;

    use super::{Park, lock};

    #[derive(Debug, Default)]
    struct State {
        woken: bool,
        /// The park moves the clock to its wake time itself.
        free: bool,
    }

    /// The fake clock and its park, one object so moving the clock wakes the park.
    #[derive(Debug)]
    pub(super) struct FakeTime {
        clock: FakeClock,
        state: Mutex<State>,
        signal: Condvar,
    }

    impl FakeTime {
        pub(super) fn new(start: Timestamp) -> Self {
            Self {
                clock: FakeClock::new(start),
                state: Mutex::new(State::default()),
                signal: Condvar::new(),
            }
        }
    }

    impl Clock for FakeTime {
        fn now(&self) -> Timestamp {
            self.clock.now()
        }
    }

    impl Park for FakeTime {
        fn park(&self, until: Option<Timestamp>) {
            let mut state = lock(&self.state);
            loop {
                if std::mem::take(&mut state.woken) {
                    return;
                }
                if let Some(until) = until {
                    if self.clock.now() >= until {
                        return;
                    }
                    if state.free {
                        self.clock.set(until);
                        return;
                    }
                }
                state = self
                    .signal
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        }

        fn unpark(&self) {
            lock(&self.state).woken = true;
            self.signal.notify_all();
        }
    }

    /// The test's hold on a fake runtime's clock. The clock starts held: it moves only by
    /// [`FakeClockHandle::advance`], and a park waits for that. [`FakeClockHandle::release`]
    /// lets the park move the clock to each wake time itself, so the thread runs through time
    /// as fast as it can tick.
    #[derive(Clone, Debug)]
    pub struct FakeClockHandle {
        pub(super) time: Arc<FakeTime>,
    }

    impl FakeClockHandle {
        /// The clock's reading.
        #[must_use]
        pub fn now(&self) -> Timestamp {
            self.time.clock.now()
        }

        /// Moves the clock forward and wakes a park whose time has come.
        pub fn advance(&self, by: Duration) {
            let _state = lock(&self.time.state);
            self.time.clock.advance(by);
            self.time.signal.notify_all();
        }

        /// From now on a park moves the clock to its own wake time.
        pub fn release(&self) {
            lock(&self.time.state).free = true;
            self.time.signal.notify_all();
        }

        /// From now on the clock moves only by [`FakeClockHandle::advance`].
        pub fn hold(&self) {
            lock(&self.time.state).free = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unpark_before_the_park_is_not_lost() {
        let runtime = Runtime::system();
        runtime.park.unpark();
        runtime.park.park(None);
    }

    #[test]
    fn a_passed_deadline_returns_at_once() {
        let runtime = Runtime::system();
        let now = runtime.clock.now();
        runtime
            .park
            .park(Some(Timestamp::from_unix_millis(now.unix_millis() - 1)));
    }

    #[test]
    fn a_released_fake_park_moves_the_clock_to_its_wake() {
        let start = Timestamp::from_unix_millis(1_000);
        let (runtime, clock) = Runtime::fake(start, 1);
        clock.release();
        runtime.park.park(Some(Timestamp::from_unix_millis(5_000)));
        assert_eq!(clock.now(), Timestamp::from_unix_millis(5_000));
    }

    #[test]
    fn a_held_fake_park_waits_for_the_clock() {
        let start = Timestamp::from_unix_millis(1_000);
        let (runtime, clock) = Runtime::fake(start, 1);
        let park = Arc::clone(&runtime.park);
        let waiter = std::thread::spawn(move || {
            park.park(Some(Timestamp::from_unix_millis(2_000)));
        });
        clock.advance(Duration::from_secs(1));
        waiter.join().unwrap();
        assert_eq!(clock.now(), Timestamp::from_unix_millis(2_000));
    }

    #[test]
    fn os_entropy_stays_below_the_bound() {
        let entropy = OsEntropy::default();
        for upper in [1, 2, 7, 1000] {
            assert!(entropy.below(upper) < upper);
        }
        assert_eq!(entropy.below(0), 0);
        let draws: Vec<u64> = (0..64).map(|_| entropy.below(1000)).collect();
        assert!(draws.iter().any(|draw| *draw != draws[0]), "{draws:?}");
    }
}
