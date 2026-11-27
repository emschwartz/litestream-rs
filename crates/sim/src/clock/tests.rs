use std::sync::Arc;
use std::time::Duration;

use lsr_core::{Clock, FakeClock};
use lsr_ltx::Timestamp;

use super::{ClockLaw, MAX_STEP_MS, SimClock, Stall};
use crate::store::HighWater;

fn clock(law: ClockLaw) -> (Arc<FakeClock>, HighWater, SimClock) {
    let (driver, high, sim, _) = stalled(law);
    (driver, high, sim)
}

fn stalled(law: ClockLaw) -> (Arc<FakeClock>, HighWater, SimClock, Stall) {
    let driver = Arc::new(FakeClock::new(Timestamp::from_unix_millis(1_000)));
    let high = HighWater::default();
    let stall = Stall::default();
    let sim = SimClock::new(Arc::clone(&driver), high.clone(), law, 7, stall.clone());
    (driver, high, sim, stall)
}

#[test]
fn per_tick_reads_return_the_drivers_clock_every_time() {
    let (driver, _, sim) = clock(ClockLaw::PerTick);
    assert_eq!(sim.now(), driver.now());
    assert_eq!(sim.now(), driver.now());
    sim.settle();
    assert_eq!(driver.now(), Timestamp::from_unix_millis(1_000));
}

#[test]
fn each_read_is_later_than_the_last_by_a_bounded_step() {
    let (driver, high, sim) = clock(ClockLaw::EachRead);
    let mut previous = driver.now();
    for _ in 0..200 {
        let now = sim.now();
        let step = now.unix_millis() - previous.unix_millis();
        assert!(step >= 1, "a read did not move: {previous:?} then {now:?}");
        assert!(step <= i64::try_from(MAX_STEP_MS).unwrap(), "step {step}");
        assert_eq!(high.at_least(driver.now()), now, "every read is noted");
        previous = now;
    }
    assert_eq!(
        driver.now(),
        Timestamp::from_unix_millis(1_000),
        "reads leave the driver be"
    );
    sim.settle();
    assert_eq!(driver.now(), previous, "settling catches the driver up");
}

#[test]
fn a_jump_back_between_ticks_reaches_the_next_reads() {
    let (driver, _, sim) = clock(ClockLaw::EachRead);
    let read = sim.now();
    sim.settle();
    driver.set(Timestamp::from_unix_millis(500));
    let after = sim.now();
    assert!(
        after < read,
        "the jump back is seen: {after:?} after {read:?}"
    );
    driver.advance(Duration::from_secs(10));
    assert!(
        sim.now() >= driver.now(),
        "a driver ahead of the reads leads them"
    );
}
/// A stall inside a tick moves every later read of it on, under either law, leaves the driver
/// where the tick began until it settles, and then moves the driver past it.
#[test]
fn a_stall_moves_the_reads_after_it_and_the_driver_at_settle() {
    for law in [ClockLaw::PerTick, ClockLaw::EachRead] {
        let (driver, high, sim, stall) = stalled(law);
        let before = sim.now();
        stall.add(60_000);
        let after = sim.now();
        assert!(
            after.unix_millis() >= 61_000 && after > before,
            "{law:?}: {after:?} after {before:?}"
        );
        assert_eq!(driver.now(), Timestamp::from_unix_millis(1_000), "{law:?}");
        assert_eq!(
            high.at_least(driver.now()),
            after,
            "{law:?}: the read is noted"
        );
        sim.settle();
        assert_eq!(
            driver.now(),
            after,
            "{law:?}: the driver ends at the last read"
        );
        assert_eq!(stall.spent(), 0, "{law:?}: the next tick starts unstalled");
        assert!(sim.now() >= after, "{law:?}: time does not run back");
    }
}

/// A pause fires at the read it was armed for, counted after the tick's own reading, and once.
#[test]
fn a_pause_fires_at_its_read_once() {
    let (driver, _, sim, _) = stalled(ClockLaw::PerTick);
    sim.pause_at(2, 3_600_000);
    let tick = sim.now();
    let first = sim.now();
    assert_eq!(
        first, tick,
        "the first read of the code is before the pause"
    );
    let second = sim.now();
    assert_eq!(second.unix_millis(), tick.unix_millis() + 3_600_000);
    assert_eq!(sim.now(), second, "the pause fires once");
    assert_eq!(sim.paused(), 1);
    sim.settle();
    assert_eq!(driver.now(), second);
}

/// A tick with fewer reads than the pause waits for is not paused, and the pause goes with it.
#[test]
fn a_pause_past_the_ticks_reads_is_dropped() {
    let (driver, _, sim, _) = stalled(ClockLaw::PerTick);
    sim.pause_at(5, 1_000);
    let _ = sim.now();
    sim.settle();
    for _ in 0..10 {
        assert_eq!(sim.now(), driver.now());
    }
    assert_eq!(sim.paused(), 0);
}
