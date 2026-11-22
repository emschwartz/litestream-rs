use std::sync::Arc;
use std::time::Duration;

use lsr_core::{Clock, FakeClock};
use lsr_ltx::Timestamp;

use super::{ClockLaw, MAX_STEP_MS, SimClock};
use crate::store::HighWater;

fn clock(law: ClockLaw) -> (Arc<FakeClock>, HighWater, SimClock) {
    let driver = Arc::new(FakeClock::new(Timestamp::from_unix_millis(1_000)));
    let high = HighWater::default();
    let sim = SimClock::new(Arc::clone(&driver), high.clone(), law, 7);
    (driver, high, sim)
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
