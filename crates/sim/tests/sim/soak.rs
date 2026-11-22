//! The rotating seed set: runs seeds from a base until the time is up.
//!
//! `just sim-soak <seconds>` sets `LSR_SIM_BASE_SEED` (default: `date +%s`)
//! and `LSR_SIM_SECONDS`. `just sim-nightly` sets `LSR_SIM_DAY` instead: the
//! nightly workflow's seeds follow from the UTC date ([`lsr_sim::nightly_base`]),
//! so each night runs seeds no other night does and any night is rerun from
//! its date. Every seed's replay line is printed before the run, so a hang or
//! a crash of the process still names its seed, and every failure is printed
//! again at the end with its replay line. A failing seed belongs in the fixed
//! set once the bug is fixed, as its list of operations: replayed by seed, it
//! would stop reproducing the bug the next time the generator changes.
//!
//! The soak draws the error domain's known-wrong cells too (`Weights::known_wrong`): a failure
//! that matches the signature of a defect its history drew is printed and counted as known, and
//! fails nothing, so the other failures of those seeds stay visible
//! (`lsr_sim::domain::KnownWrong`).

use std::time::{Duration, Instant};

use std::collections::BTreeMap;

use lsr_sim::domain::{KnownWrong, known_in};
use lsr_sim::{Seed, Weights, history, nightly_base, replay_line, run};

const STEPS: u32 = 300;

/// The replay line of a soak run: the soak draws the known-wrong cells, so its replay must too.
fn replay(seed: Seed, steps: u32) -> String {
    format!("LSR_SIM_KNOWN_WRONG=1 {}", replay_line(seed, Some(steps)))
}

/// The base seed: `LSR_SIM_DAY` (a UTC date, `YYYYMMDD`) names a night's
/// range, else `LSR_SIM_BASE_SEED` is the seed itself.
fn base() -> u64 {
    match std::env::var("LSR_SIM_DAY") {
        Ok(day) => nightly_base(&day).expect("LSR_SIM_DAY is a date, YYYYMMDD"),
        Err(_) => std::env::var("LSR_SIM_BASE_SEED")
            .expect("LSR_SIM_BASE_SEED or LSR_SIM_DAY")
            .parse()
            .expect("LSR_SIM_BASE_SEED is a number"),
    }
}

#[test]
#[ignore = "runs for LSR_SIM_SECONDS; use `just sim-soak` or `just sim-nightly`"]
#[expect(
    clippy::disallowed_methods,
    reason = "the soak runs for a wall-clock budget; each run uses simulated time"
)]
fn rotating_seeds() {
    let base = base();
    let seconds: u64 = std::env::var("LSR_SIM_SECONDS")
        .expect("LSR_SIM_SECONDS")
        .parse()
        .expect("LSR_SIM_SECONDS is a number");
    let until = Instant::now() + Duration::from_secs(seconds);
    let mut failed = Vec::new();
    let mut known = BTreeMap::<String, u64>::new();
    let mut ran = 0_u64;
    for n in base.. {
        if Instant::now() >= until {
            break;
        }
        let seed = Seed::new(n);
        println!("{}", replay(seed, STEPS));
        let mut weights = Weights::mixed(seed);
        weights.known_wrong = true;
        if let Err(failure) = run(seed, STEPS, &weights) {
            let drawn = known_in(&history(seed, STEPS, &weights));
            if let Some(defect) = KnownWrong::of(&failure.violation, &drawn) {
                println!("KNOWN {defect:?} ({}): {failure}", defect.tracking());
                *known.entry(format!("{defect:?}")).or_default() += 1;
            } else {
                println!("{failure}");
                failed.push(failure);
            }
        }
        ran += 1;
    }
    println!("ran {ran} seeds from {base}, {} failed", failed.len());
    println!("known defects reached: {known:?}");
    // Last, so the failures are the end of a log that named every seed.
    for failure in &failed {
        println!("FAILED {failure}");
    }
    let lines: Vec<String> = failed
        .iter()
        .map(|failure| replay(failure.seed, failure.steps))
        .collect();
    assert!(failed.is_empty(), "failing seeds:\n{}", lines.join("\n"));
}
