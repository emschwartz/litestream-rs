//! Replays one run, printing every operation.
//!
//! `just sim-replay 0x<seed> [steps]` sets `LSR_SIM_SEED` and `LSR_SIM_STEPS`
//! and runs this test with output shown, under the seed's mix
//! (`Weights::mixed`), as the fixed and rotating sets run it, with the overrides of
//! `unaided::weights` (`LSR_SIM_BLOCK_LAW`, `LSR_SIM_AFTER_RESTART`, `LSR_SIM_SWARM`,
//! `LSR_SIM_HOLD`, `LSR_SIM_NIGHTLY_LAWS`, which the rotating set's replay lines carry), or under
//! the default mix with `LSR_SIM_WEIGHTS=default` (what the unit
//! tests in `run` draw from). Without `LSR_SIM_SEED` the test does nothing, so `just test`
//! stays quiet.

use lsr_sim::{Opts, Seed, Weights, run_with};

const DEFAULT_STEPS: u32 = 300;

/// The mix the replay and `just sim-shrink` run `seed` under: `LSR_SIM_WEIGHTS=default`, or the
/// seed's own mix with the overrides of `unaided::weights`.
pub fn weights(seed: Seed) -> Weights {
    match std::env::var("LSR_SIM_WEIGHTS").as_deref() {
        Ok("default") => Weights::default(),
        Ok(other) => panic!("LSR_SIM_WEIGHTS is `default` or unset, not {other}"),
        Err(_) => crate::unaided::weights(seed),
    }
}

#[test]
fn replay_the_named_run() {
    let Ok(text) = std::env::var("LSR_SIM_SEED") else {
        return;
    };
    let seed: Seed = text.parse().expect("LSR_SIM_SEED is a hex seed");
    let steps = std::env::var("LSR_SIM_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_STEPS);
    let opts = Opts {
        plant: None,
        trace: true,
        ..Opts::default()
    };
    let weights = weights(seed);
    match run_with(seed, steps, &weights, &opts) {
        Ok(report) => {
            for line in &report.trace {
                println!("{line}");
            }
            println!("passed: {report:?}");
        }
        Err(failure) => {
            for line in &failure.trace {
                println!("{line}");
            }
            panic!("{}", failure.message(failure.steps));
        }
    }
}
