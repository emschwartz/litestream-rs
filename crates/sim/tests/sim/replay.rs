//! Replays one run, printing every operation.
//!
//! `just sim-replay 0x<seed> [steps]` sets `LSR_SIM_SEED` and `LSR_SIM_STEPS`
//! and runs this test with output shown, under the seed's mix
//! (`Weights::mixed`), as the fixed and rotating sets run it, with the overrides of
//! `unaided::weights` (`LSR_SIM_BLOCK_LAW`, `LSR_SIM_AFTER_RESTART`, `LSR_SIM_SWARM`,
//! `LSR_SIM_HOLD`). Without `LSR_SIM_SEED` the test does
//! nothing, so `just test` stays quiet.

use lsr_sim::{Opts, Seed, run_with};

const DEFAULT_STEPS: u32 = 300;

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
    match run_with(seed, steps, &crate::unaided::weights(seed), &opts) {
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
