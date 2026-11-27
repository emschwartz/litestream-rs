//! Shrinks one failing run and prints it as an explicit scenario.
//!
//! `just sim-shrink 0x<seed> [steps]` sets `LSR_SIM_SEED` and `LSR_SIM_STEPS` and runs this test
//! with output shown, under the mix the replay uses (`replay::weights`, so the replay line's
//! overrides apply). It replays the seed, cuts the history down with `lsr_sim::shrink`
//! (`docs/decisions/D285.md`) and prints the result as Rust source to paste into a scenario test.
//! Without `LSR_SIM_SEED` the test does nothing, so `just test` stays quiet; a seed that passes
//! prints that and stops.

use lsr_sim::{Opts, Seed, scenario_source, shrink};

const DEFAULT_STEPS: u32 = 300;

#[test]
fn shrink_the_named_run() {
    let Ok(text) = std::env::var("LSR_SIM_SEED") else {
        return;
    };
    let seed: Seed = text.parse().expect("LSR_SIM_SEED is a hex seed");
    let steps = std::env::var("LSR_SIM_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_STEPS);
    let Some(shrunk) = shrink(seed, steps, &crate::replay::weights(seed), &Opts::default()) else {
        println!("seed 0x{seed} passes in {steps} steps: nothing to shrink");
        return;
    };
    match scenario_source(seed, &shrunk) {
        Ok(source) => println!("{source}"),
        Err(unprintable) => println!("{unprintable}; the operations as Debug:\n{:#?}", shrunk.ops),
    }
}
