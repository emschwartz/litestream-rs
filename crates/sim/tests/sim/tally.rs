//! The reachability tally: what random histories reach, summed over seeds.
//!
//! `just sim-tally [seeds] [base]` sets `LSR_SIM_TALLY_SEEDS` and
//! `LSR_SIM_BASE_SEED` and prints every count of [`lsr_sim::Reach`], one per
//! line, then the failures. A state that shows zero is one no random history
//! reaches; see `crates/sim/src/reach.rs`.

use lsr_sim::{Reach, Seed, Weights, replay_line, run};

const STEPS: u32 = 300;

#[test]
#[ignore = "runs LSR_SIM_TALLY_SEEDS seeds; use `just sim-tally`"]
fn tally() {
    let seeds: u64 = std::env::var("LSR_SIM_TALLY_SEEDS")
        .expect("LSR_SIM_TALLY_SEEDS")
        .parse()
        .expect("LSR_SIM_TALLY_SEEDS is a number");
    let base: u64 = std::env::var("LSR_SIM_BASE_SEED")
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(1);
    let mut sum = Reach::default();
    let mut failed = Vec::new();
    for n in base..base.saturating_add(seeds) {
        let seed = Seed::new(n);
        match run(seed, STEPS, &Weights::mixed(seed)) {
            Ok(report) => sum.merge(&report.reach),
            Err(failure) => {
                println!("FAILED {failure}");
                failed.push(replay_line(failure.seed, Some(failure.steps)));
            }
        }
    }
    for (key, n) in sum.iter() {
        println!("{key:<48} {n}");
    }
    println!("{seeds} seeds from {base}, {} failed", failed.len());
    assert!(failed.is_empty(), "failing seeds:\n{}", failed.join("\n"));
}
