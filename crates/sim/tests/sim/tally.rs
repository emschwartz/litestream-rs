//! The reachability tally: what random histories reach, summed over seeds.
//!
//! `just sim-tally [seeds] [base]` sets `LSR_SIM_TALLY_SEEDS` and
//! `LSR_SIM_BASE_SEED` and prints every count of [`lsr_sim::Reach`], one per
//! line, then the failures. A state that shows zero is one no random history
//! reaches; see `crates/sim/src/reach.rs`. Every required and watched key is
//! printed, zeros included, and the fixed seed set must reach every required one.

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
    // The sums below are of the seeds alone; the gate's verdict adds the coverage scenarios the
    // soak adds, so its zeros are the soak's.
    let mut sum = Reach::default();
    let mut failed = Vec::new();
    for n in base..base.saturating_add(seeds) {
        let seed = Seed::new(n);
        let mut weights = Weights::mixed(seed);
        if std::env::var("LSR_SIM_NIGHTLY_LAWS").is_ok() {
            weights = weights.with_nightly_laws(seed);
        }
        match run(seed, STEPS, &weights) {
            Ok(report) => sum.merge(&report.reach),
            Err(failure) => {
                println!("FAILED {failure}");
                failed.push(replay_line(failure.seed, Some(failure.steps)));
            }
        }
    }
    for (key, n) in sum.listing() {
        println!("{key:<48} {n}");
    }
    println!("{seeds} seeds from {base}, {} failed", failed.len());
    println!("required states at zero: {:?}", sum.missing());
    for scenario in lsr_sim::scenario::coverage() {
        match scenario.run() {
            Ok(reached) => sum.merge(&reached),
            Err(why) => println!("FAILED coverage scenario {why}"),
        }
    }
    let gate = lsr_sim::marks::gate(&sum);
    println!("gate zeros: {:?}", gate.zeros);
    println!("gate stale exemptions: {:?}", gate.stale);
    println!("gate undrawn classes drawn: {:?}", gate.drawn_undrawn);
    assert!(failed.is_empty(), "failing seeds:\n{}", failed.join("\n"));
}

/// Seeds to the first failing run, for the planted-bug ledger's unaided mode
/// (`scripts/planted-bugs.py --unaided`): runs `LSR_SIM_TALLY_SEEDS` seeds from
/// `LSR_SIM_BASE_SEED` under their swarm mixes and prints
/// `first-failure: <k> <seed>`, the count of seeds run up to and including the
/// first that fails, or `first-failure: none <seeds>`. A panic counts as a
/// failure. It passes either way: the line is the result.
#[test]
#[ignore = "runs LSR_SIM_TALLY_SEEDS seeds; use `just planted-bugs --unaided`"]
fn first_failure() {
    let seeds: u64 = std::env::var("LSR_SIM_TALLY_SEEDS")
        .expect("LSR_SIM_TALLY_SEEDS")
        .parse()
        .expect("LSR_SIM_TALLY_SEEDS is a number");
    let base: u64 = std::env::var("LSR_SIM_BASE_SEED")
        .expect("LSR_SIM_BASE_SEED")
        .parse()
        .expect("LSR_SIM_BASE_SEED is a number");
    for (k, n) in (base..base.saturating_add(seeds)).enumerate() {
        let seed = Seed::new(n);
        let outcome = std::panic::catch_unwind(|| {
            run(seed, STEPS, &Weights::mixed(seed)).map_err(|failure| failure.to_string())
        });
        let failed = match outcome {
            Ok(Ok(_)) => continue,
            Ok(Err(failure)) => failure,
            Err(_) => "the run panicked".to_owned(),
        };
        println!("first-failure: {} {n:#x}", k.saturating_add(1));
        println!("{failed}");
        return;
    }
    println!("first-failure: none {seeds}");
}

/// The fixed seed set, seeds 1 to 64 under their swarm mixes, reaches every
/// state in [`lsr_sim::reach::REQUIRED`]. A generator change that stops
/// reaching one fails here, with every required and watched count printed.
#[test]
fn the_fixed_seeds_reach_every_required_state() {
    let mut sum = Reach::default();
    for n in 1..=64 {
        let seed = Seed::new(n);
        if let Ok(report) = run(seed, STEPS, &Weights::mixed(seed)) {
            sum.merge(&report.reach);
        }
        // A failing seed is `fixed_seeds`'s to report; its reach is lost here.
    }
    let missing = sum.missing();
    if !missing.is_empty() {
        for (key, n) in sum.listing() {
            println!("{key:<48} {n}");
        }
    }
    assert!(
        missing.is_empty(),
        "required states no fixed seed reaches: {missing:?}"
    );
}
