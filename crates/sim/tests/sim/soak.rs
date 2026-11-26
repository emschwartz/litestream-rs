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
//! The soak is also accountable for what it reached: every passing seed's
//! reach is summed, written as JSON to `LSR_SIM_REACH_OUT` when it is set
//! (the nightly workflow uploads it), and judged by [`lsr_sim::marks::gate`].
//! A fault class's required count that no seed reached fails the soak as a
//! failing seed does. The marks too rare for the nightly's seeds to reach reliably
//! come from `lsr_sim::scenario::coverage`, run once ahead of the seeds; a scenario that stops
//! reaching its mark fails the soak too.
//!
//! `long_seeds` is the same soak at the large profile (`docs/decisions/D277.md`): long
//! histories, larger transactions, the nightly's laws, seeds from the upper half of the night's
//! range, and `LSR_SIM_SHARD=i/n` so n jobs run disjoint seeds. It is not gated: a shard runs
//! tens of seeds, too few to reach every row, and the rotating set already gates the rows. Its
//! reach is written beside the others' so the shards can be summed by hand.

use std::time::{Duration, Instant};

use lsr_sim::scenario::coverage;
use lsr_sim::{Reach, Seed, Shard, Weights, long_base, nightly_base, replay_line, run};

const STEPS: u32 = 300;

/// The large profile's step count (`docs/decisions/D277.md`).
const LONG_STEPS: u32 = 5_000;

/// The replay line of a soak run: the soak sets the nightly's laws, so its replay must too.
fn replay(seed: Seed, steps: u32) -> String {
    format!("LSR_SIM_NIGHTLY_LAWS=1 {}", replay_line(seed, Some(steps)))
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
    let mut ran = 0_u64;
    let mut reach = Reach::default();
    // The marks a thousand random seeds do not reliably reach, from short explicit histories.
    let mut unreached = Vec::new();
    for scenario in coverage() {
        match scenario.run() {
            Ok(reached) => reach.merge(&reached),
            Err(why) => {
                println!("{why}");
                unreached.push(why);
            }
        }
    }
    for n in base.. {
        if Instant::now() >= until {
            break;
        }
        let seed = Seed::new(n);
        println!("{}", replay(seed, STEPS));
        let weights = Weights::mixed(seed).with_nightly_laws(seed);
        let outcome = run(seed, STEPS, &weights);
        if let Ok(report) = &outcome {
            reach.merge(&report.reach);
        }
        if let Err(failure) = outcome {
            println!("{failure}");
            failed.push(failure);
        }
        ran += 1;
    }
    println!("ran {ran} seeds from {base}, {} failed", failed.len());
    // Last, so the failures are the end of a log that named every seed.
    for failure in &failed {
        println!("FAILED {failure}");
    }
    let lines: Vec<String> = failed
        .iter()
        .map(|failure| replay(failure.seed, failure.steps))
        .collect();
    let gate = lsr_sim::marks::gate(&reach);
    println!("gate zeros: {:?}", gate.zeros);
    println!("gate stale exemptions: {:?}", gate.stale);
    println!("gate undrawn classes drawn: {:?}", gate.drawn_undrawn);
    write_reach("live", STEPS, base, ran, &reach, Some(&gate));
    assert!(failed.is_empty(), "failing seeds:\n{}", lines.join("\n"));
    assert!(
        unreached.is_empty(),
        "coverage scenarios that no longer reach their marks:\n{}",
        unreached.join("\n")
    );
    assert!(
        gate.passed(),
        "the soak left required marks unreached: {gate:?}"
    );
}

/// The large profile's replay line: the soak's laws and sizes, so a replay draws the same run.
fn replay_long(seed: Seed) -> String {
    format!("LSR_SIM_LARGE=1 {}", replay(seed, LONG_STEPS))
}

/// The base of a long run: `LSR_SIM_DAY` names the upper half of a night's range, else
/// `LSR_SIM_BASE_SEED` is the first seed itself.
fn long_start() -> u64 {
    match std::env::var("LSR_SIM_DAY") {
        Ok(day) => long_base(&day).expect("LSR_SIM_DAY is a date, YYYYMMDD"),
        Err(_) => base(),
    }
}

#[test]
#[ignore = "runs for LSR_SIM_SECONDS; use `just sim-long`"]
#[expect(
    clippy::disallowed_methods,
    reason = "the soak runs for a wall-clock budget; each run uses simulated time"
)]
fn long_seeds() {
    let base = long_start();
    let shard = Shard::parse(&std::env::var("LSR_SIM_SHARD").unwrap_or_else(|_| "0/1".to_owned()))
        .expect("LSR_SIM_SHARD is INDEX/COUNT");
    let seconds: u64 = std::env::var("LSR_SIM_SECONDS")
        .expect("LSR_SIM_SECONDS")
        .parse()
        .expect("LSR_SIM_SECONDS is a number");
    let until = Instant::now() + Duration::from_secs(seconds);
    let mut failed = Vec::new();
    let mut ran = 0_u64;
    let mut reach = Reach::default();
    for seed in shard.seeds(base) {
        if Instant::now() >= until {
            break;
        }
        println!("{}", replay_long(seed));
        let weights = Weights::mixed(seed)
            .with_nightly_laws(seed)
            .with_large_sizes();
        match run(seed, LONG_STEPS, &weights) {
            Ok(report) => reach.merge(&report.reach),
            Err(failure) => {
                println!("{failure}");
                failed.push(failure);
            }
        }
        ran += 1;
    }
    println!(
        "ran {ran} seeds of {shard:?} from {base}, {} failed",
        failed.len()
    );
    for failure in &failed {
        println!("FAILED {failure}");
    }
    let lines: Vec<String> = failed
        .iter()
        .map(|failure| replay_long(failure.seed))
        .collect();
    write_reach("live-large", LONG_STEPS, base, ran, &reach, None);
    assert!(failed.is_empty(), "failing seeds:\n{}", lines.join("\n"));
}

/// Writes the summed reach where `LSR_SIM_REACH_OUT` names: the commit
/// (`LSR_SIM_COMMIT`), the profile, the seeds run and every key, so nights can be
/// compared; with the gate's verdict when the soak has one.
fn write_reach(
    profile: &str,
    steps: u32,
    base: u64,
    ran: u64,
    reach: &Reach,
    gate: Option<&lsr_sim::marks::Gate>,
) {
    let Ok(path) = std::env::var("LSR_SIM_REACH_OUT") else {
        return;
    };
    let keys: serde_json::Map<String, serde_json::Value> = reach
        .listing()
        .into_iter()
        .chain(
            lsr_sim::marks::required_keys()
                .into_iter()
                .map(|key| (key, 0)),
        )
        .map(|(key, n)| (key.clone(), reach.get(&key).max(n).into()))
        .collect();
    let out = serde_json::json!({
        "commit": std::env::var("LSR_SIM_COMMIT").unwrap_or_default(),
        "generator": profile,
        "steps": steps,
        "base_seed": base,
        "shard": std::env::var("LSR_SIM_SHARD").unwrap_or_default(),
        "seeds_run": ran,
        "gate": gate.map(|gate| serde_json::json!({
            "passed": gate.passed(),
            "zeros": gate.zeros,
            "stale_exemptions": gate.stale,
            "undrawn_drawn": gate.drawn_undrawn,
        })),
        "reach": keys,
    });
    let text = serde_json::to_string_pretty(&out).expect("the reach serializes");
    std::fs::write(&path, text).expect("LSR_SIM_REACH_OUT is writable");
    println!("reach written to {path}");
}
