//! The block crash law's campaign: `LSR_SIM_SEEDS` seeds from `LSR_SIM_BASE_SEED` under their
//! rotating mix (`Weights::mixed`), each printed with whether its power losses follow the block
//! law and how the run ended, then a summary: seeds run, seeds under the law, power losses that
//! kept a block after one they lost, and the first failing seed. Run by hand; the law and its
//! swarm weight were frozen before the first run against an engine with a known defect.

use lsr_sim::{Seed, Weights, replay_line, run};

const STEPS: u32 = 300;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore = "a campaign; run by hand"]
fn block_law_campaign() {
    let (base, seeds) = (env("LSR_SIM_BASE_SEED", 1), env("LSR_SIM_SEEDS", 500));
    let (mut under_law, mut holes, mut losses) = (0_u64, 0_u64, 0_u64);
    let mut failed = Vec::new();
    for n in base..base + seeds {
        let seed = Seed::new(n);
        let weights = Weights::mixed(seed);
        under_law += u64::from(weights.block_crash_law);
        match run(seed, STEPS, &weights) {
            Ok(report) => {
                holes += report.reach.get("power.block_hole");
                losses += u64::from(report.power_losses);
                println!("sim-crash seed {n} law {} ok", weights.block_crash_law);
            }
            Err(failure) => {
                println!("sim-crash seed {n} law {} FAILED", weights.block_crash_law);
                println!("{}\n{failure}", replay_line(seed, Some(STEPS)));
                failed.push((n, weights.block_crash_law));
            }
        }
    }
    println!(
        "ran {seeds} seeds from {base}: {under_law} under the block law, {losses} power losses \
         in passing runs, {holes} of them kept a block after a lost one; failed {failed:?}"
    );
    assert!(failed.is_empty(), "{failed:?}");
}
