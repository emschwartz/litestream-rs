//! What one judge catches alone, from the swarm, with nothing written for the bug: seeds from
//! `LSR_SIM_BASE_SEED` (default 1) up to `LSR_SIM_SEEDS` of them (default 3000), each 300 steps
//! under its seed's mix (`Weights::mixed`), with only the judges `LSR_SIM_JUDGES` names
//! (`history`, `oracle` or `both`); with `LSR_SIM_BLOCK_LAW` set, every seed's power losses
//! follow the block crash law instead of half of them. Prints every failing seed and the number of seeds run to
//! the first. The generator, weights and step count are the soak's, fixed here so a count of
//! seeds to first failure compares across trees.

use lsr_sim::{Judges, Opts, Seed, Weights, replay_line, run_with};

const STEPS: u32 = 300;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map_or(default, |v| v.parse().expect("a number"))
}

#[test]
#[ignore = "runs LSR_SIM_SEEDS seeds; set LSR_SIM_JUDGES"]
fn seeds_to_first_failure() {
    let judges = match std::env::var("LSR_SIM_JUDGES").as_deref() {
        Ok("history") => Judges {
            oracle: false,
            history: true,
        },
        Ok("oracle") => Judges {
            oracle: true,
            history: false,
        },
        Ok("both") | Err(_) => Judges::default(),
        Ok(other) => panic!("LSR_SIM_JUDGES is history, oracle or both, not {other}"),
    };
    let (base, seeds) = (env("LSR_SIM_BASE_SEED", 1), env("LSR_SIM_SEEDS", 3000));
    let stop_at_first = std::env::var("LSR_SIM_ALL").is_err();
    let block_law = std::env::var("LSR_SIM_BLOCK_LAW").is_ok();
    let opts = Opts {
        judges,
        ..Opts::default()
    };
    let mut failed = Vec::new();
    for n in base..base.saturating_add(seeds) {
        let seed = Seed::new(n);
        let mut weights = Weights::mixed(seed);
        weights.block_crash_law |= block_law;
        if let Err(failure) = run_with(seed, STEPS, &weights, &opts) {
            println!(
                "FAILED after {} seeds: {failure}",
                n.saturating_sub(base).saturating_add(1)
            );
            println!("{}", replay_line(seed, Some(failure.steps)));
            failed.push(n);
            if stop_at_first {
                break;
            }
        }
    }
    println!(
        "{judges:?}: {} failed of seeds from {base}: {failed:?}",
        failed.len()
    );
}
