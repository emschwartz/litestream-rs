//! What one judge catches alone, from the swarm, with nothing written for the bug: seeds from
//! `LSR_SIM_BASE_SEED` (default 1) up to `LSR_SIM_SEEDS` of them (default 3000), each 300 steps
//! under its seed's mix (`Weights::mixed`), with only the judges `LSR_SIM_JUDGES` names
//! (`history`, `oracle` or `both`); with `LSR_SIM_BLOCK_LAW` set, every seed's power losses
//! follow the block crash law instead of half of them; with `LSR_SIM_AFTER_RESTART` set (`redo`,
//! `busy` or `nothing`), every seed's app does that after a restart instead of what its mix drew.
//! Prints every failing seed and the number of seeds run to
//! the first. The generator, weights and step count are the soak's, fixed here so a count of
//! seeds to first failure compares across trees.

use lsr_sim::{AfterRestart, Judges, Opts, Seed, Weights, replay_line, run_with};

const STEPS: u32 = 300;

/// What the app's after-restart behaviours reached, summed over the passing runs.
const REACHED: [&str; 5] = [
    "app.redo",
    "app.redo_after_loss",
    "app.redo_past_lost_end",
    "app.busy",
    "app.busy_past_end",
];

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map_or(default, |v| v.parse().expect("a number"))
}

/// `seed`'s mix, with the overrides `LSR_SIM_BLOCK_LAW` and `LSR_SIM_AFTER_RESTART` name.
pub fn weights(seed: Seed) -> Weights {
    let mut weights = Weights::mixed(seed);
    weights.block_crash_law |= std::env::var("LSR_SIM_BLOCK_LAW").is_ok();
    match std::env::var("LSR_SIM_AFTER_RESTART").as_deref() {
        Err(_) => {}
        Ok("redo") => weights.after_restart = AfterRestart::Redo,
        Ok("busy") => weights.after_restart = AfterRestart::Busy,
        Ok("nothing") => weights.after_restart = AfterRestart::Nothing,
        Ok(other) => panic!("LSR_SIM_AFTER_RESTART is redo, busy or nothing, not {other}"),
    }
    weights
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
    let opts = Opts {
        judges,
        ..Opts::default()
    };
    let mut failed = Vec::new();
    let mut reached = [0_u64; REACHED.len()];
    let mut losses = 0_u64;
    for n in base..base.saturating_add(seeds) {
        let seed = Seed::new(n);
        let weights = weights(seed);
        let ran = run_with(seed, STEPS, &weights, &opts);
        if let Ok(report) = &ran {
            for (sum, key) in reached.iter_mut().zip(REACHED) {
                *sum += report.reach.get(key);
            }
            losses += u64::from(report.power_losses);
        }
        if let Err(failure) = ran {
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
    let reached: Vec<_> = REACHED.iter().zip(reached).collect();
    println!("passing runs: {losses} power losses; {reached:?}");
    println!(
        "{judges:?}: {} failed of seeds from {base}: {failed:?}",
        failed.len()
    );
}
