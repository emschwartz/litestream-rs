//! What one judge catches alone, from the swarm, with nothing written for the bug: seeds from
//! `LSR_SIM_BASE_SEED` (default 1) up to `LSR_SIM_SEEDS` of them (default 3000), each 300 steps
//! under its seed's mix (`Weights::mixed`), with only the judges `LSR_SIM_JUDGES` names
//! (`history`, `oracle` or `both`); with `LSR_SIM_BLOCK_LAW` set, every seed's power losses
//! follow the block crash law instead of half of them; with `LSR_SIM_AFTER_RESTART` set (`redo`,
//! `busy` or `nothing`), every seed's app does that after a restart instead of what its mix drew;
//! with `LSR_SIM_NO_WAL_DAMAGE` set, no seed draws WAL damage; with `LSR_SIM_LYING_SYNC` set,
//! every seed's disk lies about the engine's WAL fsync instead of what its mix drew; with
//! `LSR_SIM_APP_IN_TICK` set to a weight, every seed draws app operations inside a tick at it; with
//! `LSR_SIM_SWARM` set, every seed runs its swarm mix (`Weights::swarm`) instead, and with
//! `LSR_SIM_DEFAULT` set the default mix; with `LSR_SIM_HOLD` set to a rate in a thousand, holds
//! are drawn at least that often (`tests/sim/survival.rs`); with `LSR_SIM_KNOWN_WRONG` set, the
//! error domain's known-wrong cells are drawn too and a failure matching a drawn defect's
//! signature is printed as known and not counted as failed (`lsr_sim::domain::KnownWrong`).
//! Prints every failing seed and the number of seeds run to
//! the first. The generator, weights and step count are the soak's, fixed here so a count of
//! seeds to first failure compares across trees.
//!
//! The redo-world campaign's budget, fixed after the app's after-restart behaviours were frozen
//! (`Weights::after_restart`, `AfterRestart`) and before their first campaign: seeds 1..=3000
//! of 300 steps for each simulator configuration, and on each tree the crash-cycle rig
//! (`crash_cycles::crash_cycle_campaign`, seeds 0..100 of 12 cycles) and the history rig
//! (`rig::real_power_loss_trials`, seeds 1..=50 of 6 cycles).
//!
//! | tree | simulator configurations |
//! |---|---|
//! | main | as drawn, both judges; redo on every seed and the block law, both judges; busy on every seed and the block law, both judges; every failure listed |
//! | main + the planted bug "the boot anchor accepts any tail in the live generation" | redo and the block law, the history judge alone; the same, the oracle alone; as drawn, the history judge alone; to the first failure |
//! | main without the WAL barrier (a scratch revert) | redo and the block law, the history judge alone; busy and the block law, the history judge alone; redo and the block law, the oracle alone; to the first failure |
//!
//! The retry campaign's budget, fixed after `AfterRestart::Retry` (one draw apiece per replayed
//! commit, verbatim or restamped, from its own stream; drawn last in the swarm as half of the
//! redoing apps) and `Weights::lying_wal_sync` (drawn after it, a quarter of swarm seeds) were
//! frozen, and before their first campaign together: seeds 1..=3000 of 300 steps. (One run of
//! the retry law alone, without a lying disk, went first: the plant below survived it.)
//!
//! | tree | simulator configurations |
//! |---|---|
//! | main | retry, the block law and a lying disk on every seed, both judges; every failure listed |
//! | main + the planted bug "the boot witness anchors on any tail it decodes" (`docs/planted-bugs/witness-anchor-held.patch`) | retry, the block law and a lying disk, the history judge alone; the same, the oracle alone; as drawn, both judges; to the first failure |
//!
//! Results: main failed none of 3000. The plant survived all three configurations (3000 seeds
//! each), and the retry law without a lying disk before them. With the plant, 1 seed in 3000
//! (126, under the history configuration) had the boot witness accept a pending file on the
//! anchor alone; a staging fault four steps later forced a snapshot over it.

use lsr_sim::domain::{KnownWrong, known_in};
use lsr_sim::{AfterRestart, Judges, Opts, Seed, Weights, history, replay_line, run_with};

const STEPS: u32 = 300;

/// What the app's after-restart behaviours reached, and how often the history judge let a
/// regression through (by a rewind the engine reported, by the driver's excuse, and excused
/// steps the engine never reported), and how many staged files an undone unlink brought back,
/// summed over the passing runs.
const REACHED: [&str; 11] = [
    "app.redo",
    "app.redo_after_loss",
    "app.redo_past_lost_end",
    "app.retry",
    "app.retry_restamped",
    "app.busy",
    "app.busy_past_end",
    "history.rewound",
    "history.excused",
    "history.excused.unreported",
    "staged.Restored",
];

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map_or(default, |v| v.parse().expect("a number"))
}

/// `seed`'s mix, with the overrides `LSR_SIM_BLOCK_LAW` and `LSR_SIM_AFTER_RESTART` name, and
/// with no WAL damage drawn when `LSR_SIM_NO_WAL_DAMAGE` is set: damage regrows the WAL with
/// other bytes by fiat, so leaving it out shows what the application's own behaviour reaches.
pub fn weights(seed: Seed) -> Weights {
    let mut weights = if std::env::var("LSR_SIM_SWARM").is_ok() {
        Weights::swarm(seed)
    } else if std::env::var("LSR_SIM_DEFAULT").is_ok() {
        Weights::default()
    } else {
        Weights::mixed(seed)
    };
    if let Ok(hold) = std::env::var("LSR_SIM_HOLD") {
        weights.hold = weights
            .hold
            .max(hold.parse().expect("LSR_SIM_HOLD is a rate in 1000"));
    }
    weights.block_crash_law |= std::env::var("LSR_SIM_BLOCK_LAW").is_ok();
    weights.known_wrong |= std::env::var("LSR_SIM_KNOWN_WRONG").is_ok();
    weights.lying_wal_sync |= std::env::var("LSR_SIM_LYING_SYNC").is_ok();
    if let Ok(weight) = std::env::var("LSR_SIM_APP_IN_TICK") {
        weights.app_in_tick = weight.parse().expect("LSR_SIM_APP_IN_TICK is a weight");
    }
    if std::env::var("LSR_SIM_NO_WAL_DAMAGE").is_ok() {
        weights.wal_damage = 0;
    }
    match std::env::var("LSR_SIM_AFTER_RESTART").as_deref() {
        Err(_) => {}
        Ok("redo") => weights.after_restart = AfterRestart::Redo,
        Ok("retry") => weights.after_restart = AfterRestart::Retry,
        Ok("busy") => weights.after_restart = AfterRestart::Busy,
        Ok("nothing") => weights.after_restart = AfterRestart::Nothing,
        Ok(other) => panic!("LSR_SIM_AFTER_RESTART is redo, retry, busy or nothing, not {other}"),
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
    let mut known = std::collections::BTreeMap::<String, Vec<u64>>::new();
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
            let drawn = known_in(&history(seed, STEPS, &weights), &weights);
            if let Some(defect) = KnownWrong::of(&failure.violation, &drawn) {
                println!("KNOWN {defect:?}: {failure}");
                known.entry(format!("{defect:?}")).or_default().push(n);
                continue;
            }
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
    for (defect, seeds) in &known {
        println!(
            "known {defect}: {} seeds, first {:?}",
            seeds.len(),
            seeds.first()
        );
    }
    println!(
        "{judges:?}: {} failed of seeds from {base}: {failed:?}",
        failed.len()
    );
}

/// Found unaided by the swarm, with nothing written for it: under the planted bug "the boot
/// anchor accepts any tail in the live generation" (`docs/planted-bugs/anchor-any-tail.patch`),
/// seed 44's drawn mix, 8 steps in, restores a transaction to pages no commit had. (Found on
/// the merged head with the boot witness; seed 15 found it before the witness landed.) Kept as
/// the operations and settings that mix drew then: replayed by seed, the history moved when
/// the generator gained the scheduled verify.
#[test]
fn swarm_seed_44_restores_only_committed_states() {
    use lsr_sim::generate::{MetaOp, WalOp};
    use lsr_sim::{ClockLaw, Op};
    let ops = [
        Op::Meta(MetaOp::ForgeNewest),
        Op::Tick,
        Op::Spill,
        Op::WalDamage {
            op: WalOp::RegrowWithoutCommitMarks(8_063_039_911_997_408_971),
            ticks: 2,
        },
        Op::Shutdown,
        Op::Commit {
            pages: 4,
            grow: false,
        },
        Op::WalDamage {
            op: WalOp::RegrowSameSalts(13_577_244_702_264_426_995),
            ticks: 7,
        },
        Op::Restore,
    ];
    let opts = Opts {
        drawn: true,
        lying_wal_sync: true,
        after_restart: AfterRestart::Busy,
        clock_law: ClockLaw::EachRead,
        ..Opts::default()
    };
    if let Err(failure) = lsr_sim::run::run_ops(Seed::new(44), &ops, &opts) {
        panic!("{failure}");
    }
}

/// Found unaided once the app could act inside a tick (`Op::AppInTick`): seed 0xf488c's drawn
/// mix commits between the engine's last sync step and its TRUNCATE, and the boundary snapshot
/// took that commit's pages for damage (`DatabasePageMismatch`) until it stopped checking them
/// (docs/decisions.md D121, plant `boundary-snapshot-keeps-page-table`). Seed 0xf49a2 found it
/// first, before main's later swarm draws reshuffled the mixes. Main's swarm, without these
/// operations, kills the plant on none of seeds 1000001 to 1002000.
#[test]
fn swarm_seed_0xf488c_takes_no_commit_before_a_truncate_for_damage() {
    // BATCH-3: main's overlap draw, which precedes the in-tick draw, moved this seed's mix, so
    // under plant boundary-snapshot-keeps-page-table the run now passes. Record the history
    // drawn at fe9badf2 as an op list, or find a new seed.
    let seed = Seed::new(0xf488c);
    let mut weights = Weights::mixed(seed);
    // The run as it was found: the scheduled verify, drawn last, came after it.
    weights.verify = 0;
    if let Err(failure) = run_with(seed, 299, &weights, &Opts::default()) {
        panic!("{failure}");
    }
}

/// Seed 0x753's swarm mix under the block crash law on a disk that lies about the engine's WAL
/// sync, with the app redoing its commits after a restart: staging is wiped, a power loss takes
/// WAL frames the replica already holds, the redo regrows them byte for byte, and the next boot
/// rightly continues on the replica's newest file. The history judge called the next `Latest`
/// an unreported step back, because it remembered the previous restore as the nodes that held
/// its content then, not the node the redo rebuilt with the same content later.
#[test]
fn swarm_seed_0x753_a_redo_that_rebuilds_the_restored_state_is_not_a_loss() {
    let seed = Seed::new(0x753);
    let mut weights = Weights::swarm(seed);
    weights.block_crash_law = true;
    if let Err(failure) = run_with(seed, 42, &weights, &Opts::default()) {
        panic!("{}", failure.message(failure.steps));
    }
}
