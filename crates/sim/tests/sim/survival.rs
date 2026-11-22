//! Restores judged while a fault still holds (`Op::Hold`), not only after it heals.
//!
//! Every hold judges the replica in the middle of its ticks and at their end, before the fault
//! is lifted: both judges over the raw replica, and, for the faults a restore itself meets,
//! restores made through the fault, each held to the raw replica's answer (see
//! `crates/sim/src/run/hold.rs`).
//!
//! The campaign's budget, registered before its first run, in the commit that added this file:
//!
//! | configuration | seeds | steps | judges |
//! |---|---|---|---|
//! | the default mix with holds at 40 in a thousand steps | 1..=2000 | 300 | both, every failure listed |
//! | each seed's swarm mix (`Weights::swarm`) with holds at no less than 20 in a thousand | 1..=2000 | 300 | both, every failure listed |
//!
//! Each failure is triaged as an engine defect, a checker defect, or the fault's own doing, and
//! the counts are reported with the change. The budget does not grow after a result is seen.
//! A failure prints its replay line; replay it under the campaign's mix with `LSR_SIM_DEFAULT=1
//! LSR_SIM_HOLD=40` (the default mix) or `LSR_SIM_SWARM=1 LSR_SIM_HOLD=20` (the swarm) set.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use lsr_sim::generate::WalOp;
use lsr_sim::run::run_ops;
use lsr_sim::{Hold, Op, Opts, RunReport, Seed, Weights, replay_line, run};

const STEPS: u32 = 300;

/// A written history: the app commits and the engine uploads, then `hold` lasts six ticks with
/// commits under it, then the run heals and settles.
fn under(hold: Hold) -> Vec<Op> {
    let mut ops = Vec::new();
    for _ in 0..6 {
        ops.push(Op::Commit {
            pages: 2,
            grow: true,
        });
        ops.push(Op::Tick);
        ops.push(Op::Tick);
    }
    ops.push(Op::Hold { hold, ticks: 6 });
    for _ in 0..6 {
        ops.push(Op::Commit {
            pages: 1,
            grow: false,
        });
        ops.push(Op::Tick);
    }
    ops.push(Op::Restore);
    ops
}

fn held(hold: Hold) -> RunReport {
    match run_ops(Seed::new(7), &under(hold), &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{hold:?}: {failure}"),
    }
}

/// The key a hold's reach is counted under.
fn key(hold: Hold) -> String {
    match hold {
        Hold::StoreDown(op) => format!("hold.store_down.{op:?}"),
        Hold::CredentialsRevoked => "hold.CredentialsRevoked".into(),
        Hold::DiskFull => "hold.DiskFull".into(),
        Hold::LaggingListing => "hold.LaggingListing".into(),
        Hold::LyingReads => "hold.LyingReads".into(),
        Hold::ReadersHeld => "hold.ReadersHeld".into(),
        Hold::Wal(op) => {
            let name = format!("{op:?}");
            let name = name.split('(').next().unwrap_or_default().to_owned();
            format!("hold.wal.{name}")
        }
        Hold::LyingDisk => "hold.LyingDisk".into(),
    }
}

#[test]
fn a_store_down_for_each_operation_leaves_restorable_files_and_restores_fail_cleanly() {
    for op in [
        lsr_core::StoreOp::List,
        lsr_core::StoreOp::Read,
        lsr_core::StoreOp::Upload,
        lsr_core::StoreOp::Delete,
    ] {
        let hold = Hold::StoreDown(op);
        let report = held(hold);
        assert_eq!(report.reach.get(&format!("{}.judged", key(hold))), 2);
    }
}

#[test]
fn revoked_credentials_degrade_and_restores_meanwhile_are_refused_or_exact() {
    let hold = Hold::CredentialsRevoked;
    let report = held(hold);
    assert_eq!(report.reach.get(&format!("{}.judged", key(hold))), 2);
    assert!(report.reach.get("degraded.Credentials") > 0);
    assert!(report.reach.get(&format!("{}.refused", key(hold))) > 0);
}

#[test]
fn a_full_staging_disk_leaves_the_replica_restorable_while_it_lasts() {
    let report = held(Hold::DiskFull);
    assert_eq!(report.reach.get("hold.DiskFull.judged"), 2);
    assert!(report.reach.get("retrying.LocalIo") > 0);
}

#[test]
fn a_restore_through_a_lagging_listing_is_the_replica_s_answer_or_an_error() {
    let report = held(Hold::LaggingListing);
    assert_eq!(report.reach.get("hold.LaggingListing.judged"), 2);
    // Commits uploaded under the hold are past what the lagging listing shows, so `Latest`
    // through it cannot be the replica's answer: it must fail.
    assert!(report.reach.get("hold.LaggingListing.refused") > 0);
}

#[test]
fn a_restore_whose_reads_return_other_bytes_fails_and_never_publishes() {
    let report = held(Hold::LyingReads);
    assert_eq!(report.reach.get("hold.LyingReads.judged"), 2);
    assert_eq!(report.reach.get("hold.LyingReads.same"), 0);
    assert!(report.reach.get("hold.LyingReads.refused") > 0);
}

#[test]
fn an_engine_booting_through_lying_reads_leaves_the_replica_restorable() {
    let mut ops = under(Hold::LyingReads);
    let at = ops
        .iter()
        .position(|op| matches!(op, Op::Hold { .. }))
        .unwrap();
    ops.insert(at, Op::Restart);
    let report = match run_ops(Seed::new(7), &ops, &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert_eq!(report.reach.get("hold.LyingReads.judged"), 2);
    // The boot's own reads of the replica met the fault.
    assert!(report.reach.get("hold.LyingReads.engine_reads_flipped") > 0);
}

#[test]
fn readers_holding_the_wal_leave_the_replica_restorable_while_they_hold() {
    let report = held(Hold::ReadersHeld);
    assert_eq!(report.reach.get("hold.ReadersHeld.judged"), 2);
}

#[test]
fn wal_damage_leaves_the_replica_restorable_while_it_lasts() {
    for op in [
        WalOp::FlipLastFrameBit,
        WalOp::TearHeader,
        WalOp::TruncateAtFrame(1),
    ] {
        let hold = Hold::Wal(op);
        let report = held(hold);
        assert_eq!(
            report.reach.get(&format!("{}.judged", key(hold))),
            2,
            "{op:?}"
        );
    }
}

#[test]
fn a_power_loss_on_a_lying_disk_leaves_the_replica_restorable_while_it_lies() {
    let report = held(Hold::LyingDisk);
    assert_eq!(report.reach.get("hold.LyingDisk.judged"), 2);
    assert_eq!(report.reach.get("hold.lying_disk.power_lost"), 1);
}

/// Drawn holds, in the gate: a few seeds of the default mix with holds on, and every hold the
/// generator can draw is drawn and judged at least once over them.
#[test]
fn drawn_holds_are_judged_while_they_hold() {
    let weights = Weights {
        hold: 60,
        ..Weights::default()
    };
    let mut judged = std::collections::BTreeMap::new();
    for n in 1..=8 {
        let seed = Seed::new(n);
        match run(seed, STEPS, &weights) {
            Ok(report) => {
                for (name, count) in report.reach.iter() {
                    if name.starts_with("hold.") && name.ends_with(".judged") {
                        *judged.entry(name.to_owned()).or_insert(0) += count;
                    }
                }
            }
            Err(failure) => panic!("{failure}"),
        }
    }
    assert!(judged.len() >= 6, "too few holds drawn: {judged:?}");
}

/// Runs `seeds` from `base` under `mix`, printing each failure and what each hold reached;
/// the number of failures.
fn campaign(base: u64, seeds: u64, mix: impl Fn(Seed) -> Weights) -> usize {
    let mut failed = 0;
    let mut reached = std::collections::BTreeMap::<String, u64>::new();
    for n in base..base + seeds {
        let seed = Seed::new(n);
        let weights = mix(seed);
        match run(seed, STEPS, &weights) {
            Ok(report) => {
                for (name, count) in report.reach.iter() {
                    if name.starts_with("hold.") {
                        *reached.entry(name.to_owned()).or_insert(0) += count;
                    }
                }
            }
            Err(failure) => {
                failed += 1;
                println!(
                    "survival FAIL {}: {failure}",
                    replay_line(seed, Some(failure.steps))
                );
            }
        }
    }
    let judged: u64 = reached
        .iter()
        .filter(|(name, _)| name.ends_with(".judged"))
        .map(|(_, count)| count)
        .sum();
    println!("survival: {seeds} seeds from {base}, {failed} failed, {judged} held judgments");
    for (name, count) in &reached {
        println!("survival reach {name} {count}");
    }
    failed
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map_or(default, |v| v.parse().expect("a number"))
}

/// The registered campaign over the default mix: `LSR_SIM_BASE_SEED` (1) and `LSR_SIM_SEEDS`
/// (2000) shard it.
#[test]
#[ignore = "a campaign; run by hand"]
fn survival_campaign_default_mix() {
    let failed = campaign(
        env("LSR_SIM_BASE_SEED", 1),
        env("LSR_SIM_SEEDS", 2000),
        |_| Weights {
            hold: 40,
            ..Weights::default()
        },
    );
    assert_eq!(failed, 0);
}

/// The registered campaign over each seed's swarm mix.
#[test]
#[ignore = "a campaign; run by hand"]
fn survival_campaign_swarm() {
    let failed = campaign(
        env("LSR_SIM_BASE_SEED", 1),
        env("LSR_SIM_SEEDS", 2000),
        |seed| {
            let mut weights = Weights::swarm(seed);
            weights.hold = weights.hold.max(20);
            weights
        },
    );
    assert_eq!(failed, 0);
}
