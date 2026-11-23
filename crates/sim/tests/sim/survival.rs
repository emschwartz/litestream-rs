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
use lsr_sim::{AfterRestart, ClockLaw, Hold, Op, Opts, RunReport, Seed, Weights, replay_line, run};

const STEPS: u32 = 300;

/// A written history: the app commits and the engine uploads, then `hold` lasts six ticks with
/// commits under it, then the run heals and settles.
fn under(hold: Hold) -> Vec<Op> {
    under_for(hold, 6)
}

/// [`under`], with the hold lasting `ticks` ticks.
fn under_for(hold: Hold, ticks: u8) -> Vec<Op> {
    let mut ops = Vec::new();
    for _ in 0..6 {
        ops.push(Op::Commit {
            pages: 2,
            grow: true,
        });
        ops.push(Op::Tick);
        ops.push(Op::Tick);
    }
    ops.push(Op::Hold { hold, ticks });
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
        Hold::LyingReads => "hold.LyingReads".into(),
        Hold::ReadersHeld => "hold.ReadersHeld".into(),
        Hold::Wal(op) => {
            let name = format!("{op:?}");
            let name = name.split('(').next().unwrap_or_default().to_owned();
            format!("hold.wal.{name}")
        }
        Hold::LyingDisk => "hold.LyingDisk".into(),
        Hold::StoreDownAll => "hold.StoreDownAll".into(),
        Hold::SecondWriter => "hold.SecondWriter".into(),
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
    assert!(report.reach.get("degraded.StoreDenied") > 0);
    assert!(report.reach.get(&format!("{}.refused", key(hold))) > 0);
}

#[test]
fn a_full_staging_disk_leaves_the_replica_restorable_while_it_lasts() {
    let report = held(Hold::DiskFull);
    assert_eq!(report.reach.get("hold.DiskFull.judged"), 2);
    // ENOSPC on staging is a full disk, not unnamed local I/O.
    assert!(report.reach.get("retrying.LocalDiskFull") > 0);
    assert_eq!(report.reach.get("retrying.LocalIo"), 0);
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

/// Swarm seed 0x13, shrunk: the prefix holds 512-byte pages, the app vacuums to 2048, the
/// operator removes another writer's level-0 file after it was compacted upward, and the
/// engine boots while every read of the replica lies. The boot read the newest file's header,
/// could not decode it, took that as no page size at all, and snapshotted 2048-byte pages onto
/// the prefix; restores of the next transaction stopped on the change.
#[test]
fn a_boot_whose_replica_reads_lie_never_writes_a_new_page_size_seed_0x13() {
    use lsr_core::{StoreError, StoreOp};
    use lsr_sim::{EnvFault, FaultShape, Target};
    let seed = Seed::new(0x13);
    let ops = [
        Op::Shutdown,
        Op::Vacuum {
            page_size: lsr_ltx::PageSize::new(2048),
        },
        Op::Hold {
            hold: Hold::LyingReads,
            ticks: 11,
        },
        Op::Shutdown,
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Read,
            ahead: 1,
            shape: FaultShape::Error(StoreError::Transient {
                op: StoreOp::Read,
                kind: std::io::ErrorKind::ConnectionReset,
            }),
        },
        Op::WalDamage {
            op: WalOp::TearHeader,
            ticks: 2,
        },
        Op::EnvFault(EnvFault::DeleteWal),
        Op::SecondWriter { ticks: 7 },
        Op::Hold {
            hold: Hold::LyingReads,
            ticks: 9,
        },
    ];
    let opts = Opts {
        drawn: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(seed, &ops, &opts) {
        panic!("{failure}");
    }
}

/// Health tells the truth about a halt: an outage that keeps every upload from landing, held
/// long enough for the engine to retry past its stall threshold, leaves a task degraded at
/// its end (oracle `HealthBlind` otherwise), for the replica store refusing uploads and for a
/// full staging disk.
#[test]
fn a_halt_long_enough_to_stall_is_reported_degraded() {
    for hold in [
        Hold::StoreDown(lsr_core::StoreOp::Upload),
        Hold::DiskFull,
        Hold::CredentialsRevoked,
    ] {
        let report = match run_ops(Seed::new(7), &under_for(hold, 40), &Opts::default()) {
            Ok(report) => report,
            Err(failure) => panic!("{hold:?}: {failure}"),
        };
        assert_eq!(
            report.reach.get(&format!("{}.halted", key(hold))),
            1,
            "{hold:?}: the hold never halted replication long enough to judge health"
        );
    }
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
fn a_store_down_for_every_operation_at_once_leaves_restorable_files() {
    let report = held(Hold::StoreDownAll);
    assert_eq!(report.reach.get("hold.StoreDownAll.judged"), 2);
}

/// The WAL regrown under the engine, judged while the regrowth stands (the plain WAL damage
/// operations judge only after it is healed).
#[test]
fn a_regrown_wal_leaves_the_replica_restorable_while_it_lasts() {
    for op in [
        WalOp::RegrowSameSalts(7),
        WalOp::RegrowWithoutCommitMarks(7),
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

/// Another writer's file past the engine's position, judged while it stands.
#[test]
fn a_second_writer_s_file_leaves_the_replica_restorable_while_it_stands() {
    let report = held(Hold::SecondWriter);
    assert_eq!(report.reach.get("remote.second_writer"), 1);
    assert_eq!(report.reach.get("hold.SecondWriter.judged"), 2);
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

/// Seed 0xb44 of the fixed set's mix, its first 83 operations, recorded: other writers' files
/// appear while the engine is down, among crashes, lagging listings and bucket rollbacks. With
/// the boot gate refusing nothing (the plant `boot-gate-refuses-nothing-sim`), the boot builds
/// on those files and a restore of transaction 10 returns pages no commit made. It replaces
/// swarm seed 0xb9 (two second writers, then a latest that went back), whose history stopped
/// at the other writer once the running engine learned to stop on one. The boot refuses replica
/// files that neither the live WAL nor a takeover record vouches for (BOOT-22).
#[test]
fn a_boot_never_builds_on_files_nothing_local_vouches_for_seed_0xb44() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        after_restart: AfterRestart::Busy,
        clock_law: ClockLaw::EachRead,
        overlap: false,
        error_domain: true,
        staging_fs: false,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0xb44), crate::histories::seed_0xb44::OPS, &opts) {
        panic!("{failure}");
    }
}

/// Seed 0xd of the fixed set's mix, 236 operations. A staged file whose upload published but
/// whose answer was lost turns unreadable in staging; the engine drops it while it runs, and
/// later boots find the replica one transaction past staging with a WAL that no longer holds
/// it. The engine now keeps a takeover through its own staged position before that drop, so
/// no boot refuses its own file. With the record capped at the confirmed transaction (the
/// first cut) the judge failed it: a refusal with staging intact and no other writer.
#[test]
fn an_unconfirmed_upload_dropped_from_staging_is_never_refused_seed_0xd() {
    let seed = lsr_sim::Seed::new(0xd);
    if let Err(failure) = lsr_sim::run(seed, 236, &lsr_sim::Weights::mixed(seed)) {
        panic!("{failure}");
    }
}

/// Seed 0x348 of the fixed set's mix, its first 103 operations, recorded: a running drop of an
/// unreadable staged copy whose upload published with its answer lost. With the takeover
/// record capped at the confirmed transaction (the plant
/// `running-drop-record-capped-at-confirmed`), a later boot refuses the engine's own file: "a
/// boot refused replica files through 8 with staging intact and no other writer". It replaces
/// unaided seed 0xf45b5, whose history no longer reached the drop on the merged tree.
#[test]
fn a_running_drop_records_past_an_unconfirmed_upload_seed_0x348() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: false,
        after_restart: AfterRestart::Nothing,
        clock_law: ClockLaw::PerTick,
        overlap: false,
        error_domain: false,
        staging_fs: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x348), crate::histories::seed_0x348::OPS, &opts) {
        panic!("{failure}");
    }
}

/// Seed 0x2e7 of the fixed set's mix, its first 198 operations, recorded: a boot drops staged
/// files the replica holds. Without the takeover kept before the drop (the plant
/// `takeover-before-drop-skipped-sim`), or with it capped at the confirmed transaction (the
/// plant `boot-drop-record-capped-at-confirmed`), the next boot refuses the engine's own files:
/// "a boot refused replica files through 11 with staging intact and no other writer". It
/// replaces seed 0x3ed, whose history no longer reached the drop on the merged tree.
#[test]
fn a_boot_drop_records_through_what_staging_vouched_for_seed_0x2e7() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: false,
        after_restart: AfterRestart::Busy,
        clock_law: ClockLaw::EachRead,
        overlap: false,
        error_domain: false,
        staging_fs: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x2e7), crate::histories::seed_0x2e7::OPS, &opts) {
        panic!("{failure}");
    }
}
