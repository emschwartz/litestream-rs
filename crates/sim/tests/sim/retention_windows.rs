//! Retention over real windows: after every delete job the replica restores every point it keeps.
//!
//! The windows are the engine's own defaults, scaled by nothing: compaction at 30 seconds, 5
//! minutes and an hour, a snapshot a day, a snapshot kept for a day, a level-0 file kept for 5
//! minutes. A history runs several days of bursts with the clock jumped between them, so jobs of
//! every kind run: compactions, a snapshot a day, and the retention deletes behind them
//! (snapshots, files under the floor, covered level-0 files). The oracle restores every
//! transaction the replica holds, `Latest` and sampled instants after every tick and compares the
//! pages with what the database held then, so a restore is judged after every delete job and not
//! only at the end.
//!
//! The kills are the part the random histories rarely reach: the engine is cut short, loses
//! power, or has its delete refused during the ticks right after a day's jump, when those ticks
//! delete. The position of the cut inside the tick comes from the seed, so each scenario is
//! swept over positions of the cut and over seeds; the history itself is written out.
//!
//! Beside the oracle, two checks of the test's own share nothing with it: the recorder's puts and
//! deletes are replayed, and after every delete call (every point a kill can land between
//! batches) the files left must cover every transaction from the oldest snapshot's end to the
//! newest. Every restore the history asks for must also have been judged by the oracle.

use std::collections::BTreeSet;

use lsr_core::{FileKey, StoreError, StoreOp};
use lsr_sim::run::{Seen, run_ops};
use lsr_sim::{CrashKind, FaultShape, Op, Opts, RecorderEvent, RunReport, Seed, Target};

const SECOND: i64 = 1000;
const MINUTE: i64 = 60 * SECOND;
const HOUR: i64 = 60 * MINUTE;
const DAY: i64 = 24 * HOUR;

const DAYS: usize = 3;
/// Ticks after a day's jump during which the kill may land: a snapshot, the compactions behind
/// it and the delete jobs follow one tick each.
const AFTER_JUMP: usize = 6;
const SEEDS: u64 = 2;

/// What happens to the engine at the chosen tick after a day's jump.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kill {
    None,
    Process,
    Power,
    /// The next delete the replica is asked for fails, and the process dies on the tick after.
    RefusedDelete,
}

fn reset() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Delete,
        kind: std::io::ErrorKind::ConnectionReset,
    }
}

fn settle(ticks: usize) -> Vec<Op> {
    (0..ticks).flat_map(|_| [Op::Tick, Op::Restore]).collect()
}

fn ticks(ticks: usize) -> Vec<Op> {
    vec![Op::Tick; ticks]
}

/// One day: bursts of commits an hour apart, each burst spaced so the level 1 to 3 windows pass
/// and compaction runs, then the jump of a day that makes a snapshot due. `kill_at` is the tick
/// after the jump that carries `kill`.
fn day(first: bool, kill: Kill, kill_at: usize) -> Vec<Op> {
    let mut ops = Vec::new();
    for hour in 0..2 {
        for burst in 0..2 {
            ops.push(Op::Commit {
                pages: 3,
                grow: first && hour == 0 && burst == 0,
            });
            ops.extend(ticks(2));
            ops.push(Op::ClockJump(2 * MINUTE));
        }
        ops.push(Op::ClockJump(HOUR));
        ops.extend(ticks(3));
        ops.push(Op::Restore);
    }
    ops.push(Op::ClockJump(DAY));
    for at in 0..AFTER_JUMP {
        if at == kill_at {
            ops.extend(killed(kill));
        } else {
            ops.extend(settle(1));
        }
    }
    ops
}

/// The tick that carries the kill, and the restore judged right after it.
fn killed(kill: Kill) -> Vec<Op> {
    match kill {
        Kill::None => settle(1),
        Kill::Process => vec![Op::Crash(CrashKind::Process), Op::Restore],
        Kill::Power => vec![Op::PowerLoss, Op::Restore],
        Kill::RefusedDelete => vec![
            Op::StoreFault {
                target: Target::Remote,
                op: StoreOp::Delete,
                ahead: 0,
                shape: FaultShape::Error(reset()),
            },
            Op::Tick,
            Op::Crash(CrashKind::Process),
            Op::Restore,
        ],
    }
}

/// The whole history: `DAYS` days, then ticks until nothing is left to do, judged to the end.
fn history(kill: Kill, kill_at: usize) -> Vec<Op> {
    let mut ops = Vec::new();
    for day_number in 0..DAYS {
        ops.extend(day(day_number == 0, kill, kill_at));
    }
    ops.extend(settle(AFTER_JUMP));
    ops.push(Op::Restore);
    ops
}

fn run(ops: &[Op], seed: u64) -> RunReport {
    let opts = Opts {
        record: true,
        ..Opts::default()
    };
    match run_ops(Seed::new(seed), ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    }
}

fn span(key: &FileKey) -> (u64, u64) {
    let range = key.range();
    (range.min().get(), range.max().get())
}

/// Why `files` do not restore from their oldest snapshot on, if they do not: no snapshot that
/// starts at transaction 1, or a transaction after the oldest snapshot's end that no file
/// covers. Written from the file format (a snapshot begins at 1; a restore applies files that
/// together cover every transaction), not from the planner.
fn hole(files: &BTreeSet<FileKey>) -> Option<String> {
    let snapshots: Vec<(u64, u64)> = files
        .iter()
        .filter(|key| key.level().is_snapshot())
        .map(span)
        .collect();
    let Some(oldest_end) = snapshots
        .iter()
        .filter(|(min, _)| *min == 1)
        .map(|(_, max)| *max)
        .min()
    else {
        return Some("no snapshot starts at transaction 1".to_owned());
    };
    let newest = files.iter().map(|key| span(key).1).max().unwrap_or(0);
    (oldest_end.saturating_add(1)..=newest).find_map(|txid| {
        let covered = files.iter().any(|key| {
            let (min, max) = span(key);
            min <= txid && txid <= max
        });
        (!covered).then(|| format!("transaction {txid} is in no file"))
    })
}

#[derive(Debug, Default)]
struct Replay {
    /// Delete calls, each judged on the files left.
    deletes: usize,
    /// Keys deleted at each level, 9 for snapshots.
    deleted_levels: BTreeSet<u8>,
}

/// Replays the replica's commits and deletes in order and checks, after every delete call, that
/// what is left restores from the oldest snapshot on.
fn replay(report: &RunReport) -> Replay {
    let mut files = BTreeSet::new();
    let mut out = Replay::default();
    for event in &report.remote_events {
        match event {
            RecorderEvent::Put { key, .. } => {
                files.insert(*key);
            }
            RecorderEvent::Delete { keys, .. } => {
                for key in keys {
                    files.remove(key);
                    out.deleted_levels.insert(key.level().get());
                }
                out.deletes = out.deletes.saturating_add(1);
                if let Some(why) = hole(&files) {
                    panic!(
                        "after a delete of {} files ({} deletes in) the replica no longer restores: {why}; left: {files:?}",
                        keys.len(),
                        out.deletes
                    );
                }
            }
        }
    }
    out
}

/// Every restore the history asks for was judged: the oracle found a verdict for each, so the
/// tick after a day's jump, where the delete jobs run, is followed by a restore of every
/// transaction compared with the database's pages. Returns how many were judged.
fn every_restore_judged(ops: &[Op], report: &RunReport) -> usize {
    let asked = ops.iter().filter(|op| matches!(op, Op::Restore)).count();
    let judged = report
        .seen
        .iter()
        .filter(|seen| matches!(seen, Seen::Oracle))
        .count();
    assert!(
        judged >= asked,
        "{asked} restores were asked for and {judged} were judged"
    );
    judged
}

/// The history alone reaches every kind of delete: snapshots, compacted levels and level 0. A
/// run that deleted only one kind would pass the checks below without testing the others.
#[test]
fn the_windows_are_reached_and_every_kind_of_file_is_deleted() {
    let ops = history(Kill::None, 0);
    let report = run(&ops, 1);
    let replayed = replay(&report);
    assert!(every_restore_judged(&ops, &report) > replayed.deletes);
    assert!(
        replayed.deletes >= DAYS,
        "only {} deletes ran",
        replayed.deletes
    );
    assert!(replayed.deleted_levels.contains(&9), "{replayed:?}");
    assert!(replayed.deleted_levels.contains(&0), "{replayed:?}");
    assert!(
        replayed
            .deleted_levels
            .iter()
            .any(|level| (1..9).contains(level)),
        "{replayed:?}"
    );
}

fn sweep(kill: Kill) {
    let mut crashed = 0_u64;
    let mut deletes = 0_usize;
    for kill_at in 0..AFTER_JUMP {
        let ops = history(kill, kill_at);
        for seed in 1..=SEEDS {
            let report = run(&ops, seed);
            deletes = deletes.saturating_add(replay(&report).deletes);
            every_restore_judged(&ops, &report);
            crashed = crashed.saturating_add(u64::from(report.crashes));
        }
    }
    assert!(crashed > 0 || kill == Kill::None, "no kill landed");
    assert!(deletes > 0, "no delete ran under {kill:?}");
}

/// The engine is killed during the ticks after a day's jump, wherever the seed cuts the tick.
#[test]
fn a_kill_during_the_ticks_after_a_jump_loses_no_retained_point() {
    sweep(Kill::Process);
}

#[test]
fn a_power_loss_during_the_ticks_after_a_jump_loses_no_retained_point() {
    sweep(Kill::Power);
}

/// A delete the replica refuses, then a kill on the next tick: the next engine plans its delete
/// again from the listing it finds, and the replica still restores every retained point.
#[test]
fn a_refused_delete_then_a_kill_loses_no_retained_point() {
    sweep(Kill::RefusedDelete);
}
