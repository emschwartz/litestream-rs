//! The snapshot liveness oracle (`crates/sim/src/liveness.rs`) judges every
//! run; these runs give it something to judge on purpose.

use lsr_sim::run::run_ops;
use lsr_sim::{CrashKind, Op, Opts, Seed};

fn commit() -> Op {
    Op::Commit {
        pages: 2,
        grow: false,
    }
}

/// Restarts and clean stops with nothing damaged in between: every boot
/// after the first must continue from the newest staged file. A boot that
/// snapshots anyway costs a whole-database upload per restart and fails the
/// oracle, though every restore still matches.
#[test]
fn restarts_with_nothing_damaged_never_snapshot() {
    let mut ops = vec![commit(), Op::Tick, Op::Tick, Op::Tick];
    for _ in 0..3 {
        ops.extend([
            commit(),
            Op::Tick,
            Op::Tick,
            Op::Restart,
            Op::Tick,
            Op::Tick,
        ]);
        ops.extend([commit(), Op::Tick, Op::Shutdown, Op::Tick, Op::Tick]);
        ops.extend([commit(), Op::Tick, Op::Crash(CrashKind::Process), Op::Tick]);
    }
    ops.push(Op::Restore);
    if let Err(failure) = run_ops(Seed::new(0x11fe), &ops, &Opts::default()) {
        panic!("{failure}");
    }
}

/// Staging can hold nothing while the replica holds the newest file: here a boot under lying
/// replica reads takes the staged file for a divergence and drops it. A bucket then rolled back
/// past its only transaction leaves nothing on either side, and a first sync is the only way on.
/// The swarm found it (seed 0x7d, under crash points inside the SQLite seam that were later
/// dropped: docs/decisions.md D120).
#[test]
fn a_rollback_that_empties_the_replica_justifies_a_first_sync() {
    let ops = vec![
        commit(),
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Restart,
        Op::Hold {
            hold: lsr_sim::Hold::LyingReads,
            ticks: 5,
        },
        Op::Restart,
        Op::Remote(lsr_sim::generate::RemoteOp::RollBack),
        Op::Tick,
        Op::Tick,
        Op::Restore,
    ];
    let opts = Opts {
        trace: true,
        ..Opts::default()
    };
    match run_ops(Seed::new(0x7d), &ops, &opts) {
        Ok(report) => {
            assert!(
                report.reach.get("liveness.justified.FirstSync") >= 2,
                "{:#?}",
                report.trace
            );
        }
        Err(failure) => panic!("{failure}\n{:#?}", failure.trace),
    }
}
