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
