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

/// On a disk that lies about the engine's fsync of the WAL, a power loss
/// takes commits the replica holds. The boot stages a snapshot of the
/// database as it now is, and the process dies before uploading it. The
/// next boot uploads that snapshot: deleting it as an earlier state of the
/// WAL gives up its restore point and snapshots a second time, with nothing
/// in the window to justify either.
#[test]
fn a_staged_snapshot_of_a_rewound_database_survives_a_restart() {
    let mut ops = vec![Op::LyingWalSync, commit(), commit()];
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops.extend([commit(), commit()]);
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops.extend([Op::PowerLoss, Op::Tick, Op::Tick, Op::Restart]);
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops.push(Op::Restore);
    if let Err(failure) = run_ops(Seed::new(0x4e3), &ops, &Opts::default()) {
        panic!("{failure}");
    }
}
