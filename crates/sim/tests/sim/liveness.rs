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
    if let Err(failure) = run_ops(Seed::new(1), &ops, &Opts::default()) {
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
    if let Err(failure) = run_ops(Seed::new(1), &ops, &Opts::default()) {
        panic!("{failure}");
    }
}

/// A power loss takes the only commit, the boot snapshots the database with
/// an empty WAL, and the next commit writes a new WAL header whose salts
/// SQLite draws at random. The running engine, pinned on read mark 0 at the
/// first frame, follows it: nothing committed can have gone with a
/// generation that held no frame (seed 0xf4846, shrunk).
#[test]
fn a_new_wal_header_over_an_empty_pinned_generation_is_followed() {
    let commit = || Op::Commit {
        pages: 4,
        grow: false,
    };
    let mut ops = vec![commit(), Op::Crash(CrashKind::Power)];
    ops.extend(std::iter::repeat_n(Op::Tick, 3));
    ops.push(commit());
    ops.extend(std::iter::repeat_n(Op::Tick, 4));
    ops.push(Op::Restore);
    if let Err(failure) = run_ops(Seed::new(1), &ops, &Opts::default()) {
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

/// A power loss stops the engine before it stages the last commit, and another process
/// checkpoints every frame into the database. The boot continues from the newest staged file,
/// short of the WAL's committed end, so no read transaction pins its position. The next commit
/// restarts the WAL before the engine stages the rest; that commit's frames are only in the
/// database file now, and the engine snapshots for `GenerationChanged`. The judge takes that
/// snapshot as needed, not as one a running engine's read lock rules out. (Shrunk from a mixed
/// campaign run; the second writer stops the first engine, so the boot is a fresh process's.)
#[test]
fn a_restart_after_a_boot_behind_the_committed_end_justifies_a_snapshot() {
    let ops = [
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Shrink { pages: 1 },
        Op::Crash(CrashKind::Power),
        Op::OtherProcessCheckpoint(lsr_sim::OtherProcessMode::Restart),
        Op::SecondWriter { ticks: 7 },
        Op::Tick,
        Op::Commit {
            pages: 3,
            grow: false,
        },
    ];
    let report = match run_ops(Seed::new(1), &ops, &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("step {}: {}", failure.step, failure.violation),
    };
    assert!(
        report.reach.get("snapshot.GenerationChanged") > 0,
        "the engine snapshots"
    );
}
