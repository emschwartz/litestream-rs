//! The retention pass a drain ends with on request (`replicate -once -enforce-retention`), in
//! short explicit scenarios. Retention is off while snapshots accumulate a day apart, so the
//! background schedule deletes nothing, and switched on for the drain: the pass deletes the
//! aged ones, a failing delete ends it without deleting, and a power loss after either leaves
//! a replica that restores every transaction and the latest state.

use lsr_core::{FinalRetention, RetentionMode, StoreError, StoreOp};
use lsr_sim::run::run_ops;
use lsr_sim::{FaultShape, Op, Opts, RunReport, Seed, Target};

const DAY: i64 = 24 * 60 * 60 * 1000;

/// Three snapshots a day apart, written with retention off.
fn snapshots() -> Vec<Op> {
    let mut ops = Vec::new();
    for day in 0..3 {
        if day > 0 {
            ops.push(Op::ClockJump(DAY + 60_000));
        }
        ops.push(Op::Commit {
            pages: 3,
            grow: true,
        });
        ops.extend(std::iter::repeat_n(Op::Tick, 12));
    }
    ops
}

/// The run's report, or its violation as text, with retention switched on just before step
/// `on`.
fn run(ops: &[Op], on: u32) -> Result<RunReport, String> {
    let opts = Opts {
        retention: RetentionMode::Off,
        switch_retention: Some((on, RetentionMode::Enforce)),
        ..Opts::default()
    };
    run_ops(Seed::new(1), ops, &opts).map_err(|failure| failure.violation.to_string())
}

fn reset() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Delete,
        kind: std::io::ErrorKind::ConnectionReset,
    }
}

/// The pass deletes the aged snapshots, the replica restores, and the next engine boots on it.
#[test]
fn the_pass_deletes_aged_snapshots_and_the_replica_restores() {
    let mut ops = snapshots();
    ops.extend([Op::Restore, Op::ShutdownWithRetention]);
    let on = u32::try_from(ops.len() - 1).unwrap();
    ops.extend([Op::Restore, Op::Tick, Op::Restore]);
    let report = run(&ops, on).unwrap();
    assert!(
        matches!(report.final_retentions[..], [FinalRetention::Done { deleted }] if deleted > 0),
        "{:?}",
        report.final_retentions
    );
}

/// A delete that fails ends the pass: nothing more is deleted, and a power loss after it
/// leaves a replica that restores.
#[test]
fn a_failed_delete_ends_the_pass_and_a_power_loss_after_it_restores() {
    let mut ops = snapshots();
    ops.extend([
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Delete,
            ahead: 0,
            shape: FaultShape::Error(reset()),
        },
        Op::ShutdownWithRetention,
    ]);
    let on = u32::try_from(ops.len() - 1).unwrap();
    ops.extend([Op::PowerLoss, Op::Restore, Op::Tick, Op::Restore]);
    let report = run(&ops, on).unwrap();
    assert!(
        matches!(
            report.final_retentions[..],
            [FinalRetention::Failed { deleted: 0, .. }]
        ),
        "{:?}",
        report.final_retentions
    );
}
