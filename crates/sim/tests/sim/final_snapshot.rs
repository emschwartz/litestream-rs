//! The level-9 snapshot a drain ends with on request (`replicate -once -force-snapshot`), in
//! short explicit scenarios: written whole after everything is uploaded, written again at an
//! unchanged position, and, when its upload fails or the machine dies after it, a replica that
//! still restores every transaction and the latest state.

use lsr_core::{FinalSnapshot, StoreError, StoreOp};
use lsr_sim::run::run_ops;
use lsr_sim::{FaultShape, Op, Opts, RunReport, Seed, Target};

fn run(ops: &[Op]) -> Result<RunReport, String> {
    run_ops(Seed::new(1), ops, &Opts::default()).map_err(|failure| failure.violation.to_string())
}

fn commits() -> Vec<Op> {
    let mut ops = Vec::new();
    for pages in [3, 2, 4] {
        ops.push(Op::Commit { pages, grow: true });
        ops.extend([Op::Tick, Op::Tick, Op::Tick]);
    }
    ops
}

fn reset() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: std::io::ErrorKind::ConnectionReset,
    }
}

/// The snapshot is written and the replica restores every transaction, before and after the
/// next process boots on it.
#[test]
fn a_drain_ends_with_a_snapshot_that_restores() {
    let mut ops = commits();
    ops.push(Op::Commit {
        pages: 2,
        grow: false,
    });
    ops.extend([Op::ShutdownWithSnapshot, Op::Restore, Op::Tick, Op::Restore]);
    let report = run(&ops).unwrap();
    assert!(
        matches!(report.final_snapshots[..], [FinalSnapshot::Written(_)]),
        "{:?}",
        report.final_snapshots
    );
}

/// A second drain at the same position writes the same snapshot again.
#[test]
fn a_second_drain_at_the_same_position_writes_it_again() {
    let mut ops = commits();
    ops.extend([
        Op::ShutdownWithSnapshot,
        Op::Tick,
        Op::ShutdownWithSnapshot,
        Op::Restore,
    ]);
    let report = run(&ops).unwrap();
    let [
        FinalSnapshot::Written(first),
        FinalSnapshot::Written(second),
    ] = report.final_snapshots[..]
    else {
        panic!("{:?}", report.final_snapshots);
    };
    assert_eq!(first.key, second.key);
}

/// The snapshot's upload fails before it publishes: no snapshot, and the replica restores.
#[test]
fn a_snapshot_whose_upload_fails_leaves_a_replica_that_restores() {
    let mut ops = commits();
    ops.extend([
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::Error(reset()),
        },
        Op::ShutdownWithSnapshot,
        Op::Restore,
    ]);
    let report = run(&ops).unwrap();
    assert!(
        matches!(report.final_snapshots[..], [FinalSnapshot::Failed(_)]),
        "{:?}",
        report.final_snapshots
    );
}

/// The snapshot publishes and its response is lost, then the machine loses power: the drain
/// says it failed, and the published file and the replica still restore after the reboot.
#[test]
fn a_snapshot_published_with_a_lost_response_then_a_power_loss_restores() {
    let mut ops = commits();
    ops.extend([
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::ErrorAfterPublish(reset()),
        },
        Op::ShutdownWithSnapshot,
        Op::PowerLoss,
        Op::Restore,
        Op::Tick,
        Op::Tick,
        Op::Restore,
    ]);
    let report = run(&ops).unwrap();
    assert!(
        matches!(report.final_snapshots[..], [FinalSnapshot::Failed(_)]),
        "{:?}",
        report.final_snapshots
    );
}
