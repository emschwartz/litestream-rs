//! One scenario per [`SnapshotCause`]: each drives the engine into that
//! snapshot and checks what was seen, with the oracle judging the replica.

use lsr_core::SnapshotCause;
use lsr_ltx::{PageSize, Txid};
use lsr_sim::scenario::{
    Scenario, meta_wiped_remote_kept, run_scenario, scenario_of, vacuum_page_size_change,
};
use lsr_wal::WalOffset;

fn local() -> Txid {
    Txid::MIN
}

/// Every cause, pinned by an exhaustive match: a new cause fails to compile here.
fn all() -> [SnapshotCause; 10] {
    let causes = [
        SnapshotCause::FirstSync,
        SnapshotCause::BehindRemote { local: local() },
        SnapshotCause::LocalTipUnreadable,
        SnapshotCause::WalMissing,
        SnapshotCause::GenerationChanged,
        SnapshotCause::PrevFrameMismatch,
        SnapshotCause::LastPageMismatch,
        SnapshotCause::LocalRewound {
            synced: WalOffset::from_frame_index(0, PageSize::MIN),
            committed: WalOffset::from_frame_index(0, PageSize::MIN),
        },
        SnapshotCause::CheckpointBoundary,
        SnapshotCause::OperatorRequested,
    ];
    for cause in causes {
        match cause {
            SnapshotCause::FirstSync
            | SnapshotCause::BehindRemote { .. }
            | SnapshotCause::LocalTipUnreadable
            | SnapshotCause::WalMissing
            | SnapshotCause::GenerationChanged
            | SnapshotCause::PrevFrameMismatch
            | SnapshotCause::LastPageMismatch
            | SnapshotCause::LocalRewound { .. }
            | SnapshotCause::CheckpointBoundary
            | SnapshotCause::OperatorRequested => {}
        }
    }
    causes
}

fn run(make: fn() -> Scenario) {
    let result = run_scenario(&make());
    assert!(result.is_ok(), "{}", result.err().unwrap_or_default());
}

#[test]
fn every_cause_has_a_scenario() {
    for cause in all() {
        assert!(!scenario_of(cause).is_empty(), "{cause:?} has no scenario");
    }
}

#[test]
fn every_scenario_runs() {
    let mut failures = Vec::new();
    for cause in all() {
        for make in scenario_of(cause) {
            if let Err(message) = run_scenario(&make()) {
                failures.push(message);
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A page-size change is no snapshot cause: a replica prefix holds one page
/// size, so the engine halts at boot and the old states still restore.
#[test]
fn a_page_size_change_halts_and_the_old_states_restore() {
    run(vacuum_page_size_change);
}

/// With the staging directory wiped and the replica kept, boot continues from
/// the replica's tip: no snapshot, and the oracle holds.
#[test]
fn a_wiped_meta_continues_from_the_replicas_tip() {
    run(meta_wiped_remote_kept);
}
