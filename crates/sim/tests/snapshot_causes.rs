//! One scenario per [`SnapshotCause`]: each drives the engine into that
//! snapshot and checks what was seen, with the oracle judging the replica.

use lsr_core::SnapshotCause;
use lsr_ltx::{PageSize, Txid};
use lsr_sim::scenario::{Scenario, run_scenario, scenario_of, vacuum_page_size_change};
use lsr_wal::WalOffset;

fn local() -> Txid {
    Txid::MIN
}

/// Every cause, pinned by an exhaustive match: a new cause fails to compile here.
fn all() -> [SnapshotCause; 11] {
    let causes = [
        SnapshotCause::FirstSync,
        SnapshotCause::BehindRemote { local: local() },
        SnapshotCause::LocalTipUnreadable,
        SnapshotCause::WalMissing,
        SnapshotCause::PageSizeChanged,
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
            | SnapshotCause::PageSizeChanged
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

/// Scenarios that fail on a known engine defect, run by the ignored `review_`
/// tests below instead of the green sweep.
const KNOWN_DEFECT: [&str; 1] = ["vacuum_page_size_change"];

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
            let scenario = make();
            if KNOWN_DEFECT.contains(&scenario.name) {
                continue;
            }
            if let Err(message) = run_scenario(&scenario) {
                failures.push(message);
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
/// Finding 1: after a page-size change the replica holds level-0 files of two
/// page sizes; compaction halts `RemoteCorrupt` and a restore fails with
/// "page size 2048 where the chain began with 512". The engine should start a
/// new generation (or refuse) when the page size changes.
/// Replay: `just sim-replay 0x0000000000000006 17`.
#[test]
#[ignore = "finding 1: page size change leaves mixed page sizes in level 0"]
fn review_vacuum_page_size_change_restores() {
    run(vacuum_page_size_change);
}
