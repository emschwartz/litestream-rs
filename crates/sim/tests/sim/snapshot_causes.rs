//! One scenario per [`SnapshotCause`]: each drives the engine into that
//! snapshot and checks what was seen, with the oracle judging the replica.

use lsr_core::SnapshotCause;
use lsr_ltx::{PageSize, Txid};
use lsr_sim::scenario::{
    Scenario, meta_wiped_remote_kept, restored_with_no_wal, run_scenario, scenario_of,
    vacuum_page_size_change,
};
use lsr_wal::WalOffset;

fn local() -> Txid {
    Txid::MIN
}

/// Every cause, pinned by an exhaustive match: a new cause fails to compile here.
fn all() -> [SnapshotCause; 11] {
    let causes = [
        SnapshotCause::FirstSync,
        SnapshotCause::BehindRemote { local: local() },
        SnapshotCause::NewestStagedFileUnreadable,
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
        SnapshotCause::StagedUnverified { from: local() },
    ];
    for cause in causes {
        match cause {
            SnapshotCause::FirstSync
            | SnapshotCause::BehindRemote { .. }
            | SnapshotCause::NewestStagedFileUnreadable
            | SnapshotCause::WalMissing
            | SnapshotCause::GenerationChanged
            | SnapshotCause::PrevFrameMismatch
            | SnapshotCause::LastPageMismatch
            | SnapshotCause::LocalRewound { .. }
            | SnapshotCause::CheckpointBoundary
            | SnapshotCause::OperatorRequested
            | SnapshotCause::StagedUnverified { .. } => {}
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
/// size, so the engine stops at boot and the old states still restore.
#[test]
fn a_page_size_change_stops_and_the_old_states_restore() {
    run(vacuum_page_size_change);
}

/// With the staging directory wiped and the replica kept, boot continues from
/// the replica's newest staged file: no snapshot, and the oracle holds.
#[test]
fn a_wiped_meta_continues_from_the_replicas_newest_staged_file() {
    run(meta_wiped_remote_kept);
}

/// A database a restore just published, whose WAL has no header and whose
/// driver writes none: the first sync is a snapshot that writes the header
/// itself, at once and with no retry.
#[test]
fn a_restored_database_with_no_wal_snapshots_with_no_retry() {
    run(restored_with_no_wal);
}

/// A disk that lies about the engine's WAL fsync from the start of a run, as a quarter of
/// swarm runs draw, lets a power loss take frames the engine staged, so the next boot
/// snapshots for `LocalRewound`. That lie justifies the snapshot as surely as one an
/// operation starts mid-run. The needless-snapshot check was told only of the second kind and
/// failed runs with the first (seeds 0x110 and 0x1e850a among them).
#[test]
fn a_disk_lying_from_the_start_justifies_a_local_rewind() {
    use lsr_sim::run::run_ops;
    use lsr_sim::{CrashKind, Op, Opts, Seed};
    let commit = Op::Commit {
        pages: 2,
        grow: false,
    };
    let ops = [
        commit.clone(),
        Op::Tick,
        Op::Tick,
        commit.clone(),
        commit,
        Op::Shutdown,
        Op::Crash(CrashKind::Power),
        Op::Tick,
        Op::Tick,
    ];
    let opts = Opts {
        drawn: true,
        lying_wal_sync: true,
        ..Opts::default()
    };
    let mut rewound = 0;
    for n in 1..=16 {
        let report =
            run_ops(Seed::new(n), &ops, &opts).unwrap_or_else(|failure| panic!("{failure}"));
        rewound += report.reach.get("snapshot.LocalRewound");
    }
    assert!(rewound > 0, "no run reached a local rewind");
}
