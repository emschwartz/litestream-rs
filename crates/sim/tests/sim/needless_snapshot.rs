//! The needless-snapshot check (`crates/sim/src/needless_snapshot.rs`) judges
//! every run; these runs give it something to judge on purpose.

use lsr_core::SourceOp;
use lsr_sim::generate::{AppAt, AppOp};
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

/// Staging can hold nothing while the replica holds the newest file: the swarm reached it
/// through a boot under lying replica reads that took the staged file for a divergence and
/// dropped it (seed 0x7d, under crash points inside the SQLite seam that were later dropped:
/// docs/decisions.md D120); here staging is wiped, which reaches it without depending on how
/// many of the engine's reads the lying hold covers. A bucket then rolled back past its only
/// transaction leaves nothing on either side, and a first sync is the only way on.
#[test]
fn a_rollback_that_empties_the_replica_justifies_a_first_sync() {
    let ops = vec![
        commit(),
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Restart,
        Op::EnvFault(lsr_sim::EnvFault::WipeStaging),
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
                report.reach.get("needless_snapshot.forced.FirstSync") >= 2,
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

/// An application's TRUNCATE checkpoint lands inside a running sync step, between the engine's
/// read of the WAL length and its read of the frames: the frames are gone from a file now
/// shorter than the one first statted. That is what a power loss leaves, but the checkpoint
/// copied every frame into the database, so the step runs again over the file as it is and
/// takes no snapshot for a rewound WAL (seed 0x6b06509b, reduced).
#[test]
fn a_truncate_checkpoint_inside_a_sync_step_is_not_a_rewound_wal() {
    use lsr_core::{SourceError, SourceOp};
    use lsr_sim::OtherProcessMode;
    use lsr_sim::generate::{AppAt, AppOp, EnvFault};
    let ops = [
        Op::EnvFault(EnvFault::Source(SourceOp::BumpSeq, SourceError::Busy)),
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Spill,
        Op::Shutdown,
        Op::Tick,
        Op::Tick,
        Op::RequestSnapshot,
        Op::AppInTick {
            at: AppAt::Call(5),
            op: AppOp::Checkpoint(OtherProcessMode::Truncate),
        },
    ];
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        after_restart: lsr_sim::AfterRestart::Redo,
        staging_fs: true,
        truncate_pages: std::num::NonZeroU32::new(7),
        remote_s3: true,
        no_create_only: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x6b06_509b), &ops, &opts) {
        panic!("{failure}");
    }
}

/// A staged file's rename is done and the sync of its directory fails. The process ends, the
/// next one finds the file whole and sends it, and then the power goes between two ticks and
/// takes the file from staging, whole or in part. The replica now holds a transaction staging
/// does not, and the boot snapshots above it. The power loss forces that snapshot wherever it
/// falls: one between two ticks went unnoted, and the run failed for a needless snapshot.
#[test]
fn a_power_loss_between_ticks_that_takes_a_sent_staged_file_forces_the_snapshot() {
    let reset = lsr_core::StoreError::Transient {
        op: lsr_core::StoreOp::Upload,
        kind: std::io::ErrorKind::ConnectionReset,
    };
    let mut ops = vec![commit()];
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops.extend([
        Op::StoreFault {
            target: lsr_sim::Target::Staging,
            op: lsr_core::StoreOp::Upload,
            ahead: 0,
            shape: lsr_sim::FaultShape::ErrorAfterPublish(reset),
        },
        commit(),
        Op::Tick,
        Op::Restart,
        // The boot, and the upload of the file. Nothing has synced the directory since.
        Op::Tick,
        Op::Tick,
        Op::PowerLoss,
    ]);
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops.push(Op::Restore);
    // The law draws what the power loss does to the file, on a coin the run's seed feeds: both
    // ways are run, and the count below says so.
    let (mut torn, mut dropped, mut snapshots) = (0, 0, 0);
    for coin in 1..=8 {
        let report = match run_ops(Seed::new(coin), &ops, &Opts::default()) {
            Ok(report) => report,
            Err(failure) => panic!("{failure}"),
        };
        torn += report.reach.get("staged.Torn");
        dropped += report.reach.get("staged.Dropped");
        snapshots += report
            .reach
            .get("needless_snapshot.forced.NewestStagedFileUnreadable");
    }
    assert_eq!(torn + dropped, 8, "torn {torn}, dropped {dropped}");
    assert!(torn > 0 && dropped > 0, "torn {torn}, dropped {dropped}");
    // A torn file is dropped by the boot, which snapshots above the replica's copy of it.
    assert_eq!(snapshots, torn);
}

/// A power loss leaves the wal-index reset ahead of the WAL header, so the boot's snapshot step
/// runs under a reset and proves no continuity. The application commits inside that step, after
/// its committed read: the commit writes the new header, so by the end of the tick the index and
/// the header agree again, and the running engine meets a generation it cannot follow. Its
/// snapshot is forced by the reset the step saw, though nothing after the tick shows it
/// (`docs/decisions/D280.md`).
#[test]
fn a_commit_inside_a_step_under_a_reset_index_justifies_the_next_snapshot() {
    let mut ops = vec![commit(), Op::Crash(CrashKind::Power), Op::Tick];
    ops.push(Op::AppInTick {
        at: AppAt::Before(SourceOp::PageCount),
        op: AppOp::Commit {
            pages: 3,
            grow: true,
        },
    });
    ops.extend(std::iter::repeat_n(Op::Tick, 4));
    ops.push(Op::Restore);
    let report = match run_ops(Seed::new(1), &ops, &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    // The running engine's snapshot, judged and forced.
    assert_eq!(
        report.reach.get("event.Resnapshot"),
        1,
        "{:?}",
        report.reach
    );
    assert_eq!(
        report
            .reach
            .get("needless_snapshot.forced.GenerationChanged"),
        1,
        "{:?}",
        report.reach
    );
}

/// The engine is down while the application commits more than one sync step takes (the
/// simulator's cap is 16 KiB of WAL) and another process checkpoints every frame into the
/// database. The boot continues from the newest staged file, behind the committed end, under
/// a read transaction on mark 0, which blocks no restart. Its first step stops at the cap, and
/// the next commit restarts the WAL: the frames past that step are now only in the database
/// file, and the engine snapshots for `GenerationChanged`. The step that stopped short proved
/// nothing, so the judge takes the snapshot as forced (a 5,000-step large-profile seed, shrunk).
#[test]
fn a_restart_after_a_sync_step_stopped_short_of_a_boot_backlog_justifies_a_snapshot() {
    let big = || Op::Commit {
        pages: 32,
        grow: true,
    };
    let mut ops = vec![commit()];
    ops.extend(std::iter::repeat_n(Op::Tick, 4));
    ops.extend(std::iter::repeat_n(big(), 8));
    ops.extend([
        // The next process's read transaction lands on a slot at the committed end, which lets
        // another process checkpoint every frame; the process after it then reads on mark 0.
        Op::Crash(CrashKind::Process),
        Op::OtherProcessCheckpoint(lsr_sim::OtherProcessMode::Passive),
        Op::Crash(CrashKind::Process),
        // The boot, then one step that stops at the cap, and a commit that restarts the WAL.
        Op::Tick,
        Op::Tick,
        commit(),
    ]);
    ops.extend(std::iter::repeat_n(Op::Tick, 6));
    ops.push(Op::Restore);
    let opts = Opts {
        trace: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!(
            "step {}: {}\n{:#?}",
            failure.step, failure.violation, failure.trace
        ),
    };
    assert!(
        report
            .reach
            .get("needless_snapshot.forced.GenerationChanged")
            > 0,
        "{:#?}",
        report.trace
    );
}
