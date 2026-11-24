//! A boot that drops a damaged staged file while the replica holds nothing yet.

use lsr_core::{Event, SnapshotCause};
use lsr_sim::generate::MetaOp;
use lsr_sim::run::{Seen, run_ops};
use lsr_sim::sqlite::CrashKind;
use lsr_sim::{Op, Opts, Seed};

/// The engine stages transaction 1 and has not uploaded it when the staged file is damaged and
/// the process dies. The next boot reads the file twice, drops it, and needs no takeover record:
/// the replica is empty, so it holds nothing staging could have vouched for. It boots with a
/// snapshot and drains. Built with optimization, rustc 1.95 to at least 1.102 compiled the
/// gate's old form so that the empty replica read as transaction zero: the boot asked for a
/// takeover through zero on every tick, failed `LocalIo`, and never drained. `just test-opt`
/// runs this scenario optimized; the unoptimized build never showed the fault.
#[test]
fn a_boot_that_drops_a_damaged_staged_file_over_an_empty_replica_needs_no_takeover() {
    let opts = Opts {
        record: true,
        trace: true,
        ..Opts::default()
    };
    let ops = [
        Op::Tick,
        Op::Commit {
            pages: 2,
            grow: false,
        },
        Op::Tick,
        Op::StagingDamage(MetaOp::FlipNewestByte),
        Op::Crash(CrashKind::Process),
        Op::Tick,
        Op::Tick,
        Op::Tick,
    ];
    let report = match run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    let wanted = report
        .seen
        .iter()
        .any(|seen| matches!(seen, Seen::Event(Event::TakeoverWanted(_))));
    assert!(!wanted, "{:#?}", report.trace);
    let booted = report.seen.iter().any(|seen| {
        matches!(
            seen,
            Seen::Event(Event::Booted {
                cause: Some(SnapshotCause::NewestStagedFileUnreadable)
            })
        )
    });
    assert!(booted, "{:#?}", report.trace);
}
