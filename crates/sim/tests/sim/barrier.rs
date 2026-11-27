//! The application's wait on the engine's write barrier (`lsr_sim::barrier`).

use lsr_sim::run::run_ops;
use lsr_sim::{Op, Opts, Seed};

/// Commits of two pages, two ticks apart, through the checkpoints they bring and the healthy
/// tail: every barrier is released within its tick, no replica request is made under one, and
/// the application waits no longer than a staging tick.
#[test]
fn every_checkpoint_lets_the_application_write_again_within_its_tick() {
    let ops: Vec<Op> = (0..12)
        .flat_map(|_| {
            [
                Op::Commit {
                    pages: 2,
                    grow: false,
                },
                Op::Tick,
                Op::Tick,
            ]
        })
        .collect();
    let report = match run_ops(Seed::new(1), &ops, &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report.reach.get("barrier.judged") >= 4,
        "{:?}",
        report.reach
    );
}
/// Another reader holds checkpoints back while 80 frames are committed and synced, as a level-9
/// snapshot holds its read for its whole upload, then lets go: the checkpoint after copies that
/// backlog with no barrier held, and no barrier copies more than its bound
/// (`docs/decisions/D303.md`). Copied under the barrier, the backlog made a drill's writer wait
/// 5.9 s.
#[test]
fn a_backlog_a_reader_held_back_is_not_copied_under_the_barrier() {
    let mut ops = vec![Op::Tick, Op::OtherProcessReader { begin: true }];
    ops.extend((0..40).flat_map(|_| {
        [
            Op::Commit {
                pages: 2,
                grow: false,
            },
            Op::Tick,
        ]
    }));
    ops.push(Op::OtherProcessReader { begin: false });
    ops.extend([Op::Tick, Op::Tick, Op::Tick, Op::Tick]);
    let report = match run_ops(Seed::new(1), &ops, &Opts::default()) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    assert!(
        report.reach.get("barrier.judged") >= 4,
        "{:?}",
        report.reach
    );
    assert!(report.max_wal_frames > 64, "{report:?}");
}
