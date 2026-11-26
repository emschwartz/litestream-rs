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
