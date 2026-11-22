//! The scheduled verify, judged against the world (`crates/sim/src/run/verify.rs`).

use lsr_sim::run::run_ops;
use lsr_sim::{Op, Opts, Seed};

/// Another writer's file sits on the replica while the engine is down and is removed after
/// the boot built on it, so the transaction the engine reads live no longer restores: the
/// plan stops at a gap. Verify must report the restore failing; one that calls it merged away
/// (nothing to compare) hides the loss it exists to report. Found by the swarm against that
/// plant at its 20th seed (seed 0x14, shrunk by deleting operations).
#[test]
fn verify_reports_a_live_transaction_that_no_longer_restores_seed_0x14() {
    let ops = [
        Op::Shutdown,
        Op::SecondWriter { ticks: 3 },
        Op::Verify,
        Op::SecondWriter { ticks: 6 },
    ];
    let opts = Opts {
        drawn: true,
        ..Opts::default()
    };
    let report = run_ops(Seed::new(0x14), &ops, &opts)
        .unwrap_or_else(|failure| panic!("{}", failure.violation));
    assert!(
        report.reach.get("verify.RestoreFails") > 0,
        "verify was not judged on a replica that cannot restore its live transaction"
    );
}
