//! The scheduled verify, judged against the world (`crates/sim/src/run/verify.rs`).

use lsr_sim::run::run_ops;
use lsr_sim::{Op, Opts, Seed};

/// A bucket rollback takes the replica's newest files while the engine is down, and the engine
/// boots without noticing (the gap BEHAVIOR D17 describes), so the transaction it reads live no
/// longer restores: the plan stops at a gap. Verify must report the restore failing; one that
/// calls it merged away (nothing to compare) hides the loss it exists to report. Found by the
/// swarm against that plant at its 13th seed (seed 0xd, shrunk by deleting operations). An
/// earlier history built on another writer's removed file stopped reaching the gap once the
/// engine learned to stop on another writer.
#[test]
fn verify_reports_a_live_transaction_a_rollback_took_seed_0xd() {
    use lsr_sim::generate::{Hold, RemoteOp, WalOp};
    let ops = [
        Op::Shutdown,
        Op::Tick,
        Op::Remote(RemoteOp::RollBack),
        Op::Verify,
        Op::Hold {
            hold: Hold::Wal(WalOp::RegrowSameSalts(2_774_443_482_631_317_401)),
            ticks: 179,
        },
    ];
    let opts = Opts {
        drawn: true,
        ..Opts::default()
    };
    let report = run_ops(Seed::new(0xd), &ops, &opts)
        .unwrap_or_else(|failure| panic!("{}", failure.violation));
    assert!(
        report.reach.get("verify.RestoreFails") > 0,
        "verify was not judged on a replica that cannot restore its live transaction"
    );
}
