//! The boot check for a database file ahead of its WAL (`docs/decisions.md` D103) when the
//! replica cannot give its own newest confirmed state.

use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Op, Opts, Seed};

fn opts(trace: bool) -> Opts {
    Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        after_restart: AfterRestart::Redo,
        clock_law: ClockLaw::PerTick,
        error_domain: true,
        trace,
        ..Opts::default()
    }
}

/// A bucket rollback leaves a gap below the replica's newest confirmed transaction, then the
/// WAL sync fails and a boot owes the check. The check cannot restore the state it compares
/// against; it must not retry that for ever and hold back the TRUNCATE that pays the debt.
#[test]
fn a_gap_below_the_confirmed_state_does_not_hold_back_the_cure_seed_0x2dc719() {
    // A restart with the debt recorded owes the check; its first attempt meets the gap, and the
    // second, after the backoff, gives it up so the cure runs.
    let retry = [
        Op::Shutdown,
        Op::Tick,
        Op::Tick,
        Op::ClockJump(2_000),
        Op::Tick,
        Op::ClockJump(5_000),
        Op::Tick,
        Op::Tick,
    ];
    let ops = [crate::histories::seed_0x2dc719::OPS, &retry].concat();
    let report = match run_ops(Seed::new(0x2dc719), &ops, &opts(true)) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}\n{}", failure.trace.join("\n")),
    };
    let trace = report.trace.join("\n");
    let at = |event: &str| {
        trace
            .find(event)
            .unwrap_or_else(|| panic!("no {event}:\n{trace}"))
    };
    let damaged = at("cause: RemoteCorrupt");
    let given_up = at("cause: DatabaseAheadOfWalUnchecked");
    let cured = at("Resnapshot(WalSyncFailed)");
    assert!(damaged < given_up && given_up < cured, "{trace}");
}
