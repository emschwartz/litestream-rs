//! The boot check for a database file ahead of its WAL (`docs/decisions.md` D103) when the
//! replica cannot give its own newest confirmed state (D106).

use lsr_core::{SourceError, SourceOp};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Op, RemoteOp};
use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Opts, Seed};

/// A bucket rollback takes the replica's newest level-0 file, and the engine uploads the next
/// one past the hole, so the replica's newest confirmed state can no longer be restored. Then the
/// WAL sync fails and a restart owes the check, which compares against exactly that state. Its
/// first attempt reports the damage, the next one, after the backoff, gives the check up, and
/// the cure runs: the check must not retry for ever and hold back the TRUNCATE that pays the
/// debt.
#[test]
fn a_gap_below_the_confirmed_state_does_not_hold_back_the_cure() {
    let ops = [
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Shutdown,
        Op::Shutdown,
        Op::Tick,
        Op::Remote(RemoteOp::RollBack),
        Op::Shutdown,
        Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
        Op::AppInTick {
            at: AppAt::Call(20),
            op: AppOp::Commit {
                pages: 4,
                grow: false,
            },
        },
        Op::Tick,
        // The restart with the debt recorded owes the check: its first attempt meets the gap.
        Op::Shutdown,
        Op::Tick,
        Op::Tick,
        // The second attempt, after the backoff, gives the check up; the cure runs after that.
        Op::ClockJump(2_000),
        Op::Tick,
        Op::ClockJump(5_000),
        Op::Tick,
        Op::Tick,
    ];
    // The disk lies about the engine's WAL syncs and power losses follow the block law; after a
    // restart the application redoes its unreplicated commits.
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: true,
        after_restart: AfterRestart::Redo,
        clock_law: ClockLaw::PerTick,
        error_domain: true,
        trace: true,
        ..Opts::default()
    };
    let report = match run_ops(Seed::new(1), &ops, &opts) {
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
