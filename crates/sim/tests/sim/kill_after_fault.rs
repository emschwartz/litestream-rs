//! The kill-after-fault law (`Weights::kill_after_fault`): the process dies right after the first
//! tick in which an injected fault fires, so whatever the fault left the engine owing must
//! survive a restart. It is the proof of the obligation ledger (`lsr_core::obligations`): with a
//! journalled field's journalling planted out, the law fails runs the swarm without it passes.

use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Opts, Seed, Weights};

/// The law is drawn in half the swarm, last, and never in the default mix, so recorded histories
/// keep their shape.
#[test]
fn half_the_swarm_draws_the_law_and_the_default_mix_never() {
    let drawn = (0..400)
        .filter(|n| Weights::swarm(Seed::new(*n)).kill_after_fault)
        .count();
    assert!((120..=280).contains(&drawn), "{drawn} of 400");
    assert!(!Weights::default().kill_after_fault);
}

/// Seed 0x116's recorded history (drawn under the swarm mix with the whole error domain, its
/// known-wrong cells and the law): a failed sync of the WAL whose tick the law ends. The restarted
/// engine still owes the TRUNCATE because the engine journal holds the debt. With the debt's
/// journalling planted out (`journal-debt-not-recorded-sim`) it stages frames the failed sync
/// covered; the same history without the law passes either way.
#[test]
fn a_kill_right_after_a_failed_wal_sync_still_owes_the_truncate_seed_0x116() {
    let opts = Opts {
        drawn: true,
        block_crash_law: false,
        lying_wal_sync: false,
        after_restart: AfterRestart::Busy,
        clock_law: ClockLaw::PerTick,
        overlap: false,
        error_domain: true,
        known_wrong: true,
        staging_fs: true,
        kill_after_fault: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x116), crate::histories::seed_0x116::OPS, &opts) {
        panic!("{failure}");
    }
}

/// A store fault arms the law, and a full staging disk then holds for 64 ticks. A fault that
/// fires inside the hold would have the law kill the process there: a restart inside a hold,
/// which resets the retry counts and degraded states the halt report reads (the open defect D9,
/// `KnownWrong::HaltForgottenAtRestart`). With the known-wrong cells left out the law does not
/// kill inside a hold, so the halt is reported degraded as it lasts.
#[test]
fn the_law_does_not_kill_inside_a_hold_where_known_defects_are_not_counted() {
    use lsr_core::{StoreError, StoreOp};
    use lsr_sim::FaultShape;
    use lsr_sim::generate::{Hold, Op, Target};

    let opts = lsr_sim::Opts {
        kill_after_fault: true,
        known_wrong: false,
        // Overlapping holds: the hold runs a drawn sub-history, so faults fire inside it.
        overlap: true,
        ..lsr_sim::Opts::default()
    };
    let ops = [
        Op::Commit {
            pages: 2,
            grow: false,
        },
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::StoreFault {
            target: Target::Staging,
            op: StoreOp::List,
            ahead: 1,
            shape: FaultShape::Error(StoreError::Transient {
                op: StoreOp::List,
                kind: std::io::ErrorKind::ConnectionReset,
            }),
        },
        Op::Hold {
            hold: Hold::DiskFull,
            ticks: 64,
        },
    ];
    if let Err(failure) = lsr_sim::run::run_ops(Seed::new(1), &ops, &opts) {
        panic!("{failure}");
    }
}
