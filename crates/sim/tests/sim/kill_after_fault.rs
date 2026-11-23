//! The kill-after-fault law (`Weights::kill_after_fault`): the process dies right after the first
//! tick in which an injected fault fires, so whatever the fault left the engine owing must
//! survive a restart. It is the proof of the obligation ledger (`lsr_core::obligations`): with a
//! journalled field's journalling planted out, the law fails runs the swarm without it passes.

use lsr_sim::{Seed, Weights};

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
        trace: true,
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
    let report = match lsr_sim::run::run_ops(Seed::new(1), &ops, &opts) {
        Ok(report) => report,
        Err(failure) => panic!("{failure}"),
    };
    let trace = report.trace.join("\n");
    let held = trace
        .split_once("Hold {")
        .and_then(|(_, rest)| rest.split_once("hold ended"))
        .map(|(inside, _)| inside)
        .expect("the hold ran and ended");
    assert!(
        !held.contains("killed after a fault"),
        "the law killed inside the hold:\n{trace}"
    );
}
