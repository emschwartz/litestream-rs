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

/// Seed 0x148 of the survival campaign (its swarm mix, holds at no less than 20 in a thousand,
/// overlapping faults, the law drawn), cut after its failing step. The law killed the process
/// inside a full-disk hold, a restart there, which resets the retry counts and degraded states
/// the halt report reads: the open defect D9 (`KnownWrong::HaltForgottenAtRestart`), which only
/// runs that count known defects may reach. With the known-wrong cells left out, as here, the
/// law does not kill inside a hold, so the halt is reported.
#[test]
fn the_law_kills_inside_a_hold_only_where_known_defects_are_counted_seed_0x148() {
    let opts = lsr_sim::Opts {
        drawn: true,
        block_crash_law: false,
        lying_wal_sync: false,
        after_restart: lsr_sim::AfterRestart::Busy,
        clock_law: lsr_sim::ClockLaw::PerTick,
        overlap: true,
        error_domain: false,
        known_wrong: false,
        staging_fs: true,
        kill_after_fault: true,
        ..lsr_sim::Opts::default()
    };
    if let Err(failure) =
        lsr_sim::run::run_ops(Seed::new(0x148), crate::histories::seed_0x148::OPS, &opts)
    {
        panic!("{failure}");
    }
}
