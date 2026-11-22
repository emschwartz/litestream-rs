//! The open engine defects the error domain reaches (`lsr_sim::domain::KnownWrong`), each pinned
//! to a seed's recorded history (`tests/sim/histories/`, drawn under the seed's mix with the
//! known-wrong cells drawn) that must still fail with the defect's signature. The per-push fixed
//! seeds leave those cells out; the nightly draws them and counts their failures as known.
//!
//! When a defect's fix lands, its test here fails, on purpose: flip the defect's cells in
//! `crates/sim/src/domain.rs` from `Cell::KnownWrong` to `Cell::Drawn`, remove the variant, and
//! move the seed to a test that requires the run to pass. A defect that stays known-wrong after
//! its fix would hide its regressions from the per-push seeds.

use lsr_sim::domain::{KnownWrong, known_in};
use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Op, Opts, Seed};

/// Runs `seed`'s recorded `ops` under `opts` and requires them to fail with `defect`'s
/// signature.
fn still_fails(seed: u64, ops: &[Op], opts: &Opts, defect: KnownWrong) {
    let seed = Seed::new(seed);
    assert!(
        known_in(ops).contains(&defect),
        "{seed} draws no cell of {defect:?}"
    );
    match run_ops(seed, ops, opts) {
        Err(failure) if defect.matches(&failure.violation) => {}
        Err(failure) => panic!("{seed} fails another way: {failure}"),
        Ok(_) => panic!(
            "{seed} passes: {defect:?} looks fixed ({}); flip its cells to Drawn",
            defect.tracking()
        ),
    }
}

/// A failed WAL sync, then a restart before the TRUNCATE: the new process stages frames the
/// failed sync covered (D2). Seed 0xf is the first of the mixed set to reach it.
#[test]
fn a_restart_after_a_failed_wal_sync_still_stages_frames_it_covered_seed_0xf() {
    let opts = Opts {
        drawn: true,
        block_crash_law: false,
        lying_wal_sync: false,
        after_restart: AfterRestart::Retry,
        clock_law: ClockLaw::EachRead,
        staging_fs: true,
        ..Opts::default()
    };
    still_fails(
        0xf,
        crate::histories::seed_0xf::OPS,
        &opts,
        KnownWrong::WalSyncForgottenAtRestart,
    );
}

/// Fixed: a staging read refused for permission during level-1 compaction degraded maintenance
/// as the replica's `StoreDenied(Permission)` until #145 routed compaction's local reads through
/// `Fault::from_staging`. Seed 0xb9e, the one that reached it, must pass now; its cell is drawn.
#[test]
fn compaction_takes_a_staging_denial_as_staging_seed_0xb9e() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        lying_wal_sync: false,
        after_restart: AfterRestart::Nothing,
        clock_law: ClockLaw::PerTick,
        staging_fs: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0xb9e), crate::histories::seed_0xb9e::OPS, &opts) {
        panic!("{failure}");
    }
}
