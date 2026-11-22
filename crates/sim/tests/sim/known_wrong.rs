//! The open engine defects the error domain reaches (`lsr_sim::domain::KnownWrong`), each pinned
//! to a seed that must still fail with the defect's signature when its known-wrong cells are
//! drawn. The per-push fixed seeds leave those cells out; the nightly draws them and counts their
//! failures as known.
//!
//! When a defect's fix lands, its test here fails, on purpose: flip the defect's cells in
//! `crates/sim/src/domain.rs` from `Cell::KnownWrong` to `Cell::Drawn`, remove the variant, and
//! move the seed to a test that requires the run to pass. A defect that stays known-wrong after
//! its fix would hide its regressions from the per-push seeds.

use lsr_sim::domain::{KnownWrong, known_in};
use lsr_sim::{Seed, Weights, history, run};

const STEPS: u32 = 300;

fn digest(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Runs `seed` under its mix with the known-wrong cells drawn and requires it to fail with
/// `defect`'s signature; `ops_digest` guards the history against a generator change.
fn still_fails(seed: u64, ops_digest: u64, defect: KnownWrong) {
    let seed = Seed::new(seed);
    let mut weights = Weights::mixed(seed);
    weights.known_wrong = true;
    // The run as it was found: the scheduled verify, drawn last, came after it.
    weights.verify = 0;
    let ops = history(seed, STEPS, &weights);
    assert_eq!(
        digest(&format!("{ops:?}")),
        ops_digest,
        "the live generator changed seed {seed}'s run; find another seed that reaches {defect:?}"
    );
    assert!(
        known_in(&ops).contains(&defect),
        "{seed} draws no cell of {defect:?}"
    );
    match run(seed, STEPS, &weights) {
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
    still_fails(
        0xf,
        0xe00e_589f_5b37_5395,
        KnownWrong::WalSyncForgottenAtRestart,
    );
}

/// Fixed: a staging read refused for permission during level-1 compaction degraded maintenance
/// as the replica's `StoreDenied(Permission)` until #145 routed compaction's local reads through
/// `Fault::from_staging`. Seed 0xb9e, the one that reached it, must pass now; its cell is drawn.
#[test]
fn compaction_takes_a_staging_denial_as_staging_seed_0xb9e() {
    let seed = Seed::new(0xb9e);
    let mut weights = Weights::mixed(seed);
    weights.known_wrong = true;
    // The run as it was found: the scheduled verify, drawn last, came after it.
    weights.verify = 0;
    let ops = history(seed, STEPS, &weights);
    assert_eq!(
        digest(&format!("{ops:?}")),
        0x0f14_5395_bc1a_570e,
        "the live generator changed seed 0xb9e's run; find another seed whose history draws a \
         staging read denied for permission"
    );
    if let Err(failure) = run(seed, STEPS, &weights) {
        panic!("{failure}");
    }
}
