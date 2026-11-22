//! The open engine defects the error domain reaches (`lsr_sim::domain::KnownWrong`), each pinned
//! to a seed that must still fail with the defect's signature when its known-wrong cells are
//! drawn. The per-push fixed seeds leave those cells out; the nightly draws them and counts their
//! failures as known.
//!
//! When a defect's fix lands, its test here fails, on purpose: flip the defect's cells in
//! `crates/sim/src/domain.rs` from `Cell::KnownWrong` to `Cell::Drawn`, remove the variant, and
//! move the seed to a test that requires the run to pass. A defect that stays known-wrong after
//! its fix would hide its regressions from the per-push seeds.

use lsr_core::{Denied, StoreError, StoreOp};
use lsr_sim::domain::{KnownWrong, known_in};
use lsr_sim::{FaultShape, Op, Seed, Target, Weights, history, run};

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
    // The mix as drawn before the lagging-listings law, which would change the run.
    weights.lagging_listings = 0;
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
/// failed sync covered (D2). Seed 0x21 is the first of the mixed set to reach it.
#[test]
fn a_restart_after_a_failed_wal_sync_still_stages_frames_it_covered_seed_0x21() {
    still_fails(
        0x21,
        0x4c29_c385_a0f0_e711,
        KnownWrong::WalSyncForgottenAtRestart,
    );
}

/// A staging read refused for permission during level-1 compaction is the staging store's
/// fault, not the replica's denial: the seed that pinned the defect now passes (#99).
#[test]
fn a_staging_denial_while_compacting_is_not_the_replicas_seed_0xb9e() {
    let seed = Seed::new(0xb9e);
    let mut weights = Weights::mixed(seed);
    weights.lagging_listings = 0;
    let ops = history(seed, STEPS, &weights);
    assert!(
        ops.iter().any(|op| matches!(
            op,
            Op::StoreFault {
                target: Target::Staging,
                op: StoreOp::Read,
                shape: FaultShape::Error(StoreError::Denied {
                    why: Denied::Permission,
                    ..
                }) | FaultShape::Rate {
                    err: StoreError::Denied {
                        why: Denied::Permission,
                        ..
                    },
                    ..
                },
                ..
            }
        )),
        "{seed} draws no staging read denial; find another seed"
    );
    if let Err(failure) = run(seed, STEPS, &weights) {
        panic!("{failure}");
    }
}
