//! The kill-after-fault law (`Weights::kill_after_fault`): the process dies right after the first
//! tick in which an injected fault fires, so whatever the fault left the engine owing must
//! survive a restart. It is the proof of the obligation ledger (`lsr_core::obligations`): with a
//! journalled field's journalling planted out, the law fails runs the swarm without it passes.

use lsr_sim::{Seed, Weights, history, run};

const STEPS: u32 = 300;

fn digest(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

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

/// Seed 0x116, under the swarm mix with the whole error domain and its known-wrong cells drawn
/// and the law on: a failed sync of the WAL whose tick the law ends. The restarted engine still
/// owes the TRUNCATE because the engine journal holds the debt. With the debt's journalling
/// planted out (`journal-debt-not-recorded-sim`) it stages frames the failed sync covered; the
/// same seed without the law passes either way.
#[test]
fn a_kill_right_after_a_failed_wal_sync_still_owes_the_truncate_seed_0x116() {
    let seed = Seed::new(0x116);
    let mut weights = Weights::swarm(seed);
    weights.error_domain = true;
    weights.known_wrong = true;
    weights.kill_after_fault = true;
    assert_eq!(
        digest(&format!("{:?}", history(seed, STEPS, &weights))),
        0xf1c2_a43d_298b_81e4,
        "the live generator changed seed 0x116's run; find another seed the debt plant fails \
         only under the law"
    );
    if let Err(failure) = run(seed, STEPS, &weights) {
        panic!("{failure}");
    }
}
