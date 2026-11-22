//! Liveness at the end of a run: once every injected fault has healed (the stores' and the
//! SQLite source failures still queued), no task may stay stalled on a retried fault that the
//! world no longer imposes. Before this check the simulator accepted any stall: a degraded
//! state ended its idle judgment, and a stale one raised no event at all.
//!
//! With the defect "a success keeps a stall" planted
//! (`docs/planted-bugs/success-keeps-a-stall.patch`), 2 of seeds 1 to 3000 fail on it unaided
//! (0xa2 first, then 0x31c) and the fixed tree fails none on it. This test runs that seed under its own mix,
//! and the digest guards its operations: a change to the live generator fails here loudly
//! instead of silently losing the kill.

use lsr_sim::{Seed, Weights, history, run};

const SEED: u64 = 0xa2;
const STEPS: u32 = 300;
const DIGEST: u64 = 0xce89_844f_911d_d122;

fn digest(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

#[test]
fn a_stall_ends_with_the_fault_that_caused_it_seed_0xa2() {
    let seed = Seed::new(SEED);
    let weights = Weights::mixed(seed);
    let ops = history(seed, STEPS, &weights);
    assert_eq!(
        digest(&format!("{ops:?} {weights:?}")),
        DIGEST,
        "the live generator changed seed 0xa2's run; find a seed the plant still fails on"
    );
    if let Err(failure) = run(seed, STEPS, &weights) {
        panic!("{failure}");
    }
}
