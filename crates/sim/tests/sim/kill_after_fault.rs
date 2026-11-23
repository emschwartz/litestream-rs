//! The kill-after-fault law (`Weights::kill_after_fault`): the process dies right after the first
//! tick in which an injected fault fires, so whatever the fault left the engine owing must
//! be found again by the restarted engine: an obligation held only in memory fails the run.

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
