//! Liveness at the end of a run: once every injected fault has healed (the stores' and the
//! SQLite source failures still queued), no task may stay stalled on a retried fault that the
//! world no longer imposes. Before this check the simulator accepted any stall: a degraded
//! state ended its idle judgment, and a stale one raised no event at all.
//!
//! With the defect "a success keeps a stall" planted
//! (`docs/planted-bugs/success-keeps-a-stall.patch`), 2 of seeds 1 to 3000 fail on it unaided
//! (0xa2 first, then 0x31c) and the fixed tree fails none on it. This test runs that seed's history, recorded,
//! with the settings its mix drew written out, so no change to the generator can lose the kill.

use lsr_sim::run::run_ops;
use lsr_sim::{Opts, Seed};

#[test]
fn a_stall_ends_with_the_fault_that_caused_it_seed_0xa2() {
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0xa2), crate::histories::seed_0xa2::OPS, &opts) {
        panic!("{failure}");
    }
}
