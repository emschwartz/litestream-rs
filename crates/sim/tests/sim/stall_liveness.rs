//! Liveness at the end of a run: once every injected fault has healed (the stores' and the
//! SQLite source failures still queued), no task may stay stalled on a retried fault that the
//! world no longer imposes. Before this check the simulator accepted any stall: a degraded
//! state ended its idle judgment, and a stale one raised no event at all.
//!
//! With the defect "a success keeps a stall" planted
//! (`docs/planted-bugs/success-keeps-a-stall.patch`), 2 of seeds 1 to 3000 failed on it unaided
//! (0xa2 first, then 0x31c). With the engine reading past its listings (#96) seed 0xa2 no longer
//! reaches the stall, so this test runs 0x31c, which still does. Its history is recorded, with the
//! settings its mix drew written out, so no change to the generator can lose the kill.

use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Opts, Seed};

// BATCH-3: on the merged tree (the takeover handshake and the read-past listings together)
// neither 0x31c nor 0xf434c fails under plant success-keeps-a-stall, and no seed of 6000 swarm
// seeds from 1000001 does either: find a seed that reaches the stall, or a scenario.
#[test]
fn a_stall_ends_with_the_fault_that_caused_it_seed_0x31c() {
    let opts = Opts {
        drawn: true,
        block_crash_law: false,
        lying_wal_sync: true,
        after_restart: AfterRestart::Nothing,
        clock_law: ClockLaw::PerTick,
        staging_fs: false,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x31c), crate::histories::seed_0x31c::OPS, &opts) {
        panic!("{failure}");
    }
}
