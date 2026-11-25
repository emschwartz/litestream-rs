//! Liveness at the end of a run: once every injected fault has healed (the stores' and the
//! SQLite source failures still queued), no task may stay stalled on a retried fault that the
//! world no longer imposes. Before this check the simulator accepted any stall: a degraded
//! state ended its idle judgment, and a stale one raised no event at all.
//!
//! With the defect "a success keeps a stall" planted
//! (`docs/planted-bugs/success-keeps-a-stall.patch`), 2 of seeds 1 to 3000 failed on it unaided
//! (0xa2 first, then 0x31c). With the engine reading past its listings (#96) seed 0xa2 no longer
//! reached the stall, and on the tree that also stops at another writer neither did 0x31c. Seed
//! 0x2a8 of the fixed set does; its history is recorded, with the settings its mix drew written
//! out, so no change to the generator can lose the kill.

use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, ClockLaw, Op, Opts, Seed};

/// Seed 0x2a8 of the fixed set's mix, recorded: a stall on a retried fault outlasts every
/// injected fault. With a task's success leaving its stall in place (the plant
/// `success-keeps-a-stall`), the run ends with "the Sync task is still stalled on WalMoved after
/// 1 failures with nothing causing it".
#[test]
fn a_stall_ends_with_the_fault_that_caused_it_seed_0x2a8() {
    let opts = Opts {
        drawn: true,
        block_crash_law: false,
        lying_wal_sync: false,
        after_restart: AfterRestart::Nothing,
        clock_law: ClockLaw::EachRead,
        overlap: true,
        staging_fs: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x2a8), crate::histories::seed_0x2a8::OPS, &opts) {
        panic!("{failure}");
    }
}

/// An upload whose answer was a size mismatch leaves the engine believing the replica holds a
/// newest file it does not, and a restart with staging lost then boots over that belief. The
/// boot's look at the replica's newest file must not fail on the missing file: it would read the
/// same file at every retry and the drain would never end (seed 0x6b064c43, reduced).
#[test]
fn a_boot_over_a_believed_newest_file_the_replica_lacks_still_drains() {
    use lsr_core::StoreOp;
    use lsr_sim::FaultShape;
    use lsr_sim::generate::{EnvFault, Hold, Target};
    let ops = [
        Op::Hold {
            hold: Hold::DiskFull,
            ticks: 101,
        },
        Op::Hold {
            hold: Hold::StoreDown(StoreOp::Read),
            ticks: 138,
        },
        Op::Shutdown,
        Op::EnvFault(EnvFault::WipeStaging),
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::SizeMismatch,
        },
        Op::SecondWriter { ticks: 3 },
    ];
    let opts = Opts {
        drawn: true,
        block_crash_law: true,
        after_restart: AfterRestart::Nothing,
        overlap: true,
        error_domain: true,
        kill_after_fault: true,
        remote_s3: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x6b06_4c43), &ops, &opts) {
        panic!("{failure}");
    }
}
