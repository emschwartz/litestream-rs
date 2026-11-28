//! Liveness at the end of a run: once every injected fault has healed (the stores' and the
//! SQLite source failures still queued), no task may stay stalled on a retried fault that the
//! world no longer imposes.
//!
//! With the defect "a success keeps a stall" planted
//! (`docs/planted-bugs/success-keeps-a-stall.patch`), 2 of seeds 1 to 3000 failed on it unaided
//! (0xa2 first, then 0x31c).

use lsr_core::{StoreError, StoreOp};
use lsr_sim::run::run_ops;
use lsr_sim::{AfterRestart, FaultShape, Op, Opts, Seed, Target};

/// The replica refuses uploads for forty ticks, long enough for the upload task to stall on the
/// fault, then heals. With a task's success leaving its stall in place (the plant
/// `success-keeps-a-stall`), the run ends with "the Upload task is still stalled on StoreTransient
/// after 1 failures with nothing causing it".
#[test]
fn a_stall_ends_with_the_fault_that_caused_it() {
    let mut ops = vec![
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::Rate {
                per_mille: 1000,
                err: StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: std::io::ErrorKind::ConnectionReset,
                },
            },
        },
        Op::Commit {
            pages: 2,
            grow: false,
        },
    ];
    ops.extend(std::iter::repeat_n(Op::Tick, 40));
    ops.push(Op::Heal);
    ops.push(Op::Commit {
        pages: 2,
        grow: false,
    });
    ops.extend(std::iter::repeat_n(Op::Tick, 30));
    if let Err(failure) = run_ops(Seed::new(0x2a8), &ops, &Opts::default()) {
        panic!("{failure}");
    }
}

/// An upload whose answer was a size mismatch leaves the engine believing the replica holds a
/// newest file it does not, and a restart with staging lost then boots over that belief. The
/// boot's look at the replica's newest file must not fail on the missing file: it would read the
/// same file at every retry and the drain would never end. The read outage's length picks which
/// upload the size mismatch lands on, and which lengths leave the believed newest file missing
/// moves whenever the boot's reads change. The scenario therefore runs at every fourth length from
/// 0 to 252 (the lengths that matter come in runs of eight) and must drain at each, so the plant
/// is caught wherever those runs fall.
#[test]
fn a_boot_over_a_believed_newest_file_the_replica_lacks_still_drains() {
    use lsr_core::StoreOp;
    use lsr_sim::FaultShape;
    use lsr_sim::generate::{EnvFault, Hold, Target};
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
    for outage in (0..=255).step_by(4) {
        let ops = [
            Op::Hold {
                hold: Hold::DiskFull,
                ticks: 101,
            },
            Op::Hold {
                hold: Hold::StoreDown(StoreOp::Read),
                ticks: outage,
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
        if let Err(failure) = run_ops(Seed::new(0x6b06_4c43), &ops, &opts) {
            panic!("with a read outage of {outage} ticks: {failure}");
        }
    }
}
