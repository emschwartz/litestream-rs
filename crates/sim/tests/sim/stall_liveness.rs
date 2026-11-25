//! Liveness at the end of a run: once every injected fault has healed (the stores' and the
//! SQLite source failures still queued), no task may stay stalled on a retried fault that the
//! world no longer imposes. Before this check the simulator accepted any stall: a degraded
//! state ended its idle judgment, and a stale one raised no event at all.
//!
//! With the defect "a success keeps a stall" planted
//! (`docs/planted-bugs/success-keeps-a-stall.patch`), 2 of seeds 1 to 3000 failed on it unaided
//! (0xa2 first, then 0x31c).

use lsr_core::{StoreError, StoreOp};
use lsr_sim::run::run_ops;
use lsr_sim::{FaultShape, Op, Opts, Seed, Target};

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
