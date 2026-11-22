//! A store that acknowledges an upload one byte short holds a truncated object (the
//! simulator's `FaultShape::SizeMismatch`). Every engine commit compares the acknowledged size
//! with what it wrote and deletes the object on a mismatch, so no restore meets it, at any
//! point. Seed 0x46 of the fixed set reaches the same fault through compaction.

use lsr_core::StoreOp;
use lsr_sim::run::run_ops;
use lsr_sim::{FaultShape, Op, Opts, Seed, Target};

/// The first upload is acknowledged short; the restore right after its tick judges the
/// replica before a retry could replace the object.
#[test]
fn an_upload_acknowledged_short_leaves_nothing_a_restore_meets() {
    let mut ops = vec![
        Op::StoreFault {
            target: Target::Remote,
            op: StoreOp::Upload,
            ahead: 0,
            shape: FaultShape::SizeMismatch,
        },
        Op::Commit {
            pages: 1,
            grow: false,
        },
        Op::Tick,
        Op::Tick,
        Op::Tick,
        Op::Restore,
    ];
    ops.extend(std::iter::repeat_n(Op::Tick, 8));
    ops.push(Op::Restore);
    if let Err(failure) = run_ops(Seed::new(1), &ops, &Opts::default()) {
        panic!("{failure}");
    }
}
