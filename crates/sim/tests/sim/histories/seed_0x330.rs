//! Seed 0x330's history of 27 operations, as the generator under the seed's own mix drew it before the merge of #188.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{StoreError, StoreOp};
use lsr_sim::generate::{Op, RemoteOp, Target};
use lsr_sim::{FaultShape, OtherProcessMode};

pub const OPS: &[Op] = &[
    Op::Heal,
    Op::SecondWriter { ticks: 7 },
    Op::CommitInTick { pages: 2 },
    Op::ClockJump(70109),
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Heal,
    Op::ClockJump(32470),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Read,
            retry_after: None,
        }),
    },
    Op::Commit {
        pages: 4,
        grow: true,
    },
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::SecondWriter { ticks: 0 },
    Op::SecondWriter { ticks: 4 },
    Op::Vacuum { page_size: None },
    Op::ClockJump(93600000),
    Op::Heal,
    Op::Vacuum { page_size: None },
    Op::Restore,
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::CommitInTick { pages: 3 },
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::ClockJump(43300),
    Op::RequestSnapshot,
    Op::Remote(RemoteOp::RollBack),
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Delete,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Delete,
            retry_after: None,
        }),
    },
    Op::CommitInTick { pages: 2 },
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::CommitInTick { pages: 2 },
];
