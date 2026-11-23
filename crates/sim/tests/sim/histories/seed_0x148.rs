//! Seed 0x148's history, as the live generator under the seed's swarm mix with holds at no less than 20 in a thousand (the survival campaign's) drew it, cut after the step that failed.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceOp, StoreError, StoreOp};
use lsr_ltx::PageSize;
use lsr_sim::generate::{AppAt, AppOp, Hold, Op, RemoteOp, Target};
use lsr_sim::{FaultShape, OtherProcessMode};
use std::io::ErrorKind;

pub const OPS: &[Op] = &[
    Op::Rollback,
    Op::Commit {
        pages: 3,
        grow: true,
    },
    Op::Rollback,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::BumpSeq),
        op: AppOp::DeleteWal,
    },
    Op::Vacuum { page_size: None },
    Op::Remote(RemoteOp::RollBack),
    Op::Verify,
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Vacuum { page_size: None },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Rollback,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 2,
        shape: FaultShape::FlipByteOnRead { at: 1340 },
    },
    Op::Verify,
    Op::Spill,
    Op::AppInTick {
        at: AppAt::Call(19),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::AppInTick {
        at: AppAt::Call(12),
        op: AppOp::Commit {
            pages: 4,
            grow: false,
        },
    },
    Op::Vacuum {
        page_size: PageSize::new(2048),
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 1,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Shrink { pages: 3 },
    Op::RequestSnapshot,
    Op::Shrink { pages: 3 },
    Op::Remote(RemoteOp::RollBack),
    Op::Rollback,
    Op::Rollback,
    Op::AppInTick {
        at: AppAt::Call(4),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Hold {
        hold: Hold::DiskFull,
        ticks: 64,
    },
];
