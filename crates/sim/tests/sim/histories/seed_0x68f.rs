//! Seed 0x68f's first 39 operations, as the live generator under the seed's own mix drew them.

use lsr_core::{Denied, SourceOp, StoreError, StoreOp};
use lsr_sim::FaultShape;
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Op, Target, WalOp};

pub const OPS: &[Op] = &[
    Op::Rollback,
    Op::Restore,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Spill,
    Op::Rollback,
    Op::Spill,
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(21),
        ticks: 7,
    },
    Op::Spill,
    Op::Spill,
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Rollback,
    Op::WalDamage {
        op: WalOp::FlipLastFrameBit,
        ticks: 7,
    },
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(5775859838715966494),
        ticks: 3,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 1,
        shape: FaultShape::CorruptPublish,
    },
    Op::Rollback,
    Op::Spill,
    Op::Vacuum { page_size: None },
    Op::EnvFault(EnvFault::WipeStaging),
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::SecondWriter { ticks: 2 },
    Op::WalDamage {
        op: WalOp::FlipLastFrameBit,
        ticks: 5,
    },
    Op::Spill,
    Op::Vacuum { page_size: None },
    Op::Rollback,
    Op::AppInTick {
        at: AppAt::Call(11),
        op: AppOp::Commit {
            pages: 4,
            grow: false,
        },
    },
    Op::Spill,
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Checkpoint),
        op: AppOp::DeleteWal,
    },
    Op::Rollback,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Denied {
            op: StoreOp::Read,
            why: Denied::Permission,
        }),
    },
    Op::Rollback,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::PageCount),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::AppInTick {
        at: AppAt::Call(17),
        op: AppOp::Commit {
            pages: 3,
            grow: true,
        },
    },
];
