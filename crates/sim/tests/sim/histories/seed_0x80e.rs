//! Seed 0x80e's history, shrunk to the operations that still fail: under a lagging-listing hold, an as-of restore through the hold's view plans from finer level-0 files than the raw replica's merged level-1 file and gives a newer state that the as-of bound still allows.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::StoreOp;
use lsr_sim::generate::{AppAt, AppOp, Hold, MetaOp, Op, RemoteOp, Target, WalOp};
use lsr_sim::{FaultShape, OtherProcessMode};

pub const OPS: &[Op] = &[
    Op::Shutdown,
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(33),
        ticks: 1,
    },
    Op::ClockJump(71669),
    Op::Shutdown,
    Op::RequestSnapshot,
    Op::ClockJump(-37198),
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(24),
        ticks: 4,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 2,
        shape: FaultShape::ChecksumMismatch,
    },
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(8),
        ticks: 1,
    },
    Op::AppInTick {
        at: AppAt::Call(17),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Shutdown,
    Op::Hold {
        hold: Hold::LaggingListing,
        ticks: 4,
    },
];
