//! Seed 0x9f9's history of 43 operations, as the live generator under the seed's own mix drew it.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_sim::OtherProcessMode;
use lsr_sim::generate::{MetaOp, Op};

pub const OPS: &[Op] = &[
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::CommitInTick { pages: 2 },
    Op::Tick,
    Op::Tick,
    Op::Heal,
    Op::Vacuum { page_size: None },
    Op::ClockJump(93600000),
    Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
    Op::Heal,
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Tick,
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::Heal,
    Op::Tick,
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Tick,
    Op::StagingDamage(MetaOp::Restore),
    Op::ClockJump(87795),
    Op::ClockJump(93600000),
    Op::Tick,
    Op::Meta(MetaOp::Save),
    Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
    Op::Tick,
    Op::ClockJump(113033),
    Op::Heal,
    Op::Tick,
    Op::ClockJump(93600000),
    Op::ClockJump(93600000),
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::ClockJump(89474),
    Op::Tick,
    Op::Restore,
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::Restore,
    Op::ClockJump(-1180),
    Op::Tick,
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::CommitInTick { pages: 4 },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Restore,
    Op::ClockJump(2156),
    Op::Tick,
];
