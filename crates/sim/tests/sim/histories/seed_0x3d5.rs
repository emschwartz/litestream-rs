//! Seed 0x3d5's history, its first 104 operations, as the live generator drew it under its mix
//! (`Weights::mixed`).
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::SourceOp;
use lsr_sim::OtherProcessMode;
use lsr_sim::generate::{AppAt, AppOp, Hold, MetaOp, Op, RemoteOp};

pub const OPS: &[Op] = &[
    Op::Spill,
    Op::Shrink { pages: 1 },
    Op::Rollback,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::EndBarrier),
        op: AppOp::ReaderEnd,
    },
    Op::Spill,
    Op::Tick,
    Op::Heal,
    Op::RequestSnapshot,
    Op::Meta(MetaOp::ForgeNewest),
    Op::LaggingListings {
        writes: 1,
        window: 9,
    },
    Op::Restore,
    Op::Tick,
    Op::Spill,
    Op::Spill,
    Op::Rollback,
    Op::Shrink { pages: 3 },
    Op::Commit {
        pages: 1,
        grow: true,
    },
    Op::Spill,
    Op::Tick,
    Op::Heal,
    Op::Shrink { pages: 3 },
    Op::RequestSnapshot,
    Op::Shrink { pages: 2 },
    Op::Meta(MetaOp::Save),
    Op::AppInTick {
        at: AppAt::Call(12),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Committed),
        op: AppOp::ReaderBegin,
    },
    Op::LaggingListings {
        writes: 7,
        window: 8,
    },
    Op::Shrink { pages: 2 },
    Op::Shrink { pages: 3 },
    Op::Shrink { pages: 1 },
    Op::Tick,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::BumpSeq),
        op: AppOp::Checkpoint(OtherProcessMode::Restart),
    },
    Op::Shrink { pages: 1 },
    Op::Shrink { pages: 2 },
    Op::StagingDamage(MetaOp::ForgeNewest),
    Op::Verify,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Open),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::RequestSnapshot,
    Op::Tick,
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Hold {
        hold: Hold::CredentialsRevoked,
        ticks: 7,
    },
    Op::Hold {
        hold: Hold::LaggingListing,
        ticks: 8,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::DeleteWal,
    },
    Op::LaggingListings {
        writes: 1,
        window: 3,
    },
    Op::AppInTick {
        at: AppAt::Call(7),
        op: AppOp::Checkpoint(OtherProcessMode::Restart),
    },
    Op::Tick,
    Op::Shrink { pages: 1 },
    Op::RequestSnapshot,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::ReadPage),
        op: AppOp::ReaderEnd,
    },
    Op::Verify,
    Op::LaggingListings {
        writes: 5,
        window: 26,
    },
    Op::Verify,
    Op::Verify,
    Op::RequestSnapshot,
    Op::StagingDamage(MetaOp::ForgeNewest),
    Op::Shrink { pages: 1 },
    Op::RequestSnapshot,
    Op::Hold {
        hold: Hold::LaggingListing,
        ticks: 5,
    },
    Op::Restore,
    Op::Meta(MetaOp::ForgeNewest),
    Op::Remote(RemoteOp::RollBack),
    Op::Meta(MetaOp::Save),
    Op::Shrink { pages: 1 },
    Op::Heal,
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Call(12),
        op: AppOp::ReaderEnd,
    },
    Op::Heal,
    Op::Shrink { pages: 1 },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::LaggingListings {
        writes: 8,
        window: 21,
    },
    Op::Shrink { pages: 2 },
    Op::Commit {
        pages: 3,
        grow: true,
    },
    Op::Shrink { pages: 2 },
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Call(15),
        op: AppOp::ReaderBegin,
    },
    Op::Heal,
    Op::AppInTick {
        at: AppAt::Call(5),
        op: AppOp::Commit {
            pages: 1,
            grow: false,
        },
    },
    Op::AppInTick {
        at: AppAt::Call(4),
        op: AppOp::Checkpoint(OtherProcessMode::Passive),
    },
    Op::Meta(MetaOp::ForgeNewest),
    Op::RequestSnapshot,
    Op::Spill,
    Op::Meta(MetaOp::Restore),
    Op::Meta(MetaOp::Save),
    Op::Verify,
    Op::Tick,
    Op::Tick,
    Op::Restore,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Checkpoint),
        op: AppOp::ReaderEnd,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::BeginBarrier),
        op: AppOp::Commit {
            pages: 4,
            grow: false,
        },
    },
    Op::Heal,
    Op::Shutdown,
    Op::Heal,
    Op::Heal,
    Op::Spill,
    Op::Shrink { pages: 3 },
    Op::Shrink { pages: 3 },
    Op::Meta(MetaOp::ForgeNewest),
    Op::Meta(MetaOp::Restore),
    Op::Spill,
    Op::RequestSnapshot,
    Op::Tick,
    Op::Hold {
        hold: Hold::LyingReads,
        ticks: 7,
    },
];
