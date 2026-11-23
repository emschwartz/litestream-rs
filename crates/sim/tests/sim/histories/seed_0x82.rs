//! Seed 0x82's first 60 operations, as the mixed generator drew them.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceOp, StoreError, StoreOp};
use lsr_sim::generate::{AppAt, AppOp, Hold, MetaOp, Op, Target, WalOp};
use lsr_sim::{CrashKind, FaultShape};
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::Shutdown,
    Op::CommitInTick { pages: 4 },
    Op::OtherConnectionCommit { pages: 1 },
    Op::LaggingListings {
        writes: 1,
        window: 32,
    },
    Op::CommitInTick { pages: 3 },
    Op::Heal,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Delete,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Delete,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::OtherConnectionCommit { pages: 4 },
    Op::Crash(CrashKind::Power),
    Op::Crash(CrashKind::Process),
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::Crash(CrashKind::Power),
    Op::Tick,
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::Meta(MetaOp::FlipNewestByte),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 2,
        shape: FaultShape::CorruptPublish,
    },
    Op::Vacuum { page_size: None },
    Op::OtherConnectionCommit { pages: 3 },
    Op::Heal,
    Op::Verify,
    Op::CommitInTick { pages: 1 },
    Op::AppInTick {
        at: AppAt::Call(16),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::Meta(MetaOp::Restore),
    Op::Shutdown,
    Op::OtherConnectionCommit { pages: 3 },
    Op::Shrink { pages: 3 },
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::StagingDamage(MetaOp::DeleteNewest),
    Op::Shutdown,
    Op::Heal,
    Op::LaggingListings {
        writes: 1,
        window: 10,
    },
    Op::OtherProcessReader { begin: true },
    Op::RequestSnapshot,
    Op::RequestSnapshot,
    Op::Heal,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::PageCount),
        op: AppOp::DeleteWal,
    },
    Op::OtherProcessReader { begin: true },
    Op::CommitInTick { pages: 2 },
    Op::Verify,
    Op::LaggingListings {
        writes: 8,
        window: 24,
    },
    Op::OtherConnectionCommit { pages: 3 },
    Op::Shutdown,
    Op::Heal,
    Op::Restore,
    Op::AppInTick {
        at: AppAt::Call(13),
        op: AppOp::ReaderEnd,
    },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::List,
        ahead: 1,
        shape: FaultShape::Rate {
            per_mille: 18,
            err: StoreError::Throttled {
                op: StoreOp::List,
                retry_after: Some(Duration::from_millis(2000)),
            },
        },
    },
    Op::Heal,
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Shutdown,
    Op::Hold {
        hold: Hold::Wal(WalOp::RegrowSameSalts(7689895909503221913)),
        ticks: 80,
    },
    Op::OtherConnectionCommit { pages: 3 },
    Op::OtherConnectionCommit { pages: 4 },
    Op::Restore,
    Op::Commit {
        pages: 2,
        grow: true,
    },
    Op::Tick,
    Op::Restore,
    Op::OtherConnectionCommit { pages: 1 },
    Op::Vacuum { page_size: None },
    Op::Shutdown,
];
