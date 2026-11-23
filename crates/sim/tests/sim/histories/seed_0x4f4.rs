//! Seed 0x4f4's history under its swarm mix, shrunk to the 72 operations that still fail when the probe past a listing names no level 1 to 8 key: under lagging listings, bucket rollbacks and a second writer, a restore of a transaction then differs from its commit.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceError, SourceOp, StoreError, StoreOp};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, MetaOp, Op, RemoteOp, Target, WalOp};
use lsr_sim::{FaultShape, OtherProcessMode};
use std::io::ErrorKind;

pub const OPS: &[Op] = &[
    Op::AppInTick {
        at: AppAt::Before(SourceOp::PageCount),
        op: AppOp::ReaderEnd,
    },
    Op::AppInTick {
        at: AppAt::Call(14),
        op: AppOp::ReaderBegin,
    },
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 6,
    },
    Op::EnvFault(EnvFault::Source(SourceOp::Checkpoint, SourceError::Busy)),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::Rate {
            per_mille: 35,
            err: StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::ConnectionReset,
            },
        },
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::BeginWriteLock),
        op: AppOp::DeleteWal,
    },
    Op::Tick,
    Op::Tick,
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(3961554088562407227),
        ticks: 2,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Delete,
        ahead: 1,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Tick,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::CorruptPublish,
    },
    Op::Tick,
    Op::ClockJump(105681),
    Op::Verify,
    Op::WalDamage {
        op: WalOp::FlipLastFrameBit,
        ticks: 4,
    },
    Op::AppInTick {
        at: AppAt::Call(10),
        op: AppOp::Commit {
            pages: 1,
            grow: false,
        },
    },
    Op::Commit {
        pages: 1,
        grow: true,
    },
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Call(21),
        op: AppOp::Checkpoint(OtherProcessMode::Restart),
    },
    Op::Remote(RemoteOp::RollBack),
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(11522890981633033340),
        ticks: 2,
    },
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(5562755892486104023),
        ticks: 7,
    },
    Op::AppInTick {
        at: AppAt::Call(21),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Committed),
        op: AppOp::Checkpoint(OtherProcessMode::Restart),
    },
    Op::Commit {
        pages: 2,
        grow: true,
    },
    Op::Tick,
    Op::LaggingListings {
        writes: 5,
        window: 26,
    },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Tick,
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 5,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 1,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Read,
            retry_after: None,
        }),
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Read,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::ClockJump(46629),
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::CorruptPublish,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::Tick,
    Op::RequestSnapshot,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Committed),
        op: AppOp::DeleteWal,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::Commit {
            pages: 2,
            grow: false,
        },
    },
    Op::RequestSnapshot,
    Op::Tick,
    Op::Remote(RemoteOp::RollBack),
    Op::LaggingListings {
        writes: 1,
        window: 24,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Remote(RemoteOp::RollBack),
    Op::LaggingListings {
        writes: 3,
        window: 14,
    },
    Op::LaggingListings {
        writes: 2,
        window: 29,
    },
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Tick,
    Op::Remote(RemoteOp::RollBack),
    Op::Tick,
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::EndBarrier),
        op: AppOp::ReaderBegin,
    },
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::Tick,
    Op::AppInTick {
        at: AppAt::Call(9),
        op: AppOp::Commit {
            pages: 4,
            grow: false,
        },
    },
    Op::Tick,
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(11748807062145564090),
        ticks: 7,
    },
    Op::EnvFault(EnvFault::WipeStaging),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Read,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 0,
        shape: FaultShape::Rate {
            per_mille: 85,
            err: StoreError::Transient {
                op: StoreOp::List,
                kind: ErrorKind::ConnectionReset,
            },
        },
    },
    Op::SecondWriter { ticks: 5 },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::ClockJump(55956),
    Op::EnvFault(EnvFault::WipeStaging),
    Op::SecondWriter { ticks: 4 },
    Op::AppInTick {
        at: AppAt::Call(22),
        op: AppOp::DeleteWal,
    },
    Op::AppInTick {
        at: AppAt::Call(2),
        op: AppOp::DeleteWal,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::ReadPage),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
];
