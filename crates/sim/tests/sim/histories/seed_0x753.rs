//! Seed 0x753's history of 42 operations, as the live generator under the seed's swarm mix with the block crash law drew it.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceError, SourceOp, StoreError, StoreOp};
use lsr_sim::generate::{EnvFault, Hold, MetaOp, Op, Target, WalOp};
use lsr_sim::{CrashKind, FaultShape, OtherProcessMode};
use std::io::ErrorKind;
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::Shutdown,
    Op::EnvFault(EnvFault::WipeStaging),
    Op::Crash(CrashKind::Process),
    Op::Restore,
    Op::EnvFault(EnvFault::Source(SourceOp::PageCount, SourceError::Busy)),
    Op::Shutdown,
    Op::Tick,
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Heal,
    Op::Restore,
    Op::Hold {
        hold: Hold::Wal(WalOp::FlipLastFrameBit),
        ticks: 7,
    },
    Op::Vacuum { page_size: None },
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Vacuum { page_size: None },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::List,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Heal,
    Op::Crash(CrashKind::Process),
    Op::Shutdown,
    Op::Tick,
    Op::Shutdown,
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Shrink { pages: 3 },
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::List,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::StagingDamage(MetaOp::DeleteNewest),
    Op::OtherProcessReader { begin: true },
    Op::Vacuum { page_size: None },
    Op::Shutdown,
    Op::EnvFault(EnvFault::Source(SourceOp::BeginBarrier, SourceError::Busy)),
    Op::Restore,
    Op::Heal,
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::List,
        ahead: 1,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Restore,
    Op::Restore,
    Op::EnvFault(EnvFault::WipeStaging),
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Crash(CrashKind::Power),
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Crash(CrashKind::Process),
    Op::Tick,
];
