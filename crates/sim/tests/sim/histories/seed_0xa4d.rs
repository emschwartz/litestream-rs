//! Seed 0xa4d's history of 89 operations, as the generator under the seed's own mix drew it before the merge of #188.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceError, SourceOp, StoreError, StoreOp};
use lsr_ltx::PageSize;
use lsr_sim::generate::{EnvFault, Hold, MetaOp, Op, Target, WalOp};
use lsr_sim::{FaultShape, OtherProcessMode};
use std::io::ErrorKind;
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::StagingDamage(MetaOp::Restore),
    Op::Restore,
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::Commit {
        pages: 1,
        grow: true,
    },
    Op::Heal,
    Op::Vacuum {
        page_size: PageSize::new(1024),
    },
    Op::ClockJump(21045),
    Op::Vacuum {
        page_size: PageSize::new(2048),
    },
    Op::Vacuum { page_size: None },
    Op::Tick,
    Op::OtherProcessReader { begin: false },
    Op::Vacuum {
        page_size: PageSize::new(2048),
    },
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::Heal,
    Op::ClockJump(93600000),
    Op::OtherProcessReader { begin: true },
    Op::ClockJump(13280),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::ChecksumMismatch,
    },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::CorruptPublish,
    },
    Op::Restore,
    Op::Restore,
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::OtherProcessReader { begin: false },
    Op::Spill,
    Op::ClockJump(-37366),
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::ClockJump(-20032),
    Op::EnvFault(EnvFault::WipeStaging),
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Upload,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::OtherProcessReader { begin: false },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Vacuum {
        page_size: PageSize::new(2048),
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Delete,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::ClockJump(117636),
    Op::Hold {
        hold: Hold::LyingDisk,
        ticks: 2,
    },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Spill,
    Op::Vacuum { page_size: None },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Restore,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::CommitInTick { pages: 4 },
    Op::Heal,
    Op::OtherProcessReader { begin: true },
    Op::OtherProcessReader { begin: false },
    Op::CommitInTick { pages: 1 },
    Op::Heal,
    Op::ClockJump(3544),
    Op::Heal,
    Op::OtherProcessReader { begin: true },
    Op::OtherProcessReader { begin: true },
    Op::EnvFault(EnvFault::Source(
        SourceOp::Open,
        SourceError::Io(ErrorKind::Other),
    )),
    Op::Hold {
        hold: Hold::DiskFull,
        ticks: 2,
    },
    Op::OtherProcessReader { begin: true },
    Op::ClockJump(16978),
    Op::Vacuum { page_size: None },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::ChecksumMismatch,
    },
    Op::OtherProcessReader { begin: false },
    Op::ClockJump(4674),
    Op::Tick,
    Op::ClockJump(-31701),
    Op::Hold {
        hold: Hold::LyingReads,
        ticks: 11,
    },
    Op::ClockJump(100217),
    Op::ClockJump(-4921),
    Op::OtherProcessReader { begin: true },
    Op::ClockJump(99611),
    Op::OtherProcessReader { begin: false },
    Op::Shrink { pages: 2 },
    Op::Heal,
    Op::OtherProcessReader { begin: false },
    Op::ClockJump(80774),
    Op::ClockJump(93600000),
    Op::ClockJump(79986),
    Op::Hold {
        hold: Hold::CredentialsRevoked,
        ticks: 8,
    },
    Op::CommitInTick { pages: 2 },
    Op::ClockJump(31420),
    Op::Heal,
    Op::Shrink { pages: 3 },
    Op::EnvFault(EnvFault::Source(
        SourceOp::Len,
        SourceError::Io(ErrorKind::Other),
    )),
    Op::ClockJump(101032),
    Op::Heal,
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::Tick,
    Op::Hold {
        hold: Hold::Wal(WalOp::TruncateAtFrame(1)),
        ticks: 3,
    },
    Op::Vacuum { page_size: None },
    Op::OtherProcessReader { begin: false },
    Op::ClockJump(4648),
    Op::Tick,
];
