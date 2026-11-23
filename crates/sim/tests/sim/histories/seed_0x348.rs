//! Seed 0x348's first 103 operations, as the live generator under the seed's own mix drew them.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).

use lsr_core::{SourceError, SourceOp, StoreError, StoreOp};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Hold, MetaOp, Op, RemoteOp, Target, WalOp};
use lsr_sim::{FaultShape, OtherProcessMode};
use std::io::ErrorKind;
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::ClockJump(-37129),
    Op::Spill,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Read,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Vacuum { page_size: None },
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::SecondWriter { ticks: 3 },
    Op::Remote(RemoteOp::RollBack),
    Op::Rollback,
    Op::Rollback,
    Op::Spill,
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(18344998601811076923),
        ticks: 1,
    },
    Op::Meta(MetaOp::Save),
    Op::AppInTick {
        at: AppAt::Before(SourceOp::ReadPage),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(4252072395766731580),
        ticks: 0,
    },
    Op::Restore,
    Op::EnvFault(EnvFault::Source(
        SourceOp::PageCount,
        SourceError::Io(ErrorKind::Other),
    )),
    Op::AppInTick {
        at: AppAt::Call(9),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 6,
    },
    Op::Verify,
    Op::Rollback,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 1,
        shape: FaultShape::SizeMismatch,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::ReaderBegin,
    },
    Op::Spill,
    Op::SecondWriter { ticks: 6 },
    Op::Restore,
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Delete,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::ClockJump(24834),
    Op::AppInTick {
        at: AppAt::Call(19),
        op: AppOp::DeleteWal,
    },
    Op::SecondWriter { ticks: 3 },
    Op::SecondWriter { ticks: 6 },
    Op::Rollback,
    Op::Rollback,
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::CommitInTick { pages: 3 },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::ErrorAfterPublish(StoreError::Throttled {
            op: StoreOp::Upload,
            retry_after: None,
        }),
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
    Op::Rollback,
    Op::AppInTick {
        at: AppAt::Call(17),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Checkpoint),
        op: AppOp::Commit {
            pages: 4,
            grow: true,
        },
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Remote(RemoteOp::RollBack),
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(16),
        ticks: 2,
    },
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::Rollback,
    Op::Hold {
        hold: Hold::CredentialsRevoked,
        ticks: 6,
    },
    Op::Hold {
        hold: Hold::ReadersHeld,
        ticks: 4,
    },
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::Verify,
    Op::AppInTick {
        at: AppAt::Call(23),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::Rollback,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 0,
        shape: FaultShape::ErrorAfterPublish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Hold {
        hold: Hold::DiskFull,
        ticks: 5,
    },
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(14666106329148629259),
        ticks: 1,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Verify,
    Op::Restore,
    Op::Remote(RemoteOp::RollBack),
    Op::Restore,
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(3444091502640408184),
        ticks: 3,
    },
    Op::WalDamage {
        op: WalOp::FlipLastFrameBit,
        ticks: 0,
    },
    Op::Restore,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::SecondWriter { ticks: 3 },
    Op::Rollback,
    Op::ClockJump(3406),
    Op::EnvFault(EnvFault::Source(SourceOp::PageCount, SourceError::Busy)),
    Op::Rollback,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::BumpSeq),
        op: AppOp::ReaderEnd,
    },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 5,
    },
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 4,
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
    Op::OtherProcessCheckpoint(OtherProcessMode::Truncate),
    Op::Hold {
        hold: Hold::CredentialsRevoked,
        ticks: 9,
    },
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(12049416926454811131),
        ticks: 5,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Rollback,
    Op::ClockJump(96589),
    Op::SecondWriter { ticks: 1 },
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(13552627155385689293),
        ticks: 0,
    },
    Op::Hold {
        hold: Hold::DiskFull,
        ticks: 7,
    },
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(2),
        ticks: 1,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Sync),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::SecondWriter { ticks: 2 },
    Op::Restore,
    Op::Restore,
    Op::ClockJump(50620),
    Op::EnvFault(EnvFault::Source(SourceOp::Len, SourceError::Busy)),
    Op::SecondWriter { ticks: 7 },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 3,
        shape: FaultShape::ErrorAfterPublish(StoreError::Throttled {
            op: StoreOp::Upload,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::Restore,
    Op::CommitInTick { pages: 3 },
    Op::Rollback,
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(7483546970242746898),
        ticks: 2,
    },
];
