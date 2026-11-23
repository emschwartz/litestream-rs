//! Swarm seed 0xf4721's history, shrunk to 89 operations, with its four forged staging and meta files replaced by restored backups (the failure needs a change there, not a forgery): an operator removed another writer's level-0 file 2-2 after maintenance had merged it into level-1 2-2, a lagging listing hid the level-1 file, and the engine took id 2 as free and uploaded its own 2, so restoring the latest state gave a state no commit made.
//!
//! Recorded with `lsr_sim::record`, so no change to the generator gives the regression
//! another history. Edit by hand only to shrink it, and run its planted bug after.

use lsr_core::{SourceError, SourceOp, StoreError, StoreOp};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Hold, MetaOp, Op, RemoteOp, Target, WalOp};
use lsr_sim::{FaultShape, OtherProcessMode};
use std::io::ErrorKind;
use std::time::Duration;

pub const OPS: &[Op] = &[
    Op::Meta(MetaOp::Restore),
    Op::AppInTick {
        at: AppAt::Before(SourceOp::ReadPage),
        op: AppOp::Commit {
            pages: 2,
            grow: false,
        },
    },
    Op::CommitInTick { pages: 3 },
    Op::LaggingListings {
        writes: 1,
        window: 40,
    },
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 7,
    },
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::List,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::SecondWriter { ticks: 6 },
    Op::Vacuum { page_size: None },
    Op::Remote(RemoteOp::RollBack),
    Op::LaggingListings {
        writes: 3,
        window: 35,
    },
    Op::CommitInTick { pages: 2 },
    Op::Vacuum { page_size: None },
    Op::SecondWriter { ticks: 4 },
    Op::LaggingListings {
        writes: 6,
        window: 37,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Throttled {
            op: StoreOp::Read,
            retry_after: Some(Duration::from_millis(2000)),
        }),
    },
    Op::EnvFault(EnvFault::DeleteWal),
    Op::Restore,
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(2071946396708064949),
        ticks: 4,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Remote(RemoteOp::RollBack),
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(18),
        ticks: 3,
    },
    Op::Meta(MetaOp::Restore),
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::Delete,
        ahead: 2,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::EnvFault(EnvFault::Source(SourceOp::PageCount, SourceError::Busy)),
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::LaggingListings {
        writes: 7,
        window: 3,
    },
    Op::SecondWriter { ticks: 1 },
    Op::LaggingListings {
        writes: 3,
        window: 32,
    },
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::SecondWriter { ticks: 7 },
    Op::Vacuum { page_size: None },
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::Tick,
    Op::Restore,
    Op::StagingDamage(MetaOp::Restore),
    Op::RequestSnapshot,
    Op::Hold {
        hold: Hold::Wal(WalOp::RegrowSameSalts(9429303579300800950)),
        ticks: 72,
    },
    Op::SecondWriter { ticks: 1 },
    Op::Tick,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 3,
        shape: FaultShape::Rate {
            per_mille: 50,
            err: StoreError::Transient {
                op: StoreOp::List,
                kind: ErrorKind::ConnectionReset,
            },
        },
    },
    Op::SecondWriter { ticks: 6 },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Delete,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::EnvFault(EnvFault::WipeStaging),
    Op::Meta(MetaOp::Restore),
    Op::Meta(MetaOp::Restore),
    Op::Vacuum { page_size: None },
    Op::LaggingListings {
        writes: 3,
        window: 21,
    },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Upload,
        ahead: 2,
        shape: FaultShape::CorruptPublish,
    },
    Op::LaggingListings {
        writes: 7,
        window: 29,
    },
    Op::Tick,
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(17286754991658344123),
        ticks: 5,
    },
    Op::Rollback,
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 1,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::SecondWriter { ticks: 2 },
    Op::SecondWriter { ticks: 4 },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::LaggingListings {
        writes: 1,
        window: 39,
    },
    Op::SecondWriter { ticks: 4 },
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::LaggingListings {
        writes: 8,
        window: 4,
    },
    Op::StagingDamage(MetaOp::Restore),
    Op::Vacuum { page_size: None },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::List,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::RequestSnapshot,
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::Vacuum { page_size: None },
    Op::Vacuum { page_size: None },
    Op::StoreFault {
        target: Target::Staging,
        op: StoreOp::Read,
        ahead: 0,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::Read,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::SecondWriter { ticks: 6 },
    Op::LaggingListings {
        writes: 8,
        window: 39,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::StoreFault {
        target: Target::Remote,
        op: StoreOp::List,
        ahead: 3,
        shape: FaultShape::Error(StoreError::Transient {
            op: StoreOp::List,
            kind: ErrorKind::ConnectionReset,
        }),
    },
    Op::Tick,
    Op::Hold {
        hold: Hold::DiskFull,
        ticks: 79,
    },
    Op::SecondWriter { ticks: 6 },
    Op::Vacuum { page_size: None },
    Op::CommitInTick { pages: 1 },
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::LaggingListings {
        writes: 4,
        window: 10,
    },
    Op::RequestSnapshot,
    Op::OtherProcessCheckpoint(OtherProcessMode::Restart),
    Op::LaggingListings {
        writes: 8,
        window: 28,
    },
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(31),
        ticks: 4,
    },
];
