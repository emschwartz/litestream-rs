//! Seed 0x95a's first 83 operations, as the live generator under the seed's swarm mix with holds at 20 in a thousand drew them (the survival campaign).
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).

use lsr_core::{SourceError, SourceOp};
use lsr_ltx::{PageNo, PageSize};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Hold, MetaOp, Op, RemoteOp, WalOp};
use std::io::ErrorKind;

pub const OPS: &[Op] = &[
    Op::LaggingListings {
        writes: 2,
        window: 40,
    },
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::EnvFault(EnvFault::Source(SourceOp::Checkpoint, SourceError::Busy)),
    Op::Hold {
        hold: Hold::Wal(WalOp::FlipLastFrameBit),
        ticks: 6,
    },
    Op::EnvFault(EnvFault::Source(
        SourceOp::ReadPage,
        SourceError::Short {
            pgno: PageNo::new(6, PageSize::new(4096).unwrap()).unwrap(),
        },
    )),
    Op::OtherProcessReader { begin: false },
    Op::LaggingListings {
        writes: 4,
        window: 24,
    },
    Op::Hold {
        hold: Hold::LyingDisk,
        ticks: 2,
    },
    Op::Shrink { pages: 1 },
    Op::OtherProcessReader { begin: true },
    Op::CommitInTick { pages: 1 },
    Op::CommitInTick { pages: 2 },
    Op::Shutdown,
    Op::Verify,
    Op::CommitInTick { pages: 4 },
    Op::CommitInTick { pages: 2 },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Tick,
    Op::CommitInTick { pages: 3 },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::CommitInTick { pages: 2 },
    Op::Meta(MetaOp::Restore),
    Op::StagingDamage(MetaOp::FlipNewestByte),
    Op::Shrink { pages: 3 },
    Op::SecondWriter { ticks: 2 },
    Op::Shutdown,
    Op::ClockJump(47489),
    Op::Restore,
    Op::Meta(MetaOp::FlipNewestByte),
    Op::LaggingListings {
        writes: 5,
        window: 24,
    },
    Op::Shutdown,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::ReadPage),
        op: AppOp::DeleteWal,
    },
    Op::AppInTick {
        at: AppAt::Call(0),
        op: AppOp::Commit {
            pages: 3,
            grow: true,
        },
    },
    Op::Remote(RemoteOp::RollBack),
    Op::EnvFault(EnvFault::WipeStaging),
    Op::ClockJump(93600000),
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::Meta(MetaOp::FlipNewestByte),
    Op::LaggingListings {
        writes: 1,
        window: 21,
    },
    Op::EnvFault(EnvFault::Source(
        SourceOp::Checkpoint,
        SourceError::DiskFull,
    )),
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Shrink { pages: 1 },
    Op::CommitInTick { pages: 2 },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Meta(MetaOp::Save),
    Op::CommitInTick { pages: 1 },
    Op::Shrink { pages: 3 },
    Op::Tick,
    Op::EnvFault(EnvFault::Source(
        SourceOp::Open,
        SourceError::Io(ErrorKind::InvalidData),
    )),
    Op::OtherProcessReader { begin: true },
    Op::OtherProcessReader { begin: false },
    Op::Tick,
    Op::CommitInTick { pages: 3 },
    Op::Remote(RemoteOp::RollBack),
    Op::CommitInTick { pages: 4 },
    Op::EnvFault(EnvFault::Source(
        SourceOp::ReadPage,
        SourceError::Short {
            pgno: PageNo::new(5, PageSize::new(4096).unwrap()).unwrap(),
        },
    )),
    Op::OtherProcessReader { begin: true },
    Op::Remote(RemoteOp::RollBack),
    Op::OtherProcessReader { begin: true },
    Op::LaggingListings {
        writes: 2,
        window: 24,
    },
    Op::OtherProcessReader { begin: false },
    Op::Meta(MetaOp::Save),
    Op::Restore,
    Op::Remote(RemoteOp::RollBack),
    Op::Meta(MetaOp::Save),
    Op::AppInTick {
        at: AppAt::Call(7),
        op: AppOp::DeleteWal,
    },
    Op::Tick,
    Op::EnvFault(EnvFault::Source(SourceOp::BumpSeq, SourceError::DiskFull)),
    Op::Shrink { pages: 2 },
    Op::Remote(RemoteOp::RollBack),
    Op::CommitInTick { pages: 1 },
    Op::Shutdown,
    Op::CommitInTick { pages: 1 },
    Op::OtherProcessReader { begin: true },
    Op::Meta(MetaOp::Save),
    Op::Rollback,
    Op::Tick,
    Op::Shrink { pages: 2 },
    Op::EnvFault(EnvFault::WipeStaging),
    Op::CommitInTick { pages: 1 },
    Op::Hold {
        hold: Hold::LaggingListing,
        ticks: 8,
    },
];
