//! Seed 0x21's first 130 operations, as the live generator under the seed's own mix with the known-wrong cells drawn drew them.
//!
//! Its forged staged files (`MetaOp::ForgeNewest`) became flipped bytes when forgery left the
//! fault model (`docs/decisions.md` D131).

use lsr_core::{SourceError, SourceOp};
use lsr_sim::generate::{AppAt, AppOp, EnvFault, Hold, MetaOp, Op, RemoteOp, WalOp};
use lsr_sim::{CrashKind, OtherProcessMode};
use std::io::ErrorKind;

pub const OPS: &[Op] = &[
    Op::Hold {
        hold: Hold::ReadersHeld,
        ticks: 11,
    },
    Op::Tick,
    Op::SecondWriter { ticks: 1 },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Open),
        op: AppOp::DeleteWal,
    },
    Op::Heal,
    Op::Spill,
    Op::Crash(CrashKind::Process),
    Op::EnvFault(EnvFault::DeleteWal),
    Op::Heal,
    Op::WalDamage {
        op: WalOp::RegrowSameSalts(5047811086311921380),
        ticks: 3,
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Crash(CrashKind::Power),
    Op::Restore,
    Op::SecondWriter { ticks: 3 },
    Op::Crash(CrashKind::Process),
    Op::AppInTick {
        at: AppAt::Call(14),
        op: AppOp::ReaderBegin,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::EndBarrier),
        op: AppOp::DeleteWal,
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Meta(MetaOp::DeleteAll),
    Op::Heal,
    Op::Heal,
    Op::EnvFault(EnvFault::Source(
        SourceOp::BeginBarrier,
        SourceError::Io(ErrorKind::Other),
    )),
    Op::AppInTick {
        at: AppAt::Call(1),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::Remote(RemoteOp::RollBack),
    Op::Spill,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::BumpSeq),
        op: AppOp::Commit {
            pages: 4,
            grow: true,
        },
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Open),
        op: AppOp::Commit {
            pages: 3,
            grow: false,
        },
    },
    Op::Heal,
    Op::Spill,
    Op::SecondWriter { ticks: 3 },
    Op::Heal,
    Op::Verify,
    Op::EnvFault(EnvFault::Source(SourceOp::BumpSeq, SourceError::DiskFull)),
    Op::ClockJump(53615),
    Op::EnvFault(EnvFault::DeleteWal),
    Op::Crash(CrashKind::Power),
    Op::Meta(MetaOp::DeleteAll),
    Op::WalDamage {
        op: WalOp::TruncateAtFrame(3),
        ticks: 5,
    },
    Op::Commit {
        pages: 2,
        grow: true,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::Commit {
            pages: 4,
            grow: false,
        },
    },
    Op::Hold {
        hold: Hold::CredentialsRevoked,
        ticks: 8,
    },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::Commit {
        pages: 4,
        grow: false,
    },
    Op::EnvFault(EnvFault::Source(SourceOp::BeginBarrier, SourceError::Busy)),
    Op::Crash(CrashKind::Process),
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Spill,
    Op::Spill,
    Op::Heal,
    Op::Remote(RemoteOp::RollBack),
    Op::Tick,
    Op::Tick,
    Op::EnvFault(EnvFault::Source(
        SourceOp::BeginWriteLock,
        SourceError::DiskFull,
    )),
    Op::Restore,
    Op::AppInTick {
        at: AppAt::Call(7),
        op: AppOp::DeleteWal,
    },
    Op::SecondWriter { ticks: 2 },
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::Spill,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Checkpoint),
        op: AppOp::Commit {
            pages: 1,
            grow: false,
        },
    },
    Op::Crash(CrashKind::Process),
    Op::Spill,
    Op::Verify,
    Op::EnvFault(EnvFault::DeleteWal),
    Op::Restore,
    Op::Restore,
    Op::Heal,
    Op::AppInTick {
        at: AppAt::Before(SourceOp::PageCount),
        op: AppOp::ReaderEnd,
    },
    Op::Heal,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 5,
    },
    Op::Crash(CrashKind::Process),
    Op::SecondWriter { ticks: 2 },
    Op::WalDamage {
        op: WalOp::RegrowWithoutCommitMarks(13618443591509156411),
        ticks: 4,
    },
    Op::StagingDamage(MetaOp::TruncateNewest),
    Op::Heal,
    Op::Restore,
    Op::ClockJump(87152),
    Op::Verify,
    Op::Meta(MetaOp::Restore),
    Op::Crash(CrashKind::Power),
    Op::Meta(MetaOp::Save),
    Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
    Op::Restore,
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 3,
    },
    Op::Heal,
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Heal,
    Op::Commit {
        pages: 4,
        grow: true,
    },
    Op::Restore,
    Op::Crash(CrashKind::Power),
    Op::Restore,
    Op::Crash(CrashKind::Power),
    Op::WalDamage {
        op: WalOp::TearHeader,
        ticks: 4,
    },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::Remote(RemoteOp::RollBack),
    Op::Remote(RemoteOp::RollBack),
    Op::Spill,
    Op::ClockJump(76932),
    Op::WalDamage {
        op: WalOp::FlipLastFrameBit,
        ticks: 2,
    },
    Op::AppInTick {
        at: AppAt::Call(16),
        op: AppOp::ReaderEnd,
    },
    Op::Meta(MetaOp::FlipNewestByte),
    Op::EnvFault(EnvFault::Source(
        SourceOp::BeginBarrier,
        SourceError::Io(ErrorKind::PermissionDenied),
    )),
    Op::Heal,
    Op::Commit {
        pages: 1,
        grow: false,
    },
    Op::AppInTick {
        at: AppAt::Call(12),
        op: AppOp::ReaderBegin,
    },
    Op::EnvFault(EnvFault::DeleteWal),
    Op::Tick,
    Op::ClockJump(48242),
    Op::AppInTick {
        at: AppAt::Before(SourceOp::Len),
        op: AppOp::Commit {
            pages: 3,
            grow: true,
        },
    },
    Op::Crash(CrashKind::Process),
    Op::Meta(MetaOp::DeleteAll),
    Op::Meta(MetaOp::Save),
    Op::Heal,
    Op::Heal,
    Op::AppInTick {
        at: AppAt::Call(0),
        op: AppOp::Checkpoint(OtherProcessMode::Truncate),
    },
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::AppInTick {
        at: AppAt::Before(SourceOp::EndBarrier),
        op: AppOp::DeleteWal,
    },
];
