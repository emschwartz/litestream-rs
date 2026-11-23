//! A history written by hand: the shortest that leaves a database file no commit made after a
//! failed WAL sync, with no bucket rollback in it (the rollbacks of seed 0x585's history arm a
//! takeover's spend and take it off this path).
//!
//! A commit is replicated; the engine's next WAL sync fails, so the frames of the commit after it
//! are in the page cache only; the engine drains and lets go of its read mark; another process's
//! checkpoint, whose file descriptor never saw the error, copies those frames into the database
//! file and syncs it; a power loss then cuts the WAL back to its durable frames. The boot after it
//! finds the database file ahead of its WAL (`docs/decisions.md` D103).

use lsr_core::{SourceError, SourceOp};
use lsr_sim::OtherProcessMode;
use lsr_sim::generate::{EnvFault, Op};

pub const OPS: &[Op] = &[
    Op::Commit {
        pages: 2,
        grow: false,
    },
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::EnvFault(EnvFault::Source(SourceOp::Sync, SourceError::FsyncFailed)),
    Op::Commit {
        pages: 3,
        grow: false,
    },
    Op::Tick,
    Op::Shutdown,
    Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
    Op::PowerLoss,
    Op::Tick,
    Op::Tick,
];
