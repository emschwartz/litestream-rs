//! The cure after a failed WAL sync when the kernel evicts the pages the failed sync lost before
//! the cure reads them (issue #197, R2; `docs/decisions.md` D108): SQLite's checkpoint does not
//! verify frames, so a TRUNCATE then copies the disk's older bytes into the database file.

use lsr_core::{SourceError, SourceOp};
use lsr_sim::generate::{EnvFault, Op};
use lsr_sim::run::run_ops;
use lsr_sim::{Opts, Seed};

/// A commit is replicated; the next WAL sync fails; the application commits again; the kernel
/// evicts the pages the failed sync lost; the engine's cure runs; the oracle restores. Before
/// the fix the cure's TRUNCATE copied the evicted frames' older bytes into the database file and
/// the snapshot after it shipped them as the latest state, a state no commit made.
const OPS: &[Op] = &[
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
    Op::EnvFault(EnvFault::EvictPageCache),
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Tick,
    Op::Restore,
];

#[test]
fn an_eviction_before_the_cure_never_ships_the_disk_s_older_bytes() {
    let opts = Opts {
        drawn: false,
        trace: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(1), OPS, &opts) {
        panic!("{failure}\n{}", failure.trace.join("\n"));
    }
}
