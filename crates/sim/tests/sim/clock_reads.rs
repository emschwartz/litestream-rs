//! Each clock read its own time ([`ClockLaw::EachRead`]): what one reading per tick could not
//! show.
//!
//! Under [`ClockLaw::PerTick`] every read in a tick returned the same time, so a file stamped
//! with the tick's first reading and one stamped after the committed read were equal, and the
//! defect fixed by "stamp a staged file after the committed read that bounds it" (636 ms on the
//! real replicator under load, `docs/planted-bugs/stamp-before-committed-read.patch`) was
//! invisible to the simulator. Under `EachRead` the swarm finds it unaided: with the bug planted,
//! 10 of seeds 1 to 3000 fail the history judge's `AsOf` check (seed 7 first), and none fail on
//! the fixed tree. The run below is the shortest of them, seed 0x681, recorded as its operations
//! so a later generator cannot lose it.

use lsr_sim::run::run_ops;
use lsr_sim::{ClockLaw, Op, Opts, Seed};

fn each_read(seed: u64, ops: &[Op]) {
    let opts = Opts {
        drawn: true,
        clock_law: ClockLaw::EachRead,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(seed), ops, &opts) {
        panic!("{}", failure.violation);
    }
}

/// The application commits after the tick read its clock and before the sync reads the
/// committed end; the file that carries the commit must not be stamped before it, or `AsOf`
/// one millisecond after the stamp restores a commit made after that instant.
#[test]
fn a_commit_after_the_tick_read_its_clock_is_not_stamped_before_it_seed_0x681() {
    let ops = [
        Op::CommitInTick { pages: 2 },
        Op::Rollback,
        Op::Rollback,
        Op::CommitInTick { pages: 3 },
        Op::SecondWriter { ticks: 2 },
        Op::Shutdown,
    ];
    each_read(0x681, &ops);
}

/// The application commits inside the tick, right before the checkpoint's barrier: the seal
/// stages that commit, so it must be stamped after it. Found unaided by the swarm under the
/// planted defect "the seal is stamped at the checkpoint's start"
/// (`docs/planted-bugs/seal-stamped-at-checkpoint-start.patch`): 2 of seeds 1 to 3000 fail, this
/// one first; recorded as its operations, with its mix's block crash law.
#[test]
fn a_commit_before_the_barrier_is_not_stamped_before_it_seed_0x552() {
    use lsr_core::{SourceError, SourceOp};
    use lsr_sim::generate::MetaOp;
    use lsr_sim::{EnvFault, OtherProcessMode};
    let ops = [
        Op::EnvFault(EnvFault::WipeStaging),
        Op::Commit {
            pages: 4,
            grow: true,
        },
        Op::EnvFault(EnvFault::Source(SourceOp::Len, SourceError::Busy)),
        Op::Restore,
        Op::Restore,
        Op::EnvFault(EnvFault::Source(
            SourceOp::PageCount,
            SourceError::Io(std::io::ErrorKind::Other),
        )),
        Op::CommitInTick { pages: 2 },
        Op::Restore,
        Op::EnvFault(EnvFault::Source(SourceOp::BumpSeq, SourceError::Busy)),
        Op::Restore,
        Op::Rollback,
        Op::StagingDamage(MetaOp::ForgeNewest),
        Op::Commit {
            pages: 2,
            grow: false,
        },
        Op::CommitInTick { pages: 1 },
        Op::SecondWriter { ticks: 5 },
        Op::ClockJump(79_037),
        Op::OtherProcessCheckpoint(OtherProcessMode::Passive),
        Op::Restore,
        Op::RequestSnapshot,
        Op::Commit {
            pages: 3,
            grow: false,
        },
        Op::Rollback,
        Op::SecondWriter { ticks: 6 },
        Op::Restore,
        Op::Meta(MetaOp::Restore),
        Op::SecondWriter { ticks: 0 },
        Op::CommitInTick { pages: 2 },
        Op::Spill,
        Op::Rollback,
        Op::CommitInTick { pages: 4 },
        Op::Shrink { pages: 2 },
        Op::Shutdown,
        Op::Restore,
    ];
    let opts = Opts {
        drawn: true,
        clock_law: ClockLaw::EachRead,
        block_crash_law: true,
        ..Opts::default()
    };
    if let Err(failure) = run_ops(Seed::new(0x552), &ops, &opts) {
        panic!("{}", failure.violation);
    }
}
