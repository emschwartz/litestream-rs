//! Scenarios that reach the marks a soak of a thousand seeds does not reliably reach.
//!
//! The soak gate ([`crate::marks::gate`]) requires every drawn app action point
//! ([`crate::domain::points`]) and every drawn error cell to be reached. A random history
//! reaches a point only when it arms the app's action in the one tick that makes the engine's
//! call, and several calls are rare: the engine takes the write lock only at the end of a
//! TRUNCATE, reads the page count only for a snapshot that finds nothing in the WAL, and reads
//! pages only for a snapshot. Measured over 6,000 seeds, the rarest of these points ran
//! 0 to 3 times per 1,000 seeds, so a soak of 1,600 seeds left each at zero about as often as
//! it reached it.
//!
//! Each [`Coverage`] is a short explicit history that puts the engine in the situation that
//! makes the call and arms the action right before it. The soak runs them all once, ahead of its
//! seeds, and adds the keys each names to its reach ([`Coverage::run`]); a scenario whose
//! situation stops making the call fails the soak, as well as the test that runs them. Only
//! the named keys are added, so a scenario never counts a class the generator is expected to
//! leave undrawn.

use std::num::NonZeroU32;

use lsr_core::{SourceError, SourceOp};

use crate::domain::points::{Action, point_key};
use crate::generate::{AppAt, AppOp, EnvFault, MetaOp, Op, WalOp};
use crate::reach::Reach;
use crate::run::{Opts, run_ops};
use crate::seed::Seed;
use crate::sqlite::OtherProcessMode;

/// The seed every coverage scenario runs under; it draws the salts SQLite picks.
const SEED: u64 = 1;

/// The state the engine is put in before the armed tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Situation {
    /// The operator asked for a snapshot, which reads the database's pages.
    SnapshotRequested,
    /// Enough frames are waiting that the engine's next tick runs a PASSIVE checkpoint.
    PassiveDue,
    /// As [`Self::PassiveDue`], with an app reader open that the armed action can end.
    PassiveDueWithReader,
    /// The TRUNCATE threshold is low, a reader held off the PASSIVE checkpoint that tried first,
    /// and the reader is gone: the next tick truncates the WAL and snapshots it under the write
    /// lock.
    TruncateDue,
    /// The engine is down, its newest staged file is gone and the WAL's header is torn: the
    /// next tick boots, finds nothing readable in the WAL and snapshots from the database file,
    /// which reads the page count.
    TornWalBoot,
}

impl Situation {
    fn setup(self) -> Vec<Op> {
        let commit = |pages| Op::Commit { pages, grow: false };
        match self {
            Self::SnapshotRequested => vec![Op::Tick, commit(2), Op::RequestSnapshot],
            Self::PassiveDue => {
                let mut ops = vec![Op::Tick];
                ops.extend((0..6).map(|_| commit(2)));
                ops
            }
            Self::PassiveDueWithReader => {
                let mut ops = vec![
                    Op::Tick,
                    Op::AppInTick {
                        at: AppAt::Call(0),
                        op: AppOp::ReaderBegin,
                    },
                ];
                ops.extend((0..6).map(|_| commit(2)));
                ops
            }
            Self::TruncateDue => vec![
                Op::Tick,
                Op::OtherProcessReader { begin: true },
                commit(2),
                commit(2),
                commit(2),
                Op::Tick,
                commit(2),
                Op::OtherProcessReader { begin: false },
            ],
            Self::TornWalBoot => vec![
                Op::Shutdown,
                Op::StagingDamage(MetaOp::DeleteNewest),
                Op::Wal(WalOp::TearHeader),
            ],
        }
    }

    /// What follows the armed tick: the torn header is healed as an application would, and the
    /// engine settles, so the run's own checks can judge it.
    fn after(self) -> Vec<Op> {
        match self {
            Self::TornWalBoot => vec![
                Op::WalDamage {
                    op: WalOp::TearHeader,
                    ticks: 0,
                },
                Op::Tick,
                Op::Tick,
            ],
            Self::SnapshotRequested
            | Self::PassiveDue
            | Self::PassiveDueWithReader
            | Self::TruncateDue => vec![Op::Tick, Op::Tick],
        }
    }

    /// The engine's TRUNCATE threshold in frames, when the situation lowers it.
    fn truncate_pages(self) -> Option<NonZeroU32> {
        match self {
            Self::TruncateDue => NonZeroU32::new(2),
            Self::SnapshotRequested
            | Self::PassiveDue
            | Self::PassiveDueWithReader
            | Self::TornWalBoot => None,
        }
    }
}

/// A short history that reaches the keys it names.
#[derive(Clone, Debug)]
pub struct Coverage {
    name: &'static str,
    truncate_pages: Option<NonZeroU32>,
    ops: Vec<Op>,
    keys: Vec<String>,
}

impl Coverage {
    /// The scenario's name, as a failure shows it.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The reach keys the scenario exists for.
    #[must_use]
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Runs the history and returns the named keys it reached, with their counts.
    ///
    /// # Errors
    /// The run failed a check of its own, or a named key was not reached: the situation no
    /// longer makes the engine call what the key is for.
    pub fn run(&self) -> Result<Reach, String> {
        let opts = Opts {
            truncate_pages: self.truncate_pages,
            ..Opts::default()
        };
        let report = run_ops(Seed::new(SEED), &self.ops, &opts).map_err(|failure| {
            format!(
                "{}: failed at step {}: {}",
                self.name, failure.step, failure.violation
            )
        })?;
        let mut reached = Reach::default();
        for key in &self.keys {
            let n = report.reach.get(key);
            if n == 0 {
                return Err(format!("{}: the run never reached {key}", self.name));
            }
            reached.add(key.clone(), n);
        }
        Ok(reached)
    }
}

/// The app does `op` right before the engine's `call`, in `situation`'s armed tick.
fn point(name: &'static str, situation: Situation, call: SourceOp, op: AppOp) -> Coverage {
    let mut ops = situation.setup();
    ops.push(Op::AppInTick {
        at: AppAt::Before(call),
        op,
    });
    ops.extend(situation.after());
    Coverage {
        name,
        truncate_pages: situation.truncate_pages(),
        ops,
        keys: vec![format!("{}.ran", point_key(call, Action::of(op)))],
    }
}

/// Every scenario, one per mark it exists for.
///
/// Each names why the soak needs it: the mark's measured rate over 6,000 seeds is in
/// `docs/decisions/` as part of the entry that added this list.
#[must_use]
pub fn coverage() -> Vec<Coverage> {
    let passive = AppOp::Checkpoint(OtherProcessMode::Passive);
    let restart = AppOp::Checkpoint(OtherProcessMode::Restart);
    let mut all = vec![
        point(
            "reader_begins_before_a_snapshot_reads_a_page",
            Situation::SnapshotRequested,
            SourceOp::ReadPage,
            AppOp::ReaderBegin,
        ),
        point(
            "passive_checkpoint_before_a_snapshot_reads_a_page",
            Situation::SnapshotRequested,
            SourceOp::ReadPage,
            passive,
        ),
        point(
            "restart_checkpoint_before_a_snapshot_reads_a_page",
            Situation::SnapshotRequested,
            SourceOp::ReadPage,
            restart,
        ),
        point(
            "passive_checkpoint_before_the_engines_checkpoint",
            Situation::PassiveDue,
            SourceOp::Checkpoint,
            passive,
        ),
        point(
            "restart_checkpoint_before_the_sequence_bump",
            Situation::PassiveDue,
            SourceOp::BumpSeq,
            restart,
        ),
        point(
            "reader_ends_before_the_sequence_bump",
            Situation::PassiveDueWithReader,
            SourceOp::BumpSeq,
            AppOp::ReaderEnd,
        ),
        point(
            "reader_ends_before_the_barrier_begins",
            Situation::PassiveDueWithReader,
            SourceOp::BeginBarrier,
            AppOp::ReaderEnd,
        ),
        point(
            "reader_ends_before_the_barrier_ends",
            Situation::PassiveDueWithReader,
            SourceOp::EndBarrier,
            AppOp::ReaderEnd,
        ),
        point(
            "commit_before_the_write_lock",
            Situation::TruncateDue,
            SourceOp::BeginWriteLock,
            AppOp::Commit {
                pages: 1,
                grow: false,
            },
        ),
        point(
            "delete_wal_before_the_page_count",
            Situation::TornWalBoot,
            SourceOp::PageCount,
            AppOp::DeleteWal,
        ),
    ];
    all.push(disk_full_at_the_write_lock());
    all
}

/// The write lock fails for lack of disk, in the tick that takes it.
fn disk_full_at_the_write_lock() -> Coverage {
    let situation = Situation::TruncateDue;
    let mut ops = situation.setup();
    ops.push(Op::EnvFault(EnvFault::Source(
        SourceOp::BeginWriteLock,
        SourceError::DiskFull,
    )));
    ops.push(Op::Tick);
    ops.extend(situation.after());
    ops.extend(vec![Op::Tick; 6]);
    Coverage {
        name: "disk_full_at_the_write_lock",
        truncate_pages: situation.truncate_pages(),
        ops,
        keys: vec!["cell.source.BeginWriteLock.DiskFull.hit".to_owned()],
    }
}
