//! Scenarios that reach every app action point, so no point's count rests on luck.
//!
//! The soak gate ([`crate::marks::gate`]) requires every drawn app action point
//! ([`crate::domain::points`]) and every drawn error cell to be reached. A random history
//! reaches a point only when it arms the app's action in the one tick that makes the engine's
//! call, and several calls are rare: the engine copies the database after a TRUNCATE only
//! when the TRUNCATE moved the WAL, reads the page count only for a snapshot that finds nothing
//! in the WAL, and reads pages only for a snapshot. Over the nightly soaks the rarest points ran
//! about once per 10,000 seeds and dozens ran 1 to 3 times per 1,000, so a soak shorter than the
//! nightly's left one or another at zero by chance. Every point gets a scenario; the other
//! required marks are left to the seeds for the reasons in [`LEFT_TO_THE_SEEDS`].
//!
//! Each [`Coverage`] is a short explicit history that puts the engine in the situation that
//! makes the call and arms the action right before it. The soak runs them all once, ahead of its
//! seeds, and adds the keys each names to its reach ([`Coverage::run`]); a scenario whose
//! situation stops making the call fails the soak, as well as the test that runs them. Only
//! the named keys are added, so a scenario never counts a class the generator is expected to
//! leave undrawn.

use std::num::NonZeroU32;

use lsr_core::SourceOp;

use crate::domain::points::{ACTIONS, Action, point_cell, point_key};
use crate::domain::{Cell, SOURCE_OPS};
use crate::generate::{AppAt, AppOp, MetaOp, Op, WalOp};
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
    /// The TRUNCATE threshold is low, a reader held off the PASSIVE checkpoint that tried first,
    /// and the reader is gone: the next tick runs the barrier's PASSIVE and then the TRUNCATE,
    /// whose call no lock keeps the application's commit out of.
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
            Self::SnapshotRequested | Self::PassiveDue | Self::TruncateDue => {
                vec![Op::Tick, Op::Tick]
            }
        }
    }

    /// The engine's TRUNCATE threshold in frames, when the situation lowers it.
    fn truncate_pages(self) -> Option<NonZeroU32> {
        match self {
            Self::TruncateDue => NonZeroU32::new(2),
            Self::SnapshotRequested | Self::PassiveDue | Self::TornWalBoot => None,
        }
    }
}

/// A short history that reaches the keys it names.
#[derive(Clone, Debug)]
pub struct Coverage {
    name: String,
    truncate_pages: Option<NonZeroU32>,
    ops: Vec<Op>,
    keys: Vec<String>,
}

impl Coverage {
    /// The scenario's name, as a failure shows it.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
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

/// The situation that makes the engine's `call` in its next tick. Exhaustive, so a new call
/// breaks the build here until it has one.
const fn situation(call: SourceOp, action: Action) -> Situation {
    match (call, action) {
        (SourceOp::ReadPage, _) => Situation::SnapshotRequested,
        (SourceOp::Checkpoint, Action::Commit) => Situation::TruncateDue,
        (SourceOp::PageCount, _) => Situation::TornWalBoot,
        (
            SourceOp::Open
            | SourceOp::Len
            | SourceOp::Committed
            | SourceOp::BeginBarrier
            | SourceOp::EndBarrier
            | SourceOp::Checkpoint
            | SourceOp::BumpSeq
            | SourceOp::BeginWriteLock
            | SourceOp::EndWriteLock
            | SourceOp::Sync,
            _,
        ) => Situation::PassiveDue,
    }
}

/// The app's operation for `action`: one page, never growing; a checkpoint keeps its mode.
const fn app_op(action: Action) -> AppOp {
    match action {
        Action::Commit => AppOp::Commit {
            pages: 1,
            grow: false,
        },
        Action::ReaderBegin => AppOp::ReaderBegin,
        Action::ReaderEnd => AppOp::ReaderEnd,
        Action::CheckpointPassive => AppOp::Checkpoint(OtherProcessMode::Passive),
        Action::CheckpointRestart => AppOp::Checkpoint(OtherProcessMode::Restart),
        Action::CheckpointTruncate => AppOp::Checkpoint(OtherProcessMode::Truncate),
        Action::CheckpointFull => AppOp::Checkpoint(OtherProcessMode::Full),
        Action::DeleteWal => AppOp::DeleteWal,
        Action::Spill => AppOp::Spill { pages: 1 },
    }
}

/// The app does `action` right before the engine's `call`, in the armed tick of the situation
/// that makes the call. An action that ends a reader gets one, begun in a tick of its own first.
fn point(call: SourceOp, action: Action) -> Coverage {
    let situation = situation(call, action);
    let mut ops = Vec::new();
    if action == Action::ReaderEnd {
        ops.extend([
            Op::Tick,
            Op::AppInTick {
                at: AppAt::Call(0),
                op: AppOp::ReaderBegin,
            },
        ]);
    }
    ops.extend(situation.setup());
    ops.push(Op::AppInTick {
        at: AppAt::Before(call),
        op: app_op(action),
    });
    ops.extend(situation.after());
    let key = point_key(call, action);
    Coverage {
        name: key.clone(),
        truncate_pages: situation.truncate_pages(),
        ops,
        keys: vec![format!("{key}.ran")],
    }
}

/// One scenario for every app action point the gate requires (`docs/decisions/D256.md`).
///
/// Random histories reach the rarest points too seldom (see the module docs), so every point
/// has a scenario, and a new call or action has one from the start.
#[must_use]
pub fn coverage() -> Vec<Coverage> {
    SOURCE_OPS
        .iter()
        .flat_map(|call| ACTIONS.iter().map(|action| (*call, *action)))
        .filter(|(call, action)| matches!(point_cell(*call, *action), Cell::Drawn))
        .map(|(call, action)| point(call, action))
        .collect()
}

/// The required marks no scenario reaches, by key prefix, with why random histories reach them
/// reliably. `marks::every_required_mark_has_a_scenario_or_a_reason` holds every required key to
/// one or the other. Rates are the rarest key of each family over the nightly soaks of 24 and 25
/// November 2026 (23,017 seeds).
pub const LEFT_TO_THE_SEEDS: &[(&str, &str)] = &[
    (
        "op.",
        "an app or operator step is drawn as a step of its own; the rarest count ran over 66 times per 1,000 seeds",
    ),
    (
        "fault.",
        "a fault class is drawn as a step of its own; the rarest count ran 40 times per 1,000 seeds",
    ),
    (
        "cell.",
        "an error cell is drawn as a fault on its call; the rarest ran 18 times per 1,000 seeds",
    ),
    (
        "liveness.",
        "every run ends with the healthy tail, whose commits are judged unless a stop or a persistent cause ends it first",
    ),
    (
        "wal_bound.",
        "the healthy tail judges the WAL after each commit that reached the replica, unless a fault fired in its window or a reader is open",
    ),
    (
        "barrier.",
        "every checkpoint takes the write barrier, and the seeds checkpoint in nearly every run",
    ),
    (
        "tick_cost.",
        "every tick of every run is judged against its bound",
    ),
];
