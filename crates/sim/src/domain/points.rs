//! The app's action points: every kind of engine call into SQLite, crossed with every action the
//! app can take right before it inside a tick ([`crate::Op::AppInTick`] with
//! [`AppAt::Before`]).
//!
//! The engine's read-lock proofs are claims about what the app can do between two of the
//! engine's calls, so each pair is a cell of its own: `point.<SourceOp>.<Action>`, counted
//! `.ran` when the action ran right before that call and `.missed` when the tick made no such
//! call. A drawn point must run at least once over a soak; an impossible one says why no tick
//! can reach it. "Ran" means the app acted right before the call, or tried to: an action SQLite
//! refuses there still exercises the point. Points by call number ([`AppAt::Call`]) are not cells: a number names no step
//! of the protocol, and the kinds already cover every step.

use lsr_core::SourceOp;

use super::Cell;
use super::SOURCE_OPS;
use super::marks::source_op_name;
use crate::generate::{AppAt, AppOp};
use crate::sqlite::OtherProcessMode;

/// An app action, without its payload; a checkpoint keeps its mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// [`AppOp::Commit`].
    Commit,
    /// [`AppOp::ReaderBegin`].
    ReaderBegin,
    /// [`AppOp::ReaderEnd`].
    ReaderEnd,
    /// [`AppOp::Checkpoint`] in PASSIVE mode.
    CheckpointPassive,
    /// [`AppOp::Checkpoint`] in RESTART mode.
    CheckpointRestart,
    /// [`AppOp::Checkpoint`] in TRUNCATE mode.
    CheckpointTruncate,
    /// [`AppOp::Checkpoint`] in FULL mode.
    CheckpointFull,
    /// [`AppOp::DeleteWal`].
    DeleteWal,
    /// [`AppOp::Spill`].
    Spill,
}

/// Every action.
pub const ACTIONS: [Action; 9] = [
    Action::Commit,
    Action::ReaderBegin,
    Action::ReaderEnd,
    Action::CheckpointPassive,
    Action::CheckpointRestart,
    Action::CheckpointTruncate,
    Action::CheckpointFull,
    Action::DeleteWal,
    Action::Spill,
];

impl Action {
    /// The action of `op`; exhaustive, so a new app operation breaks here.
    #[must_use]
    pub const fn of(op: AppOp) -> Self {
        match op {
            AppOp::Commit { .. } => Self::Commit,
            AppOp::ReaderBegin => Self::ReaderBegin,
            AppOp::ReaderEnd => Self::ReaderEnd,
            AppOp::Checkpoint(OtherProcessMode::Passive) => Self::CheckpointPassive,
            AppOp::Checkpoint(OtherProcessMode::Restart) => Self::CheckpointRestart,
            AppOp::Checkpoint(OtherProcessMode::Truncate) => Self::CheckpointTruncate,
            AppOp::Checkpoint(OtherProcessMode::Full) => Self::CheckpointFull,
            AppOp::DeleteWal => Self::DeleteWal,
            AppOp::Spill { .. } => Self::Spill,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Commit => "Commit",
            Self::ReaderBegin => "ReaderBegin",
            Self::ReaderEnd => "ReaderEnd",
            Self::CheckpointPassive => "Checkpoint.Passive",
            Self::CheckpointRestart => "Checkpoint.Restart",
            Self::CheckpointTruncate => "Checkpoint.Truncate",
            Self::CheckpointFull => "Checkpoint.Full",
            Self::DeleteWal => "DeleteWal",
            Self::Spill => "Spill",
        }
    }
}

/// The key of a point, without its count.
#[must_use]
pub fn point_key(call: SourceOp, action: Action) -> String {
    format!("point.{}.{}", source_op_name(call), action.name())
}

/// The point an app step names, if it names one by kind of call.
#[must_use]
pub fn point_of(at: AppAt, op: AppOp) -> Option<String> {
    match at {
        AppAt::Before(call) => Some(point_key(call, Action::of(op))),
        AppAt::Call(_) => None,
    }
}

/// The cell of `call` and `action`. Every pair of a call the engine makes is drawn: the app
/// can act right before any of them, and an action SQLite refuses there (a commit while the
/// engine holds the barrier, a checkpoint while the app is in a transaction) is the point
/// exercised, not a reason to leave it out. The write lock's calls are never made. A new call or
/// action breaks the build here until someone decides its cells.
#[must_use]
pub const fn point_cell(call: SourceOp, action: Action) -> Cell {
    if let SourceOp::BeginWriteLock | SourceOp::EndWriteLock = call {
        return Cell::Impossible(super::NOT_CALLED);
    }
    match (call, action) {
        (
            SourceOp::Open
            | SourceOp::Len
            | SourceOp::Committed
            | SourceOp::PageCount
            | SourceOp::ReadPage
            | SourceOp::BeginBarrier
            | SourceOp::EndBarrier
            | SourceOp::Checkpoint
            | SourceOp::BumpSeq
            | SourceOp::BeginWriteLock
            | SourceOp::EndWriteLock
            | SourceOp::Sync
            | SourceOp::RenewRead,
            Action::Commit
            | Action::ReaderBegin
            | Action::ReaderEnd
            | Action::CheckpointPassive
            | Action::CheckpointRestart
            | Action::CheckpointTruncate
            | Action::DeleteWal
            | Action::Spill,
        ) => Cell::Drawn,
        (
            SourceOp::Open
            | SourceOp::Len
            | SourceOp::Committed
            | SourceOp::PageCount
            | SourceOp::ReadPage
            | SourceOp::BeginBarrier
            | SourceOp::EndBarrier
            | SourceOp::Checkpoint
            | SourceOp::BumpSeq
            | SourceOp::BeginWriteLock
            | SourceOp::EndWriteLock
            | SourceOp::Sync
            | SourceOp::RenewRead,
            Action::CheckpointFull,
        ) => Cell::Unmodelled(
            "the generator draws no FULL checkpoint by another process; only scenarios run one",
        ),
    }
}

/// The `.ran` keys the gate requires above zero: every drawn point.
#[must_use]
pub fn required_keys() -> Vec<String> {
    points()
        .into_iter()
        .filter(|(_, cell)| matches!(cell, Cell::Drawn))
        .map(|(key, _)| format!("{key}.ran"))
        .collect()
}

/// Every point, by key.
#[must_use]
pub fn points() -> Vec<(String, Cell)> {
    SOURCE_OPS
        .iter()
        .flat_map(|call| {
            ACTIONS
                .iter()
                .map(|action| (point_key(*call, *action), point_cell(*call, *action)))
        })
        .collect()
}
