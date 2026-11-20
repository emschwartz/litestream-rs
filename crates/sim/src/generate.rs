//! The history generator: a seed and a length become a list of operations.
//!
//! A history is a pure function of its seed: it draws from one sequential
//! stream and looks at no state, so the first `k` operations of a history of
//! `n` are the history of `k` (what [`crate::shrink`] relies on). Whatever
//! needs the world's state (which pages a commit touches, which stranger ends)
//! is decided when the runner applies the operation, from the step number.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Entropy, SeededEntropy, SourceError, SourceOp, StoreError, StoreOp};
use lsr_ltx::PageSize;

use crate::seed::Seed;
use crate::sqlite::{CrashKind, StrangerMode};
use crate::store::FaultShape;

/// Which store a fault is scheduled on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The staging directory.
    Staging,
    /// The replica.
    Remote,
}

/// A fault in the app's or the machine's world rather than a store's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvFault {
    /// The last connection closes and the WAL file is deleted (when no
    /// connection holds a transaction).
    DeleteWal,
    /// The staging directory is wiped and the process dies with it.
    WipeStaging,
    /// The next call of an operation on our SQLite handles fails.
    Source(SourceOp, SourceError),
}

/// One step of a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// The app commits `pages` random pages (more when it grows).
    Commit {
        /// How many pages it writes besides those it adds.
        pages: u8,
        /// The database grows by a few pages.
        grow: bool,
    },
    /// The app writes frames of a transaction and does not commit.
    Spill,
    /// The app rolls its open transaction back.
    Rollback,
    /// The app vacuums: into pages of `page_size`, or (`None`) at the same size,
    /// which is one large transaction that rewrites every page.
    Vacuum {
        /// The new page size.
        page_size: Option<PageSize>,
    },
    /// A stranger begins a read transaction, or the oldest one ends.
    StrangerReader {
        /// Begin (otherwise end).
        begin: bool,
    },
    /// A checkpoint from outside the engine.
    StrangerCheckpoint(StrangerMode),
    /// The next tick is cut short, or the machine fails right after it.
    Crash(CrashKind),
    /// A store call fails `ahead` calls from now.
    StoreFault {
        /// Which store.
        target: Target,
        /// Which operation.
        op: StoreOp,
        /// How many calls of it pass first.
        ahead: u8,
        /// What the failure looks like.
        shape: FaultShape,
    },
    /// A fault outside the stores.
    EnvFault(EnvFault),
    /// The clock moves by this many milliseconds, either way.
    ClockJump(i64),
    /// The actor runs one tick.
    Tick,
    /// The operator stops the engine: it drains, then a fresh one boots later.
    Shutdown,
    /// The operator asks for a snapshot.
    RequestSnapshot,
    /// The oracle restores every transaction and `Latest` and judges them.
    Restore,
}

/// How often each kind of operation is drawn, in relative weights.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Weights {
    /// [`Op::Tick`].
    pub tick: u32,
    /// [`Op::Commit`].
    pub commit: u32,
    /// [`Op::Spill`].
    pub spill: u32,
    /// [`Op::Rollback`].
    pub rollback: u32,
    /// [`Op::Vacuum`].
    pub vacuum: u32,
    /// [`Op::StrangerReader`].
    pub stranger_reader: u32,
    /// [`Op::StrangerCheckpoint`].
    pub stranger_checkpoint: u32,
    /// [`Op::Crash`].
    pub crash: u32,
    /// [`Op::StoreFault`].
    pub store_fault: u32,
    /// [`Op::EnvFault`].
    pub env_fault: u32,
    /// [`Op::ClockJump`].
    pub clock_jump: u32,
    /// [`Op::Shutdown`].
    pub shutdown: u32,
    /// [`Op::RequestSnapshot`].
    pub request_snapshot: u32,
    /// [`Op::Restore`].
    pub restore: u32,
    /// Whether a vacuum may change the page size. Maintenance once halted on a
    /// chain that spans a page-size change; the recover fixes on main cured it.
    pub vacuum_resizes: bool,
}

impl Default for Weights {
    /// Ticks and commits dominate and faults are rare, so a history makes
    /// progress between them.
    fn default() -> Self {
        Self {
            tick: 40,
            commit: 25,
            spill: 3,
            rollback: 2,
            vacuum: 1,
            stranger_reader: 4,
            stranger_checkpoint: 3,
            crash: 4,
            store_fault: 3,
            env_fault: 1,
            clock_jump: 2,
            shutdown: 1,
            request_snapshot: 1,
            restore: 2,
            vacuum_resizes: true,
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Tick,
    Commit,
    Spill,
    Rollback,
    Vacuum,
    StrangerReader,
    StrangerCheckpoint,
    Crash,
    StoreFault,
    EnvFault,
    ClockJump,
    Shutdown,
    RequestSnapshot,
    Restore,
}

impl Weights {
    fn table(&self) -> [(Kind, u32); 14] {
        [
            (Kind::Tick, self.tick),
            (Kind::Commit, self.commit),
            (Kind::Spill, self.spill),
            (Kind::Rollback, self.rollback),
            (Kind::Vacuum, self.vacuum),
            (Kind::StrangerReader, self.stranger_reader),
            (Kind::StrangerCheckpoint, self.stranger_checkpoint),
            (Kind::Crash, self.crash),
            (Kind::StoreFault, self.store_fault),
            (Kind::EnvFault, self.env_fault),
            (Kind::ClockJump, self.clock_jump),
            (Kind::Shutdown, self.shutdown),
            (Kind::RequestSnapshot, self.request_snapshot),
            (Kind::Restore, self.restore),
        ]
    }

    fn total(&self) -> u64 {
        self.table()
            .iter()
            .map(|(_, w)| u64::from(*w))
            .sum::<u64>()
            .max(1)
    }

    fn pick(&self, draw: u64) -> Kind {
        let mut left = draw;
        for (kind, weight) in self.table() {
            let weight = u64::from(weight);
            if left < weight {
                return kind;
            }
            left = left.saturating_sub(weight);
        }
        Kind::Tick
    }
}

fn below(entropy: &SeededEntropy, upper: u64) -> u64 {
    entropy.below(upper.max(1))
}

fn small(entropy: &SeededEntropy, upper: u64) -> u8 {
    u8::try_from(below(entropy, upper)).unwrap_or(u8::MAX)
}

fn choose<T: Copy>(entropy: &SeededEntropy, from: &[T]) -> Option<T> {
    let at = usize::try_from(below(entropy, u64::try_from(from.len()).unwrap_or(1))).ok()?;
    from.get(at).copied()
}

fn store_error(entropy: &SeededEntropy, op: StoreOp) -> StoreError {
    if below(entropy, 3) == 0 {
        StoreError::Throttled {
            op,
            retry_after: (below(entropy, 2) == 0).then_some(Duration::from_secs(2)),
        }
    } else {
        StoreError::Transient {
            op,
            kind: ErrorKind::ConnectionReset,
        }
    }
}

fn store_fault(entropy: &SeededEntropy) -> Op {
    let target = if below(entropy, 2) == 0 {
        Target::Staging
    } else {
        Target::Remote
    };
    let op = choose(
        entropy,
        &[
            StoreOp::List,
            StoreOp::Read,
            StoreOp::Upload,
            StoreOp::Upload,
        ],
    )
    .unwrap_or(StoreOp::Upload);
    let error = store_error(entropy, op);
    let shape = match (op, below(entropy, 4)) {
        (StoreOp::Upload, 0) => FaultShape::ErrorAfterPublish(error),
        (StoreOp::Upload, 1) => FaultShape::SizeMismatch,
        (StoreOp::List | StoreOp::Read | StoreOp::Upload | StoreOp::Delete, _) => {
            FaultShape::Error(error)
        }
    };
    Op::StoreFault {
        target,
        op,
        ahead: small(entropy, 4),
        shape,
    }
}

fn env_fault(entropy: &SeededEntropy) -> Op {
    let fault = match below(entropy, 4) {
        0 => EnvFault::DeleteWal,
        1 => EnvFault::WipeStaging,
        _ => {
            let op = choose(
                entropy,
                &[
                    SourceOp::Open,
                    SourceOp::Len,
                    SourceOp::Committed,
                    SourceOp::PageCount,
                    SourceOp::ReadPage,
                    SourceOp::BeginBarrier,
                    SourceOp::Checkpoint,
                    SourceOp::BumpSeq,
                    SourceOp::BeginWriteLock,
                ],
            )
            .unwrap_or(SourceOp::Len);
            let err = if below(entropy, 2) == 0 {
                SourceError::Busy
            } else {
                SourceError::Io(ErrorKind::Other)
            };
            EnvFault::Source(op, err)
        }
    };
    Op::EnvFault(fault)
}

fn clock_jump(entropy: &SeededEntropy) -> Op {
    let millis = match below(entropy, 10) {
        0 => 0_i64
            .saturating_sub(i64::try_from(below(entropy, 60_000).saturating_add(1)).unwrap_or(1)),
        1 => 93_600_000,
        _ => i64::try_from(below(entropy, 120_000).saturating_add(1)).unwrap_or(1),
    };
    Op::ClockJump(millis)
}

fn vacuum(entropy: &SeededEntropy, resizes: bool) -> Op {
    // The draw happens either way so a history keeps its shape across the flag.
    let page_size = match below(entropy, 3) {
        0 if resizes => PageSize::new(1024),
        1 if resizes => PageSize::new(2048),
        _ => None,
    };
    Op::Vacuum { page_size }
}

fn draw(weights: &Weights, entropy: &SeededEntropy) -> Op {
    match weights.pick(below(entropy, weights.total())) {
        Kind::Tick => Op::Tick,
        Kind::Commit => Op::Commit {
            pages: small(entropy, 4).saturating_add(1),
            grow: below(entropy, 6) == 0,
        },
        Kind::Spill => Op::Spill,
        Kind::Rollback => Op::Rollback,
        Kind::Vacuum => vacuum(entropy, weights.vacuum_resizes),
        Kind::StrangerReader => Op::StrangerReader {
            begin: below(entropy, 2) == 0,
        },
        Kind::StrangerCheckpoint => Op::StrangerCheckpoint(
            choose(
                entropy,
                &[
                    StrangerMode::Passive,
                    StrangerMode::Restart,
                    StrangerMode::Truncate,
                ],
            )
            .unwrap_or(StrangerMode::Passive),
        ),
        Kind::Crash => Op::Crash(if below(entropy, 4) == 0 {
            CrashKind::Power
        } else {
            CrashKind::Process
        }),
        Kind::StoreFault => store_fault(entropy),
        Kind::EnvFault => env_fault(entropy),
        Kind::ClockJump => clock_jump(entropy),
        Kind::Shutdown => Op::Shutdown,
        Kind::RequestSnapshot => Op::RequestSnapshot,
        Kind::Restore => Op::Restore,
    }
}

/// The history of `steps` operations that `seed` names.
#[must_use]
pub fn history(seed: Seed, steps: u32, weights: &Weights) -> Vec<Op> {
    let entropy = SeededEntropy::new(seed.stream(10));
    (0..steps).map(|_| draw(weights, &entropy)).collect()
}

#[cfg(test)]
mod tests;
