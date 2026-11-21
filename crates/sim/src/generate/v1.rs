//! The first version of the history generator, frozen.
//!
//! A regression seed is a seed, a length and the generator that drew its
//! history. A new draw in the live generator ([`super::history`]) reshapes
//! every history after it, so a seed replayed through it silently stops
//! reproducing its bug. This copy never changes: the live generator moves on,
//! and the regressions recorded against this one keep their operations.
//! `tests/sim/fixed_seeds.rs` pins a digest of each regression's history, so
//! an edit here fails there.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Entropy, SeededEntropy, SourceError, SourceOp, StoreError, StoreOp};
use lsr_ltx::PageSize;

use super::{EnvFault, Op, Target};
use crate::seed::Seed;
use crate::sqlite::{CrashKind, OtherProcessMode};
use crate::store::FaultShape;

#[derive(Clone, Copy)]
enum Kind {
    Tick,
    Commit,
    CommitInTick,
    Spill,
    Rollback,
    Vacuum,
    OtherProcessReader,
    OtherProcessCheckpoint,
    Crash,
    StoreFault,
    EnvFault,
    ClockJump,
    Shutdown,
    RequestSnapshot,
    Restore,
}

/// The weights `Weights::default()` had when this version was frozen.
const WEIGHTS: [(Kind, u64); 15] = [
    (Kind::Tick, 40),
    (Kind::Commit, 25),
    (Kind::CommitInTick, 3),
    (Kind::Spill, 3),
    (Kind::Rollback, 2),
    (Kind::Vacuum, 1),
    (Kind::OtherProcessReader, 4),
    (Kind::OtherProcessCheckpoint, 3),
    (Kind::Crash, 4),
    (Kind::StoreFault, 3),
    (Kind::EnvFault, 1),
    (Kind::ClockJump, 2),
    (Kind::Shutdown, 1),
    (Kind::RequestSnapshot, 1),
    (Kind::Restore, 2),
];

fn total() -> u64 {
    WEIGHTS.iter().map(|(_, w)| *w).sum::<u64>().max(1)
}

fn pick(draw: u64) -> Kind {
    let mut left = draw;
    for (kind, weight) in WEIGHTS {
        if left < weight {
            return kind;
        }
        left = left.saturating_sub(weight);
    }
    Kind::Tick
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

fn vacuum(entropy: &SeededEntropy) -> Op {
    let page_size = match below(entropy, 3) {
        0 => PageSize::new(1024),
        1 => PageSize::new(2048),
        _ => None,
    };
    Op::Vacuum { page_size }
}

fn draw(entropy: &SeededEntropy) -> Op {
    match pick(below(entropy, total())) {
        Kind::Tick => Op::Tick,
        Kind::Commit => Op::Commit {
            pages: small(entropy, 4).saturating_add(1),
            grow: below(entropy, 6) == 0,
        },
        Kind::CommitInTick => Op::CommitInTick {
            pages: small(entropy, 4).saturating_add(1),
        },
        Kind::Spill => Op::Spill,
        Kind::Rollback => Op::Rollback,
        Kind::Vacuum => vacuum(entropy),
        Kind::OtherProcessReader => Op::OtherProcessReader {
            begin: below(entropy, 2) == 0,
        },
        Kind::OtherProcessCheckpoint => Op::OtherProcessCheckpoint(
            choose(
                entropy,
                &[
                    OtherProcessMode::Passive,
                    OtherProcessMode::Restart,
                    OtherProcessMode::Truncate,
                ],
            )
            .unwrap_or(OtherProcessMode::Passive),
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

/// The history of `steps` operations that `seed` names under this version.
pub(super) fn history(seed: Seed, steps: u32) -> Vec<Op> {
    let entropy = SeededEntropy::new(seed.stream(10));
    (0..steps).map(|_| draw(&entropy)).collect()
}
