//! The second version of the history generator, frozen: the live generator
//! as it was before staging replaced under a running engine was drawn, with
//! the default and swarm mixes of that time.
//!
//! A regression found with the live generator of that time (for example the
//! full history of seed 0x1919f) replays through this copy, which never
//! changes. `tests/sim/fixed_seeds.rs` pins a digest of each regression's
//! history, so an edit here fails there.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Entropy, SeededEntropy, SourceError, SourceOp, StoreError, StoreOp};
use lsr_ltx::PageSize;

use super::{EnvFault, MetaOp, Op, RemoteOp, Target, WalOp};
use crate::seed::Seed;
use crate::sqlite::{CrashKind, OtherProcessMode};
use crate::store::FaultShape;

/// How often each kind of operation is drawn, in relative weights.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Mix {
    /// [`Op::Tick`].
    tick: u32,
    /// [`Op::Commit`].
    commit: u32,
    /// [`Op::CommitInTick`].
    commit_in_tick: u32,
    /// [`Op::Spill`].
    spill: u32,
    /// [`Op::Rollback`].
    rollback: u32,
    /// [`Op::Vacuum`].
    vacuum: u32,
    /// [`Op::OtherProcessReader`].
    other_process_reader: u32,
    /// [`Op::OtherProcessCheckpoint`].
    other_process_checkpoint: u32,
    /// [`Op::Crash`].
    crash: u32,
    /// [`Op::StoreFault`].
    store_fault: u32,
    /// [`Op::EnvFault`].
    env_fault: u32,
    /// [`Op::ClockJump`].
    clock_jump: u32,
    /// [`Op::Shutdown`].
    shutdown: u32,
    /// [`Op::RequestSnapshot`].
    request_snapshot: u32,
    /// [`Op::Restore`].
    restore: u32,
    /// [`Op::Shrink`].
    shrink: u32,
    /// [`Op::SecondWriter`].
    second_writer: u32,
    /// [`Op::Remote`] with [`RemoteOp::RollBack`].
    roll_back: u32,
    /// [`Op::Remote`] with [`RemoteOp::DropCreatedAt`].
    drop_created_at: u32,
    /// [`Op::WalDamage`].
    wal_damage: u32,
    /// [`Op::StagingDamage`], and [`Op::Meta`] with the steps that need no
    /// restart.
    staging_damage: u32,
    /// [`Op::Heal`].
    heal: u32,
    /// Whether a vacuum may change the page size. A replica prefix holds one
    /// page size, so after a resize the engine's next boot stops, or keeps
    /// retrying in a degraded state, on `PageSizeChanged`, and the states
    /// acknowledged before it still restore; the runner expects either.
    vacuum_resizes: bool,
}

impl Default for Mix {
    /// Ticks and commits dominate and faults are rare, so a history makes
    /// progress between them.
    fn default() -> Self {
        Self {
            tick: 40,
            commit: 25,
            commit_in_tick: 3,
            spill: 3,
            rollback: 2,
            vacuum: 1,
            other_process_reader: 4,
            other_process_checkpoint: 3,
            crash: 4,
            store_fault: 3,
            env_fault: 1,
            clock_jump: 2,
            shutdown: 1,
            request_snapshot: 1,
            restore: 2,
            shrink: 2,
            second_writer: 1,
            roll_back: 1,
            // Off in the shared mixes so their recorded histories keep their
            // shape; `tests/sim/dropped_created_at.rs` turns it on.
            drop_created_at: 0,
            wal_damage: 1,
            staging_damage: 1,
            heal: 1,
            vacuum_resizes: true,
        }
    }
}

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
    Shrink,
    SecondWriter,
    RollBack,
    DropCreatedAt,
    WalDamage,
    StagingDamage,
    Heal,
}

impl Mix {
    fn table(&self) -> [(Kind, u32); 22] {
        [
            (Kind::Tick, self.tick),
            (Kind::Commit, self.commit),
            (Kind::CommitInTick, self.commit_in_tick),
            (Kind::Spill, self.spill),
            (Kind::Rollback, self.rollback),
            (Kind::Vacuum, self.vacuum),
            (Kind::OtherProcessReader, self.other_process_reader),
            (Kind::OtherProcessCheckpoint, self.other_process_checkpoint),
            (Kind::Crash, self.crash),
            (Kind::StoreFault, self.store_fault),
            (Kind::EnvFault, self.env_fault),
            (Kind::ClockJump, self.clock_jump),
            (Kind::Shutdown, self.shutdown),
            (Kind::RequestSnapshot, self.request_snapshot),
            (Kind::Restore, self.restore),
            (Kind::Shrink, self.shrink),
            (Kind::SecondWriter, self.second_writer),
            (Kind::RollBack, self.roll_back),
            (Kind::DropCreatedAt, self.drop_created_at),
            (Kind::WalDamage, self.wal_damage),
            (Kind::StagingDamage, self.staging_damage),
            (Kind::Heal, self.heal),
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

impl Mix {
    /// A mix of its own for `seed` (swarm testing): about a third of the
    /// kinds besides ticks and commits are left out, the rest weigh 1 to
    /// 100, and a vacuum may or may not resize. One fixed mix suppresses
    /// states: interleaved crashes keep compaction shallow, a page-size
    /// change leaves the second half of most histories degraded, faults
    /// rarely overlap. Drawn from the seed's own stream, so the history's
    /// stream is untouched.
    #[must_use]
    fn swarm(seed: Seed) -> Self {
        let entropy = SeededEntropy::new(seed.stream(12));
        let weight = |keep_always: bool| {
            let left_out = !keep_always && below(&entropy, 3) == 0;
            let drawn = u32::try_from(below(&entropy, 100).saturating_add(1)).unwrap_or(1);
            if left_out { 0 } else { drawn }
        };
        let mut mix = Self {
            tick: weight(true),
            commit: weight(true),
            commit_in_tick: weight(false),
            spill: weight(false),
            rollback: weight(false),
            vacuum: weight(false),
            other_process_reader: weight(false),
            other_process_checkpoint: weight(false),
            crash: weight(false),
            store_fault: weight(false),
            env_fault: weight(false),
            clock_jump: weight(false),
            shutdown: weight(false),
            request_snapshot: weight(false),
            restore: weight(false),
            shrink: weight(false),
            second_writer: 0,
            roll_back: weight(false),
            drop_created_at: 0,
            wal_damage: weight(false),
            staging_damage: weight(false),
            heal: weight(false),
            vacuum_resizes: below(&entropy, 2) == 0,
        };
        // Drawn last, so every other kind keeps the weight it had before
        // second writers were drawn.
        mix.second_writer = weight(false);
        mix
    }

    /// What the rotating seed sets run: the default mix for half the seeds
    /// and a swarm mix for the other half, chosen by the seed.
    #[must_use]
    fn mixed(seed: Seed) -> Self {
        if seed.stream(13).is_multiple_of(2) {
            Self::default()
        } else {
            Self::swarm(seed)
        }
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
            StoreOp::Delete,
        ],
    )
    .unwrap_or(StoreOp::Upload);
    let error = store_error(entropy, op);
    Op::StoreFault {
        target,
        op,
        ahead: small(entropy, 4),
        shape: shape(entropy, op, error),
    }
}

/// A fault shape that means something for `op`: the commit shapes for an
/// upload, a flipped byte for a read, and a rate (between 1% and 10%, until
/// an [`Op::Heal`]) or a plain error for any.
fn shape(entropy: &SeededEntropy, op: StoreOp, error: StoreError) -> FaultShape {
    let rate = |entropy: &SeededEntropy, err| FaultShape::Rate {
        per_mille: u16::try_from(below(entropy, 91).saturating_add(10)).unwrap_or(10),
        err,
    };
    match (op, below(entropy, 8)) {
        (StoreOp::Upload, 0) => FaultShape::ErrorAfterPublish(error),
        (StoreOp::Upload, 1) => FaultShape::SizeMismatch,
        (StoreOp::Upload, 2) => FaultShape::ChecksumMismatch,
        (StoreOp::Upload, 3) => FaultShape::CorruptPublish,
        (StoreOp::Read, 0 | 1) => FaultShape::FlipByteOnRead {
            at: below(entropy, 4096),
        },
        (_, 7) => rate(entropy, error),
        (StoreOp::List | StoreOp::Read | StoreOp::Upload | StoreOp::Delete, _) => {
            FaultShape::Error(error)
        }
    }
}

/// Damage to the WAL. [`WalOp::RewriteSameLength`] is left out: it leaves the
/// wal-index naming a generation the file no longer holds, which SQLite's
/// next open would recover and the rules never do, so the engine would wait
/// for ever on a state no real database stays in.
fn wal_damage(entropy: &SeededEntropy) -> Op {
    let seed = below(entropy, u64::MAX);
    let op = match below(entropy, 5) {
        0 => WalOp::TearHeader,
        1 => WalOp::TruncateAtFrame(u32::try_from(below(entropy, 40)).unwrap_or(0)),
        2 => WalOp::RegrowSameSalts(seed),
        3 => WalOp::RegrowWithoutCommitMarks(seed),
        _ => WalOp::FlipLastFrameBit,
    };
    Op::WalDamage {
        op,
        ticks: small(entropy, 8),
    }
}

fn staging_damage(entropy: &SeededEntropy) -> Op {
    match below(entropy, 8) {
        0 => Op::Meta(MetaOp::Save),
        1 => Op::Meta(MetaOp::DeleteAll),
        2 => Op::StagingDamage(MetaOp::Restore),
        3 => Op::StagingDamage(MetaOp::DeleteNewest),
        4 => Op::StagingDamage(MetaOp::FlipNewestByte),
        5 => Op::StagingDamage(MetaOp::TruncateNewest),
        6 => Op::StagingDamage(MetaOp::StripNewestOrigin),
        _ => Op::Meta(MetaOp::FlipNewestByte),
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

fn draw(weights: &Mix, entropy: &SeededEntropy) -> Op {
    match weights.pick(below(entropy, weights.total())) {
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
        Kind::Vacuum => vacuum(entropy, weights.vacuum_resizes),
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
        Kind::Shrink => Op::Shrink {
            pages: small(entropy, 3).saturating_add(1),
        },
        Kind::SecondWriter => Op::SecondWriter {
            ticks: small(entropy, 8),
        },
        Kind::RollBack => Op::Remote(RemoteOp::RollBack),
        Kind::DropCreatedAt => Op::Remote(RemoteOp::DropCreatedAt),
        Kind::WalDamage => wal_damage(entropy),
        Kind::StagingDamage => staging_damage(entropy),
        Kind::Heal => Op::Heal,
    }
}

/// The history of `steps` operations that `seed` named under the seed's
/// own mix of this version ([`Mix::mixed`]).
#[must_use]
pub fn history(seed: Seed, steps: u32) -> Vec<Op> {
    let weights = Mix::mixed(seed);
    let entropy = SeededEntropy::new(seed.stream(10));
    (0..steps).map(|_| draw(&weights, &entropy)).collect()
}
