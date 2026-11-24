//! The error domain: every error the engine's SQLite seam and its two stores can return, by
//! operation, each cell either drawn by a history or impossible for a stated reason.
//!
//! The SQLite seam's cells are judged against `lsr-sqlite`, the one implementation the engine
//! ships with; the staging store's against `store-fs`, the replica's against `store-s3`. A cell
//! is one of three things:
//!
//! - [`Cell::Drawn`]: the implementation returns this error from this call while the world
//!   stays as it was, so injecting it is honest.
//! - [`Cell::World`]: the error is a fact about the world (the WAL file was replaced), so the
//!   draw changes the world and the call then reports it; injecting the error alone would lie.
//! - [`Cell::Impossible`]: the implementation never returns it from this call, or returns it
//!   only for a world fault drawn elsewhere; the reason says which.
//! - [`Cell::Unmodelled`]: real, but the simulator's model of SQLite cannot follow it yet, so it
//!   is not drawn; the reason names the seeds that showed the model losing track. These are
//!   gaps in the checker, listed so they are not mistaken for coverage.
//!
//! The tables are matches with no wildcard arm, so a new operation or error variant breaks the
//! build here until someone decides its cells. Histories draw from the table only when the run's
//! mix sets [`crate::Weights::error_domain`], drawn last in the swarm mix, so every recorded
//! history keeps its draws. [`expected_stop`] and [`source_stop`] say which stop each drawn
//! error legitimately causes, for the runner's expected-stop bookkeeping; they are written here,
//! not taken from the engine's fault policy, so the check does not share the engine's beliefs.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{
    CorruptCause, Denied, Entropy, SeededEntropy, SourceError, SourceOp, StopCause, StoreError,
    StoreOp,
};
use lsr_ltx::{PageNo, PageSize};

use crate::generate::{EnvFault, Target};

/// What a history may do with one (operation, error) pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cell {
    /// The call returns this error with the world unchanged.
    Drawn,
    /// The error reports a change to the world: the draw makes the change instead.
    World(EnvFault),
    /// Never returned by this call, or only for a world fault drawn elsewhere.
    Impossible(&'static str),
    /// Real, but the simulator's model cannot follow it yet, so no history draws it.
    Unmodelled(&'static str),
}

/// The classes of [`SourceError`], without their payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceClass {
    /// [`SourceError::Io`].
    Io,
    /// [`SourceError::DiskFull`].
    DiskFull,
    /// [`SourceError::FsyncFailed`].
    FsyncFailed,
    /// [`SourceError::Busy`].
    Busy,
    /// [`SourceError::ReadersHeld`].
    ReadersHeld,
    /// [`SourceError::Short`].
    Short,
    /// [`SourceError::Unlinked`].
    Unlinked,
    /// [`SourceError::ReadTxLost`].
    ReadTxLost,
}

/// Every source operation, in declaration order ([`source_ordinal`] checks it).
pub const SOURCE_OPS: [SourceOp; 12] = [
    SourceOp::Open,
    SourceOp::Len,
    SourceOp::Committed,
    SourceOp::PageCount,
    SourceOp::ReadPage,
    SourceOp::BeginBarrier,
    SourceOp::EndBarrier,
    SourceOp::Checkpoint,
    SourceOp::BumpSeq,
    SourceOp::BeginWriteLock,
    SourceOp::EndWriteLock,
    SourceOp::Sync,
];

/// Every source error class, in declaration order ([`source_class`] checks it).
pub const SOURCE_CLASSES: [SourceClass; 8] = [
    SourceClass::Io,
    SourceClass::DiskFull,
    SourceClass::FsyncFailed,
    SourceClass::Busy,
    SourceClass::ReadersHeld,
    SourceClass::Short,
    SourceClass::Unlinked,
    SourceClass::ReadTxLost,
];

/// A source operation's place in [`SOURCE_OPS`]; exhaustive, so a new operation breaks here.
#[must_use]
pub const fn source_ordinal(op: SourceOp) -> usize {
    match op {
        SourceOp::Open => 0,
        SourceOp::Len => 1,
        SourceOp::Committed => 2,
        SourceOp::PageCount => 3,
        SourceOp::ReadPage => 4,
        SourceOp::BeginBarrier => 5,
        SourceOp::EndBarrier => 6,
        SourceOp::Checkpoint => 7,
        SourceOp::BumpSeq => 8,
        SourceOp::BeginWriteLock => 9,
        SourceOp::EndWriteLock => 10,
        SourceOp::Sync => 11,
    }
}

/// The class of a source error; exhaustive, so a new error variant breaks here.
#[must_use]
pub const fn source_class(err: SourceError) -> SourceClass {
    match err {
        SourceError::Io(_) => SourceClass::Io,
        SourceError::DiskFull => SourceClass::DiskFull,
        SourceError::FsyncFailed => SourceClass::FsyncFailed,
        SourceError::Busy => SourceClass::Busy,
        SourceError::ReadersHeld => SourceClass::ReadersHeld,
        SourceError::Short { .. } => SourceClass::Short,
        SourceError::Unlinked => SourceClass::Unlinked,
        SourceError::ReadTxLost => SourceClass::ReadTxLost,
    }
}

const NO_LOCK: &str = "the call takes no SQLite lock, so nothing reports busy";
const WRITES_NOTHING: &str = "the call writes nothing, so the disk cannot be full for it";
const NOT_FSYNC: &str = "only WalSource::sync maps a failed fsync to FsyncFailed; SQLite reports its own as an I/O code";
const NO_READERS_HELD: &str =
    "no source returns it: lsr-sqlite reports readers holding the WAL as a busy checkpoint";
const NOT_A_PAGE: &str = "only a database page read can end inside a page";
const NOT_THE_WAL_PATH: &str = "only the calls that look at the WAL path can see it replaced";
const UNLINK_UNMODELLED: &str = "an unlinked WAL is drawn as EnvFault::UnlinkWal, which the \
                                 model cannot yet follow through a later power loss";
const RETAKE_ONLY: &str = "only a retake after a failed one reports it, and the checkpoint cell \
                           draws the first failure; this call takes no read transaction of its own \
                           otherwise";

/// The engine no longer calls these (docs/decisions/D190.md); the trait keeps them.
pub(crate) const NOT_CALLED: &str =
    "the engine takes no write lock: its boundary snapshot copies the database without one";

/// The cell of `op` and `class` against `lsr-sqlite`.
#[must_use]
pub const fn source_cell(op: SourceOp, class: SourceClass) -> Cell {
    if let SourceOp::BeginWriteLock | SourceOp::EndWriteLock = op {
        return Cell::Impossible(NOT_CALLED);
    }
    match class {
        SourceClass::Io => io_cell(op),
        SourceClass::DiskFull => disk_full_cell(op),
        SourceClass::FsyncFailed => fsync_cell(op),
        SourceClass::Busy => busy_cell(op),
        SourceClass::ReadersHeld => Cell::Impossible(NO_READERS_HELD),
        SourceClass::Short => short_cell(op),
        SourceClass::Unlinked => unlinked_cell(op),
        SourceClass::ReadTxLost => read_tx_lost_cell(op),
    }
}

const fn io_cell(op: SourceOp) -> Cell {
    match op {
        SourceOp::Sync => {
            Cell::Impossible("sync's only I/O error is a missing WAL, drawn as EnvFault::DeleteWal")
        }
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
        | SourceOp::EndWriteLock => Cell::Drawn,
    }
}

const fn disk_full_cell(op: SourceOp) -> Cell {
    match op {
        SourceOp::BeginBarrier
        | SourceOp::Checkpoint
        | SourceOp::BumpSeq
        | SourceOp::BeginWriteLock => Cell::Drawn,
        SourceOp::EndBarrier | SourceOp::EndWriteLock => {
            Cell::Impossible("a rollback in WAL mode discards pages and writes nothing")
        }
        SourceOp::Sync => Cell::Impossible(NOT_FSYNC),
        SourceOp::Open
        | SourceOp::Len
        | SourceOp::Committed
        | SourceOp::PageCount
        | SourceOp::ReadPage => Cell::Impossible(WRITES_NOTHING),
    }
}

const fn fsync_cell(op: SourceOp) -> Cell {
    match op {
        SourceOp::Sync => Cell::Unmodelled(
            "a failed sync of the WAL is met and goes on (D173); a checkpoint that copies the frames \
             it covered, then a power loss, leaves a database no commit made, which v1 does not \
             detect or repair, so a random history cannot judge it. \
             `wal_fsync_fails` and `wal_fsync_fails_then_power_loss` are the scenarios",
        ),
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
        | SourceOp::EndWriteLock => Cell::Impossible(NOT_FSYNC),
    }
}

const fn short_cell(op: SourceOp) -> Cell {
    match op {
        SourceOp::ReadPage => Cell::Drawn,
        SourceOp::Open
        | SourceOp::Len
        | SourceOp::Committed
        | SourceOp::PageCount
        | SourceOp::BeginBarrier
        | SourceOp::EndBarrier
        | SourceOp::Checkpoint
        | SourceOp::BumpSeq
        | SourceOp::BeginWriteLock
        | SourceOp::EndWriteLock
        | SourceOp::Sync => Cell::Impossible(NOT_A_PAGE),
    }
}

const fn read_tx_lost_cell(op: SourceOp) -> Cell {
    match op {
        SourceOp::Checkpoint => Cell::Drawn,
        SourceOp::Committed | SourceOp::Sync | SourceOp::PageCount | SourceOp::ReadPage => {
            Cell::Impossible(RETAKE_ONLY)
        }
        SourceOp::Open
        | SourceOp::Len
        | SourceOp::BeginBarrier
        | SourceOp::EndBarrier
        | SourceOp::BumpSeq
        | SourceOp::BeginWriteLock
        | SourceOp::EndWriteLock => {
            Cell::Impossible("the call neither holds nor retakes the read transaction")
        }
    }
}

const fn busy_cell(op: SourceOp) -> Cell {
    match op {
        SourceOp::Committed
        | SourceOp::BeginBarrier
        | SourceOp::Checkpoint
        | SourceOp::BumpSeq
        | SourceOp::BeginWriteLock => Cell::Drawn,
        SourceOp::EndBarrier | SourceOp::EndWriteLock => {
            Cell::Impossible("a rollback releases locks and takes none")
        }
        SourceOp::Open
        | SourceOp::Len
        | SourceOp::PageCount
        | SourceOp::ReadPage
        | SourceOp::Sync => Cell::Impossible(NO_LOCK),
    }
}

const fn unlinked_cell(op: SourceOp) -> Cell {
    match op {
        // The draw would be the world fault EnvFault::UnlinkWal, after which these calls report
        // it. Drawn in histories, it showed the model losing track of the database after an
        // unlinked WAL and a later power loss (seeds 0x79, 0x94 and 0x7fb of the mixed set).
        SourceOp::Open | SourceOp::Len | SourceOp::Committed => Cell::Unmodelled(UNLINK_UNMODELLED),
        SourceOp::PageCount
        | SourceOp::ReadPage
        | SourceOp::BeginBarrier
        | SourceOp::EndBarrier
        | SourceOp::Checkpoint
        | SourceOp::BumpSeq
        | SourceOp::BeginWriteLock
        | SourceOp::EndWriteLock
        | SourceOp::Sync => Cell::Impossible(NOT_THE_WAL_PATH),
    }
}

/// A concrete error of `class`, its payload drawn.
fn source_error(entropy: &SeededEntropy, class: SourceClass) -> SourceError {
    match class {
        SourceClass::Io => SourceError::Io(pick(
            entropy,
            &[
                ErrorKind::Other,
                ErrorKind::PermissionDenied,
                ErrorKind::InvalidData,
            ],
            ErrorKind::Other,
        )),
        SourceClass::DiskFull => SourceError::DiskFull,
        SourceClass::FsyncFailed => SourceError::FsyncFailed,
        SourceClass::Busy => SourceError::Busy,
        SourceClass::ReadersHeld => SourceError::ReadersHeld,
        SourceClass::Short => {
            let pgno = u32::try_from(below(entropy, 8))
                .unwrap_or(0)
                .saturating_add(1);
            PageSize::new(4096)
                .and_then(|size| PageNo::new(pgno, size))
                .map_or(SourceError::Io(ErrorKind::UnexpectedEof), |pgno| {
                    SourceError::Short { pgno }
                })
        }
        SourceClass::Unlinked => SourceError::Unlinked,
        SourceClass::ReadTxLost => SourceError::ReadTxLost,
    }
}

/// What a drawn source fault does: an injected error, or a change to the world.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceDraw {
    /// The next call of the operation fails with the error.
    Inject(SourceOp, SourceError),
    /// The world changes, and the calls report it.
    World(EnvFault),
}

/// Whether a history draws `cell`: a drawn or world cell.
#[must_use]
pub const fn drawable(cell: Cell) -> bool {
    match cell {
        Cell::Drawn | Cell::World(_) => true,
        Cell::Impossible(_) | Cell::Unmodelled(_) => false,
    }
}

/// One cell of the source table, uniform over the cells a history draws ([`drawable`]).
#[must_use]
pub fn draw_source(entropy: &SeededEntropy) -> SourceDraw {
    let cells: Vec<(SourceOp, SourceClass, Cell)> = SOURCE_OPS
        .iter()
        .flat_map(|op| {
            SOURCE_CLASSES
                .iter()
                .map(move |class| (*op, *class, source_cell(*op, *class)))
        })
        .filter(|(_, _, cell)| drawable(*cell))
        .collect();
    match pick_ref(entropy, &cells) {
        Some((_, _, Cell::World(fault))) => SourceDraw::World(*fault),
        Some((op, class, _)) => SourceDraw::Inject(*op, source_error(entropy, *class)),
        None => SourceDraw::Inject(SourceOp::Len, SourceError::Busy),
    }
}

/// `err` for `op` if the table lets a history draw it, else an I/O error: what the older subset
/// (runs without [`crate::Weights::error_domain`]) draws in place of a busy answer from a call
/// that takes no lock. The subset's entropy is spent as before, so its other draws keep their
/// values.
#[must_use]
pub const fn possible_source(op: SourceOp, err: SourceError) -> SourceError {
    if drawable(source_cell(op, source_class(err))) {
        err
    } else {
        SourceError::Io(ErrorKind::Other)
    }
}

/// `err` for `op` on `target` if the table lets a history draw it, else a transient error of the
/// same operation: what the older subset draws in place of a filesystem asking to slow down.
#[must_use]
pub fn possible_store(target: Target, op: StoreOp, err: StoreError) -> StoreError {
    if drawable(store_cell(target, op, store_class(&err))) {
        err
    } else {
        StoreError::Transient {
            op,
            kind: ErrorKind::Other,
        }
    }
}

/// The stop a source error legitimately causes under the default responses, if any: written
/// from the engine's contract (BEHAVIOR's stop conditions), not taken from its policy.
#[must_use]
pub const fn source_stop(err: SourceError) -> Option<StopCause> {
    match err {
        SourceError::FsyncFailed => Some(StopCause::WalSyncFailed),
        SourceError::Unlinked => Some(StopCause::WalUnlinked),
        SourceError::Io(_)
        | SourceError::DiskFull
        | SourceError::Busy
        | SourceError::ReadersHeld
        | SourceError::Short { .. }
        | SourceError::ReadTxLost => None,
    }
}

/// The classes of [`StoreError`], without their payloads; a denial keeps its kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreClass {
    /// [`StoreError::Transient`].
    Transient,
    /// [`StoreError::Throttled`].
    Throttled,
    /// [`StoreError::Denied`], of one kind.
    Denied(Denied),
    /// [`StoreError::NotFound`].
    NotFound,
    /// [`StoreError::AlreadyExists`].
    AlreadyExists,
    /// [`StoreError::ScratchExhausted`].
    ScratchExhausted,
    /// [`StoreError::Corrupt`].
    Corrupt,
    /// [`StoreError::CreateOnlyUnsupported`].
    CreateOnlyUnsupported,
    /// [`StoreError::Refused`].
    Refused,
}

/// Every store operation, in declaration order ([`store_ordinal`] checks it).
pub const STORE_OPS: [StoreOp; 4] = [
    StoreOp::List,
    StoreOp::Read,
    StoreOp::Upload,
    StoreOp::Delete,
];

/// Every store error class, every denial kind among them ([`store_class`] checks it).
pub const STORE_CLASSES: [StoreClass; 13] = [
    StoreClass::Transient,
    StoreClass::Throttled,
    StoreClass::Denied(Denied::Credentials),
    StoreClass::Denied(Denied::Permission),
    StoreClass::Denied(Denied::ClockSkew),
    StoreClass::Denied(Denied::BucketMissing),
    StoreClass::Denied(Denied::Tls),
    StoreClass::NotFound,
    StoreClass::AlreadyExists,
    StoreClass::ScratchExhausted,
    StoreClass::Corrupt,
    StoreClass::CreateOnlyUnsupported,
    StoreClass::Refused,
];

/// Both stores.
pub const TARGETS: [Target; 2] = [Target::Staging, Target::Remote];

/// A store operation's place in [`STORE_OPS`]; exhaustive, so a new operation breaks here.
#[must_use]
pub const fn store_ordinal(op: StoreOp) -> usize {
    match op {
        StoreOp::List => 0,
        StoreOp::Read => 1,
        StoreOp::Upload => 2,
        StoreOp::Delete => 3,
    }
}

/// The class of a store error; exhaustive, so a new error variant or denial kind breaks here.
#[must_use]
pub const fn store_class(err: &StoreError) -> StoreClass {
    match err {
        StoreError::Transient { .. } => StoreClass::Transient,
        StoreError::Throttled { .. } => StoreClass::Throttled,
        StoreError::Denied { why, .. } => StoreClass::Denied(match why {
            Denied::Credentials => Denied::Credentials,
            Denied::Permission => Denied::Permission,
            Denied::ClockSkew => Denied::ClockSkew,
            Denied::BucketMissing => Denied::BucketMissing,
            Denied::Tls => Denied::Tls,
        }),
        StoreError::NotFound { .. } => StoreClass::NotFound,
        StoreError::AlreadyExists { .. } => StoreClass::AlreadyExists,
        StoreError::ScratchExhausted { .. } => StoreClass::ScratchExhausted,
        StoreError::Corrupt { .. } => StoreClass::Corrupt,
        StoreError::CreateOnlyUnsupported { .. } => StoreClass::CreateOnlyUnsupported,
        StoreError::Refused { .. } => StoreClass::Refused,
    }
}

const CORRUPT_IS_WORLD: &str = "an unusable stored object is a fact about the object: drawn as the \
                                CorruptPublish, SizeMismatch, ChecksumMismatch and FlipByteOnRead \
                                shapes, which change or misreport the object itself";
const NOT_FOUND_IS_STALE: &str = "a NotFound for an object the store holds is a stale or negative \
                                  read, the store-staleness draw (issue #127, item F); for one it \
                                  does not hold, the read follows a delete or a loss drawn elsewhere";
const TAKEN_IS_WORLD: &str = "a taken key is a fact about the store: a create-only upload is \
                              answered from what the key holds, an earlier ambiguous commit or \
                              another writer, each drawn elsewhere";
const NO_CREATE_ONLY_IS_WORLD: &str = "a store without create-only writes is a fact about the \
                                       store for the whole run: drawn as the world's \
                                       Weights::no_create_only, every create-only commit refused";
const NOT_FOUND_NEVER: &str = "neither store reports NotFound here: a listing of nothing is empty, a delete of nothing succeeds";

/// The cell of `op` and `class` on `target`: the staging store against `store-fs`, the replica
/// against `store-s3`.
#[must_use]
pub const fn store_cell(target: Target, op: StoreOp, class: StoreClass) -> Cell {
    match class {
        StoreClass::Transient => Cell::Drawn,
        StoreClass::Throttled => match target {
            Target::Remote => Cell::Drawn,
            Target::Staging => Cell::Impossible("a filesystem never asks to slow down"),
        },
        StoreClass::Denied(why) => denied_cell(target, why),
        StoreClass::NotFound => match op {
            StoreOp::Read => Cell::Impossible(NOT_FOUND_IS_STALE),
            StoreOp::List | StoreOp::Upload | StoreOp::Delete => Cell::Impossible(NOT_FOUND_NEVER),
        },
        StoreClass::AlreadyExists => match op {
            StoreOp::Upload => Cell::Impossible(TAKEN_IS_WORLD),
            StoreOp::List | StoreOp::Read | StoreOp::Delete => {
                Cell::Impossible("only a create-only upload asks whether a key is taken")
            }
        },
        StoreClass::ScratchExhausted => match (target, op) {
            (Target::Staging, StoreOp::Upload) => Cell::Drawn,
            (Target::Staging, StoreOp::List | StoreOp::Read | StoreOp::Delete) => {
                Cell::Impossible("scratch names are taken only by an upload")
            }
            (Target::Remote, StoreOp::List | StoreOp::Read | StoreOp::Upload | StoreOp::Delete) => {
                Cell::Impossible("store-s3 uploads through no scratch name")
            }
        },
        StoreClass::Corrupt => Cell::Impossible(CORRUPT_IS_WORLD),
        StoreClass::CreateOnlyUnsupported => match (target, op) {
            (Target::Remote, StoreOp::Upload) => Cell::Impossible(NO_CREATE_ONLY_IS_WORLD),
            (Target::Staging, StoreOp::Upload) => Cell::Impossible(
                "the engine's staging writes replace; it never asks for create-only",
            ),
            (_, StoreOp::List | StoreOp::Read | StoreOp::Delete) => {
                Cell::Impossible("only a create-only upload can be refused for lack of the check")
            }
        },
        StoreClass::Refused => match target {
            Target::Remote => Cell::Drawn,
            Target::Staging => Cell::Impossible("store-fs has no status codes and refuses nothing"),
        },
    }
}

const fn denied_cell(target: Target, why: Denied) -> Cell {
    match (target, why) {
        // A staging read refused during level-1 compaction was classified as the replica's
        // until #145 (compaction routes its local reads through `Fault::from_staging`).
        (Target::Staging, Denied::Permission) | (Target::Remote, _) => Cell::Drawn,
        (
            Target::Staging,
            Denied::Credentials | Denied::ClockSkew | Denied::BucketMissing | Denied::Tls,
        ) => Cell::Impossible("a local directory has only permissions to deny"),
    }
}

/// A concrete error of `class` for `op`, its payload drawn.
fn store_error(
    entropy: &SeededEntropy,
    target: Target,
    op: StoreOp,
    class: StoreClass,
) -> StoreError {
    match class {
        StoreClass::Transient => StoreError::Transient {
            op,
            kind: match target {
                Target::Remote => pick(
                    entropy,
                    &[
                        ErrorKind::ConnectionReset,
                        ErrorKind::TimedOut,
                        ErrorKind::BrokenPipe,
                    ],
                    ErrorKind::ConnectionReset,
                ),
                Target::Staging => pick(
                    entropy,
                    &[
                        ErrorKind::Other,
                        ErrorKind::StorageFull,
                        ErrorKind::QuotaExceeded,
                    ],
                    ErrorKind::Other,
                ),
            },
        },
        StoreClass::Throttled => StoreError::Throttled {
            op,
            retry_after: (below(entropy, 2) == 0).then_some(Duration::from_secs(2)),
        },
        StoreClass::Denied(why) => StoreError::Denied { op, why },
        StoreClass::Refused => StoreError::Refused {
            op,
            status: pick(entropy, &[400, 405, 411, 501], 400),
        },
        StoreClass::NotFound
        | StoreClass::AlreadyExists
        | StoreClass::ScratchExhausted
        | StoreClass::Corrupt
        | StoreClass::CreateOnlyUnsupported => {
            // NotFound, AlreadyExists and Corrupt are never drawn. A scratch exhaustion names a key the
            // engine only reports, never acts on.
            placeholder_error(op, class)
        }
    }
}

fn placeholder_error(op: StoreOp, class: StoreClass) -> StoreError {
    let key = lsr_ltx::Txid::new(1).map(lsr_core::FileKey::snapshot);
    let transient = StoreError::Transient {
        op,
        kind: ErrorKind::Other,
    };
    match class {
        StoreClass::ScratchExhausted => {
            key.map_or(transient, |key| StoreError::ScratchExhausted { key })
        }
        StoreClass::Corrupt => StoreError::Corrupt {
            key,
            cause: CorruptCause::NotARegularFile,
        },
        StoreClass::Transient
        | StoreClass::Throttled
        | StoreClass::Denied(_)
        | StoreClass::NotFound
        | StoreClass::AlreadyExists
        | StoreClass::CreateOnlyUnsupported
        | StoreClass::Refused => transient,
    }
}

/// An error for `op` on `target`, uniform over the classes a history draws ([`drawable`]).
#[must_use]
pub fn draw_store(entropy: &SeededEntropy, target: Target, op: StoreOp) -> StoreError {
    let classes: Vec<StoreClass> = STORE_CLASSES
        .iter()
        .copied()
        .filter(|class| drawable(store_cell(target, op, *class)))
        .collect();
    let class = pick_ref(entropy, &classes)
        .copied()
        .unwrap_or(StoreClass::Transient);
    store_error(entropy, target, op, class)
}

/// Whether an error says the request was not carried out, so it may not follow a publish:
/// a denial, a refusal or a full scratch space comes before anything is stored.
#[must_use]
pub const fn refused_before_acting(err: &StoreError) -> bool {
    match store_class(err) {
        StoreClass::Denied(_)
        | StoreClass::Refused
        | StoreClass::ScratchExhausted
        | StoreClass::CreateOnlyUnsupported => true,
        StoreClass::Transient
        | StoreClass::Throttled
        | StoreClass::NotFound
        | StoreClass::AlreadyExists
        | StoreClass::Corrupt => false,
    }
}

/// The stop a store error on `target` legitimately causes, if any. The staging store's errors
/// all retry: an operator can fix the directory without a restart.
#[must_use]
pub const fn store_stop(target: Target, err: &StoreError) -> Option<StopCause> {
    match (target, store_class(err)) {
        (Target::Remote, StoreClass::Denied(why)) => Some(StopCause::StoreDenied(why)),
        (Target::Remote, StoreClass::Refused) => Some(StopCause::StoreRefused),
        (Target::Remote, StoreClass::Corrupt) => Some(StopCause::RemoteCorrupt),
        (Target::Remote, StoreClass::CreateOnlyUnsupported) => {
            Some(StopCause::ConditionalWritesUnsupported)
        }
        (
            Target::Remote,
            StoreClass::Transient
            | StoreClass::Throttled
            | StoreClass::NotFound
            | StoreClass::AlreadyExists
            | StoreClass::ScratchExhausted,
        )
        | (Target::Staging, _) => None,
    }
}

fn below(entropy: &SeededEntropy, upper: u64) -> u64 {
    entropy.below(upper.max(1))
}

/// One of `from`, or `default` when `from` is empty.
fn pick<T: Copy>(entropy: &SeededEntropy, from: &[T], default: T) -> T {
    pick_ref(entropy, from).copied().unwrap_or(default)
}

fn pick_ref<'a, T>(entropy: &SeededEntropy, from: &'a [T]) -> Option<&'a T> {
    let at = usize::try_from(below(entropy, u64::try_from(from.len()).ok()?)).ok()?;
    from.get(at)
}

pub mod marks;
pub mod points;

#[cfg(test)]
mod tests;
