//! The checkpoint decision and the protocol that carries it out.
//!
//! Two halves. The decision is four pure functions over [`CheckpointState`]:
//! [`decide`] says whether to checkpoint and how, [`after_sync`] and
//! [`after_checkpoint`] move the state, [`next_wake`] says when the time trigger is
//! due. The protocol, [`checkpoint`], is Go's `checkpointWithExecutor`
//! (`db.go:2617-2801`) as one function over the source traits.
//!
//! What it changes from Go, each with a test:
//! - Six Go fields are one enum. `syncedToWALEnd` is gone: [`Continuity`]
//!   says what a restart can lose.
//! - The interval counts from the last checkpoint this engine finished, not from the database
//!   file's mtime, and it is inclusive.
//! - The PASSIVE that precedes a TRUNCATE hands back [`Next::Now`] instead of running
//!   the TRUNCATE inside the same call, so one tick runs at most one PASSIVE.
//! - A checkpoint writes one frame of its own (`bump_seq`). The state expects that frame
//!   and does not count it as a reason to checkpoint again, so an idle database stays idle (Go #896).
//!   For the same reason a busy PASSIVE does not leave the trigger armed: the next write re-arms it.
//! - A TRUNCATE that found the WAL unchanged takes no boundary snapshot (Go does).
//!
//! Protocol notes. A PASSIVE takes a write barrier, seals (syncs through the committed end),
//! checkpoints, and releases the barrier on every path; a barrier that could not be released never
//! replaces the fault that came first. The checkpoint releases and retakes the read transaction, so
//! from the moment it is called the position is demoted to [`Continuity::Unpinned`] and is
//! pinned again only by a step that proves `synced == committed` at the new epoch (or by step 6,
//! which the barrier makes sound). A fault returns the last position that is true.

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::Timestamp;
use lsr_wal::{OpenError, WalHeader, WalReader};

use crate::fault::{Clamped, Fault, Next, SnapshotCause};
use crate::layout::FileInfo;
use crate::position::{Continuity, Position, ReadEpoch, SyncedThrough};
use crate::source::{CheckpointMode, Checkpointer, PageSource, WalSource};
use crate::store::ObjectStore;
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step};

/// When to checkpoint. `None` turns a trigger off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointConfig {
    /// Checkpoint when this long has passed since the last one and something was synced (Go 1 minute).
    pub interval: Option<Duration>,
    /// Checkpoint PASSIVE at this many synced frames (Go 1000).
    pub min_pages: Option<NonZeroU32>,
    /// Checkpoint TRUNCATE at this many synced frames (Go 121359, about 500 MB of 4 KiB pages).
    pub truncate_pages: Option<NonZeroU32>,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval: Some(Duration::from_secs(60)),
            min_pages: NonZeroU32::new(1000),
            truncate_pages: NonZeroU32::new(121_359),
        }
    }
}

/// Where the checkpoint policy stands; one enum for what Go keeps in six fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointState {
    /// Nothing new since the last checkpoint finished at `last`.
    Idle {
        /// When it finished.
        last: Timestamp,
        /// Our own bump frame is in the WAL and not yet synced; one one-transaction sync is that frame.
        expect_bump: bool,
    },
    /// Something was synced since `last`; the time trigger is armed.
    Pending {
        /// When the last checkpoint finished.
        last: Timestamp,
    },
    /// A PASSIVE at the truncate threshold did not restart the WAL: the next decision is TRUNCATE.
    PassiveFailedAtTruncate {
        /// When the attempt finished.
        last: Timestamp,
    },
}

impl CheckpointState {
    /// The state of an engine that has just started: the WAL may hold uncheckpointed work.
    #[must_use]
    pub const fn boot(now: Timestamp) -> Self {
        Self::Pending { last: now }
    }

    /// When the last checkpoint finished (or the engine started).
    #[must_use]
    pub const fn last(&self) -> Timestamp {
        match self {
            Self::Idle { last, .. }
            | Self::Pending { last }
            | Self::PassiveFailedAtTruncate { last } => *last,
        }
    }
}

/// What the policy looks at, read after a sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Observed {
    /// The clock.
    pub now: Timestamp,
    /// Frames of the WAL the synced position covers. Not the file size: a WAL
    /// file that SQLite reused is longer than what matters (Go #997).
    pub synced_frames: u32,
    /// The last step reached the committed end (it was not stopped by the cap).
    pub through: bool,
}

impl Observed {
    /// Reads the frame count from the position the last step left.
    #[must_use]
    pub const fn of(now: Timestamp, position: &Position, through: bool) -> Self {
        Self {
            now,
            synced_frames: position.synced().offset().frame_index(),
            through,
        }
    }
}

/// Why a PASSIVE checkpoint is called for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// The WAL is at the truncate threshold; try PASSIVE before the blocking TRUNCATE.
    TruncateThreshold,
    /// The WAL is at the minimum.
    MinPages,
    /// The interval has passed since the last checkpoint.
    Interval,
}

/// What to do now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Nothing.
    Nothing,
    /// A PASSIVE checkpoint.
    Passive(Why),
    /// A TRUNCATE checkpoint.
    Truncate,
}

fn over_truncate(observed: &Observed, cfg: &CheckpointConfig) -> bool {
    cfg.truncate_pages
        .is_some_and(|limit| observed.synced_frames >= limit.get())
}

fn since(last: Timestamp, now: Timestamp) -> Duration {
    u64::try_from(now.unix_millis().saturating_sub(last.unix_millis()))
        .map_or(Duration::ZERO, Duration::from_millis)
}

fn after(last: Timestamp, wait: Duration) -> Timestamp {
    let millis = i64::try_from(wait.as_millis()).unwrap_or(i64::MAX);
    Timestamp::from_unix_millis(last.unix_millis().saturating_add(millis))
}

/// Go's priority ladder (`checkpointIfNeeded`, `db.go:1507-1594`).
///
/// 1. Only the truncate threshold can trigger while the last step was stopped
///    by the cap (`db.go:1410-1414`). It counts synced frames (#997).
/// 2. At the truncate threshold: TRUNCATE after a failed PASSIVE, else PASSIVE.
/// 3. At the minimum, PASSIVE; not from `Idle`: nothing was synced since the last
///    checkpoint, so another one has nothing new to move and, with a reader that
///    blocks the restart, would repeat on its own bump frame forever.
/// 4. Pending, the interval passed since `last`, and more than one frame: PASSIVE.
#[must_use]
pub fn decide(state: &CheckpointState, observed: &Observed, cfg: &CheckpointConfig) -> Decision {
    if over_truncate(observed, cfg) {
        return match state {
            CheckpointState::PassiveFailedAtTruncate { .. } => Decision::Truncate,
            CheckpointState::Idle { .. } | CheckpointState::Pending { .. } => {
                Decision::Passive(Why::TruncateThreshold)
            }
        };
    }
    if !observed.through {
        return Decision::Nothing;
    }
    let armed = !matches!(state, CheckpointState::Idle { .. });
    if armed
        && cfg
            .min_pages
            .is_some_and(|min| observed.synced_frames >= min.get())
    {
        return Decision::Passive(Why::MinPages);
    }
    if let CheckpointState::Pending { last } = state
        && cfg
            .interval
            .is_some_and(|interval| since(*last, observed.now) >= interval)
        && observed.synced_frames > 1
    {
        return Decision::Passive(Why::Interval);
    }
    Decision::Nothing
}

/// The state after a sync step that staged `wrote_txs` transactions.
///
/// Below the truncate threshold a failed PASSIVE no longer matters and the
/// time trigger is armed again (`db.go:1511-1513`). A sync that wrote arms the
/// time trigger too, except that `Idle { expect_bump: true }` absorbs exactly one
/// one-transaction sync: our own bump frame (#896, `db.go:1503-1506`).
#[must_use]
pub fn after_sync(
    state: CheckpointState,
    wrote_txs: u32,
    observed: &Observed,
    cfg: &CheckpointConfig,
) -> CheckpointState {
    let state = match state {
        CheckpointState::PassiveFailedAtTruncate { last } if !over_truncate(observed, cfg) => {
            CheckpointState::Pending { last }
        }
        CheckpointState::PassiveFailedAtTruncate { .. }
        | CheckpointState::Pending { .. }
        | CheckpointState::Idle { .. } => state,
    };
    match state {
        CheckpointState::Idle { last, expect_bump } if wrote_txs > 0 => {
            if expect_bump && wrote_txs == 1 {
                CheckpointState::Idle {
                    last,
                    expect_bump: false,
                }
            } else {
                CheckpointState::Pending { last }
            }
        }
        CheckpointState::Idle { .. }
        | CheckpointState::Pending { .. }
        | CheckpointState::PassiveFailedAtTruncate { .. } => state,
    }
}

/// What a checkpoint did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointOutcome {
    /// SQLite could not finish and the WAL is the same generation.
    Busy,
    /// The checkpoint finished or stopped short without a restart.
    NotRestarted,
    /// A PASSIVE that moved the WAL to a new generation.
    Restarted,
    /// A TRUNCATE; the boundary snapshot covers the new generation.
    Truncated,
}

/// The state and the wake after a checkpoint ran for `decision`.
///
/// - A PASSIVE at the truncate threshold that did not restart the WAL goes to
///   `PassiveFailedAtTruncate` and [`Next::Now`]; Go runs the TRUNCATE in the same call
///   (`db.go:1524-1546`). A TRUNCATE that found the WAL unchanged stays there and waits
///   one interval, so a long reader costs one blocking attempt per interval.
/// - A busy PASSIVE at the minimum or the interval does not leave the trigger
///   armed: Go ignores busy there (`db.go:1556-1561`, `1583-1586`) and so retries on its own
///   bump frame every tick. Here the minimum waits for the next write, and the
///   interval for one more interval.
/// - Every other finished checkpoint is `Idle`; it expects a bump frame unless the
///   protocol already synced it (a restart or a truncate does).
#[must_use]
pub fn after_checkpoint(
    state: CheckpointState,
    decision: Decision,
    outcome: &CheckpointOutcome,
    observed: &Observed,
    cfg: &CheckpointConfig,
) -> (CheckpointState, Next) {
    let now = observed.now;
    let idle = |expect_bump| CheckpointState::Idle {
        last: now,
        expect_bump,
    };
    let next = match (decision, outcome) {
        (Decision::Nothing, _) => state,
        (
            Decision::Passive(Why::TruncateThreshold),
            CheckpointOutcome::Busy | CheckpointOutcome::NotRestarted,
        ) => {
            return (
                CheckpointState::PassiveFailedAtTruncate { last: now },
                Next::Now,
            );
        }
        (Decision::Truncate, CheckpointOutcome::Busy | CheckpointOutcome::NotRestarted) => {
            let wake = cfg
                .interval
                .map_or(Next::OnWalChange, |interval| Next::At(after(now, interval)));
            return (CheckpointState::PassiveFailedAtTruncate { last: now }, wake);
        }
        (Decision::Passive(Why::MinPages), CheckpointOutcome::Busy) => idle(true),
        (Decision::Passive(Why::Interval), CheckpointOutcome::Busy) => {
            CheckpointState::Pending { last: now }
        }
        (
            Decision::Passive(_) | Decision::Truncate,
            CheckpointOutcome::Restarted | CheckpointOutcome::Truncated,
        ) => idle(false),
        (Decision::Passive(_), CheckpointOutcome::NotRestarted) => idle(true),
    };
    let wake = next_wake(&next, cfg).map_or(Next::OnWalChange, Next::At);
    (next, wake)
}

/// When the time trigger comes due, if it is armed.
#[must_use]
pub fn next_wake(state: &CheckpointState, cfg: &CheckpointConfig) -> Option<Timestamp> {
    match (state, cfg.interval) {
        (CheckpointState::Pending { last }, Some(interval)) => Some(after(*last, interval)),
        (CheckpointState::Pending { .. }, None)
        | (CheckpointState::Idle { .. }, _)
        | (CheckpointState::PassiveFailedAtTruncate { .. }, _) => None,
    }
}

/// A finished checkpoint.
#[derive(Debug, PartialEq, Eq)]
pub struct Checkpointed {
    /// The proof that the position reached the committed end at the current epoch
    /// (a PASSIVE that restarted nothing leaves our own bump frame past it, by design).
    pub through: SyncedThrough,
    /// What happened.
    pub outcome: CheckpointOutcome,
    /// The files staged on the way: the seal, the new generation or the boundary snapshot.
    pub files: Vec<FileInfo>,
    /// Clock steps back the staged files were stamped around.
    pub clamped: Vec<Clamped>,
}

/// A checkpoint that did not finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointFailed {
    /// What went wrong.
    pub fault: Fault,
    /// The last position that is true. It moved only for a step that completed,
    /// and is unpinned once the checkpoint released the read transaction.
    pub position: Position,
}

/// Why the WAL could not be opened as a reader.
const fn open_fault(err: &OpenError) -> Fault {
    match err {
        OpenError::Io(_) => Fault::LocalIo,
        // A header torn mid-restart: the next recover classifies it.
        OpenError::Truncated { .. } | OpenError::Header(_) => Fault::WalMoved,
    }
}

struct Protocol<'a, W, P, C, L> {
    wal: &'a mut W,
    db: &'a mut P,
    ck: &'a mut C,
    staging: &'a L,
    cfg: SyncConfig,
    now: Timestamp,
    good: Position,
    files: Vec<FileInfo>,
    clamped: Vec<Clamped>,
}

impl<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore> Protocol<'_, W, P, C, L> {
    fn reader(&mut self) -> Result<WalReader<W::Wal>, Fault> {
        let file = self
            .wal
            .open()
            .map_err(Fault::from_source)?
            .ok_or(Fault::WalUnlinked)?;
        WalReader::open(file).map_err(|err| open_fault(&err))
    }

    fn header(&mut self) -> Result<WalHeader, Fault> {
        Ok(*self.reader()?.header())
    }

    fn unpin(&mut self) {
        self.good = self.good.with_continuity(Continuity::Unpinned);
    }

    fn pin(&mut self, epoch: ReadEpoch) {
        if self.wal.epoch() == epoch {
            self.good = self.good.with_continuity(Continuity::Pinned(epoch));
        }
    }

    /// One uncapped step. The position moves only when the file is staged; one
    /// that stepped over an epoch change is not pinned.
    async fn step(&mut self, step: Step) -> Result<(), Fault> {
        let mut reader = self.reader()?;
        let committed = self.wal.committed().map_err(Fault::from_source)?;
        let epoch = self.wal.epoch();
        let cfg = SyncConfig {
            cap: None,
            ..self.cfg
        };
        let synced = sync_step(
            step,
            &mut reader,
            committed,
            epoch,
            &mut *self.db,
            self.staging,
            &cfg,
            self.now,
        )
        .await
        .map_err(|failed| failed.fault)?;
        let through = match synced {
            Synced::Idle(through) => through,
            Synced::Wrote {
                file,
                end: StepEnd::Through(through),
                clamped,
                ..
            } => {
                self.files.push(file);
                self.clamped.extend(clamped);
                through
            }
            Synced::Wrote {
                end: StepEnd::Limited(_),
                ..
            } => return Err(Fault::Bug),
        };
        self.good = through.into_position();
        if self.wal.epoch() != epoch {
            self.unpin();
        }
        Ok(())
    }

    async fn passive(&mut self) -> Result<CheckpointOutcome, Fault> {
        let before = self.header()?;
        let barrier = self.ck.begin_barrier().map_err(Fault::from_source)?;
        let sealed = self.seal_and_checkpoint().await;
        let released = self.ck.end_barrier(barrier);
        let run = sealed?;
        released.map_err(Fault::from_source)?;
        self.ck.bump_seq().map_err(Fault::from_source)?;
        if self.header()? == before {
            // Sound only because the barrier kept writers out until the read
            // transaction was retaken.
            self.pin(run.epoch);
            return Ok(if run.busy {
                CheckpointOutcome::Busy
            } else {
                CheckpointOutcome::NotRestarted
            });
        }
        self.step(Step::NewGeneration(self.good)).await?;
        Ok(CheckpointOutcome::Restarted)
    }

    async fn seal_and_checkpoint(&mut self) -> Result<crate::source::CheckpointRun, Fault> {
        self.step(Step::Incremental(self.good)).await?;
        let run = self.ck.checkpoint(CheckpointMode::Passive);
        self.unpin();
        run.map_err(Fault::from_source)
    }

    async fn truncate(&mut self) -> Result<CheckpointOutcome, Fault> {
        let before = self.header()?;
        let run = self.ck.checkpoint(CheckpointMode::Truncate);
        self.unpin();
        let run = run.map_err(Fault::from_source)?;
        self.ck.bump_seq().map_err(Fault::from_source)?;
        let after = self.header()?;
        let shorter = self
            .wal
            .len()
            .map_err(Fault::from_source)?
            .is_none_or(|len| !len.covers(self.good.synced()));
        if after == before && !shorter {
            // No barrier held writers out, so a commit may sit between the
            // proof and the checkpoint: catch up before claiming the end.
            self.step(Step::Incremental(self.good)).await?;
            return Ok(if run.busy {
                CheckpointOutcome::Busy
            } else {
                CheckpointOutcome::NotRestarted
            });
        }
        self.boundary_snapshot().await?;
        Ok(CheckpointOutcome::Truncated)
    }

    /// Everything SQLite backfilled and truncated is in the database file or the new generation:
    /// snapshot it under the write lock so no commit slips between the two.
    async fn boundary_snapshot(&mut self) -> Result<(), Fault> {
        let lock = self.ck.begin_write_lock().map_err(Fault::from_source)?;
        let snapshot = Step::Snapshot {
            after: Some(self.good.txid()),
            prev_timestamp: Some(self.good.timestamp()),
            cause: SnapshotCause::CheckpointBoundary,
        };
        let stepped = self.step(snapshot).await;
        let released = self.ck.end_write_lock(lock);
        stepped?;
        released.map_err(Fault::from_source)
    }
}

/// Runs one checkpoint from a position that reached the committed end.
///
/// The proof is consumed and a new one returned: it carries the position, so a
/// stale one cannot be passed. A PASSIVE seals, checkpoints under a write
/// barrier, and follows a restarted WAL; a TRUNCATE checkpoints and, when the
/// WAL changed, snapshots under the write lock. Every barrier or lock taken is
/// released first on a fault, and a failed release never replaces that fault.
///
/// # Errors
/// [`CheckpointFailed`], with the last position that is true.
#[expect(
    clippy::too_many_arguments,
    reason = "the plan fixes this signature; each argument is a distinct source of the protocol"
)]
pub async fn checkpoint<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore>(
    proof: SyncedThrough,
    mode: CheckpointMode,
    wal: &mut W,
    db: &mut P,
    ck: &mut C,
    staging: &L,
    cfg: &SyncConfig,
    now: Timestamp,
) -> Result<Checkpointed, CheckpointFailed> {
    let mut protocol = Protocol {
        wal,
        db,
        ck,
        staging,
        cfg: *cfg,
        now,
        good: proof.into_position(),
        files: Vec::new(),
        clamped: Vec::new(),
    };
    let result = match mode {
        CheckpointMode::Passive => protocol.passive().await,
        CheckpointMode::Truncate => protocol.truncate().await,
    };
    match result {
        Ok(outcome) => Ok(Checkpointed {
            through: SyncedThrough::new(protocol.good),
            outcome,
            files: protocol.files,
            clamped: protocol.clamped,
        }),
        Err(fault) => Err(CheckpointFailed {
            fault,
            position: protocol.good,
        }),
    }
}

#[cfg(test)]
mod tests;
