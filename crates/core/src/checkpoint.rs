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
//! - A TRUNCATE takes no snapshot of the database. Go copies the whole database after every
//!   TRUNCATE that moved the WAL (`db.go:2737-2797`), because a commit may have slipped in
//!   between its last sync and the TRUNCATE and been truncated unseen. Here the TRUNCATE runs
//!   after the barrier's PASSIVE, from the writer connection, while the read transaction that
//!   PASSIVE retook is held: such a commit then makes it busy (docs/decisions/D263.md).
//! - Another connection holding the write lock is not a failure. Go's connection waits its
//!   busy timeout inside SQLite for the barrier and, still refused, skips the checkpoint until
//!   its next sync without a word (`db.go:1556-1561`). Ours never waits inside SQLite, so a
//!   refused barrier is [`CheckpointOutcome::LockBusy`] and the caller asks again after
//!   [`lock_busy_wait`]. A sequence bump refused the same way means that writer's commit
//!   moves the WAL on in place of our frame: [`CheckpointOutcome::Overtaken`].
//!   Neither counts a failure, waits out a backoff or degrades the task.
//!
//! Protocol notes. A PASSIVE takes a write barrier, seals (syncs through the committed end),
//! checkpoints, and releases the barrier on every path; a barrier that could not be released never
//! replaces the fault that came first. The checkpoint releases and retakes the read transaction.
//! Under the PASSIVE's barrier no writer can commit between the seal's proof of
//! `synced == committed` and that retake, so the position is pinned to the new hold as soon as
//! the call returns, whatever SQLite reported and whatever fails afterwards: a WAL the next
//! writer restarts is then followed, never snapshotted. Only a retake that was lost leaves the
//! position [`Continuity::Broken`]. A TRUNCATE is the same PASSIVE, then the TRUNCATE on the
//! writer connection, which leaves that hold alone. Success and fault
//! both return a [`Staged`]: the last position
//! that is true together with the files the completed steps staged, so a fault after the seal
//! (a checkpoint that failed, a failed bump or release) cannot strand the sealed file unuploaded. Its
//! proof is present only when the last step reached the committed end at the current epoch; a
//! PASSIVE that restarted nothing ends pinned but behind our own bump frame. A step that
//! finds the wal-index reset ahead of the WAL header proves nothing, so the protocol stops there
//! with `WalMoved` and the position it had; a snapshot such a step staged is written again over
//! the same key by the next step from that position.

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::Timestamp;
use lsr_wal::{OpenError, WalHeader, WalReader};

use crate::env::Clock;
use crate::fault::role::{Role, Staging};
use crate::fault::{BugSite, Fault, Next};
use crate::layout::{FileInfo, FileKey};
use crate::pages::PageChecksumTable;
use crate::position::{Continuity, Position, ReadLockHold, SyncedThrough};
use crate::source::{
    CheckpointMode, CheckpointRun, Checkpointer, PageSource, SourceError, WalSource,
};
use crate::store::ObjectStore;
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step, wal_barrier};
use crate::witness::Witnessed;

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
pub enum CheckpointTrigger {
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
    Passive(CheckpointTrigger),
    /// A TRUNCATE checkpoint.
    Truncate,
}

fn over_truncate(synced_frames: u32, cfg: &CheckpointConfig) -> bool {
    cfg.truncate_pages
        .is_some_and(|limit| synced_frames >= limit.get())
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
    if over_truncate(observed.synced_frames, cfg) {
        return match state {
            CheckpointState::PassiveFailedAtTruncate { .. } => Decision::Truncate,
            CheckpointState::Idle { .. } | CheckpointState::Pending { .. } => {
                Decision::Passive(CheckpointTrigger::TruncateThreshold)
            }
        };
    }
    if !observed.through {
        return Decision::Nothing;
    }
    let armed = match state {
        CheckpointState::Idle { .. } => false,
        CheckpointState::Pending { .. } | CheckpointState::PassiveFailedAtTruncate { .. } => true,
    };
    if armed
        && cfg
            .min_pages
            .is_some_and(|min| observed.synced_frames >= min.get())
    {
        return Decision::Passive(CheckpointTrigger::MinPages);
    }
    if let CheckpointState::Pending { last } = state
        && cfg
            .interval
            .is_some_and(|interval| observed.now.saturating_duration_since(*last) >= interval)
        && observed.synced_frames > 1
    {
        return Decision::Passive(CheckpointTrigger::Interval);
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
pub(crate) fn after_sync(
    state: CheckpointState,
    wrote_txs: u32,
    synced_frames: u32,
    cfg: &CheckpointConfig,
) -> CheckpointState {
    let state = match state {
        CheckpointState::PassiveFailedAtTruncate { last } if !over_truncate(synced_frames, cfg) => {
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
    /// Another connection held the write lock when the PASSIVE asked for its barrier: SQLite
    /// was not asked to checkpoint and nothing changed. The application is writing; the
    /// decision is asked again after [`lock_busy_wait`].
    LockBusy,
    /// SQLite could not finish and the WAL is the same generation.
    Busy,
    /// The checkpoint finished or stopped short without a restart.
    NotRestarted,
    /// A PASSIVE ran, and another connection held the write lock when the sequence bump was
    /// due: that writer's commit, not a frame of ours, moves the WAL on, in this generation
    /// or a new one. The position is pinned as after any PASSIVE, so the next sync follows
    /// either.
    Overtaken,
    /// A PASSIVE that moved the WAL to a new generation.
    Restarted,
    /// A TRUNCATE emptied the WAL, and the protocol followed it into the new generation.
    Truncated,
}

/// The first wait after a write lock that was busy ([`CheckpointOutcome::LockBusy`]).
pub const LOCK_BUSY_WAIT: Duration = Duration::from_millis(10);

/// How long to wait before asking for the write lock again after `skips` refusals in a row:
/// [`LOCK_BUSY_WAIT`] doubled for each refusal after the first, at most `cap`.
///
/// SQLite's busy handler, which Go's connection relies on, asks again after a few
/// milliseconds and then less often; this is the same shape outside SQLite. A writer between
/// two short transactions is past within the first waits; one that holds the lock for long
/// costs one attempt per `cap`, the cadence of an ordinary sync.
#[must_use]
pub fn lock_busy_wait(skips: NonZeroU32, cap: Duration) -> Duration {
    crate::fault::retry_wait(skips, LOCK_BUSY_WAIT).min(cap)
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
/// - A write lock that was busy at the barrier changes nothing and is asked again
///   ([`CheckpointOutcome::LockBusy`]). A PASSIVE overtaken at its bump counts as one that
///   did not restart the WAL at the truncate threshold, and otherwise as finished.
/// - Every other finished checkpoint is `Idle`; it expects a bump frame unless the
///   protocol already synced it (a restart or a truncate does) or wrote none (an overtaken
///   bump).
#[must_use]
pub(crate) fn after_checkpoint(
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
        // Nothing ran: the state stands and the decision is asked again. The caller spaces
        // the asking ([`lock_busy_wait`]).
        (Decision::Passive(_) | Decision::Truncate, CheckpointOutcome::LockBusy) => {
            return (state, Next::Now);
        }
        (
            Decision::Passive(CheckpointTrigger::TruncateThreshold),
            CheckpointOutcome::Busy
            | CheckpointOutcome::NotRestarted
            | CheckpointOutcome::Overtaken,
        ) => {
            return (
                CheckpointState::PassiveFailedAtTruncate { last: now },
                Next::Now,
            );
        }
        (
            Decision::Truncate,
            CheckpointOutcome::Busy
            | CheckpointOutcome::NotRestarted
            | CheckpointOutcome::Overtaken,
        ) => {
            let wake = cfg.interval.map_or(Next::OnWalChange, |interval| {
                Next::At(now.saturating_add(interval))
            });
            return (CheckpointState::PassiveFailedAtTruncate { last: now }, wake);
        }
        (Decision::Passive(CheckpointTrigger::MinPages), CheckpointOutcome::Busy) => idle(true),
        (Decision::Passive(CheckpointTrigger::Interval), CheckpointOutcome::Busy) => {
            CheckpointState::Pending { last: now }
        }
        // No frame of ours is in the WAL to expect: a restart or a truncate synced it, and
        // an overtaken PASSIVE wrote none (the writer that overtook it arms the triggers).
        (
            Decision::Passive(_) | Decision::Truncate,
            CheckpointOutcome::Restarted | CheckpointOutcome::Truncated,
        )
        | (
            Decision::Passive(CheckpointTrigger::MinPages | CheckpointTrigger::Interval),
            CheckpointOutcome::Overtaken,
        ) => idle(false),
        (Decision::Passive(_), CheckpointOutcome::NotRestarted) => idle(true),
    };
    let wake = next_wake(&next, cfg).map_or(Next::OnWalChange, Next::At);
    (next, wake)
}

/// When the time trigger comes due, if it is armed.
#[must_use]
pub(crate) fn next_wake(state: &CheckpointState, cfg: &CheckpointConfig) -> Option<Timestamp> {
    match (state, cfg.interval) {
        (CheckpointState::Pending { last }, Some(interval)) => Some(last.saturating_add(interval)),
        (CheckpointState::Pending { .. }, None)
        | (CheckpointState::Idle { .. }, _)
        | (CheckpointState::PassiveFailedAtTruncate { .. }, _) => None,
    }
}

/// Where the protocol's last step left the position, and whether that step
/// proved it reached the committed end.
#[derive(Debug, PartialEq, Eq)]
enum Reached {
    /// A step reached the committed end at the read-lock hold that still holds.
    Through(SyncedThrough),
    /// No step since proved the end: the checkpoint released the read
    /// transaction, or a PASSIVE that restarted nothing left our own bump
    /// frame past the position (by design).
    Short(Position),
}

impl Reached {
    const fn position(&self) -> Position {
        match self {
            Self::Through(through) => *through.position(),
            Self::Short(position) => *position,
        }
    }
}

/// What a checkpoint staged and the position that follows from it, whether
/// the checkpoint finished or not.
///
/// One value so a caller cannot take the position and drop the files: the
/// position's transaction is the newest file staged, and a file a step staged
/// before a later step failed must still be uploaded.
#[derive(Debug, PartialEq, Eq)]
pub struct Staged {
    reached: Reached,
    files: Vec<FileInfo>,
    witnesses: Vec<Witnessed>,
    quiet: Vec<FileKey>,
}

impl Staged {
    /// The last position that is true; its transaction is the newest staged
    /// file. It is unpinned once the checkpoint released the read transaction,
    /// unless a later step pinned it again.
    #[must_use]
    pub const fn position(&self) -> Position {
        self.reached.position()
    }

    /// The proof that the position equals the committed end, present only when
    /// the protocol ended on a step that reached it at the current epoch.
    #[must_use]
    #[cfg(any(test, feature = "testing"))]
    pub const fn proof(&self) -> Option<&SyncedThrough> {
        match &self.reached {
            Reached::Through(through) => Some(through),
            Reached::Short(_) => None,
        }
    }

    /// The proof, giving up the rest.
    #[must_use]
    #[cfg(any(test, feature = "testing"))]
    pub fn into_proof(self) -> Option<SyncedThrough> {
        match self.reached {
            Reached::Through(through) => Some(through),
            Reached::Short(_) => None,
        }
    }

    /// The files staged on the way: the seal and the new generation, or a busy TRUNCATE's catch-up.
    #[must_use]
    #[cfg(any(test, feature = "testing"))]
    pub fn files(&self) -> &[FileInfo] {
        &self.files
    }

    /// The uploader's tokens for [`Self::files`]: this process wrote them.
    #[must_use]
    pub fn witnesses(&self) -> &[Witnessed] {
        &self.witnesses
    }

    /// Those of [`Self::files`] that change no page of the state the files before them left:
    /// the engine's own writes, such as the frame that moves the WAL past a restart, which
    /// fail-stop does not count as lag (`crate::failstop`).
    #[must_use]
    pub fn quiet(&self) -> &[FileKey] {
        &self.quiet
    }
}

/// A finished checkpoint.
#[derive(Debug, PartialEq, Eq)]
pub struct Checkpointed {
    /// What was staged, the position it leaves and its proof, if any.
    pub staged: Staged,
    /// What happened.
    pub outcome: CheckpointOutcome,
    /// What SQLite's checkpoint call reported as the read transaction was retaken: where the
    /// read lock now sits, for the application-checkpoint observer (`crate::app`).
    pub run: Option<CheckpointRun>,
}

/// A checkpoint that did not finish.
#[derive(Debug, PartialEq, Eq)]
pub struct CheckpointFailed {
    /// What went wrong.
    pub fault: Fault,
    /// What the steps that completed staged, and the last position that is
    /// true. The position moved only for a step that completed.
    pub staged: Staged,
    /// What SQLite's checkpoint call reported, when it returned before the fault.
    pub run: Option<CheckpointRun>,
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
    pages: &'a mut PageChecksumTable,
    ck: &'a mut C,
    staging: &'a L,
    cfg: SyncConfig,
    /// Read after each step's committed read for that step's file.
    clock: &'a dyn Clock,
    good: Reached,
    files: Vec<FileInfo>,
    witnesses: Vec<Witnessed>,
    quiet: Vec<FileKey>,
    run: Option<CheckpointRun>,
}

impl<W: WalSource, P: PageSource, C: Checkpointer, L: Role> Protocol<'_, W, P, C, L> {
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

    /// The last position that is true.
    const fn position(&self) -> Position {
        self.good.position()
    }

    fn release_read_lock(&mut self) {
        self.good = Reached::Short(self.position().with_continuity(Continuity::Broken));
    }

    /// Pinned again, but not through: the proof was consumed by the checkpoint, and our bump
    /// frame may come after the position.
    fn take_read_lock(&mut self, epoch: ReadLockHold) {
        if self.wal.epoch() == epoch {
            self.good = Reached::Short(self.position().with_continuity(Continuity::Held(epoch)));
        }
    }

    /// One uncapped step. The position moves only when the file is staged; one
    /// that stepped over an epoch change is not pinned.
    async fn step(&mut self, step: Step) -> Result<(), Fault> {
        let mut reader = self.reader()?;
        let committed = self.wal.committed().map_err(Fault::from_source)?;
        // After the committed read, so the file's created-at time follows every
        // transaction it carries (see `Env::clock`).
        let stamp = self.clock.now();
        wal_barrier(&mut *self.wal)?;
        let cfg = SyncConfig {
            cap: None,
            ..self.cfg
        };
        let synced = sync_step(
            step,
            &mut reader,
            committed,
            &mut *self.db,
            &mut *self.pages,
            self.staging,
            &cfg,
            stamp,
        )
        .await
        .map_err(|failed| failed.fault)?;
        let through = match synced {
            Synced::Idle(through) => through,
            // A wal-index reset ahead of the header proves no end: nothing may
            // be checkpointed on it, and the position stays as it was.
            Synced::IndexReset
            | Synced::Wrote {
                end: StepEnd::Unproven(_),
                ..
            } => return Err(Fault::WalMoved),
            Synced::Wrote {
                file,
                witness,
                end: StepEnd::Through(through),
                changes,
                ..
            } => {
                if !changes {
                    self.quiet.push(file.key);
                }
                self.files.push(file);
                self.witnesses.push(witness);
                through
            }
            Synced::Wrote {
                end: StepEnd::Limited(_),
                ..
            } => return Err(BugSite::CappedCheckpointStep.hit()),
        };
        self.good = match through.confirm(&*self.wal) {
            Ok(through) => Reached::Through(through),
            Err(position) => Reached::Short(position),
        };
        Ok(())
    }

    /// The sequence bump; `false` when another connection holds the write lock, whose commit
    /// moves the WAL on in place of our frame.
    fn bump(&mut self) -> Result<bool, Fault> {
        let bumped = self.ck.bump_seq();
        if bumped == Err(SourceError::Busy) {
            return Ok(false);
        }
        bumped.map_err(Fault::from_source)?;
        Ok(true)
    }

    /// A PASSIVE, or with `truncate` a TRUNCATE: the barrier, the seal and the PASSIVE on the
    /// reader, then for a TRUNCATE the TRUNCATE on the writer connection, with the read
    /// transaction the PASSIVE retook still held ([`Self::truncate_held`]).
    async fn checkpoint(&mut self, truncate: bool) -> Result<CheckpointOutcome, Fault> {
        let before = self.header()?;
        let barrier = self.ck.begin_barrier();
        if barrier == Err(SourceError::Busy) {
            return Ok(CheckpointOutcome::LockBusy);
        }
        let barrier = barrier.map_err(Fault::from_source)?;
        let sealed = self.seal_and_checkpoint().await;
        let released = self.ck.end_barrier(barrier);
        let run = sealed?;
        released.map_err(Fault::from_source)?;
        let truncated = truncate && self.truncate_held()?;
        if !self.bump()? {
            return Ok(CheckpointOutcome::Overtaken);
        }
        if self.header()? == before {
            if truncate {
                // Our bump frame and any commit the busy TRUNCATE left: staged now, so the
                // frame does not wake the next TRUNCATE.
                self.step(Step::Incremental(self.position())).await?;
                return Ok(CheckpointOutcome::Busy);
            }
            return Ok(if run.busy {
                CheckpointOutcome::Busy
            } else {
                CheckpointOutcome::NotRestarted
            });
        }
        if truncated && self.position().continuity() == Continuity::Broken {
            return Ok(CheckpointOutcome::Truncated);
        }
        self.step(Step::NewGeneration(self.position())).await?;
        Ok(if truncated {
            CheckpointOutcome::Truncated
        } else {
            CheckpointOutcome::Restarted
        })
    }

    /// The TRUNCATE, run on the writer connection while the read transaction the barrier's
    /// PASSIVE retook is still held, and only when the position is pinned to that hold:
    /// whether it emptied the WAL.
    ///
    /// That hold began at `synced == committed` under the barrier. A TRUNCATE succeeds only
    /// with every frame backfilled and no reader on a mark above 0 (`walCheckpoint`), and while
    /// the engine holds its read no backfill can pass its mark: on mark 0 none runs at all (a
    /// backfill needs read lock 0 exclusive), and on a mark above 0 the TRUNCATE is busy on the
    /// engine's own slot. So a commit after the barrier's release leaves frames unbackfilled
    /// and the TRUNCATE busy, and one that succeeds truncated only frames the seal staged: the
    /// next generation is followed as after a PASSIVE, with no snapshot of the database
    /// (docs/decisions/D263.md). What SQLite reports is not kept for the application-checkpoint
    /// observer: where the read lock sits is the PASSIVE's answer.
    fn truncate_held(&mut self) -> Result<bool, Fault> {
        let epoch = self.wal.epoch();
        if self.position().continuity() != Continuity::Held(epoch) {
            return Ok(false);
        }
        let sealed = self.position().cursor().salts();
        let run = match self.ck.checkpoint(CheckpointMode::Truncate) {
            // Another connection held the checkpoint lock, or our writer sat in a transaction:
            // SQLite truncated nothing.
            Err(SourceError::Busy) => return Ok(false),
            run => run.map_err(Fault::from_source)?,
        };
        // A backend that ended the read transaction voids the proof: the position is not
        // pinned, and the next sync snapshots a WAL that moved.
        if self.wal.epoch() != epoch {
            self.release_read_lock();
            return Err(Fault::ReadTxLost);
        }
        if run.busy {
            return Ok(false);
        }
        // The TRUNCATE reset the generation the seal staged, and nothing reset it in between:
        // a restart by a writer that then died before clearing the backfill count could make
        // frames nobody copied look copied. And no other copy of SQLite in the process could
        // take a lock past ours. Short of either, the proof is void: the position is not
        // pinned, and the next sync snapshots the new generation.
        let now = self
            .wal
            .committed()
            .map_err(Fault::from_source)?
            .end()
            .salts();
        if !run.one_sqlite || now.salt1 != sealed.salt1.wrapping_add(1) {
            self.release_read_lock();
        }
        Ok(true)
    }

    /// The seal and the PASSIVE, both under the barrier.
    ///
    /// The read transaction the checkpoint call retakes is the proof of continuity from here
    /// on, and it is sound only because the barrier keeps writers out from the seal's
    /// committed read until that retake: the hold begins at `synced == committed`. So the
    /// position is pinned to it as the call returns, on every answer but a lost retake, and
    /// stays pinned through whatever fails later (a busy bump, a release that failed, a step
    /// over the new generation). A writer that restarts the WAL after the barrier is released
    /// is then followed by the next sync; before, a fault in between left the position
    /// unpinned and that restart cost a snapshot of the whole database.
    async fn seal_and_checkpoint(&mut self) -> Result<CheckpointRun, Fault> {
        self.step(Step::Incremental(self.position())).await?;
        let sealed = matches!(self.good, Reached::Through(_));
        let run = self.ck.checkpoint(CheckpointMode::Passive);
        self.release_read_lock();
        if sealed && run != Err(SourceError::ReadTxLost) {
            // A call that failed otherwise still retook the read transaction (or never
            // released it): the hold the WAL names now is the one to pin.
            let epoch = run.map_or_else(|_err| self.wal.epoch(), |run| run.epoch);
            self.take_read_lock(epoch);
        }
        let run = run.map_err(Fault::from_source)?;
        self.run = Some(run);
        Ok(run)
    }
}

/// Runs one checkpoint from a position that reached the committed end.
///
/// The proof is consumed, and a new one comes back in [`Staged`] only when the
/// last step reached the committed end: it carries the position, so a stale
/// one cannot be passed. A PASSIVE seals, checkpoints under a write barrier,
/// and follows a restarted WAL; a TRUNCATE does the same and then truncates the WAL
/// from the writer connection, holding the read transaction. Every barrier taken is
/// released first on a fault, and a failed release never replaces that fault.
/// Each staged file's created-at time is read from `clock` after that step's
/// committed read (`Env::clock`).
///
/// # Errors
/// [`CheckpointFailed`], with what the completed steps staged and the last
/// position that is true.
#[expect(
    clippy::too_many_arguments,
    reason = "the plan fixes this signature; each argument is a distinct source of the protocol"
)]
#[expect(
    clippy::result_large_err,
    reason = "a failure carries what was staged, as a success does; one is returned per checkpoint"
)]
pub async fn checkpoint<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore>(
    proof: SyncedThrough,
    mode: CheckpointMode,
    wal: &mut W,
    db: &mut P,
    pages: &mut PageChecksumTable,
    ck: &mut C,
    staging: &L,
    cfg: &SyncConfig,
    clock: &dyn Clock,
) -> Result<Checkpointed, CheckpointFailed> {
    let staging = Staging(staging);
    let mut protocol = Protocol {
        wal,
        db,
        pages,
        ck,
        staging: &staging,
        cfg: *cfg,
        clock,
        good: Reached::Through(proof),
        files: Vec::new(),
        witnesses: Vec::new(),
        quiet: Vec::new(),
        run: None,
    };
    let result = match mode {
        CheckpointMode::Passive => protocol.checkpoint(false).await,
        CheckpointMode::Truncate => protocol.checkpoint(true).await,
        // The policy decides PASSIVE or TRUNCATE; only the cure after a failed WAL sync runs FULL,
        // outside the protocol.
        CheckpointMode::Full => Err(BugSite::FullCheckpointInProtocol.hit()),
    };
    let staged = Staged {
        reached: protocol.good,
        files: protocol.files,
        witnesses: protocol.witnesses,
        quiet: protocol.quiet,
    };
    match result {
        Ok(outcome) => Ok(Checkpointed {
            staged,
            outcome,
            run: protocol.run,
        }),
        Err(fault) => Err(CheckpointFailed {
            fault,
            staged,
            run: protocol.run,
        }),
    }
}

#[cfg(test)]
mod tests;
