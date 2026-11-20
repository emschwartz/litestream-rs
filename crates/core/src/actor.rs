//! The sequential driver: one actor runs the sync, checkpoint, upload and
//! maintenance loops of Go's `DB` and `Replica` in one priority order.
//!
//! Sans-IO: it owns no thread, no timer and no clock. Each [`Actor::tick`] is
//! told the time, does at most one unit of work through the [`Env`] it is
//! lent, and says when to be called again. Go runs these loops as goroutines
//! (`db.go:3322-3406`, `replica.go:392-516`, `store.go:591-656`); one order
//! is legal because each is idempotent against the replica's listing.
//!
//! The order, first ready wins:
//!
//! 1. Sync when the WAL's committed end moved, a snapshot step is pending, or
//!    the checkpoint decision wants a proof; then, in the same tick, the
//!    checkpoint the decision calls for, consuming that sync's proof (F1: no
//!    checkpoint without one, and a proof never outlives its tick).
//! 2. Upload while staged files are not on the replica.
//! 3. Maintenance, only once uploads are idle, so a snapshot's last
//!    transaction is at most what was published.
//!
//! Every failure becomes a [`Fault`] and goes through [`policy`] once, in
//! [`Actor::fault`]. A retry waits on its own lane; the others keep running.
//! Every store call is an await point, and a future dropped at any of them is
//! a crash that the next boot recovers from.

mod state;

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{Timestamp, Txid};
use lsr_wal::{OpenError, WalReader};

pub use state::Phase;
use state::{Gate, Lane, Lanes, Maintain};

use crate::checkpoint::{
    CheckpointConfig, CheckpointFailed, CheckpointOutcome, CheckpointState, Decision, Observed,
    after_checkpoint, after_sync, checkpoint, decide, next_wake,
};
use crate::compact::CompactError;
use crate::fault::{
    Backoff, Clamped, Fault, HaltCause, Next, Policy, SnapshotCause, policy, retry_wait,
};
use crate::inventory::Inventory;
use crate::layout::FileInfo;
use crate::maintain::{
    Due, Hold, Job, MaintainError, Maintenance, Outcome, due, run_compact, run_delete,
};
use crate::position::{Continuity, Position, SyncedThrough};
use crate::recover::{FrameCheck, Miss, Probes, Recovered, Tip, TipView, WalView, recover};
use crate::snapshot::{SnapshotFailed, write_snapshot};
use crate::source::{CheckpointMode, Checkpointer, CommittedRead, PageSource, WalSource};
use crate::store::ObjectStore;
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step};
use crate::upload::{
    Did, UploadConfig, UploadPlan, UploadState, inventory_fault, plan_upload, upload_next,
};

/// What one tick may touch, lent for that tick only.
#[derive(Debug)]
pub struct Env<'a, W, P, C, L, R> {
    /// The WAL.
    pub wal: &'a mut W,
    /// The database file.
    pub db: &'a mut P,
    /// The checkpointer.
    pub ck: &'a mut C,
    /// The staging directory.
    pub staging: &'a L,
    /// The replica.
    pub remote: &'a R,
}

/// How the actor runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorConfig {
    /// What a sync step may do.
    pub sync: SyncConfig,
    /// When to checkpoint.
    pub checkpoint: CheckpointConfig,
    /// Upload limits.
    pub upload: UploadConfig,
    /// The compaction and retention schedule.
    pub maintenance: Maintenance,
    /// The first retry wait; each further failure doubles it, up to
    /// [`crate::MAX_BACKOFF`] (Go's sync interval, `replica.go:440-457`).
    pub retry_interval: Duration,
    /// How long a drain may take before work left is a halt.
    pub shutdown_grace: Duration,
}

/// What a maintenance tick did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaintainedEvent {
    /// A compaction output is published.
    Compacted(FileInfo),
    /// A level-9 snapshot is published.
    Snapshot(FileInfo),
    /// A delete call named this many keys.
    Deleted(usize),
    /// An input vanished: the next tick lists and plans again.
    Replan,
    /// Nothing is due.
    Waiting {
        /// Why deletions are refused, if they are.
        held: Option<Hold>,
    },
}

/// What a tick did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The replica was listed and the local position recovered; `cause` is
    /// set when the first sync is a snapshot.
    Booted {
        /// Why the first sync is a snapshot.
        cause: Option<SnapshotCause>,
    },
    /// A sync step ran; `files` is how many it staged (0 or 1).
    Synced {
        /// Files staged.
        files: u32,
    },
    /// A sync, then a checkpoint, ran.
    Checkpointed {
        /// The mode.
        mode: CheckpointMode,
        /// SQLite could not finish.
        busy: bool,
    },
    /// Uploads ran; this is what the last one did.
    Uploaded(Did),
    /// A maintenance step ran.
    Maintained(MaintainedEvent),
    /// Nothing was ready.
    Idle,
    /// A fault whose policy is to retry; its lane waits.
    Retrying {
        /// What went wrong.
        fault: Fault,
        /// When the retry is due, read from where the wait is kept: the
        /// level's backoff for a maintenance job, the lane's for the rest.
        until: Timestamp,
    },
    /// The next sync is a snapshot.
    Resnapshot(SnapshotCause),
    /// The clock read earlier than the last time used; nothing else ran.
    Clamped(Clamped),
}

/// How a tick ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tick {
    /// Work ran (or nothing was ready); call again at `next`.
    Ran {
        /// What happened.
        event: Event,
        /// When to tick again. An earlier tick is always safe.
        next: Next,
    },
    /// Stopped; every later tick returns this.
    Halted(HaltCause),
    /// A drain finished: everything committed is on the replica.
    Drained,
}

/// What a sync step left.
enum Stepped {
    /// The step ran; the proof is set when it reached the committed end at an
    /// unchanged read epoch.
    Synced {
        files: u32,
        proof: Option<SyncedThrough>,
    },
    /// Recovery says the next step is a snapshot.
    Resnapshot(SnapshotCause),
    /// Recovery says stop.
    Halt(HaltCause),
}

/// The replication engine for one database.
#[derive(Clone, Debug)]
pub struct Actor {
    cfg: ActorConfig,
    phase: Phase,
    booted: bool,
    /// The last synced position; `None` until the first sync after a boot
    /// that found nothing to continue.
    position: Option<Position>,
    /// The newest staged transaction.
    staged: Option<Txid>,
    /// Set when the next sync is a snapshot.
    pending: Option<SnapshotCause>,
    /// The last sync stopped at the cap: more is committed.
    more_wal: bool,
    checkpoint: CheckpointState,
    gate: Gate,
    upload: UploadState,
    lanes: Lanes,
    maintain: Maintain,
    last: Option<Timestamp>,
    clamp_reported: bool,
}

fn later(at: Timestamp, wait: Duration) -> Timestamp {
    let millis = i64::try_from(wait.as_millis()).unwrap_or(i64::MAX);
    Timestamp::from_unix_millis(at.unix_millis().saturating_add(millis))
}

/// Where a retry's wait is kept.
#[derive(Clone, Copy, Debug)]
enum Waiter {
    /// The lane counts the failures and keeps the wait.
    Lane(Lane),
    /// Kept elsewhere (a maintenance level's backoff); this is when it ends.
    Kept(Timestamp),
}

impl From<Lane> for Waiter {
    fn from(lane: Lane) -> Self {
        Self::Lane(lane)
    }
}

const fn mode_of(decision: Decision) -> Option<CheckpointMode> {
    match decision {
        Decision::Nothing => None,
        Decision::Passive(_) => Some(CheckpointMode::Passive),
        Decision::Truncate => Some(CheckpointMode::Truncate),
    }
}

/// A WAL reader, or `None` when there is no file or its header is torn
/// (`recover` classifies both).
fn open_reader<W: WalSource>(wal: &mut W) -> Result<Option<WalReader<W::Wal>>, Fault> {
    let Some(file) = wal.open().map_err(Fault::from_source)? else {
        return Ok(None);
    };
    match WalReader::open(file) {
        Ok(reader) => Ok(Some(reader)),
        Err(OpenError::Io(_)) => Err(Fault::LocalIo),
        Err(OpenError::Truncated { .. } | OpenError::Header(_)) => Ok(None),
    }
}

/// What `recover` reads of the WAL besides the reader.
fn wal_view<W: WalSource, R: std::io::Read + std::io::Seek>(
    wal: &mut W,
    reader: Option<&WalReader<R>>,
    committed: CommittedRead,
) -> Result<WalView, Fault> {
    let epoch = wal.epoch();
    Ok(WalView {
        header: reader.map(|reader| *reader.header()),
        len: wal.len().map_err(Fault::from_source)?,
        committed: Some(committed),
        epoch,
    })
}

fn compact_fault(err: &CompactError) -> Fault {
    match err {
        CompactError::InputVanished { .. } => Fault::RemoteObjectMissing,
        CompactError::Source(err) | CompactError::Dest(err) => Fault::from_remote(err),
        CompactError::Input { .. } | CompactError::Encode(_) => Fault::RemoteCorrupt,
        CompactError::Inputs(_) | CompactError::SnapshotLevel { .. } => Fault::Bug,
    }
}

fn maintain_fault(err: &MaintainError) -> Fault {
    match err {
        MaintainError::Compact(err) | MaintainError::OutputDistrusted { source: err, .. } => {
            compact_fault(err)
        }
        MaintainError::Delete(err) => Fault::from_remote(err),
    }
}

impl Actor {
    /// An actor that boots on its first tick.
    #[must_use]
    pub fn new(cfg: ActorConfig) -> Self {
        Self {
            cfg,
            phase: Phase::Boot,
            booted: false,
            position: None,
            staged: None,
            pending: None,
            more_wal: false,
            // Replaced at boot, when the time is known.
            checkpoint: CheckpointState::boot(Timestamp::from_unix_millis(0)),
            gate: Gate::Open,
            upload: UploadState::Unknown,
            lanes: Lanes::default(),
            maintain: Maintain::default(),
            last: None,
            clamp_reported: false,
        }
    }

    /// What the actor is doing.
    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    /// Makes the next sync a snapshot (an operator asked). A cause already
    /// pending is kept.
    pub fn request_snapshot(&mut self) {
        self.pending.get_or_insert(SnapshotCause::OperatorRequested);
    }

    /// Starts a drain that must finish by `now` plus the shutdown grace.
    pub fn begin_shutdown(&mut self, now: Timestamp) {
        self.phase = self.phase.drain(later(now, self.cfg.shutdown_grace));
    }

    /// Runs at most one unit of work.
    pub async fn tick<W, P, C, L, R>(
        &mut self,
        mut env: Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    {
        if let Phase::Halted(cause) = self.phase {
            return Tick::Halted(cause);
        }
        let now = match self.clock(now) {
            Ok(now) => now,
            Err(clamped) => {
                return Tick::Ran {
                    event: Event::Clamped(clamped),
                    next: Next::Now,
                };
            }
        };
        self.expire(now);
        if !self.booted {
            return self.boot(&mut env, now).await;
        }
        match self.phase {
            Phase::Boot | Phase::Running => self.run(&mut env, now).await,
            Phase::Draining { deadline } => self.drain(&mut env, now, deadline).await,
            Phase::Halted(cause) => Tick::Halted(cause),
        }
    }

    /// The time to use: never earlier than the last. The first earlier read
    /// of a run of them is reported and ends the tick.
    fn clock(&mut self, now: Timestamp) -> Result<Timestamp, Clamped> {
        match self.last {
            Some(prev) if now < prev => {
                if self.clamp_reported {
                    Ok(prev)
                } else {
                    self.clamp_reported = true;
                    Err(Clamped { prev, now })
                }
            }
            Some(_) | None => {
                self.clamp_reported = false;
                self.last = Some(now);
                Ok(now)
            }
        }
    }

    /// Routes a fault by its policy. The only place `policy` is called.
    fn fault(&mut self, waiter: impl Into<Waiter>, fault: Fault, now: Timestamp) -> Tick {
        match policy(&fault) {
            Policy::Retry(backoff) => {
                let until = match waiter.into() {
                    Waiter::Kept(until) => until,
                    Waiter::Lane(lane) => {
                        let interval = self.cfg.retry_interval;
                        self.lanes.get_mut(lane).fail(|attempt| {
                            let wait = match backoff {
                                Backoff::Doubling => retry_wait(attempt, interval),
                                Backoff::After(wait) => wait,
                            };
                            later(now, wait)
                        })
                    }
                };
                self.ran(Event::Retrying { fault, until }, now)
            }
            Policy::Resnapshot(cause) => {
                self.pending = Some(cause);
                self.ran(Event::Resnapshot(cause), now)
            }
            Policy::Halt(cause) => self.halt(cause),
        }
    }

    /// Lets every passed deadline go, so the tick runs its lane and `next`
    /// never names a time already past (W1).
    fn expire(&mut self, now: Timestamp) {
        self.lanes.expire(now);
        self.gate = self.gate.expire(now);
        self.maintain.expire(now);
    }

    const fn halt(&mut self, cause: HaltCause) -> Tick {
        self.phase = Phase::Halted(cause);
        Tick::Halted(cause)
    }

    fn ran(&self, event: Event, now: Timestamp) -> Tick {
        Tick::Ran {
            event,
            next: self.next(now),
        }
    }

    /// Lists the replica, reads the newest staged file and recovers (4d's
    /// order). A failure anywhere boots again from the start.
    async fn boot<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        if !self.lanes.ready(Lane::Sync, now) {
            return self.ran(Event::Idle, now);
        }
        // At boot a replica ahead of the local tip is `recover`'s to judge (a
        // snapshot after it), not the uploader's (a halt), so the listing names no tip.
        let start = UploadState::ListFirst { confirmed: None };
        let upload = match upload_next(start, env.staging, env.remote, Some(Txid::MAX)).await {
            Ok(listed) => listed.state,
            Err(failed) => return self.fault(Lane::Sync, failed.fault, now),
        };
        let (tip, recovered) = match boot_recover(env.wal, env.staging, upload.confirmed()).await {
            Ok(found) => found,
            Err(fault) => return self.fault(Lane::Sync, fault, now),
        };
        let cause = match recovered {
            Recovered::Continue(position) | Recovered::WalRestarted(position) => {
                self.position = Some(position);
                None
            }
            Recovered::Snapshot { cause, .. } => Some(cause),
            Recovered::Halt(cause) => return self.halt(cause),
        };
        if let Some(cause) = cause {
            self.pending.get_or_insert(cause);
        }
        self.upload = upload;
        self.staged = tip.map(|tip| tip.txid());
        self.checkpoint = CheckpointState::boot(now);
        self.lanes.get_mut(Lane::Sync).succeed();
        self.booted = true;
        self.phase = self.phase.booted();
        self.ran(Event::Booted { cause }, now)
    }

    async fn run<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        if let Some(tick) = self.wal_unit(env, now, true).await {
            return tick;
        }
        if let Some(tick) = self.upload_unit(env, now).await {
            return tick;
        }
        if let Some(tick) = self.maintain_unit(env, now).await {
            return tick;
        }
        self.ran(Event::Idle, now)
    }

    /// Only sync and upload run; maintenance and checkpoints wait for the next start.
    async fn drain<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
        deadline: Timestamp,
    ) -> Tick {
        if now >= deadline {
            return if self.settled(env.wal) {
                Tick::Drained
            } else {
                self.fault(Lane::Sync, Fault::ShutdownDeadline, now)
            };
        }
        if let Some(tick) = self.wal_unit(env, now, false).await {
            return tick;
        }
        if let Some(tick) = self.upload_unit(env, now).await {
            return tick;
        }
        if self.settled(env.wal) {
            Tick::Drained
        } else {
            self.ran(Event::Idle, now)
        }
    }

    /// Everything committed is staged and everything staged is on the replica.
    fn settled<W: WalSource>(&self, wal: &mut W) -> bool {
        let synced = wal
            .committed()
            .is_ok_and(|committed| !self.wal_moved(committed));
        synced && self.pending.is_none() && !self.more_wal && self.uploads_idle()
    }

    fn wal_moved(&self, committed: CommittedRead) -> bool {
        let end = committed.end();
        self.position.is_none_or(|position| {
            end.salts() != position.cursor().salts() || end.offset() != position.synced().offset()
        })
    }

    fn uploads_idle(&self) -> bool {
        plan_upload(&self.upload, self.staged) == UploadPlan::Idle
    }

    /// The newest transaction anything has: a snapshot follows it.
    fn snapshot_after(&self) -> Option<Txid> {
        self.position
            .map(|position| position.txid())
            .max(self.staged)
            .max(self.upload.confirmed())
    }

    /// The checkpoint the policy wants now, when its lane and the sync lane
    /// that must prove it first may both run.
    fn checkpoint_decision(&self, now: Timestamp) -> Option<(Decision, CheckpointMode)> {
        let position = self.position?;
        let free = self.gate.open(now)
            && self.lanes.ready(Lane::Checkpoint, now)
            && self.lanes.ready(Lane::Sync, now);
        if !free {
            return None;
        }
        let observed = Observed::of(now, &position, !self.more_wal);
        let decision = decide(&self.checkpoint, &observed, &self.cfg.checkpoint);
        mode_of(decision).map(|mode| (decision, mode))
    }

    /// Sync, then the checkpoint that sync's proof allows.
    async fn wal_unit<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
        may_checkpoint: bool,
    ) -> Option<Tick> {
        if !self.lanes.ready(Lane::Sync, now) {
            return None;
        }
        let committed = match env.wal.committed() {
            Ok(committed) => committed,
            Err(err) => return Some(self.fault(Lane::Sync, Fault::from_source(err), now)),
        };
        let wants_proof = may_checkpoint && self.checkpoint_decision(now).is_some();
        let quiet = !self.wal_moved(committed) && self.pending.is_none() && !self.more_wal;
        if quiet && !wants_proof {
            return None;
        }
        let stepped = self.step(env, now).await;
        let (files, proof) = match self.stepped(stepped, now) {
            Ok(synced) => synced,
            Err(tick) => return Some(tick),
        };
        let decision = self.checkpoint_decision(now).filter(|_| may_checkpoint);
        if let (Some(proof), Some((decision, mode))) = (proof, decision) {
            return Some(self.checkpoint_unit(env, proof, decision, mode, now).await);
        }
        (files > 0).then(|| self.ran(Event::Synced { files }, now))
    }

    /// A step's result as what was staged, or as the tick a fault or a
    /// recovery verdict ends.
    fn stepped(
        &mut self,
        stepped: Result<Stepped, Fault>,
        now: Timestamp,
    ) -> Result<(u32, Option<SyncedThrough>), Tick> {
        match stepped {
            Ok(Stepped::Synced { files, proof }) => {
                self.lanes.get_mut(Lane::Sync).succeed();
                Ok((files, proof))
            }
            Ok(Stepped::Resnapshot(cause)) => {
                self.pending = Some(cause);
                Err(self.ran(Event::Resnapshot(cause), now))
            }
            Ok(Stepped::Halt(cause)) => Err(self.halt(cause)),
            Err(fault) => Err(self.fault(Lane::Sync, fault, now)),
        }
    }

    /// One sync step from what recovery says about the WAL now.
    async fn step<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault> {
        let epoch = env.wal.epoch();
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        let Some(mut reader) = open_reader(env.wal)? else {
            return self.without_wal(env.wal, committed);
        };
        let step = match self.plan_step(env.wal, &mut reader, committed)? {
            Ok(step) => step,
            Err(verdict) => return Ok(verdict),
        };
        let synced = sync_step(
            step,
            &mut reader,
            committed,
            epoch,
            &mut *env.db,
            env.staging,
            &self.cfg.sync,
            now,
        )
        .await
        .map_err(|failed| failed.fault)?;
        Ok(self.absorb(synced, env.wal.epoch() != epoch, now))
    }

    fn plan_step<W: WalSource>(
        &self,
        wal: &mut W,
        reader: &mut WalReader<W::Wal>,
        committed: CommittedRead,
    ) -> Result<Result<Step, Stepped>, Fault> {
        match (self.pending, self.position) {
            (Some(cause), position) => Ok(Ok(Step::Snapshot {
                after: self.snapshot_after(),
                prev_timestamp: position.map(|position| position.timestamp()),
                cause,
            })),
            (None, Some(position)) => {
                let probes = Probes::read(reader, &position)?;
                let view = wal_view(wal, Some(&*reader), committed)?;
                let tip = Tip::Running(&position, probes);
                Ok(step_of(recover(Some(tip), &view, self.upload.confirmed())))
            }
            (None, None) => Err(Fault::Bug),
        }
    }

    /// No readable WAL. A pending snapshot waits for SQLite to write one;
    /// otherwise recovery classifies the loss.
    fn without_wal<W: WalSource>(
        &self,
        wal: &mut W,
        committed: CommittedRead,
    ) -> Result<Stepped, Fault> {
        let (None, Some(position)) = (self.pending, self.position) else {
            return Err(Fault::WalMoved);
        };
        let view = wal_view::<W, W::Wal>(wal, None, committed)?;
        // With no header, recovery decides before it looks at the probes.
        let probes = Probes {
            prev: FrameCheck::Miss(Miss::Truncated),
            stale_next: false,
        };
        let tip = Tip::Running(&position, probes);
        Ok(
            match step_of(recover(Some(tip), &view, self.upload.confirmed())) {
                Ok(_) => Stepped::Resnapshot(SnapshotCause::WalMissing),
                Err(verdict) => verdict,
            },
        )
    }

    /// Takes in a step's result. `epoch_moved`: the read epoch changed
    /// between the step's start and its staging commit, so (F1) the position
    /// is not pinned and there is no proof.
    fn absorb(&mut self, synced: Synced, epoch_moved: bool, now: Timestamp) -> Stepped {
        let (files, txs, end) = match synced {
            Synced::Idle(through) => (0, 0, StepEnd::Through(through)),
            Synced::Wrote { file, end, txs, .. } => {
                self.staged = self.staged.max(Some(file.key.range().max()));
                self.pending = None;
                if self.gate == Gate::NextWrite {
                    self.gate = Gate::Open;
                }
                (1, txs, end)
            }
        };
        self.more_wal = matches!(end, StepEnd::Limited(_));
        let (position, proof) = match end {
            StepEnd::Through(through) if !epoch_moved => (*through.position(), Some(through)),
            StepEnd::Through(through) => (through.into_position(), None),
            StepEnd::Limited(position) => (position, None),
        };
        let position = if epoch_moved {
            position.with_continuity(Continuity::Unpinned)
        } else {
            position
        };
        self.position = Some(position);
        let observed = Observed::of(now, &position, !self.more_wal);
        self.checkpoint = after_sync(self.checkpoint, txs, &observed, &self.cfg.checkpoint);
        Stepped::Synced { files, proof }
    }

    /// The checkpoint `decision` asked for, on the proof this tick's sync left.
    async fn checkpoint_unit<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        proof: SyncedThrough,
        decision: Decision,
        mode: CheckpointMode,
        now: Timestamp,
    ) -> Tick {
        let done = checkpoint(
            proof,
            mode,
            &mut *env.wal,
            &mut *env.db,
            &mut *env.ck,
            env.staging,
            &self.cfg.sync,
            now,
        )
        .await;
        let done = match done {
            Ok(done) => done,
            Err(CheckpointFailed { fault, position }) => {
                self.position = Some(position);
                return self.fault(Lane::Checkpoint, fault, now);
            }
        };
        let position = *done.through.position();
        self.position = Some(position);
        for file in &done.files {
            self.staged = self.staged.max(Some(file.key.range().max()));
        }
        let observed = Observed::of(now, &position, true);
        let (state, next) = after_checkpoint(
            self.checkpoint,
            decision,
            &done.outcome,
            &observed,
            &self.cfg.checkpoint,
        );
        self.checkpoint = state;
        self.gate = Gate::of(next);
        self.lanes.get_mut(Lane::Checkpoint).succeed();
        let busy = done.outcome == CheckpointOutcome::Busy;
        self.ran(Event::Checkpointed { mode, busy }, now)
    }

    /// Staged files to the replica, up to the per-tick limit.
    async fn upload_unit<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick> {
        if !self.lanes.ready(Lane::Upload, now) || self.uploads_idle() {
            return None;
        }
        let limit = self
            .cfg
            .upload
            .files_per_tick
            .map_or(u32::MAX, NonZeroU32::get);
        let mut last = Did::Nothing;
        for _ in 0..limit {
            match upload_next(self.upload, env.staging, env.remote, self.staged).await {
                Ok(uploaded) => {
                    self.upload = uploaded.state;
                    last = uploaded.did;
                }
                Err(failed) => {
                    self.upload = failed.state;
                    return Some(self.fault(Lane::Upload, failed.fault, now));
                }
            }
            match last {
                Did::Nothing => break,
                Did::Listed { .. } | Did::Put(_) => self.maintain.stale = true,
            }
            if self.uploads_idle() {
                break;
            }
        }
        self.lanes.get_mut(Lane::Upload).succeed();
        Some(self.ran(Event::Uploaded(last), now))
    }

    /// Lists the replica and runs what maintenance says is due (S4: only once
    /// every staged file is published).
    async fn maintain_unit<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick> {
        let ready = self.running()
            && self.lanes.ready(Lane::Maintain, now)
            && self.uploads_idle()
            && self.maintain.wants_listing(now);
        if !ready {
            return None;
        }
        let inventory = match Inventory::load(env.remote).await {
            Ok(inventory) => inventory,
            Err(err) => return Some(self.fault(Lane::Maintain, inventory_fault(&err), now)),
        };
        self.maintain.stale = false;
        let published = self.upload.confirmed();
        let cfg = &self.cfg.maintenance;
        match due(&inventory, now, cfg, published, &mut self.maintain.state) {
            Due::Wait { until, held } => {
                self.lanes.get_mut(Lane::Maintain).succeed();
                self.maintain.until = until;
                let event = Event::Maintained(MaintainedEvent::Waiting { held });
                Some(self.ran(event, now))
            }
            Due::Run(job) => {
                // Whatever the job does, the next listing is what to plan from.
                self.maintain.stale = true;
                Some(self.run_job(env, &job, now).await)
            }
        }
    }

    async fn run_job<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        job: &Job,
        now: Timestamp,
    ) -> Tick {
        let outcome = match job {
            Job::Compact(compact) => run_compact(compact, env.staging, env.remote).await,
            Job::Delete(delete) => run_delete(delete, env.remote).await,
            Job::Snapshot(_) => return self.snapshot_job(env, job, now).await,
        };
        self.maintain.state.record(now, job, &outcome);
        let event = match outcome {
            Outcome::Compacted { info, .. } => MaintainedEvent::Compacted(info),
            Outcome::Deleted { count } => MaintainedEvent::Deleted(count),
            Outcome::Replan { .. } => MaintainedEvent::Replan,
            Outcome::Failed(err) => {
                let waiter = self.job_waiter(job, now);
                return self.fault(waiter, maintain_fault(&err), now);
            }
        };
        self.lanes.get_mut(Lane::Maintain).succeed();
        self.ran(Event::Maintained(event), now)
    }

    /// Where a failed job waits: its level's backoff when `MaintainState`
    /// keeps one (R3: the other levels keep running), else the maintenance
    /// lane, so a failing retention delete backs off instead of relisting
    /// at once. A level's wait is not the lane's failure, so the lane resets.
    fn job_waiter(&mut self, job: &Job, now: Timestamp) -> Waiter {
        match self.maintain.state.retry_at(job, now) {
            Some(until) => {
                self.lanes.get_mut(Lane::Maintain).succeed();
                Waiter::Kept(until)
            }
            None => Waiter::Lane(Lane::Maintain),
        }
    }

    /// A level-9 snapshot of a fresh proof. A sync that staged a file first
    /// ends the tick: the upload runs before the snapshot is due again.
    async fn snapshot_job<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        job: &Job,
        now: Timestamp,
    ) -> Tick {
        let stepped = self.step(env, now).await;
        let proof = match self.stepped(stepped, now) {
            Ok((0, Some(proof))) => proof,
            Ok((files, _)) => return self.ran(Event::Synced { files }, now),
            Err(tick) => return tick,
        };
        let written = write_snapshot(&proof, &mut *env.wal, &mut *env.db, env.remote, now).await;
        self.maintain.state.record_snapshot(now, written.is_ok());
        match written {
            Ok(info) => {
                self.lanes.get_mut(Lane::Maintain).succeed();
                self.ran(Event::Maintained(MaintainedEvent::Snapshot(info)), now)
            }
            Err(SnapshotFailed { fault, .. }) => {
                let waiter = self.job_waiter(job, now);
                self.fault(waiter, fault, now)
            }
        }
    }

    const fn running(&self) -> bool {
        match self.phase {
            Phase::Running => true,
            Phase::Boot | Phase::Draining { .. } | Phase::Halted(_) => false,
        }
    }

    /// Whether some lane could run now on work it already knows of. A WAL
    /// change it does not know of is the caller's to wake on.
    fn ready_work(&self, now: Timestamp) -> bool {
        let running = self.running();
        let sync = self.lanes.ready(Lane::Sync, now)
            && (!self.booted
                || self.pending.is_some()
                || self.more_wal
                || (running && self.checkpoint_decision(now).is_some()));
        let upload = self.lanes.ready(Lane::Upload, now) && !self.uploads_idle();
        let maintain = running
            && self.lanes.ready(Lane::Maintain, now)
            && self.uploads_idle()
            && self.maintain.wants_listing(now);
        sync || upload || maintain
    }

    /// Every deadline a lane holds: retry waits, the checkpoint gate, the
    /// maintenance schedule (whose `Due::Wait` includes each level's backoff)
    /// and the drain's end. Lanes a draining actor does not run hold none.
    fn deadlines(&self) -> impl Iterator<Item = Timestamp> {
        let running = self.running();
        let lane = |lane: Lane| self.lanes.get(lane).until();
        let deadline = match self.phase {
            Phase::Draining { deadline } => Some(deadline),
            Phase::Boot | Phase::Running | Phase::Halted(_) => None,
        };
        let only_running = [
            lane(Lane::Checkpoint),
            lane(Lane::Maintain),
            self.gate.until(),
            self.maintain.until,
        ];
        [lane(Lane::Sync), lane(Lane::Upload), deadline]
            .into_iter()
            .chain(only_running.into_iter().filter(move |_| running))
            .flatten()
    }

    /// When to tick next: never later than the earliest held deadline, and
    /// at once when one has passed (W1). The checkpoint schedule is not a
    /// held deadline: `decide` reads it, so only its future wake counts.
    fn next(&self, now: Timestamp) -> Next {
        if self.ready_work(now) {
            return Next::Now;
        }
        let schedule = next_wake(&self.checkpoint, &self.cfg.checkpoint)
            .filter(|at| self.running() && *at > now);
        match self.deadlines().chain(schedule).min() {
            Some(at) if at <= now => Next::Now,
            Some(at) => Next::At(at),
            None => Next::OnWalChange,
        }
    }
}

/// The newest staged file and what recovery makes of it.
async fn boot_recover<W: WalSource, L: ObjectStore>(
    wal: &mut W,
    staging: &L,
    remote: Option<Txid>,
) -> Result<(Option<TipView>, Recovered), Fault> {
    let committed = wal.committed().map_err(Fault::from_source)?;
    let mut reader = open_reader(wal)?;
    let tip = TipView::load(staging, reader.as_mut()).await?;
    let view = wal_view(wal, reader.as_ref(), committed)?;
    let recovered = recover(tip.as_ref().map(Tip::Boot), &view, remote);
    Ok((tip, recovered))
}

/// A recovery verdict as the step to run, or as what ends the tick.
const fn step_of(recovered: Recovered) -> Result<Step, Stepped> {
    match recovered {
        Recovered::Continue(position) => Ok(Step::Incremental(position)),
        Recovered::WalRestarted(position) => Ok(Step::NewGeneration(position)),
        Recovered::Snapshot { cause, .. } => Err(Stepped::Resnapshot(cause)),
        Recovered::Halt(cause) => Err(Stepped::Halt(cause)),
    }
}

#[cfg(test)]
mod tests;
