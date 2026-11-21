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
//! 3. Prune staged level-0 files the replica's level 1 covers, keeping the
//!    tip.
//! 4. Maintenance, only once uploads are idle, so a snapshot's last
//!    transaction is at most what was published. Its first listing after a
//!    boot is read back first: the newest file of each level must verify.
//!
//! Every failure becomes a [`Fault`] and goes through [`policy`] once, in
//! [`Actor::fault`]. A retry waits on its own lane; the others keep running.
//! Every store call is an await point, and a future dropped at any of them is
//! a crash that the next boot recovers from.
//!
//! A persistent fault, and a recovery verdict that names a stop cause, goes
//! to the configured [`Responses`] (`crate::fault` has the rules). Under
//! `KeepTrying` the lane that failed is degraded: each retry that fails ends
//! its tick with [`Event::Degraded`], the wait doubles up to
//! [`crate::MAX_BACKOFF`], the other lanes run, and the lane's next success
//! clears it. A failed retention delete degrades maintenance; sync and upload
//! go on. While any lane is degraded, retention pins the newest transaction
//! the replica held when the first one began (`maintain::retain`), and the
//! pin goes once every lane has recovered. A bug, and a second writer whose
//! response is to continue, rebuild the actor and boot again after the wait;
//! a replica that went backwards is listed again. Under `Stop` the actor
//! halts, as every persistent fault did before.
//!
//! A drain ends in one of two ticks: [`Tick::Drained`] with the newest
//! transaction the replica holds, or, at its deadline, [`Tick::DeadlineReached`]
//! with what was left ([`Unfinished`]); every tick after that one is
//! `Halted(Shutdown)`. The wal-index gives a frame offset, not a transaction
//! id, so what is left is counted in WAL bytes and staged files.
//!
//! The actor also hands every wal-index read, and the read after each of its
//! own checkpoints, to a [`HostObserver`] (`crate::host`), which counts the
//! checkpoints it did not run and infers the host's automatic checkpoints that
//! cannot run ([`Actor::host_checkpoints`]). The count changes no decision.

mod state;

use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{PageSize, Timestamp, Txid};
use lsr_wal::{CommittedEnd, OpenError, WalReader};

pub use state::{Degraded, Lane, Phase};
use state::{Gate, Lanes, Maintain};

use crate::checkpoint::{
    CheckpointConfig, CheckpointFailed, CheckpointOutcome, CheckpointState, Decision, Observed,
    Staged, after_checkpoint, after_sync, checkpoint, decide, next_wake,
};
use crate::compact::CompactError;
use crate::fault::{
    Action, Backoff, Clamped, Fault, HaltCause, Next, PersistentCause, Policy, Responses,
    SnapshotCause, backoff_wait, policy,
};
use crate::host::{HostCheckpoints, HostObserver};
use crate::inventory::Inventory;
use crate::layout::{FileInfo, FileKey, Level};
use crate::maintain::{
    Due, Hold, Job, MAX_DELETE, MaintainError, Maintenance, Outcome, ReadBack, due, read_back,
    run_compact, run_delete,
};
use crate::pages::{PageTable, seeded};
use crate::position::{Continuity, Position, ReadEpoch, SyncedThrough, WalFileLen};
use crate::recover::{
    FrameCheck, Miss, Probes, Recovered, Tip, TipView, WalView, read_header, recover,
};
use crate::snapshot::{SnapshotFailed, read_live, write_snapshot};
use crate::source::{CheckpointMode, Checkpointer, CommittedRead, PageSource, WalSource};
use crate::stamps::{ResolveError, Stamps, resolve};
use crate::store::{ObjectStore, PageLimit};
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step};
use crate::upload::{
    Did, UploadConfig, UploadPlan, UploadState, drop_staged_from, inventory_fault, plan_upload,
    sweep_staged, upload_next,
};
use crate::verify::LivePages;

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
    /// How long a drain may take when the caller has no deadline of its own
    /// ([`Actor::shutdown_deadline`]); [`Actor::begin_shutdown`] takes the
    /// deadline itself.
    pub shutdown_grace: Duration,
    /// What each persistent failure does: keep trying, or stop.
    pub responses: Responses,
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
    /// The read-back after a boot found this file does not verify and deleted
    /// it: the replica's other files still restore its last transaction.
    Repaired(FileKey),
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
    Maintained {
        /// What it did.
        event: MaintainedEvent,
        /// How many stamps the decision read from the replica (zero when the
        /// listing carried them, or when the step did not consult the planner).
        stamps_read: u64,
    },
    /// Nothing was ready.
    Idle,
    /// A persistent fault whose configured response is to keep trying: the
    /// lane is degraded and waits, the others run.
    Degraded {
        /// The lane that failed.
        lane: Lane,
        /// Why.
        cause: PersistentCause,
        /// When the lane first failed this way without a success between.
        since: Timestamp,
        /// When the retry is due.
        until: Timestamp,
    },
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
    Drained {
        /// The newest transaction the replica holds; `None` when it holds
        /// none (nothing was ever committed).
        txid: Option<Txid>,
    },
    /// A drain's deadline passed with work left. The actor is halted with
    /// [`HaltCause::Shutdown`]; every later tick returns that.
    DeadlineReached(Unfinished),
}

/// What a drain left when its deadline passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unfinished {
    /// Committed WAL bytes past the synced position: the whole committed WAL
    /// when the position is in another generation or unknown.
    pub unsynced_wal_bytes: u64,
    /// Staged level-0 files the replica was not confirmed to hold.
    pub staged_not_uploaded: u32,
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
    /// Recovery names a stop cause; the configured response decides.
    Halt(HaltCause),
}

/// Where the next sync step resumes, which also names the newest staged
/// transaction.
///
/// One value, not a position beside a staged transaction id: every staged
/// level-0 file comes from a step whose position names it, so a position's
/// transaction is the newest staged one, and the two cannot disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resume {
    /// The last synced position; its transaction is the newest staged.
    From(Position),
    /// No position: a boot that found nothing to continue, or a staged file
    /// lost. The next sync is a snapshot; this is the newest transaction
    /// still staged (or, with nothing staged, the replica's newest level-0
    /// file), if any.
    Snapshot(Option<Txid>),
}

impl Resume {
    const fn position(self) -> Option<Position> {
        match self {
            Self::From(position) => Some(position),
            Self::Snapshot(_) => None,
        }
    }

    const fn staged(self) -> Option<Txid> {
        match self {
            Self::From(position) => Some(position.txid()),
            Self::Snapshot(staged) => staged,
        }
    }
}

/// The replication engine for one database.
#[derive(Clone, Debug)]
pub struct Actor {
    cfg: ActorConfig,
    phase: Phase,
    booted: bool,
    /// Where the next sync resumes, and so the newest staged transaction.
    resume: Resume,
    /// Set when the next sync is a snapshot.
    pending: Option<SnapshotCause>,
    /// The replica's page size, one per prefix; `None` while the replica and
    /// staging are both empty.
    page_size: Option<PageSize>,
    /// The last sync stopped at the cap: more is committed.
    more_wal: bool,
    /// What each database-file page should hold (`crate::pages`).
    pages: PageTable,
    /// The wal-index read at which the last step found the index reset ahead
    /// of the WAL header with nothing to stage: nothing to sync, and no proof
    /// to checkpoint on, until the index changes.
    reset: Option<CommittedEnd>,
    checkpoint: CheckpointState,
    gate: Gate,
    upload: UploadState,
    lanes: Lanes,
    maintain: Maintain,
    /// Staged level-0 files through this transaction were pruned (W3).
    pruned: Option<Txid>,
    last: Option<Timestamp>,
    clamp_reported: bool,
    /// A lane is degraded and retention holds its pin (which is `None` when
    /// the replica held nothing as the degradation began).
    pinned: bool,
    /// The newest transaction the replica held when the first lane degraded.
    lane_pin: Option<Txid>,
    /// The restore point a scheduled verify asked retention to keep.
    verify_pin: Option<Txid>,
    /// A scheduled verify waits for the live database's pages.
    live_wanted: bool,
    /// The live database's pages, read for a scheduled verify and not yet taken.
    live: Option<LivePages>,
    /// What the host's checkpoints did between reads.
    host: HostObserver,
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

/// Whether a failed snapshot may have left an object under its key: the store
/// answered the commit with a size or a checksum that is not the one sent.
const fn leaves_bad_snapshot(fault: Fault) -> bool {
    matches!(
        fault,
        Fault::UploadAmbiguous | Fault::UploadChecksumMismatch
    )
}

fn stamp_fault(err: &ResolveError) -> Fault {
    match err {
        ResolveError::Store(err) => Fault::from_remote(err),
        ResolveError::Repeated { .. } => Fault::Bug,
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
            resume: Resume::Snapshot(None),
            pending: None,
            page_size: None,
            more_wal: false,
            pages: PageTable::new(),
            reset: None,
            // Replaced at boot, when the time is known.
            checkpoint: CheckpointState::boot(Timestamp::from_unix_millis(0)),
            gate: Gate::Open,
            upload: UploadState::Unknown,
            lanes: Lanes::default(),
            maintain: Maintain::default(),
            pruned: None,
            last: None,
            clamp_reported: false,
            pinned: false,
            lane_pin: None,
            verify_pin: None,
            live_wanted: false,
            live: None,
            host: HostObserver::default(),
        }
    }

    /// What the actor is doing.
    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.phase
    }

    /// The lane's degraded state, if a persistent failure left it in one.
    #[must_use]
    pub fn degraded(&self, lane: Lane) -> Option<Degraded> {
        self.lanes.get(lane).degraded()
    }

    /// An operator accepts the database file as it is: every page the page
    /// checksum table reported as mismatched is trusted with the bytes it now
    /// holds, and the degraded state and retention pin it held go.
    pub fn accept_database_pages(&mut self) {
        self.pages.accept();
    }

    /// The newest transaction staged locally.
    #[must_use]
    pub const fn staged(&self) -> Option<Txid> {
        self.resume.staged()
    }

    /// The last synced position, `None` until the first sync after a boot
    /// that found nothing to continue.
    const fn position(&self) -> Option<Position> {
        self.resume.position()
    }

    /// The newest transaction the replica was confirmed to hold.
    #[must_use]
    pub const fn replicated(&self) -> Option<Txid> {
        self.upload.confirmed()
    }

    /// Why the next sync is a snapshot, when it is one.
    #[must_use]
    pub const fn pending_snapshot(&self) -> Option<SnapshotCause> {
        self.pending
    }

    /// Checkpoints someone else ran on the database since this actor began.
    #[must_use]
    pub const fn foreign_checkpoints(&self) -> u64 {
        self.host.counts().ran()
    }

    /// What the host's checkpoints did since this actor began.
    #[must_use]
    pub const fn host_checkpoints(&self) -> HostCheckpoints {
        self.host.counts()
    }

    /// The host's `wal_autocheckpoint`, in frames (`None`: it runs none). The
    /// default is SQLite's, 1000; it only changes what the host-checkpoint
    /// notice infers.
    pub const fn set_host_autocheckpoint(&mut self, frames: Option<NonZeroU32>) {
        self.host.set_autocheckpoint(frames);
    }

    /// Committed WAL bytes past the synced position: all of `committed` when
    /// the position is unknown or in another generation.
    #[must_use]
    pub fn unsynced_bytes(&self, committed: CommittedRead) -> u64 {
        let end = committed.end();
        let total = end.offset().get();
        match self.position() {
            Some(position) if position.cursor().salts() == end.salts() => {
                total.saturating_sub(position.synced().offset().get())
            }
            Some(_) | None => total,
        }
    }

    /// Asks for the live database's pages, one checksum each, at the next
    /// transaction the replica is confirmed to hold, for a scheduled verify
    /// ([`crate::verify_against`]). A running engine reads them in a sync tick
    /// once uploads have caught up, under that tick's proof, as a level-9
    /// snapshot reads the database; [`Self::take_live_pages`] hands them over.
    pub const fn read_live_pages(&mut self) {
        self.live_wanted = true;
    }

    /// The live pages read since the last call, if any.
    pub const fn take_live_pages(&mut self) -> Option<LivePages> {
        self.live.take()
    }

    /// Keeps the restore chain of `txid` from retention, for a scheduled verify
    /// that found a later restore wrong (`None` lets it go). A degraded lane's
    /// pin, if older, still wins.
    pub fn pin_for_verify(&mut self, txid: Option<Txid>) {
        let released = self.verify_pin.is_some() && txid.is_none();
        self.verify_pin = txid;
        self.apply_pin();
        if released {
            self.maintain.stale = true;
        }
    }

    /// The restore point retention keeps, if one is pinned.
    #[must_use]
    pub const fn retention_pin(&self) -> Option<Txid> {
        self.maintain.state.pin()
    }

    /// Makes the next sync a snapshot (an operator asked). A cause already
    /// pending is kept.
    pub fn request_snapshot(&mut self) {
        self.pending.get_or_insert(SnapshotCause::OperatorRequested);
    }

    /// The deadline of a drain begun at `now` by a caller with none of its
    /// own: `now` plus the configured shutdown grace.
    #[must_use]
    pub fn shutdown_deadline(&self, now: Timestamp) -> Timestamp {
        later(now, self.cfg.shutdown_grace)
    }

    /// Starts a drain that must finish by `deadline`. A drain already begun
    /// keeps the earlier of the two deadlines, so a second call can shorten
    /// it and never lengthen it.
    pub fn begin_shutdown(&mut self, deadline: Timestamp) {
        self.phase = self.phase.drain(deadline);
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
        if self.pages.mismatch().is_none() {
            self.lanes.page_matches_again();
        }
        self.release_pin();
        // A boot that keeps failing must not outlast a drain's deadline.
        if let (false, Phase::Draining { deadline }) = (self.booted, self.phase)
            && now >= deadline
        {
            return self.missed(&mut env, now).await;
        }
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
                let until = self.wait(waiter.into(), backoff, now);
                self.ran(Event::Retrying { fault, until }, now)
            }
            Policy::Persistent(cause) => self.persistent(waiter.into(), cause, now),
            Policy::Shutdown => self.halt(HaltCause::Shutdown),
        }
    }

    /// Counts a failure where its wait is kept; returns when the wait ends.
    fn wait(&mut self, waiter: Waiter, backoff: Backoff, now: Timestamp) -> Timestamp {
        match waiter {
            Waiter::Kept(until) => until,
            Waiter::Lane(lane) => {
                let interval = self.cfg.retry_interval;
                self.lanes.get_mut(lane).fail(|attempt| {
                    let wait = backoff_wait(backoff, attempt, interval);
                    later(now, wait)
                })
            }
        }
    }

    /// A recovery verdict that names a stop cause: a persistent one goes to
    /// the configured response, the end of a drain stops.
    fn verdict(&mut self, lane: Lane, cause: HaltCause, now: Timestamp) -> Tick {
        match PersistentCause::of(cause) {
            Some(cause) => self.persistent(Waiter::Lane(lane), cause, now),
            None => self.halt(cause),
        }
    }

    /// What the configured response makes of a persistent cause: a stop, or
    /// a degraded lane that waits the doubling backoff. The only place
    /// `Responses` is read.
    fn persistent(&mut self, waiter: Waiter, cause: PersistentCause, now: Timestamp) -> Tick {
        let action = self.cfg.responses.action(cause);
        if action == Action::Stop {
            return self.halt(HaltCause::from(cause));
        }
        self.pin_retention();
        let waiter = match action {
            Action::Stop | Action::Retry => waiter,
            Action::Relist => {
                // What the replica was known to hold is gone from it: the
                // retry lists it and continues from what it holds now.
                self.upload = UploadState::ListFirst { confirmed: None };
                waiter
            }
            Action::Rebuild => {
                self.rebuild();
                Waiter::Lane(Lane::Sync)
            }
        };
        let lane = match waiter {
            Waiter::Lane(lane) => lane,
            Waiter::Kept(_) => Lane::Maintenance,
        };
        let until = self.wait(waiter, Backoff::Doubling, now);
        let since = self.lanes.get_mut(lane).degrade(cause, now);
        let event = Event::Degraded {
            lane,
            cause,
            since,
            until,
        };
        self.ran(event, now)
    }

    /// Pins the newest transaction the replica holds for retention, unless a
    /// degradation already pinned one.
    fn pin_retention(&mut self) {
        if !self.pinned {
            self.pinned = true;
            self.lane_pin = self.upload.confirmed();
            self.apply_pin();
        }
    }

    /// Lets the pin go once no lane is degraded; the next listing may then
    /// delete what it kept.
    fn release_pin(&mut self) {
        if self.pinned && !self.lanes.any_degraded() {
            self.pinned = false;
            self.lane_pin = None;
            self.apply_pin();
            self.maintain.stale = true;
        }
    }

    /// Retention keeps the older of the two restore points asked for: a
    /// degraded lane's and a scheduled verify's.
    fn apply_pin(&mut self) {
        let lane = self.lane_pin.filter(|_| self.pinned);
        let pin = match (lane, self.verify_pin) {
            (Some(lane), Some(verify)) => Some(lane.min(verify)),
            (lane, verify) => lane.or(verify),
        };
        self.maintain.state.set_pin(pin);
    }

    /// Replaces every piece of engine state with a fresh actor's, so the next
    /// tick boots from the replica and the WAL again. Kept: the phase, the
    /// clock's last reading, the lanes (their waits and degraded states are
    /// reports, not engine state) and the retention pin.
    fn rebuild(&mut self) {
        let mut fresh = Self::new(self.cfg.clone());
        fresh.phase = match self.phase {
            Phase::Running => Phase::Boot,
            phase @ (Phase::Boot | Phase::Draining { .. } | Phase::Halted(_)) => phase,
        };
        fresh.lanes = self.lanes;
        fresh.last = self.last;
        fresh.clamp_reported = self.clamp_reported;
        fresh.pinned = self.pinned;
        fresh.lane_pin = self.lane_pin;
        fresh.verify_pin = self.verify_pin;
        fresh.live_wanted = self.live_wanted;
        fresh.maintain.state.set_pin(self.maintain.state.pin());
        fresh.host = self.host;
        *self = fresh;
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
        let lost = match sweep_staged(env.staging, upload.confirmed()).await {
            Ok(lost) => lost,
            Err(fault) => return self.fault(Lane::Sync, fault, now),
        };
        let booted = boot_recover(env.wal, env.staging, env.remote, upload.confirmed()).await;
        let Booted {
            tip,
            recovered,
            page_size,
        } = match booted {
            Ok(found) => found,
            Err(fault) => return self.fault(Lane::Sync, fault, now),
        };
        self.page_size = page_size;
        let cause = match recovered {
            Recovered::Continue(position) | Recovered::WalRestarted(position) => {
                self.resume = Resume::From(position);
                None
            }
            Recovered::Snapshot { cause, .. } => {
                self.resume = Resume::Snapshot(tip.map(|tip| tip.txid()));
                Some(cause)
            }
            Recovered::Halt(cause) => return self.verdict(Lane::Sync, cause, now),
        };
        // A staged file that did not verify was deleted with all after it:
        // the next file is a snapshot even when the WAL could continue.
        let cause = lost.map(|_| SnapshotCause::LocalTipUnreadable).or(cause);
        if let Some(cause) = cause {
            self.pending.get_or_insert(cause);
        }
        // Only a boot that continues seeds the table (ruling P5): a snapshot
        // records every page it reads.
        if let (None, Some(position)) = (self.pending, self.position()) {
            match seeded(env.staging, position.commit()).await {
                Ok(pages) => self.pages = pages,
                Err(fault) => return self.fault(Lane::Sync, fault, now),
            }
        }
        self.upload = upload;
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
        if let Some(tick) = self.prune_unit(env, now).await {
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
                self.drained()
            } else {
                self.missed(env, now).await
            };
        }
        if let Some(tick) = self.wal_unit(env, now, false).await {
            return tick;
        }
        if let Some(tick) = self.upload_unit(env, now).await {
            return tick;
        }
        if self.settled(env.wal) {
            self.drained()
        } else {
            self.ran(Event::Idle, now)
        }
    }

    const fn drained(&self) -> Tick {
        Tick::Drained {
            txid: self.upload.confirmed(),
        }
    }

    /// The drain's deadline passed with work left: what was left, and the
    /// halt the deadline's fault routes to.
    async fn missed<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        let unfinished = self.unfinished(env).await;
        match self.fault(Lane::Sync, Fault::ShutdownDeadline, now) {
            Tick::Halted(HaltCause::Shutdown) => Tick::DeadlineReached(unfinished),
            other @ (Tick::Ran { .. }
            | Tick::Halted(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_)) => other,
        }
    }

    /// What is not on the replica. A failed read counts high, never low: the
    /// WAL file's length for the bytes, every staged transaction past the
    /// confirmed one for the files.
    async fn unfinished<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &self,
        env: &mut Env<'_, W, P, C, L, R>,
    ) -> Unfinished {
        let unsynced_wal_bytes = match env.wal.committed() {
            Ok(committed) => self.unsynced_bytes(committed),
            Err(_) => env.wal.len().ok().flatten().map_or(0, WalFileLen::bytes),
        };
        let confirmed = self.upload.confirmed();
        let count = match env.staging.list(Level::ZERO, None, PageLimit::MAX).await {
            Ok(listing) => listing
                .files
                .iter()
                .filter(|info| Some(info.key.range().min()) > confirmed)
                .count(),
            Err(_) => {
                let staged = self.staged().map_or(0, Txid::get);
                let confirmed = confirmed.map_or(0, Txid::get);
                usize::try_from(staged.saturating_sub(confirmed)).unwrap_or(usize::MAX)
            }
        };
        Unfinished {
            unsynced_wal_bytes,
            staged_not_uploaded: u32::try_from(count).unwrap_or(u32::MAX),
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
        self.position().is_none_or(|position| {
            self.reset != Some(end)
                && (end.salts() != position.cursor().salts()
                    || end.offset() != position.synced().offset())
        })
    }

    fn uploads_idle(&self) -> bool {
        plan_upload(&self.upload, self.staged()) == UploadPlan::Idle
    }

    /// The newest transaction anything has: a snapshot follows it.
    fn snapshot_after(&self) -> Option<Txid> {
        self.staged().max(self.upload.confirmed())
    }

    /// The checkpoint the policy wants now, when its lane and the sync lane
    /// that must prove it first may both run.
    fn checkpoint_decision(&self, now: Timestamp) -> Option<(Decision, CheckpointMode)> {
        let position = self.position().filter(|_| self.reset.is_none())?;
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
        self.host.poll(committed);
        let wants_live = may_checkpoint && self.live_wanted && self.caught_up();
        let wants_proof = may_checkpoint && (self.checkpoint_decision(now).is_some() || wants_live);
        let quiet = !self.wal_moved(committed) && self.pending.is_none() && !self.more_wal;
        if quiet && !wants_proof {
            return None;
        }
        let stepped = self.step(env, now).await;
        let (files, proof) = match self.stepped(stepped, now) {
            Ok(synced) => synced,
            Err(tick) => return Some(tick),
        };
        if wants_live
            && let Some(proof) = &proof
            && let Err(fault) = self.live_read(env, proof)
        {
            return Some(self.fault(Lane::Sync, fault, now));
        }
        let decision = self.checkpoint_decision(now).filter(|_| may_checkpoint);
        if let (Some(proof), Some((decision, mode))) = (proof, decision) {
            return Some(self.checkpoint_unit(env, proof, decision, mode, now).await);
        }
        (files > 0).then(|| self.ran(Event::Synced { files }, now))
    }

    /// Whether everything staged is on the replica.
    fn caught_up(&self) -> bool {
        self.staged().is_some() && self.staged() == self.upload.confirmed()
    }

    /// Reads the live pages when `proof` is at the transaction the replica
    /// holds; a proof past it (this tick staged more) waits for its upload.
    fn live_read<W: WalSource, P: PageSource, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        proof: &SyncedThrough,
    ) -> Result<(), Fault> {
        if Some(proof.position().txid()) != self.upload.confirmed() {
            return Ok(());
        }
        let live = read_live(proof, env.wal, env.db, &mut self.pages)?;
        self.live = Some(live);
        self.live_wanted = false;
        Ok(())
    }

    /// A step's result as what was staged, or as the tick a fault or a
    /// recovery verdict ends.
    #[expect(
        clippy::result_large_err,
        reason = "Tick is the early-exit value of a private step and is returned at once; FileInfo's stamp pushed it past the threshold"
    )]
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
            Ok(Stepped::Halt(cause)) => Err(self.verdict(Lane::Sync, cause, now)),
            Err(fault) => Err(self.fault(Lane::Sync, fault, now)),
        }
    }

    /// One sync step from what recovery says about the WAL now.
    async fn step<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault> {
        let (mut reader, committed, epoch) = match self.open_wal(env)? {
            Ok(opened) => opened,
            Err(verdict) => return Ok(verdict),
        };
        let size = reader.header().page_size();
        if let Some(was) = self.page_size.filter(|was| *was != size) {
            return Err(Fault::PageSizeChanged { was, now: size });
        }
        self.page_size = Some(size);
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
            &mut self.pages,
            env.staging,
            &self.cfg.sync,
            now,
        )
        .await
        .map_err(|failed| failed.fault)?;
        Ok(self.absorb(synced, committed.end(), env.wal.epoch() != epoch, now))
    }

    fn plan_step<W: WalSource>(
        &self,
        wal: &mut W,
        reader: &mut WalReader<W::Wal>,
        committed: CommittedRead,
    ) -> Result<Result<Step, Stepped>, Fault> {
        match (self.pending, self.position()) {
            (Some(cause), position) => Ok(Ok(Step::Snapshot {
                after: self.snapshot_after(),
                prev_timestamp: position.map(|position| position.timestamp()),
                cause,
            })),
            (None, Some(position)) => {
                let probes = Probes::read_against(reader, &position, &committed)?;
                let view = wal_view(wal, Some(&*reader), committed)?;
                let tip = Tip::Running(&position, probes);
                Ok(step_of(recover(Some(tip), &view, self.upload.confirmed())))
            }
            (None, None) => Err(Fault::Bug),
        }
    }

    /// The WAL a step reads, with the wal-index read and the read epoch it
    /// runs under, or what recovery makes of a WAL with no header.
    ///
    /// A pending snapshot that finds no header writes one first, through one
    /// `_litestream_seq` bump, as Go's `ensureWALExists` does before every
    /// sync (`db.go:826`). A database a restore just published, or one whose
    /// WAL a host truncated while we were down, has none until someone writes,
    /// and the snapshot is what puts the database file's pages on the replica:
    /// waiting for the host's next write would leave them off it, and a drain
    /// could never finish. A WAL still without a header after the bump is
    /// `WalMoved`, retried on the sync lane's backoff.
    #[expect(
        clippy::type_complexity,
        reason = "a private step's two exits: the opened WAL, or the verdict that ends it"
    )]
    fn open_wal<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &self,
        env: &mut Env<'_, W, P, C, L, R>,
    ) -> Result<Result<(WalReader<W::Wal>, CommittedRead, ReadEpoch), Stepped>, Fault> {
        let epoch = env.wal.epoch();
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        if let Some(reader) = open_reader(env.wal)? {
            return Ok(Ok((reader, committed, epoch)));
        }
        if self.pending.is_none() {
            return self.without_wal(env.wal, committed).map(Err);
        }
        env.ck.bump_seq().map_err(Fault::from_source)?;
        let epoch = env.wal.epoch();
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        let reader = open_reader(env.wal)?.ok_or(Fault::WalMoved)?;
        Ok(Ok((reader, committed, epoch)))
    }

    /// No readable WAL and no snapshot pending: recovery classifies the loss.
    fn without_wal<W: WalSource>(
        &self,
        wal: &mut W,
        committed: CommittedRead,
    ) -> Result<Stepped, Fault> {
        let (None, Some(position)) = (self.pending, self.position()) else {
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
    /// is not pinned and there is no proof. `committed` is the wal-index read
    /// the step ran under: a reset at it is remembered, so the actor waits for
    /// the index to change instead of stepping again.
    fn absorb(
        &mut self,
        synced: Synced,
        committed: CommittedEnd,
        epoch_moved: bool,
        now: Timestamp,
    ) -> Stepped {
        self.reset = None;
        let (files, txs, end) = match synced {
            Synced::Idle(through) => (0, 0, StepEnd::Through(through)),
            Synced::IndexReset => {
                self.reset = Some(committed);
                self.more_wal = false;
                return Stepped::Synced {
                    files: 0,
                    proof: None,
                };
            }
            Synced::Wrote { end, txs, .. } => {
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
            StepEnd::Unproven(position) => {
                self.reset = Some(committed);
                (position, None)
            }
        };
        let position = if epoch_moved {
            position.with_continuity(Continuity::Unpinned)
        } else {
            position
        };
        self.resume = Resume::From(position);
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
            &mut self.pages,
            &mut *env.ck,
            env.staging,
            &self.cfg.sync,
            now,
        )
        .await;
        let run = match &done {
            Ok(done) => done.run,
            Err(failed) => failed.run,
        };
        self.host.ours(env.wal.committed().ok(), run);
        let done = match done {
            Ok(done) => done,
            Err(CheckpointFailed { fault, staged, .. }) => {
                self.took(staged);
                return self.fault(Lane::Checkpoint, fault, now);
            }
        };
        let position = self.took(done.staged);
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

    /// What a checkpoint staged, finished or not: the files its steps staged
    /// are uploaded from the position it left, whose transaction is the
    /// newest of them, so a fault after the seal cannot leave one behind.
    fn took(&mut self, staged: Staged) -> Position {
        let position = staged.position();
        self.resume = Resume::From(position);
        position
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
            let plan = plan_upload(&self.upload, self.staged());
            match upload_next(self.upload, env.staging, env.remote, self.staged()).await {
                Ok(uploaded) => {
                    self.upload = uploaded.state;
                    last = uploaded.did;
                }
                Err(failed) => {
                    self.upload = failed.state;
                    if let (Fault::StagedLost, UploadPlan::Upload(key)) = (failed.fault, plan) {
                        return Some(self.lose_staged(env, key.range().min(), now).await);
                    }
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

    /// The staged file `from` is missing or does not verify. It and every
    /// staged file after it are deleted, and the next sync is a snapshot at
    /// `from`: the replica holds everything before it (the upload lane only
    /// uploads the confirmed transaction plus one), so the chain has no hole.
    async fn lose_staged<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        from: Txid,
        now: Timestamp,
    ) -> Tick {
        if let Err(fault) = drop_staged_from(env.staging, from).await {
            return self.fault(Lane::Upload, fault, now);
        }
        // The position is past `from`; a snapshot after it would leave a hole.
        self.resume = Resume::Snapshot(from.prev());
        let cause = SnapshotCause::LocalTipUnreadable;
        self.pending = Some(cause);
        self.lanes.get_mut(Lane::Upload).succeed();
        self.ran(Event::Resnapshot(cause), now)
    }

    /// Deletes staged level-0 files the replica no longer needs from staging,
    /// oldest first, at most `MAX_DELETE` (W3). A failure waits on its own
    /// lane and holds nothing else.
    async fn prune_unit<
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
        let bound = self
            .prune_bound()
            .filter(|_| self.running() && self.lanes.ready(Lane::Prune, now))?;
        let listing = match env.staging.list(Level::ZERO, None, PageLimit::MAX).await {
            Ok(listing) => listing,
            Err(err) => return Some(self.fault(Lane::Prune, Fault::from_staging(&err), now)),
        };
        let keys: Vec<FileKey> = listing
            .files
            .iter()
            .map(|info| info.key)
            .take_while(|key| key.range().max() <= bound)
            .take(MAX_DELETE)
            .collect();
        // A full page of candidates may have more behind it: continue from
        // the last one next tick rather than skip them.
        let more = keys.len() == listing.files.len() && listing.next.is_some();
        let through = keys
            .last()
            .filter(|_| more)
            .map_or(bound, |key| key.range().max());
        if !keys.is_empty()
            && let Err(err) = env.staging.delete(&keys).await
        {
            return Some(self.fault(Lane::Prune, Fault::from_staging(&err), now));
        }
        self.lanes.get_mut(Lane::Prune).succeed();
        self.pruned = Some(through);
        if keys.is_empty() {
            return None;
        }
        let event = Event::Maintained {
            event: MaintainedEvent::Deleted(keys.len()),
            stamps_read: 0,
        };
        Some(self.ran(event, now))
    }

    /// The newest staged transaction a prune may delete through, when it is
    /// past what was pruned: confirmed at the replica, compacted into its
    /// level 1 (compaction reads staged inputs first, R3), and below the tip
    /// (boot's rule 9 and V3 compare it with the WAL). `None` before the
    /// first maintenance listing.
    fn prune_bound(&self) -> Option<Txid> {
        let below_tip = self.staged()?.prev()?;
        let bound = self
            .upload
            .confirmed()?
            .min(self.maintain.level_one?)
            .min(below_tip);
        Some(bound).filter(|bound| self.pruned < Some(*bound))
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
            && self.lanes.ready(Lane::Maintenance, now)
            && self.uploads_idle()
            && self.maintain.wants_listing(now);
        if !ready {
            return None;
        }
        let inventory = match Inventory::load(env.remote).await {
            Ok(inventory) => inventory,
            Err(err) => return Some(self.fault(Lane::Maintenance, inventory_fault(&err), now)),
        };
        if self.maintain.read_back_owed
            && let Some(tick) = self.read_back_unit(env.remote, &inventory, now).await
        {
            return Some(tick);
        }
        let published = self.upload.confirmed();
        let decided = {
            let (cfg, state) = (&self.cfg.maintenance, &mut self.maintain.state);
            let mut step = |stamps: &Stamps| due(&inventory, now, cfg, published, state, stamps);
            resolve(env.remote, &mut step).await
        };
        // A failed stamp read leaves the listing owed: the next tick lists again.
        let decided = match decided {
            Ok(decided) => decided,
            Err(err) => return Some(self.fault(Lane::Maintenance, stamp_fault(&err), now)),
        };
        self.maintain.stale = false;
        self.maintain.level_one = Level::new(1)
            .and_then(|level| inventory.level(level).last())
            .map(|info| info.key.range().max());
        let stamps_read = decided.stamps_read;
        match decided.value {
            Due::Wait { until, held } => {
                self.lanes.get_mut(Lane::Maintenance).succeed();
                self.maintain.until = until;
                let event = Event::Maintained {
                    event: MaintainedEvent::Waiting { held },
                    stamps_read,
                };
                Some(self.ran(event, now))
            }
            Due::Run(job) => {
                // Whatever the job does, the next listing is what to plan from.
                self.maintain.stale = true;
                Some(self.run_job(env, &job, now, stamps_read).await)
            }
        }
    }

    /// The read-back a boot owes (see `maintain::audit`). `None` when every
    /// newest file is sound: the same listing then plans the next job.
    async fn read_back_unit<R: ObjectStore>(
        &mut self,
        remote: &R,
        inventory: &Inventory,
        now: Timestamp,
    ) -> Option<Tick> {
        let key = match read_back(remote, inventory).await {
            Ok(ReadBack::Sound) => {
                self.maintain.read_back_owed = false;
                return None;
            }
            Ok(ReadBack::Rotten { key, covered: true }) => key,
            Ok(ReadBack::Rotten { covered: false, .. }) => {
                return Some(self.fault(Lane::Maintenance, Fault::RemoteCorrupt, now));
            }
            Err(fault) => return Some(self.fault(Lane::Maintenance, fault, now)),
        };
        if let Err(err) = remote.delete(&[key]).await {
            return Some(self.fault(Lane::Maintenance, Fault::from_remote(&err), now));
        }
        // The listing no longer holds: the next tick lists and reads back again.
        self.maintain.stale = true;
        self.lanes.get_mut(Lane::Maintenance).succeed();
        let event = Event::Maintained {
            event: MaintainedEvent::Repaired(key),
            stamps_read: 0,
        };
        Some(self.ran(event, now))
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
        stamps_read: u64,
    ) -> Tick {
        let outcome = match job {
            Job::Compact(compact) => run_compact(compact, env.staging, env.remote).await,
            Job::Delete(delete) => run_delete(delete, env.remote).await,
            Job::Snapshot(_) => return self.snapshot_job(env, job, now, stamps_read).await,
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
        self.lanes.get_mut(Lane::Maintenance).succeed();
        self.ran(Event::Maintained { event, stamps_read }, now)
    }

    /// Where a failed job waits: its level's backoff when `MaintainState`
    /// keeps one (R3: the other levels keep running), else the maintenance
    /// lane, so a failing retention delete backs off instead of relisting
    /// at once. A level's wait is not the lane's failure, so the lane's wait
    /// resets; a degraded state stays.
    fn job_waiter(&mut self, job: &Job, now: Timestamp) -> Waiter {
        match self.maintain.state.retry_at(job, now) {
            Some(until) => {
                self.lanes.get_mut(Lane::Maintenance).clear_wait();
                Waiter::Kept(until)
            }
            None => Waiter::Lane(Lane::Maintenance),
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
        stamps_read: u64,
    ) -> Tick {
        // The snapshot's step is the sync lane's work and waits on its wait.
        if !self.lanes.ready(Lane::Sync, now) {
            self.wait_for_sync();
            let event = MaintainedEvent::Waiting { held: None };
            return self.ran(Event::Maintained { event, stamps_read }, now);
        }
        let stepped = self.step(env, now).await;
        let proof = match self.stepped(stepped, now) {
            Ok((0, Some(proof))) => proof,
            // A reset wal-index gives no proof until the next commit, and
            // nothing new is committed until then: the listing after that
            // commit's upload plans the snapshot again.
            Ok((0, None)) if self.reset.is_some() => {
                self.maintain.stale = false;
                self.maintain.until = None;
                let event = MaintainedEvent::Waiting { held: None };
                return self.ran(Event::Maintained { event, stamps_read }, now);
            }
            Ok((files, _)) => return self.ran(Event::Synced { files }, now),
            // A failed step waits on the sync lane, and the listing with it:
            // the next is due when the step may run again, not at once.
            Err(Tick::Ran { event, .. }) => {
                self.wait_for_sync();
                return self.ran(event, now);
            }
            Err(tick @ (Tick::Halted(_) | Tick::Drained { .. } | Tick::DeadlineReached(_))) => {
                return tick;
            }
        };
        let written = write_snapshot(
            &proof,
            &mut *env.wal,
            &mut *env.db,
            &mut self.pages,
            env.remote,
            now,
        )
        .await;
        self.maintain
            .state
            .record_snapshot(now, written.as_ref().err().map(|failed| &failed.fault));
        match written {
            Ok(info) => {
                self.lanes.get_mut(Lane::Maintenance).succeed();
                let event = MaintainedEvent::Snapshot(info);
                self.ran(Event::Maintained { event, stamps_read }, now)
            }
            Err(SnapshotFailed { fault, key }) => {
                // The store took the commit and then said the object is not
                // what was sent: it is listed, so it goes, as a rejected
                // compaction output does (R2).
                if leaves_bad_snapshot(fault) && env.remote.delete(&[key]).await.is_err() {
                    self.maintain.state.distrust_snapshot(key);
                }
                let waiter = self.job_waiter(job, now);
                self.fault(waiter, fault, now)
            }
        }
    }

    /// Maintenance lists again when the sync lane's wait ends, or after the
    /// next upload when it holds none (a recovery verdict made the next sync a
    /// snapshot, whose upload marks the listing stale).
    fn wait_for_sync(&mut self) {
        self.maintain.stale = false;
        self.maintain.until = self.lanes.get(Lane::Sync).until();
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
        // Before the boot only the boot runs, on the sync lane.
        if !self.booted {
            return self.lanes.ready(Lane::Sync, now);
        }
        let running = self.running();
        let sync = self.lanes.ready(Lane::Sync, now)
            && (!self.booted
                || self.pending.is_some()
                || self.more_wal
                || (running && self.checkpoint_decision(now).is_some()));
        let upload = self.lanes.ready(Lane::Upload, now) && !self.uploads_idle();
        let prune = running && self.lanes.ready(Lane::Prune, now) && self.prune_bound().is_some();
        let maintain = running
            && self.lanes.ready(Lane::Maintenance, now)
            && self.uploads_idle()
            && self.maintain.wants_listing(now);
        sync || upload || prune || maintain
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
            lane(Lane::Prune),
            lane(Lane::Maintenance),
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

/// What a boot found.
struct Booted {
    /// The newest staged file, or the replica's newest level-0 file.
    tip: Option<TipView>,
    /// What recovery makes of it.
    recovered: Recovered,
    /// The replica's page size, as far as boot could tell.
    page_size: Option<PageSize>,
}

/// The newest staged file (or, with none staged, the replica's newest
/// level-0 file), the replica's page size, and what recovery makes of them.
/// A WAL whose page size is not the replica's halts here, before
/// anything is staged.
async fn boot_recover<W: WalSource, L: ObjectStore, R: ObjectStore>(
    wal: &mut W,
    staging: &L,
    remote: &R,
    confirmed: Option<Txid>,
) -> Result<Booted, Fault> {
    let committed = wal.committed().map_err(Fault::from_source)?;
    let mut reader = open_reader(wal)?;
    let tip = match (TipView::load(staging, reader.as_mut()).await?, confirmed) {
        // Nothing staged: the replica's newest level-0 file is the tip.
        (None, Some(newest)) => TipView::load_remote(remote, reader.as_mut(), newest).await?,
        (staged, _) => staged,
    };
    let view = wal_view(wal, reader.as_ref(), committed)?;
    let page_size = match tip {
        Some(TipView::Readable { page_size, .. }) => Some(page_size),
        Some(TipView::Unreadable { .. }) | None if confirmed.is_some() => {
            replica_page_size(remote).await?
        }
        Some(TipView::Unreadable { .. }) | None => None,
    };
    let now = view.header.map(|header| header.page_size());
    let recovered = match page_size.zip(now).filter(|(was, now)| was != now) {
        Some((was, now)) => Recovered::Halt(HaltCause::PageSizeChanged { was, now }),
        None => recover(tip.as_ref().map(Tip::Boot), &view, confirmed),
    };
    Ok(Booted {
        tip,
        recovered,
        page_size,
    })
}

/// The page size in the header of the replica's newest file; `None` when the
/// replica is empty or that header does not decode. Every file of a prefix
/// has one page size, so one header answers for all of them.
async fn replica_page_size<R: ObjectStore>(remote: &R) -> Result<Option<PageSize>, Fault> {
    let inventory = Inventory::load(remote)
        .await
        .map_err(|err| inventory_fault(&err))?;
    let newest = inventory
        .files()
        .iter()
        .max_by_key(|file| file.key.range().max());
    let Some(newest) = newest else {
        return Ok(None);
    };
    let header = read_header(remote, &newest.key, Fault::from_remote).await?;
    Ok(header.map(|header| header.page_size()))
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
