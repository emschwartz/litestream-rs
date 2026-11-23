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
//!    checkpoint the decision calls for, consuming that sync's proof (no
//!    checkpoint runs without one, and a proof never outlives its tick).
//! 2. Upload while staged files are not on the replica.
//! 3. Prune staged level-0 files the replica's level 1 covers, keeping the
//!    newest staged file.
//! 4. Maintenance. Its first listing after a
//!    boot is read back first: the newest file of each level must verify.
//!    Every listing must also show what this process watched since the last
//!    one (`crate::regression_watch`); a watched file a read finds gone is a regression.
//!    A snapshot job runs only once uploads are idle, so its last
//!    transaction is at most what was published; with uploads busy it waits,
//!    and the next listing waits with it.
//!
//!    Fair share: after [`state::FAIR_SHARE`] uploads in a row, prune and
//!    maintenance, when either has work, take the next unit ahead of uploads
//!    (sync still comes first). Without it an engine whose uploads
//!    never go idle (a backlog on a slow disk, a process killed every few
//!    seconds) never compacts, never prunes staging and never deletes what
//!    retention lets go: maintenance must progress across short-lived
//!    processes, and its schedule is already the replica's, never uptime.
//!    An upload does not make a listing due at once: the next one waits for
//!    the next compaction window or the level-0 retention check interval,
//!    whichever comes first ([`crate::listing_after_upload`]), as Go's
//!    monitors wait for their tickers. A job, a boot, a released kept restore point or a
//!    switched retention still lists on the next tick, and every job is
//!    planned from a listing made in its own tick, so a delete never acts on
//!    an older one.
//!
//! Every failure becomes a [`Fault`] and goes through [`policy`] once, in
//! [`Actor::fault`]. A retry waits on its own task; the others keep running.
//! Every store call is an await point, and a future dropped at any of them is
//! a crash that the next boot recovers from.
//!
//! A persistent fault, and a recovery verdict that names a stop cause, goes
//! to the configured [`Responses`] (`crate::fault` has the rules). Under
//! `KeepTrying` the task that failed is degraded: each retry that fails ends
//! its tick with [`Event::Degraded`], the wait doubles up to
//! [`crate::MAX_BACKOFF`], the other tasks run, and the task's next success
//! clears it. A failed retention delete degrades maintenance; sync and upload
//! go on. While any task is degraded, retention pins the newest transaction
//! the replica held when the first one began (`maintain::retain`), and the
//! kept restore point goes once every task has recovered. A bug, and another writer whose
//! response is to continue, rebuild the actor and boot again after the wait;
//! a replica that went backwards is listed again. Under `Stop` the actor
//! stops, as every persistent fault did before.
//!
//! The actor is two halves (`parts`): what a rebuild keeps (the
//! configuration, what was asked of it, what it reports) and the engine a
//! rebuild replaces whole (what it believes about the WAL, the staging
//! directory and the replica). A driver that drops an actor after a panic
//! builds the next one from it with [`Actor::rebuilt`].
//!
//! A drain ends in one of two ticks: [`Tick::Drained`] with the newest
//! transaction the replica holds, or, at its deadline, [`Tick::DeadlineReached`]
//! with what was left ([`Unfinished`]); every tick after that one is
//! `Stopped(Shutdown)`. The wal-index gives a frame offset, not a transaction
//! id, so what is left is counted in WAL bytes and staged files.
//!
//! The actor also hands every wal-index read, and the read after each of its
//! own checkpoints, to a [`AppCheckpointObserver`] (`crate::app`), which counts the
//! checkpoints it did not run and infers the application's automatic checkpoints that
//! cannot run ([`Actor::app_checkpoints`]). The count changes no decision.

mod ahead;
mod clock;
mod lag;
mod parts;
mod state;
mod unproven;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{PageSize, Timestamp, Txid, TxidRange};
use lsr_wal::{CommittedEnd, OpenError, WalReader};

pub use ahead::AHEAD_KEPT_FOR;
use clock::EngineClock;
use parts::{Course, Engine, Kept, Stop, Tail, TaskRestorePoint};
pub use state::{Degraded, Phase, Task};
use state::{Gate, Live};

use crate::app::AppCheckpoints;
use crate::checkpoint::{
    CheckpointConfig, CheckpointFailed, CheckpointOutcome, CheckpointState, Decision, Observed,
    Staged, after_checkpoint, after_sync, checkpoint, decide, next_wake,
};
use crate::counts::{Counted, WorkCounts};
use crate::created_at::{ResolvedCreatedAt, resolve};
use crate::env::Clock;
use crate::failstop::MaxUnreplicatedAge;
use crate::fault::{
    Action, Backoff, Clamped, Fault, Next, OtherWriter, PersistentCause, Policy, Responses,
    STALLED_AFTER, SnapshotCause, SnapshotCounts, Stall, StopCause, backoff_wait, policy,
};
use crate::inventory::Inventory;
use crate::journal::{JournalFile, JournalSignal, JournalWriteError, JournalWriter, judge};
use crate::layout::{FileInfo, FileKey, Level};
use crate::maintain::{
    Due, Job, MAX_DELETE, Maintenance, Outcome, ReadBack, RetentionHold, due, listing_after_upload,
    read_back, run_compact, run_delete,
};
use crate::pages::{PageChecksumTable, seeded, snapshot_clears};
use crate::position::{Position, SyncedThrough, WalFileLen};
use crate::publish::Why;
use crate::recover::{
    BootWal, FrameCheck, Miss, NewestStagedFile, PositionSource, Probes, Recovered, WalView,
    newest_level_zero, newest_replica_level_zero, read_header, recover,
};
use crate::regression_watch::RegressionWatch;
use crate::snapshot::{SnapshotFailed, read_live, write_snapshot};
use crate::source::{CheckpointMode, Checkpointer, CommittedRead, PageSource, WalSource};
use crate::store::{ObjectStore, PageLimit, ReadLen, StoreError};
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step, wal_barrier};
use crate::takeover::TakeoverRecord;
use crate::upload::{
    Did, Swept, UploadConfig, UploadPlan, UploadState, Uploaded, drop_staged_from, inventory_fault,
    past_known, plan_upload, probe_past, sweep_staged, upload_next,
};
use crate::verify::LivePages;
use crate::witness::{LiveDb, Pending, Witnesses, still_holds, witness_pending};

/// What one tick may touch, lent for that tick only.
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
    /// The clock a staged file's created-at time is read from. The tick's own
    /// `now` is read before the tick looks at the WAL, so the application can
    /// commit after it and before the committed end that commit lands under is
    /// read. A file stamped with that `now` would be recorded as created
    /// before a transaction it carries, and an `AsOf` restore for an instant
    /// between the two would apply a transaction committed after the instant.
    /// So a stamp is read here, after the committed read that bounds the
    /// file: every transaction the file carries committed before it.
    pub clock: &'a dyn Clock,
    /// The engine journal (`docs/journal.md`), or `None` to run without one. Boot reads it and
    /// reports what it found ([`Actor::journal`]); every verdict, the worst included, leaves the
    /// engine doing exactly what it does with `None`.
    pub journal: Option<&'a mut (dyn JournalFile + Send)>,
}

impl<
    W: std::fmt::Debug,
    P: std::fmt::Debug,
    C: std::fmt::Debug,
    L: std::fmt::Debug,
    R: std::fmt::Debug,
> std::fmt::Debug for Env<'_, W, P, C, L, R>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Env")
            .field("wal", &self.wal)
            .field("db", &self.db)
            .field("ck", &self.ck)
            .field("staging", &self.staging)
            .field("remote", &self.remote)
            .field("journal", &self.journal.is_some())
            .finish_non_exhaustive()
    }
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
    /// How long a commit may stay off the replica before fail-stop trips for it
    /// (`crate::failstop`), and when the probe at the bound runs.
    pub max_unreplicated_age: MaxUnreplicatedAge,
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
        held: Option<RetentionHold>,
    },
}

/// The most files one tick stages: a sync step's one, then a checkpoint's
/// seal and the new generation it follows (a TRUNCATE stages one of its two).
const MOST_STAGED_PER_TICK: usize = 3;

/// What a tick did. Times in an event are engine time, which runs ahead of
/// the wall clock for a while after it steps back (`clock`).
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
        /// How many created-at times the decision read from the replica (zero when the
        /// listing carried them, or when the step did not consult the planner).
        created_at_read: u64,
    },
    /// Nothing was ready.
    Idle,
    /// A persistent fault whose configured response is to keep trying: the
    /// task is degraded and waits, the others run.
    Degraded {
        /// The task that failed.
        task: Task,
        /// Why.
        cause: PersistentCause,
        /// When the task first failed this way without a success between.
        since: Timestamp,
        /// When the retry is due.
        until: Timestamp,
    },
    /// A fault whose policy is to retry; its task waits.
    Retrying {
        /// What went wrong.
        fault: Fault,
        /// When the retry is due, read from where the wait is kept: the
        /// level's backoff for a maintenance job, the task's for the rest.
        until: Timestamp,
    },
    /// The next sync is a snapshot.
    Resnapshot(SnapshotCause),
    /// The clock read earlier than its last reading; nothing else ran.
    Clamped(Clamped),
    /// The engine is about to delete staged files the replica holds, and
    /// needs this takeover record durable first (`crate::takeover`): the
    /// driver writes it, hands it back with [`Actor::remember_takeover`], and
    /// ticks again. Nothing else ran.
    TakeoverWanted(TakeoverRecord),
    /// Where a standing ahead-of-WAL verdict is (`docs/decisions.md` D103); nothing else ran.
    AheadOfWal(AheadNotice),
}

/// What an operator is told about a standing ahead-of-WAL verdict besides its first report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AheadNotice {
    /// A boot found the verdict recorded: the state before the damage is kept until `until`.
    Kept {
        /// The kept restore point.
        restore_point: Txid,
        /// When it goes back to normal retention.
        until: Timestamp,
    },
    /// The window passed: the restore point is back under normal retention.
    Released {
        /// The restore point no longer kept.
        restore_point: Txid,
    },
}

/// How a tick ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Tick {
    /// Work ran (or nothing was ready); call again at `next`.
    Ran {
        /// What happened.
        event: Event,
        /// When to tick again, on the wall clock the caller reads. An earlier
        /// tick is always safe.
        next: Next,
    },
    /// Stopped; every later tick returns this.
    Stopped(StopCause),
    /// A drain finished: everything committed is on the replica.
    Drained {
        /// The newest transaction the replica holds; `None` when it holds
        /// none (nothing was ever committed).
        txid: Option<Txid>,
    },
    /// A drain's deadline passed with work left. The actor is stopped with
    /// [`StopCause::Shutdown`]; every later tick returns that.
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
    /// unchanged read-lock hold.
    Synced {
        files: u32,
        proof: Option<SyncedThrough>,
    },
    /// Recovery says the next step is a snapshot.
    Resnapshot(SnapshotCause),
    /// Recovery names a stop cause; the configured response decides.
    Stop(StopCause),
    /// Nothing was staged: the pending files hold as many tokens as allowed
    /// (`crate::witness::MAX_WITNESSED`). The position holds, the WAL keeps
    /// the frames, and staging resumes once uploads let tokens go.
    Held,
}

/// The replication engine for one database.
///
/// Two halves (`parts`): what a rebuild keeps, and the engine a rebuild
/// replaces whole.
#[derive(Debug)]
pub struct Actor {
    kept: Kept,
    engine: Engine,
}

/// Where a retry's wait is kept.
#[derive(Clone, Copy, Debug)]
enum Waiter {
    /// The task counts the failures and keeps the wait.
    Task(Task),
    /// Kept elsewhere (a maintenance level's backoff); this is when it ends.
    Kept(Timestamp),
}

impl From<Task> for Waiter {
    fn from(task: Task) -> Self {
        Self::Task(task)
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
    Ok(WalView {
        header: reader.map(|reader| *reader.header()),
        len: wal.len().map_err(Fault::from_source)?,
        committed: Some(committed),
        epoch: committed.epoch(),
    })
}

impl Actor {
    /// An actor that boots on its first tick.
    #[must_use]
    pub fn new(cfg: ActorConfig) -> Self {
        Self {
            kept: Kept::new(cfg),
            engine: Engine::fresh(),
        }
    }

    /// The actor to go on with after this one's engine can no longer be
    /// trusted (a panic unwound through a tick): a fresh engine that boots
    /// from the replica and the WAL again, beside everything that is not the
    /// engine's to forget. Kept: the configuration, a drain's deadline or a
    /// stop, each task's wait and degraded state and the kept restore point they
    /// hold, a scheduled verify's kept restore point and its request for live pages (pages
    /// read and not yet taken are read again), an operator's snapshot request
    /// not yet staged, the clock's last reading and the application-checkpoint
    /// counts (not the wal-index read they were counted from: the caller
    /// may have reopened the connections since).
    #[must_use]
    pub fn rebuilt(&self) -> Self {
        Self {
            kept: self.kept.rebuilt(self.kept.app.reopened()),
            engine: Engine::fresh(),
        }
    }

    /// Hands the actor the takeover record an operator or a restore left
    /// (`crate::takeover`), before its first tick, or the one it asked for
    /// ([`Self::takeover_wanted`]) once it is durable: a boot may then build
    /// on replica files the WAL does not vouch for, through its transaction.
    /// The newer of this and the one held stands.
    pub fn remember_takeover(&mut self, record: TakeoverRecord) {
        let held = self
            .kept
            .takeover
            .filter(|held| held.txid() > record.txid());
        self.kept.takeover = Some(held.unwrap_or(record));
        if self
            .kept
            .wanted_takeover
            .is_some_and(|wanted| record.covers(wanted.txid()))
        {
            self.kept.wanted_takeover = None;
        }
    }

    /// The takeover record the engine needs durable before its next tick
    /// deletes staged files the replica holds ([`Event::TakeoverWanted`]).
    ///
    /// Without it a crash between that delete and the snapshot replacing
    /// them leaves staging below the replica, and the next boot could not
    /// tell this engine's own files from another writer's. The record names
    /// only what staging vouched for before the delete: the newest staged
    /// transaction, or the replica's newest if that is lower.
    #[must_use]
    pub const fn takeover_wanted(&self) -> Option<TakeoverRecord> {
        self.kept.wanted_takeover
    }

    /// The replica's newest transaction when the last boot refused to build
    /// on replica files nothing vouched for (`crate::takeover`): what the
    /// stop's message names. `None` otherwise.
    #[must_use]
    pub const fn refused_through(&self) -> Option<Txid> {
        self.kept.refused_through
    }

    /// Whether the engine has booted: listed the replica and recovered its
    /// position. False again after a rebuild, until the next boot.
    #[must_use]
    pub const fn booted(&self) -> bool {
        self.engine.course.booted()
    }

    /// What the actor is doing.
    #[must_use]
    pub const fn phase(&self) -> Phase {
        self.kept.stop.phase(self.booted())
    }

    /// What the last boot's read of the journal reported, if anything: `None` with no journal,
    /// a readable one, or none on a first run.
    #[must_use]
    pub const fn journal(&self) -> Option<JournalSignal> {
        match self.kept.journal_write {
            Err(kind) => Some(JournalSignal::Unwritable(kind)),
            Ok(()) => self.kept.journal,
        }
    }

    /// The task's degraded state, if a persistent failure left it in one.
    #[must_use]
    pub fn degraded(&self, task: Task) -> Option<Degraded> {
        self.kept.tasks.get(task).degraded()
    }

    /// An operator accepts the database file as it is: every page the page
    /// checksum table reported as mismatched is trusted with the bytes it now
    /// holds, and the degraded state and kept restore point it held go.
    pub fn accept_database_pages(&mut self) {
        self.engine.pages.accept();
    }

    /// The newest transaction staged locally.
    #[must_use]
    pub const fn staged(&self) -> Option<Txid> {
        self.engine.course.staged()
    }

    /// The last synced position, `None` until the first sync after a boot
    /// that found nothing to continue.
    const fn position(&self) -> Option<Position> {
        self.engine.course.position()
    }

    /// The newest transaction the replica was confirmed to hold.
    #[must_use]
    pub const fn replicated(&self) -> Option<Txid> {
        self.engine.upload.confirmed()
    }

    /// Snapshots staged since the actor began, per cause; kept across a rebuild.
    #[must_use]
    pub const fn snapshots(&self) -> SnapshotCounts {
        self.kept.snapshots
    }

    /// What the engine's sync steps and checkpoints did since the actor began.
    #[must_use]
    pub const fn work(&self) -> WorkCounts {
        self.kept.work
    }

    /// Where in the WAL the newest staged file ends (Go's `litestream_wal_size`, the offset
    /// its last sync read to); 0 before the first sync.
    #[must_use]
    pub fn synced_wal_offset(&self) -> u64 {
        self.position().map_or(0, |p| p.synced().offset().get())
    }

    /// Why the next sync is a snapshot, when it is one.
    #[must_use]
    pub fn pending_snapshot(&self) -> Option<SnapshotCause> {
        self.pending()
    }

    /// Why the next sync is a snapshot. Before the boot, which knows no
    /// cause of its own yet, only an operator's request is one.
    fn pending(&self) -> Option<SnapshotCause> {
        match self.engine.course {
            Course::Unbooted => self.kept.asked(),
            Course::Snapshot { .. } | Course::At { .. } => self.engine.course.pending(),
        }
    }

    /// Whether a tick may stage: every file it can stage (`MOST_STAGED_PER_TICK`)
    /// would get a token.
    fn staging_has_room(&self) -> bool {
        self.engine.witnesses.has_room(MOST_STAGED_PER_TICK)
    }

    /// The last sync stopped at the cap: more is committed.
    fn more_wal(&self) -> bool {
        self.engine.course.tail() == Tail::More
    }

    /// The wal-index read at which the last step found the index reset.
    const fn reset(&self) -> Option<CommittedEnd> {
        match self.engine.course.tail() {
            Tail::Reset(at) => Some(at),
            Tail::Synced | Tail::More => None,
        }
    }

    /// Checkpoints someone else ran on the database since this actor began.
    #[must_use]
    pub const fn app_checkpoints_ran(&self) -> u64 {
        self.kept.app.counts().ran()
    }

    /// What the application's checkpoints did since this actor began.
    #[must_use]
    pub const fn app_checkpoints(&self) -> AppCheckpoints {
        self.kept.app.counts()
    }

    /// The application's `wal_autocheckpoint`, in frames (`None`: it runs none). The
    /// default is SQLite's, 1000; it only changes what the application-checkpoint
    /// notice infers.
    pub const fn set_app_autocheckpoint(&mut self, frames: Option<NonZeroU32>) {
        self.kept.app.set_autocheckpoint(frames);
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
    pub fn read_live_pages(&mut self) {
        self.kept.live.ask();
    }

    /// The live pages read since the last call, if any.
    pub fn take_live_pages(&mut self) -> Option<LivePages> {
        self.kept.live.take()
    }

    /// Keeps the restore chain of `txid` from retention, for a scheduled verify
    /// that found a later restore wrong (`None` lets it go). A degraded task's
    /// kept restore point, if older, still wins.
    pub fn keep_restore_point_for_verify(&mut self, txid: Option<Txid>) {
        let released = self.kept.verify_restore_point.is_some() && txid.is_none();
        self.kept.verify_restore_point = txid;
        if released {
            self.engine.maintain.stale = true;
        }
    }

    /// The restore point retention keeps, if one is pinned.
    #[must_use]
    pub fn kept_restore_point(&self) -> Option<Txid> {
        self.kept.kept_restore_point()
    }

    /// Makes the next sync a snapshot (an operator asked). A cause already
    /// pending is kept.
    pub fn request_snapshot(&mut self) {
        mark!(Run23);
        self.kept.snapshot_asked = true;
        self.engine
            .course
            .snapshot_for(SnapshotCause::OperatorRequested, true);
    }

    /// The deadline of a drain begun at `now` by a caller with none of its
    /// own: `now` plus the configured shutdown grace.
    #[must_use]
    pub fn shutdown_deadline(&self, now: Timestamp) -> Timestamp {
        now.saturating_add(self.kept.cfg.shutdown_grace)
    }

    /// Starts a drain that must finish by `deadline`. A drain already begun
    /// keeps the earlier of the two deadlines, so a second call can shorten
    /// it and never lengthen it.
    pub fn begin_shutdown(&mut self, deadline: Timestamp) {
        self.kept.stop = self.kept.stop.drain(deadline);
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
        if let Stop::Stopped(cause) = self.kept.stop {
            return Tick::Stopped(cause);
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
        let tick = self.counted_tick(&mut env, now).await;
        let tick = self.reconcile_journal(env.journal.as_deref_mut(), tick, now);
        self.observe_lag(env.wal, now);
        let tick = self.wake_for_probe(tick);
        self.wake_on_wall(tick)
    }

    /// [`Self::tick_at`], with the checkpoints it asks SQLite for counted (`crate::counts`).
    async fn counted_tick<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    {
        let mut ck = Counted::new(&mut *env.ck, env.clock);
        let mut counted = Env {
            wal: &mut *env.wal,
            db: &mut *env.db,
            ck: &mut ck,
            staging: env.staging,
            remote: env.remote,
            clock: env.clock,
            journal: shorter(&mut env.journal),
        };
        let tick = self.tick_at(&mut counted, now).await;
        self.kept.work.add_checkpoints(ck.totals());
        tick
    }

    /// One unit of work at engine time `now` (`clock`).
    async fn tick_at<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    {
        self.expire(now);
        if let Some(released) = self.expire_ahead(now) {
            // The verdict's restore point goes back to retention, which may now delete its chain.
            self.engine.maintain.stale = true;
            self.kept.ahead_notice = Some(AheadNotice::Released {
                restore_point: released,
            });
        }
        if let Some(notice) = self.kept.ahead_notice.take() {
            return Tick::Ran {
                event: Event::AheadOfWal(notice),
                next: Next::Now,
            };
        }
        self.probe_at_bound(now);
        if self.engine.pages.mismatch().is_none() {
            self.kept.tasks.page_matches_again();
        }
        self.release_restore_point();
        // A boot that keeps failing must not outlast a drain's deadline.
        if let (false, Some(deadline)) = (self.booted(), self.drain_deadline())
            && now >= deadline
        {
            return self.missed(env, now).await;
        }
        if !self.booted() {
            return self.boot(env, now).await;
        }
        match self.kept.stop {
            Stop::No => self.run(env, now).await,
            Stop::Draining { deadline } => {
                mark!(Run24);
                let deadline = self.on_engine_clock(deadline);
                self.drain(env, now, deadline).await
            }
            Stop::Stopped(cause) => Tick::Stopped(cause),
        }
    }

    /// Engine time for the wall reading `now` (`clock`): never earlier than
    /// the last, never stopped. A reading earlier than the last is reported
    /// and ends the tick.
    fn clock(&mut self, now: Timestamp) -> Result<Timestamp, Clamped> {
        let (clock, stepped) = self
            .kept
            .clock
            .map_or((EngineClock::start(now), false), |clock| clock.read(now));
        self.kept.clock = Some(clock);
        if stepped {
            mark!(Run19);
            return Err(Clamped {
                prev: clock.now(),
                now,
            });
        }
        Ok(clock.now())
    }

    /// The drain's deadline in engine time.
    fn drain_deadline(&self) -> Option<Timestamp> {
        match self.kept.stop {
            Stop::Draining { deadline } => Some(self.on_engine_clock(deadline)),
            Stop::No | Stop::Stopped(_) => None,
        }
    }

    /// A time the caller named on the wall clock (a drain's deadline), in
    /// engine time.
    fn on_engine_clock(&self, wall: Timestamp) -> Timestamp {
        self.kept
            .clock
            .map_or(wall, |clock| clock.engine_when(wall))
    }

    /// The earliest stamp for a file made in a tick at engine time `now`: the
    /// highest wall reading, never engine time (`clock`).
    fn stamp_floor(&self, now: Timestamp) -> Timestamp {
        self.kept.clock.map_or(now, EngineClock::stamp_floor)
    }

    /// The tick with its wake moved to the wall clock the host sleeps on.
    fn wake_on_wall(&self, tick: Tick) -> Tick {
        match (tick, self.kept.clock) {
            (
                Tick::Ran {
                    event,
                    next: Next::At(at),
                },
                Some(clock),
            ) => Tick::Ran {
                event,
                next: Next::At(clock.wall_at(at)),
            },
            (tick, _) => tick,
        }
    }

    /// Routes a fault by its policy. The only place `policy` is called.
    fn fault(&mut self, waiter: impl Into<Waiter>, fault: Fault, now: Timestamp) -> Tick {
        let waiter = waiter.into();
        self.sync_failed(fault);
        self.lag_failed(waiter, Some(fault), now);
        match policy(&fault) {
            Policy::Retry(backoff) => {
                if let Some(stall) = self.stalled(waiter, &fault) {
                    return self.persistent(waiter, PersistentCause::Stalled(stall), backoff, now);
                }
                let until = self.wait(waiter, backoff, now);
                self.ran(Event::Retrying { fault, until }, now)
            }
            Policy::Persistent(cause) => self.persistent(waiter, cause, Backoff::Doubling, now),
            Policy::Shutdown => self.stop(StopCause::Shutdown),
        }
    }

    /// The stall a retried fault makes of its task on this failure: the
    /// [`STALLED_AFTER`]th in a row, or any later one. A wait a maintenance level
    /// keeps never stalls: maintenance blocks no write's way to the replica.
    fn stalled(&self, waiter: Waiter, fault: &Fault) -> Option<Stall> {
        let Waiter::Task(task) = waiter else {
            return None;
        };
        let failures = self.kept.tasks.get(task).failures();
        (failures >= STALLED_AFTER.get().saturating_sub(1))
            .then(|| Stall::of(fault))
            .flatten()
    }

    /// Counts a failure where its wait is kept; returns when the wait ends.
    fn wait(&mut self, waiter: Waiter, backoff: Backoff, now: Timestamp) -> Timestamp {
        match waiter {
            Waiter::Kept(until) => until,
            Waiter::Task(task) => {
                let interval = self.kept.cfg.retry_interval;
                self.kept.tasks.get_mut(task).fail(|attempt| {
                    let wait = backoff_wait(backoff, attempt, interval);
                    now.saturating_add(wait)
                })
            }
        }
    }

    /// A recovery verdict that names a stop cause: a persistent one goes to
    /// the configured response, the end of a drain stops.
    fn verdict(&mut self, task: Task, cause: StopCause, now: Timestamp) -> Tick {
        self.lag_failed(Waiter::Task(task), None, now);
        match PersistentCause::of(cause) {
            Some(cause) => self.persistent(Waiter::Task(task), cause, Backoff::Doubling, now),
            None => self.stop(cause),
        }
    }

    /// What the configured response makes of a persistent cause: a stop, or
    /// a degraded task that waits `backoff` (the doubling one, or a stalled
    /// fault's own). The only place `Responses` is read.
    fn persistent(
        &mut self,
        waiter: Waiter,
        cause: PersistentCause,
        backoff: Backoff,
        now: Timestamp,
    ) -> Tick {
        let action = self.kept.cfg.responses.action(cause);
        if let (Action::Stop, Some(stop)) = (action, cause.stop()) {
            return self.stop(stop);
        }
        self.keep_restore_point();
        let waiter = match action {
            Action::Stop | Action::Retry => waiter,
            Action::Relist => {
                // What the replica was known to hold is gone from it: the
                // retry lists it and continues from what it holds now. Every
                // fact taken from a listing goes with it (level 1's cover,
                // which bounds the staged files a prune may delete, and the
                // next maintenance listing is owed now); the acknowledged
                // mark stays, so no snapshot takes an acknowledged id again.
                self.engine.upload = UploadState::ListFirst { confirmed: None };
                self.engine.maintain.level_one = None;
                self.engine.maintain.stale = true;
                self.engine.maintain.watch = None;
                waiter
            }
            Action::Rebuild => {
                self.rebuild();
                Waiter::Task(Task::Sync)
            }
        };
        let task = match waiter {
            Waiter::Task(task) => task,
            Waiter::Kept(_) => Task::Maintenance,
        };
        let until = self.wait(waiter, backoff, now);
        let since = self.kept.tasks.get_mut(task).degrade(cause, now);
        let event = Event::Degraded {
            task,
            cause,
            since,
            until,
        };
        self.ran(event, now)
    }

    /// Pins the newest transaction the replica holds for retention, unless a
    /// degradation already pinned one.
    fn keep_restore_point(&mut self) {
        if self.kept.task_restore_point == TaskRestorePoint::Free {
            self.kept.task_restore_point = TaskRestorePoint::Held(self.engine.upload.confirmed());
        }
    }

    /// Lets the kept restore point go once no task is degraded; the next listing may then
    /// delete what it kept.
    fn release_restore_point(&mut self) {
        if self.kept.task_restore_point != TaskRestorePoint::Free && !self.kept.tasks.any_degraded()
        {
            self.kept.task_restore_point = TaskRestorePoint::Free;
            self.engine.maintain.stale = true;
        }
    }

    /// Replaces the engine with a fresh one, so the next tick boots from the
    /// replica and the WAL again ([`Self::rebuilt`] says what is kept). The
    /// connections are the same, so the application observer's last read stands.
    fn rebuild(&mut self) {
        *self = Self {
            kept: self.kept.rebuilt(self.kept.app),
            engine: Engine::fresh(),
        };
    }

    /// Lets every passed deadline go, so the tick runs its task and `next`
    /// never names a time already past (W1).
    fn expire(&mut self, now: Timestamp) {
        self.kept.tasks.expire(now);
        self.engine.gate = self.engine.gate.expire(now);
        self.engine.maintain.expire(now);
    }

    fn stop(&mut self, cause: StopCause) -> Tick {
        // A stop for another writer needs no record to outlive the process:
        // the next boot refuses the other writer's files again, whether they
        // stand above staging or under a key staging holds another copy of
        // (`crate::takeover`).
        self.kept.stop = Stop::Stopped(cause);
        Tick::Stopped(cause)
    }

    /// The takeover a boot may build on (`crate::takeover`): `Off` when the
    /// configuration continues past another writer, which checks nothing, so
    /// the boot takes the replica as it is.
    const fn boot_takeover(&self) -> Takeover {
        if matches!(
            self.kept.cfg.responses.other_writer,
            OtherWriter::ContinueAndInterleaveHistories
        ) {
            return Takeover::Off;
        }
        Takeover::Vouch(self.kept.takeover)
    }

    fn ran(&self, event: Event, now: Timestamp) -> Tick {
        Tick::Ran {
            event,
            next: self.next(now),
        }
    }

    /// A boot that could not finish: a fault, or a takeover it needs durable
    /// before it drops staged files.
    fn boot_step(&mut self, step: BootStep, now: Timestamp) -> Tick {
        match step {
            BootStep::Fault(fault) => self.boot_failed(fault, now),
            BootStep::Vouch(record) => self.want_takeover(Task::Sync, record, now),
            BootStep::Refused(through) => {
                self.kept.refused_through = Some(through);
                self.verdict(Task::Sync, StopCause::OtherWriter, now)
            }
        }
    }

    /// Asks the driver for `record` before `task` deletes staged files. Asked
    /// already and still not handed back, the driver could not write it: the
    /// task fails `LocalIo` and waits its backoff, then asks again, so a
    /// meta directory that refuses writes never spins and never loses files
    /// unrecorded.
    fn want_takeover(&mut self, task: Task, record: TakeoverRecord, now: Timestamp) -> Tick {
        if self.kept.wanted_takeover.take() == Some(record) {
            return self.fault(task, Fault::LocalIo, now);
        }
        self.kept.wanted_takeover = Some(record);
        Tick::Ran {
            event: Event::TakeoverWanted(record),
            next: Next::Now,
        }
    }

    /// The takeover a running task needs before it deletes staged files the
    /// replica may hold: through the staged position, or `cap` if that is
    /// lower, unless one held already covers it or the configuration checks
    /// nothing at boot. A running engine's staged files are its own, and any
    /// of them may be on the replica unconfirmed (an upload whose answer was
    /// lost), so only a key the replica is known to end at caps it.
    fn vouch_before_drop(&mut self, task: Task, cap: Option<Txid>, now: Timestamp) -> Option<Tick> {
        if self.kept.cfg.responses.other_writer == OtherWriter::ContinueAndInterleaveHistories {
            return None;
        }
        let staged = self.staged()?;
        let through = cap.map_or(staged, |cap| staged.min(cap));
        if self.kept.takeover.is_some_and(|held| held.covers(through)) {
            return None;
        }
        Some(self.want_takeover(task, TakeoverRecord::through(through), now))
    }

    /// Lists the replica, witnesses the pending staged files against the
    /// live WAL (`crate::witness`), reads the newest staged file left and
    /// recovers (in that order). A failure anywhere boots again from the start.
    /// A boot step failed: the boot ends and is retried.
    fn boot_failed(&mut self, fault: Fault, now: Timestamp) -> Tick {
        mark!(Boot23);
        self.fault(Task::Sync, fault, now)
    }

    async fn boot<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        if !self.kept.tasks.ready(Task::Sync, now) {
            return self.ran(Event::Idle, now);
        }
        let takeover = self.boot_takeover();
        let journal = read_journal(env.journal.as_deref_mut());
        let suspect = &mut self.kept.suspect_staged;
        let listed = boot_listing(env.staging, env.remote, suspect, takeover).await;
        let Listed {
            upload,
            dropped,
            gate,
        } = match listed {
            Ok(listed) => listed,
            Err(step) => return self.boot_step(step, now),
        };
        let Judged {
            pending: Pending { tokens, refused },
            upload,
            booted:
                Booted {
                    newest_staged,
                    recovered,
                    page_size,
                    refused_through,
                },
        } = match boot_judge(env, upload, gate).await {
            Ok(found) => found,
            Err(step) => return self.boot_step(step, now),
        };
        let continued = matches!(recovered, Recovered::Continue(_));
        let from = match recovered {
            Recovered::Continue(at) | Recovered::WalRestarted(at) => boot_continues(at),
            Recovered::Snapshot { cause, .. } => Err(cause),
            Recovered::Stop(cause) => {
                self.kept.refused_through = refused_through;
                return self.verdict(Task::Sync, cause, now);
            }
        };
        // A boot that fails later reads the journal again.
        self.adopt_journal(journal, newest_staged.as_ref(), &upload);
        self.owe_ahead_check(continued);
        let cause = boot_cause(dropped, refused, from.err());
        let staged = newest_staged.map(|newest_staged| newest_staged.txid());
        let course = boot_course(from, cause, self.kept.asked(), staged);
        let (pages, quiet) = match boot_seed(env.staging, &course, upload.confirmed()).await {
            Ok(seeded) => seeded,
            Err(fault) => return self.boot_failed(fault, now),
        };
        self.quiet_at_boot(quiet);
        // Every step succeeded: the engine is assigned whole.
        self.engine = Engine {
            course,
            page_size,
            pages,
            upload,
            acknowledged: upload.confirmed(),
            checkpoint: CheckpointState::boot(now),
            witnesses: tokens.into_iter().collect(),
            ..Engine::fresh()
        };
        self.kept.tasks.get_mut(Task::Sync).succeed();
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
        if self.fair_turn() {
            if let Some(tick) = self.prune_unit(env, now).await {
                return tick;
            }
            if let Some(tick) = self.maintain_unit(env, now).await {
                return tick;
            }
        }
        if let Some(tick) = self.upload_unit(env, now).await {
            let maintain = &mut self.engine.maintain;
            maintain.passed_over = maintain.passed_over.saturating_add(1);
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
            txid: self.engine.upload.confirmed(),
        }
    }

    /// The drain's deadline passed with work left: what was left, and the
    /// stop the deadline's fault routes to.
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
        match self.fault(Task::Sync, Fault::ShutdownDeadline, now) {
            Tick::Stopped(StopCause::Shutdown) => Tick::DeadlineReached(unfinished),
            other @ (Tick::Ran { .. }
            | Tick::Stopped(_)
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
        let confirmed = self.engine.upload.confirmed();
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
        synced && self.pending().is_none() && !self.more_wal() && self.uploads_idle()
    }

    fn wal_moved(&self, committed: CommittedRead) -> bool {
        let end = committed.end();
        self.position().is_none_or(|position| {
            self.reset() != Some(end)
                && (end.salts() != position.cursor().salts()
                    || end.offset() != position.synced().offset())
        })
    }

    fn uploads_idle(&self) -> bool {
        plan_upload(&self.engine.upload, self.staged()) == UploadPlan::Idle
    }

    /// The newest transaction anything has, or had: a snapshot follows it.
    fn snapshot_after(&self) -> Option<Txid> {
        self.staged()
            .max(self.engine.upload.confirmed())
            .max(self.engine.acknowledged)
    }

    /// Takes the upload state a step left, raising the acknowledged mark.
    fn set_upload(&mut self, state: UploadState) {
        self.engine.upload = state;
        self.engine.acknowledged = self.engine.acknowledged.max(state.confirmed());
        self.engine.witnesses.confirmed(state.confirmed());
    }

    /// The checkpoint the policy wants now, when its task and the sync task
    /// that must prove it first may both run.
    fn checkpoint_decision(&self, now: Timestamp) -> Option<(Decision, CheckpointMode)> {
        let position = self.position().filter(|_| self.reset().is_none())?;
        let free = self.engine.gate.open(now)
            && self.kept.tasks.ready(Task::Checkpoint, now)
            && self.kept.tasks.ready(Task::Sync, now);
        if !free {
            return None;
        }
        let observed = Observed::of(now, &position, !self.more_wal());
        let decision = decide(
            &self.engine.checkpoint,
            &observed,
            &self.kept.cfg.checkpoint,
        );
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
        if !self.kept.tasks.ready(Task::Sync, now) {
            return None;
        }
        let committed = match env.wal.committed() {
            Ok(committed) => committed,
            Err(err) => return Some(self.fault(Task::Sync, Fault::from_source(err), now)),
        };
        self.kept.app.poll(committed);
        let wants_live = may_checkpoint && self.kept.live.wanted() && self.caught_up();
        let wants_proof = may_checkpoint && (self.checkpoint_decision(now).is_some() || wants_live);
        let quiet = !self.wal_moved(committed) && self.pending().is_none() && !self.more_wal();
        if quiet && !wants_proof && !self.owes_ahead_check() {
            return None;
        }
        if self.owes_truncate() && !may_checkpoint {
            return Some(self.owed_in_drain(now));
        }
        let stepped = self.counted_step(env, now).await;
        let (files, proof) = match self.stepped(stepped, now) {
            Ok(synced) => synced,
            Err(tick) => return Some(tick),
        };
        if wants_live
            && let Some(proof) = &proof
            && let Err(fault) = self.live_read(env, proof)
        {
            return Some(self.fault(Task::Sync, fault, now));
        }
        let decision = self.checkpoint_decision(now).filter(|_| may_checkpoint);
        if let (Some(proof), Some((decision, mode))) = (proof, decision) {
            return Some(self.checkpoint_unit(env, proof, decision, mode, now).await);
        }
        (files > 0).then(|| self.ran(Event::Synced { files }, now))
    }

    /// Whether everything staged is on the replica.
    fn caught_up(&self) -> bool {
        self.staged().is_some() && self.staged() == self.engine.upload.confirmed()
    }

    /// Reads the live pages when `proof` is at the transaction the replica
    /// holds; a proof past it (this tick staged more) waits for its upload.
    fn live_read<W: WalSource, P: PageSource, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        proof: &SyncedThrough,
    ) -> Result<(), Fault> {
        if Some(proof.position().txid()) != self.engine.upload.confirmed() {
            return Ok(());
        }
        let live = read_live(proof, env.wal, env.db, &mut self.engine.pages)?;
        self.kept.live = Live::Read(live);
        Ok(())
    }

    /// A step's result as what was staged, or as the tick a fault or a
    /// recovery verdict ends.
    #[expect(
        clippy::result_large_err,
        reason = "Tick is the early-exit value of a private step and is returned at once; FileInfo's created-at time pushed it past the threshold"
    )]
    fn stepped(
        &mut self,
        stepped: Result<Stepped, Fault>,
        now: Timestamp,
    ) -> Result<(u32, Option<SyncedThrough>), Tick> {
        match stepped {
            Ok(Stepped::Synced { files, proof }) => {
                self.kept.tasks.get_mut(Task::Sync).succeed();
                if files > 0 {
                    self.staged_after_truncate();
                }
                Ok((files, proof))
            }
            Ok(Stepped::Resnapshot(cause)) => {
                self.engine.course.snapshot_for(cause, false);
                Err(self.ran(Event::Resnapshot(cause), now))
            }
            Ok(Stepped::Held) => Ok((0, None)),
            Ok(Stepped::Stop(cause)) => Err(self.verdict(Task::Sync, cause, now)),
            Err(fault) => Err(self.fault(Task::Sync, fault, now)),
        }
    }

    /// [`Self::step`], counted (`crate::counts`).
    async fn counted_step<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault>
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    {
        let began = env.clock.now();
        let from = self.position();
        let stepped = self.step(env, now).await;
        let took = env.clock.now().saturating_duration_since(began);
        let fault = stepped.as_ref().err().copied();
        let to = self.position();
        self.kept
            .work
            .sync_step(fault, took, from.as_ref(), to.as_ref());
        stepped
    }

    /// One sync step from what recovery says about the WAL now.
    async fn step<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault> {
        if self.owes_ahead_check() {
            return self.check_ahead(env, now).await;
        }
        if self.owes_truncate() {
            return self.unprove(env);
        }
        if !self.staging_has_room() {
            // Marked as a step cut short, so the sync runs again when room
            // frees up: no new WAL change may come to wake it.
            mark!(Run18);
            self.engine.course.set_tail(Tail::More);
            return Ok(Stepped::Held);
        }
        let (mut reader, committed) = match self.open_wal(env)? {
            Ok(opened) => opened,
            Err(verdict) => return Ok(verdict),
        };
        // After the committed read: see `Env::clock`.
        let stamp = env.clock.now().max(self.stamp_floor(now));
        let size = reader.header().page_size();
        if let Some(was) = self.engine.page_size.filter(|was| *was != size) {
            mark!(Run11);
            return Err(Fault::PageSizeChanged { was, now: size });
        }
        self.engine.page_size = Some(size);
        let step = match self.plan_step(env.wal, &mut reader, committed)? {
            Ok(step) => step,
            Err(verdict) => return Ok(verdict),
        };
        wal_barrier(&mut *env.wal, &committed)?;
        let synced = sync_step(
            step,
            &mut reader,
            committed,
            &mut *env.db,
            &mut self.engine.pages,
            env.staging,
            &self.kept.cfg.sync,
            stamp,
        )
        .await
        .map_err(|failed| failed.fault)?;
        Ok(self.absorb(synced, committed.end(), &*env.wal))
    }

    fn plan_step<W: WalSource>(
        &self,
        wal: &mut W,
        reader: &mut WalReader<W::Wal>,
        committed: CommittedRead,
    ) -> Result<Result<Step, Stepped>, Fault> {
        match (self.pending(), self.position()) {
            // A snapshot that keeps the page table trusts it to describe the
            // database, which holds only while the WAL still follows the
            // position: recovery first, and a verdict (a WAL that moved
            // under the position) replaces the cause with one that clears it.
            (Some(cause), Some(position)) if !snapshot_clears(&cause) => Ok(self
                .recovered(wal, reader, committed, &position)?
                .map(|_| Step::Snapshot {
                    after: self.snapshot_after(),
                    prev_timestamp: Some(position.timestamp()),
                    cause,
                })),
            (Some(cause), position) => Ok(Ok(Step::Snapshot {
                after: self.snapshot_after(),
                prev_timestamp: position.map(|position| position.timestamp()),
                cause,
            })),
            (None, Some(position)) => self.recovered(wal, reader, committed, &position),
            // Unreachable by construction: no position means a `Course`
            // that names a snapshot's cause. Retried as the WAL having moved.
            (None, None) => Err(Fault::WalMoved),
        }
    }

    /// What recovery makes of the WAL past a running position.
    fn recovered<W: WalSource>(
        &self,
        wal: &mut W,
        reader: &mut WalReader<W::Wal>,
        committed: CommittedRead,
        position: &Position,
    ) -> Result<Result<Step, Stepped>, Fault> {
        let probes = Probes::read_against(reader, position, &committed)?;
        let view = wal_view(wal, Some(&*reader), committed)?;
        let source = PositionSource::Running(position, probes);
        Ok(step_of(recover(
            Some(source),
            &view,
            self.engine.upload.confirmed(),
        )))
    }

    /// The WAL a step reads, with the wal-index read and the read-lock hold it
    /// runs under, or what recovery makes of a WAL with no header.
    ///
    /// A pending snapshot that finds no header writes one first, through one
    /// `_litestream_seq` bump, as Go's `ensureWALExists` does before every
    /// sync (`db.go:826`). A database a restore just published, or one whose
    /// WAL an application truncated while we were down, has none until someone writes,
    /// and the snapshot is what puts the database file's pages on the replica:
    /// waiting for the application's next write would leave them off it, and a drain
    /// could never finish. A WAL still without a header after the bump is
    /// `WalMoved`, retried on the sync task's backoff.
    #[expect(
        clippy::type_complexity,
        reason = "a private step's two exits: the opened WAL, or the verdict that ends it"
    )]
    fn open_wal<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore>(
        &self,
        env: &mut Env<'_, W, P, C, L, R>,
    ) -> Result<Result<(WalReader<W::Wal>, CommittedRead), Stepped>, Fault> {
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        if let Some(reader) = open_reader(env.wal)? {
            return Ok(Ok((reader, committed)));
        }
        if self.pending().is_none() {
            return self.without_wal(env.wal, committed).map(Err);
        }
        env.ck.bump_seq().map_err(Fault::from_source)?;
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        let reader = open_reader(env.wal)?.ok_or(Fault::WalMoved)?;
        Ok(Ok((reader, committed)))
    }

    /// No readable WAL and no snapshot pending: recovery classifies the loss.
    fn without_wal<W: WalSource>(
        &self,
        wal: &mut W,
        committed: CommittedRead,
    ) -> Result<Stepped, Fault> {
        let (None, Some(position)) = (self.pending(), self.position()) else {
            return Err(Fault::WalMoved);
        };
        mark!(Run9);
        let view = wal_view::<W, W::Wal>(wal, None, committed)?;
        // With no header, recovery decides before it looks at the probes.
        let probes = Probes {
            prev: FrameCheck::Miss(Miss::Truncated),
            stale_next: false,
            rechained: false,
        };
        let source = PositionSource::Running(&position, probes);
        Ok(
            match step_of(recover(Some(source), &view, self.engine.upload.confirmed())) {
                Ok(_) => Stepped::Resnapshot(SnapshotCause::WalMissing),
                Err(verdict) => verdict,
            },
        )
    }

    /// Takes in a step's result. A step that reached the committed end is
    /// confirmed against `wal`'s epoch, read again here, after the staging
    /// commit: when it moved, the position is not pinned and there is no
    /// proof. `committed` is the wal-index read
    /// the step ran under: a reset at it is remembered, so the actor waits for
    /// the index to change instead of stepping again.
    fn absorb<W: WalSource>(
        &mut self,
        synced: Synced,
        committed: CommittedEnd,
        wal: &W,
    ) -> Stepped {
        let (files, txs, end) = match synced {
            Synced::Idle(through) => (0, 0, StepEnd::Through(through)),
            Synced::IndexReset => {
                mark!(Run10);
                self.engine.course.set_tail(Tail::Reset(committed));
                return Stepped::Synced {
                    files: 0,
                    proof: None,
                };
            }
            Synced::Wrote {
                end,
                txs,
                witness,
                changes,
                ..
            } => {
                mark!(Run1);
                let key = witness.key();
                self.engine.witnesses.insert(witness);
                // With a cause pending, the step that wrote was the snapshot.
                if !changes {
                    self.note_quiet_file(key);
                }
                if let Some(cause) = self.pending() {
                    if changes {
                        self.note_quiet_snapshot(cause, key, &end);
                    }
                    self.kept.snapshot_asked = false;
                    self.kept.snapshots.count(&cause);
                }
                if self.engine.gate == Gate::NextWrite {
                    self.engine.gate = Gate::Open;
                }
                (1, txs, end)
            }
        };
        let (position, proof, tail) = match end {
            StepEnd::Through(reached) => match reached.confirm(wal) {
                Ok(through) => (*through.position(), Some(through), Tail::Synced),
                Err(position) => (position, None, Tail::Synced),
            },
            // Neither is pinned: the step never claimed the committed end.
            StepEnd::Limited(position) => (position, None, Tail::More),
            StepEnd::Unproven(position) => (position, None, Tail::Reset(committed)),
        };
        self.engine.course.moved(position, tail, files == 1);
        self.engine.checkpoint = after_sync(
            self.engine.checkpoint,
            txs,
            position.synced().offset().frame_index(),
            &self.kept.cfg.checkpoint,
        );
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
            &mut self.engine.pages,
            &mut *env.ck,
            env.staging,
            &self.kept.cfg.sync,
            env.clock,
        )
        .await;
        let run = match &done {
            Ok(done) => done.run,
            Err(failed) => failed.run,
        };
        self.kept.app.ours(env.wal.committed().ok(), run);
        let done = match done {
            Ok(done) => done,
            Err(CheckpointFailed { fault, staged, .. }) => {
                mark!(Ckpt5);
                self.took(staged);
                return self.fault(Task::Checkpoint, fault, now);
            }
        };
        let position = self.took(done.staged);
        if done.outcome == CheckpointOutcome::Truncated {
            // A TRUNCATE that changed the WAL staged its boundary snapshot.
            self.kept
                .snapshots
                .count(&SnapshotCause::CheckpointBoundary);
        }
        let observed = Observed::of(now, &position, true);
        let (state, next) = after_checkpoint(
            self.engine.checkpoint,
            decision,
            &done.outcome,
            &observed,
            &self.kept.cfg.checkpoint,
        );
        self.engine.checkpoint = state;
        self.engine.gate = Gate::of(next);
        self.kept.tasks.get_mut(Task::Checkpoint).succeed();
        let busy = done.outcome == CheckpointOutcome::Busy;
        self.ran(Event::Checkpointed { mode, busy }, now)
    }

    /// What a checkpoint staged, finished or not: the files its steps staged
    /// are uploaded from the position it left, whose transaction is the
    /// newest of them, so a fault after the seal cannot leave one behind.
    fn took(&mut self, staged: Staged) -> Position {
        for token in staged.witnesses() {
            self.engine.witnesses.insert(*token);
        }
        for key in staged.quiet() {
            self.note_quiet_file(*key);
        }
        let position = staged.position();
        let tail = self.engine.course.tail();
        self.engine.course.moved(position, tail, false);
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
        if !self.upload_ready(now) {
            return None;
        }
        let limit = self
            .kept
            .cfg
            .upload
            .files_per_tick
            .map_or(u32::MAX, NonZeroU32::get);
        let mut last = Did::Nothing;
        for _ in 0..limit {
            let plan = plan_upload(&self.engine.upload, self.staged());
            let witnesses = &self.engine.witnesses;
            let uploaded = upload_next(
                self.engine.upload,
                env.staging,
                env.remote,
                self.staged(),
                witnesses,
            )
            .await;
            match uploaded {
                Ok(uploaded) => {
                    if let Did::Diverged { from, .. } = uploaded.did {
                        return Some(self.diverged(env, uploaded.state, from, now).await);
                    }
                    self.set_upload(uploaded.state);
                    last = uploaded.did;
                }
                Err(failed) => {
                    self.set_upload(failed.state);
                    // One failed read says nothing about the staged bytes: the
                    // file is dropped only when a read on a later tick fails too.
                    if let (Fault::StagedLost, UploadPlan::Upload(key)) = (failed.fault, plan)
                        && self.kept.suspect_staged.replace(key) == Some(key)
                    {
                        if let Some(tick) = self.vouch_before_drop(Task::Upload, None, now) {
                            return Some(tick);
                        }
                        self.kept.suspect_staged = None;
                        mark!(Run16);
                        return Some(self.lose_staged(env, key.range().min(), now).await);
                    }
                    return Some(self.fault(Task::Upload, failed.fault, now));
                }
            }
            if matches!(last, Did::Put(_) | Did::AlreadyPut(_)) {
                self.kept.suspect_staged = None;
            }
            match last {
                Did::Nothing | Did::Diverged { .. } => break,
                Did::Listed { .. } => self.engine.maintain.stale = true,
                Did::Put(FileInfo { key, .. }) | Did::AlreadyPut(key) => {
                    if matches!(last, Did::Put(_)) {
                        mark!(Up1);
                    }
                    let at = listing_after_upload(now, &self.kept.cfg.maintenance);
                    self.engine.maintain.list_by(at);
                    let watch = self.engine.maintain.watch;
                    self.engine.maintain.watch = RegressionWatch::put(watch, key);
                }
            }
            if self.uploads_idle() {
                break;
            }
        }
        self.kept.tasks.get_mut(Task::Upload).succeed();
        Some(self.ran(Event::Uploaded(last), now))
    }

    /// A listing while running found the replica's object at `from` verifying
    /// and other than the staged file. If the staged file is still the bytes
    /// this process witnessed, the object is another writer's: this process
    /// uploads a key only from its witnessed copy and never takes an
    /// acknowledged id again, so nothing it wrote can differ from that copy.
    /// That is the stop for another writer, never a snapshot above its file,
    /// which would interleave the two histories; the stop's record leaves the
    /// contested id out, so a restart stops again while the object stands.
    /// A staged file that changed under the engine (a backup put back) says
    /// nothing about writers, and the divergence goes as before
    /// ([`Self::lose_staged`]). A boot's divergence is recovery's: it cannot
    /// tell an earlier run's upload from another writer's.
    async fn diverged<
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: ObjectStore,
        R: ObjectStore,
    >(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        state: UploadState,
        from: Txid,
        now: Timestamp,
    ) -> Tick {
        let key = FileKey::new(Level::ZERO, TxidRange::single(from));
        let token = self.engine.witnesses.get(&key).copied();
        let ours = match token {
            Some(token) => still_holds(env.staging, &token).await,
            None => Ok(false),
        };
        match ours {
            Ok(true) => self.fault(Task::Upload, Fault::RemoteAheadOfLocal, now),
            Ok(false) => {
                if let Some(tick) = self.vouch_before_drop(Task::Upload, Some(from), now) {
                    return tick;
                }
                self.set_upload(state);
                self.lose_staged(env, from, now).await
            }
            Err(fault) => self.fault(Task::Upload, fault, now),
        }
    }

    /// The staged file `from` is missing, does not verify, or differs from the
    /// replica's verifying object under the same key. It and every staged file
    /// after it are deleted, and the next sync is a snapshot after the newer of
    /// `from - 1` and the confirmed transaction: the replica holds everything
    /// before `from` (the upload task only uploads the confirmed transaction
    /// plus one), so the chain has no hole, and when it holds `from` itself
    /// (a divergence confirms it) the snapshot goes above it.
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
        self.engine.witnesses.drop_from(from);
        if let Err(fault) = drop_staged_from(env.staging, from).await {
            // The staged files from `from` on are still there, and the next
            // upload would take the one after the confirmed transaction as
            // following it. List first, from below `from`, so the listing
            // judges the divergence again and this runs again before anything
            // is uploaded; the acknowledged mark is kept apart.
            mark!(Up9);
            let confirmed = self.engine.upload.confirmed().min(from.prev());
            self.set_upload(UploadState::ListFirst { confirmed });
            return self.fault(Task::Upload, fault, now);
        }
        // Acknowledged ids from `from` on that the replica lost (a rollback)
        // are gone with their staged copies: they are never taken again, so
        // the uploads resume with the snapshot above them.
        let acknowledged = self.engine.acknowledged.filter(|ack| *ack >= from);
        if let (Some(ack), UploadState::At(confirmed)) = (acknowledged, self.engine.upload) {
            self.engine.upload = UploadState::At(confirmed.max(Some(ack)));
        }
        // The position is past `from`; a snapshot after it would leave a hole.
        let cause = SnapshotCause::NewestStagedFileUnreadable;
        self.engine.course = Course::Snapshot {
            staged: from.prev(),
            cause,
        };
        self.kept.tasks.get_mut(Task::Upload).succeed();
        self.ran(Event::Resnapshot(cause), now)
    }

    /// Deletes staged level-0 files the replica no longer needs from staging,
    /// oldest first, at most `MAX_DELETE` (W3). A failure waits on its own
    /// task and holds nothing else.
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
        let bound = self.prune_ready(now)?;
        let listing = match env.staging.list(Level::ZERO, None, PageLimit::MAX).await {
            Ok(listing) => listing,
            Err(err) => return Some(self.fault(Task::Prune, Fault::from_staging(&err), now)),
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
            && let Err(err) = crate::publish::remove_staged(env.staging, &keys).await
        {
            return Some(self.fault(Task::Prune, Fault::from_staging(&err), now));
        }
        self.kept.tasks.get_mut(Task::Prune).succeed();
        mark!(Up16);
        self.engine.pruned = Some(through);
        if keys.is_empty() {
            return None;
        }
        self.engine.maintain.passed_over = 0;
        let event = Event::Maintained {
            event: MaintainedEvent::Deleted(keys.len()),
            created_at_read: 0,
        };
        Some(self.ran(event, now))
    }

    /// The newest staged transaction a prune may delete through, when it is
    /// past what was pruned: confirmed at the replica, compacted into its
    /// level 1 (compaction reads staged inputs first), and below the newest staged file
    /// (boot's rule 9 compares the whole newest staged file with the WAL). `None` before the
    /// first maintenance listing.
    fn prune_bound(&self) -> Option<Txid> {
        let below_newest_staged_file = self.staged()?.prev()?;
        let bound = self
            .engine
            .upload
            .confirmed()?
            .min(self.engine.maintain.level_one?)
            .min(below_newest_staged_file);
        Some(bound).filter(|bound| self.engine.pruned < Some(*bound))
    }

    /// Lists the replica and runs what maintenance says is due, only once
    /// every staged file is published (so a snapshot's last transaction is at or below the
    /// published one).
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
        if !self.maintain_ready(now) {
            return None;
        }
        let inventory = match Inventory::load(env.remote).await {
            Ok(inventory) => inventory,
            Err(err) => return Some(self.fault(Task::Maintenance, inventory_fault(&err), now)),
        };
        // Uploads are idle, so everything this engine wrote is at or below
        // its position: a file past it is another writer's. In steady state
        // this listing is the first to see one, and nothing may be planned
        // from it (no compaction, snapshot or delete takes it as input).
        if let Some(tick) = self.listing_untrusted(env.remote, &inventory, now).await {
            return Some(tick);
        }
        if let Some(tick) = self.watch_unit(env.remote, &inventory, now).await {
            return Some(tick);
        }
        if self.engine.maintain.read_back_owed
            && let Some(tick) = self.read_back_unit(env.remote, &inventory, now).await
        {
            return Some(tick);
        }
        let published = self.engine.upload.confirmed();
        // The planner reads the kept restore point from its state: handed over here, where
        // it is read, and stored nowhere else.
        self.engine
            .maintain
            .state
            .set_kept_restore_point(self.kept.kept_restore_point());
        let decided = {
            let (cfg, state) = (&self.kept.cfg.maintenance, &mut self.engine.maintain.state);
            let mut step = |resolved: &ResolvedCreatedAt| {
                due(&inventory, now, cfg, published, state, resolved)
            };
            resolve(env.remote, &mut step).await
        };
        // A failed created-at time read leaves the listing owed: the next tick lists again.
        let decided = match decided {
            Ok(decided) => decided,
            Err(err) => {
                return Some(self.fault(Task::Maintenance, Fault::from_created_at(&err), now));
            }
        };
        self.engine.maintain.stale = false;
        self.engine.maintain.passed_over = 0;
        self.engine.maintain.snapshot_waits = false;
        self.engine.maintain.level_one = Level::new(1)
            .and_then(|level| inventory.level(level).last())
            .map(|info| info.key.range().max());
        let created_at_read = decided.created_at_read;
        match decided.value {
            Due::Wait { until, held } => {
                self.maintained(now);
                self.engine.maintain.until = until;
                let event = Event::Maintained {
                    event: MaintainedEvent::Waiting { held },
                    created_at_read,
                };
                Some(self.ran(event, now))
            }
            // A snapshot's last transaction must be at most what was published: with
            // uploads busy it waits, and the next listing with them.
            Due::Run(Job::Snapshot(_)) if !self.uploads_idle() => {
                self.engine.maintain.stale = true;
                self.engine.maintain.snapshot_waits = true;
                let event = Event::Maintained {
                    event: MaintainedEvent::Waiting { held: None },
                    created_at_read,
                };
                Some(self.ran(event, now))
            }
            Due::Run(job) => {
                // Whatever the job does, the next listing is what to plan from.
                self.engine.maintain.stale = true;
                Some(self.run_job(env, &job, now, created_at_read).await)
            }
        }
    }

    /// Everything this engine watched since its last maintenance listing must
    /// be listed (`crate::watch`). One that is not is read: gone, the replica
    /// lost files this process confirmed, and the uploader lists again (or
    /// the engine stops, as configured); found, the listing lags and nothing
    /// is planned from it, so the watch stands for the next listing. With nothing
    /// missing the watch moves to this listing.
    async fn watch_unit<R: ObjectStore>(
        &mut self,
        remote: &R,
        inventory: &Inventory,
        now: Timestamp,
    ) -> Option<Tick> {
        let watch = self.engine.maintain.watch;
        let Some(key) = watch.and_then(|watch| watch.missing(inventory)) else {
            let published = self.engine.upload.confirmed();
            self.engine.maintain.watch = RegressionWatch::after_listing(inventory, published);
            return None;
        };
        match remote.read(&key, 0, ReadLen::or_chunk(1)).await {
            Ok(_) => Some(self.list_again_later(now)),
            Err(StoreError::NotFound { .. }) => {
                Some(self.fault(Task::Upload, Fault::RemoteRegressed, now))
            }
            Err(err) => Some(self.fault(Task::Maintenance, Fault::from_remote(&err), now)),
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
        let (key, covered) = match read_back(remote, inventory).await {
            Ok(ReadBack::Sound) => {
                self.engine.maintain.read_back_owed = false;
                self.engine.maintain.suspect = None;
                return None;
            }
            Ok(ReadBack::Rotten { key, covered }) => (key, covered),
            Err(fault) => return Some(self.fault(Task::Maintenance, fault, now)),
        };
        // One failed read says nothing about the bytes: act only when a read on
        // a later tick fails too.
        if self.engine.maintain.suspect.replace(key) != Some(key) {
            return Some(self.list_again_later(now));
        }
        self.engine.maintain.suspect = None;
        if !covered {
            mark!(Maint4);
            return Some(self.fault(Task::Maintenance, Fault::RemoteCorrupt, now));
        }
        mark!(Maint4);
        // Watched no more, whether the delete goes through or not.
        self.engine.maintain.watch = RegressionWatch::forget(self.engine.maintain.watch, key);
        if let Err(err) = crate::publish::remove_one(remote, key, Why::RottenCovered).await {
            return Some(self.fault(Task::Maintenance, Fault::from_remote(&err), now));
        }
        // The listing no longer holds: the next tick lists and reads back again.
        self.engine.maintain.stale = true;
        self.maintained(now);
        let event = Event::Maintained {
            event: MaintainedEvent::Repaired(key),
            created_at_read: 0,
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
        created_at_read: u64,
    ) -> Tick {
        let outcome = match job {
            Job::Compact(compact) => run_compact(compact, env.staging, env.remote).await,
            Job::Delete(delete) => run_delete(delete, env.remote).await,
            Job::Snapshot(_) => return self.snapshot_job(env, job, now, created_at_read).await,
        };
        self.engine.maintain.state.record(now, job, &outcome);
        let event = match outcome {
            Outcome::Compacted { info, .. } => MaintainedEvent::Compacted(info),
            Outcome::Deleted { count } => MaintainedEvent::Deleted(count),
            Outcome::Replan { .. } => MaintainedEvent::Replan,
            Outcome::Failed(err) => {
                let waiter = self.job_waiter(job, now);
                return self.fault(waiter, Fault::from_maintain(&err), now);
            }
        };
        self.maintained(now);
        self.ran(
            Event::Maintained {
                event,
                created_at_read,
            },
            now,
        )
    }

    /// What ends a maintenance tick before anything is planned from `inventory`: a file
    /// past everything this engine staged or confirmed is another writer's, and nothing may
    /// be planned from that listing (no compaction, snapshot or delete takes it as input).
    /// In steady state this listing is the first to see one. A listing that lags a delete
    /// shows a file that is gone, so a read must find one before the engine stops; a
    /// listing a read contradicts plans nothing either, and is taken again later.
    async fn listing_untrusted<R: ObjectStore>(
        &mut self,
        remote: &R,
        inventory: &Inventory,
        now: Timestamp,
    ) -> Option<Tick> {
        let known = self.snapshot_after();
        if inventory.newest() > known {
            return Some(match past_known(remote, inventory, known).await {
                Ok(true) => {
                    mark!(Up10);
                    mark!(Maint10);
                    self.fault(Task::Maintenance, Fault::RemoteAheadOfLocal, now)
                }
                Ok(false) => self.list_again_later(now),
                Err(fault) => self.fault(Task::Maintenance, fault, now),
            });
        }
        self.listing_lags(remote, inventory, now).await
    }

    /// A listing that ends before what this engine confirmed either lags its
    /// uploads or shows a replica that lost them. Reads decide: when the
    /// confirmed key (or a key past it) is there, nothing is planned from
    /// the listing (a compaction would merge a shorter run and write it again
    /// later) and maintenance lists again later; when it is gone, the listing
    /// is the replica's and plans as before (retention holds on it), and the
    /// upload lane learns of the loss at its next listing.
    async fn listing_lags<R: ObjectStore>(
        &mut self,
        remote: &R,
        inventory: &Inventory,
        now: Timestamp,
    ) -> Option<Tick> {
        let published = self.engine.upload.confirmed();
        if inventory.newest() >= published || self.engine.maintain.gone == published {
            return None;
        }
        match probe_past(remote, inventory.newest(), published, None).await {
            Ok(found) if found.is_empty() => {
                self.engine.maintain.gone = published;
                None
            }
            Ok(_) => Some(self.list_again_later(now)),
            Err(fault) => Some(self.fault(Task::Maintenance, fault, now)),
        }
    }

    /// Maintenance plans nothing from this listing and lists again after the
    /// retry interval: a read contradicted it, or a failed read must fail
    /// again on a later tick before anything acts on it.
    fn list_again_later(&mut self, now: Timestamp) -> Tick {
        let again = now.saturating_add(self.kept.cfg.retry_interval);
        self.engine.maintain.stale = false;
        self.engine.maintain.until = Some(again);
        self.maintained(now);
        let event = Event::Maintained {
            event: MaintainedEvent::Waiting { held: None },
            created_at_read: 0,
        };
        self.ran(event, now)
    }

    /// A maintenance step went well. The task's degraded state goes unless a
    /// job's level is still waiting out its own backoff: that job has not
    /// succeeded, whatever else did, and the listing that finds it waiting
    /// (or another level's job) is not its success.
    fn maintained(&mut self, now: Timestamp) {
        let task = self.kept.tasks.get_mut(Task::Maintenance);
        if self.engine.maintain.state.backing_off(now) {
            task.clear_wait();
        } else {
            task.succeed();
        }
    }

    /// Where a failed job waits: its level's backoff when `MaintainState`
    /// keeps one (the other levels keep running), else the maintenance
    /// task, so a failing retention delete backs off instead of relisting
    /// at once. A level's wait is not the task's failure, so the task's wait
    /// resets; a degraded state stays.
    fn job_waiter(&mut self, job: &Job, now: Timestamp) -> Waiter {
        match self.engine.maintain.state.retry_at(job, now) {
            Some(until) => {
                self.kept.tasks.get_mut(Task::Maintenance).clear_wait();
                Waiter::Kept(until)
            }
            None => Waiter::Task(Task::Maintenance),
        }
    }

    /// A snapshot the store took: the next listing must show it, so a listing that hides it
    /// plans nothing (`crate::regression_watch`), and no second snapshot takes its key.
    fn snapshot_written(&mut self, info: FileInfo, now: Timestamp, created_at_read: u64) -> Tick {
        self.maintained(now);
        self.engine.maintain.watch = RegressionWatch::made(self.engine.maintain.watch, info.key);
        let event = MaintainedEvent::Snapshot(info);
        self.ran(
            Event::Maintained {
                event,
                created_at_read,
            },
            now,
        )
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
        created_at_read: u64,
    ) -> Tick {
        // The snapshot's step is the sync task's work and waits on its wait.
        if !self.kept.tasks.ready(Task::Sync, now) {
            self.wait_for_sync();
            let event = MaintainedEvent::Waiting { held: None };
            return self.ran(
                Event::Maintained {
                    event,
                    created_at_read,
                },
                now,
            );
        }
        let stepped = self.counted_step(env, now).await;
        let proof = match self.stepped(stepped, now) {
            Ok((0, Some(proof))) => proof,
            // A reset wal-index gives no proof until the next commit, and
            // nothing new is committed until then: the listing after that
            // commit's upload plans the snapshot again.
            Ok((0, None)) if self.reset().is_some() => {
                self.engine.maintain.stale = false;
                self.engine.maintain.until = None;
                let event = MaintainedEvent::Waiting { held: None };
                return self.ran(
                    Event::Maintained {
                        event,
                        created_at_read,
                    },
                    now,
                );
            }
            Ok((files, _)) => return self.ran(Event::Synced { files }, now),
            // A failed step waits on the sync task, and the listing with it:
            // the next is due when the step may run again, not at once.
            Err(Tick::Ran { event, .. }) => {
                self.wait_for_sync();
                return self.ran(event, now);
            }
            Err(tick @ (Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_))) => {
                return tick;
            }
        };
        let written = write_snapshot(
            &proof,
            &mut *env.wal,
            &mut *env.db,
            &mut self.engine.pages,
            env.remote,
            self.kept.clock.map_or(now, EngineClock::stamp_floor),
        )
        .await;
        self.engine
            .maintain
            .state
            .record_snapshot(now, written.as_ref().err().map(|failed| &failed.fault));
        match written {
            Ok(info) => self.snapshot_written(info, now, created_at_read),
            Err(failed) => self.snapshot_failed(env.remote, job, failed, now).await,
        }
    }

    /// A snapshot that did not publish. When the store took the commit and
    /// then said the object is not what was sent, it is listed, so it goes,
    /// as a rejected compaction output does.
    async fn snapshot_failed<R: ObjectStore>(
        &mut self,
        remote: &R,
        job: &Job,
        SnapshotFailed { fault, key }: SnapshotFailed,
        now: Timestamp,
    ) -> Tick {
        if fault.leaves_bad_snapshot()
            && crate::publish::remove_one(remote, key, Why::RejectedSnapshot)
                .await
                .is_err()
        {
            self.engine.maintain.state.distrust_snapshot(key);
        }
        let waiter = self.job_waiter(job, now);
        self.fault(waiter, fault, now)
    }

    /// Maintenance lists again when the sync task's wait ends, or after the
    /// next upload when it holds none (a recovery verdict made the next sync a
    /// snapshot, whose upload makes a listing due).
    fn wait_for_sync(&mut self) {
        self.engine.maintain.stale = false;
        self.engine.maintain.until = self.kept.tasks.get(Task::Sync).until();
    }

    fn running(&self) -> bool {
        self.kept.stop == Stop::No && self.booted()
    }

    /// Whether some task could run now on work it already knows of. A WAL
    /// change it does not know of is the caller's to wake on.
    fn ready_work(&self, now: Timestamp) -> bool {
        // Before the boot only the boot runs, on the sync task.
        if !self.booted() {
            return self.kept.tasks.ready(Task::Sync, now);
        }
        let sync = self.kept.tasks.ready(Task::Sync, now)
            && self.staging_has_room()
            && (self.pending().is_some()
                || self.more_wal()
                || (self.running() && self.checkpoint_decision(now).is_some()));
        sync || self.upload_ready(now)
            || self.prune_ready(now).is_some()
            || self.maintain_ready(now)
    }

    /// Whether the upload task has a staged file to send and may run. The
    /// one statement of it: the task's unit and the next wake both ask here.
    fn upload_ready(&self, now: Timestamp) -> bool {
        self.kept.tasks.ready(Task::Upload, now) && !self.uploads_idle()
    }

    /// The transaction the prune task may delete through, when it may run.
    fn prune_ready(&self, now: Timestamp) -> Option<Txid> {
        self.prune_bound()
            .filter(|_| self.running() && self.kept.tasks.ready(Task::Prune, now))
    }

    /// Whether maintenance may list the replica now: when a listing could change what is
    /// due, and uploads are idle or have had their [`state::FAIR_SHARE`] (unless the last
    /// listing's snapshot waits for idle uploads).
    fn maintain_ready(&self, now: Timestamp) -> bool {
        let maintain = &self.engine.maintain;
        let turn = self.uploads_idle() || (self.fair_turn() && !maintain.snapshot_waits);
        self.running()
            && self.kept.tasks.ready(Task::Maintenance, now)
            && turn
            && maintain.wants_listing(now)
    }

    /// Whether prune and maintenance take the next unit ahead of uploads.
    const fn fair_turn(&self) -> bool {
        self.engine.maintain.passed_over >= state::FAIR_SHARE
    }

    /// Every deadline a task holds: retry waits, the checkpoint gate, the
    /// maintenance schedule (whose `Due::Wait` includes each level's backoff)
    /// and the drain's end. Tasks a draining actor does not run hold none.
    fn deadlines(&self) -> impl Iterator<Item = Timestamp> {
        let running = self.running();
        let task = |task: Task| self.kept.tasks.get(task).until();
        let deadline = self.drain_deadline();
        let only_running = [
            task(Task::Checkpoint),
            task(Task::Prune),
            task(Task::Maintenance),
            self.engine.gate.until(),
            self.engine.maintain.until,
            self.kept.ahead.map(ahead::kept_until),
        ];
        [task(Task::Sync), task(Task::Upload), deadline]
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
        let schedule = next_wake(&self.engine.checkpoint, &self.kept.cfg.checkpoint)
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
    newest_staged: Option<NewestStagedFile>,
    /// What recovery makes of it.
    recovered: Recovered,
    /// The replica's page size, as far as boot could tell.
    page_size: Option<PageSize>,
    /// The replica's newest transaction, when the boot refused to build on
    /// files above the newest staged file that nothing vouched for.
    refused_through: Option<Txid>,
}

/// Why a boot step did not finish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootStep {
    /// A fault; the boot runs again by its policy.
    Fault(Fault),
    /// The boot would drop staged files the replica holds, and needs this
    /// takeover durable first.
    Vouch(TakeoverRecord),
    /// The replica holds another writer's file through this transaction:
    /// the boot stops with `OtherWriter`.
    Refused(Txid),
}

impl From<Fault> for BootStep {
    fn from(fault: Fault) -> Self {
        Self::Fault(fault)
    }
}

/// Whether a boot needs replica files above the local position vouched for
/// (`crate::takeover`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Takeover {
    /// No: the configuration continues past another writer.
    Off,
    /// The live WAL's word, or this takeover record covering them.
    Vouch(Option<TakeoverRecord>),
}

/// What a boot needs before it builds on replica files above the local
/// position: the newest staged transaction before the boot dropped any of
/// its own staged files (a drop of this engine's own files never makes the
/// replica's foreign).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BootGate {
    takeover: Takeover,
    local: Option<Txid>,
}

impl BootGate {
    /// Whether the replica's newest transaction `tip` is above the local
    /// position.
    fn above(self, tip: Txid) -> bool {
        self.local < Some(tip)
    }

    /// Before the boot drops staged files: the takeover that keeps what
    /// staging vouched for once they are gone, unless the held record covers
    /// it already. That is `local`, capped at the replica's newest key: the
    /// confirmed `tip`, or a newer level-0 key judged torn (it may be this
    /// engine's own upload, read wrong), listed or found by a read past a
    /// listing that lags. Pending files above every replica key vouch for
    /// nothing there, so one put back from a backup never raises it.
    async fn vouch<R: ObjectStore>(self, remote: &R, tip: Option<Txid>) -> Result<(), BootStep> {
        let Takeover::Vouch(record) = self.takeover else {
            return Ok(());
        };
        let covers = |txid: Txid| record.is_some_and(|record| record.covers(txid));
        let Some(local) = self.local.filter(|local| !covers(*local)) else {
            return Ok(());
        };
        let listed = newest_replica_level_zero(remote)
            .await?
            .map(|info| info.key.range().max());
        // A listing may lag the replica's writes: keys past it are read for, through the
        // staged position (any staged file may be on the replica), as boot reads past every
        // listing. A record capped short of a key the listing hid refuses this engine's own
        // upload at a later boot.
        let past = probe_past(remote, listed, tip, Some(local)).await?;
        let found = past.last().map(|key| key.range().max());
        let replica = tip.max(listed).max(found);
        match replica.map(|replica| replica.min(local)) {
            Some(through) if !covers(through) => {
                Err(BootStep::Vouch(TakeoverRecord::through(through)))
            }
            Some(_) | None => Ok(()),
        }
    }

    /// Whether replica files through `tip` that the WAL did not vouch for
    /// stop the boot.
    fn refuses(self, tip: Txid) -> bool {
        match self.takeover {
            Takeover::Off => false,
            Takeover::Vouch(record) => !record.is_some_and(|record| record.covers(tip)),
        }
    }
}

/// The newest staged file (or, with none staged, the replica's newest
/// level-0 file), the replica's page size, and what recovery makes of them.
/// A WAL whose page size is not the replica's stops here, before
/// anything is staged.
/// Removes the scratch files a killed process's uploads left, in staging and in the replica.
/// A crash loop otherwise leaves one per boot, each as large as the file it was writing; at
/// boot nothing of this engine is uploading yet. A failure is the directory's, which every
/// upload and prune needs too.
async fn remove_scratch<L: ObjectStore, R: ObjectStore>(
    staging: &L,
    remote: &R,
) -> Result<(), Fault> {
    staging
        .remove_scratch()
        .await
        .map_err(|err| Fault::from_staging(&err))?;
    remote
        .remove_scratch()
        .await
        .map_err(|err| Fault::from_remote(&err))?;
    Ok(())
}

/// What boot's listing and sweep deleted from staging, from which transaction on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Dropped {
    /// Nothing.
    Nothing,
    /// A staged file that failed verification twice, and every one after it.
    Unreadable(Txid),
    /// The staged files from the replica's newest level-0 transaction on:
    /// the replica holds another verifying file under that key.
    Diverged(Txid),
}

/// The journal as boot reads it, with the writer that continues from it, or `None` with no
/// journal lent.
fn read_journal(
    file: Option<&mut (dyn JournalFile + Send + '_)>,
) -> Option<(JournalWriter, lsr_journal::Parsed)> {
    file.map(JournalWriter::open)
}

impl Actor {
    /// Takes what boot read of the journal: the report (a missing journal is a first run, and
    /// reports nothing, only while staging and the replica are both empty), the writer, and a
    /// recorded unproven-WAL debt, which a restart never forgives (`unproven`).
    fn adopt_journal(
        &mut self,
        journal: Option<(JournalWriter, lsr_journal::Parsed)>,
        newest_staged: Option<&NewestStagedFile>,
        upload: &UploadState,
    ) {
        let fresh = newest_staged.is_none() && upload.confirmed().is_none();
        let Some((writer, parsed)) = journal else {
            self.kept.journal = None;
            self.kept.writer = None;
            self.kept.journal_read = None;
            return;
        };
        self.kept.journal = judge(&parsed, fresh);
        self.kept.journal_read = match parsed.verdict {
            lsr_journal::Verdict::Read { journal, .. } => Some(journal),
            lsr_journal::Verdict::Newer { .. }
            | lsr_journal::Verdict::Missing
            | lsr_journal::Verdict::Unreadable(_) => None,
        };
        if let Some(read) = self.kept.journal_read {
            self.kept.unproven = self.kept.unproven.with_recorded(read.unproven);
            self.adopt_ahead(read.ahead);
        }
        self.kept.writer = Some(writer);
    }

    /// Writes the journal when what it holds differs from what the engine holds, at the end of
    /// every tick: the facts are a function of the engine's state, never a step to remember.
    /// A process that neither read nor wrote a record writes one, so the next boot finds a
    /// journal and a missing one means it was lost. A failed write is tried again next tick;
    /// the engine proceeds as with no journal.
    fn reconcile_journal(
        &mut self,
        file: Option<&mut (dyn JournalFile + Send + '_)>,
        tick: Tick,
        now: Timestamp,
    ) -> Tick {
        let (Some(file), Some(writer)) = (file, self.kept.writer.as_mut()) else {
            return tick;
        };
        let held = writer.durable().copied().or(self.kept.journal_read);
        let wanted = (self.kept.unproven.recorded(), self.kept.ahead);
        if held.map(|held| (held.unproven, held.ahead)) == Some(wanted) {
            // Nothing the engine holds is missing from the journal: nothing lags it.
            self.kept.journal_write = Ok(());
            return tick;
        }
        let record = lsr_journal::Journal {
            unproven: wanted.0,
            ahead: wanted.1,
            ..held.unwrap_or(lsr_journal::Journal::ABSENT)
        };
        match writer.write(file, &record) {
            Ok(()) => self.kept.journal_write = Ok(()),
            Err(JournalWriteError::Unwritable(kind)) => self.kept.journal_write = Err(kind),
            Err(JournalWriteError::Bug(fault)) => return self.fault(Task::Sync, fault, now),
        }
        tick
    }
}

/// Boot's scratch removal, listing and staging sweep: the upload state to start from, and
/// what was deleted.
///
/// A staged file that fails verification is deleted, with all after it, only
/// when it is `suspect`, the file a read on an earlier tick failed on;
/// otherwise it becomes the suspect and the boot fails with `StagedLost`, to
/// read it again on the next try.
///
/// A replica newest level-0 file that verifies and differs from the staged
/// copy under the same key is another writer's unless a takeover record
/// covers it: this engine uploads only from witnessed, synced staged copies
/// and keeps a takeover record before deleting its own, so without an
/// operator's hand a boot divergence is a second writer (the contested id).
/// The boot refuses it before touching staging (`BootStep::Refused`).
async fn boot_listing<L: ObjectStore, R: ObjectStore>(
    staging: &L,
    remote: &R,
    suspect: &mut Option<FileKey>,
    takeover: Takeover,
) -> Result<Listed, BootStep> {
    remove_scratch(staging, remote).await?;
    // Where staging stood before this boot dropped anything of its own: the
    // position replica files must be vouched for above.
    let local = newest_level_zero(staging)
        .await?
        .map(|info| info.key.range().max());
    // At boot a replica ahead of the local newest staged file is `recover`'s to judge (a
    // snapshot after it), not the uploader's (a stop), so the listing names no newest staged file.
    let start = UploadState::ListFirst { confirmed: None };
    let none = Witnesses::new();
    let listed = upload_next(start, staging, remote, Some(Txid::MAX), &none).await;
    let (upload, diverged) = match listed {
        Ok(Uploaded {
            state,
            did: Did::Diverged { from, .. },
        }) => (state, Some(from)),
        Ok(listed) => (listed.state, None),
        Err(failed) => return Err(failed.fault.into()),
    };
    let tip = upload.confirmed();
    let gate = BootGate { takeover, local };
    // The staged files from `from` on cannot continue the replica: unless
    // the replica's object is another writer's, they go, and recovery
    // snapshots above what the replica holds.
    if let Some(from) = diverged {
        mark!(Boot13);
        if let Some(tip) = tip.filter(|tip| gate.refuses(*tip)) {
            return Err(BootStep::Refused(tip));
        }
        gate.vouch(remote, tip).await?;
        drop_staged_from(staging, from).await?;
    }
    let lost = match sweep_staged(staging, tip, *suspect).await? {
        Swept::Clean => None,
        Swept::Suspect(key) => {
            *suspect = Some(key);
            return Err(Fault::StagedLost.into());
        }
        Swept::Condemned(from) => {
            gate.vouch(remote, tip).await?;
            drop_staged_from(staging, from).await?;
            Some(from)
        }
    };
    *suspect = None;
    let dropped = match (diverged, lost) {
        (Some(from), _) => Dropped::Diverged(from),
        (None, Some(from)) => Dropped::Unreadable(from),
        (None, None) => Dropped::Nothing,
    };
    Ok(Listed {
        upload,
        dropped,
        gate,
    })
}

/// What boot's listing leaves for the rest of the boot.
struct Listed {
    /// The upload state to start from.
    upload: UploadState,
    /// What the listing or the sweep deleted from staging.
    dropped: Dropped,
    /// What replica files above the local position need.
    gate: BootGate,
}

/// Boot's witness pass over the pending staged files (`crate::witness`).
/// The first file the WAL does not vouch for is deleted with every one
/// after it, once a second pass agrees: one read never condemns a file. Two
/// passes that disagree retry the boot.
///
/// The listing leaves a torn newest key unconfirmed, for its staged copy to
/// be uploaded over it. When that copy is the file refused and a read finds
/// the key on the replica (a read, since a listing may lag), the key stays as
/// the replica holds it and is confirmed: new content never takes a
/// transaction the replica holds, so the snapshot goes above it.
async fn boot_witness<W: WalSource, P: PageSource, C, L: ObjectStore, R: ObjectStore>(
    env: &mut Env<'_, W, P, C, L, R>,
    mut reader: Option<&mut WalReader<W::Wal>>,
    committed: CommittedRead,
    upload: UploadState,
    gate: BootGate,
) -> Result<(Pending, UploadState), BootStep> {
    let confirmed = upload.confirmed();
    let first = witness_once(env, reader.as_deref_mut(), committed, confirmed).await?;
    let Some(from) = first.refused else {
        return Ok((first, upload));
    };
    let second = witness_once(env, reader, committed, confirmed).await?;
    if second.refused != Some(from) {
        mark!(Boot12);
        return Err(Fault::LocalIo.into());
    }
    mark!(Boot11);
    gate.vouch(env.remote, confirmed).await?;
    drop_staged_from(env.staging, from).await?;
    let next = confirmed.map_or(Some(Txid::MIN), Txid::next);
    let key = FileKey::new(Level::ZERO, TxidRange::single(from));
    // A read, not a listing: a listing may lag the replica's writes, and one that leaves the
    // key out would let new content take a transaction the replica holds.
    let held = crate::probe::exists(env.remote, &key)
        .await
        .map_err(|err| Fault::from_remote(&err))?;
    if next == Some(from) && held {
        return Ok((first, UploadState::At(Some(from))));
    }
    Ok((first, upload))
}

async fn witness_once<W: WalSource, P: PageSource, C, L: ObjectStore, R: ObjectStore>(
    env: &mut Env<'_, W, P, C, L, R>,
    reader: Option<&mut WalReader<W::Wal>>,
    committed: CommittedRead,
    confirmed: Option<Txid>,
) -> Result<Pending, Fault> {
    let live = reader.map(|reader| LiveDb {
        wal: BootWal {
            reader,
            committed: committed.end(),
        },
        db: &mut *env.db,
    });
    witness_pending(env.staging, env.remote, live, confirmed).await
}

/// The boot's reader bounded by its committed end.
fn boot_wal_of<R>(
    reader: Option<&mut WalReader<R>>,
    committed: CommittedEnd,
) -> Option<BootWal<'_, R>> {
    reader.map(|reader| BootWal { reader, committed })
}

/// What the boot's judges found: the witness pass, the upload state it leaves, and
/// recovery.
struct Judged {
    pending: Pending,
    upload: UploadState,
    booted: Booted,
}

/// The page table and the quiet files a boot starts with. Only a boot that continues seeds
/// the table, from the staged files boot already verified: a snapshot records every page it
/// reads.
async fn boot_seed<L: ObjectStore>(
    staging: &L,
    course: &Course,
    confirmed: Option<Txid>,
) -> Result<(PageChecksumTable, BTreeSet<Txid>), Fault> {
    match course {
        Course::At {
            position,
            snapshot: None,
            ..
        } => {
            let seeded = seeded(staging, confirmed, position.commit()).await?;
            Ok((seeded.table, seeded.quiet))
        }
        Course::At {
            snapshot: Some(_), ..
        }
        | Course::Snapshot { .. }
        | Course::Unbooted => Ok((PageChecksumTable::new(), BTreeSet::new())),
    }
}

/// The witness pass and recovery, on one wal-index read and one reader opened after it:
/// both judge the same WAL, and no range past that committed end.
async fn boot_judge<W: WalSource, P: PageSource, C, L: ObjectStore, R: ObjectStore>(
    env: &mut Env<'_, W, P, C, L, R>,
    upload: UploadState,
    gate: BootGate,
) -> Result<Judged, BootStep> {
    let committed = env.wal.committed().map_err(Fault::from_source)?;
    let mut reader = open_reader(env.wal)?;
    let (pending, upload) = boot_witness(env, reader.as_mut(), committed, upload, gate).await?;
    let booted = boot_recover(env, reader.as_mut(), committed, upload.confirmed(), gate).await?;
    Ok(Judged {
        pending,
        upload,
        booted,
    })
}

/// Recovery at boot. When the replica holds transactions past the newest
/// staged file (or anything, with nothing staged), its newest level-0 file is
/// read and checked against the live WAL: a WAL that still holds its range
/// with the same pages (`recover` continues from it) vouches for it, and the
/// engine continues from there. Otherwise, if those files are above where
/// staging stood when the boot began, nothing local vouches for them, and the
/// boot stops with `OtherWriter` unless the gate is off or a takeover record
/// covers them; then the verdict is the one recovery gives without the gate,
/// a snapshot above them.
async fn boot_recover<W: WalSource, P, C, L: ObjectStore, R: ObjectStore>(
    env: &mut Env<'_, W, P, C, L, R>,
    mut reader: Option<&mut WalReader<W::Wal>>,
    committed: CommittedRead,
    confirmed: Option<Txid>,
    gate: BootGate,
) -> Result<Booted, Fault> {
    let (staging, remote) = (env.staging, env.remote);
    let end = committed.end();
    let staged = NewestStagedFile::load(staging, boot_wal_of(reader.as_deref_mut(), end)).await?;
    // The replica's newest level-0 file stands in for an empty staging
    // directory (a fault reading it is the boot's), and is read for the WAL's
    // word when it is above the local position (one that does not decode
    // gets none).
    let behind = confirmed.filter(|tip| staged.as_ref().map(NewestStagedFile::txid) < Some(*tip));
    let wanted = behind.filter(|tip| staged.is_none() || gate.above(*tip));
    let tip_file = match wanted {
        Some(tip) => {
            if staged.is_none() {
                mark!(Boot2);
            }
            let wal = boot_wal_of(reader.as_deref_mut(), end);
            match NewestStagedFile::load_remote(remote, wal, tip).await {
                Ok(file) => file,
                Err(Fault::RemoteCorrupt) if staged.is_some() => None,
                Err(fault) => return Err(fault),
            }
        }
        None => None,
    };
    let view = wal_view(env.wal, reader.as_deref(), committed)?;
    let vouched = tip_file.as_ref().is_some_and(|file| {
        let verdict = recover(Some(PositionSource::Boot(file)), &view, confirmed);
        matches!(verdict, Recovered::Continue(_) | Recovered::WalRestarted(_))
    });
    let unvouched = confirmed.filter(|tip| !vouched && gate.above(*tip));
    let newest_staged = if vouched {
        tip_file
    } else {
        staged.or(tip_file)
    };
    let page_size = prefix_page_size(newest_staged.as_ref(), confirmed, remote).await?;
    let now = view.header.map(|header| header.page_size());
    let refused_through = unvouched.filter(|tip| gate.refuses(*tip));
    let recovered = match (
        page_size.zip(now).filter(|(was, now)| was != now),
        refused_through,
    ) {
        (Some((was, now)), _) => {
            mark!(Boot15);
            Recovered::Stop(StopCause::PageSizeChanged { was, now })
        }
        (None, Some(_)) => Recovered::Stop(StopCause::OtherWriter),
        (None, None) => recover(
            newest_staged.as_ref().map(PositionSource::Boot),
            &view,
            confirmed,
        ),
    };
    Ok(Booted {
        newest_staged,
        recovered,
        page_size,
        refused_through,
    })
}

/// The page size the next file must have: the newest staged file's, unless
/// the replica holds a later transaction than that file (staging is behind
/// it, or a staging backup was put back) or the staged file names none; then
/// the replica's newest file's, since the next file goes on its prefix. A newest
/// staged file unusable as a position names nothing: it can never be
/// uploaded (it gets no witness), so its page size constrains no file.
async fn prefix_page_size<R: ObjectStore>(
    newest_staged: Option<&NewestStagedFile>,
    confirmed: Option<Txid>,
    remote: &R,
) -> Result<Option<PageSize>, Fault> {
    let staged = match newest_staged {
        Some(NewestStagedFile::Readable { page_size, .. }) => Some(*page_size),
        Some(NewestStagedFile::Unreadable { .. }) | None => None,
    };
    let behind = confirmed > newest_staged.map(NewestStagedFile::txid);
    if confirmed.is_some() && (behind || staged.is_none()) {
        return Ok(replica_page_size(remote).await?.or(staged));
    }
    Ok(staged)
}

/// Headers a boot reads, newest file first, for the replica's page size before it calls the
/// replica corrupt.
const PAGE_SIZE_HEADERS: usize = 8;

/// The page size in the header of the replica's newest file whose header decodes; `None`
/// when the replica is empty. Every file of a prefix has one page size, so any header answers
/// for all of them. A header that does not decode (a rotten object, or a read that returned
/// other bytes) says nothing, so the next newest file is read; when none of the newest
/// [`PAGE_SIZE_HEADERS`] decodes, the replica is corrupt as far as a boot can tell. Taking no
/// answer for no constraint let a boot snapshot a new page size onto the prefix.
async fn replica_page_size<R: ObjectStore>(remote: &R) -> Result<Option<PageSize>, Fault> {
    let inventory = Inventory::load(remote)
        .await
        .map_err(|err| inventory_fault(&err))?;
    let mut files: Vec<&FileInfo> = inventory.files().iter().collect();
    if files.is_empty() {
        return Ok(None);
    }
    files.sort_by_key(|file| std::cmp::Reverse(file.key.range().max()));
    for file in files.into_iter().take(PAGE_SIZE_HEADERS) {
        if let Some(header) = read_header(remote, &file.key, Fault::from_remote).await? {
            return Ok(Some(header.page_size()));
        }
    }
    mark!(Boot14);
    Err(Fault::RemoteCorrupt)
}

/// Why a boot's first sync is a snapshot.
///
/// Staged files that diverged from the replica's newest file are a snapshot
/// whatever the WAL says: the replica holds other content under that
/// transaction, and the next file must go above it. A staged file deleted as
/// unreadable, or one the WAL did not vouch for (`refused`), is staged again
/// from the WAL when recovery continues: recovery judged the newest file left
/// against the live WAL, so the WAL holds every frame after it. A deleted
/// file the replica already holds leaves staging behind the replica, which
/// recovery answers with a snapshot (`BehindRemote`), so continuing never
/// stages new content under a transaction the replica shows. When recovery
/// cannot continue, the snapshot is named for what it replaces.
/// The boot's recovery proved the staged position: the engine continues from it.
/// The course a boot takes from where recovery left it, the snapshot cause the
/// boot found, and an operator's request made before it.
fn boot_course(
    from: Result<Position, SnapshotCause>,
    cause: Option<SnapshotCause>,
    asked: Option<SnapshotCause>,
    staged: Option<Txid>,
) -> Course {
    // A snapshot the boot needs answers an operator's request made before
    // it: every cause a boot finds clears the table of page checksums
    // (`snapshot_clears`), which a requested snapshot alone would trust.
    debug_assert!(cause.as_ref().is_none_or(snapshot_clears), "{cause:?}");
    let pending = cause.or(asked);
    match (from, pending) {
        (Ok(position), snapshot) => Course::At {
            position,
            tail: Tail::Synced,
            snapshot,
        },
        (Err(found), pending) => Course::Snapshot {
            staged,
            cause: pending.unwrap_or(found),
        },
    }
}

fn boot_continues(position: Position) -> Result<Position, SnapshotCause> {
    mark!(Boot3);
    Ok(position)
}

const fn boot_cause(
    dropped: Dropped,
    refused: Option<Txid>,
    recovered: Option<SnapshotCause>,
) -> Option<SnapshotCause> {
    match (dropped, refused, recovered) {
        (Dropped::Diverged(_), _, _) | (Dropped::Unreadable(_), _, Some(_)) => {
            Some(SnapshotCause::NewestStagedFileUnreadable)
        }
        (Dropped::Nothing, Some(from), Some(_)) => Some(SnapshotCause::StagedUnverified { from }),
        (Dropped::Unreadable(_) | Dropped::Nothing, _, recovered) => recovered,
    }
}

/// A recovery verdict as the step to run, or as what ends the tick.
const fn step_of(recovered: Recovered) -> Result<Step, Stepped> {
    match recovered {
        Recovered::Continue(position) => Ok(Step::Incremental(position)),
        Recovered::WalRestarted(position) => Ok(Step::NewGeneration(position)),
        Recovered::Snapshot { cause, .. } => Err(Stepped::Resnapshot(cause)),
        Recovered::Stop(cause) => Err(Stepped::Stop(cause)),
    }
}

#[cfg(test)]
mod tests;

/// The journal for a borrow shorter than the one it was lent for: `Option::as_deref_mut`
/// keeps the trait object's own lifetime, which ties a reborrowed [`Env`] to the outer one.
fn shorter<'b>(
    journal: &'b mut Option<&mut (dyn JournalFile + Send)>,
) -> Option<&'b mut (dyn JournalFile + Send)> {
    match journal {
        Some(journal) => Some(&mut **journal),
        None => None,
    }
}
