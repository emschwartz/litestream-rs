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

mod apart;
mod clock;
mod lag;
mod local;
mod once;
mod once_retention;
mod parts;
mod short;
mod state;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{PageSize, Timestamp, Txid, TxidRange};
use lsr_wal::{CommittedEnd, OpenError, WalReader};

use clock::EngineClock;
pub use once::{FinalSnapshot, FinalSnapshotFailure};
pub use once_retention::FinalRetention;
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
use crate::fault::role::{Replica, Role, Staging};
use crate::fault::{
    Action, Backoff, Clamped, Fault, Next, OtherWriter, PersistentCause, Policy, Responses,
    STALLED_AFTER, SnapshotCause, SnapshotCounts, Stall, StopCause, backoff_wait, policy,
};
use crate::identity::{self, Identity};
use crate::inventory::Inventory;
use crate::layout::{FileInfo, FileKey, Level};
use crate::maintain::{
    Due, Job, MAX_DELETE, MaintainError, Maintenance, Outcome, ReadBack, RetentionHold, due_full,
    listing_after_upload, read_back, run_compaction, run_delete,
};
use crate::pages::{PageChecksumTable, Seeding, snapshot_clears};
use crate::position::{Position, SyncedThrough, WalFileLen};
use crate::publish::Why;
use crate::recover::{
    BootWal, FrameCheck, Miss, NewestStagedFile, PositionSource, Probes, RecoverEvent, Recovered,
    Scanned, Verdict, WalView, newest_level_zero, newest_replica_level_zero, read_header,
    recover_traced,
};
use crate::snapshot::{SnapshotFailed, read_live, write_snapshot};
use crate::source::{CheckpointMode, Checkpointer, CommittedRead, PageSource, WalSource};
use crate::store::{Denied, ObjectStore, PageLimit, StoreError};
use crate::sync::{Step, StepEnd, SyncConfig, Synced, sync_step, wal_barrier};
use crate::takeover::TakeoverRecord;
use crate::upload::{
    Did, Guard, UploadConfig, UploadFailed, UploadPlan, UploadState, Uploaded, drop_staged_from,
    inventory_fault, plan_upload, upload_next, upload_next_in,
};
use crate::verify::LivePages;
use crate::witness::{Bad, Base, Scan, Witnessed, Witnesses, scan, still_holds};

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
    /// Told of long work inside the tick before it starts, so the driver can say what holds the
    /// start while the tick runs ([`Event::CheckingIdentity`]); the tick goes on whatever it
    /// does. A notice changes nothing the engine decides or reads.
    pub notice: &'a mut dyn FnMut(&Event),
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
    /// A step that writes to the database waited instead of running: the replicator holds the
    /// application's writes ([`Fault::WritesHeld`]). Not a failure: nothing is counted, and
    /// the task never stalls or degrades for it.
    WritesHeld {
        /// When the task tries again.
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
    /// A boot with a brand-new meta directory found replica files through `tip` that nothing
    /// local vouches for: before it decides, it restores the replica's state at `tip` and
    /// compares it with the database, page by page (`crate::identity`), which takes about a
    /// restore's time. Never a tick's event: the boot hands it to [`Env::notice`] as the check
    /// starts, in the tick that runs it, and the start is held through it
    /// (`docs/decisions/D201.md`).
    CheckingIdentity {
        /// The replica's newest transaction, the state compared.
        tip: Txid,
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

/// What `recover` reads of the WAL besides the reader. `len` is the file's length read by the
/// caller, before the reads it goes with: a boot awaits the store between its reads, and an
/// application's TRUNCATE checkpoint that lands in between would otherwise show a file shorter
/// than the header it names, which recovery takes for a power loss.
fn wal_view<R: std::io::Read + std::io::Seek>(
    len: Option<WalFileLen>,
    reader: Option<&WalReader<R>>,
    committed: CommittedRead,
) -> WalView {
    WalView {
        header: reader.map(|reader| *reader.header()),
        len,
        committed: Some(committed),
        epoch: committed.epoch(),
    }
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

    /// A takeover record this engine spent: a put above the record's
    /// transaction is confirmed on the replica (the boot's snapshot is the
    /// first), so the record has done its work and must not vouch for a file
    /// another writer puts at one of its ids later. The driver deletes the
    /// durable record, then hands it back with [`Actor::forget_takeover`];
    /// until then it asks again every tick.
    #[must_use]
    pub const fn takeover_spent(&self) -> Option<TakeoverRecord> {
        self.kept.spent_takeover
    }

    /// The driver deleted the durable copy of `record`, a spent takeover
    /// ([`Actor::takeover_spent`]).
    pub fn forget_takeover(&mut self, record: TakeoverRecord) {
        if self.kept.spent_takeover == Some(record) {
            self.kept.spent_takeover = None;
        }
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

    /// Counts the event a recovery verdict names, and hands on its decision.
    fn noted(&mut self, verdict: Verdict) -> Recovered {
        match verdict.event {
            Some(RecoverEvent::ReadLockViolated { .. }) => {
                self.kept.read_lock_violations = self.kept.read_lock_violations.saturating_add(1);
            }
            // Its snapshot cause carries what it found.
            Some(RecoverEvent::LocalRewound { .. }) | None => {}
        }
        verdict.recovered
    }

    /// How many recoveries found a commit abandoned after the read lock was taken
    /// (`RecoverEvent::ReadLockViolated`): a broken lock or a bug in the continuity proof.
    /// The snapshot each one takes is named `GenerationChanged`, so this count is the only
    /// place the violation shows.
    #[must_use]
    pub const fn read_lock_violations(&self) -> u64 {
        self.kept.read_lock_violations
    }

    /// Whether the replica refused the boot's sweep of its leftover scratch files and open
    /// uploads for want of permission, so the sweep was skipped for this process. Replication
    /// is unaffected; what a killed process left open stays until the bucket's own lifecycle
    /// rule or an operator removes it. Go sweeps nothing, and its documented S3 policy
    /// allows neither listing nor aborting multipart uploads.
    #[must_use]
    pub const fn scratch_sweep_denied(&self) -> bool {
        self.kept.scratch_sweep_denied
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
            // A local course takes no snapshot: an operator's request waits for the boot.
            Course::Local(_) | Course::Snapshot { .. } | Course::At { .. } => {
                self.engine.course.pending()
            }
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
            self.engine.maintain.remembered.owe_full();
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
    pub async fn tick<W, P, C, L, R>(&mut self, env: Env<'_, W, P, C, L, R>, now: Timestamp) -> Tick
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
        // From here on each store is reached as its role: an error from it is
        // classified as that store's, never the other's (`fault::role`).
        let (staging, remote) = (Staging(env.staging), Replica(env.remote));
        // Each checkpoint asked of SQLite is counted (`crate::counts`).
        let mut ck = Counted::new(&mut *env.ck, env.clock);
        let mut roles = Env {
            wal: &mut *env.wal,
            db: &mut *env.db,
            ck: &mut ck,
            staging: &staging,
            remote: &remote,
            clock: env.clock,
            notice: &mut *env.notice,
        };
        // Boxed: the tick's state machine is the largest future the replicator thread polls,
        // and on the heap it is not part of every poll frame below `block_on`, which in a
        // debug build is what the half-stack headroom gate measures (`lsr_replica::stack`).
        let tick = Box::pin(self.tick_at(&mut roles, now)).await;
        self.kept.work.add_checkpoints(ck.totals());
        self.observe_lag(env.wal, now);
        let tick = self.wake_for_probe(tick);
        self.wake_on_wall(tick)
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
        L: Role,
        R: Role,
    {
        self.expire(now);
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
            // The 15-minute backstop for a whole listing runs on engine time, which holds and
            // then slews after a step back: the next pass lists everything rather than wait.
            self.engine.maintain.remembered.owe_full();
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
        // What the engine remembers of the replica may be what the fault is about.
        self.engine.maintain.remembered.forget();
        let waiter = waiter.into();
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
            Policy::Wait => {
                // Only a trip holds a judged start, and nothing writes before the boot decides
                // (`docs/decisions/D201.md`): the hold lasts, so the task waits.
                let until = self.hold(waiter, now);
                self.ran(Event::WritesHeld { until }, now)
            }
        }
    }

    /// Waits one retry interval without counting a failure ([`Policy::Wait`]); returns when
    /// the wait ends.
    fn hold(&mut self, waiter: Waiter, now: Timestamp) -> Timestamp {
        match waiter {
            Waiter::Kept(until) => until,
            Waiter::Task(task) => {
                let until = now.saturating_add(self.kept.cfg.retry_interval);
                self.kept.tasks.get_mut(task).hold(until);
                until
            }
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
                // next maintenance listing is owed now). What staging still
                // holds above what the listing shows is sent again.
                self.engine.upload = UploadState::ListFirst { confirmed: None };
                self.engine.maintain.level_one = None;
                self.engine.maintain.stale = true;
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
            self.engine.maintain.remembered.owe_full();
        }
    }

    /// Replaces the engine with a fresh one, so the next tick boots from the
    /// replica and the WAL again ([`Self::rebuilt`] says what is kept). The
    /// connections are the same, so the application observer's last read stands.
    fn rebuild(&mut self) {
        match self.engine.course {
            Course::Local(_) => self.leave_local(),
            Course::Unbooted | Course::Snapshot { .. } | Course::At { .. } => {}
        }
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
    fn boot_step(&mut self, step: BootStep, took: Duration, now: Timestamp) -> Tick {
        match step {
            BootStep::Fault(fault) => self.boot_failed(fault, took, now),
            BootStep::Vouch(record) => self.want_takeover(self.boot_task(), record, now),
            BootStep::Refused(through) => {
                self.kept.refused_through = Some(through);
                self.verdict(self.boot_task(), StopCause::OtherWriter, now)
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

    /// This engine's put of `txid` is confirmed: a held takeover below it is spent. The
    /// record vouches only for replica files through its transaction, and the replica now
    /// holds this engine's own file above it (the snapshot a boot over the record takes is
    /// the first), so the record has nothing left to vouch for, and kept it would vouch for
    /// a file another writer puts at one of its ids after a rollback. A put of a covered
    /// transaction (an upload sent again) spends nothing: the record still describes the
    /// replica there, and an ordinary power-loss boot may still need it.
    fn spend_takeover(&mut self, txid: Txid) {
        if let Some(held) = self.kept.takeover.filter(|held| txid > held.txid()) {
            self.kept.takeover = None;
            self.kept.spent_takeover = Some(held);
        }
    }

    /// What one upload step that succeeded leaves behind; false when the uploader has
    /// nothing more to do this tick.
    fn after_upload_step(&mut self, did: &Did, now: Timestamp) -> bool {
        match *did {
            Did::Nothing => return false,
            Did::Diverged { .. } => {
                self.engine.maintain.remembered.forget();
                return false;
            }
            Did::Listed { .. } => {
                self.engine.maintain.stale = true;
                self.engine.maintain.remembered.owe_full();
            }
            Did::Put(FileInfo { key, .. }) | Did::AlreadyPut(key) => {
                let at = listing_after_upload(now, &self.kept.cfg.maintenance);
                self.engine.maintain.list_by(at);
                self.spend_takeover(key.range().max());
            }
        }
        true
    }

    /// Notes the put `plan` is about to send: its id may reach the replica though its answer
    /// is lost, so a takeover record before a running drop may vouch for it.
    fn note_attempt(&mut self, plan: UploadPlan) {
        if let UploadPlan::Upload(key) = plan {
            self.engine.attempted = self.engine.attempted.max(Some(key.range().max()));
        }
    }

    /// The takeover a running task needs before it deletes staged files the
    /// replica may hold: through the staged position, or `cap` if that is
    /// lower, unless one held already covers it or the configuration checks
    /// nothing at boot. A staged file this engine sent a put for may be on the
    /// replica unconfirmed (an upload whose answer was lost), so the record
    /// reaches the newest one it sent, and no further: an id past every put it
    /// sent and every confirmed one is not its own on the replica, and a record
    /// that vouched for it would let another writer's file there pass the next
    /// boot. With nothing confirmed and nothing sent, no record is needed.
    fn vouch_before_drop(&mut self, task: Task, cap: Option<Txid>, now: Timestamp) -> Option<Tick> {
        if self.kept.cfg.responses.other_writer == OtherWriter::ContinueAndInterleaveHistories {
            return None;
        }
        let staged = self.staged()?;
        let reach = self.engine.upload.confirmed().max(self.engine.attempted)?;
        let through = cap.map_or(staged, |cap| staged.min(cap)).min(reach);
        if self.kept.takeover.is_some_and(|held| held.covers(through)) {
            return None;
        }
        Some(self.want_takeover(task, TakeoverRecord::through(through), now))
    }

    /// A boot about to snapshot because one read of the newest staged file disagreed with
    /// what the file is (it did not verify, or its last page is not the WAL's) reads it once
    /// more first: one failed read says nothing, as for the sweep (`boot_listing`). The file
    /// becomes the suspect and the boot fails with `StagedLost`; the next boot that reads the
    /// same answer snapshots. `doubted` is the suspect this boot began with.
    fn doubt_newest_read(
        &mut self,
        doubted: Option<FileKey>,
        newest: Option<&NewestStagedFile>,
        recovered: &Recovered,
        took: Duration,
        now: Timestamp,
    ) -> Option<Tick> {
        let read_disagreed = matches!(
            recovered,
            Recovered::Snapshot {
                cause: SnapshotCause::NewestStagedFileUnreadable | SnapshotCause::LastPageMismatch,
                ..
            }
        );
        let key = FileKey::new(Level::ZERO, TxidRange::single(newest?.txid()));
        if !read_disagreed || doubted == Some(key) {
            return None;
        }
        self.kept.suspect_staged = Some(key);
        Some(self.boot_failed(Fault::StagedLost, took, now))
    }

    /// A boot step failed: the boot ends and is retried. The attempt is a sync step that
    /// failed, as Go counts the `init` its sync runs first (`db.go:1360-1376`, `db.go:1210`),
    /// which is where a store that refuses the credentials or cannot be reached shows in
    /// `litestream_sync_error_count`; Go's later upload failures are only logged. `took` is the
    /// attempt's time on the engine's clock.
    fn boot_failed(&mut self, fault: Fault, took: Duration, now: Timestamp) -> Tick {
        self.kept.work.sync_step(Some(fault), took, None, None);
        self.fault(self.boot_task(), fault, now)
    }

    /// One unit of an engine that has not booted. A local course's own work (`local`) comes
    /// first, as the WAL step does in `run`, and a boot attempt whose task may run goes ahead
    /// of it once [`state::FAIR_SHARE`] ticks in a row ended in that work, as an upload does
    /// (`owed_upload`): an application that never stops writing does not keep the engine from
    /// seeing the store answer.
    async fn boot<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        let ready = self.kept.tasks.ready(self.boot_task(), now);
        let owed = ready && self.engine.syncs_ahead >= state::FAIR_SHARE;
        if !owed && let Some(tick) = self.local_unit(env, now).await {
            return self.passed_over_uploads(tick);
        }
        if !ready {
            return self.ran(Event::Idle, now);
        }
        let tick = self.boot_attempt(env, now).await;
        self.keep_local(env, tick, now).await
    }

    /// Lists the replica, checks the pending staged files
    /// (`crate::witness`), reads the newest staged file left and
    /// recovers (in that order). A failure anywhere boots again from the start.
    async fn boot_attempt<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        let clock = env.clock;
        let began = clock.now();
        let took = || clock.now().saturating_duration_since(began);
        let takeover = self.boot_takeover();
        let doubted = self.kept.suspect_staged;
        let listed = match self.boot_listed(env, takeover).await {
            Ok(listed) => listed,
            Err(step) => {
                let tick = self.boot_step(step, took(), now);
                return self.or_local(env, tick, now).await;
            }
        };
        // After the listing: it may have ended a local course, whose attempts wait elsewhere.
        let task = self.boot_task();
        let suspect = &mut self.kept.suspect_staged;
        let Judged {
            staged:
                BootStaged {
                    upload,
                    dropped,
                    refused,
                    tokens,
                    seeding,
                    scanned: _,
                },
            booted:
                Booted {
                    newest_staged,
                    recovered,
                    page_size,
                    refused_through,
                },
        } = match boot_judge(env, listed, suspect).await {
            Ok(found) => found,
            Err(step) => return self.boot_step(step, took(), now),
        };
        // A refusal or a drop the sweep and the staged-file check already proved explains the
        // snapshot; only a verdict resting on the newest staged file's one read is doubted.
        let proven = refused.is_some() || !matches!(dropped, Dropped::Nothing);
        if let Some(tick) = (!proven)
            .then(|| {
                self.doubt_newest_read(doubted, newest_staged.as_ref(), &recovered, took(), now)
            })
            .flatten()
        {
            return tick;
        }
        let from = match recovered {
            Recovered::Continue(at) | Recovered::WalRestarted(at) => boot_continues(at),
            Recovered::Snapshot { cause, .. } => Err(cause),
            Recovered::Stop(cause) => {
                self.kept.refused_through = refused_through;
                return self.verdict(task, cause, now);
            }
        };
        let cause = boot_cause(dropped, refused, from.err());
        let staged = newest_staged.map(|newest_staged| newest_staged.txid());
        let course = boot_course(from, cause, self.kept.asked(), staged);
        let (pages, quiet) = match boot_seed(env.remote, &course, upload.confirmed(), seeding).await
        {
            Ok(seeded) => seeded,
            Err(fault) => return self.boot_failed(fault, took(), now),
        };
        self.quiet_at_boot(quiet);
        // Every step succeeded: the engine is assigned whole.
        self.engine = Engine {
            course,
            page_size,
            pages,
            upload,
            checkpoint: CheckpointState::boot(now),
            witnesses: tokens.into_iter().collect(),
            ..Engine::fresh()
        };
        self.kept.tasks.get_mut(task).succeed();
        self.ran(Event::Booted { cause }, now)
    }

    async fn run<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick {
        if let Some(tick) = self.owed_upload(env, now).await {
            return tick;
        }
        if let Some(tick) = self.wal_unit(env, now, true).await {
            return self.passed_over_uploads(tick);
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
    async fn drain<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
        deadline: Timestamp,
    ) -> Tick {
        // A retention pass the drain was asked to end with is not cut by the deadline once
        // everything is uploaded (`once_retention`).
        if now >= deadline {
            return if self.settled(env.wal) {
                self.drain_ends(env, now).await
            } else {
                self.missed(env, now).await
            };
        }
        if let Some(tick) = self.owed_upload(env, now).await {
            return tick;
        }
        if let Some(tick) = self.wal_unit(env, now, false).await {
            return self.passed_over_uploads(tick);
        }
        if let Some(tick) = self.upload_unit(env, now).await {
            return tick;
        }
        if self.settled(env.wal) {
            self.drain_ends(env, now).await
        } else {
            self.ran(Event::Idle, now)
        }
    }

    /// Everything is uploaded: the work the drain was asked to end with, then the end.
    async fn drain_ends<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Tick
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: Role,
        R: Role,
    {
        // The snapshot first, as Go takes it before retention; a snapshot that was not written
        // skips this database's retention, as Go returns before it.
        let snapshot = self.final_snapshot_unit(env, now).await;
        if matches!(self.kept.final_snapshot, FinalSnapshot::Failed(_)) {
            self.skip_final_retention();
        }
        if let Some(tick) = snapshot {
            return tick;
        }
        match self.final_retention_unit(env, now).await {
            Some(tick) => tick,
            None => self.drained(),
        }
    }

    const fn drained(&self) -> Tick {
        Tick::Drained {
            txid: self.engine.upload.confirmed(),
        }
    }

    /// The drain's deadline passed with work left: what was left, and the
    /// stop the deadline's fault routes to.
    async fn missed<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
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
    async fn unfinished<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
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
        self.staged().max(self.engine.upload.confirmed())
    }

    /// Takes the upload state a step left.
    fn set_upload(&mut self, state: UploadState) {
        self.engine.upload = state;
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
    async fn wal_unit<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
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
        if quiet && !wants_proof {
            return None;
        }
        let began = self.step_began(env.clock);
        let stepped = self.step(env, now).await;
        self.step_ended(env.clock, began, &stepped);
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
                Ok((files, proof))
            }
            Ok(Stepped::Resnapshot(cause)) => {
                match self.engine.course {
                    // The WAL no longer continues staging, and only a boot numbers a
                    // snapshot: it knows what the replica holds.
                    Course::Local(_) => self.leave_local(),
                    Course::Unbooted | Course::Snapshot { .. } | Course::At { .. } => {
                        self.engine.course.snapshot_for(cause, false);
                    }
                }
                Err(self.ran(Event::Resnapshot(cause), now))
            }
            Ok(Stepped::Held) => Ok((0, None)),
            Ok(Stepped::Stop(cause)) => Err(self.verdict(Task::Sync, cause, now)),
            Err(fault) => Err(self.fault(Task::Sync, fault, now)),
        }
    }

    /// What a sync step is counted from (`crate::counts`): when it began and where the
    /// position stood. Plain calls around the step, not a wrapper future, so counting adds no
    /// frame to the replicator thread's stack.
    fn step_began(&self, clock: &dyn Clock) -> (Timestamp, Option<Position>) {
        (clock.now(), self.position())
    }

    /// Counts the sync step that began at `began` from `from` and ended as `stepped`.
    fn step_ended(
        &mut self,
        clock: &dyn Clock,
        (began, from): (Timestamp, Option<Position>),
        stepped: &Result<Stepped, Fault>,
    ) {
        let took = clock.now().saturating_duration_since(began);
        let fault = stepped.as_ref().err().copied();
        let to = self.position();
        self.kept
            .work
            .sync_step(fault, took, from.as_ref(), to.as_ref());
    }

    /// One sync step from what recovery says about the WAL now.
    async fn step<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Result<Stepped, Fault> {
        if !self.staging_has_room() {
            // Marked as a step cut short, so the sync runs again when room
            // frees up: no new WAL change may come to wake it.
            self.engine.course.set_tail(Tail::More);
            return Ok(Stepped::Held);
        }
        let (mut reader, committed, len) = match self.open_wal(env)? {
            Ok(opened) => opened,
            Err(verdict) => return Ok(verdict),
        };
        // After the committed read: see `Env::clock`.
        let stamp = env.clock.now().max(self.stamp_floor(now));
        let size = reader.header().page_size();
        if let Some(was) = self.engine.page_size.filter(|was| *was != size) {
            return Err(Fault::PageSizeChanged { was, now: size });
        }
        self.engine.page_size = Some(size);
        let step = match self.plan_step(len, &mut reader, committed)? {
            Ok(step) => step,
            Err(verdict) => {
                // The frames are read after the length: a TRUNCATE checkpoint that lands
                // between shows as frames gone from a file now shorter than the one first
                // statted, what a power loss leaves with no power loss. The step runs again
                // over the file as it is.
                if matches!(
                    verdict,
                    Stepped::Resnapshot(SnapshotCause::LocalRewound { .. })
                ) && let Some(was) = len
                    && env
                        .wal
                        .len()
                        .map_err(Fault::from_source)?
                        .map_or(0, WalFileLen::bytes)
                        < was.bytes()
                {
                    return Err(Fault::WalMoved);
                }
                return Ok(verdict);
            }
        };
        wal_barrier(&mut *env.wal)?;
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

    fn plan_step<R: std::io::Read + std::io::Seek>(
        &mut self,
        len: Option<WalFileLen>,
        reader: &mut WalReader<R>,
        committed: CommittedRead,
    ) -> Result<Result<Step, Stepped>, Fault> {
        match (self.pending(), self.position()) {
            // A snapshot that keeps the page table trusts it to describe the
            // database, which holds only while the WAL still follows the
            // position: recovery first, and a verdict (a WAL that moved
            // under the position) replaces the cause with one that clears it.
            (Some(cause), Some(position)) if !snapshot_clears(&cause) => Ok(self
                .recovered(len, reader, committed, &position)?
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
            (None, Some(position)) => self.recovered(len, reader, committed, &position),
            // Unreachable by construction: no position means a `Course`
            // that names a snapshot's cause. Retried as the WAL having moved.
            (None, None) => Err(Fault::WalMoved),
        }
    }

    /// What recovery makes of the WAL past a running position.
    fn recovered<R: std::io::Read + std::io::Seek>(
        &mut self,
        len: Option<WalFileLen>,
        reader: &mut WalReader<R>,
        committed: CommittedRead,
        position: &Position,
    ) -> Result<Result<Step, Stepped>, Fault> {
        let probes = Probes::read_against(reader, position, &committed)?;
        let view = wal_view(len, Some(&*reader), committed);
        let source = PositionSource::Running(position, probes);
        let verdict = recover_traced(Some(source), &view, self.engine.upload.confirmed());
        Ok(step_of(self.noted(verdict)))
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
    fn open_wal<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
    ) -> Result<Result<(WalReader<W::Wal>, CommittedRead, Option<WalFileLen>), Stepped>, Fault>
    {
        // The length first, as the boot reads it: a file an application's TRUNCATE checkpoint
        // empties after it shows in the wal-index or header read below (a missing WAL, a new
        // generation), never as a file shorter than the header, which recovery takes for a power
        // loss.
        let len = env.wal.len().map_err(Fault::from_source)?;
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        if let Some(reader) = open_reader(env.wal)? {
            return Ok(Ok((reader, committed, len)));
        }
        if self.pending().is_none() {
            return self.without_wal(len, committed).map(Err);
        }
        env.ck.bump_seq().map_err(Fault::from_source)?;
        let len = env.wal.len().map_err(Fault::from_source)?;
        let committed = env.wal.committed().map_err(Fault::from_source)?;
        let reader = open_reader(env.wal)?.ok_or(Fault::WalMoved)?;
        Ok(Ok((reader, committed, len)))
    }

    /// No readable WAL and no snapshot pending: recovery classifies the loss.
    fn without_wal(
        &mut self,
        len: Option<WalFileLen>,
        committed: CommittedRead,
    ) -> Result<Stepped, Fault> {
        let (None, Some(position)) = (self.pending(), self.position()) else {
            return Err(Fault::WalMoved);
        };
        let view = wal_view::<std::io::Cursor<Vec<u8>>>(len, None, committed);
        // With no header, recovery decides before it looks at the probes.
        let probes = Probes {
            prev: FrameCheck::Miss(Miss::Truncated),
            stale_next: false,
            rechained: false,
        };
        let source = PositionSource::Running(&position, probes);
        Ok(
            match step_of(self.noted(recover_traced(
                Some(source),
                &view,
                self.engine.upload.confirmed(),
            ))) {
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
    async fn checkpoint_unit<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
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

    /// The newest id an upload step may treat as this engine's own.
    ///
    /// A listing judges the replica against everything this engine put
    /// there: what it staged, and what one of its own drops vouched for
    /// (files the replica may hold unconfirmed). Other plans ignore drops.
    fn upload_bound(&self, plan: UploadPlan) -> Option<Txid> {
        match plan {
            UploadPlan::List => self.staged().max(self.engine.dropped_through),
            UploadPlan::Idle | UploadPlan::Upload(_) => self.staged(),
        }
    }

    /// Staged files to the replica, up to the per-tick limit.
    async fn upload_unit<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick> {
        if !self.upload_ready(now) {
            return None;
        }
        self.engine.syncs_ahead = 0;
        let limit = self
            .kept
            .cfg
            .upload
            .files_per_tick
            .map_or(u32::MAX, NonZeroU32::get);
        let mut last = Did::Nothing;
        for _ in 0..limit {
            let plan = plan_upload(&self.engine.upload, self.staged());
            self.note_attempt(plan);
            let bound = self.upload_bound(plan);
            let witnesses = &self.engine.witnesses;
            let uploaded = upload_next_in(
                self.engine.upload,
                env.staging,
                env.remote,
                bound,
                (witnesses, self.guard()),
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
                Err(failed) => return Some(self.upload_failed(env, failed, plan, now).await),
            }
            if matches!(last, Did::Put(_) | Did::AlreadyPut(_)) {
                self.kept.suspect_staged = None;
            }
            if !self.after_upload_step(&last, now) {
                break;
            }
            if self.uploads_idle() {
                break;
            }
        }
        self.upload_succeeded();
        Some(self.ran(Event::Uploaded(last), now))
    }

    /// The upload task succeeded. Its degraded state goes, unless the store cannot make
    /// create-only writes: the uploads replace for the rest of the process, so that cause stands
    /// again whatever other cause the success just cleared (D79). Every success of the upload
    /// task goes through here.
    fn upload_succeeded(&mut self) {
        let upload = self.kept.tasks.get_mut(Task::Upload);
        upload.succeed();
        if let Some(since) = self.kept.unguarded {
            upload.stand(PersistentCause::ConditionalWritesUnsupported, since);
        }
    }

    /// How level-0 uploads write: create-only until the store said it cannot (D79).
    const fn guard(&self) -> Guard {
        match self.kept.unguarded {
            Some(_) => Guard::Unguarded,
            None => Guard::BucketRefuses,
        }
    }

    /// An upload step that failed: a staged file read wrong twice is dropped, a store that
    /// cannot make create-only writes switches the uploads to replacing ones, and the fault is
    /// routed.
    async fn upload_failed<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        failed: UploadFailed,
        plan: UploadPlan,
        now: Timestamp,
    ) -> Tick {
        self.set_upload(failed.state);
        // One failed read says nothing about the staged bytes: the
        // file is dropped only when a read on a later tick fails too.
        if let (Fault::StagedLost, UploadPlan::Upload(key)) = (failed.fault, plan)
            && self.kept.suspect_staged.replace(key) == Some(key)
        {
            if let Some(tick) = self.vouch_before_drop(Task::Upload, None, now) {
                return tick;
            }
            self.kept.suspect_staged = None;
            return self.lose_staged(env, key.range().min(), None, now).await;
        }
        if failed.fault == Fault::CreateOnlyUnsupported {
            // Raised before any replacing write: `fault` degrades the upload task (or stops),
            // and only the next put replaces (D79).
            self.kept.unguarded = self.kept.unguarded.or(Some(now));
        }
        self.fault(Task::Upload, failed.fault, now)
    }

    /// A listing while running found the replica's object at `from` verifying
    /// and other than the staged file. If the staged file is still the bytes
    /// this process witnessed, the object is another writer's: this process
    /// uploads a key only from its witnessed copy, so nothing it wrote can differ from that copy.
    /// That is the stop for another writer, never a snapshot above its file,
    /// which would interleave the two histories; the stop's record leaves the
    /// contested id out, so a restart stops again while the object stands.
    /// A staged file that changed under the engine (a backup put back) says
    /// nothing about writers, and the divergence goes as before
    /// ([`Self::lose_staged`]). A boot's divergence is recovery's: it cannot
    /// tell an earlier run's upload from another writer's.
    async fn diverged<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        state: UploadState,
        from: Txid,
        now: Timestamp,
    ) -> Tick {
        let key = FileKey::new(Level::ZERO, TxidRange::single(from));
        let token = self.engine.witnesses.get(&key).copied();
        // An id one of this engine's own drops vouched for holds its own
        // earlier upload on the replica (one whose answer was lost), not
        // another writer's: that object stands, and the staged file that
        // took its id goes, as for a staged file that changed.
        let vouched = Some(from) <= self.engine.dropped_through;
        let ours = match token {
            Some(token) if !vouched => still_holds(env.staging, &token).await,
            Some(_) | None => Ok(false),
        };
        match ours {
            Ok(true) => self.fault(Task::Upload, Fault::RemoteAheadOfLocal, now),
            Ok(false) => {
                if let Some(tick) = self.vouch_before_drop(Task::Upload, Some(from), now) {
                    return tick;
                }
                self.set_upload(state);
                self.lose_staged(env, from, Some(from), now).await
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
    async fn lose_staged<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        from: Txid,
        cap: Option<Txid>,
        now: Timestamp,
    ) -> Tick {
        // What the takeover record kept before this drop vouches for
        // (`Self::vouch_before_drop`, with the same cap): the replica may hold
        // those files unconfirmed, and they stay this engine's own.
        let vouched = self
            .staged()
            .map(|staged| cap.map_or(staged, |cap| staged.min(cap)));
        self.engine.witnesses.drop_from(from);
        if let Err(fault) = drop_staged_from(env.staging, from).await {
            // The staged files from `from` on are still there, and the next
            // upload would take the one after the confirmed transaction as
            // following it. List first, from below `from`, so the listing
            // judges the divergence again and this runs again before anything
            // is uploaded.
            let confirmed = self.engine.upload.confirmed().min(from.prev());
            self.set_upload(UploadState::ListFirst { confirmed });
            return self.fault(Task::Upload, fault, now);
        }
        self.engine.dropped_through = self.engine.dropped_through.max(vouched);
        // Files the drop vouched for may be on the replica unconfirmed (an
        // upload whose answer was lost): list first, so the listing confirms
        // those it finds and the snapshot goes above them, before maintenance
        // or a snapshot judges them.
        let confirmed = self.engine.upload.confirmed();
        if self.engine.dropped_through > confirmed {
            self.engine.upload = UploadState::ListFirst { confirmed };
        }
        // The position is past `from`; a snapshot after it would leave a hole.
        let cause = SnapshotCause::NewestStagedFileUnreadable;
        self.engine.course = Course::Snapshot {
            staged: from.prev(),
            cause,
        };
        // The unreadable files are gone, so the next upload may run at once; but nothing has
        // reached the replica, so the failures so far still count toward a stall, and a
        // degraded state stays until an upload goes through.
        self.kept.tasks.get_mut(Task::Upload).probe();
        self.ran(Event::Resnapshot(cause), now)
    }

    /// Deletes staged level-0 files the replica no longer needs from staging,
    /// oldest first, at most `MAX_DELETE` (W3). A failure waits on its own
    /// task and holds nothing else.
    async fn prune_unit<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick> {
        let bound = self.prune_ready(now)?;
        let listing = match env.staging.list(Level::ZERO, None, PageLimit::MAX).await {
            Ok(listing) => listing,
            Err(err) => return Some(self.fault(Task::Prune, L::fault(&err), now)),
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
            return Some(self.fault(Task::Prune, L::fault(&err), now));
        }
        self.kept.tasks.get_mut(Task::Prune).succeed();
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
    async fn maintain_unit<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick> {
        if !self.maintain_ready(now) {
            return None;
        }
        if self.short_pass_ok(now) {
            return Some(match self.maintain_short(env, now).await {
                Ok(pass) => {
                    self.act_on(env, pass.due, pass.after_delete, pass.created_at_read, now)
                        .await
                }
                Err(tick) => *tick,
            });
        }
        let loaded = Inventory::load(env.remote).await;
        let inventory = match loaded.map_err(|err| inventory_fault::<R>(&err)) {
            Ok(inventory) => inventory,
            Err(fault) => return Some(self.fault(Task::Maintenance, fault, now)),
        };
        // Uploads are idle, so everything this engine wrote is at or below
        // its position: a file past it is another writer's. In steady state
        // this listing is the first to see one, and nothing may be planned
        // from it (no compaction, snapshot or delete takes it as input).
        if let Some(tick) = self.listing_untrusted(&inventory, now) {
            return Some(tick);
        }
        if let Some(tick) = self.listing_regressed(&inventory, now) {
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
                due_full(&inventory, now, cfg, published, state, resolved)
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
        self.engine.maintain.remembered.learn_full(
            &inventory,
            now,
            &self.kept.cfg.maintenance,
            &decided.value,
        );
        Some(
            self.act_on(env, decided.value.due, None, decided.created_at_read, now)
                .await,
        )
    }

    /// A listing that ends below the transaction this engine confirmed: the replica lost
    /// its newest files (a bucket rolled back, files deleted by hand). Reported as
    /// `RemoteRegressed`, whose configured response relists and goes on. The same
    /// comparison the uploader's own listing makes, with nothing remembered: a hole below
    /// the newest file is not seen here, and restore reports it.
    fn listing_regressed(&mut self, inventory: &Inventory, now: Timestamp) -> Option<Tick> {
        (inventory.newest() < self.engine.upload.confirmed())
            .then(|| self.fault(Task::Upload, Fault::RemoteRegressed, now))
    }

    /// The read-back a boot owes (see `maintain::audit`). `None` when every
    /// newest file is sound: the same listing then plans the next job.
    async fn read_back_unit<R: Role>(
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
            return Some(self.fault(Task::Maintenance, Fault::RemoteCorrupt, now));
        }
        if let Err(err) = crate::publish::remove_one(remote, key, Why::RottenCovered).await {
            return Some(self.fault(Task::Maintenance, R::fault(&err), now));
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

    async fn run_job<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        job: &Job,
        now: Timestamp,
        created_at_read: u64,
    ) -> Tick {
        // One call site for every merge, so the tick's future holds one compaction.
        let outcome = if let Some((inputs, level)) = job.merge() {
            run_compaction(inputs, level, env.staging, env.remote).await
        } else if let Job::Delete(delete) = job {
            run_delete(delete, env.remote).await
        } else {
            return self.snapshot_job(env, job, now, created_at_read).await;
        };
        if let Job::Snapshot(_) = job {
            return self.snapshot_merged(job, outcome, now, created_at_read);
        }
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
    /// In steady state this listing is the first to see one.
    fn listing_untrusted(&mut self, inventory: &Inventory, now: Timestamp) -> Option<Tick> {
        (inventory.newest() > self.staged().max(self.engine.upload.confirmed()))
            .then(|| self.fault(Task::Maintenance, Fault::RemoteAheadOfLocal, now))
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

    /// A snapshot the store took.
    fn snapshot_written(&mut self, info: FileInfo, now: Timestamp, created_at_read: u64) -> Tick {
        self.maintained(now);
        let event = MaintainedEvent::Snapshot(info);
        self.ran(
            Event::Maintained {
                event,
                created_at_read,
            },
            now,
        )
    }

    /// How a snapshot built from the replica's level-0 files ended ([`Job::merge`]): no step, no
    /// proof and no read of the database file. A failure makes the next snapshot a read of the
    /// database, so a replica whose files cannot be merged still gets one.
    fn snapshot_merged(
        &mut self,
        job: &Job,
        outcome: Outcome,
        now: Timestamp,
        created_at_read: u64,
    ) -> Tick {
        let state = &mut self.engine.maintain.state;
        match outcome {
            Outcome::Compacted { info, .. } => {
                state.record_snapshot(now, None);
                self.snapshot_written(info, now, created_at_read)
            }
            // The listing changed under the job; the next listing is what to plan from.
            Outcome::Replan { .. } | Outcome::Deleted { .. } => {
                self.engine.maintain.remembered.owe_full();
                self.maintained(now);
                let event = MaintainedEvent::Replan;
                self.ran(
                    Event::Maintained {
                        event,
                        created_at_read,
                    },
                    now,
                )
            }
            Outcome::Failed(err) => {
                if let MaintainError::OutputDistrusted { key, .. } = &err {
                    state.distrust_snapshot(*key);
                }
                let fault = Fault::from_maintain(&err);
                state.record_snapshot(now, Some(&fault));
                state.refuse_seed();
                let waiter = self.job_waiter(job, now);
                self.fault(waiter, fault, now)
            }
        }
    }

    /// A level-9 snapshot of a fresh proof. A sync that staged a file first
    /// ends the tick: the upload runs before the snapshot is due again.
    async fn snapshot_job<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
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
        let began = self.step_began(env.clock);
        let stepped = self.step(env, now).await;
        self.step_ended(env.clock, began, &stepped);
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
    async fn snapshot_failed<R: Role>(
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
        let sync = self.kept.tasks.ready(Task::Sync, now)
            && self.staging_has_room()
            && (self.pending().is_some()
                || self.more_wal()
                || (self.checkpoints() && self.checkpoint_decision(now).is_some()));
        match self.engine.course {
            // Before the boot only the boot runs, on the sync task.
            Course::Unbooted => self.kept.tasks.ready(Task::Sync, now),
            // A local course syncs between boot attempts, which wait on the upload task.
            Course::Local(_) => sync || self.kept.tasks.ready(Task::Upload, now),
            Course::Snapshot { .. } | Course::At { .. } => {
                sync || self.upload_ready(now)
                    || self.prune_ready(now).is_some()
                    || self.maintain_ready(now)
            }
        }
    }

    /// Whether the upload task has a staged file to send and may run. The
    /// one statement of it: the task's unit and the next wake both ask here.
    fn upload_ready(&self, now: Timestamp) -> bool {
        self.kept.tasks.ready(Task::Upload, now) && !self.uploads_idle()
    }

    /// The upload step, ahead of the WAL step, once [`state::FAIR_SHARE`] ticks in a row ended
    /// in the WAL step; it runs only when uploads are ready, and resets the count when it does.
    async fn owed_upload<W: WalSource, P: PageSource, C: Checkpointer, L: Role, R: Role>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick> {
        if self.engine.syncs_ahead < state::FAIR_SHARE {
            return None;
        }
        self.upload_unit(env, now).await
    }

    /// `tick`, from the WAL step, counted: the upload step resets the count when it runs, and
    /// so does a local course's boot attempt.
    fn passed_over_uploads(&mut self, tick: Tick) -> Tick {
        self.engine.syncs_ahead = self.engine.syncs_ahead.saturating_add(1);
        tick
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
    /// and the drain's end. Tasks a draining actor does not run hold none, nor do those an
    /// engine that has not booted does not run.
    fn deadlines(&self) -> impl Iterator<Item = Timestamp> {
        let (running, checkpoints) = (self.running(), self.checkpoints());
        let task = |task: Task| self.kept.tasks.get(task).until();
        let deadline = self.drain_deadline();
        let only_checkpointing = [task(Task::Checkpoint), self.engine.gate.until()];
        let only_running = [
            task(Task::Prune),
            task(Task::Maintenance),
            self.engine.maintain.until,
        ];
        [task(Task::Sync), task(Task::Upload), deadline]
            .into_iter()
            .chain(only_checkpointing.into_iter().filter(move |_| checkpoints))
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
            .filter(|at| self.checkpoints() && *at > now);
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
    /// engine's own upload, read wrong), as listed. Pending files above every replica key vouch for
    /// nothing there, so one put back from a backup never raises it.
    async fn vouch<R: Role>(self, remote: &R, tip: Option<Txid>) -> Result<(), BootStep> {
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
        // Early returns, not `Option::map` with `min` and a guarded match: rustc 1.95 to at
        // least 1.102 (1.94 was sound) miscompiles that form with optimization in some
        // instantiations, so an empty replica read as transaction zero and a boot that dropped
        // staged files asked for a takeover through zero forever. LLVM's SimplifyCFG hoists the
        // `umin` out of `map` keeping its nonzero range attributes, InstCombine folds the
        // `None` arm into it, and EarlyCSE then proves the result nonzero
        // (https://github.com/llvm/llvm-project/issues/245656, tracked for rustc at
        // https://github.com/rust-lang/rust/issues/166505). `just test-opt` runs the scenarios
        // that catch it.
        let Some(replica) = tip.max(listed) else {
            return Ok(());
        };
        let through = replica.min(local);
        if covers(through) {
            return Ok(());
        }
        Err(BootStep::Vouch(TakeoverRecord::through(through)))
    }

    /// Whether the meta directory is brand new: nothing staged when the boot began and no
    /// takeover record. Only then does a refusal ask whether the local database is the
    /// replica's state (`crate::identity`); either is a record of this engine's past, and the
    /// gate's own rules judge with it (`docs/decisions/D176.md`).
    const fn fresh(self) -> bool {
        self.local.is_none() && matches!(self.takeover, Takeover::Vouch(None))
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

/// Removes the scratch files a killed process's uploads left, in staging and in the replica.
/// A crash loop otherwise leaves one per boot, each as large as the file it was writing; at
/// boot nothing of this engine is uploading yet. A failure is the directory's, which every
/// upload and prune needs too.
///
/// The replica's sweep is best effort, as Go's nothing is: S3 policies that allow all Go needs
/// do not allow listing or aborting multipart uploads, and a refusal there says nothing about
/// the replica's contents. A refusal for want of permission is noted in `denied`, skipped, and
/// not asked again by this process; the listing right after fails the same way if the
/// permission it needs is missing too. Any other failure is the replica not answering.
async fn remove_scratch<L: Role, R: Role>(
    staging: &L,
    remote: &R,
    denied: &mut bool,
) -> Result<(), Fault> {
    staging
        .remove_scratch()
        .await
        .map_err(|err| L::fault(&err))?;
    if *denied {
        return Ok(());
    }
    match remote.remove_scratch().await {
        Ok(_) => Ok(()),
        Err(StoreError::Denied {
            why: Denied::Permission,
            ..
        }) => {
            *denied = true;
            Ok(())
        }
        Err(err) => Err(R::fault(&err)),
    }
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

/// Boot's scratch removal and listing: the upload state to start from, the transaction the
/// staged files diverge from the replica at, if they do, and the gate. Nothing in staging is
/// read or deleted here: a boot that cannot reach the replica leaves it as it found it.
async fn boot_listing<L: Role, R: Role>(
    staging: &L,
    remote: &R,
    denied: &mut bool,
    takeover: Takeover,
) -> Result<Listed, BootStep> {
    remove_scratch(staging, remote, denied).await?;
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
    Ok(Listed {
        upload,
        diverged,
        gate: BootGate { takeover, local },
    })
}

/// What boot's listing leaves for the rest of the boot.
struct Listed {
    /// The upload state to start from.
    upload: UploadState,
    /// The staged files from this transaction on hold other content than the replica's
    /// newest level-0 file.
    diverged: Option<Txid>,
    /// What replica files above the local position need.
    gate: BootGate,
}

/// What a boot makes of the staged files once the replica is listed.
struct BootStaged {
    /// The upload state to start from.
    upload: UploadState,
    /// What was deleted from staging for diverging or failing verification.
    dropped: Dropped,
    /// The first pending file deleted, with those after it, for not following the file
    /// before it.
    refused: Option<Txid>,
    /// The tokens of the pending files left.
    tokens: Vec<Witnessed>,
    /// The files the pass read, for recovery.
    scanned: Scanned,
    /// The page checksum table and the quiet files, from the same reads.
    seeding: Seeding,
}

/// Judges the staged files against the listed replica: drops the ones that diverged from it,
/// then reads every one the replica may still need, once (`crate::witness::scan`).
///
/// A replica newest level-0 file that verifies and differs from the staged
/// copy under the same key is another writer's unless a takeover record
/// covers it: this engine uploads only from witnessed, synced staged copies
/// and keeps a takeover record before deleting its own, so without an
/// operator's hand a boot divergence is a second writer (the contested id).
/// The boot refuses it before touching staging (`BootStep::Refused`).
///
/// One read never condemns a file. The first staged file that fails verification is deleted,
/// with all after it, only when it is `suspect`, the file a read on an earlier tick failed on;
/// otherwise it becomes the suspect and the boot fails with `StagedLost`, to read it again on
/// the next try. The first pending file that does not follow the file before it is deleted,
/// with all after it, once a second pass agrees; two passes that disagree retry the boot.
///
/// The listing leaves a torn newest key unconfirmed, for its staged copy to
/// be uploaded over it. When that copy is the file refused and a read finds
/// the key on the replica (a read, since a listing may lag), the key stays as
/// the replica holds it and is confirmed: new content never takes a
/// transaction the replica holds, so the snapshot goes above it.
async fn boot_staged<L: Role, R: Role, W: std::io::Read + std::io::Seek>(
    staging: &L,
    remote: &R,
    listed: Listed,
    suspect: &mut Option<FileKey>,
    mut wal: Option<BootWal<'_, W>>,
) -> Result<BootStaged, BootStep> {
    let Listed {
        mut upload,
        diverged,
        gate,
    } = listed;
    let tip = upload.confirmed();
    // The staged files from `from` on cannot continue the replica: unless
    // the replica's object is another writer's, they go, and recovery
    // snapshots above what the replica holds.
    if let Some(from) = diverged {
        if let Some(tip) = tip.filter(|tip| gate.refuses(*tip)) {
            return Err(BootStep::Refused(tip));
        }
        gate.vouch(remote, tip).await?;
        drop_staged_from(staging, from).await?;
    }
    let Scan {
        tokens,
        bad,
        scanned,
        seeding,
    } = scan(
        staging,
        Base::Replica(remote, tip),
        boot_wal_again(&mut wal),
    )
    .await?;
    let (lost, refused) = match bad {
        None => (None, None),
        Some((key, Bad::Unreadable)) if *suspect != Some(key) => {
            *suspect = Some(key);
            return Err(Fault::StagedLost.into());
        }
        Some((key, Bad::Unreadable)) => {
            let from = key.range().min();
            gate.vouch(remote, tip).await?;
            drop_staged_from(staging, from).await?;
            (Some(from), None)
        }
        Some(refused @ (key, Bad::Unchained)) => {
            if scan(staging, Base::Replica(remote, tip), wal).await?.bad != Some(refused) {
                return Err(Fault::LocalIo.into());
            }
            let from = key.range().min();
            gate.vouch(remote, tip).await?;
            drop_staged_from(staging, from).await?;
            // A read, not a listing: a listing may lag the replica's writes, and one that
            // leaves the key out would let new content take a transaction the replica holds.
            let held = crate::store::exists(remote, &key)
                .await
                .map_err(|err| R::fault(&err))?;
            if held && tip.map_or(Some(Txid::MIN), Txid::next) == Some(from) {
                upload = UploadState::At(Some(from));
            }
            (None, Some(from))
        }
    };
    *suspect = None;
    let dropped = match (diverged, lost) {
        (Some(from), _) => Dropped::Diverged(from),
        (None, Some(from)) => Dropped::Unreadable(from),
        (None, None) => Dropped::Nothing,
    };
    Ok(BootStaged {
        upload,
        dropped,
        refused,
        tokens,
        scanned,
        seeding,
    })
}

/// The boot's WAL lent once more, for a second read under the same committed end.
fn boot_wal_again<'a, R>(wal: &'a mut Option<BootWal<'_, R>>) -> Option<BootWal<'a, R>> {
    wal.as_mut().map(|wal| BootWal {
        reader: &mut *wal.reader,
        committed: wal.committed,
    })
}

/// The boot's reader bounded by its committed end.
fn boot_wal_of<R>(
    reader: Option<&mut WalReader<R>>,
    committed: CommittedEnd,
) -> Option<BootWal<'_, R>> {
    reader.map(|reader| BootWal { reader, committed })
}

/// What the boot's judges found: what was made of the staged files, and recovery.
struct Judged {
    staged: BootStaged,
    booted: Booted,
}

/// The page table and the quiet files a boot starts with, from the reads its pass made
/// (`seeding`). Only a boot that continues keeps the table, truncated to the newest staged
/// file's size: a snapshot records every page it reads. The quiet files are kept for every
/// course: a pending file that holds no commit stays no commit when this boot takes a snapshot
/// of its own, and dated by its stamp it would trip the age bound after a long pause.
async fn boot_seed<R: Role>(
    remote: &R,
    course: &Course,
    confirmed: Option<Txid>,
    seeding: Seeding,
) -> Result<(PageChecksumTable, BTreeSet<Txid>), Fault> {
    let mut seeded = seeding.finish();
    seeded.place(remote, confirmed).await?;
    let table = match course {
        Course::At {
            position,
            snapshot: None,
            ..
        } => {
            seeded.table.truncate(position.commit());
            seeded.table
        }
        Course::At {
            snapshot: Some(_), ..
        }
        | Course::Snapshot { .. }
        | Course::Local(_)
        | Course::Unbooted => PageChecksumTable::new(),
    };
    Ok((table, seeded.quiet))
}

/// The pass over the staged files and recovery. Both judge the WAL on one wal-index read and
/// one reader opened after it, and no range past that committed end.
async fn boot_judge<W: WalSource, P: PageSource, C, L: Role, R: Role>(
    env: &mut Env<'_, W, P, C, L, R>,
    listed: Listed,
    suspect: &mut Option<FileKey>,
) -> Result<Judged, BootStep> {
    // The length first: a file that shrinks after it shows in the header or the wal-index read
    // after it (a missing WAL, a new generation), never as a short file.
    let len = env.wal.len().map_err(Fault::from_source)?;
    let committed = env.wal.committed().map_err(Fault::from_source)?;
    let mut reader = open_reader(env.wal)?;
    let gate = listed.gate;
    let wal = boot_wal_of(reader.as_mut(), committed.end());
    let staged = boot_staged(env.staging, env.remote, listed, suspect, wal).await?;
    let booted = boot_recover(
        env,
        reader.as_mut(),
        (len, committed),
        (staged.upload.confirmed(), &staged.scanned),
        gate,
    )
    .await?;
    Ok(Judged { staged, booted })
}

/// The newest staged file (or, with none staged, the replica's newest
/// level-0 file), the replica's page size, and what recovery makes of them.
/// A WAL whose page size is not the replica's stops here, before
/// anything is staged.
///
/// When the replica holds transactions past the newest
/// staged file (or anything, with nothing staged), its newest level-0 file is
/// read and checked against the live WAL: a WAL that still holds its range
/// with the same pages (`recover` continues from it) vouches for it, and the
/// engine continues from there. Otherwise, if those files are above where
/// staging stood when the boot began, nothing local vouches for them, and the
/// boot stops with `OtherWriter` unless the gate is off or a takeover record
/// covers them; then the verdict is the one recovery gives without the gate,
/// a snapshot above them.
async fn boot_recover<W: WalSource, P: PageSource, C, L: Role, R: Role>(
    env: &mut Env<'_, W, P, C, L, R>,
    mut reader: Option<&mut WalReader<W::Wal>>,
    (len, committed): (Option<WalFileLen>, CommittedRead),
    (confirmed, scanned): (Option<Txid>, &Scanned),
    gate: BootGate,
) -> Result<Booted, BootStep> {
    let (staging, remote) = (env.staging, env.remote);
    let end = committed.end();
    let wal = boot_wal_of(reader.as_deref_mut(), end);
    let staged = NewestStagedFile::load_in(staging, wal, scanned).await?;
    // The replica's newest level-0 file stands in for an empty staging
    // directory (a fault reading it is the boot's), and is read for the WAL's
    // word when it is above the local position (one that does not decode
    // gets none).
    let behind = confirmed.filter(|tip| staged.as_ref().map(NewestStagedFile::txid) < Some(*tip));
    let wanted = behind.filter(|tip| staged.is_none() || gate.above(*tip));
    let tip_file = match wanted {
        Some(tip) => {
            let wal = boot_wal_of(reader.as_deref_mut(), end);
            match NewestStagedFile::load_remote(remote, wal, tip).await {
                Ok(file) => file,
                Err(Fault::RemoteCorrupt) if staged.is_some() => None,
                Err(fault) => return Err(fault.into()),
            }
        }
        None => None,
    };
    let view = wal_view(len, reader.as_deref(), committed);
    let vouched = tip_file.as_ref().is_some_and(|file| {
        let verdict = recover_traced(Some(PositionSource::Boot(file)), &view, confirmed);
        matches!(
            verdict.recovered,
            Recovered::Continue(_) | Recovered::WalRestarted(_)
        )
    });
    let unvouched = confirmed.filter(|tip| !vouched && gate.above(*tip));
    let newest_staged = if vouched {
        tip_file
    } else {
        staged.or(tip_file)
    };
    let page_size = prefix_page_size(newest_staged.as_ref(), confirmed, remote).await?;
    let now = view.header.map(|header| header.page_size());
    let refused = unvouched.filter(|tip| gate.refuses(*tip));
    let resized = page_size.zip(now).filter(|(was, now)| was != now);
    let (refused_through, ahead) = match refused {
        Some(tip) if resized.is_none() && gate.fresh() => {
            // Said in the tick that lists the replica: a tick of its own to announce it would
            // list the replica again, and a crash between the two would leave the check unrun.
            (env.notice)(&Event::CheckingIdentity { tip });
            let local =
                identity::local_against_replica(env.wal, reader, committed, env.db, remote, tip)
                    .await?;
            match local {
                Identity::Same => (None, None),
                Identity::Ahead => (None, Some(tip)),
                Identity::Other => (Some(tip), None),
            }
        }
        Some(_) | None => (refused, None),
    };
    let recovered = match (resized, refused_through, ahead) {
        (Some((was, now)), _, _) => Recovered::Stop(StopCause::PageSizeChanged { was, now }),
        (None, Some(_), _) => Recovered::Stop(StopCause::OtherWriter),
        // The database reads as the replica's state plus commits it never got: the snapshot
        // goes above the replica's newest transaction whatever the WAL says.
        (None, None, Some(tip)) => Recovered::Snapshot {
            after: Some(tip),
            cause: SnapshotCause::AheadOfReplica { tip },
        },
        // A boot position holds no read lock, so no verdict here names a broken one; a
        // rewound WAL's event repeats what its snapshot cause carries.
        (None, None, None) => {
            recover_traced(
                newest_staged.as_ref().map(PositionSource::Boot),
                &view,
                confirmed,
            )
            .recovered
        }
    };
    // The reader reads the file as the boot goes, so a TRUNCATE checkpoint that lands during
    // the boot shows as frames gone from a file that is now shorter than the one the boot
    // statted first: what a power loss leaves, with no power loss. The boot judges the WAL
    // again, and finds the file empty or in a new generation.
    if matches!(
        recovered,
        Recovered::Snapshot {
            cause: SnapshotCause::LocalRewound { .. },
            ..
        }
    ) && let Some(was) = len
        && env
            .wal
            .len()
            .map_err(Fault::from_source)?
            .map_or(0, WalFileLen::bytes)
            < was.bytes()
    {
        return Err(Fault::WalMoved.into());
    }
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
async fn prefix_page_size<R: Role>(
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
async fn replica_page_size<R: Role>(remote: &R) -> Result<Option<PageSize>, Fault> {
    let inventory = Inventory::load(remote)
        .await
        .map_err(|err| inventory_fault::<R>(&err))?;
    let mut files: Vec<&FileInfo> = inventory.files().iter().collect();
    if files.is_empty() {
        return Ok(None);
    }
    files.sort_by_key(|file| std::cmp::Reverse(file.key.range().max()));
    for file in files.into_iter().take(PAGE_SIZE_HEADERS) {
        if let Some(header) = read_header(remote, &file.key, R::fault).await? {
            return Ok(Some(header.page_size()));
        }
    }
    Err(Fault::RemoteCorrupt)
}

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

/// The boot's recovery proved the staged position: the engine continues from it.
fn boot_continues(position: Position) -> Result<Position, SnapshotCause> {
    Ok(position)
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
