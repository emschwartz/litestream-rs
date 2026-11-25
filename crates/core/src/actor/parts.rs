//! The actor's two halves: what a rebuild keeps, and the engine it replaces.
//!
//! A rebuild (after a bug, or past another writer the operator chose to
//! continue past) must forget everything the engine believed about the WAL,
//! the staging directory and the replica, and nothing anyone asked of it.
//! [`Kept`] is the second kind, [`Engine`] the first. A rebuild builds the
//! next actor from `Kept::rebuilt`, which names every field, and
//! `Engine::fresh`; a field added to either lands on one side by where it is
//! declared, and cannot be reset or carried by an omission.
//!
//! The engine's place in the WAL is one value, [`Course`]: not booted, on a
//! local course while the replica cannot be listed (`super::local`), no
//! position (the next sync is a snapshot, for a cause), or at a position.
//! What used to be separate flags (a snapshot pending, more WAL behind the
//! cap, a wal-index reset seen) are fields of the variant they are true in,
//! so "no position and no snapshot pending" cannot be written down. A boot
//! builds a whole `Engine` and assigns it only when every step succeeded.

use std::collections::BTreeSet;

use lsr_ltx::{PageSize, Timestamp, Txid};
use lsr_wal::CommittedEnd;

use super::ActorConfig;
use super::clock::EngineClock;
use super::local::{LocalAt, LocalTry};
use super::once::FinalSnapshot;
use super::once_retention::FinalRetention;
use super::state::{Gate, Live, Maintain, Phase, Tasks};
use crate::app::AppCheckpointObserver;
use crate::checkpoint::CheckpointState;
use crate::counts::WorkCounts;
use crate::failstop::{Lag, WalMark};
use crate::fault::{SnapshotCause, SnapshotCounts, StopCause};
use crate::layout::FileKey;
use crate::pages::PageChecksumTable;
use crate::position::Position;
use crate::store::Denied;
use crate::takeover::TakeoverRecord;
use crate::upload::UploadState;
use crate::witness::Witnesses;

/// Whether the actor was asked to stop, which no rebuild forgets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Stop {
    /// Not asked.
    No,
    /// Shutting down: only sync and upload run, until the deadline.
    Draining {
        /// When work still left becomes [`StopCause::Shutdown`].
        deadline: Timestamp,
    },
    /// Stopped: every later tick returns at once, with no I/O.
    Stopped(StopCause),
}

impl Stop {
    /// After `begin_shutdown`. A stop stays a stop, and a second shutdown
    /// keeps the earlier of the two deadlines: a later call can shorten a
    /// drain, never lengthen it.
    pub(super) fn drain(self, deadline: Timestamp) -> Self {
        match self {
            Self::No => Self::Draining { deadline },
            Self::Draining { deadline: was } => Self::Draining {
                deadline: was.min(deadline),
            },
            Self::Stopped(_) => self,
        }
    }

    /// The public phase: a stop, or whether the engine has booted.
    pub(super) const fn phase(self, booted: bool) -> Phase {
        match (self, booted) {
            (Self::Stopped(cause), _) => Phase::Stopped(cause),
            (Self::Draining { deadline }, _) => Phase::Draining { deadline },
            (Self::No, false) => Phase::Boot,
            (Self::No, true) => Phase::Running,
        }
    }
}

/// The restore point retention keeps while a task is degraded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TaskRestorePoint {
    /// No task is degraded, or none has been since the last release.
    Free,
    /// The newest transaction the replica held when the first task degraded
    /// (`None`: it held nothing).
    Held(Option<Txid>),
}

impl TaskRestorePoint {
    const fn txid(self) -> Option<Txid> {
        match self {
            Self::Free => None,
            Self::Held(txid) => txid,
        }
    }
}

/// What a rebuild keeps: the configuration, what was asked of the actor, and
/// what it reports. None of it is a belief about the WAL or the replica.
#[derive(Debug)]
pub(super) struct Kept {
    pub(super) cfg: ActorConfig,
    pub(super) stop: Stop,
    /// Each task's wait and degraded state: reports, not engine state.
    pub(super) tasks: Tasks,
    /// Engine time and the wall readings it follows (`clock`); `None` before
    /// the first tick.
    pub(super) clock: Option<EngineClock>,
    pub(super) task_restore_point: TaskRestorePoint,
    /// The restore point a scheduled verify asked retention to keep.
    pub(super) verify_restore_point: Option<Txid>,
    /// The live database's pages a scheduled verify asked for.
    pub(super) live: Live,
    /// An operator asked for a snapshot that has not been staged yet.
    pub(super) snapshot_asked: bool,
    /// What the application's checkpoints did between reads.
    pub(super) app: AppCheckpointObserver,
    /// A staged file whose last read failed verification. One failed read
    /// says nothing about the stored bytes, so the file and those after it
    /// are deleted only when a read on a later tick fails too. Kept across
    /// boots, since boot's sweep is one of the readers.
    pub(super) suspect_staged: Option<FileKey>,
    /// Since when level-0 uploads replace, because the store cannot make create-only writes:
    /// `None` (create-only) until the store says so, then for the rest of the process (D79).
    pub(super) unguarded: Option<Timestamp>,
    /// Snapshots staged since the actor began, per cause: a report.
    pub(super) snapshots: SnapshotCounts,
    /// What the sync steps and checkpoints did since the actor began: a report.
    pub(super) work: WorkCounts,
    /// The snapshot a drain was asked to end with (`once`): a request of this process.
    pub(super) final_snapshot: FinalSnapshot,
    /// The retention pass a drain was asked to end with (`once_retention`): a request of this
    /// process.
    pub(super) final_retention: FinalRetention,
    /// The takeover the driver handed in (`crate::takeover`): an operator's,
    /// a restore's, or one this engine asked for before dropping its own
    /// staged files.
    pub(super) takeover: Option<TakeoverRecord>,
    /// A takeover the engine needs durable before it deletes staged files the
    /// replica holds: the driver writes it and hands it back.
    pub(super) wanted_takeover: Option<TakeoverRecord>,
    /// A takeover this engine spent by confirming a put above it: the driver deletes the
    /// durable record and hands it back ([`super::Actor::forget_takeover`]). Kept across a
    /// rebuild, so the record cannot come back at the next start.
    pub(super) spent_takeover: Option<TakeoverRecord>,
    /// The replica's newest transaction when a boot refused to build on files
    /// nothing vouched for: what the stop reports.
    pub(super) refused_through: Option<Txid>,
    /// The fail-stop clock (`crate::failstop`): reports, kept across boots so a rebuild does not
    /// restart it.
    pub(super) lag: Lag,
    /// Whether any engine of this process has booted, so listed the replica once. A rebuild
    /// keeps it: fail-stop's refusal at boot is about this process, not this engine.
    pub(super) listed: bool,
    /// The last boot failure before the first listing was the store refusing access.
    pub(super) refused_at_boot: Option<Denied>,
    /// When the current run of boot attempts that failed, none by a refusal, began (the first
    /// failure after the last refusal, or after the start). Only a run that outlasts
    /// `BOOT_UNDECIDED_BOUND` without a decision judges the boot and lets the application
    /// start (`crate::failstop`); one failed attempt never does.
    pub(super) boot_failing_since: Option<Timestamp>,
    /// The run above has outlasted the bound and the last failure was not a refusal, as of
    /// the last tick: the boot is judged without having decided.
    pub(super) boot_overdue: bool,
    /// Pending staged files that hold no commit of the application's: a file that changes no
    /// page, or a snapshot for a doubt alone staged while nothing was off the replica. They are
    /// not lag (`actor::lag`). At most one per pending file, so bounded by the witness tokens.
    pub(super) quiet: BTreeSet<Txid>,
    /// Where the WAL ended when a snapshot owed for a doubt alone was first observed, with
    /// every staged file on the replica: frames up to it are not lag (`actor::lag`).
    pub(super) doubt_baseline: Option<WalMark>,
    /// Recoveries that found a commit abandoned after the read lock was taken: a report,
    /// kept across rebuilds, since the snapshot that follows names only the generation change.
    pub(super) read_lock_violations: u64,
    /// The replica refused the boot's sweep of its scratch files and open uploads for want of
    /// permission, and the process does not ask again: the sweep is best effort. Kept across
    /// rebuilds.
    pub(super) scratch_sweep_denied: bool,
    /// Whether this process may still try the local course (`super::local`). Kept across
    /// rebuilds: the try reads every staged file, so it is made once.
    pub(super) local: LocalTry,
}

impl Kept {
    pub(super) fn new(cfg: ActorConfig) -> Self {
        Self {
            cfg,
            stop: Stop::No,
            tasks: Tasks::default(),
            clock: None,
            task_restore_point: TaskRestorePoint::Free,
            verify_restore_point: None,
            live: Live::Idle,
            snapshot_asked: false,
            app: AppCheckpointObserver::default(),
            suspect_staged: None,
            unguarded: None,
            snapshots: SnapshotCounts::default(),
            work: WorkCounts::default(),
            final_snapshot: FinalSnapshot::NotAsked,
            final_retention: FinalRetention::NotAsked,
            takeover: None,
            wanted_takeover: None,
            spent_takeover: None,
            refused_through: None,
            lag: Lag::default(),
            listed: false,
            refused_at_boot: None,
            boot_failing_since: None,
            boot_overdue: false,
            quiet: BTreeSet::new(),
            doubt_baseline: None,
            read_lock_violations: 0,
            scratch_sweep_denied: false,
            local: LocalTry::Open,
        }
    }

    /// What the next engine starts beside, with `app` as its observer of
    /// the application's checkpoints (whether its last read still stands is the
    /// caller's to say). Every field is named, so a new one does not compile
    /// until it says what a rebuild makes of it.
    pub(super) fn rebuilt(&self, observer: AppCheckpointObserver) -> Self {
        let Self {
            cfg,
            stop,
            tasks,
            clock,
            task_restore_point,
            verify_restore_point,
            live,
            snapshot_asked,
            app: _,
            suspect_staged,
            unguarded,
            snapshots,
            work,
            final_snapshot,
            final_retention,
            takeover,
            wanted_takeover: _,
            spent_takeover,
            refused_through,
            lag,
            listed,
            refused_at_boot,
            boot_failing_since,
            boot_overdue,
            quiet: _,
            doubt_baseline: _,
            read_lock_violations,
            scratch_sweep_denied,
            local,
        } = self;
        Self {
            cfg: cfg.clone(),
            stop: *stop,
            tasks: *tasks,
            clock: *clock,
            task_restore_point: *task_restore_point,
            verify_restore_point: *verify_restore_point,
            live: live.rebuilt(),
            snapshot_asked: *snapshot_asked,
            app: observer,
            suspect_staged: *suspect_staged,
            // The store does not learn conditional writes from a rebuild: only a new process
            // asks it again.
            unguarded: *unguarded,
            snapshots: *snapshots,
            work: *work,
            final_snapshot: *final_snapshot,
            final_retention: final_retention.clone(),
            takeover: *takeover,
            // A rebuilt engine boots again and asks again for what it needs.
            wanted_takeover: None,
            spent_takeover: *spent_takeover,
            refused_through: *refused_through,
            lag: *lag,
            listed: *listed,
            refused_at_boot: *refused_at_boot,
            boot_failing_since: *boot_failing_since,
            boot_overdue: *boot_overdue,
            // Beliefs about staged files: the rebuilt engine's boot judges them again.
            quiet: BTreeSet::new(),
            doubt_baseline: None,
            read_lock_violations: *read_lock_violations,
            scratch_sweep_denied: *scratch_sweep_denied,
            local: *local,
        }
    }

    /// The restore point retention keeps: the oldest of those asked for, a degraded task's and a
    /// scheduled verify's. Derived on every read, so there is no copy to fall behind.
    pub(super) fn kept_restore_point(&self) -> Option<Txid> {
        [self.task_restore_point.txid(), self.verify_restore_point]
            .into_iter()
            .flatten()
            .min()
    }

    /// The cause a snapshot is owed for besides the engine's own, if any.
    pub(super) fn asked(&self) -> Option<SnapshotCause> {
        self.snapshot_asked
            .then_some(SnapshotCause::OperatorRequested)
    }
}

/// What is known of the WAL past the synced position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Tail {
    /// Nothing: the last step reached the committed end it read.
    Synced,
    /// The last step stopped at the cap: more is committed.
    More,
    /// The last step found the wal-index, at this read, reset ahead of the
    /// WAL header: nothing to sync, and no proof to checkpoint on, until the
    /// index changes.
    Reset(CommittedEnd),
}

/// Where the engine is in the WAL, which also names the newest staged
/// transaction: every staged level-0 file comes from a step whose position
/// names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Course {
    /// Not booted: the next tick lists the replica and recovers.
    Unbooted,
    /// Not booted, and the replica could not be listed for long enough that the engine
    /// syncs and checkpoints from local state meanwhile (`super::local`). Nothing is
    /// uploaded: the boot that lists the replica replaces the engine whole.
    Local(LocalAt),
    /// No position: a boot that found nothing to continue, or a staged file
    /// lost. The next sync is a snapshot.
    Snapshot {
        /// The newest transaction still staged (or, with nothing staged, the
        /// replica's newest level-0 file), if any.
        staged: Option<Txid>,
        /// Why.
        cause: SnapshotCause,
    },
    /// The last synced position; its transaction is the newest staged.
    At {
        position: Position,
        tail: Tail,
        /// Set when the next sync is a snapshot all the same.
        snapshot: Option<SnapshotCause>,
    },
}

impl Course {
    pub(super) const fn booted(&self) -> bool {
        match self {
            Self::Unbooted | Self::Local(_) => false,
            Self::Snapshot { .. } | Self::At { .. } => true,
        }
    }

    pub(super) const fn position(&self) -> Option<Position> {
        match self {
            Self::At { position, .. } => Some(*position),
            Self::Local(at) => Some(at.position()),
            Self::Unbooted | Self::Snapshot { .. } => None,
        }
    }

    pub(super) const fn staged(&self) -> Option<Txid> {
        match self {
            Self::At { position, .. } => Some(position.txid()),
            Self::Local(at) => Some(at.position().txid()),
            Self::Snapshot { staged, .. } => *staged,
            Self::Unbooted => None,
        }
    }

    /// Why the next sync is a snapshot, when it is one.
    pub(super) const fn pending(&self) -> Option<SnapshotCause> {
        match self {
            Self::At { snapshot, .. } => *snapshot,
            Self::Snapshot { cause, .. } => Some(*cause),
            // A local course takes no snapshot: only a boot knows what the replica holds.
            Self::Unbooted | Self::Local(_) => None,
        }
    }

    pub(super) const fn tail(&self) -> Tail {
        match self {
            Self::At { tail, .. } => *tail,
            Self::Local(at) => at.tail(),
            Self::Unbooted | Self::Snapshot { .. } => Tail::Synced,
        }
    }

    /// Makes the next sync a snapshot for `cause`; with `keep`, a cause
    /// already pending stays. An unbooted engine has no next sync yet, and a
    /// local course leaves its snapshots to the boot.
    pub(super) fn snapshot_for(&mut self, new: SnapshotCause, keep: bool) {
        match self {
            Self::At { snapshot, .. } => {
                *snapshot = Some(snapshot.filter(|_| keep).unwrap_or(new));
            }
            Self::Snapshot { cause, .. } => {
                if !keep {
                    *cause = new;
                }
            }
            Self::Unbooted | Self::Local(_) => {}
        }
    }

    /// The position moved, by a sync step or a checkpoint's; a pending
    /// snapshot goes when `wrote_pending` says the step staged it.
    pub(super) fn moved(&mut self, position: Position, tail: Tail, wrote_pending: bool) {
        let snapshot = self.pending().filter(|_| !wrote_pending);
        *self = match self {
            Self::Local(at) => Self::Local(at.moved(position, tail)),
            Self::Unbooted | Self::Snapshot { .. } | Self::At { .. } => Self::At {
                position,
                tail,
                snapshot,
            },
        };
    }

    /// The same position with what is now known of the WAL past it.
    pub(super) const fn set_tail(&mut self, now: Tail) {
        match self {
            Self::At { tail, .. } => *tail = now,
            Self::Local(at) => *at = at.moved(at.position(), now),
            Self::Unbooted | Self::Snapshot { .. } => {}
        }
    }
}

/// Everything the engine believes about the WAL, the staging directory and
/// the replica. A rebuild and a boot replace it whole.
#[derive(Debug)]
pub(super) struct Engine {
    pub(super) course: Course,
    /// The replica's page size, one per prefix; `None` while the replica and
    /// staging are both empty.
    pub(super) page_size: Option<PageSize>,
    /// What each database-file page should hold (`crate::pages`).
    pub(super) pages: PageChecksumTable,
    pub(super) checkpoint: CheckpointState,
    pub(super) gate: Gate,
    pub(super) upload: UploadState,
    pub(super) maintain: Maintain,
    /// Staged level-0 files through this transaction were pruned (W3).
    pub(super) pruned: Option<Txid>,
    /// The newest transaction a running drop of this engine's own staged
    /// files vouched for, through the takeover record it kept first: the
    /// replica may hold those files unconfirmed (an upload whose answer was
    /// lost), so a listing never calls them another writer's; one that finds
    /// them confirms them, and the snapshot goes above them. Only this
    /// engine's own drops set it, never an operator's or a boot's record.
    pub(super) dropped_through: Option<Txid>,
    /// The uploader's tokens for the pending staged files (`crate::witness`):
    /// held in memory only, so a new engine witnesses again at boot.
    pub(super) witnesses: Witnesses,
    /// The newest level-0 transaction this engine sent a put for. Past what a listing or a
    /// commit confirmed, only its own puts whose answer was lost can be on the replica: a
    /// boot's listing showed everything earlier processes put. So a takeover record before
    /// a running drop vouches for no id past this one.
    pub(super) attempted: Option<Txid>,
    /// Ticks in a row that ended in the WAL step since the upload step last ran; at
    /// [`super::state::FAIR_SHARE`] the upload step runs first. An app that commits before
    /// every tick keeps the WAL step busy on each one, and the upload step that follows it
    /// would otherwise never run.
    pub(super) syncs_ahead: u32,
}

impl Engine {
    /// An engine that knows nothing and boots on its first tick.
    pub(super) fn fresh() -> Self {
        Self {
            course: Course::Unbooted,
            page_size: None,
            pages: PageChecksumTable::new(),
            // Replaced at boot, when the time is known.
            checkpoint: CheckpointState::boot(Timestamp::from_unix_millis(0)),
            gate: Gate::Open,
            upload: UploadState::Unknown,
            maintain: Maintain::default(),
            pruned: None,
            dropped_through: None,
            witnesses: Witnesses::new(),
            attempted: None,
            syncs_ahead: 0,
        }
    }
}
