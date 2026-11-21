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
//! The engine's place in the WAL is one value, [`Course`]: not booted, no
//! position (the next sync is a snapshot, for a cause), or at a position.
//! What used to be separate flags (a snapshot pending, more WAL behind the
//! cap, a wal-index reset seen) are fields of the variant they are true in,
//! so "no position and no snapshot pending" cannot be written down. A boot
//! builds a whole `Engine` and assigns it only when every step succeeded.

use lsr_ltx::{PageSize, Timestamp, Txid};
use lsr_wal::CommittedEnd;

use super::ActorConfig;
use super::state::{Gate, Live, Maintain, Phase, Tasks};
use crate::app::AppCheckpointObserver;
use crate::checkpoint::CheckpointState;
use crate::fault::{SnapshotCause, StopCause};
use crate::pages::PageChecksumTable;
use crate::position::Position;
use crate::upload::UploadState;

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
    /// The clock's last reading.
    pub(super) last: Option<Timestamp>,
    pub(super) clamp_reported: bool,
    pub(super) task_restore_point: TaskRestorePoint,
    /// The restore point a scheduled verify asked retention to keep.
    pub(super) verify_restore_point: Option<Txid>,
    /// The live database's pages a scheduled verify asked for.
    pub(super) live: Live,
    /// An operator asked for a snapshot that has not been staged yet.
    pub(super) snapshot_asked: bool,
    /// What the application's checkpoints did between reads.
    pub(super) app: AppCheckpointObserver,
}

impl Kept {
    pub(super) fn new(cfg: ActorConfig) -> Self {
        Self {
            cfg,
            stop: Stop::No,
            tasks: Tasks::default(),
            last: None,
            clamp_reported: false,
            task_restore_point: TaskRestorePoint::Free,
            verify_restore_point: None,
            live: Live::Idle,
            snapshot_asked: false,
            app: AppCheckpointObserver::default(),
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
            last,
            clamp_reported,
            task_restore_point,
            verify_restore_point,
            live,
            snapshot_asked,
            app: _,
        } = self;
        Self {
            cfg: cfg.clone(),
            stop: *stop,
            tasks: *tasks,
            last: *last,
            clamp_reported: *clamp_reported,
            task_restore_point: *task_restore_point,
            verify_restore_point: *verify_restore_point,
            live: live.rebuilt(),
            snapshot_asked: *snapshot_asked,
            app: observer,
        }
    }

    /// The restore point retention keeps: the older of the two asked for, a
    /// degraded task's and a scheduled verify's. Derived on every read, so
    /// there is no copy to fall behind.
    pub(super) fn kept_restore_point(&self) -> Option<Txid> {
        match (self.task_restore_point.txid(), self.verify_restore_point) {
            (Some(task), Some(verify)) => Some(task.min(verify)),
            (task, verify) => task.or(verify),
        }
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
            Self::Unbooted => false,
            Self::Snapshot { .. } | Self::At { .. } => true,
        }
    }

    pub(super) const fn position(&self) -> Option<Position> {
        match self {
            Self::At { position, .. } => Some(*position),
            Self::Unbooted | Self::Snapshot { .. } => None,
        }
    }

    pub(super) const fn staged(&self) -> Option<Txid> {
        match self {
            Self::At { position, .. } => Some(position.txid()),
            Self::Snapshot { staged, .. } => *staged,
            Self::Unbooted => None,
        }
    }

    /// Why the next sync is a snapshot, when it is one.
    pub(super) const fn pending(&self) -> Option<SnapshotCause> {
        match self {
            Self::At { snapshot, .. } => *snapshot,
            Self::Snapshot { cause, .. } => Some(*cause),
            Self::Unbooted => None,
        }
    }

    pub(super) const fn tail(&self) -> Tail {
        match self {
            Self::At { tail, .. } => *tail,
            Self::Unbooted | Self::Snapshot { .. } => Tail::Synced,
        }
    }

    /// Makes the next sync a snapshot for `cause`; with `keep`, a cause
    /// already pending stays. An unbooted engine has no next sync yet.
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
            Self::Unbooted => {}
        }
    }

    /// The position moved, by a sync step or a checkpoint's; a pending
    /// snapshot goes when `wrote_pending` says the step staged it.
    pub(super) fn moved(&mut self, position: Position, tail: Tail, wrote_pending: bool) {
        let snapshot = self.pending().filter(|_| !wrote_pending);
        *self = Self::At {
            position,
            tail,
            snapshot,
        };
    }

    /// The same position with what is now known of the WAL past it.
    pub(super) const fn set_tail(&mut self, now: Tail) {
        match self {
            Self::At { tail, .. } => *tail = now,
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
        }
    }
}
