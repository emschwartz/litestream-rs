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

use std::collections::BTreeSet;

use lsr_ltx::{PageSize, Timestamp, Txid};
use lsr_wal::CommittedEnd;

use super::ActorConfig;
use super::ahead::AheadCheck;
use super::clock::EngineClock;
use super::once::FinalSnapshot;
use super::state::{Gate, Live, Maintain, Phase, Tasks};
use super::unproven::Unproven;
use crate::app::AppCheckpointObserver;
use crate::checkpoint::CheckpointState;
use crate::failstop::{Lag, WalMark};
use crate::fault::{SnapshotCause, SnapshotCounts, StopCause};
use crate::journal::{JournalSignal, JournalWriter};
use crate::layout::FileKey;
#[cfg(test)]
use crate::obligations::{Keep, Record};
use crate::pages::PageChecksumTable;
use crate::position::Position;
use crate::store::Denied;
use crate::takeover::TakeoverRecord;
use crate::upload::UploadState;
use crate::witness::Witnesses;
use lsr_journal::Journal;
use std::io::ErrorKind;

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
    /// Snapshots staged since the actor began, per cause: a report.
    pub(super) snapshots: SnapshotCounts,
    /// The snapshot a drain was asked to end with (`once`): a request of this process.
    pub(super) final_snapshot: FinalSnapshot,
    /// How far the engine is past a failed sync of the WAL (`unproven`). Kept across boots:
    /// a frame that sync covered is no truer after a rebuild.
    pub(super) unproven: Unproven,
    /// The takeover the driver handed in (`crate::takeover`): an operator's,
    /// a restore's, or one this engine asked for before dropping its own
    /// staged files.
    pub(super) takeover: Option<TakeoverRecord>,
    /// A takeover the engine needs durable before it deletes staged files the
    /// replica holds: the driver writes it and hands it back.
    pub(super) wanted_takeover: Option<TakeoverRecord>,
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
    /// A boot attempt of this process has failed: with no refusal behind it, the boot is
    /// judged and the application may start (`crate::failstop`).
    pub(super) boot_failed: bool,
    /// A sync step failed after the first listing: a snapshot the boot asked for was tried.
    pub(super) sync_failed_after_listing: bool,
    /// Pending staged files that hold no commit of the application's: a file that changes no
    /// page, or a snapshot for a doubt alone staged while nothing was off the replica. They are
    /// not lag (`actor::lag`). At most one per pending file, so bounded by the witness tokens.
    pub(super) quiet: BTreeSet<Txid>,
    /// Where the WAL ended when a snapshot owed for a doubt alone was first observed, with
    /// every staged file on the replica: frames up to it are not lag (`actor::lag`).
    pub(super) doubt_baseline: Option<WalMark>,
    /// What the last boot's read of the journal reported. A report, not engine state: nothing
    /// decides on it.
    pub(super) journal: Option<JournalSignal>,
    /// The journal's writer, from the last boot that was lent a journal.
    pub(super) writer: Option<JournalWriter>,
    /// The record that boot read, if any: what is durable until this lifetime writes.
    pub(super) journal_read: Option<Journal>,
    /// How the last journal write ended: an error is `JournalUnwritable` until one succeeds.
    pub(super) journal_write: Result<(), ErrorKind>,
    /// Recoveries that found a commit abandoned after the read lock was taken: a report,
    /// kept across rebuilds, since the snapshot that follows names only the generation change.
    pub(super) read_lock_violations: u64,
    /// Whether the next sync step is the ahead-of-WAL check (`super::ahead`). A rebuild keeps
    /// it: the rebuilt engine boots over the same files.
    pub(super) ahead_check: AheadCheck,
    /// The standing ahead-of-WAL verdict, journalled, while its restore point is kept.
    pub(super) ahead: Option<lsr_journal::Ahead>,
    /// What the next tick tells the operator about the verdict, before anything else runs.
    pub(super) ahead_notice: Option<super::AheadNotice>,
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
            snapshots: SnapshotCounts::default(),
            final_snapshot: FinalSnapshot::NotAsked,
            unproven: Unproven::No,
            takeover: None,
            wanted_takeover: None,
            refused_through: None,
            lag: Lag::default(),
            listed: false,
            refused_at_boot: None,
            boot_failed: false,
            sync_failed_after_listing: false,
            quiet: BTreeSet::new(),
            doubt_baseline: None,
            journal: None,
            writer: None,
            journal_read: None,
            journal_write: Ok(()),
            read_lock_violations: 0,
            ahead_check: AheadCheck::No,
            ahead: None,
            ahead_notice: None,
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
            snapshots,
            final_snapshot,
            unproven,
            takeover,
            wanted_takeover: _,
            refused_through,
            lag,
            listed,
            refused_at_boot,
            boot_failed,
            sync_failed_after_listing,
            quiet: _,
            doubt_baseline: _,
            journal,
            writer,
            journal_read,
            journal_write,
            read_lock_violations,
            ahead_check,
            ahead,
            ahead_notice,
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
            snapshots: *snapshots,
            final_snapshot: *final_snapshot,
            unproven: *unproven,
            takeover: *takeover,
            // A rebuilt engine boots again and asks again for what it needs.
            wanted_takeover: None,
            refused_through: *refused_through,
            lag: *lag,
            listed: *listed,
            refused_at_boot: *refused_at_boot,
            boot_failed: *boot_failed,
            sync_failed_after_listing: *sync_failed_after_listing,
            // Beliefs about staged files: the rebuilt engine's boot judges them again.
            quiet: BTreeSet::new(),
            doubt_baseline: None,
            journal: *journal,
            writer: writer.clone(),
            journal_read: *journal_read,
            journal_write: *journal_write,
            read_lock_violations: *read_lock_violations,
            ahead_check: *ahead_check,
            ahead: *ahead,
            ahead_notice: *ahead_notice,
        }
    }

    /// The restore point retention keeps: the oldest of those asked for, a degraded task's, a
    /// scheduled verify's and an ahead-of-WAL verdict's. Retention keeps one chain, so while an
    /// older point is kept the verdict's is not; the older one is also before the damage.
    /// Derived on every read, so there is no copy to fall behind.
    pub(super) fn kept_restore_point(&self) -> Option<Txid> {
        [
            self.task_restore_point.txid(),
            self.verify_restore_point,
            self.ahead.map(|ahead| ahead.restore_point),
        ]
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
    /// The newest transaction any listing or commit of this engine
    /// confirmed on the replica. Never lowered, not even by a relist after the
    /// replica went backwards: an acknowledged id is never taken again for
    /// other pages, so a snapshot always goes above it.
    pub(super) acknowledged: Option<Txid>,
    /// The uploader's tokens for the pending staged files (`crate::witness`):
    /// held in memory only, so a new engine witnesses again at boot.
    pub(super) witnesses: Witnesses,
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
            acknowledged: None,
            witnesses: Witnesses::new(),
        }
    }
}

/// What each field of [`Kept`] is to a restart of the process (`crate::obligations`).
#[cfg(test)]
pub(super) const KEPT_LEDGER: &[(&str, Keep)] = &[
    (
        "cfg",
        Keep::Derived("the caller's configuration, given to every start"),
    ),
    (
        "stop",
        Keep::Ephemeral(
            "a stop holds until a person restarts the process: the restart is the way out \
             (docs/BEHAVIOR.md section 5)",
        ),
    ),
    (
        "tasks",
        Keep::Ephemeral(
            "retry waits and degraded reports; a restarted engine tries every task at once and \
             meets a failure that persists again",
        ),
    ),
    (
        "clock",
        Keep::Ephemeral(
            "engine time follows the wall clock from each start; the one journalled fact dated by \
             it, the ahead-of-WAL verdict, re-dates itself when the clock is behind",
        ),
    ),
    (
        "task_restore_point",
        Keep::Ephemeral(
            "pinned while a task is degraded and pinned again when the failure is met again; \
             cost: a retention pass that runs before a restarted engine meets the failure keeps \
             only the newest restore point",
        ),
    ),
    (
        "verify_restore_point",
        Keep::Derived("asked for again by the replica thread after every tick, from its verify"),
    ),
    (
        "live",
        Keep::Ephemeral("a verify's read of the live pages in flight; the verify asks again"),
    ),
    (
        "snapshot_asked",
        Keep::Ephemeral(
            "an operator's request to this process; cost: a restart before it is staged drops it",
        ),
    ),
    (
        "app",
        Keep::Ephemeral("what the application's checkpoints did between reads: counts reported"),
    ),
    (
        "suspect_staged",
        Keep::Ephemeral(
            "one failed read says nothing about the stored bytes; the next boot's sweep reads the \
             file again",
        ),
    ),
    (
        "snapshots",
        Keep::Ephemeral("snapshots staged since the process began: a report"),
    ),
    (
        "final_snapshot",
        Keep::Ephemeral(
            "a -once request of this process: a restart is a new run, which asks again or not",
        ),
    ),
    (
        "unproven",
        Keep::Journalled(
            Record::EngineJournal,
            "a failed WAL sync's debt: a restart before the cure must still pay it \
             (docs/decisions.md D100)",
        ),
    ),
    (
        "takeover",
        Keep::Journalled(
            Record::TakeoverRecord,
            "read from the meta directory by the driver and handed in at every start",
        ),
    ),
    (
        "wanted_takeover",
        Keep::Ephemeral(
            "nothing is deleted until the driver hands it back durable; a restart asks again",
        ),
    ),
    (
        "refused_through",
        Keep::Ephemeral("what a boot's refusal reports; a restarted boot refuses again"),
    ),
    (
        "lag",
        Keep::Derived(
            "the fail-stop clock, started again at boot from the pending staged files \
             (docs/decisions.md D80)",
        ),
    ),
    (
        "listed",
        Keep::Ephemeral("fail-stop's refusal at boot is about this process, not the database"),
    ),
    (
        "refused_at_boot",
        Keep::Ephemeral("fail-stop's refusal at boot is about this process, not the database"),
    ),
    (
        "boot_failed",
        Keep::Ephemeral("fail-stop's judgement of this process's boot"),
    ),
    (
        "sync_failed_after_listing",
        Keep::Ephemeral("fail-stop's judgement of this process's boot"),
    ),
    (
        "quiet",
        Keep::Derived("judged again by boot from the pending staged files"),
    ),
    (
        "doubt_baseline",
        Keep::Ephemeral(
            "lag accounting for this process; cost: frames before a restart count as lag \
             (docs/decisions.md D80)",
        ),
    ),
    (
        "journal",
        Keep::Ephemeral("what the last boot read of the journal: the next boot reads again"),
    ),
    (
        "writer",
        Keep::Ephemeral("the writer's place in the file: the next process reads the file again"),
    ),
    (
        "journal_read",
        Keep::Derived("the record this process's boot read"),
    ),
    (
        "journal_write",
        Keep::Ephemeral("whether the last journal write failed; tried again every tick"),
    ),
    (
        "read_lock_violations",
        Keep::Ephemeral("read-lock violations recovery found since the process began: a report"),
    ),
    (
        "ahead_check",
        Keep::Derived(
            "owed by a boot that restores a debt over the staged generation: judged again from \
             the journal and recovery at every boot",
        ),
    ),
    (
        "ahead",
        Keep::Journalled(
            Record::EngineJournal,
            "a database found ahead of its WAL: the restore point before it stays kept for the \
             window across restarts (docs/decisions.md D103)",
        ),
    ),
    (
        "ahead_notice",
        Keep::Ephemeral("what the next tick says; a boot that finds the verdict says it again"),
    ),
];

/// What each field of [`Engine`] is to a restart: a rebuild replaces the engine whole, so most
/// of it is derived at boot.
#[cfg(test)]
pub(super) const ENGINE_LEDGER: &[(&str, Keep)] = &[
    (
        "course",
        Keep::Derived("recovered at every boot from the newest staged file and the WAL"),
    ),
    (
        "page_size",
        Keep::Derived("read from the replica, staging and the WAL"),
    ),
    (
        "pages",
        Keep::Derived("seeded at boot from the staged files after the replica's newest"),
    ),
    (
        "checkpoint",
        Keep::Ephemeral("the checkpoint schedule, which starts again at boot"),
    ),
    (
        "gate",
        Keep::Ephemeral("when to ask about a checkpoint next; asked again after a restart"),
    ),
    (
        "upload",
        Keep::Derived("listed from the replica at every boot"),
    ),
    (
        "maintain",
        Keep::Ephemeral("the maintenance schedule and backoffs; the next listing rebuilds it"),
    ),
    (
        "pruned",
        Keep::Derived("what staging still holds tells the next boot what was pruned"),
    ),
    (
        "acknowledged",
        Keep::Owed(
            "never lowered while the process lives, but set at boot from the replica's listing: \
             a replica that went backwards before a restart lets a snapshot take an \
             acknowledged id again until the confirmed high-water mark is journalled \
             (issue #171, item 4)",
        ),
    ),
    (
        "witnesses",
        Keep::Derived("witnessed again at boot against the live WAL (crate::witness)"),
    ),
];
