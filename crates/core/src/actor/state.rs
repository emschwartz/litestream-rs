//! What the actor is doing, and what each task remembers between ticks.
//!
//! [`Phase`] is the one closed answer to "what is the actor doing". The tasks
//! keep only what a later tick needs: a retry wait, whether the run of
//! failures behind it is a persistent one (the task's [`Degraded`] state), the
//! checkpoint gate, and when maintenance must list the replica again. None of
//! it holds a borrow of the environment.

use std::num::NonZeroU32;

use lsr_ltx::{Timestamp, Txid};

use super::Actor;
use crate::fault::{Next, PersistentCause, StopCause};
use crate::layout::FileKey;
use crate::maintain::{MaintainState, RetentionMode};
use crate::verify::LivePages;

/// What the actor is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Not booted: the next tick lists the replica and recovers.
    Boot,
    /// Replicating: sync, checkpoint, upload and maintenance all run.
    Running,
    /// Shutting down: only sync and upload run, until the deadline.
    Draining {
        /// When work still left becomes [`StopCause::Shutdown`].
        deadline: Timestamp,
    },
    /// Stopped for a person, by a cause whose configured response is to stop
    /// or by a drain's deadline: every later tick returns at once, with no I/O.
    Stopped(StopCause),
}

/// The five kinds of work, each with its own retry wait and its own degraded
/// state: a failure in one task holds no other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Task {
    /// Reading the WAL into staged files (and booting).
    Sync,
    /// The checkpoint protocol.
    Checkpoint,
    /// Staged files to the replica.
    Upload,
    /// Deleting staged level-0 files the replica's level 1 covers.
    Prune,
    /// Listing the replica for maintenance, and a failed job whose level
    /// keeps no wait of its own (a retention delete). A job whose level does
    /// keep one waits there, not here, so one failing level starves no other, but its persistent failure is
    /// still this task's degraded state.
    Maintenance,
}

impl Task {
    /// Every task, in priority order.
    pub const ALL: [Self; 5] = [
        Self::Sync,
        Self::Checkpoint,
        Self::Upload,
        Self::Prune,
        Self::Maintenance,
    ];
}

/// A task that met a persistent failure and keeps trying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Degraded {
    /// The newest failure's cause.
    pub cause: PersistentCause,
    /// When the task first failed this way without a success between.
    pub since: Timestamp,
    /// Failures in a row, the first included.
    pub retries: u32,
}

/// Whether the cause is a database page that did not match: the one
/// degradation a task's success does not end.
const fn is_page_mismatch(cause: PersistentCause) -> bool {
    match cause {
        PersistentCause::DatabasePageMismatch { .. } => true,
        PersistentCause::StoreDenied(_)
        | PersistentCause::StoreRefused
        | PersistentCause::RemoteCorrupt
        | PersistentCause::OtherWriter
        | PersistentCause::RemoteRegressed
        | PersistentCause::WalCorrupt
        | PersistentCause::WalTruncated
        | PersistentCause::WalUnlinked
        | PersistentCause::WalSyncFailed
        | PersistentCause::PageSizeChanged { .. }
        | PersistentCause::Bug
        | PersistentCause::Stalled(_) => false,
    }
}

/// One task's retry memory: how many failures in a row, until when it waits,
/// and whether the run of failures is a persistent one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Retry {
    attempts: Option<NonZeroU32>,
    until: Option<Timestamp>,
    /// The degraded state counts its own failures: a maintenance job whose
    /// level keeps the wait clears `attempts` on every failure.
    degraded: Option<Degraded>,
}

impl Retry {
    /// Failures in a row so far.
    pub(super) fn failures(&self) -> u32 {
        self.attempts.map_or(0, NonZeroU32::get)
    }

    /// Whether the task may run at `now`.
    pub(super) fn ready(&self, now: Timestamp) -> bool {
        self.until.is_none_or(|until| now >= until)
    }

    /// When the task may run again, until a tick at or after it lets it go.
    pub(super) const fn until(&self) -> Option<Timestamp> {
        self.until
    }

    /// Lets a passed wait go: the task is ready, and its deadline is no
    /// longer one the actor holds. The failure count stays for the doubling.
    pub(super) fn expire(&mut self, now: Timestamp) {
        self.until = self.until.filter(|until| *until > now);
    }

    /// Counts one more failure (saturating: a retried fault never gives up)
    /// and waits until what `wait` makes of the count; returns that instant.
    pub(super) fn fail(&mut self, wait: impl FnOnce(NonZeroU32) -> Timestamp) -> Timestamp {
        let attempt = self
            .attempts
            .map_or(NonZeroU32::MIN, |attempts| attempts.saturating_add(1));
        self.attempts = Some(attempt);
        let until = wait(attempt);
        self.until = Some(until);
        until
    }

    /// Marks the task degraded by `cause`; returns when the degradation began,
    /// which a later failure of the same run keeps.
    pub(super) fn degrade(&mut self, cause: PersistentCause, now: Timestamp) -> Timestamp {
        let since = self.degraded.map_or(now, |was| was.since);
        let counted = self.degraded.map_or(1, |was| was.retries.saturating_add(1));
        self.degraded = Some(Degraded {
            cause,
            since,
            retries: counted.max(self.attempts.map_or(1, NonZeroU32::get)),
        });
        since
    }

    /// The task's degraded state, if it has one. Its count is never below
    /// the task's own: a failure that only retries counts too.
    pub(super) fn degraded(&self) -> Option<Degraded> {
        self.degraded.map(|was| Degraded {
            retries: was.retries.max(self.attempts.map_or(1, NonZeroU32::get)),
            ..was
        })
    }

    /// Forgets the wait and the count, not a degradation: a maintenance job
    /// whose level keeps its own wait failed.
    pub(super) const fn clear_wait(&mut self) {
        self.attempts = None;
        self.until = None;
    }

    /// A success: the next failure starts from the first attempt, and the
    /// task is no longer degraded, unless by a database page that did not
    /// match: shipping the file's bytes is the success, and the page stays
    /// reported until the table no longer holds it as mismatched
    /// ([`Tasks::page_matches_again`]).
    pub(super) fn succeed(&mut self) {
        let kept = self
            .degraded
            .filter(|was| is_page_mismatch(was.cause))
            .map(|was| Degraded { retries: 1, ..was });
        *self = Self {
            degraded: kept,
            ..Self::default()
        };
    }
}

/// Every task's retry memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Tasks {
    sync: Retry,
    checkpoint: Retry,
    upload: Retry,
    prune: Retry,
    maintain: Retry,
}

impl Tasks {
    pub(super) const fn get(&self, task: Task) -> &Retry {
        match task {
            Task::Sync => &self.sync,
            Task::Checkpoint => &self.checkpoint,
            Task::Upload => &self.upload,
            Task::Prune => &self.prune,
            Task::Maintenance => &self.maintain,
        }
    }

    pub(super) const fn get_mut(&mut self, task: Task) -> &mut Retry {
        match task {
            Task::Sync => &mut self.sync,
            Task::Checkpoint => &mut self.checkpoint,
            Task::Upload => &mut self.upload,
            Task::Prune => &mut self.prune,
            Task::Maintenance => &mut self.maintain,
        }
    }

    pub(super) fn ready(&self, task: Task, now: Timestamp) -> bool {
        self.get(task).ready(now)
    }

    /// Whether any task is degraded.
    pub(super) fn any_degraded(&self) -> bool {
        Task::ALL
            .iter()
            .any(|task| self.get(*task).degraded().is_some())
    }

    /// The page checksum table holds no mismatched page any more (a WAL frame
    /// rewrote it, the table was emptied, or an operator accepted it): no task
    /// is degraded by one.
    pub(super) fn page_matches_again(&mut self) {
        for task in Task::ALL {
            let retry = self.get_mut(task);
            if retry
                .degraded
                .is_some_and(|was| is_page_mismatch(was.cause))
            {
                retry.degraded = None;
            }
        }
    }

    /// Lets every passed wait go (W1: a passed deadline is work, not a wait).
    pub(super) fn expire(&mut self, now: Timestamp) {
        for task in Task::ALL {
            self.get_mut(task).expire(now);
        }
    }
}

/// When the checkpoint decision may be asked again, from [`crate::after_checkpoint`]'s
/// wake: a TRUNCATE that found the WAL unchanged waits rather than repeating.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Gate {
    /// Ask on any tick.
    Open,
    /// Ask from this time on.
    Until(Timestamp),
    /// Ask after the next sync that stages a file.
    NextWrite,
}

impl Gate {
    pub(super) const fn of(next: Next) -> Self {
        match next {
            Next::Now => Self::Open,
            Next::At(at) => Self::Until(at),
            Next::OnWalChange => Self::NextWrite,
        }
    }

    pub(super) fn open(self, now: Timestamp) -> bool {
        match self {
            Self::Open => true,
            Self::Until(at) => now >= at,
            Self::NextWrite => false,
        }
    }

    pub(super) const fn until(self) -> Option<Timestamp> {
        match self {
            Self::Until(at) => Some(at),
            Self::Open | Self::NextWrite => None,
        }
    }

    /// A passed `Until` is open: the decision is asked on this tick.
    pub(super) fn expire(self, now: Timestamp) -> Self {
        match self {
            Self::Until(at) if at <= now => Self::Open,
            Self::Until(_) | Self::Open | Self::NextWrite => self,
        }
    }
}

/// The live database's pages a scheduled verify compares a restore with:
/// asked for, read, taken. One value, so a request cannot be forgotten while
/// the pages it was answered with are dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Live {
    /// Nobody asked.
    Idle,
    /// Asked for; the next sync tick with the uploads caught up reads them.
    Wanted,
    /// Read, and not yet taken.
    Read(LivePages),
}

impl Live {
    /// Asks for the pages; pages already read and not taken are read again.
    pub(super) fn ask(&mut self) {
        *self = Self::Wanted;
    }

    pub(super) const fn wanted(&self) -> bool {
        match self {
            Self::Wanted => true,
            Self::Idle | Self::Read(_) => false,
        }
    }

    /// The pages, once: taken, nobody is asking any more.
    pub(super) fn take(&mut self) -> Option<LivePages> {
        match std::mem::replace(self, Self::Idle) {
            Self::Read(live) => Some(live),
            asked @ (Self::Idle | Self::Wanted) => {
                *self = asked;
                None
            }
        }
    }

    /// What a rebuilt engine starts with: pages read and not yet taken were
    /// read by the engine being replaced, so they are asked for again.
    pub(super) const fn rebuilt(&self) -> Self {
        match self {
            Self::Idle => Self::Idle,
            Self::Wanted | Self::Read(_) => Self::Wanted,
        }
    }
}

/// What maintenance remembers: the planner's state, and when a fresh listing
/// could change its answer.
#[derive(Clone, Debug)]
pub(super) struct Maintain {
    /// The planner's per-level backoff and distrusted outputs.
    pub(super) state: MaintainState,
    /// A job, a boot or a setting changed what a listing would decide: the
    /// next maintenance tick lists.
    pub(super) stale: bool,
    /// When the next listing is due: the last `Due::Wait.until`, brought
    /// forward by each upload to when its file can first matter.
    pub(super) until: Option<Timestamp>,
    /// The last transaction of the replica's level 1 in the last listing;
    /// `None` before the first. A staged file at or below it was compacted.
    pub(super) level_one: Option<Txid>,
    /// The read-back of the newest file per level is still owed: set at
    /// every boot, cleared by the first listing that finds them all sound.
    pub(super) read_back_owed: bool,
    /// A newest file whose last read-back failed verification. One failed
    /// read says nothing about the stored bytes, so the read-back acts on the
    /// file only when a read on a later maintenance tick fails too.
    pub(super) suspect: Option<FileKey>,
    /// Uploads run since prune or a maintenance listing last did work; at [`FAIR_SHARE`]
    /// they take the next unit ahead of uploads. Counted per process, so a boot with a
    /// backlog gets its first turn after that many uploads.
    pub(super) passed_over: u32,
    /// The last listing planned a snapshot while uploads were busy: the next listing waits
    /// for them to go idle, so the snapshot's last transaction is at most what was published.
    pub(super) snapshot_waits: bool,
}

/// How many uploads in a row run before prune or maintenance, when either has work, takes
/// a unit ahead of them. Uploads otherwise come first, so an engine whose uploads never go
/// idle (a backlog, a slow disk, a process killed every few seconds) would never compact,
/// prune staging or delete what retention lets go.
pub(super) const FAIR_SHARE: u32 = 8;

impl Default for Maintain {
    fn default() -> Self {
        Self {
            state: MaintainState::default(),
            stale: true,
            until: None,
            level_one: None,
            read_back_owed: true,
            suspect: None,
            passed_over: 0,
            snapshot_waits: false,
        }
    }
}

impl Maintain {
    /// A passed wait becomes a listing still owed, so it is no longer a
    /// deadline the actor holds while uploads keep maintenance back.
    pub(super) fn expire(&mut self, now: Timestamp) {
        if self.until.is_some_and(|until| until <= now) {
            self.until = None;
            self.stale = true;
        }
    }

    /// Brings the next listing forward to `at`, never back.
    pub(super) fn list_by(&mut self, at: Timestamp) {
        self.until = Some(self.until.map_or(at, |until| until.min(at)));
    }

    /// Whether `due` could answer differently than it last did.
    pub(super) fn wants_listing(&self, now: Timestamp) -> bool {
        self.stale || self.until.is_some_and(|until| now >= until)
    }
}

impl Actor {
    /// Switches retention on or off while the actor runs, as Go's
    /// `Store.SetRetentionEnabled` does (`store.go:573-581`). Compaction and
    /// snapshots are unaffected. The next maintenance tick lists the replica
    /// again, so switching it on resumes deletes under the usual rules at once
    /// rather than at the last wait's end. A restart takes the setting from
    /// the [`super::ActorConfig`] it is given.
    pub fn set_retention(&mut self, retention: RetentionMode) {
        self.kept.cfg.maintenance.set_retention(retention);
        self.engine.maintain.stale = true;
    }

    /// Whether retention deletes anything right now.
    #[must_use]
    pub const fn retention(&self) -> RetentionMode {
        self.kept.cfg.maintenance.retention()
    }
}
