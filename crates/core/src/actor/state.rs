//! What the actor is doing, and what each lane remembers between ticks.
//!
//! [`Phase`] is the one closed answer to "what is the actor doing". The lanes
//! keep only what a later tick needs: a retry wait, whether the run of
//! failures behind it is a persistent one (the lane's [`Degraded`] state), the
//! checkpoint gate, and when maintenance must list the replica again. None of
//! it holds a borrow of the environment.

use std::num::NonZeroU32;

use lsr_ltx::{Timestamp, Txid};

use super::Actor;
use crate::fault::{HaltCause, Next, PersistentCause};
use crate::maintain::{MaintainState, RetentionMode};

/// What the actor is doing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Not booted: the next tick lists the replica and recovers.
    Boot,
    /// Replicating: sync, checkpoint, upload and maintenance all run.
    Running,
    /// Shutting down: only sync and upload run, until the deadline.
    Draining {
        /// When work still left becomes [`HaltCause::Shutdown`].
        deadline: Timestamp,
    },
    /// Stopped for a person, by a cause whose configured response is to stop
    /// or by a drain's deadline: every later tick returns at once, with no I/O.
    Halted(HaltCause),
}

impl Phase {
    /// The phase after `begin_shutdown`. A halt stays a halt, and a second
    /// shutdown keeps the first deadline.
    pub(super) const fn drain(self, deadline: Timestamp) -> Self {
        match self {
            Self::Boot | Self::Running => Self::Draining { deadline },
            Self::Draining { .. } | Self::Halted(_) => self,
        }
    }

    /// The phase once booted: a drain that began before boot stays a drain.
    pub(super) const fn booted(self) -> Self {
        match self {
            Self::Boot => Self::Running,
            Self::Running | Self::Draining { .. } | Self::Halted(_) => self,
        }
    }
}

/// The five kinds of work, each with its own retry wait and its own degraded
/// state: a failure in one lane holds no other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
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
    /// keep one waits there (R3), not here, but its persistent failure is
    /// still this lane's degraded state.
    Maintenance,
}

impl Lane {
    /// Every lane, in priority order.
    pub const ALL: [Self; 5] = [
        Self::Sync,
        Self::Checkpoint,
        Self::Upload,
        Self::Prune,
        Self::Maintenance,
    ];
}

/// A lane that met a persistent failure and keeps trying.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Degraded {
    /// The newest failure's cause.
    pub cause: PersistentCause,
    /// When the lane first failed this way without a success between.
    pub since: Timestamp,
    /// Failures in a row, the first included.
    pub retries: u32,
}

/// One lane's retry memory: how many failures in a row, until when it waits,
/// and whether the run of failures is a persistent one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Retry {
    attempts: Option<NonZeroU32>,
    until: Option<Timestamp>,
    degraded: Option<(PersistentCause, Timestamp)>,
}

impl Retry {
    /// Whether the lane may run at `now`.
    pub(super) fn ready(&self, now: Timestamp) -> bool {
        self.until.is_none_or(|until| now >= until)
    }

    /// When the lane may run again, until a tick at or after it lets it go.
    pub(super) const fn until(&self) -> Option<Timestamp> {
        self.until
    }

    /// Lets a passed wait go: the lane is ready, and its deadline is no
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

    /// Marks the lane degraded by `cause`; returns when the degradation began,
    /// which a later failure of the same run keeps.
    pub(super) fn degrade(&mut self, cause: PersistentCause, now: Timestamp) -> Timestamp {
        let since = self.degraded.map_or(now, |(_, since)| since);
        self.degraded = Some((cause, since));
        since
    }

    /// The lane's degraded state, if it has one.
    pub(super) fn degraded(&self) -> Option<Degraded> {
        self.degraded.map(|(cause, since)| Degraded {
            cause,
            since,
            retries: self.attempts.map_or(1, NonZeroU32::get),
        })
    }

    /// Forgets the wait and the count, not a degradation: a maintenance job
    /// whose level keeps its own wait failed.
    pub(super) const fn clear_wait(&mut self) {
        self.attempts = None;
        self.until = None;
    }

    /// A success: the next failure starts from the first attempt, and the
    /// lane is no longer degraded, unless by a database page that did not
    /// match: shipping the file's bytes is the success, and the page stays
    /// reported until the table no longer holds it as mismatched
    /// ([`Lanes::page_matches_again`]).
    pub(super) fn succeed(&mut self) {
        let kept = self
            .degraded
            .filter(|(cause, _)| matches!(cause, PersistentCause::DatabasePageMismatch { .. }));
        *self = Self {
            degraded: kept,
            ..Self::default()
        };
    }
}

/// Every lane's retry memory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Lanes {
    sync: Retry,
    checkpoint: Retry,
    upload: Retry,
    prune: Retry,
    maintain: Retry,
}

impl Lanes {
    pub(super) const fn get(&self, lane: Lane) -> &Retry {
        match lane {
            Lane::Sync => &self.sync,
            Lane::Checkpoint => &self.checkpoint,
            Lane::Upload => &self.upload,
            Lane::Prune => &self.prune,
            Lane::Maintenance => &self.maintain,
        }
    }

    pub(super) const fn get_mut(&mut self, lane: Lane) -> &mut Retry {
        match lane {
            Lane::Sync => &mut self.sync,
            Lane::Checkpoint => &mut self.checkpoint,
            Lane::Upload => &mut self.upload,
            Lane::Prune => &mut self.prune,
            Lane::Maintenance => &mut self.maintain,
        }
    }

    pub(super) fn ready(&self, lane: Lane, now: Timestamp) -> bool {
        self.get(lane).ready(now)
    }

    /// Whether any lane is degraded.
    pub(super) fn any_degraded(&self) -> bool {
        Lane::ALL
            .iter()
            .any(|lane| self.get(*lane).degraded().is_some())
    }

    /// The page checksum table holds no mismatched page any more (a WAL frame
    /// rewrote it, the table was emptied, or an operator accepted it): no lane
    /// is degraded by one.
    pub(super) fn page_matches_again(&mut self) {
        for lane in Lane::ALL {
            let retry = self.get_mut(lane);
            if matches!(
                retry.degraded,
                Some((PersistentCause::DatabasePageMismatch { .. }, _))
            ) {
                retry.degraded = None;
            }
        }
    }

    /// Lets every passed wait go (W1: a passed deadline is work, not a wait).
    pub(super) fn expire(&mut self, now: Timestamp) {
        for lane in Lane::ALL {
            self.get_mut(lane).expire(now);
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

/// What maintenance remembers: the planner's state, and when a fresh listing
/// could change its answer.
#[derive(Clone, Debug)]
pub(super) struct Maintain {
    /// The planner's per-level backoff and distrusted outputs.
    pub(super) state: MaintainState,
    /// An upload or a job changed the replica since the last listing.
    pub(super) stale: bool,
    /// The last `Due::Wait.until`.
    pub(super) until: Option<Timestamp>,
    /// The last transaction of the replica's level 1 in the last listing;
    /// `None` before the first. A staged file at or below it was compacted.
    pub(super) level_one: Option<Txid>,
    /// The read-back of the newest file per level is still owed: set at
    /// every boot, cleared by the first listing that finds them all sound.
    pub(super) read_back_owed: bool,
}

impl Default for Maintain {
    fn default() -> Self {
        Self {
            state: MaintainState::default(),
            stale: true,
            until: None,
            level_one: None,
            read_back_owed: true,
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
        self.cfg.maintenance.set_retention(retention);
        self.maintain.stale = true;
    }

    /// Whether retention deletes anything right now.
    #[must_use]
    pub const fn retention(&self) -> RetentionMode {
        self.cfg.maintenance.retention()
    }
}
