//! What the actor is doing, and what each lane remembers between ticks.
//!
//! [`Phase`] is the one closed answer to "what is the actor doing". The lanes
//! keep only what a later tick needs: a retry wait, the checkpoint gate, and
//! when maintenance must list the replica again. None of it holds a borrow of
//! the environment.

use std::num::NonZeroU32;

use lsr_ltx::{Timestamp, Txid};

use crate::fault::{HaltCause, Next};
use crate::maintain::MaintainState;

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
    /// Stopped for a person: every later tick returns at once, with no I/O.
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

/// The four kinds of work, each with its own retry wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lane {
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
    /// keep one waits there (R3), not here.
    Maintain,
}

/// One lane's retry memory: how many failures in a row, and until when it waits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Retry {
    attempts: Option<NonZeroU32>,
    until: Option<Timestamp>,
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

    /// A success: the next failure starts from the first attempt.
    pub(super) fn succeed(&mut self) {
        *self = Self::default();
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
            Lane::Maintain => &self.maintain,
        }
    }

    pub(super) const fn get_mut(&mut self, lane: Lane) -> &mut Retry {
        match lane {
            Lane::Sync => &mut self.sync,
            Lane::Checkpoint => &mut self.checkpoint,
            Lane::Upload => &mut self.upload,
            Lane::Prune => &mut self.prune,
            Lane::Maintain => &mut self.maintain,
        }
    }

    pub(super) fn ready(&self, lane: Lane, now: Timestamp) -> bool {
        self.get(lane).ready(now)
    }

    /// Lets every passed wait go (W1: a passed deadline is work, not a wait).
    pub(super) fn expire(&mut self, now: Timestamp) {
        for lane in [
            Lane::Sync,
            Lane::Checkpoint,
            Lane::Upload,
            Lane::Prune,
            Lane::Maintain,
        ] {
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
}

impl Default for Maintain {
    fn default() -> Self {
        Self {
            state: MaintainState::default(),
            stale: true,
            until: None,
            level_one: None,
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
