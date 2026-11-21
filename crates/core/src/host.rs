//! What the host's own checkpoints look like from the replicator's side.
//!
//! A host that cannot be told to keep `wal_autocheckpoint = 0` (a sidecar's host, or an
//! in-process host that did not use `app_connection`) runs SQLite's automatic PASSIVE
//! checkpoint after every commit that leaves the WAL at or past its threshold, 1000 frames
//! by default. None of it costs the replica anything (the pin stops it), but it costs the
//! host, and the replicator should say so. The observer reads only what each poll already
//! reads (the wal-index's frame count, backfill count and salts) and the replicator's own
//! checkpoints, and reports three things:
//!
//! - [`HostCheckpoint::Backfilled`]: the backfill count rose within one generation with no
//!   checkpoint of ours since the last read. SQLite caps a checkpoint at the mark of the
//!   pin, so this shows only while the pin is on a read mark above 0.
//! - [`HostCheckpoint::Restarted`]: the generation changed although the last read was not
//!   fully backfilled and no checkpoint of ours came between, so someone else finished the
//!   backfill and restarted the log. A restart after a read that was fully backfilled is
//!   just the next writer starting a new generation (`walRestartLog`), not a checkpoint.
//! - [`HostCheckpoint::Stalled`]: the half the backfill count cannot show. With the pin on
//!   read mark 0 a checkpoint needs that mark exclusively, fails, and reports OK to a
//!   PASSIVE caller, so it leaves no trace in the wal-index (`docs/spikes.md`, the host's
//!   automatic checkpoints against a pin in another process). The observer infers it: the
//!   WAL was already at or past the host's threshold at the last read, it grew since (a
//!   host commit, so a host checkpoint attempt), nothing new was backfilled, and the
//!   backfill already stands where the pin stops any checkpoint (the *ceiling*). Every such
//!   attempt does nothing until the replicator checkpoints, and the WAL grows until then.
//!   Reported once per episode: an episode ends at a checkpoint of ours, a backfill or a
//!   new generation.
//!
//! The ceiling is where the pin stops a checkpoint. After our own checkpoint it comes from
//! what SQLite's checkpoint call reported as the pin was retaken: 0 when the call left the
//! WAL fully backfilled (the pin is then on mark 0, where nothing can be copied), else the
//! frame count (the pin's mark; the PASSIVE's write barrier keeps commits out until the
//! retake). Without that report it is the frame count at the read after, or at the first
//! read: an estimate from above, so that a wrong one can only miss a stall, never invent
//! one. A new generation sets it to 0 (SQLite restarts the log under a held pin only when the
//! pin is on mark 0), and a host checkpoint that stopped short sets it to where it stopped.
//!
//! The wal-index cannot tell a host at `wal_autocheckpoint = 0` from a host whose attempts
//! fail on mark 0: both leave the WAL growing with nothing backfilled. Where the pin is above
//! mark 0 the two differ (an attempt would have backfilled to the ceiling), and no stall is
//! inferred. On mark 0 only the threshold the observer is given tells them apart, so a host
//! known to run at 0 should be stated as `None`.
//!
//! None of this is a [`crate::Fault`] or a degraded state, and none of it changes a decision.

use std::num::NonZeroU32;

use crate::source::{CheckpointRun, CommittedRead};

#[cfg(test)]
mod tests;

/// SQLite's default `wal_autocheckpoint`, in frames: what a host that sets nothing runs.
pub const SQLITE_AUTOCHECKPOINT: NonZeroU32 = NonZeroU32::MIN.saturating_add(999);

/// One checkpoint, or one checkpoint attempt, of the host's, seen between two reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostCheckpoint {
    /// Someone else copied frames into the database file: the backfill count rose from
    /// `from` to `to` within one generation.
    Backfilled {
        /// The backfill count at the read before.
        from: u32,
        /// The backfill count now.
        to: u32,
    },
    /// Someone else finished the backfill and restarted the WAL.
    Restarted,
    /// The WAL grew past the host's automatic checkpoint threshold with nothing new copied
    /// into the database file: the host's attempts cannot run until the replicator
    /// checkpoints.
    Stalled {
        /// Frames in the WAL at the read that showed it.
        frames: u32,
    },
}

/// Every host checkpoint seen since the observer began.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HostCheckpoints {
    /// [`HostCheckpoint::Backfilled`] observations.
    pub backfilled: u64,
    /// [`HostCheckpoint::Restarted`] observations.
    pub restarted: u64,
    /// [`HostCheckpoint::Stalled`] episodes.
    pub stalled: u64,
    /// The newest observation.
    pub last: Option<HostCheckpoint>,
}

impl HostCheckpoints {
    /// Nothing seen.
    pub const NONE: Self = Self {
        backfilled: 0,
        restarted: 0,
        stalled: 0,
        last: None,
    };

    /// Checkpoints that ran and changed the wal-index: backfills and restarts.
    #[must_use]
    pub const fn ran(&self) -> u64 {
        self.backfilled.saturating_add(self.restarted)
    }

    /// Every observation, stalls included.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.ran().saturating_add(self.stalled)
    }

    /// These counts followed by `later`'s, whose newest observation wins: an engine that
    /// was replaced keeps what its predecessor saw.
    #[must_use]
    pub fn then(self, later: Self) -> Self {
        Self {
            backfilled: self.backfilled.saturating_add(later.backfilled),
            restarted: self.restarted.saturating_add(later.restarted),
            stalled: self.stalled.saturating_add(later.stalled),
            last: later.last.or(self.last),
        }
    }

    fn count(&mut self, seen: HostCheckpoint) {
        let bump = |n: &mut u64| *n = n.saturating_add(1);
        match seen {
            HostCheckpoint::Backfilled { .. } => bump(&mut self.backfilled),
            HostCheckpoint::Restarted => bump(&mut self.restarted),
            HostCheckpoint::Stalled { .. } => bump(&mut self.stalled),
        }
        self.last = Some(seen);
    }
}

/// What the observer remembers between two reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Watch {
    read: CommittedRead,
    ceiling: u32,
    stalled: bool,
}

impl Watch {
    /// The baseline a read sets when nothing came before it, or after a checkpoint of ours:
    /// the pin was taken at or before it, so its frame count bounds the pin's mark.
    #[must_use]
    pub const fn baseline(read: CommittedRead) -> Self {
        Self {
            read,
            ceiling: read.end().frame_count(),
            stalled: false,
        }
    }

    /// The baseline after a checkpoint of ours: `read` is the read after it, `run` what
    /// SQLite's checkpoint call reported as the pin was retaken, if the call returned.
    #[must_use]
    pub const fn after_ours(read: CommittedRead, run: Option<CheckpointRun>) -> Self {
        let ceiling = match run {
            Some(run) if run.checkpointed >= run.log => 0,
            Some(run) => run.log,
            None => read.end().frame_count(),
        };
        Self {
            read,
            ceiling,
            stalled: false,
        }
    }

    /// The read this watch was last given.
    #[must_use]
    pub const fn read(&self) -> CommittedRead {
        self.read
    }
}

/// One read against the last: what the host did between them, and the new watch.
/// `autocheckpoint` is the host's threshold, `None` for a host that runs none.
#[must_use]
pub fn observe(
    prev: Option<Watch>,
    now: CommittedRead,
    autocheckpoint: Option<NonZeroU32>,
) -> (Watch, Option<HostCheckpoint>) {
    let Some(prev) = prev else {
        return (Watch::baseline(now), None);
    };
    let before = prev.read;
    if before.end().salts() != now.end().salts() {
        let seen = (!before.fully_backfilled()).then_some(HostCheckpoint::Restarted);
        let watch = Watch {
            read: now,
            ceiling: 0,
            stalled: false,
        };
        return (watch, seen);
    }
    if now.backfilled() > before.backfilled() {
        let watch = Watch {
            read: now,
            ceiling: now.backfilled(),
            stalled: false,
        };
        let seen = HostCheckpoint::Backfilled {
            from: before.backfilled(),
            to: now.backfilled(),
        };
        return (watch, Some(seen));
    }
    let frames = now.end().frame_count();
    let stalled = !prev.stalled
        && autocheckpoint.is_some_and(|at| before.end().frame_count() >= at.get())
        && frames > before.end().frame_count()
        && now.backfilled() >= prev.ceiling;
    let watch = Watch {
        read: now,
        ceiling: prev.ceiling,
        stalled: prev.stalled || stalled,
    };
    (watch, stalled.then_some(HostCheckpoint::Stalled { frames }))
}

/// Watches the host's checkpoints across polls: [`observe`] with its state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostObserver {
    autocheckpoint: Option<NonZeroU32>,
    watch: Option<Watch>,
    counts: HostCheckpoints,
}

impl Default for HostObserver {
    fn default() -> Self {
        Self::new(Some(SQLITE_AUTOCHECKPOINT))
    }
}

impl HostObserver {
    /// An observer of a host whose automatic checkpoint runs at `autocheckpoint` frames
    /// (`None`: the host runs none).
    #[must_use]
    pub const fn new(autocheckpoint: Option<NonZeroU32>) -> Self {
        Self {
            autocheckpoint,
            watch: None,
            counts: HostCheckpoints::NONE,
        }
    }

    /// The host's threshold the observer assumes.
    #[must_use]
    pub const fn autocheckpoint(&self) -> Option<NonZeroU32> {
        self.autocheckpoint
    }

    /// Changes the host's threshold; what was seen stays.
    pub const fn set_autocheckpoint(&mut self, autocheckpoint: Option<NonZeroU32>) {
        self.autocheckpoint = autocheckpoint;
    }

    /// One poll's read of the wal-index, and what it shows the host did since the last.
    pub fn poll(&mut self, now: CommittedRead) -> Option<HostCheckpoint> {
        let (watch, seen) = observe(self.watch, now, self.autocheckpoint);
        self.watch = Some(watch);
        if let Some(seen) = seen {
            self.counts.count(seen);
        }
        seen
    }

    /// A checkpoint of ours ran (or failed): `after` is the read after it, if one could be
    /// made, and `run` what SQLite's checkpoint call reported, if it returned. Whatever
    /// changed is ours, and the read is the next baseline.
    pub fn ours(&mut self, after: Option<CommittedRead>, run: Option<CheckpointRun>) {
        self.watch = after.map(|read| Watch::after_ours(read, run));
    }

    /// Everything seen so far.
    #[must_use]
    pub const fn counts(&self) -> HostCheckpoints {
        self.counts
    }
}
