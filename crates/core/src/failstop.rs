//! Fail-stop: whether replication's state holds or stops the application (`if-replication-stops`,
//! issue #115; the design is `docs/proposals/fail-stop.md`, the trade-offs `docs/decisions.md`
//! D14 to D22).
//!
//! Three things trip it, and nothing else ([`Trip`]):
//!
//! 1. **An unsafe stop.** The engine stopped for any cause but the end of a drain. A stop is
//!    terminal, so this has no bound and never clears by itself.
//! 2. **Unreplicated too long.** A commit has been off the replica for [`MaxUnreplicatedAge`],
//!    and an attempt to move it, made at or after the bound, failed. The cause of the delay is
//!    only the label. A task that is degraded while uploads still land keeps the age at zero
//!    and never trips; that one rule keeps blips, throttling and maintenance faults from taking
//!    the application down.
//! 3. **Refused at boot.** Before this process has listed the replica once, the store refuses
//!    it ([`crate::Denied`]). Timeouts, 5xx and throttling never hold. The first successful
//!    listing ends it for the life of the process.
//!
//! **The clock** ([`Lag`]) is the time of the oldest commit not on the replica: the stamp of
//! the oldest pending staged file, which a restart does not reset, or the first time this
//! process saw a commit it had not staged, whichever is older. A crash loop therefore cannot
//! run the application forever in slices shorter than the bound, as long as each life stages
//! what it reads. WAL frames no life managed to stage carry no time of their own; they count
//! from the first life that sees them, and a restart restarts that part of the clock
//! (`docs/decisions.md` D25). A snapshot the engine owes after it wrote unstaged frames into
//! the database file (a TRUNCATE after a failed WAL sync, a checkpoint past the synced end)
//! keeps the lag that was there: those commits are in no staged file and in no WAL frame.
//!
//! **The probe.** Backoff alone would trip falsely: the store heals at 260 s and the next retry
//! is at 511 s. When the age reaches the bound, the actor lets the sync and upload waits go so
//! they run at once ([`Lag::probe_due`]); only a failure at or after the bound trips.
//!
//! **Clearing.** A trip for age clears when the age is zero again, not merely under the bound,
//! so writes do not flap at the edge. A refusal at boot clears at the first listing. A stop
//! never clears without an operator.
//!
//! **What the trip does** is [`gate`]'s answer, per [`IfReplicationStops`]: `report` never
//! holds; `hold-start` holds an application that has not been released yet (an unsafe stop or
//! a refusal at boot); `stop-app` holds on every trip. [`AppGate`] latches the release.

use std::time::Duration;

use lsr_ltx::Timestamp;

use crate::fault::{MAX_BACKOFF, StopCause};
use crate::store::Denied;

/// The bound on how long a commit may stay off the replica before [`Trip::UnreplicatedTooLong`].
/// At least [`MaxUnreplicatedAge::MIN`], so one capped retry always fits inside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MaxUnreplicatedAge(Duration);

impl MaxUnreplicatedAge {
    /// The shortest bound accepted: the backoff cap.
    pub const MIN: Duration = MAX_BACKOFF;

    /// The default: past every blip and short incident, and past the retries at 255 s and
    /// 511 s (`docs/decisions.md` D22).
    pub const DEFAULT: Self = Self(Duration::from_secs(10 * 60));

    /// `None` below [`Self::MIN`].
    #[must_use]
    pub const fn new(bound: Duration) -> Option<Self> {
        if bound.as_millis() < Self::MIN.as_millis() {
            return None;
        }
        Some(Self(bound))
    }

    /// The bound.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Default for MaxUnreplicatedAge {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a trip does to the application (`docs/decisions.md` D13, D14).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IfReplicationStops {
    /// Health and metrics say so; the application is never held.
    Report,
    /// An unsafe stop or a refusal at boot holds an application not yet started; once it has
    /// been released, a trip only reports.
    #[default]
    HoldStart,
    /// Every trip holds the application, started or not.
    StopApp,
}

/// Why fail-stop tripped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trip {
    /// The engine stopped for a cause other than the end of a drain.
    Stopped(StopCause),
    /// The oldest commit off the replica dates from `since`, the bound has passed, and an
    /// attempt made after the bound failed.
    UnreplicatedTooLong {
        /// The clock's start: the oldest unreplicated commit as far as it is known.
        since: Timestamp,
    },
    /// The store refused this process before it listed the replica once.
    NotYetReachable(Denied),
}

impl Trip {
    /// Whether the trip ends without an operator: the age falling to zero, or the first
    /// listing. A stop never does.
    #[must_use]
    pub const fn clears_by_itself(self) -> bool {
        match self {
            Self::Stopped(_) => false,
            Self::UnreplicatedTooLong { .. } | Self::NotYetReachable(_) => true,
        }
    }
}

/// Whether the application may run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    /// It may: started, writes accepted.
    Open,
    /// It is held: not started, writes refused, or stopped.
    Held(Trip),
}

/// The engine's trip, from what it knows. A stop wins over the others: it is terminal.
#[must_use]
pub const fn trip(
    stopped: Option<StopCause>,
    refused_at_boot: Option<Denied>,
    unreplicated: Option<Timestamp>,
) -> Option<Trip> {
    match (stopped, refused_at_boot, unreplicated) {
        (Some(StopCause::Shutdown) | None, None, None) => None,
        (Some(StopCause::Shutdown) | None, Some(why), _) => Some(Trip::NotYetReachable(why)),
        (Some(StopCause::Shutdown) | None, None, Some(since)) => {
            Some(Trip::UnreplicatedTooLong { since })
        }
        (
            Some(
                cause @ (StopCause::StoreDenied(_)
                | StopCause::StoreRefused
                | StopCause::RemoteCorrupt
                | StopCause::OtherWriter
                | StopCause::RemoteRegressed
                | StopCause::WalCorrupt
                | StopCause::WalTruncated
                | StopCause::WalUnlinked
                | StopCause::WalSyncFailed
                | StopCause::PageSizeChanged { .. }
                | StopCause::DatabasePageMismatch { .. }
                | StopCause::Bug),
            ),
            _,
            _,
        ) => Some(Trip::Stopped(cause)),
    }
}

/// What a trip does under `mode`, for an application that has (`released`) or has not yet
/// been let run.
#[must_use]
pub const fn gate(mode: IfReplicationStops, released: bool, trip: Option<Trip>) -> Gate {
    let Some(trip) = trip else {
        return Gate::Open;
    };
    let holds = match (mode, trip) {
        (IfReplicationStops::Report, _) => false,
        (IfReplicationStops::HoldStart, Trip::Stopped(_) | Trip::NotYetReachable(_)) => !released,
        (IfReplicationStops::HoldStart, Trip::UnreplicatedTooLong { .. }) => false,
        // A refusal at boot is about starting; once released, the store's refusal grows the
        // age like any other fault and trips for that.
        (IfReplicationStops::StopApp, Trip::NotYetReachable(_)) => !released,
        (IfReplicationStops::StopApp, Trip::Stopped(_) | Trip::UnreplicatedTooLong { .. }) => true,
    };
    if holds { Gate::Held(trip) } else { Gate::Open }
}

/// Whether the application has been let run, which only the first open gate decides: under
/// `hold-start` an application released once is never held again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppGate {
    released: bool,
}

impl AppGate {
    /// The gate now, latching the release when it is open.
    pub fn judge(&mut self, mode: IfReplicationStops, trip: Option<Trip>) -> Gate {
        let gate = gate(mode, self.released, trip);
        if gate == Gate::Open {
            self.released = true;
        }
        gate
    }

    /// Whether the application was ever let run.
    #[must_use]
    pub const fn released(self) -> bool {
        self.released
    }
}

/// The clock of the oldest unreplicated commit, and the age trip it drives. Pure: the actor
/// feeds it what it observes after each tick and every failure of the sync and upload tasks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lag {
    /// When this process first saw the current run of lag; `None` while caught up.
    seen: Option<Timestamp>,
    /// The clock's start: `seen` or the oldest pending stamp, the older.
    since: Option<Timestamp>,
    /// The bound's instant a probe was made for, so each instant gets one.
    probed: Option<Timestamp>,
    /// The newest failure of the sync or upload task.
    failed: Option<Timestamp>,
    /// The age trip, latched until the age is zero.
    tripped: bool,
}

impl Lag {
    /// After a tick: whether anything committed is off the replica, the stamp of the oldest
    /// pending staged file, and the time.
    pub fn observe(
        &mut self,
        behind: bool,
        oldest: Option<Timestamp>,
        bound: MaxUnreplicatedAge,
        now: Timestamp,
    ) {
        if !behind {
            *self = Self::default();
            return;
        }
        let seen = *self.seen.get_or_insert(now);
        // A stamp ahead of now (a clock that stepped back) says nothing older than now.
        let since = oldest.map_or(seen, |stamp| stamp.min(seen)).min(now);
        self.since = Some(since);
        let at = since.saturating_add(bound.get());
        if now >= at && self.failed.is_some_and(|failed| failed >= at) {
            self.tripped = true;
        }
    }

    /// The sync or upload task failed at `now`.
    pub const fn failed(&mut self, now: Timestamp) {
        self.failed = Some(now);
    }

    /// Whether the waits of the sync and upload tasks should be let go now: the bound has
    /// passed and no probe was made for it. Marks the probe made.
    pub fn probe_due(&mut self, bound: MaxUnreplicatedAge, now: Timestamp) -> bool {
        let Some(since) = self.since else {
            return false;
        };
        let at = since.saturating_add(bound.get());
        if now < at || self.probed == Some(at) || self.tripped {
            return false;
        }
        self.probed = Some(at);
        true
    }

    /// When the next probe is due, while one is: the instant the actor must wake at even with
    /// no other work, or a host asleep on a long retry wait would only probe at its end.
    #[must_use]
    pub fn next_probe(&self, bound: MaxUnreplicatedAge) -> Option<Timestamp> {
        let at = self.since?.saturating_add(bound.get());
        (!self.tripped && self.probed != Some(at)).then_some(at)
    }

    /// The clock's start while anything is off the replica.
    #[must_use]
    pub const fn since(&self) -> Option<Timestamp> {
        self.since
    }

    /// The clock's start, when the age trip holds.
    #[must_use]
    pub const fn tripped(&self) -> Option<Timestamp> {
        if self.tripped { self.since } else { None }
    }
}

#[cfg(test)]
mod tests;
