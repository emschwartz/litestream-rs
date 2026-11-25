//! Fail-stop: whether replication's state holds or stops the application (`if-replication-stops`,
//! issue #115; the trade-offs are in `docs/decisions.md` D14 to D22).
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
//! **The clock** ([`Lag`]) is the time of the oldest commit not on the replica now, each kind
//! of lag dated on its own, never the start of an unbroken run of lag: a writer whose uploads
//! always trail by a tick has lag at the end of every tick, yet no commit older than a tick
//! or two. The dates, the oldest of which is the clock's start:
//!
//! - a pending staged file holding a commit: its stamp, which a restart does not reset, so a
//!   crash loop cannot run the application forever in slices shorter than the bound. A pending
//!   file that holds no commit is not one: a boot finds it by what the file changes against the
//!   staged file before it, or, with none staged before it, by the file's WAL span ending where
//!   the replica's newest file's ends (`crate::pages::Seeded::place`);
//! - WAL frames past the synced position: when this process first saw the WAL end where it
//!   did ([`WalMark`]). Once the synced position passes that mark, the date moves to the
//!   files the frames went into and lasts until the replica holds them; frames that arrived
//!   since date from the observation that saw them. WAL frames no life managed to stage carry
//!   no time of their own, so a restart restarts that part of the clock (`docs/decisions.md`
//!   D80);
//! - a snapshot the engine owes after it wrote unstaged frames into the database file (a
//!   TRUNCATE after a failed WAL sync, a checkpoint past the synced end): the date those
//!   frames had, since those commits are now in no staged file and in no WAL frame.
//!
//! Lag with no date of its own (a pending file with no stamp known) dates from when this
//! process first saw it, and only while it lasts. Every rule errs late, never early: a date
//! moves later when it cannot be kept exactly.
//!
//! **The probe.** Backoff alone would trip falsely: the store heals at 260 s and the next retry
//! is at 511 s. When the age reaches the bound, the actor lets the sync and upload waits go so
//! they run at once ([`Lag::probe_due`]); only a failure at or after the bound trips.
//!
//! **Clearing.** A trip for age clears once every commit as old as the clock's start it
//! tripped on has reached the replica: the start moves on. Not merely when the age falls
//! under the bound, so writes do not flap at the edge (a new trip needs a new bound and a
//! failure after it), and not only at zero age, which a busy database may never reach. A
//! refusal at boot clears at the first listing. A stop never clears without an operator.
//!
//! **What the trip does** is [`gate`]'s answer, per [`IfReplicationStops`]: `report` never
//! holds; `hold-start` holds an application that has not been released yet (an unsafe stop or
//! a refusal at boot); `stop-app` holds on every trip. [`AppGate`] latches the release.
//!
//! **Holding the application's writes** is [`blocks_writes`]'s answer: a held gate, and under
//! `hold-start` and `stop-app` also a boot not judged yet ([`Gate::Pending`]). The replicator
//! holds them with a write transaction of its own, which stops every writer whatever its
//! binding or process (`docs/decisions/D192.md`).

use std::time::Duration;

use lsr_ltx::{Timestamp, Txid};
use lsr_wal::Salts;

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
    RefusedAtStart(Denied),
}

impl Trip {
    /// Whether the trip ends without an operator: the age falling to zero, or the first
    /// listing. A stop never does.
    #[must_use]
    pub const fn clears_by_itself(self) -> bool {
        match self {
            Self::Stopped(_) => false,
            Self::UnreplicatedTooLong { .. } | Self::RefusedAtStart(_) => true,
        }
    }
}

/// Whether the application may run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Gate {
    /// The boot has not been judged yet (no listing, and no boot attempt that failed for
    /// anything but a refusal): an application not yet started waits, writes are held except
    /// under `report` ([`blocks_writes`]), and nothing is released.
    Pending,
    /// It may: started, writes accepted.
    Allowed,
    /// It is held: not started, writes refused, or stopped.
    Held(Trip),
}

/// Why the application's writes are held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhyBlocked {
    /// The boot is still being judged ([`Gate::Pending`]): listing the replica and deciding how
    /// this start proceeds. Bounded, since a boot attempt that fails judges the boot.
    Booting,
    /// The boot is still being judged, and is comparing the database with the replica's
    /// newest state before it decides (a brand-new meta directory, `crate::identity`): it
    /// restores that state and reads every local page, so it lasts as long as a restore of
    /// the database does. The host names it while the tick runs, from the engine's
    /// [`crate::Event::CheckingIdentity`] notice ([`crate::Env::notice`]); [`blocks_writes`]
    /// says [`WhyBlocked::Booting`].
    CheckingIdentity,
    /// Fail-stop tripped and holds the application ([`Gate::Held`]).
    Tripped(Trip),
}

/// Whether the application's writes are held under `mode` while the gate is `gate`, and why:
/// a held gate always, and a boot still being judged under `hold-start` and `stop-app`.
#[must_use]
pub const fn blocks_writes(mode: IfReplicationStops, gate: Gate) -> Option<WhyBlocked> {
    match (gate, mode) {
        (Gate::Held(trip), _) => Some(WhyBlocked::Tripped(trip)),
        (Gate::Pending, IfReplicationStops::HoldStart | IfReplicationStops::StopApp) => {
            Some(WhyBlocked::Booting)
        }
        (Gate::Pending, IfReplicationStops::Report) | (Gate::Allowed, _) => None,
    }
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
        (Some(StopCause::Shutdown) | None, Some(why), _) => Some(Trip::RefusedAtStart(why)),
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
                | StopCause::ConditionalWritesUnsupported
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
        return Gate::Allowed;
    };
    let holds = match (mode, trip) {
        (IfReplicationStops::Report, _) => false,
        (IfReplicationStops::HoldStart, Trip::Stopped(_) | Trip::RefusedAtStart(_)) => !released,
        (IfReplicationStops::HoldStart, Trip::UnreplicatedTooLong { .. }) => false,
        // A refusal at boot is about starting; once released, the store's refusal grows the
        // age like any other fault and trips for that.
        (IfReplicationStops::StopApp, Trip::RefusedAtStart(_)) => !released,
        (IfReplicationStops::StopApp, Trip::Stopped(_) | Trip::UnreplicatedTooLong { .. }) => true,
    };
    if holds {
        Gate::Held(trip)
    } else {
        Gate::Allowed
    }
}

/// Whether the application has been let run, which only the first open gate decides: under
/// `hold-start` an application released once is never held again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppGate {
    released: bool,
}

impl AppGate {
    /// The gate now, for a boot that has (`judged`) or has not been judged yet, latching the
    /// release when it is open. A trip is judgment enough: a stop or a refusal holds at once.
    pub fn judge(&mut self, mode: IfReplicationStops, trip: Option<Trip>, judged: bool) -> Gate {
        let gate = gate(mode, self.released, trip);
        match gate {
            Gate::Allowed if !judged && !self.released => Gate::Pending,
            Gate::Allowed => {
                self.released = true;
                gate
            }
            Gate::Pending | Gate::Held(_) => gate,
        }
    }

    /// Whether the application was ever let run.
    #[must_use]
    pub const fn released(self) -> bool {
        self.released
    }
}

/// Where the committed WAL ended: its generation and frame count. The clock dates WAL frames
/// by the first observation that saw the WAL end at a mark, until the synced position passes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalMark {
    salts: Salts,
    frames: u32,
}

impl WalMark {
    /// The WAL of generation `salts`, `frames` frames long.
    #[must_use]
    pub const fn new(salts: Salts, frames: u32) -> Self {
        Self { salts, frames }
    }

    /// Whether every frame up to this mark is behind the synced position `synced`: it reached
    /// the mark, or moved to another generation (the frames were staged before the WAL
    /// restarted, or a checkpoint took them, which leaves a snapshot owed).
    fn passed_by(self, synced: Option<Self>) -> bool {
        synced.is_some_and(|synced| synced.salts != self.salts || synced.frames >= self.frames)
    }
}

/// What the actor saw after a tick, for the clock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Observed {
    /// The committed WAL end, when it is past the synced position.
    pub ahead: Option<WalMark>,
    /// The synced position.
    pub synced: Option<WalMark>,
    /// The newest staged file, pending or not.
    pub staged: Option<Txid>,
    /// The newest pending staged file holding a commit, if any.
    pub pending: Option<Txid>,
    /// The newest transaction the replica holds.
    pub replicated: Option<Txid>,
    /// The stamp of the oldest pending staged file holding a commit, when known.
    pub oldest: Option<Timestamp>,
    /// The engine owes a snapshot for commits that are only in the database file.
    pub owed: bool,
}

/// The clock of the oldest unreplicated commit, and the age trip it drives. Pure: the actor
/// feeds it what it observes after each tick and every failure of the sync and upload tasks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Lag {
    /// When the WAL frames now past the synced position were first seen, where the WAL ended
    /// then, and the newest staged file then; kept while a snapshot owed holds those commits.
    unstaged: Option<Unstaged>,
    /// The date of frames now staged, until the replica holds the file `.0`, the first file
    /// staged after they were seen: they may be in later files too, but a date kept longer
    /// could outlive them, and one dropped early only trips late. One slot, the older kept: a
    /// later run of frames loses its date to its files' stamps, also a late error.
    carried: Option<(Txid, Timestamp)>,
    /// When this process first saw lag with no date of its own, while it lasts.
    seen: Option<Timestamp>,
    /// The clock's start: the oldest of the dates.
    since: Option<Timestamp>,
    /// The bound's instant a probe was made for, so each instant gets one.
    probed: Option<Timestamp>,
    /// The newest failure of the sync or upload task.
    failed: Option<Timestamp>,
    /// The age trip, latched with the clock's start it tripped on until that start has moved
    /// on: every commit that old has reached the replica.
    tripped: Option<Timestamp>,
}

/// WAL frames past the synced position, as the clock dates them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Unstaged {
    /// The observation that first saw them.
    at: Timestamp,
    /// Where the WAL ended then.
    mark: WalMark,
    /// The newest staged file then: the frames go into the files after it.
    after: Option<Txid>,
}

impl Lag {
    /// After a tick: what is off the replica, and the time.
    pub fn observe(&mut self, seen: Observed, bound: MaxUnreplicatedAge, now: Timestamp) {
        self.date_unstaged(seen, now);
        let undated =
            seen.pending.is_some() && seen.oldest.is_none() || seen.owed && self.unstaged.is_none();
        self.seen = if undated {
            Some(self.seen.unwrap_or(now))
        } else {
            None
        };
        let dates = [
            self.unstaged.map(|unstaged| unstaged.at),
            self.carried.map(|(_, at)| at),
            seen.oldest.filter(|_| seen.pending.is_some()),
            self.seen,
        ];
        let Some(oldest) = dates.into_iter().flatten().min() else {
            *self = Self::default();
            return;
        };
        // A date ahead of now (a clock that stepped back) says nothing older than now.
        let since = oldest.min(now);
        self.since = Some(since);
        self.tripped = self.tripped.filter(|tripped| since <= *tripped);
        let at = since.saturating_add(bound.get());
        if self.tripped.is_none() && now >= at && self.failed.is_some_and(|failed| failed >= at) {
            self.tripped = Some(since);
        }
    }

    /// Moves the date of WAL frames the synced position passed to the files they went into,
    /// drops a carried date the replica has passed, and dates frames newly seen.
    fn date_unstaged(&mut self, seen: Observed, now: Timestamp) {
        if let Some(unstaged) = self.unstaged
            && !seen.owed
            && unstaged.mark.passed_by(seen.synced)
        {
            self.unstaged = None;
            let first = unstaged.after.map_or(Some(Txid::MIN), Txid::next);
            if self.carried.is_none() {
                self.carried = first.map(|first| (first, unstaged.at));
            }
        }
        // Gone once the replica holds the file, or nothing pending holds a commit.
        self.carried = self.carried.filter(|(txid, _)| {
            seen.pending.is_some_and(|pending| pending >= *txid)
                && seen.replicated.is_none_or(|replicated| replicated < *txid)
        });
        match (seen.ahead, self.unstaged) {
            (Some(mark), None) => {
                self.unstaged = Some(Unstaged {
                    at: now,
                    mark,
                    after: seen.staged,
                });
            }
            (None, Some(_)) if !seen.owed => self.unstaged = None,
            (Some(_) | None, Some(_)) | (None, None) => {}
        }
    }

    /// Where the WAL ended when the frames the clock dates were first seen, while it dates any.
    #[must_use]
    pub fn unstaged_mark(&self) -> Option<WalMark> {
        self.unstaged.map(|unstaged| unstaged.mark)
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
        if now < at || self.probed == Some(at) || self.tripped.is_some() {
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
        (self.tripped.is_none() && self.probed != Some(at)).then_some(at)
    }

    /// The clock's start while anything is off the replica.
    #[must_use]
    pub const fn since(&self) -> Option<Timestamp> {
        self.since
    }

    /// The clock's start, when the age trip holds.
    #[must_use]
    pub const fn tripped(&self) -> Option<Timestamp> {
        match self.tripped {
            Some(_) => self.since,
            None => None,
        }
    }
}

#[cfg(test)]
mod tests;
