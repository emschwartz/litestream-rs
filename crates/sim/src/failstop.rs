//! The fail-stop judge: no false trip, over every run (`docs/proposals/fail-stop.md` section 2,
//! property P1).
//!
//! It shares nothing with the engine's clock (`lsr_core::Lag`). Its truth is the simulator's:
//! when the app committed, and whether a restore of the raw replica's `Latest` is the model's
//! newest image. From those it keeps the time of the first commit since the replica last held
//! everything, which is never later than the oldest commit truly off the replica: the age it
//! measures is an upper bound, so the judge can only be lenient, never raise a false alarm.
//! That leniency is the price of judging every run; the named scenarios
//! (`crates/sim/tests/sim/failstop.rs`) judge the other direction (P2, a missed trip; P3,
//! clearing; P5, a crash loop) where the run's truth is exact.
//!
//! Age is measured two ways and the larger is taken: on the clock the engine reads (a forward
//! step counts as time passing, which the engine cannot tell from a long pause), and on the
//! simulator's own time with every step taken out (a step back does not make a commit younger).
//!
//! What may trip, and what the judge asks of each:
//! - a stop: the run already judges every stop against what it expects;
//! - unreplicated too long: the age is at least the bound, and some attempt failed at or after
//!   the bound's instant (a store or staging fault fired, or the engine reported a retry or a
//!   degraded data-path task on that tick);
//! - refused at boot: a store refusal fired in this life, and this life has not booted.
//!
//! **The one exemption: committed WAL frames the engine never read.** The engine is judged on
//! what it can know (`docs/decisions.md` D81, amended). When the WAL holds committed frames, no
//! staged file waits for the replica (so the lag can only be those frames), and the engine has
//! read the WAL successfully at no point after the clock's start (no tick that booted, synced,
//! took a snapshot or checkpointed), it cannot tell whether those frames hold commits the
//! replica lacks, and an age trip is correct even if the simulator knows the
//! replica holds every commit (a vacuum that rewrote identical pages, say). Such trips are
//! excused and counted ([`FailStopJudge::excused`]), so a generator change that makes them
//! common shows.

use std::time::Duration;

use lsr_core::{Task, Trip};
use lsr_ltx::Timestamp;

use crate::oracle::Violation;

/// The simulator's view of what is off the replica.
#[derive(Clone, Debug, Default)]
pub struct FailStopJudge {
    /// The run's first instant, on both clocks: the earliest any commit can date from.
    first: Option<(Timestamp, i64)>,
    /// The first commit since the replica last held everything: on the engine's clock, and on
    /// the simulator's time with the clock's steps taken out.
    since: Option<(Timestamp, i64)>,
    /// The digest of every image the database held since then, the one before the first
    /// commit included. When the newest appears twice (a commit that changed nothing, a vacuum
    /// at the same size), a restore that shows it cannot tell which commit the replica holds.
    images: Vec<lsr_history::Digest>,
    /// The latest tick on which an attempt failed, on both clocks.
    failed: Option<(Timestamp, i64)>,
    /// A store refusal fired since the last start.
    refused: bool,
    /// The latest tick on which the engine read the WAL successfully, on its clock.
    wal_read: Option<Timestamp>,
    /// Age trips excused because the engine had not read the WAL's committed frames.
    excused: u64,
    /// The last tick's age trip was one the judge accepted, and how many ticks have passed
    /// since the replica was last seen whole: an accepted trip may take [`CLEAR_TICKS`] to
    /// clear after the catch-up (the engine learns of it at its next sync).
    cleared: Option<u32>,
    accepted: bool,
    /// An upload's answer could not be believed and no clean upload has landed since: the
    /// replica may hold what the engine cannot know it holds, so a restore that finds
    /// everything there does not count as caught up.
    doubt: bool,
}

/// Ticks an accepted age trip may take to clear once the replica is whole again: the engine
/// learns of the catch-up at its next upload or sync (P3, clearing, allows the same slack).
pub const CLEAR_TICKS: u32 = 4;

/// What one tick showed the judge.
#[derive(Clone, Copy, Debug)]
pub struct TickSeen {
    /// When it began, on the engine's clock.
    pub at: Timestamp,
    /// The same instant with every clock step taken out.
    pub true_at: i64,
    /// A store or staging fault fired in it.
    pub fault_fired: bool,
    /// The engine reported a retry, or a data-path task degraded.
    pub reported_failure: bool,
    /// A store refusal fired in it.
    pub refusal_fired: bool,
    /// The engine read the WAL successfully in it: it booted, synced, took a snapshot or
    /// checkpointed.
    pub read_wal: bool,
}

impl FailStopJudge {
    /// A judge for a run whose database already holds content at `start`, on no replica yet.
    #[must_use]
    pub fn starting(start: Timestamp, image: lsr_history::Digest) -> Self {
        let first = Some((start, start.unix_millis()));
        Self {
            first,
            since: first,
            images: vec![image],
            ..Self::default()
        }
    }

    /// The app committed at `at` (`true_at` with steps taken out), taking the database from the
    /// image `before` to `after`.
    pub fn committed(
        &mut self,
        at: Timestamp,
        true_at: i64,
        before: lsr_history::Digest,
        after: lsr_history::Digest,
    ) {
        if self.since.is_none() {
            self.images.clear();
            self.images.push(before);
        }
        self.since.get_or_insert((at, true_at));
        self.images.push(after);
    }

    /// Something outside the engine changed the replica (a rollback, an operator's delete,
    /// another writer): commits the judge counted as replicated may be gone, and it cannot tell
    /// which, so it dates what is off the replica from the run's start until it next sees the
    /// replica hold everything. Lenient, never strict.
    pub fn replica_changed_outside(&mut self) {
        self.since = self.first.or(self.since);
    }

    /// A restore of the replica's `Latest` is the model's newest image, `newest`. Ignored while
    /// an upload's answer is in doubt, and while that image is not one commit's alone.
    pub fn caught_up(&mut self, newest: lsr_history::Digest) {
        let alone = self.images.iter().filter(|image| **image == newest).count() <= 1;
        if !self.doubt && alone {
            self.since = None;
            self.images.clear();
            self.cleared = Some(0);
        }
    }

    /// After a tick: whether an upload answer in it could not be believed, and whether the
    /// engine took an upload as landed. Only the engine's own believed upload ends a doubt: a
    /// file another writer puts tells the engine nothing.
    pub const fn put(&mut self, doubtful: bool, believed: bool) {
        if doubtful {
            self.doubt = true;
        } else if believed {
            self.doubt = false;
        }
    }

    /// Whether anything may be off the replica.
    #[must_use]
    pub const fn behind(&self) -> bool {
        self.since.is_some()
    }

    /// A process start: refusals seen before it are the last life's.
    pub fn started(&mut self) {
        self.refused = false;
    }

    /// Takes in what a tick showed.
    pub fn saw(&mut self, tick: TickSeen) {
        self.cleared = self.cleared.map(|ticks| ticks.saturating_add(1));
        if tick.fault_fired || tick.reported_failure {
            self.failed = Some((tick.at, tick.true_at));
        }
        self.refused |= tick.refusal_fired;
        if tick.read_wal {
            self.wal_read = Some(tick.at);
        }
    }

    /// How many age trips were excused because the engine had not read the WAL's committed
    /// frames since its clock started.
    #[must_use]
    pub const fn excused(&self) -> u64 {
        self.excused
    }

    /// Whether `trip` is an age trip the engine could not have avoided: the WAL holds committed
    /// frames and nothing staged waits for the replica (`unstaged_only`), and the engine has
    /// read the WAL at no point after its clock began.
    fn unread_wal(&self, trip: Option<Trip>, unstaged_only: bool) -> bool {
        let Some(Trip::UnreplicatedTooLong { since }) = trip else {
            return false;
        };
        // A read in the tick that first saw the frames came before the sighting: the engine
        // reads during the tick and looks at the WAL's end after it.
        unstaged_only && self.wal_read.is_none_or(|read| read <= since)
    }

    /// The age of the first unreplicated commit at `tick`: the larger of the two clocks.
    fn age(&self, tick: TickSeen) -> Option<Duration> {
        let (wall, true_since) = self.since?;
        let on_wall = tick.at.saturating_duration_since(wall);
        let elapsed = u64::try_from(tick.true_at.saturating_sub(true_since)).unwrap_or(0);
        Some(on_wall.max(Duration::from_millis(elapsed)))
    }

    /// Judges the engine's trip after `tick`, for a life that has (`booted`) or has not booted.
    /// `unstaged_only`: the WAL holds committed frames and no staged file waits for the replica.
    ///
    /// # Errors
    /// [`Violation::FalseTrip`] when nothing the simulator knows explains it.
    pub fn judge(
        &mut self,
        trip: Option<Trip>,
        tick: TickSeen,
        bound: Duration,
        booted: bool,
        unstaged_only: bool,
    ) -> Result<(), Violation> {
        let lingering = self.accepted
            && self.since.is_none()
            && self.cleared.is_some_and(|ticks| ticks <= CLEAR_TICKS);
        let age_trip = matches!(trip, Some(Trip::UnreplicatedTooLong { .. }));
        let judged = if age_trip && lingering {
            Ok(())
        } else {
            match self.judge_now(trip, tick, bound, booted) {
                Err(_) if self.unread_wal(trip, unstaged_only) => {
                    self.excused = self.excused.saturating_add(1);
                    Ok(())
                }
                verdict => verdict,
            }
        };
        self.accepted = age_trip && judged.is_ok();
        judged
    }

    fn judge_now(
        &self,
        trip: Option<Trip>,
        tick: TickSeen,
        bound: Duration,
        booted: bool,
    ) -> Result<(), Violation> {
        let wrong = |detail: String| {
            Err(Violation::FalseTrip {
                trip: format!("{trip:?}"),
                detail,
            })
        };
        match trip {
            None | Some(Trip::Stopped(_)) => Ok(()),
            Some(Trip::RefusedAtStart(_)) if booted => {
                wrong("this life already listed the replica".to_owned())
            }
            Some(Trip::RefusedAtStart(_)) if !self.refused => {
                wrong("no store refusal fired in this life".to_owned())
            }
            Some(Trip::RefusedAtStart(_)) => Ok(()),
            Some(Trip::UnreplicatedTooLong { .. }) => {
                let Some(age) = self.age(tick) else {
                    return wrong("the replica holds every commit".to_owned());
                };
                if age < bound {
                    return wrong(format!("the oldest commit off the replica is {age:?} old"));
                }
                // On either clock: a failure at or after the bound's instant on that clock.
                let after = match (self.since, self.failed) {
                    (Some((wall, true_since)), Some((failed, true_failed))) => {
                        failed >= wall.saturating_add(bound)
                            || true_failed
                                >= true_since.saturating_add(
                                    i64::try_from(bound.as_millis()).unwrap_or(i64::MAX),
                                )
                    }
                    (None | Some(_), None) | (None, Some(_)) => false,
                };
                if after {
                    Ok(())
                } else {
                    wrong("no attempt failed at or after the bound's instant".to_owned())
                }
            }
        }
    }
}

/// Whether a degraded task's failure can keep a commit off the replica.
#[must_use]
pub const fn data_path(task: Task) -> bool {
    match task {
        Task::Sync | Task::Upload => true,
        Task::Checkpoint | Task::Prune | Task::Maintenance => false,
    }
}

/// How often a store's scheduled faults have fired: in all, as a refusal, and as an answer that
/// cannot be believed (the object published and the commit reported failed or wrong).
#[must_use]
pub fn fired(handle: &crate::store::FaultHandle) -> Fired {
    handle
        .hits()
        .iter()
        .fold(Fired::default(), |was, (_, shape, hits)| {
            let hits = u64::from(*hits);
            let (denied, doubtful) = match shape {
                crate::store::FaultShape::Error(err)
                | crate::store::FaultShape::Rate { err, .. } => {
                    (matches!(err, lsr_core::StoreError::Denied { .. }), false)
                }
                crate::store::FaultShape::ErrorAfterPublish(_)
                | crate::store::FaultShape::SizeMismatch
                | crate::store::FaultShape::ChecksumMismatch
                | crate::store::FaultShape::CorruptPublish => (false, true),
                crate::store::FaultShape::FlipByteOnRead { .. } => (false, false),
            };
            let only = |yes: bool| if yes { hits } else { 0 };
            Fired {
                all: was.all.saturating_add(hits),
                refused: was.refused.saturating_add(only(denied)),
                doubtful: was.doubtful.saturating_add(only(doubtful)),
            }
        })
}

/// Fault fires, counted by [`fired`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fired {
    /// Every fire.
    pub all: u64,
    /// Fires that were a store refusal.
    pub refused: u64,
    /// Fires whose commit published the object and answered failed or wrong.
    pub doubtful: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUND: Duration = Duration::from_secs(600);

    fn ts(millis: i64) -> Timestamp {
        Timestamp::from_unix_millis(millis)
    }

    fn tick(at: i64, read_wal: bool) -> TickSeen {
        TickSeen {
            at: ts(at),
            true_at: at,
            fault_fired: true,
            reported_failure: true,
            refusal_fired: false,
            read_wal,
        }
    }

    /// A judge that has seen the replica hold every commit: any age trip is false to it.
    fn caught_up() -> FailStopJudge {
        let image = lsr_history::Digest::genesis();
        let mut judge = FailStopJudge::starting(ts(0), image);
        judge.caught_up(image);
        judge
    }

    fn age_trip(since: i64) -> Option<Trip> {
        Some(Trip::UnreplicatedTooLong { since: ts(since) })
    }

    /// Committed frames the engine never read, with nothing staged waiting: excused, counted.
    #[test]
    fn a_trip_on_frames_never_read_is_excused_and_counted() {
        let mut judge = caught_up();
        judge.saw(tick(1_000, false));
        judge
            .judge(age_trip(1_000), tick(700_000, false), BOUND, true, true)
            .unwrap();
        assert_eq!(judge.excused(), 1);
    }

    /// A read in the tick that first saw the frames came before the sighting: still excused.
    #[test]
    fn a_read_in_the_sighting_tick_does_not_count() {
        let mut judge = caught_up();
        judge.saw(tick(1_000, true));
        judge
            .judge(age_trip(1_000), tick(700_000, false), BOUND, true, true)
            .unwrap();
    }

    /// A read after the clock's start, or a staged file waiting for the replica: not excused.
    #[test]
    fn a_later_read_or_a_waiting_staged_file_is_not_excused() {
        let mut judge = caught_up();
        judge.saw(tick(2_000, true));
        let later = judge.judge(age_trip(1_000), tick(700_000, false), BOUND, true, true);
        assert!(
            matches!(later, Err(Violation::FalseTrip { .. })),
            "{later:?}"
        );
        let mut judge = caught_up();
        let waiting = judge.judge(age_trip(1_000), tick(700_000, false), BOUND, true, false);
        assert!(
            matches!(waiting, Err(Violation::FalseTrip { .. })),
            "{waiting:?}"
        );
        assert_eq!(judge.excused(), 0);
    }
}
