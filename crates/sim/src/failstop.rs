//! The fail-stop judge: no false trip, over every run.
//!
//! It shares nothing with the engine's clock (`lsr_core::Lag`). Its truth is the simulator's:
//! when the app committed, and whether a restore of the raw replica's `Latest` is the model's
//! newest image. From those it keeps the time of the first commit since the replica last held
//! everything (on the engine's clock the earliest stamp among them, or the earliest reading of
//! the clock at a tick since then: after a step back the engine can date frames by a sighting
//! earlier than any commit's stamp), which is never later than the oldest commit truly off the replica: the age it
//! measures is an upper bound, so the judge can only be lenient, never raise a false alarm.
//! That leniency is the price of judging every run; the named scenarios
//! (`crates/sim/tests/sim/failstop.rs`) judge the other direction (P2, a missed trip; P3,
//! clearing; P5, a crash loop) where the run's truth is exact.
//!
//! Age is measured two ways and the larger is taken: on the clock the engine reads (a forward
//! step counts as time passing, which the engine cannot tell from a long pause), and on the
//! simulator's own time with every step taken out (a step back does not make a commit younger).
//!
//! A trip it accepted stands on the later ticks while its clock's start has not moved later and the
//! replica still lacks commits: the engine clears a trip when the start moves on, never when the
//! age falls under the bound (`lsr_core::failstop`, Clearing), so a clock stepped back that
//! leaves the age short of the bound is the same trip, not an early one. The same holds while
//! the engine's own clock still runs, though the replica holds every commit the simulator knows:
//! an accepted trip over frames the engine never read lasts until the engine lets go of them.
//!
//! What may trip, and what the judge asks of each:
//! - a stop: the run already judges every stop against what it expects;
//! - unreplicated too long: the age is at least the bound, and some attempt failed at or after
//!   the bound's instant (a store or staging fault fired, or the engine reported a retry or a
//!   degraded data-path task on that tick);
//! - refused at boot: a store refusal fired in this life, and this life has not booted.
//!
//! Four exemptions, each excused and counted on its own ([`Excused`]), so a generator change that
//! makes any of them common shows.
//!
//! **Committed WAL frames the engine never read.** The engine is judged on
//! what it can know (`docs/decisions.md` D81, amended). When the WAL holds committed frames, no
//! staged file waits for the replica (so the lag can only be those frames), and the engine has
//! read the WAL successfully at no point after the clock's start (no tick that booted, synced,
//! took a snapshot or checkpointed), it cannot tell whether those frames hold commits the
//! replica lacks, and an age trip is correct even if the simulator knows the
//! replica holds every commit (a vacuum that rewrote identical pages, say).
//!
//! **A snapshot taken at a boot that broke continuity.** When this life booted with a logged
//! cause that says continuity broke (staged files the live WAL could not vouch for were deleted,
//! or the WAL rewound: `SnapshotCause::StagedUnverified`, `SnapshotCause::LocalRewound`), the
//! snapshot it stages may hold commits that were never staged, and the engine has nothing to
//! compare it with (Go writes LTX files without a checksum, so none is owed). Until the engine's
//! own upload lands after that boot, that snapshot is lag it cannot rule out, and an age trip
//! dated at or after the boot is correct even if the simulator knows the replica holds every
//! commit. Only an age trip, only in the life that booted so, only before its first upload.

//!
//! **Reads of the replica that lie.** While `Hold::LyingReads` holds, every read the engine
//! makes of the replica returns a flipped byte, so it cannot confirm that its newest upload
//! landed: it dates that file from its stamp, and an age trip over a replica the simulator knows
//! is whole is correct while the lie lasts. Only an age trip, only during the hold; the trip
//! clears once reads are honest (a trip lingers for `CLEAR_TICKS` after the catch-up).
//!
//! **A staged file an earlier life left that the boot cannot place.** A restart forgets which
//! pending staged files hold no commit. The boot finds one again by comparing it with the staged
//! file before it, or, with none, by its WAL span ending where the replica's newest level-0
//! file's ends; a snapshot over a restarted WAL ends in another generation and stays counted
//! (`docs/decisions/D282.md`). Only an age trip dated at that file's header stamp, only while the
//! simulator knows the replica holds every commit, only for the first pending file, when this
//! life found it in staging at its start, no staged file stands before it, and its WAL salts are
//! not those of the replica's file at the replica's newest transaction (or there is none).

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
    /// When this life booted with a cause that broke continuity, on the engine's clock; cleared
    /// by the engine's first believed upload after it, and by a restart.
    unvouched: Option<Timestamp>,
    /// Age trips excused, by exemption.
    excused: Excused,
    /// How many ticks have passed since the replica was last seen whole: an accepted trip may
    /// take [`CLEAR_TICKS`] to clear after the catch-up (the engine learns of it at its next sync).
    cleared: Option<u32>,
    /// The clock's start of the last tick's age trip, when the judge accepted it. The engine
    /// holds a trip until its clock's start moves on, whatever the age does meanwhile, so the
    /// same trip over the same commits is accepted on the later ticks too.
    accepted: Option<Timestamp>,
    /// The start of the engine's own clock as of the last tick, while it runs. A trip accepted
    /// for frames the engine never read stands until the engine itself lets go of them, which
    /// it cannot do while it still dates them, whatever the replica holds (the engine cannot
    /// tell a vacuum that rewrote identical pages from a lost commit).
    engine_since: Option<Timestamp>,
    /// Whether the tick before the last one ran while every read of the replica lied.
    lied_before: bool,
    /// Whether the last tick ran while every read of the replica lied.
    lied_last: bool,
    /// The header stamp of a pending staged file an earlier life left that the boot cannot
    /// place against the replica (`Self::unplaced`).
    unplaced: Option<Timestamp>,
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
    /// When its reads ended, on the engine's clock: its start, and what it stalled inside.
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
    /// The engine booted in it with a logged cause that broke continuity.
    pub unvouched_boot: bool,
    /// Every read of the replica lied in it (`Hold::LyingReads`).
    pub reads_lying: bool,
}

/// Age trips the judge excused, by exemption.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Excused {
    /// On committed WAL frames the engine never read.
    pub unread_wal: u64,
    /// While a snapshot taken at a boot that broke continuity was not uploaded.
    pub unvouched_snapshot: u64,
    /// While every read of the replica lied.
    pub lying_reads: u64,
    /// On a pending staged file an earlier life left that the boot could not place.
    pub unplaced_file: u64,
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
        // The engine dates each commit by its own clock's stamp and takes the oldest, so after a
        // step back a later commit can date earlier than the first: the wall date is the least
        // of them, which only lengthens the age the judge measures. The stepless date never goes
        // back, so the first commit's stays.
        let (wall, true_since) = self.since.unwrap_or((at, true_at));
        self.since = Some((wall.min(at), true_since));
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
        if believed {
            self.unvouched = None;
        }
    }

    /// Whether anything may be off the replica.
    #[must_use]
    pub const fn behind(&self) -> bool {
        self.since.is_some()
    }

    /// A process start: refusals seen before it are the last life's.
    pub const fn started(&mut self) {
        self.refused = false;
        self.unvouched = None;
    }

    /// Takes in what a tick showed.
    pub fn saw(&mut self, tick: TickSeen) {
        self.lied_before = self.lied_last;
        self.lied_last = tick.reads_lying;
        self.cleared = self.cleared.map(|ticks| ticks.saturating_add(1));
        if tick.fault_fired || tick.reported_failure {
            self.failed = Some((tick.at, tick.true_at));
        }
        self.refused |= tick.refusal_fired;
        // The engine dates WAL frames by the observation that first saw them, and after a clock
        // step back that reading is earlier than every later commit's stamp: the wall date is
        // also the least reading of the clock since the first commit.
        if let Some((wall, true_since)) = self.since {
            self.since = Some((wall.min(tick.at), true_since));
        }
        if tick.read_wal {
            self.wal_read = Some(tick.at);
        }
        if tick.unvouched_boot {
            self.unvouched = Some(tick.at);
        }
    }

    /// How many age trips were excused, by exemption.
    #[must_use]
    pub const fn excused(&self) -> Excused {
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

    /// Tells the judge the header stamp of the first pending staged file, when an earlier life
    /// left it with no staged file before it and its WAL span in another generation than the
    /// replica's newest level-0 file's (or that file is gone): the boot has nothing to compare
    /// it with and counts it as a commit (`lsr_core`'s `Seeded::place`).
    pub const fn unplaced(&mut self, stamp: Option<Timestamp>) {
        self.unplaced = stamp;
    }

    /// Whether `trip` is an age trip dated by the unplaced staged file, while the judge knows the
    /// replica holds every commit.
    fn unplaced_file(&self, trip: Option<Trip>) -> bool {
        let Some(Trip::UnreplicatedTooLong { since }) = trip else {
            return false;
        };
        self.since.is_none() && self.unplaced == Some(since)
    }

    /// Whether `trip` is an age trip dated at or after a boot of this life that broke
    /// continuity, before the engine's first upload since.
    fn unvouched_snapshot(&self, trip: Option<Trip>) -> bool {
        let Some(Trip::UnreplicatedTooLong { since }) = trip else {
            return false;
        };
        self.unvouched.is_some_and(|booted| since >= booted)
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
        let lingering = self.accepted.is_some()
            && self.since.is_none()
            && self.cleared.is_some_and(|ticks| ticks <= CLEAR_TICKS);
        let age_trip = matches!(trip, Some(Trip::UnreplicatedTooLong { .. }));
        let held = self.holds_accepted_trip(trip);
        let judged = if age_trip && (lingering || held) {
            Ok(())
        } else {
            match self.judge_now(trip, tick, bound, booted) {
                Err(_) if self.unread_wal(trip, unstaged_only) => {
                    self.excused.unread_wal = self.excused.unread_wal.saturating_add(1);
                    Ok(())
                }
                Err(_) if age_trip && (tick.reads_lying || self.dated_by_lying_reads()) => {
                    self.excused.lying_reads = self.excused.lying_reads.saturating_add(1);
                    Ok(())
                }
                Err(_) if self.unplaced_file(trip) => {
                    self.excused.unplaced_file = self.excused.unplaced_file.saturating_add(1);
                    Ok(())
                }
                Err(_) if self.unvouched_snapshot(trip) => {
                    self.excused.unvouched_snapshot =
                        self.excused.unvouched_snapshot.saturating_add(1);
                    Ok(())
                }
                verdict => verdict,
            }
        };
        self.accepted = match trip {
            Some(Trip::UnreplicatedTooLong { since }) if judged.is_ok() => Some(since),
            _ => None,
        };
        judged
    }

    /// Whether the engine's clock was armed by what lying reads told it: the tick before ran with
    /// every read lying, and the engine's clock starts earlier than the judge's own (the engine
    /// dates a pending staged file from its stamp, which a restart keeps, when a listing hid the
    /// replica's copy). The engine has had no honest read since to learn better.
    fn dated_by_lying_reads(&self) -> bool {
        self.lied_before
            && matches!((self.engine_since, self.since), (Some(engine), Some((judge, _))) if engine < judge)
    }

    /// Whether `trip` is the age trip the judge accepted on the last tick, still over commits
    /// that are off the replica: its clock's start has not moved later, which is what clears a
    /// trip in the engine (`lsr_core::failstop`, Clearing), so a clock stepped back that leaves
    /// the age under the bound does not make it a new, early trip.
    fn holds_accepted_trip(&self, trip: Option<Trip>) -> bool {
        let (Some(Trip::UnreplicatedTooLong { since }), Some(accepted)) = (trip, self.accepted)
        else {
            return false;
        };
        (self.since.is_some() || self.engine_since.is_some()) && since <= accepted
    }

    /// Tells the judge where the engine's own clock starts after a tick, when it runs.
    pub const fn engine_clock(&mut self, since: Option<Timestamp>) {
        self.engine_since = since;
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
                // A timeout says nothing of whether the request landed.
                crate::store::FaultShape::Hang | crate::store::FaultShape::HangEvery => {
                    (false, false)
                }
                crate::store::FaultShape::ErrorAfterPublish(_)
                | crate::store::FaultShape::HangAfterPublish
                | crate::store::FaultShape::SizeMismatch
                | crate::store::FaultShape::ChecksumMismatch
                | crate::store::FaultShape::CorruptPublish => (false, true),
                // The sweep is best effort: its refusal never trips.
                crate::store::FaultShape::FlipByteOnRead { .. }
                | crate::store::FaultShape::Scratch(_) => (false, false),
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
            unvouched_boot: false,
            reads_lying: false,
        }
    }

    /// A commit made after the clock stepped back dates earlier than the first one, and the engine
    /// dates its lag by the oldest stamp: a failure a bound after that stamp, with more clock
    /// steps forward since, is a true trip on the engine's clock.
    #[test]
    fn a_commit_dated_earlier_by_a_clock_step_back_starts_the_engine_clock() {
        let mut judge = caught_up();
        let image = lsr_history::Digest::genesis();
        judge.committed(ts(100_000), 100_000, image, image);
        judge.committed(ts(40_000), 101_000, image, image);
        let failing = TickSeen {
            at: ts(640_000),
            true_at: 300_000,
            ..tick(640_000, true)
        };
        judge.saw(failing);
        judge
            .judge(age_trip(40_000), failing, BOUND, true, false)
            .unwrap();
    }

    /// The engine dates WAL frames by the observation that first saw them. After a clock step
    /// back that reading is earlier than the stamp of the next commit, so a trip a bound after
    /// the sighting is a true one, though it is under the bound from every commit's stamp.
    #[test]
    fn frames_seen_after_a_clock_step_back_date_from_the_sighting() {
        let mut judge = caught_up();
        let image = lsr_history::Digest::genesis();
        judge.committed(ts(100_000), 100_000, image, image);
        judge.saw(tick(40_000, true));
        judge.committed(ts(40_100), 100_200, image, image);
        let failing = TickSeen {
            at: ts(640_000),
            true_at: 650_000,
            ..tick(640_000, true)
        };
        judge.saw(failing);
        judge
            .judge(age_trip(40_000), failing, BOUND, true, false)
            .unwrap();
        // With no reading of the clock before the later commit, nothing dates from 40 s.
        let mut unseen = caught_up();
        unseen.committed(ts(100_000), 100_000, image, image);
        unseen.committed(ts(40_100), 100_200, image, image);
        unseen.saw(failing);
        let early = unseen.judge(age_trip(40_000), failing, BOUND, true, false);
        assert!(
            matches!(early, Err(Violation::FalseTrip { .. })),
            "{early:?}"
        );
    }

    /// A trip the judge accepted stands while the commits it tripped on are off the replica,
    /// though a clock stepped back leaves the age under the bound; a trip that was never
    /// accepted, or one whose clock's start moved later, is judged afresh.
    #[test]
    fn an_accepted_trip_stands_after_a_step_back_but_a_new_one_is_judged() {
        let mut judge = FailStopJudge::starting(ts(0), lsr_history::Digest::genesis());
        judge.saw(tick(700_000, true));
        judge
            .judge(age_trip(0), tick(700_000, true), BOUND, true, false)
            .unwrap();
        let stepped_back = TickSeen {
            at: ts(500_000),
            true_at: 100_000,
            ..tick(500_000, true)
        };
        judge
            .judge(age_trip(0), stepped_back, BOUND, true, false)
            .unwrap();
        let moved_on = judge.judge(age_trip(50_000), stepped_back, BOUND, true, false);
        assert!(
            matches!(moved_on, Err(Violation::FalseTrip { .. })),
            "{moved_on:?}"
        );
        let mut fresh = FailStopJudge::starting(ts(0), lsr_history::Digest::genesis());
        let never_accepted = fresh.judge(age_trip(0), stepped_back, BOUND, true, false);
        assert!(
            matches!(never_accepted, Err(Violation::FalseTrip { .. })),
            "{never_accepted:?}"
        );
    }

    /// A trip accepted for frames the engine never read stands after the replica holds every
    /// commit the simulator knows, while the engine's own clock still runs; once it stops, a
    /// trip is judged afresh.
    #[test]
    fn an_accepted_trip_stands_while_the_engine_still_dates_the_frames() {
        let mut judge = FailStopJudge::starting(ts(0), lsr_history::Digest::genesis());
        judge.saw(tick(700_000, true));
        judge
            .judge(age_trip(0), tick(700_000, true), BOUND, true, true)
            .unwrap();
        judge.caught_up(lsr_history::Digest::genesis());
        for _ in 0..=CLEAR_TICKS {
            judge.saw(tick(700_000, false));
            judge.engine_clock(Some(ts(0)));
            judge
                .judge(age_trip(0), tick(700_000, false), BOUND, true, false)
                .unwrap();
        }
        judge.saw(tick(700_000, false));
        judge.engine_clock(None);
        let later = judge.judge(age_trip(0), tick(700_000, false), BOUND, true, false);
        assert!(
            matches!(later, Err(Violation::FalseTrip { .. })),
            "{later:?}"
        );
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
        assert_eq!(judge.excused().unread_wal, 1);
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
        assert_eq!(judge.excused(), Excused::default());
    }

    fn booted(at: i64) -> TickSeen {
        TickSeen {
            unvouched_boot: true,
            ..tick(at, true)
        }
    }

    /// An age trip dated after a boot that broke continuity, before any upload: excused, and
    /// counted apart from the unread-WAL exemption.
    #[test]
    fn a_trip_on_an_unvouched_boot_snapshot_is_excused_and_counted() {
        let mut judge = caught_up();
        judge.saw(booted(1_000));
        judge
            .judge(age_trip(1_000), tick(700_000, false), BOUND, true, false)
            .unwrap();
        let want = Excused {
            unvouched_snapshot: 1,
            ..Excused::default()
        };
        assert_eq!(judge.excused(), want);
    }

    /// An age trip while every read of the replica lies, over a replica the judge knows is
    /// whole: excused and counted; the same trip with honest reads is a false trip.
    #[test]
    fn a_trip_while_reads_of_the_replica_lie_is_excused_and_counted() {
        let mut judge = caught_up();
        let lying = TickSeen {
            reads_lying: true,
            ..tick(700_000, false)
        };
        judge
            .judge(age_trip(1_000), lying, BOUND, true, false)
            .unwrap();
        assert_eq!(judge.excused().lying_reads, 1);
        let mut judge = caught_up();
        assert!(
            judge
                .judge(age_trip(1_000), tick(700_000, false), BOUND, true, false)
                .is_err()
        );
    }

    /// An age trip dated by a pending staged file an earlier life left that the boot could not
    /// place, over a replica the judge knows is whole: excused and counted. Any other date, no
    /// such file, or commits the judge knows are off the replica: judged as usual.
    #[test]
    fn a_trip_on_an_unplaced_staged_file_is_excused_and_counted() {
        let mut judge = caught_up();
        judge.unplaced(Some(ts(1_000)));
        judge
            .judge(age_trip(1_000), tick(700_000, false), BOUND, true, false)
            .unwrap();
        assert_eq!(judge.excused().unplaced_file, 1);
        let mut judge = caught_up();
        judge.unplaced(Some(ts(1_000)));
        let other = judge.judge(age_trip(2_000), tick(700_000, false), BOUND, true, false);
        assert!(
            matches!(other, Err(Violation::FalseTrip { .. })),
            "{other:?}"
        );
        let mut judge = caught_up();
        let none = judge.judge(age_trip(1_000), tick(700_000, false), BOUND, true, false);
        assert!(matches!(none, Err(Violation::FalseTrip { .. })), "{none:?}");
        let image = lsr_history::Digest::genesis();
        judge.committed(ts(5_000), 5_000, image, image);
        judge.unplaced(Some(ts(1_000)));
        let behind = judge.judge(age_trip(1_000), tick(500_000, false), BOUND, true, false);
        assert!(
            matches!(behind, Err(Violation::FalseTrip { .. })),
            "{behind:?}"
        );
        assert_eq!(judge.excused(), Excused::default());
    }

    /// A trip dated before that boot, after the engine's first upload since it, or in the next
    /// life: not excused.
    #[test]
    fn an_older_trip_an_upload_or_a_restart_ends_the_unvouched_excuse() {
        let false_trip = |judge: &mut FailStopJudge, since: i64| {
            let got = judge.judge(age_trip(since), tick(700_000, false), BOUND, true, false);
            assert!(matches!(got, Err(Violation::FalseTrip { .. })), "{got:?}");
        };
        let mut judge = caught_up();
        judge.saw(booted(2_000));
        false_trip(&mut judge, 1_000);
        judge.put(false, true);
        false_trip(&mut judge, 2_000);
        let mut judge = caught_up();
        judge.saw(booted(2_000));
        judge.started();
        false_trip(&mut judge, 2_000);
        assert_eq!(judge.excused(), Excused::default());
    }
}
