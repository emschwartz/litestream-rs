//! Engine time: the wall clock, never earlier than a reading already used,
//! and never stopped.
//!
//! Every timer the engine holds (retry waits, the checkpoint schedule,
//! compaction windows, retention) is a timestamp compared with engine time,
//! and the files it stages are stamped with it. A wall clock that steps back
//! cannot be followed, or a timestamp would go backwards; holding the last
//! reading until the wall clock passes it again froze every timer for the
//! size of the step (an hour of RTC error corrected by NTP stopped retries,
//! checkpoints and uploads for an hour).
//!
//! So at a step back of `N` the engine keeps its time and from there advances
//! at half the wall clock's rate until the wall clock catches up, after `2N`
//! of wall time; from then on the two agree. Timers run at half speed in that
//! window, never at zero, and stamps rejoin the wall clock instead of staying
//! `N` ahead for the life of the process. A step forward is followed at once:
//! timers that pass early is the only reading of a clock that says time passed.
//!
//! The wake the engine asks its host for is wall time ([`EngineClock::wall_at`]),
//! since the host sleeps on the wall clock; event times are engine time.
//!
//! Stamps are not engine time. A staged or snapshot file is stamped with the
//! wall clock, never earlier than the highest reading so far
//! ([`EngineClock::stamp_floor`]), so a stamp always lies between the wall
//! clock when the file was made and the latest time the clock has shown. Engine
//! time can run past every reading while it slews; a stamp from it would date
//! a file after any instant the clock showed, and an `AsOf` restore at that
//! instant would miss it. Comparisons of stamps with engine time (retention,
//! the snapshot schedule) see files as older by at most half the step, and
//! retention anchors on the replica's newest stamp rather than on the clock.
//!
//! Engine time is computed from an anchor (the readings at the last step back)
//! rather than accumulated reading by reading, so frequent readings do not
//! lose the half-milliseconds that integer division drops.

use lsr_ltx::Timestamp;

/// Engine time and the readings it is anchored to. The invariant: `engine`
/// is never below `wall`, and both are what the last reading returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EngineClock {
    /// The wall clock's reading at the last step back (or the first reading).
    anchor_wall: i64,
    /// Engine time at that reading.
    anchor_engine: i64,
    /// The last wall reading.
    wall: i64,
    /// The engine time it gave.
    engine: i64,
    /// The highest wall reading so far.
    high: i64,
}

impl EngineClock {
    /// A clock whose first reading is `wall`.
    pub(super) const fn start(wall: Timestamp) -> Self {
        let wall = wall.unix_millis();
        Self {
            anchor_wall: wall,
            anchor_engine: wall,
            wall,
            engine: wall,
            high: wall,
        }
    }

    /// Takes a reading of the wall clock. The flag is set when it stepped back
    /// from the last one: engine time then stays where it was and is anchored
    /// there.
    pub(super) fn read(self, wall: Timestamp) -> (Self, bool) {
        let wall = wall.unix_millis();
        if wall < self.wall {
            let stepped = Self {
                anchor_wall: wall,
                anchor_engine: self.engine,
                wall,
                engine: self.engine,
                high: self.high,
            };
            return (stepped, true);
        }
        let engine = self.engine_at(wall).max(self.engine);
        debug_assert!(engine >= wall, "engine time is never behind the wall clock");
        let read = Self {
            wall,
            engine,
            high: self.high.max(wall),
            ..self
        };
        (read, false)
    }

    /// Engine time now.
    pub(super) const fn now(self) -> Timestamp {
        Timestamp::from_unix_millis(self.engine)
    }

    /// The earliest stamp a file made now may take: the highest wall reading
    /// so far, never engine time.
    pub(super) const fn stamp_floor(self) -> Timestamp {
        Timestamp::from_unix_millis(self.high)
    }

    /// Engine time at the wall reading `wall`, at or after the anchor.
    fn engine_at(self, wall: i64) -> i64 {
        let elapsed = wall.saturating_sub(self.anchor_wall);
        let slewed = self.anchor_engine.saturating_add(elapsed.div_euclid(2));
        slewed.max(wall)
    }

    /// Engine time when the wall clock reads `wall`, if it steps back no more.
    pub(super) fn engine_when(self, wall: Timestamp) -> Timestamp {
        let wall = wall.unix_millis().max(self.wall);
        Timestamp::from_unix_millis(self.engine_at(wall).max(self.engine))
    }

    /// The first wall reading at which engine time reaches `at`, if the clock
    /// steps back no more: the time a host sleeping on the wall clock wakes.
    /// The last wall reading when `at` has already passed.
    pub(super) fn wall_at(self, at: Timestamp) -> Timestamp {
        let at = at.unix_millis();
        if at <= self.engine {
            return Timestamp::from_unix_millis(self.wall);
        }
        let ahead = at.saturating_sub(self.anchor_engine);
        let slewed = self.anchor_wall.saturating_add(ahead.saturating_mul(2));
        Timestamp::from_unix_millis(slewed.min(at).max(self.wall))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn ts(n: i64) -> Timestamp {
        Timestamp::from_unix_millis(n)
    }

    fn read_all(start: i64, readings: &[i64]) -> Vec<(EngineClock, bool)> {
        let mut clock = EngineClock::start(ts(start));
        readings
            .iter()
            .map(|wall| {
                let (next, stepped) = clock.read(ts(*wall));
                clock = next;
                (next, stepped)
            })
            .collect()
    }

    #[test]
    fn a_clock_that_never_steps_back_is_the_wall_clock() {
        for (clock, stepped) in read_all(0, &[0, 5, 5, 1_000, 3_600_000]) {
            assert!(!stepped);
            assert_eq!(clock.now(), ts(clock.wall));
        }
    }

    #[test]
    fn a_step_back_holds_then_slews_at_half_speed_until_it_rejoins() {
        let seen = read_all(10_000, &[0, 1_000, 10_000, 20_000, 25_000]);
        let engine: Vec<i64> = seen.iter().map(|(c, _)| c.now().unix_millis()).collect();
        let stepped: Vec<bool> = seen.iter().map(|(_, s)| *s).collect();
        assert_eq!(engine, [10_000, 10_500, 15_000, 20_000, 25_000]);
        assert_eq!(stepped, [true, false, false, false, false]);
    }

    #[test]
    fn readings_one_millisecond_apart_still_advance() {
        let mut clock = EngineClock::start(ts(1_000));
        clock = clock.read(ts(0)).0;
        for wall in 1..=400 {
            clock = clock.read(ts(wall)).0;
        }
        assert_eq!(clock.now(), ts(1_200));
    }

    #[test]
    fn a_wake_is_wall_time() {
        let clock = EngineClock::start(ts(10_000)).read(ts(0)).0;
        assert_eq!(clock.wall_at(ts(11_000)), ts(2_000));
        assert_eq!(clock.wall_at(ts(9_000)), ts(0));
        assert_eq!(clock.wall_at(ts(25_000)), ts(25_000));
        assert_eq!(clock.engine_when(ts(2_000)), ts(11_000));
        assert_eq!(clock.engine_when(ts(25_000)), ts(25_000));
    }

    proptest! {
        /// Engine time never goes back and never trails the wall clock; it
        /// advances by at least half of every forward move of the wall clock.
        /// The stamp floor is the highest reading.
        #[test]
        fn engine_time_is_monotone_and_never_stops(
            start in -1_000_000_i64..1_000_000,
            steps in prop::collection::vec(-100_000_i64..100_000, 1..50),
        ) {
            let mut clock = EngineClock::start(ts(start));
            let mut wall = start;
            for step in steps {
                wall = wall.saturating_add(step);
                let (next, stepped) = clock.read(ts(wall));
                prop_assert_eq!(stepped, step < 0);
                prop_assert_eq!(next.stamp_floor(), ts(clock.high.max(wall)));
                prop_assert!(next.engine >= clock.engine);
                prop_assert!(next.engine >= wall);
                if step > 0 {
                    let moved = next.engine.saturating_sub(clock.engine);
                    prop_assert!(moved >= step.div_euclid(2), "{} < {}/2", moved, step);
                }
                clock = next;
            }
        }

        /// The wake a host is given is the first wall reading at which the
        /// engine is due: due there, and not a millisecond before.
        #[test]
        fn a_wake_is_the_first_wall_reading_that_is_due(
            start in 0_i64..1_000_000,
            back in 1_i64..100_000,
            at in 0_i64..2_000_000,
        ) {
            let clock = EngineClock::start(ts(start)).read(ts(start.saturating_sub(back))).0;
            let wake = clock.wall_at(ts(at)).unix_millis();
            prop_assert!(clock.read(ts(wake)).0.engine >= at);
            if wake > clock.wall {
                prop_assert!(clock.read(ts(wake.saturating_sub(1))).0.engine < at);
            }
        }
    }
}
