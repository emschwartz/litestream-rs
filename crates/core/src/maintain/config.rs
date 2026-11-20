//! The maintenance schedule: how often each level compacts, how often a
//! snapshot is taken, and how long files are kept.
//!
//! Everything here is a whole number of milliseconds and nonzero, because the
//! planner's windows are integer arithmetic on milliseconds and a zero
//! interval has no window. [`Maintenance`] cannot be built in a shape that
//! makes retention or cadence meaningless; Go loads some of those shapes, so
//! each stricter rule says so.

use std::time::Duration;

use thiserror::Error;

use crate::layout::Level;

/// Most compaction levels above level 0 (Go's `maxLevel`); level 9 is the
/// snapshot level.
const MAX_LEVELS: usize = 8;

/// Go's level intervals for levels 1 to 3, in milliseconds (`compaction_level.go:12-17`).
const DEFAULT_LEVEL_MILLIS: [u64; 3] = [30_000, 300_000, 3_600_000];
/// Go's snapshot interval and snapshot retention, 24 h (`store.go:80-81`).
const DEFAULT_SNAPSHOT_MILLIS: u64 = 86_400_000;
/// Go's level-0 retention, 5 min (`store.go:88`).
const DEFAULT_L0_RETENTION_MILLIS: u64 = 300_000;

/// Which setting a [`ConfigError`] is about.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Setting {
    /// The compaction interval of one level (1 to 8).
    Level(Level),
    /// How often a snapshot is taken.
    SnapshotInterval,
    /// How long snapshots are kept.
    SnapshotRetention,
    /// How long level 0 is kept after it is covered.
    L0Retention,
    /// An [`Interval`] built on its own, tied to no setting yet.
    Standalone,
}

/// Why a maintenance schedule is refused.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum ConfigError {
    /// No level above 0 is configured, so nothing would ever compact.
    #[error("no compaction levels")]
    NoLevels,
    /// More than 8 levels above level 0.
    #[error("{found} compaction levels, at most 8")]
    TooManyLevels {
        /// How many were given.
        found: usize,
    },
    /// A duration of zero where a window or an age is needed. Level-0
    /// retention of zero is spelled `L0Retention::Keep`.
    #[error("{what:?} is zero")]
    ZeroInterval {
        /// The setting.
        what: Setting,
    },
    /// A duration with a part below one millisecond; the planner counts whole
    /// milliseconds and would round it silently.
    #[error("{what:?} is not a whole number of milliseconds")]
    SubMillisecond {
        /// The setting.
        what: Setting,
    },
    /// A level whose interval is not above the level below it. Stricter than
    /// Go, which accepts any order: an equal or shorter interval compacts a
    /// level before its source has had time to fill.
    #[error("{level} interval {interval:?} is not above the level below ({below:?})")]
    NotIncreasing {
        /// The level.
        level: Level,
        /// Its interval.
        interval: Duration,
        /// The interval of the level beneath it.
        below: Duration,
    },
    /// A snapshot interval not above the top level's interval. Stricter than
    /// Go: snapshots would be taken more often than the files they replace
    /// are compacted.
    #[error("snapshot interval {snapshot:?} is not above the top level's ({top:?})")]
    SnapshotIntervalNotAboveTop {
        /// The snapshot interval.
        snapshot: Duration,
        /// The top level's interval.
        top: Duration,
    },
    /// Snapshot retention shorter than the snapshot interval. Go only warns:
    /// the newest snapshot would age out before the next one exists, so
    /// retention here refuses the shape.
    #[error("snapshot retention {retention:?} is below the snapshot interval {interval:?}")]
    RetentionBelowInterval {
        /// The retention.
        retention: Duration,
        /// The snapshot interval.
        interval: Duration,
    },
}

/// A nonzero whole number of milliseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Interval(u64);

impl Interval {
    /// Checks one duration.
    ///
    /// # Errors
    /// Zero, or a remainder below one millisecond, reported against
    /// [`Setting::Standalone`]; the schedule's own constructor names the real
    /// setting.
    pub fn new(d: Duration) -> Result<Self, ConfigError> {
        Self::for_setting(d, Setting::Standalone)
    }

    fn for_setting(d: Duration, what: Setting) -> Result<Self, ConfigError> {
        if d.is_zero() {
            return Err(ConfigError::ZeroInterval { what });
        }
        if !d.subsec_nanos().is_multiple_of(1_000_000) {
            return Err(ConfigError::SubMillisecond { what });
        }
        // A whole-millisecond duration over u64::MAX ms is 584 million years.
        let millis = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        Ok(Self(millis))
    }

    /// The interval as a duration.
    #[must_use]
    pub const fn as_duration(self) -> Duration {
        Duration::from_millis(self.0)
    }

    /// Whole milliseconds, at least 1.
    #[must_use]
    pub const fn millis(self) -> u64 {
        self.0
    }
}

/// What happens to level 0 once a higher level covers it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum L0Retention {
    /// Level 0 is never deleted (Go's `l0-retention: 0`, upstream #1425).
    Keep,
    /// A covered level-0 file is deleted once it is this old.
    After(Duration),
}

/// A checked maintenance schedule.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Maintenance {
    levels: Vec<Interval>,
    top: Level,
    snapshot_interval: Interval,
    snapshot_retention: Duration,
    l0: L0Retention,
}

impl Maintenance {
    /// Checks a schedule. `levels[i]` is level `i + 1`'s interval.
    ///
    /// Checked in order: the level count; every duration nonzero and whole
    /// milliseconds; intervals strictly increasing; the snapshot interval above
    /// the top level's; retention at least the snapshot interval. The last
    /// three are stricter than Go.
    ///
    /// # Errors
    /// The first violation, as a [`ConfigError`].
    pub fn new(
        levels: Vec<Duration>,
        snapshot_interval: Duration,
        snapshot_retention: Duration,
        l0: L0Retention,
    ) -> Result<Self, ConfigError> {
        if levels.is_empty() {
            return Err(ConfigError::NoLevels);
        }
        let too_many = ConfigError::TooManyLevels {
            found: levels.len(),
        };
        let top = u8::try_from(levels.len())
            .ok()
            .filter(|n| usize::from(*n) <= MAX_LEVELS)
            .and_then(Level::new)
            .ok_or(too_many)?;
        let mut intervals = Vec::with_capacity(levels.len());
        for (level, d) in Level::ALL.iter().skip(1).zip(&levels) {
            intervals.push(Interval::for_setting(*d, Setting::Level(*level))?);
        }
        let snapshot = Interval::for_setting(snapshot_interval, Setting::SnapshotInterval)?;
        Interval::for_setting(snapshot_retention, Setting::SnapshotRetention)?;
        if let L0Retention::After(d) = l0 {
            Interval::for_setting(d, Setting::L0Retention)?;
        }
        Self::check_order(&intervals, snapshot, snapshot_retention)?;
        Ok(Self {
            levels: intervals,
            top,
            snapshot_interval: snapshot,
            snapshot_retention,
            l0,
        })
    }

    fn check_order(
        levels: &[Interval],
        snapshot: Interval,
        retention: Duration,
    ) -> Result<(), ConfigError> {
        for (level, pair) in Level::ALL.iter().skip(2).zip(levels.windows(2)) {
            if let [below, this] = pair
                && this <= below
            {
                return Err(ConfigError::NotIncreasing {
                    level: *level,
                    interval: this.as_duration(),
                    below: below.as_duration(),
                });
            }
        }
        if let Some(top) = levels.last()
            && snapshot <= *top
        {
            return Err(ConfigError::SnapshotIntervalNotAboveTop {
                snapshot: snapshot.as_duration(),
                top: top.as_duration(),
            });
        }
        if retention < snapshot.as_duration() {
            return Err(ConfigError::RetentionBelowInterval {
                retention,
                interval: snapshot.as_duration(),
            });
        }
        Ok(())
    }

    const DEFAULT_TOP: Level = match Level::new(3) {
        Some(level) => level,
        None => Level::ZERO,
    };

    /// Go's defaults: levels of 30 s, 5 min and 1 h, snapshots every 24 h
    /// kept 24 h, level 0 kept 5 min once covered. A test feeds the same
    /// numbers through [`Maintenance::new`].
    #[must_use]
    pub fn go_default() -> Self {
        Self {
            levels: DEFAULT_LEVEL_MILLIS.iter().map(|m| Interval(*m)).collect(),
            top: Self::DEFAULT_TOP,
            snapshot_interval: Interval(DEFAULT_SNAPSHOT_MILLIS),
            snapshot_retention: Duration::from_millis(DEFAULT_SNAPSHOT_MILLIS),
            l0: L0Retention::After(Duration::from_millis(DEFAULT_L0_RETENTION_MILLIS)),
        }
    }

    /// The highest compaction level in use (1 to 8).
    #[must_use]
    pub const fn top(&self) -> Level {
        self.top
    }

    /// A level's compaction interval. `None` for level 0, levels above
    /// [`Self::top`] and level 9.
    #[must_use]
    pub fn interval(&self, level: Level) -> Option<Interval> {
        let index = usize::from(level.get()).checked_sub(1)?;
        self.levels.get(index).copied()
    }

    /// How often a snapshot is taken.
    #[must_use]
    pub const fn snapshot_interval(&self) -> Interval {
        self.snapshot_interval
    }

    /// How long snapshots are kept.
    #[must_use]
    pub const fn snapshot_retention(&self) -> Duration {
        self.snapshot_retention
    }

    /// What happens to level 0 once covered.
    #[must_use]
    pub const fn l0_retention(&self) -> L0Retention {
        self.l0
    }
}
