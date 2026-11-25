//! The maintenance schedule: how often each level compacts, how often a
//! snapshot is taken, and how long files are kept.
//!
//! Everything here is a whole number of milliseconds and nonzero, because the
//! planner's windows are integer arithmetic on milliseconds and a zero
//! interval has no window. [`Maintenance`] cannot be built in a shape that
//! makes retention or cadence meaningless; Go loads some of those shapes, so
//! each stricter rule says so.
//!
//! Retention can be switched off ([`RetentionMode::Off`], Go's
//! `retention.enabled: false`), for a bucket whose lifecycle rules delete old
//! files instead, or a replica that keeps everything. It is a separate
//! setting rather than a zero duration, because a duration of zero is refused
//! everywhere above and "keep forever" is not a length of time.
//!
//! The level-0 retention check interval (Go's `l0-retention-check-interval`)
//! bounds how long an upload waits for the next listing, and with it how soon
//! a covered level-0 file is seen to be old enough to delete. Go sweeps level
//! 0 on that ticker (`store.go:658-680`); here it is a wake time of the one
//! maintenance listing, so it changes when the replica is listed and never
//! what a listing decides.

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
/// Go's level-0 retention check interval, 15 s (`store.go:92`).
const DEFAULT_L0_CHECK_MILLIS: u64 = 15_000;
/// How often level-0 retention is decided when the interval was not set: 8 times Go's 15 s
/// (D193). A set interval is used as it is.
const DEFAULT_RETENTION_CHECK_MILLIS: u64 = 120_000;

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

/// Why a duration is not an [`Interval`]. It names no setting;
/// [`Maintenance::new`] maps it to a [`ConfigError`] with the real one.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum IntervalError {
    /// The duration is zero.
    #[error("zero")]
    Zero,
    /// The duration has a part below one millisecond.
    #[error("not a whole number of milliseconds")]
    SubMillisecond,
}

impl IntervalError {
    const fn at(self, what: Setting) -> ConfigError {
        match self {
            Self::Zero => ConfigError::ZeroInterval { what },
            Self::SubMillisecond => ConfigError::SubMillisecond { what },
        }
    }
}

/// A nonzero whole number of milliseconds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Interval(u64);

impl Interval {
    /// Checks one duration.
    ///
    /// # Errors
    /// [`IntervalError::Zero`], or [`IntervalError::SubMillisecond`] for a
    /// remainder below one millisecond.
    pub fn new(d: Duration) -> Result<Self, IntervalError> {
        if d.is_zero() {
            return Err(IntervalError::Zero);
        }
        if !d.subsec_nanos().is_multiple_of(1_000_000) {
            return Err(IntervalError::SubMillisecond);
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

/// Whether maintenance deletes old files from the replica (Go's
/// `retention.enabled`, `store.go:133-136`).
///
/// With [`Self::Off`], maintenance still compacts and snapshots on the same
/// schedule, and the staged copies on local disk are still pruned once level
/// 1 covers them (Go: "Local file cleanup still occurs"). Two deletes are not
/// retention and still run: removing a file this process wrote that the store
/// rejected, and the boot read-back's repair of a newest file that does not
/// verify. Both remove bytes that do not decode, never history, and without
/// them compaction of that level would stop; Go does neither.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum RetentionMode {
    /// Delete what the snapshot retention and [`L0Retention`] allow. Go's
    /// default (`store.go:168`).
    #[default]
    Enforce,
    /// Delete no snapshot, compacted file or level-0 file: the replica keeps
    /// every file until something outside this process removes it.
    Off,
}

/// A checked maintenance schedule.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Maintenance {
    levels: Vec<Interval>,
    top: Level,
    snapshot_interval: Interval,
    snapshot_retention: Duration,
    l0: L0Retention,
    /// The level-0 retention check interval, `None` while defaulted.
    l0_check: Option<Interval>,
    retention: RetentionMode,
    /// Whether the engine runs the schedule at all; see [`Self::one_off`].
    run: Run,
}

/// Whether a [`Maintenance`] runs its schedule.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Run {
    /// The daemon's loop: levels compact, snapshots are taken and retention deletes as the
    /// schedule says.
    Scheduled,
    /// A `replicate -once` run: nothing is scheduled. Only the retention pass the drain is
    /// asked to end with uses the rest of the settings.
    OneOff,
}

impl Maintenance {
    /// Checks a schedule. `levels[i]` is level `i + 1`'s interval.
    ///
    /// Checked in order: the level count; every duration nonzero and whole
    /// milliseconds; intervals strictly increasing; the snapshot interval above
    /// the top level's; retention at least the snapshot interval. The last
    /// three are stricter than Go. Retention is enforced; see
    /// [`Self::with_retention`].
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
            intervals.push(Interval::new(*d).map_err(|e| e.at(Setting::Level(*level)))?);
        }
        let snapshot =
            Interval::new(snapshot_interval).map_err(|e| e.at(Setting::SnapshotInterval))?;
        Interval::new(snapshot_retention).map_err(|e| e.at(Setting::SnapshotRetention))?;
        if let L0Retention::After(d) = l0 {
            Interval::new(d).map_err(|e| e.at(Setting::L0Retention))?;
        }
        Self::check_order(&intervals, snapshot, snapshot_retention)?;
        Ok(Self {
            levels: intervals,
            top,
            snapshot_interval: snapshot,
            snapshot_retention,
            l0,
            l0_check: None,
            retention: RetentionMode::Enforce,
            run: Run::Scheduled,
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
    /// kept 24 h, level 0 kept 5 min once covered and checked every 15 s,
    /// retention enforced. A test feeds the same numbers through
    /// [`Maintenance::new`].
    #[must_use]
    pub fn go_default() -> Self {
        Self {
            levels: DEFAULT_LEVEL_MILLIS.iter().map(|m| Interval(*m)).collect(),
            top: Self::DEFAULT_TOP,
            snapshot_interval: Interval(DEFAULT_SNAPSHOT_MILLIS),
            snapshot_retention: Duration::from_millis(DEFAULT_SNAPSHOT_MILLIS),
            l0: L0Retention::After(Duration::from_millis(DEFAULT_L0_RETENTION_MILLIS)),
            l0_check: None,
            retention: RetentionMode::Enforce,
            run: Run::Scheduled,
        }
    }

    /// The same settings for a `replicate -once` run: the engine plans no compaction, no
    /// snapshot and no retention pass of its own. Go switches every background monitor off
    /// before it opens the store under `-once` (`cmd/litestream/replicate.go:374-383`:
    /// `CompactionMonitorEnabled = false`, `L0RetentionCheckInterval = 0`, `MonitorInterval = 0`),
    /// and its one-off run only syncs, uploads and then snapshots and enforces retention when
    /// asked. The settings stay for that retention pass (`docs/decisions/D221.md`).
    #[must_use]
    pub const fn one_off(mut self) -> Self {
        self.run = Run::OneOff;
        self
    }

    /// Whether this is [`Self::one_off`]: the engine runs none of the schedule.
    #[must_use]
    pub const fn is_one_off(&self) -> bool {
        matches!(self.run, Run::OneOff)
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

    /// How often level-0 retention is checked while uploads continue (Go's
    /// `l0-retention-check-interval`, 15 s unless set). [`Self::retention_check_every`] is how
    /// often the check runs: the set value, or 120 s when unset.
    #[must_use]
    pub const fn l0_check_interval(&self) -> Interval {
        match self.l0_check {
            Some(set) => set,
            None => Interval(DEFAULT_L0_CHECK_MILLIS),
        }
    }

    /// How often level-0 retention is decided: the set interval exactly, else 120 s (and up to
    /// twice that behind a compaction job), which is a difference from Go's 15 s default
    /// (docs/decisions/D193.md).
    #[must_use]
    pub const fn retention_check_every(&self) -> Interval {
        match self.l0_check {
            Some(set) => set,
            None => Interval(DEFAULT_RETENTION_CHECK_MILLIS),
        }
    }

    /// How many check intervals the check may wait behind a compaction job: one more for the
    /// default, so the job goes first and the pass lists once; none for a set interval, because
    /// the operator's value means what it says.
    pub(crate) const fn retention_check_behind_a_job(&self) -> i64 {
        if self.l0_check.is_some() { 1 } else { 2 }
    }

    /// The same schedule with another level-0 retention check interval.
    #[must_use]
    pub const fn with_l0_check_interval(mut self, every: Interval) -> Self {
        self.l0_check = Some(every);
        self
    }

    /// Whether retention deletes anything.
    #[must_use]
    pub const fn retention(&self) -> RetentionMode {
        self.retention
    }

    /// The same schedule with retention switched on or off. Every other
    /// setting is already checked and does not depend on it.
    #[must_use]
    pub const fn with_retention(mut self, retention: RetentionMode) -> Self {
        self.retention = retention;
        self
    }

    /// The same schedule with level-0 deletion off: what Go's one-off retention pass deletes
    /// (`store.go:863-883` skips level 0), for `replicate -once -enforce-retention`.
    #[must_use]
    pub(crate) fn without_level_zero(&self) -> Self {
        Self {
            l0: L0Retention::Keep,
            ..self.clone()
        }
    }
}
