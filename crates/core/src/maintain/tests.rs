//! Planner, schedule and executor tests: one test per error variant, Go's
//! window arithmetic, hand-built inventories for each rule of `due`, a
//! property over generated write histories, and the executor on `MemStore`.

use std::collections::BTreeMap;
use std::time::Duration;

use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::created_at::{Resolve, ResolvedCreatedAt};
use crate::fault::Fault;
use crate::fault::role::{Replica, Staging};
use crate::layout::{CreatedAt, ListedTime};
use crate::mem::MemStore;
use crate::plan::{KeyTarget, PlanOutcome, plan};
use crate::store::WriteMode;
use crate::store::{PageLimit, StoreOp, Upload};

/// A day-aligned instant (2023-11-15), so every default window starts on it.
pub(super) const T0: i64 = 1_700_006_400_000;

pub(super) fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

pub(super) fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(txid(min), txid(max)).unwrap()
}

pub(super) fn level(n: u8) -> Level {
    Level::new(n).unwrap()
}

pub(super) fn key(l: u8, min: u64, max: u64) -> FileKey {
    FileKey::new(level(l), range(min, max))
}

pub(super) fn at(ms: i64) -> Timestamp {
    Timestamp::from_unix_millis(ms)
}

pub(super) fn file(l: u8, min: u64, max: u64, created: i64) -> FileInfo {
    FileInfo {
        key: key(l, min, max),
        size: 1,
        listed: ListedTime::Created(CreatedAt::mtime(at(created))),
    }
}

pub(super) fn inv(files: impl IntoIterator<Item = FileInfo>) -> Inventory {
    Inventory::from_files(files).unwrap()
}

pub(super) fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn iv(n: u64) -> Interval {
    Interval::new(secs(n)).unwrap()
}

pub(super) fn defaults() -> Maintenance {
    Maintenance::go_default()
}

/// `due` from a fresh state: the listing alone decides.
pub(super) fn due(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
) -> Due {
    due_state(
        inventory,
        now,
        config,
        published,
        &mut MaintainState::default(),
    )
}

/// `super::due` on a listing that carries every created-at time, so nothing is asked.
pub(super) fn due_state(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &mut MaintainState,
) -> Due {
    match super::due(
        inventory,
        now,
        config,
        published,
        state,
        &ResolvedCreatedAt::default(),
    ) {
        Resolve::Done(due) => due,
        Resolve::Need(file) => panic!("a stored listing asked for a created-at time: {file:?}"),
    }
}

/// `run_compaction` for a compaction job.
pub(super) async fn run_compact<L: Role, R: Role>(
    job: &CompactJob,
    local: &L,
    replica: &R,
) -> Outcome {
    run_compaction(job.inputs(), job.level(), local, replica).await
}

/// `due` for a replica whose first snapshot is backing off, so the levels are planned as they
/// are once the snapshot has failed: the level tests below read compaction on a listing that
/// holds no snapshot.
fn due_levels(i: &Inventory, now: Timestamp, config: &Maintenance) -> Due {
    let mut state = MaintainState::default();
    state.record_snapshot(now, Some(&Fault::StoreThrottled { retry_after: None }));
    due_state(i, now, config, None, &mut state)
}

fn due_at(i: &Inventory, now: i64) -> Due {
    due(i, at(now), &defaults(), None)
}

fn compact_job(job: &Job) -> &CompactJob {
    match job {
        Job::Compact(job) => job,
        other @ (Job::Snapshot(_) | Job::Delete(_)) => panic!("not a compaction: {other:?}"),
    }
}

fn run_job(due: Due) -> Job {
    match due {
        Due::Run(job) => job,
        Due::Wait { until, .. } => panic!("expected a job, got Wait until {until:?}"),
    }
}

fn input_keys(job: &CompactJob) -> Vec<FileKey> {
    job.inputs().files().iter().map(|f| f.key).collect()
}

// Configuration.

fn config(levels: &[u64], snapshot: u64, retention: u64) -> Result<Maintenance, ConfigError> {
    Maintenance::new(
        levels.iter().map(|s| secs(*s)).collect(),
        secs(snapshot),
        secs(retention),
        L0Retention::After(secs(60)),
    )
}

/// Go's defaults, each value with its source line.
#[test]
fn go_defaults_run_through_new_and_match_go() {
    let from_new = Maintenance::new(
        vec![secs(30), secs(300), secs(3600)], // compaction_level.go:12-17
        secs(86_400),                          // store.go:80
        secs(86_400),                          // store.go:81
        L0Retention::After(secs(300)),         // store.go:88
    )
    .unwrap();
    assert_eq!(Maintenance::go_default(), from_new);
    let go = Maintenance::go_default();
    assert_eq!(go.top(), level(3));
    assert_eq!(go.interval(level(1)), Some(iv(30)));
    assert_eq!(go.interval(level(2)), Some(iv(300)));
    assert_eq!(go.interval(level(3)), Some(iv(3600)));
    assert_eq!(go.snapshot_interval(), iv(86_400));
    assert_eq!(go.snapshot_retention(), secs(86_400));
    assert_eq!(go.l0_retention(), L0Retention::After(secs(300)));
    assert_eq!(go.l0_check_interval(), iv(15)); // store.go:92
    assert_eq!(from_new.l0_check_interval(), iv(15));
}

#[test]
fn interval_is_none_outside_the_configured_levels() {
    let go = defaults();
    for l in [0, 4, 8, 9] {
        assert_eq!(go.interval(level(l)), None, "level {l}");
    }
}

#[test]
fn interval_refuses_zero_and_sub_millisecond_remainders() {
    assert_eq!(Interval::new(Duration::ZERO), Err(IntervalError::Zero));
    assert_eq!(
        Interval::new(Duration::from_micros(1500)),
        Err(IntervalError::SubMillisecond)
    );
    let one = Interval::new(Duration::from_millis(1)).unwrap();
    assert_eq!(
        (one.millis(), one.as_duration()),
        (1, Duration::from_millis(1))
    );
}

#[test]
fn no_levels_is_refused() {
    assert_eq!(config(&[], 100, 100), Err(ConfigError::NoLevels));
}

#[test]
fn nine_levels_is_too_many_and_eight_is_fine() {
    let nine: Vec<u64> = (1..=9).collect();
    assert_eq!(
        config(&nine, 100, 100),
        Err(ConfigError::TooManyLevels { found: 9 })
    );
    let eight: Vec<u64> = (1..=8).collect();
    assert_eq!(config(&eight, 100, 100).unwrap().top(), level(8));
}

#[test]
fn a_zero_duration_names_its_setting() {
    let zero = |what| Err(ConfigError::ZeroInterval { what });
    assert_eq!(config(&[10, 0], 100, 100), zero(Setting::Level(level(2))));
    assert_eq!(config(&[10], 0, 100), zero(Setting::SnapshotInterval));
    assert_eq!(config(&[10], 100, 0), zero(Setting::SnapshotRetention));
    let keep_spelled_zero = Maintenance::new(
        vec![secs(10)],
        secs(100),
        secs(100),
        L0Retention::After(Duration::ZERO),
    );
    assert_eq!(keep_spelled_zero, zero(Setting::L0Retention));
    let keep = Maintenance::new(vec![secs(10)], secs(100), secs(100), L0Retention::Keep);
    assert_eq!(keep.unwrap().l0_retention(), L0Retention::Keep);
}

#[test]
fn a_sub_millisecond_duration_names_its_setting() {
    let micro = Duration::from_micros(1500);
    let err = |what| Err(ConfigError::SubMillisecond { what });
    let levels = Maintenance::new(vec![micro], secs(100), secs(100), L0Retention::Keep);
    assert_eq!(levels, err(Setting::Level(level(1))));
    let snap = Maintenance::new(vec![secs(1)], micro, secs(100), L0Retention::Keep);
    assert_eq!(snap, err(Setting::SnapshotInterval));
    let keep = Maintenance::new(vec![secs(1)], secs(100), micro, L0Retention::Keep);
    assert_eq!(keep, err(Setting::SnapshotRetention));
    let l0 = Maintenance::new(
        vec![secs(1)],
        secs(100),
        secs(100),
        L0Retention::After(micro),
    );
    assert_eq!(l0, err(Setting::L0Retention));
}

#[test]
fn intervals_must_strictly_increase() {
    let want = |l, interval, below| {
        Err(ConfigError::NotIncreasing {
            level: level(l),
            interval: secs(interval),
            below: secs(below),
        })
    };
    assert_eq!(config(&[10, 10], 100, 100), want(2, 10, 10));
    assert_eq!(config(&[10, 60, 30], 100, 100), want(3, 30, 60));
}

#[test]
fn the_snapshot_interval_must_be_above_the_top_level() {
    let want = Err(ConfigError::SnapshotIntervalNotAboveTop {
        snapshot: secs(60),
        top: secs(60),
    });
    assert_eq!(config(&[10, 60], 60, 100), want);
    config(&[10, 60], 61, 100).unwrap();
}

#[test]
fn retention_must_be_at_least_the_snapshot_interval() {
    let want = Err(ConfigError::RetentionBelowInterval {
        retention: secs(99),
        interval: secs(100),
    });
    assert_eq!(config(&[10], 100, 99), want);
    config(&[10], 100, 100).unwrap();
}

// Jobs.

fn inputs(files: &[FileInfo]) -> CompactionInputs {
    CompactionInputs::new(files.to_vec()).unwrap()
}

#[test]
fn a_compact_job_needs_a_destination_from_one_to_eight() {
    for l in [0, 9] {
        let got = CompactJob::new(inputs(&[file(0, 1, 1, 0)]), level(l));
        assert_eq!(
            got,
            Err(JobError::LevelOutOfRange { level: level(l) }),
            "{l}"
        );
    }
    let job = CompactJob::new(inputs(&[file(7, 1, 2, 0), file(7, 3, 3, 0)]), level(8)).unwrap();
    assert_eq!(job.level(), level(8));
    assert_eq!(job.output_key(), key(8, 1, 3));
    assert_eq!(job.inputs().range(), range(1, 3));
}

#[test]
fn every_input_must_be_one_level_below_the_destination() {
    let two_up = file(0, 3, 3, 0);
    let got = CompactJob::new(inputs(&[file(1, 1, 2, 0), two_up]), level(2));
    assert_eq!(
        got,
        Err(JobError::InputAtWrongLevel {
            key: two_up.key,
            expected: level(1)
        })
    );
    let same = CompactJob::new(inputs(&[file(2, 1, 2, 0)]), level(2));
    assert_eq!(
        same,
        Err(JobError::InputAtWrongLevel {
            key: key(2, 1, 2),
            expected: level(1)
        })
    );
}

#[test]
fn a_delete_job_needs_one_to_a_thousand_ascending_keys() {
    assert_eq!(DeleteJob::new(Vec::new()), Err(JobError::NoKeys));
    let many: Vec<FileKey> = (1..=1001).map(|n| key(0, n, n)).collect();
    assert_eq!(
        DeleteJob::new(many.clone()),
        Err(JobError::TooManyKeys { found: 1001 })
    );
    let thousand: Vec<FileKey> = many.into_iter().take(MAX_DELETE).collect();
    assert_eq!(
        DeleteJob::new(thousand.clone()).unwrap().keys(),
        &thousand[..]
    );
    let (a, b) = (key(0, 2, 2), key(0, 1, 1));
    assert_eq!(
        DeleteJob::new(vec![a, b]),
        Err(JobError::KeysNotAscending { after: a, next: b })
    );
    assert_eq!(
        DeleteJob::new(vec![a, a]),
        Err(JobError::KeysNotAscending { after: a, next: a })
    );
}

// Go's window arithmetic.

fn trunc(now: i64, interval: Duration) -> i64 {
    go_truncate(at(now), Interval::new(interval).unwrap()).unix_millis()
}

#[test]
fn go_truncate_agrees_with_unix_epoch_for_intervals_dividing_a_day() {
    let samples = [
        0,
        1,
        29_999,
        30_000,
        T0,
        T0 + 1,
        T0 + 86_399_999,
        -1,
        -30_001,
    ];
    for interval in [secs(30), secs(300), secs(3600), secs(86_400)] {
        let ms = i64::try_from(interval.as_millis()).unwrap();
        for now in samples {
            assert_eq!(
                trunc(now, interval),
                now.div_euclid(ms) * ms,
                "{now} {interval:?}"
            );
        }
    }
}

#[test]
fn go_truncate_counts_a_week_from_go_zero_time_not_the_epoch() {
    let week = secs(7 * 86_400);
    // 0001-01-01 was a Monday; the Unix epoch fell on a Thursday, so the
    // week it sits in began on Monday 1969-12-29.
    assert_eq!(trunc(0, week), -259_200_000);
    assert_eq!(trunc(-259_200_000, week), -259_200_000);
    assert_eq!(trunc(-259_200_001, week), -259_200_000 - 604_800_000);
}

#[test]
fn go_truncate_rounds_negative_instants_toward_the_past() {
    assert_eq!(trunc(-1, secs(30)), -30_000);
    assert_eq!(trunc(-30_000, secs(30)), -30_000);
    assert_eq!(trunc(-30_001, secs(30)), -60_000);
}

#[test]
fn go_truncate_saturates_at_the_extremes() {
    let week = secs(7 * 86_400);
    let top = trunc(i64::MAX, week);
    assert!(i64::MAX - top < 604_800_000);
    assert_eq!(trunc(i64::MIN, week), i64::MIN);
    assert_eq!(trunc(i64::MIN, secs(30)), i64::MIN);
    assert_eq!(trunc(i64::MIN + 1, secs(30)), i64::MIN);
}

// The planner, rule by rule.

/// L0 files 1..=n, one transaction each, all created at `created`.
pub(super) fn l0_run(first: u64, last: u64, created: i64) -> Vec<FileInfo> {
    (first..=last).map(|n| file(0, n, n, created)).collect()
}

pub(super) fn snap(max: u64, created: i64) -> FileInfo {
    file(9, 1, max, created)
}

#[test]
fn an_empty_replica_with_nothing_published_has_nothing_to_do() {
    assert_eq!(
        due_at(&Inventory::default(), T0),
        Due::Wait {
            until: None,
            held: None
        }
    );
}

#[test]
fn a_published_position_alone_asks_for_the_first_snapshot() {
    let got = due(&Inventory::default(), at(T0), &defaults(), Some(txid(5)));
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: txid(5),
            from: None,
        }))
    );
}

#[test]
fn level_one_compacts_the_whole_source_when_the_destination_is_empty() {
    let i = inv(l0_run(1, 3, T0 - 100_000));
    let job = run_job(due_levels(&i, at(T0), &defaults()));
    let job = compact_job(&job);
    assert_eq!(job.level(), level(1));
    assert_eq!(job.output_key(), key(1, 1, 3));
}

#[test]
fn only_files_above_the_destination_max_are_inputs() {
    let mut files = l0_run(1, 5, T0 - 100_000);
    files.push(file(1, 1, 3, T0 - 100_000));
    let job = run_job(due_at(&inv(files), T0));
    assert_eq!(
        input_keys(compact_job(&job)),
        vec![key(0, 4, 4), key(0, 5, 5)]
    );
}

#[test]
fn a_level_with_nothing_above_its_destination_is_idle_with_no_wake_time() {
    let mut files = l0_run(1, 3, T0 - 100_000);
    files.extend([file(1, 1, 3, T0 - 90_000), file(2, 1, 3, T0 - 90_000)]);
    files.extend([file(3, 1, 3, T0 - 90_000), snap(3, T0 - 90_000)]);
    assert_eq!(
        due_at(&inv(files), T0),
        Due::Wait {
            until: None,
            held: None
        }
    );
}

#[test]
fn the_first_snapshot_goes_before_level_one() {
    let i = inv(l0_run(1, 3, T0 - 100_000));
    let Due::Run(Job::Snapshot(snapshot)) = due_at(&i, T0) else {
        panic!("the snapshot is planned first");
    };
    assert_eq!(snapshot.through, txid(3));
}

#[test]
fn a_backing_off_first_snapshot_lets_level_one_run() {
    let i = inv(l0_run(1, 3, T0 - 100_000));
    let job = run_job(due_levels(&i, at(T0), &defaults()));
    assert_eq!(compact_job(&job).level(), level(1));
}

#[test]
fn level_one_starts_above_the_snapshot_and_stays_contiguous() {
    // The snapshot holds 1..=3: level 1 (empty) takes 4..=5 only, then goes on from there.
    let mut files = l0_run(1, 5, T0 - 100_000);
    files.push(snap(3, T0 - 100_000));
    let job = run_job(due_at(&inv(files.clone()), T0));
    assert_eq!(
        input_keys(compact_job(&job)),
        vec![key(0, 4, 4), key(0, 5, 5)]
    );
    files.push(file(1, 4, 5, T0 - 100_000));
    files.extend(l0_run(6, 6, T0 - 100_000));
    let next = run_job(due_at(&inv(files), T0 + 30_000));
    assert_eq!(compact_job(&next).output_key(), key(1, 6, 6));
}

#[test]
fn a_level_with_files_takes_what_the_snapshot_holds_too() {
    // Level 1 holds 1..=2 already: the snapshot at 4 does not cut what follows it.
    let mut files = l0_run(1, 5, T0 - 100_000);
    files.push(file(1, 1, 2, T0 - 100_000));
    files.push(snap(4, T0 - 100_000));
    let job = run_job(due_at(&inv(files), T0));
    assert_eq!(compact_job(&job).output_key(), key(1, 3, 5));
}

#[test]
fn a_destination_written_this_window_waits_for_the_next_one() {
    let mut files = l0_run(1, 5, T0 - 100_000);
    files.push(file(1, 3, 3, T0 + 1));
    // The snapshot ends before level 1's file, so level 2 (empty) starts at it (D224).
    files.push(snap(2, T0 + 1));
    // Window of 30 s starting at T0; the next starts 30 s later. Level 2
    // has L1 input (3..3) with an empty destination and runs first.
    let job = run_job(due_at(&inv(files.clone()), T0 + 10_000));
    assert_eq!(compact_job(&job).level(), level(2));
    files.push(file(2, 3, 3, T0 + 1));
    files.push(file(3, 3, 3, T0 + 1));
    let waiting = due_at(&inv(files.clone()), T0 + 10_000);
    assert_eq!(
        waiting,
        Due::Wait {
            until: Some(at(T0 + 30_000)),
            held: None
        }
    );
    let later = run_job(due_at(&inv(files), T0 + 30_000));
    assert_eq!(compact_job(&later).output_key(), key(1, 4, 5));
}

#[test]
fn the_earliest_wake_time_wins_and_a_ready_level_is_not_held_by_a_waiting_one() {
    // Level 1 waits (written this window); level 2 is ready and runs.
    let files = vec![
        file(0, 4, 4, T0 - 100_000),
        file(1, 1, 3, T0 + 1),
        file(1, 4, 4, T0 - 50_000),
        file(2, 1, 2, T0 - 1_000_000),
    ];
    let got = run_job(due_at(&inv(files), T0 + 10_000));
    assert_eq!(compact_job(&got).level(), level(2));
}

#[test]
fn a_backlog_of_a_hundred_drains_as_four_jobs_in_one_window() {
    let mut model = Model::default();
    model.files.extend(l0_run(1, 96, T0 - 100_000));
    model.files.extend(l0_run(97, 100, T0 + 1000));
    let cfg = defaults();
    let mut sizes = Vec::new();
    let mut now = T0 + 5000;
    loop {
        match due_levels(&model.inv(), at(now), &cfg) {
            Due::Run(Job::Compact(job)) => {
                if job.level() == level(1) {
                    sizes.push(job.inputs().files().len());
                }
                model.apply(&Job::Compact(job), now);
            }
            Due::Run(job) => model.apply(&job, now),
            Due::Wait { .. } => break,
        }
        now += 1;
    }
    assert_eq!(sizes, vec![32, 32, 32, 4]);
}

#[test]
fn a_hole_in_the_source_is_the_same_hole_in_the_destination() {
    let mut model = Model::default();
    model.files.extend(l0_run(1, 3, T0 - 100_000));
    model.files.extend(l0_run(5, 6, T0 - 100_000));
    let cfg = defaults();
    let mut outputs = Vec::new();
    for _ in 0..10 {
        match due_levels(&model.inv(), at(T0), &cfg) {
            Due::Run(job) => {
                if let Job::Compact(c) = &job
                    && c.level() == level(1)
                {
                    outputs.push(c.output_key());
                }
                model.apply(&job, T0);
            }
            Due::Wait { .. } => break,
        }
    }
    assert_eq!(outputs, vec![key(1, 1, 3), key(1, 5, 6)]);
    let l1 = model.inv();
    assert!(
        l1.level(level(1))
            .iter()
            .all(|f| !f.key.range().contains(txid(4)))
    );
}

#[test]
fn a_source_file_straddling_the_destination_max_is_skipped_as_in_go() {
    let files = vec![
        file(1, 1, 2, T0 - 100_000),
        file(1, 3, 6, T0 - 100_000),
        file(1, 7, 8, T0 - 100_000),
        file(2, 1, 4, T0 - 100_000),
    ];
    let job = run_job(due_at(&inv(files), T0));
    assert_eq!(input_keys(compact_job(&job)), vec![key(1, 7, 8)]);
}

#[test]
fn level_one_goes_before_level_two_and_both_before_the_snapshot() {
    let files = vec![file(0, 3, 3, T0 - 100_000), file(1, 1, 2, T0 - 100_000)];
    let job = run_job(due_at(&inv(files), T0));
    assert_eq!(compact_job(&job).level(), level(1));
    let files = vec![file(1, 1, 2, T0 - 100_000)];
    let job = run_job(due_at(&inv(files), T0));
    assert_eq!(compact_job(&job).level(), level(2));
}

#[test]
fn a_first_snapshot_is_due_at_once() {
    let i = inv(l0_run(1, 3, T0));
    // L1 is too early for neither reason: it is empty, so compaction comes first.
    let mut files = i.files().to_vec();
    files.extend([
        file(1, 1, 3, T0 - 1),
        file(2, 1, 3, T0 - 1),
        file(3, 1, 3, T0 - 1),
    ]);
    let got = due_at(&inv(files), T0 + 1);
    // Level 0 holds the database from transaction 1, so the snapshot is built from it.
    let Due::Run(Job::Snapshot(job)) = got else {
        panic!("{got:?}");
    };
    assert_eq!((job.after, job.through), (None, txid(3)));
    assert_eq!(
        job.from.as_ref().map(CompactionInputs::files),
        Some(i.level(Level::ZERO))
    );
}

/// Level 0 that does not start at transaction 1 holds no database to build from: the
/// snapshot reads the database file.
#[test]
fn a_first_snapshot_is_read_from_the_database_when_level_zero_starts_later() {
    let mut files = l0_run(2, 4, T0);
    files.extend([
        file(1, 2, 4, T0 - 1),
        file(2, 2, 4, T0 - 1),
        file(3, 2, 4, T0 - 1),
    ]);
    let got = due_at(&inv(files), T0 + 1);
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: txid(4),
            from: None,
        }))
    );
}

/// A first snapshot built from level 0 failed: the next one reads the database.
#[test]
fn a_refused_seed_makes_the_first_snapshot_a_read_of_the_database() {
    let mut files = l0_run(1, 3, T0);
    files.extend([
        file(1, 1, 3, T0 - 1),
        file(2, 1, 3, T0 - 1),
        file(3, 1, 3, T0 - 1),
    ]);
    let i = inv(files);
    let mut state = MaintainState::default();
    state.refuse_seed();
    let got = due_state(&i, at(T0 + 1), &defaults(), None, &mut state);
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: txid(3),
            from: None,
        }))
    );
}

/// Level 0 with a hole, or more files than one compaction takes, does not reach the newest
/// transaction: the snapshot reads the database, at that transaction.
#[test]
fn a_level_zero_that_stops_short_of_the_newest_transaction_gives_no_inputs() {
    let mut files = l0_run(1, 2, T0);
    files.extend(l0_run(5, 6, T0));
    files.extend([
        file(1, 1, 6, T0 - 1),
        file(2, 1, 6, T0 - 1),
        file(3, 1, 6, T0 - 1),
    ]);
    let got = due_at(&inv(files), T0 + 1);
    let Due::Run(Job::Snapshot(job)) = got else {
        panic!("{got:?}");
    };
    assert_eq!((job.through, job.from), (txid(6), None));
}

#[test]
fn a_malformed_snapshot_does_not_count_as_one() {
    let mut files = vec![file(9, 2, 5, T0 - 1)];
    files.extend(l0_run(1, 5, T0 - 100_000));
    files.extend([
        file(1, 1, 5, T0 - 1),
        file(2, 1, 5, T0 - 1),
        file(3, 1, 5, T0 - 1),
    ]);
    let got = due_at(&inv(files), T0 + 1);
    let Due::Run(Job::Snapshot(job)) = got else {
        panic!("{got:?}");
    };
    assert_eq!((job.after, job.through), (None, txid(5)));
}

fn converged(snapshot_created: i64, snapshot_max: u64, pos: u64) -> Inventory {
    let mut files = l0_run(1, pos, snapshot_created);
    files.extend([
        file(1, 1, pos, snapshot_created),
        file(2, 1, pos, snapshot_created),
        file(3, 1, pos, snapshot_created),
        snap(snapshot_max, snapshot_created),
    ]);
    inv(files)
}

#[test]
fn an_idle_database_writes_no_snapshot_however_old_the_last_one() {
    let i = converged(T0 - 5 * 86_400_000, 4, 4);
    assert_eq!(
        due_at(&i, T0),
        Due::Wait {
            until: None,
            held: None
        }
    );
}

#[test]
fn a_snapshot_this_window_waits_for_the_next_day() {
    let i = converged(T0 + 1000, 3, 4);
    assert_eq!(
        due_at(&i, T0 + 5000),
        Due::Wait {
            until: Some(at(T0 + 86_400_000)),
            held: None
        }
    );
}

#[test]
fn an_old_snapshot_and_a_newer_position_ask_for_a_new_snapshot() {
    let i = converged(T0 - 1000, 3, 4);
    let got = due_at(&i, T0 + 5000);
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: Some(txid(3)),
            through: txid(4),
            from: None,
        }))
    );
    let got = due(&i, at(T0 + 5000), &defaults(), Some(txid(9)));
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: Some(txid(3)),
            through: txid(9),
            from: None,
        }))
    );
}

/// A replica Go wrote: L1 files of fan-in 500 (no limit in Go, upstream
/// #1467) and a 24-hour snapshot. Each instant below is one Go decides the
/// same way.
#[test]
fn a_go_shaped_inventory_plans_what_go_plans_at_three_instants() {
    let day = 86_400_000;
    let mut files = l0_run(1, 1020, T0 - day);
    // Go's level 1 holds 1..1000 as two files of fan-in 500 and has written
    // nothing since.
    let l1_created = T0 - 90_000;
    files.push(file(1, 1, 500, l1_created));
    files.push(file(1, 501, 1000, l1_created));
    files.push(file(2, 1, 1000, l1_created));
    files.push(file(3, 1, 1000, l1_created));
    files.push(snap(1000, T0 - day - 1000));
    let i = inv(files);
    // Window start for 30 s is T0 and the last L1 file predates it, so L1
    // compacts the 20 files above 1000, as Go's seek does (store.go:797-858,
    // compactor.go:104-195).
    let job = run_job(due_at(&i, T0 + 1000));
    assert_eq!(compact_job(&job).output_key(), key(1, 1001, 1020));
    // Once that output is recorded as created inside the window, Go's level is too early
    // (store.go:797-858) and the next job is the snapshot: its newest
    // snapshot is over 24 h old and the position moved (store.go:584-589).
    let mut done = i.files().to_vec();
    done.push(file(1, 1001, 1020, T0 + 500));
    let done = inv(done);
    let job = run_job(due_at(&done, T0 + 1000));
    assert_eq!(compact_job(&job).level(), level(2));
}

#[test]
fn a_go_shaped_snapshot_waits_inside_its_window_and_runs_after() {
    let day = 86_400_000;
    let i = converged(T0 + 1, 4, 6);
    // The newest snapshot (4) is below position 6, recorded as created inside today's
    // window: too early (store.go:797-858). The next window runs it
    // (store.go:584-589).
    assert_eq!(
        due_at(&i, T0 + 9000),
        Due::Wait {
            until: Some(at(T0 + day)),
            held: None
        }
    );
    let got = due_at(&i, T0 + day);
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: Some(txid(4)),
            through: txid(6),
            from: None,
        }))
    );
}

// The planner harness.

/// An inventory the model executor edits; no bytes.
#[derive(Default, Clone)]
pub(super) struct Model {
    pub(super) files: Vec<FileInfo>,
}

impl Model {
    pub(super) fn inv(&self) -> Inventory {
        inv(self.files.clone())
    }

    pub(super) fn apply(&mut self, job: &Job, now: i64) {
        match job {
            Job::Compact(c) => {
                let last = c.inputs().files().last().unwrap();
                self.files.push(FileInfo {
                    key: c.output_key(),
                    size: 1,
                    listed: ListedTime::Created(CreatedAt::mtime(last.created_at().unwrap().at())),
                });
            }
            Job::Snapshot(s) => self.files.push(file(9, 1, s.through.get(), now)),
            Job::Delete(d) => self.files.retain(|f| !d.keys().contains(&f.key)),
        }
    }
}

/// What every job and every wait must satisfy.
#[derive(Default)]
struct Checker {
    /// Per level: the window start of the last job, and its output's created-at time.
    last: BTreeMap<Level, (Timestamp, Timestamp)>,
    newest_l0: Option<Txid>,
}

impl Checker {
    fn job(&mut self, before: &Inventory, now: Timestamp, cfg: &Maintenance, job: &Job) {
        let Job::Compact(c) = job else { return };
        let files = c.inputs().files();
        assert!((1..=MAX_INPUTS).contains(&files.len()));
        for pair in files.windows(2) {
            assert_eq!(pair.len(), 2);
            assert_eq!(
                pair[0].key.range().max().next(),
                Some(pair[1].key.range().min())
            );
        }
        // Level 0 has no hole in these histories, so no level gains one: every output
        // starts at the transaction after its level's newest file.
        let dst = before.level(c.level());
        if let Some(last) = dst.last() {
            assert_eq!(
                Some(c.inputs().range().min()),
                last.key.range().max().next(),
                "does not follow {last:?}"
            );
        }
        let interval = cfg.interval(c.level()).unwrap();
        let start = go_truncate(now, interval);
        if let Some((window, created_at)) = self.last.get(&c.level())
            && *window == start
        {
            assert!(
                *created_at <= start,
                "a second job in one window at {}",
                c.level()
            );
        }
        let created_at = files.last().unwrap().created_at().unwrap().at();
        self.last.insert(c.level(), (start, created_at));
    }

    fn after(&self, model: &Model) {
        let Some(newest) = self.newest_l0 else { return };
        match plan(&model.inv(), KeyTarget::Latest) {
            PlanOutcome::Plan(chain) => assert_eq!(chain.reaches(), newest),
            other @ (PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::AsOfInsideFile { .. }
            | PlanOutcome::Gap { .. }
            | PlanOutcome::InsideFile { .. }
            | PlanOutcome::MalformedSnapshot { .. }) => panic!("no plan after a job: {other:?}"),
        }
    }

    /// A `Wait` means every level is idle or too early: the idle branch for a
    /// run that cannot be built is never taken.
    fn waiting(inv: &Inventory, now: Timestamp, cfg: &Maintenance) {
        for l in 1..=cfg.top().get() {
            let dst = level(l);
            let src = inv.level(level(l - 1));
            let last = inv.level(dst).last();
            let covered = super::snapshot_end(inv);
            let pending = src.iter().any(|f| match last {
                Some(last) => f.key.range().min() > last.key.range().max(),
                None => covered.is_none_or(|end| f.key.range().max() > end),
            });
            if pending {
                let start = go_truncate(now, cfg.interval(dst).unwrap());
                assert!(
                    last.is_some_and(|f| f.created_at().unwrap().at() > start),
                    "level {l} idle with work"
                );
            }
        }
    }
}

/// Runs jobs until `due` waits; returns the wait time.
fn drive(model: &mut Model, check: &mut Checker, now: i64, cfg: &Maintenance) -> Option<Timestamp> {
    for _ in 0..5000 {
        let before = model.inv();
        let got = due(&before, at(now), cfg, None);
        assert_eq!(got, due(&before, at(now), cfg, None), "not deterministic");
        match got {
            Due::Run(job) => {
                check.job(&before, at(now), cfg, &job);
                model.apply(&job, now);
                check.after(model);
            }
            Due::Wait { until, .. } => {
                Checker::waiting(&before, at(now), cfg);
                return until;
            }
        }
    }
    panic!("due never settled");
}

pub(super) fn small() -> Maintenance {
    Maintenance::new(
        vec![secs(10), secs(60), secs(600)],
        secs(3600),
        secs(3600),
        L0Retention::Keep,
    )
    .unwrap()
}

pub(super) fn history() -> impl Strategy<Value = (usize, Vec<(u8, u32)>)> {
    (
        0..2usize,
        prop::collection::vec((0u8..10, 0u32..200_000), 0..250),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn the_planner_keeps_its_promises_over_any_write_history((which, events) in history()) {
        let cfg = if which == 0 { defaults() } else { small() };
        let mut model = Model::default();
        let mut check = Checker::default();
        let (mut now, mut next) = (T0, 1u64);
        for (kind, gap) in events {
            now += i64::from(gap);
            if kind < 7 {
                model.files.push(file(0, next, next, now));
                check.newest_l0 = Txid::new(next);
                next += 1;
            } else {
                drive(&mut model, &mut check, now, &cfg);
            }
        }
        // Converge: follow the wake times until nothing is pending.
        for _ in 0..200 {
            match drive(&mut model, &mut check, now, &cfg) {
                Some(t) => now = t.unix_millis().max(now + 1),
                None => break,
            }
        }
        let settled = model.inv();
        for later in [1, 86_400_000, 1000 * 86_400_000] {
            prop_assert_eq!(
                due(&settled, at(now + later), &cfg, None),
                Due::Wait { until: None, held: None }
            );
        }
    }
}

// Holes: a level never gains a file that leaves a hole a lower level could have closed.

/// `plan_level` into `dst` at `now` under the defaults, on stored created-at times.
fn level_step(inventory: &Inventory, now: i64, dst: u8, state: &MaintainState, view: View) -> Step {
    match plan_level(
        inventory,
        at(now),
        &defaults(),
        level(dst),
        state,
        &ResolvedCreatedAt::default(),
        view,
    ) {
        Ok(step) => step,
        Err(_) => panic!("a stored listing asked for a created-at time"),
    }
}

fn compaction(step: Step) -> CompactJob {
    match step {
        Step::Run(Job::Compact(job)) => job,
        other @ (Step::Run(Job::Snapshot(_) | Job::Delete(_))
        | Step::Wait(_)
        | Step::Idle
        | Step::Whole) => panic!("not a compaction: {other:?}"),
    }
}

fn level_one(ranges: &[(u64, u64)]) -> Vec<FileInfo> {
    ranges
        .iter()
        .map(|(min, max)| file(1, *min, *max, T0 - 100_000))
        .collect()
}

#[test]
fn the_first_hole_is_before_the_first_source_or_between_two_neighbours() {
    let hole = |after: Option<u64>, files: &[(u64, u64)]| {
        first_hole(after.map(txid), level_one(files).iter())
    };
    // Before the first file, one transaction wide and wider.
    assert_eq!(hole(Some(10), &[(12, 15)]), Some(range(11, 11)));
    assert_eq!(hole(Some(10), &[(13, 15)]), Some(range(11, 12)));
    // Between two neighbours.
    assert_eq!(hole(Some(10), &[(11, 12), (15, 16)]), Some(range(13, 14)));
    // The first one, when there are two.
    assert_eq!(
        hole(Some(10), &[(11, 12), (14, 14), (17, 18)]),
        Some(range(13, 13))
    );
    // None: each file starts right after the one before.
    assert_eq!(hole(Some(10), &[(11, 12), (13, 14)]), None);
    assert_eq!(hole(Some(10), &[]), None);
    // A destination with no file may start anywhere, and a hole after its first source counts.
    assert_eq!(hole(None, &[(5, 6), (7, 8)]), None);
    assert_eq!(hole(None, &[(5, 6), (9, 9)]), Some(range(7, 8)));
    // Files that overlap leave no hole, and a short file inside a longer one hides none.
    assert_eq!(hole(Some(10), &[(11, 14), (13, 16), (17, 17)]), None);
    assert_eq!(hole(Some(10), &[(11, 14), (12, 13), (15, 15)]), None);
    assert_eq!(
        hole(Some(10), &[(11, 14), (12, 13), (16, 16)]),
        Some(range(15, 15))
    );
    // Files at or below the destination's newest change nothing, and one that ends above it
    // holds what follows.
    assert_eq!(
        hole(Some(10), &[(1, 5), (6, 10), (12, 15)]),
        Some(range(11, 11))
    );
    assert_eq!(hole(Some(10), &[(8, 15), (16, 20)]), None);
}

#[test]
fn a_hole_is_closed_from_the_files_the_level_below_holds_inside_it() {
    let files = level_one(&[(1, 10), (11, 12), (13, 14), (15, 16)]);
    let job = fill(&inv(files), level(2), range(11, 14)).unwrap();
    assert_eq!(job.level(), level(2));
    assert_eq!(input_keys(&job), vec![key(1, 11, 12), key(1, 13, 14)]);
}

/// A fill never claims a transaction outside the hole: a file that ends past it is left out,
/// and the fill is the first part of the hole.
#[test]
fn a_fill_stops_before_a_file_that_ends_past_the_hole() {
    let files = level_one(&[(11, 12), (13, 16)]);
    let job = fill(&inv(files), level(2), range(11, 14)).unwrap();
    assert_eq!(input_keys(&job), vec![key(1, 11, 12)]);
    // A file ending exactly at the hole's last transaction is taken.
    let files = level_one(&[(11, 12), (13, 14)]);
    let job = fill(&inv(files), level(2), range(11, 14)).unwrap();
    assert_eq!(input_keys(&job), vec![key(1, 11, 12), key(1, 13, 14)]);
}

#[test]
fn a_hole_whose_first_transaction_no_file_starts_at_is_not_filled() {
    // The file at the hole's start is larger than the hole.
    assert_eq!(
        fill(&inv(level_one(&[(11, 16)])), level(2), range(11, 14)),
        None
    );
    // The level below holds the hole's first transaction in a file that starts before it.
    let straddling = level_one(&[(9, 12), (13, 14)]);
    assert_eq!(fill(&inv(straddling), level(2), range(11, 14)), None);
    // Also when that file starts at the first transaction there is, and the level below it
    // holds the hole in files of its own.
    let mut from_the_start = l0_run(11, 14, T0);
    from_the_start.extend(level_one(&[(1, 12), (13, 14)]));
    assert_eq!(fill(&inv(from_the_start), level(2), range(11, 14)), None);
    // Nothing below holds it: the range is lost.
    assert_eq!(
        fill(
            &inv(level_one(&[(1, 10), (15, 16)])),
            level(2),
            range(11, 14)
        ),
        None
    );
    assert_eq!(fill(&inv([]), level(2), range(11, 14)), None);
    // Level 0 has nothing below it.
    assert_eq!(
        fill(&inv(l0_run(11, 14, T0)), Level::ZERO, range(11, 14)),
        None
    );
}

/// Level 1 is filled from level 0 while level 0 still holds the files.
#[test]
fn a_hole_in_level_one_is_closed_from_level_zero() {
    let job = fill(&inv(l0_run(9, 16, T0)), level(1), range(11, 14)).unwrap();
    assert_eq!(job.level(), level(1));
    assert_eq!(
        input_keys(&job),
        (11..=14).map(|n| key(0, n, n)).collect::<Vec<_>>()
    );
}

/// When the level below lacks the hole's start too, its own hole is closed first, from the
/// level below it, and no wider than either hole.
#[test]
fn a_hole_the_level_below_shares_is_closed_there_first() {
    // Level 2 has nothing from 11 to 20; level 1 holds all of it.
    let mut files = level_one(&[(11, 12), (13, 20), (21, 22)]);
    files.extend([file(2, 1, 10, T0), file(2, 21, 30, T0)]);
    let job = fill(&inv(files), level(3), range(11, 20)).unwrap();
    assert_eq!(job.level(), level(2));
    assert_eq!(input_keys(&job), vec![key(1, 11, 12), key(1, 13, 20)]);
    // Level 2 resumes inside the hole, at 16: its own hole is 11 to 15.
    let mut files = level_one(&[(11, 12), (13, 15), (16, 18)]);
    files.push(file(2, 16, 20, T0));
    let job = fill(&inv(files), level(3), range(11, 20)).unwrap();
    assert_eq!(job.level(), level(2));
    assert_eq!(input_keys(&job), vec![key(1, 11, 12), key(1, 13, 15)]);
    // Level 2 holds nothing at all past the hole's start.
    let job = fill(
        &inv(level_one(&[(11, 12), (13, 20)])),
        level(3),
        range(11, 20),
    )
    .unwrap();
    assert_eq!(job.level(), level(2));
    assert_eq!(input_keys(&job), vec![key(1, 11, 12), key(1, 13, 20)]);
}

/// The replica the trial drill left: level 1 whole, level 2 without 11 to 20, level 3 behind.
fn level_two_with_a_hole(level_three_created: i64) -> Vec<FileInfo> {
    let mut files = level_one(&[(1, 10), (11, 20), (21, 30)]);
    files.extend([
        file(2, 1, 10, T0 - 100_000),
        file(2, 21, 30, T0 - 100_000),
        file(3, 1, 10, level_three_created),
        snap(30, T0 - 100_000),
    ]);
    files
}

/// The fill comes before the compaction it makes possible, whatever the destination's window:
/// Go's compaction into level 3 fails at every tick while level 2 has the hole.
#[test]
fn a_whole_listing_closes_a_hole_before_the_window_opens() {
    for created in [T0 - 100_000, T0 + 1] {
        let files = level_two_with_a_hole(created);
        let job = run_job(due_at(&inv(files.clone()), T0 + 2));
        assert_eq!(compact_job(&job).output_key(), key(2, 11, 20), "{created}");
        assert_eq!(input_keys(compact_job(&job)), vec![key(1, 11, 20)]);
    }
    // With the hole closed, level 3 takes the whole run in one file once its window is open.
    let mut files = level_two_with_a_hole(T0 - 100_000);
    files.push(file(2, 11, 20, T0 - 100_000));
    let job = run_job(due_at(&inv(files), T0 + 2));
    assert_eq!(
        input_keys(compact_job(&job)),
        vec![key(2, 11, 20), key(2, 21, 30)]
    );
}

/// A hole goes before every other job, at whatever level: a lower level's compaction would
/// be followed by passes that list only tails, where the hole no longer shows.
#[test]
fn a_hole_is_closed_before_a_lower_level_compacts() {
    let mut files = level_two_with_a_hole(T0 - 100_000);
    files.extend(l0_run(31, 33, T0 - 100_000));
    let job = run_job(due_at(&inv(files.clone()), T0 + 2));
    assert_eq!(compact_job(&job).output_key(), key(2, 11, 20));
    // Closed, the lower level has its turn.
    files.push(file(2, 11, 20, T0 - 100_000));
    let job = run_job(due_at(&inv(files), T0 + 2));
    assert_eq!(compact_job(&job).output_key(), key(1, 31, 33));
}

/// A fill whose level is backing off holds nothing else back.
#[test]
fn a_fill_that_waits_leaves_the_other_levels_to_compact() {
    let mut files = level_two_with_a_hole(T0 - 100_000);
    files.extend(l0_run(31, 33, T0 - 100_000));
    let files = inv(files);
    let mut state = MaintainState::default();
    let fill = compaction(level_step(&files, T0, 3, &state, View::Whole));
    let failed = Outcome::Failed(MaintainError::Compact(CompactError::Dest(delete_fails())));
    state.record(at(T0), &Job::Compact(fill), &failed);
    let job = run_job(due_state(&files, at(T0 + 2), &defaults(), None, &mut state));
    assert_eq!(compact_job(&job).output_key(), key(1, 31, 33));
}

/// A rejected output is never an input, of a fill as of any compaction, and a level that holds
/// one gains nothing before its delete: while the fill's level or one below holds one, the
/// hole is neither closed nor planned past.
#[test]
fn a_fill_waits_for_the_delete_of_a_rejected_output_at_or_below_its_level() {
    let files = inv(level_two_with_a_hole(T0 - 100_000));
    let reject = |dst: u8, inputs: Vec<FileInfo>| {
        let job = CompactJob::new(CompactionInputs::new(inputs).unwrap(), level(dst)).unwrap();
        let mut state = MaintainState::default();
        let failed = Outcome::Failed(MaintainError::OutputDistrusted {
            key: job.output_key(),
            source: CompactError::Dest(delete_fails()),
        });
        state.record(at(T0), &Job::Compact(job), &failed);
        state
    };
    let fill = plan_fill(&files, at(T0), level(3), &MaintainState::default());
    assert_eq!(compaction(fill.unwrap()).output_key(), key(2, 11, 20));
    // In the level the fill reads.
    let below = reject(1, l0_run(31, 33, T0));
    assert_eq!(
        plan_fill(&files, at(T0), level(3), &below),
        Some(Step::Idle)
    );
    assert_eq!(level_step(&files, T0, 3, &below, View::Whole), Step::Idle);
    // In the level the fill writes: the delete goes first.
    let own = reject(2, level_one(&[(31, 40)]));
    assert_eq!(plan_fill(&files, at(T0), level(3), &own), Some(Step::Idle));
    // In the level above, which the fill neither reads nor writes.
    let above = reject(3, vec![file(2, 31, 40, T0)]);
    let fill = plan_fill(&files, at(T0), level(3), &above);
    assert_eq!(compaction(fill.unwrap()).output_key(), key(2, 11, 20));
}

/// A fill that keeps failing waits out its own level's backoff: it is neither planned again at
/// once nor given up for a compaction past the hole.
#[test]
fn a_failing_fill_waits_for_its_level_and_the_hole_is_not_crossed() {
    let files = inv(level_two_with_a_hole(T0 - 100_000));
    let mut state = MaintainState::default();
    let fill = compaction(level_step(&files, T0, 3, &state, View::Whole));
    assert_eq!(fill.level(), level(2));
    let failed = Outcome::Failed(MaintainError::Compact(CompactError::Dest(delete_fails())));
    state.record(at(T0), &Job::Compact(fill), &failed);
    let retry = state.backing_off_until(level(2), at(T0)).unwrap();
    assert_eq!(
        level_step(&files, T0, 3, &state, View::Whole),
        Step::Wait(retry)
    );
}

/// A job that failed at `level` at `T0`, recorded in `state`: the level is backing off.
fn fail_a_compaction_at(state: &mut MaintainState, dst: u8, inputs: Vec<FileInfo>) -> Timestamp {
    let job = CompactJob::new(CompactionInputs::new(inputs).unwrap(), level(dst)).unwrap();
    let failed = Outcome::Failed(MaintainError::Compact(CompactError::Dest(delete_fails())));
    state.record(at(T0), &Job::Compact(job), &failed);
    state.backing_off_until(level(dst), at(T0)).unwrap()
}

/// A fill that starts two levels down writes the lower level, and it is that level's backoff
/// it waits out: a fill that keeps failing is not planned again at every pass.
#[test]
fn a_fill_that_starts_two_levels_down_waits_for_the_level_it_writes() {
    let mut files = l0_run(11, 20, T0 - 100_000);
    files.extend(level_one(&[(1, 10)]));
    files.extend([
        file(2, 1, 10, T0 - 100_000),
        file(2, 21, 30, T0 - 100_000),
        file(3, 1, 10, T0 - 100_000),
    ]);
    let files = inv(files);
    let mut state = MaintainState::default();
    let fill = compaction(level_step(&files, T0, 3, &state, View::Whole));
    assert_eq!(fill.output_key(), key(1, 11, 20));
    let failed = Outcome::Failed(MaintainError::Compact(CompactError::Dest(delete_fails())));
    state.record(at(T0), &Job::Compact(fill), &failed);
    let retry = state.backing_off_until(level(1), at(T0)).unwrap();
    assert_eq!(state.backing_off_until(level(2), at(T0)), None);
    assert_eq!(
        level_step(&files, T0, 3, &state, View::Whole),
        Step::Wait(retry)
    );
}

/// While a level a fill would write waits out a failure, a tail whose sources do not follow
/// waits with it: one whole listing per attempt, not one per pass.
#[test]
fn a_tail_waits_out_a_failed_fill_instead_of_listing_whole_again() {
    // The source level itself.
    let mut files = level_one(&[(21, 30)]);
    files.push(file(2, 1, 10, T0 - 100_000));
    let mut state = MaintainState::default();
    let retry = fail_a_compaction_at(&mut state, 1, l0_run(11, 20, T0));
    assert_eq!(
        level_step(&inv(files.clone()), T0, 2, &state, View::Tail),
        Step::Wait(retry)
    );
    // A level further down, where a fill that starts there would write.
    let two = vec![file(2, 21, 30, T0 - 100_000), file(3, 1, 10, T0 - 100_000)];
    assert_eq!(
        level_step(&inv(two), T0, 3, &state, View::Tail),
        Step::Wait(retry)
    );
    // A level above the sources is no part of a fill.
    let mut above = MaintainState::default();
    fail_a_compaction_at(&mut above, 3, vec![file(2, 1, 10, T0)]);
    assert_eq!(
        level_step(&inv(files), T0, 2, &above, View::Tail),
        Step::Whole
    );
}

/// A source file that starts at or below the destination's newest and ends above it holds
/// those transactions: nothing is written under it.
#[test]
fn a_source_that_straddles_the_destinations_newest_file_is_not_filled_under() {
    let mut files = l0_run(11, 15, T0 - 100_000);
    files.extend(level_one(&[(8, 15), (16, 20)]));
    files.push(file(2, 1, 10, T0 - 100_000));
    let step = level_step(&inv(files), T0, 2, &MaintainState::default(), View::Whole);
    assert_eq!(compaction(step).output_key(), key(2, 16, 20));
}

/// A range no lower level holds is lost, and the level goes on past it as before: the same
/// hole one level up, and no file that claims the transactions.
#[test]
fn a_whole_listing_goes_past_a_range_no_level_holds() {
    let mut files = level_one(&[(1, 10), (21, 30)]);
    files.push(file(2, 1, 10, T0 - 100_000));
    let job = compaction(level_step(
        &inv(files),
        T0,
        2,
        &MaintainState::default(),
        View::Whole,
    ));
    assert_eq!(input_keys(&job), vec![key(1, 21, 30)]);
}

/// A short pass sees only tails, so it cannot tell a lost range from one a lower level still
/// holds: it plans nothing past a hole and asks for the whole listing, once the window is open.
#[test]
fn a_tail_whose_sources_do_not_follow_the_destination_asks_for_a_whole_listing() {
    let state = MaintainState::default();
    let mut files = level_one(&[(21, 30)]);
    files.push(file(2, 1, 10, T0 - 100_000));
    assert_eq!(
        level_step(&inv(files.clone()), T0, 2, &state, View::Tail),
        Step::Whole
    );
    // One transaction is enough.
    let mut one = level_one(&[(12, 30)]);
    one.push(file(2, 1, 10, T0 - 100_000));
    assert_eq!(
        level_step(&inv(one), T0, 2, &state, View::Tail),
        Step::Whole
    );
    // A closed window is a wait, with no whole listing for it.
    let mut closed = level_one(&[(21, 30)]);
    closed.push(file(2, 1, 10, T0 + 1));
    assert_eq!(
        level_step(&inv(closed), T0 + 2, 2, &state, View::Tail),
        Step::Wait(at(T0 + 300_000))
    );
}

/// A tail plans the run that follows the destination's newest file, and never a fill: the
/// files that would close a hole further on are below what it listed.
#[test]
fn a_tail_plans_the_run_that_follows_and_leaves_a_later_hole_to_a_whole_listing() {
    let mut files = l0_run(15, 16, T0 - 100_000);
    files.extend(level_one(&[(11, 14), (17, 20)]));
    files.push(file(2, 1, 10, T0 - 100_000));
    let tail = level_step(
        &inv(files.clone()),
        T0,
        2,
        &MaintainState::default(),
        View::Tail,
    );
    assert_eq!(input_keys(&compaction(tail)), vec![key(1, 11, 14)]);
    // The whole listing closes the hole first.
    let whole = level_step(&inv(files), T0, 2, &MaintainState::default(), View::Whole);
    assert_eq!(compaction(whole).output_key(), key(1, 15, 16));
    // A destination with no file follows from wherever its sources start.
    let empty = level_step(
        &inv(level_one(&[(21, 30)])),
        T0,
        2,
        &MaintainState::default(),
        View::Tail,
    );
    assert_eq!(input_keys(&compaction(empty)), vec![key(1, 21, 30)]);
}

// When an upload makes the next listing due.

/// Go's defaults: the earlier of level 1's next 30 s window and 15 s from now.
#[test]
fn an_upload_lists_at_the_next_window_or_the_check_interval() {
    let go = defaults();
    // 10 s into a window: the check interval, 15 s later, comes first.
    let window = go_truncate(at(T0), iv(30)).unix_millis();
    assert_eq!(
        listing_after_upload(at(window + 1_000), &go),
        at(window + 16_000)
    );
    // 20 s into it: the window's end comes first.
    assert_eq!(
        listing_after_upload(at(window + 20_000), &go),
        at(window + 30_000)
    );
    // With nothing to delete from level 0 only the windows wake it.
    let keep = Maintenance::new(
        vec![secs(30), secs(300), secs(3600)],
        secs(86_400),
        secs(86_400),
        L0Retention::Keep,
    )
    .unwrap();
    let off = go.clone().with_retention(RetentionMode::Off);
    for cfg in [keep, off] {
        assert_eq!(
            listing_after_upload(at(window + 1_000), &cfg),
            at(window + 30_000)
        );
    }
    let slow = go.with_l0_check_interval(iv(60));
    assert_eq!(
        listing_after_upload(at(window + 1_000), &slow),
        at(window + 30_000)
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Always after the upload, never after the next window of any level or
    /// of the snapshot, and never after the check interval while level 0 is
    /// deleted.
    #[test]
    fn an_upload_s_listing_is_after_it_and_no_later_than_any_wake(
        now in -10_000_000_000i64..10_000_000_000,
        which in 0..2usize,
        check in 1u64..120,
        off in any::<bool>(),
    ) {
        let base = if which == 0 { defaults() } else { small() };
        let mode = if off { RetentionMode::Off } else { RetentionMode::Enforce };
        let cfg = base.with_l0_check_interval(iv(check)).with_retention(mode);
        let wake = listing_after_upload(at(now), &cfg);
        prop_assert!(wake > at(now));
        for l in 1..=9 {
            let interval = if l == 9 { Some(cfg.snapshot_interval()) } else { cfg.interval(level(l)) };
            if let Some(interval) = interval {
                let (_, next) = next_window(at(now), interval);
                prop_assert!(wake <= next);
            }
        }
        let deletes_l0 = !off && matches!(cfg.l0_retention(), L0Retention::After(_));
        if deletes_l0 {
            let millis = i64::try_from(check).unwrap() * 1000;
            prop_assert!(wake <= at(now + millis));
        }
    }
}

// The schedule on a listing that carries only last-modified times.

/// A lazy store holding `files` (one byte each, recorded as created as listed).
fn lazy_store(files: &[FileInfo]) -> MemStore {
    let store = MemStore::with_listed_last_modified();
    block_on(async {
        for f in files {
            let created_at = f.created_at().unwrap().at();
            let mut up = store
                .begin_upload(&f.key, created_at, WriteMode::Replace)
                .await
                .unwrap();
            up.write(b"x").await.unwrap();
            up.commit().await.unwrap();
        }
    });
    store
}

/// `due` over a lazy listing of `files`: the decision and the keys whose
/// created-at times it asked for.
pub(super) fn lazy_due(
    files: &[FileInfo],
    now: i64,
    cfg: &Maintenance,
    published: Option<Txid>,
) -> (Due, Vec<FileKey>) {
    let store = lazy_store(files);
    let listing = load(&store);
    let mut state = MaintainState::default();
    let mut asked = Vec::new();
    let mut step = |resolved: &ResolvedCreatedAt| {
        let got = super::due(&listing, at(now), cfg, published, &mut state, resolved);
        if let Resolve::Need(file) = &got {
            asked.push(file.first().key);
        }
        got
    };
    let done = block_on(crate::created_at::resolve(&store, &mut step)).unwrap();
    assert_eq!(done.created_at_read, store.created_at_calls());
    assert_eq!(done.created_at_read, u64::try_from(asked.len()).unwrap());
    (done.value, asked)
}

#[test]
fn schedule_reads_only_lasts_with_work() {
    // L1 has files above its last, so its last file's created-at time is needed and
    // no other: L2 is empty, L0 and the snapshot decide without a created-at time.
    let mut files = l0_run(1, 5, T0 - 100_000);
    files.push(file(1, 1, 3, T0 - 1));
    let (got, asked) = lazy_due(&files, T0 + 1, &defaults(), None);
    assert_eq!(compact_job(&run_job(got)).level(), level(1));
    assert_eq!(asked, vec![key(1, 1, 3)]);

    // Every level caught up and no snapshot yet: the snapshot is due
    // without any created-at time.
    let mut files = l0_run(1, 3, T0 - 100_000);
    files.extend([
        file(1, 1, 3, T0 - 1),
        file(2, 1, 3, T0 - 1),
        file(3, 1, 3, T0 - 1),
    ]);
    let (got, asked) = lazy_due(&files, T0 + 1, &defaults(), None);
    assert!(matches!(got, Due::Run(Job::Snapshot(_))), "{got:?}");
    assert!(asked.is_empty(), "{asked:?}");

    // Only an otherwise-due snapshot asks for the newest snapshot's created-at time.
    let due_files = converged(T0 - 1000, 3, 4).files().to_vec();
    let (got, asked) = lazy_due(&due_files, T0 + 5000, &defaults(), None);
    assert!(matches!(got, Due::Run(Job::Snapshot(_))), "{got:?}");
    assert_eq!(asked, vec![key(9, 1, 3)]);
    // Idle, nothing is due: the schedule asks for no snapshot created-at time, so every
    // created-at time read is retention's (lasts, snapshot starts, scanned level 0).
    let idle_files = converged(T0 - 5 * 86_400_000, 4, 4).files().to_vec();
    let (got, asked) = lazy_due(&idle_files, T0, &defaults(), None);
    assert_eq!(
        got,
        Due::Wait {
            until: None,
            held: None
        }
    );
    assert!(
        asked.contains(&key(9, 1, 4)),
        "retention reads snapshot starts"
    );
}

/// Two decisions agree: a compaction is the same when it reads the same
/// keys, whatever created-at time kind its inputs were listed with.
fn same_decision(a: &Due, b: &Due) -> bool {
    match (a, b) {
        (Due::Run(Job::Compact(x)), Due::Run(Job::Compact(y))) => {
            x.output_key() == y.output_key() && input_keys(x) == input_keys(y)
        }
        (Due::Run(Job::Snapshot(x)), Due::Run(Job::Snapshot(y))) => {
            let keys = |job: &SnapshotJob| {
                job.from.as_ref().map(|inputs| {
                    inputs
                        .files()
                        .iter()
                        .map(|info| info.key)
                        .collect::<Vec<_>>()
                })
            };
            (x.after, x.through, keys(x)) == (y.after, y.through, keys(y))
        }
        _ => a == b,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// A lazy listing decides what the stored one does at every tick of a
    /// write history, and never asks for a created-at time it could do without.
    #[test]
    fn due_matches_main_on_stored_listings((which, events) in history()) {
        let cfg = if which == 0 { defaults() } else { small() };
        let mut model = Model::default();
        let (mut now, mut next) = (T0, 1u64);
        for (kind, gap) in events {
            now += i64::from(gap);
            if kind < 7 {
                model.files.push(file(0, next, next, now));
                next += 1;
                continue;
            }
            for _ in 0..5000 {
                let want = due(&model.inv(), at(now), &cfg, None);
                let (got, asked) = lazy_due(&model.files, now, &cfg, None);
                prop_assert!(same_decision(&got, &want), "{got:?} vs {want:?}");
                let mut sorted = asked.clone();
                sorted.sort();
                sorted.dedup();
                prop_assert_eq!(sorted.len(), asked.len());
                match want {
                    Due::Run(job) => model.apply(&job, now),
                    Due::Wait { .. } => break,
                }
            }
        }
    }
}

// The executor.

const PAGE: u32 = 4096;

fn ltx(min: u64, max: u64, created: i64) -> Vec<u8> {
    ltx_filled(min, max, created, u8::try_from(max % 251).unwrap())
}

/// [`ltx`] with page 1 filled with `fill`.
fn ltx_filled(min: u64, max: u64, created: i64, fill: u8) -> Vec<u8> {
    let page_size = PageSize::new(PAGE).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: 1,
        txids: range(min, max),
        timestamp: at(created),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let data = vec![fill; page_size.bytes()];
    encoder
        .write_page(PageNo::new(1, page_size).unwrap(), &data)
        .unwrap();
    encoder.finish(None).unwrap().0
}

fn put(store: &MemStore, l: u8, n: u64, created: i64) {
    block_on(async {
        let k = key(l, n, n);
        let mut up = store
            .begin_upload(&k, at(created), WriteMode::Replace)
            .await
            .unwrap();
        up.write(&ltx(n, n, created)).await.unwrap();
        up.commit().await.unwrap();
    });
}

fn load(store: &MemStore) -> Inventory {
    block_on(Inventory::load(store)).unwrap()
}

fn listed(store: &MemStore, l: u8) -> Vec<FileKey> {
    block_on(store.list(level(l), None, PageLimit::MAX))
        .unwrap()
        .files
        .iter()
        .map(|f| f.key)
        .collect()
}

fn three_l0(local: &MemStore, replica: &MemStore) {
    for n in 1..=3 {
        put(local, 0, n, T0 - 100_000 + i64::try_from(n).unwrap());
        put(replica, 0, n, T0 - 100_000 + i64::try_from(n).unwrap());
    }
}

fn l1_job(replica: &MemStore) -> CompactJob {
    let job = run_job(due_levels(&load(replica), at(T0), &defaults()));
    compact_job(&job).clone()
}

fn no_level_one_job(replica: &MemStore) {
    match due_at(&load(replica), T0) {
        Due::Run(Job::Compact(c)) => assert_ne!(c.level(), level(1)),
        Due::Run(Job::Snapshot(_)) | Due::Run(Job::Delete(_)) | Due::Wait { .. } => {}
    }
}

#[test]
fn a_compact_job_publishes_its_output_key_and_is_not_planned_again() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    let Outcome::Compacted { info, bytes_read } =
        block_on(run_compact(&job, &Staging(&local), &Replica(&replica)))
    else {
        panic!("not compacted");
    };
    assert_eq!(info.key, job.output_key());
    assert!(bytes_read > 0);
    assert_eq!(listed(&replica, 1), vec![job.output_key()]);
    assert_eq!(listed(&replica, 0).len(), 3, "inputs are never deleted");
    no_level_one_job(&replica);
}

/// The replica's objects keep their trailers but have a page byte flipped,
/// so they do not decode: the compaction succeeds only if it read the local
/// copies, after comparing the trailers.
#[test]
fn level_one_reads_the_local_copy_first() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &MemStore::new());
    for n in 1..=3 {
        let created = T0 - 100_000 + i64::try_from(n).unwrap();
        let mut bytes = ltx(n, n, created);
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        block_on(async {
            let mut up = replica
                .begin_upload(&key(0, n, n), at(created), WriteMode::Replace)
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }
    let job = l1_job(&replica);
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
}

#[test]
fn level_one_falls_back_to_the_replica_when_the_local_copy_is_gone() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&MemStore::new(), &replica);
    let job = l1_job(&replica);
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
    assert_eq!(listed(&replica, 1), vec![key(1, 1, 3)]);
}

fn whole(store: &MemStore, k: &FileKey) -> Vec<u8> {
    block_on(store.read(
        k,
        0,
        crate::store::ReadLen::new(crate::store::ReadLen::MAX_RAW).unwrap(),
    ))
    .unwrap()
}

/// A staged level-0 file that verifies but is not the replica's object under
/// the same key (a staging backup put back, say) is not compaction input:
/// level 1 would hold one content for a transaction while level 0 holds
/// another. Level 1 is built from the replica's copy (seed 0xcf8).
#[test]
fn level_one_is_built_from_the_replica_when_the_local_copy_differs() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let created = T0 - 100_000 + 3;
    let stale = ltx_filled(3, 3, created, 0xee);
    block_on(async {
        let mut up = local
            .begin_upload(&key(0, 3, 3), at(created), WriteMode::Replace)
            .await
            .unwrap();
        up.write(&stale).await.unwrap();
        up.commit().await.unwrap();
    });
    assert_eq!(
        stale.len(),
        whole(&replica, &key(0, 3, 3)).len(),
        "same size, other bytes"
    );
    let job = l1_job(&replica);
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
    let want = MemStore::new();
    three_l0(&MemStore::new(), &want);
    let built = block_on(run_compact(
        &job,
        &Staging(&MemStore::new()),
        &Replica(&want),
    ));
    assert!(matches!(built, Outcome::Compacted { .. }), "{built:?}");
    assert_eq!(
        whole(&replica, &job.output_key()),
        whole(&want, &job.output_key())
    );
}

#[test]
fn an_input_missing_everywhere_is_a_replan() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    block_on(async {
        local.delete(&[key(0, 1, 1)]).await.unwrap();
        replica.delete(&[key(0, 1, 1)]).await.unwrap();
    });
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(
        matches!(out, Outcome::Replan { key: k } if k == key(0, 1, 1)),
        "{out:?}"
    );
    assert!(listed(&replica, 1).is_empty());
}

#[test]
fn a_higher_level_reads_only_the_replica() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    for n in 1..=2 {
        put(&replica, 1, n, T0 - 100_000);
    }
    let job = run_job(due_at(&load(&replica), T0));
    let job = compact_job(&job);
    assert_eq!(job.level(), level(2));
    let out = block_on(run_compact(job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
}

#[test]
fn a_commit_that_fails_after_publish_is_failed_and_the_next_plan_skips_the_level() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    let fault = StoreError::Transient {
        op: StoreOp::Upload,
        kind: std::io::ErrorKind::TimedOut,
    };
    replica.inject_failure_after_publish(fault);
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(
        matches!(
            &out,
            Outcome::Failed(MaintainError::Compact(CompactError::Dest(_)))
        ),
        "{out:?}"
    );
    assert_eq!(listed(&replica, 1), vec![job.output_key()]);
    no_level_one_job(&replica);
}

#[test]
fn a_local_source_read_error_is_failed_and_publishes_nothing() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    let fault = StoreError::Denied {
        op: StoreOp::Read,
        why: crate::store::Denied::Credentials,
    };
    local.inject_failure(StoreOp::Read, fault);
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(
        matches!(
            &out,
            Outcome::Failed(MaintainError::Compact(CompactError::Local(_)))
        ),
        "{out:?}"
    );
    assert!(listed(&replica, 1).is_empty());
}

/// A compaction reads nothing of its output key before it writes: the listing it was planned
/// from shows every file this engine wrote, so the key holds nothing (Go reads nothing either).
/// Run twice by hand, the second run writes the same file again over the first.
#[test]
fn a_compaction_writes_its_output_without_reading_the_key_first() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
    let first = block_on(replica.read(&job.output_key(), 0, ReadLen::CHUNK)).unwrap();
    let again = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(again, Outcome::Compacted { .. }), "{again:?}");
    assert_eq!(listed(&replica, 1), vec![job.output_key()]);
    let after = block_on(replica.read(&job.output_key(), 0, ReadLen::CHUNK)).unwrap();
    assert_eq!(first, after);
}

#[test]
fn run_delete_counts_tolerates_gone_keys_and_maps_a_store_error() {
    let replica = MemStore::new();
    for n in 1..=3 {
        put(&replica, 0, n, T0);
    }
    let job = DeleteJob::new(vec![key(0, 1, 1), key(0, 2, 2), key(0, 9, 9)]).unwrap();
    let out = block_on(run_delete(&job, &replica));
    assert!(matches!(out, Outcome::Deleted { count: 3 }), "{out:?}");
    assert_eq!(listed(&replica, 0), vec![key(0, 3, 3)]);
    let again = block_on(run_delete(&job, &replica));
    assert!(matches!(again, Outcome::Deleted { count: 3 }));
    replica.inject_failure(
        StoreOp::Delete,
        StoreError::Denied {
            op: StoreOp::Delete,
            why: crate::store::Denied::Credentials,
        },
    );
    let failed = block_on(run_delete(&job, &replica));
    assert!(
        matches!(
            &failed,
            Outcome::Failed(MaintainError::Delete(StoreError::Denied { .. }))
        ),
        "{failed:?}"
    );
}

// Review: failing tests (blind adversarial review of the executor and its
// interplay with retention).

const REVIEW_DAY: i64 = 86_400_000;

/// Rewrites `out` at half its length, as a store that acknowledged a short commit holds it.
fn truncate_to_half(replica: &MemStore, out: &FileKey) {
    block_on(async {
        let whole = replica
            .read(
                out,
                0,
                crate::store::ReadLen::new(crate::store::ReadLen::MAX_RAW).unwrap(),
            )
            .await
            .unwrap();
        let created = load(replica).level(level(1))[0].created_at().unwrap().at();
        let mut up = replica
            .begin_upload(out, created, WriteMode::Replace)
            .await
            .unwrap();
        up.write(&whole[..whole.len() / 2]).await.unwrap();
        up.commit().await.unwrap();
    });
}

/// A Retrying destination whose post-commit delete failed leaves a short
/// object listed under the output key ("removed: false": compact.rs says the
/// caller must not trust a listing of it). The next plan trusts it anyway:
/// level 1 counts as done and retention deletes the inputs it "covers", so the
/// only copy of transactions 2 to 4 is an object that does not decode.
#[test]
fn review_a_short_compaction_output_left_by_a_failed_commit_does_not_license_deleting_its_inputs() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    // A snapshot this window, so no snapshot job is due today.
    put(&replica, 9, 1, T0 + 1);
    for n in 2..=5u64 {
        let created = T0 + i64::try_from(n).unwrap() * 600_000;
        put(&local, 0, n, created);
        put(&replica, 0, n, created);
    }
    let cfg = Maintenance::new(
        vec![secs(30)],
        secs(24 * 3600),
        secs(24 * 3600),
        L0Retention::After(secs(300)),
    )
    .unwrap();
    let now = at(T0 + 2 * 3_600_000);
    let mut state = MaintainState::default();
    let due = due_state(&load(&replica), now, &cfg, None, &mut state);
    let Due::Run(Job::Compact(job)) = due else {
        panic!("expected the level-1 job, got {due:?}")
    };
    let out = job.output_key();
    replica.inject_failure_after_publish(StoreError::Corrupt {
        key: Some(out),
        cause: crate::store::CorruptCause::CommitSizeMismatch {
            wrote: 1,
            stored: 0,
            removed: false,
        },
    });
    // The executor's delete of the rejected output fails too.
    replica.inject_failure(
        StoreOp::Delete,
        StoreError::Transient {
            op: StoreOp::Delete,
            kind: std::io::ErrorKind::TimedOut,
        },
    );
    let ran = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(ran, Outcome::Failed(_)), "{ran:?}");
    state.record(now, &Job::Compact(job.clone()), &ran);
    // What the store holds after a size mismatch it could not remove.
    truncate_to_half(&replica, &out);
    let inputs: Vec<FileKey> = job.inputs().files().iter().map(|f| f.key).collect();
    for _ in 0..10 {
        match due_state(&load(&replica), now, &cfg, None, &mut state) {
            Due::Run(Job::Delete(d)) => {
                let doomed: Vec<&FileKey> =
                    d.keys().iter().filter(|k| inputs.contains(k)).collect();
                assert!(
                    doomed.is_empty(),
                    "deletes {doomed:?}, covered only by {out}, which the store holds short"
                );
                block_on(run_delete(&d, &replica));
            }
            Due::Run(Job::Compact(c)) => {
                block_on(run_compact(&c, &Staging(&local), &Replica(&replica)));
            }
            Due::Run(Job::Snapshot(_)) | Due::Wait { .. } => break,
        }
    }
}

/// Go runs each level and the snapshot on their own tickers, so one failing
/// level never stops a snapshot. Here `due` returns the first ready job, and a
/// level-1 job whose local input does not decode is ready again on every
/// fresh listing: the snapshot that has been due for days is never planned,
/// and neither is any delete.
#[test]
fn review_a_level_one_job_that_always_fails_does_not_starve_the_snapshot_forever() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    put(&replica, 9, 1, T0 - 2 * REVIEW_DAY);
    for n in 2..=3u64 {
        let created = T0 - 100_000 + i64::try_from(n).unwrap();
        put(&replica, 0, n, created);
        put(&local, 0, n, created);
    }
    // Transaction 2 rots in both places (a rotten local copy alone now falls
    // back to the replica's, which is the next test), so the job always fails.
    for store in [&local, &replica] {
        block_on(async {
            let k = key(0, 2, 2);
            let mut up = store
                .begin_upload(&k, at(T0 - 100_000), WriteMode::Replace)
                .await
                .unwrap();
            up.write(&[0xAB; 200]).await.unwrap();
            up.commit().await.unwrap();
        });
    }
    let mut state = MaintainState::default();
    let mut snapshot_planned = false;
    for day in 0..3 {
        let now = at(T0 + day * REVIEW_DAY);
        // The loop lists and asks again at the same instant after each failure.
        for _ in 0..3 {
            match due_state(&load(&replica), now, &defaults(), None, &mut state) {
                Due::Run(Job::Compact(job)) => {
                    let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
                    assert!(matches!(out, Outcome::Failed(_)), "{out:?}");
                    state.record(now, &Job::Compact(job), &out);
                }
                Due::Run(Job::Snapshot(_)) => {
                    snapshot_planned = true;
                    break;
                }
                other @ (Due::Run(_) | Due::Wait { .. }) => panic!("unexpected {other:?}"),
            }
        }
        if snapshot_planned {
            break;
        }
    }
    assert!(
        snapshot_planned,
        "a level-1 compaction failing on every attempt kept the due snapshot from being planned for three days"
    );
}

fn corrupt_output_error(removed: bool) -> StoreError {
    StoreError::Corrupt {
        key: None,
        cause: crate::store::CorruptCause::CommitSizeMismatch {
            wrote: 1,
            stored: 0,
            removed,
        },
    }
}

fn delete_fails() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Delete,
        kind: std::io::ErrorKind::TimedOut,
    }
}

/// Two L0 files and a snapshot this window: level 1 is the only job due.
fn rejected_setup() -> (MemStore, MemStore, MaintainState, CompactJob) {
    let (local, replica) = (MemStore::new(), MemStore::new());
    put(&replica, 9, 1, T0 + 1);
    for n in 2..=3u64 {
        let created = T0 + i64::try_from(n).unwrap() * 600_000;
        put(&local, 0, n, created);
        put(&replica, 0, n, created);
    }
    let mut state = MaintainState::default();
    let Due::Run(Job::Compact(job)) = due_state(
        &load(&replica),
        at(T0 + 3_600_000),
        &defaults(),
        None,
        &mut state,
    ) else {
        panic!("expected the level-1 job")
    };
    (local, replica, state, job)
}

#[test]
fn a_rejected_compaction_output_is_deleted_by_the_executor() {
    for mismatch in [
        corrupt_output_error(false),
        StoreError::Corrupt {
            key: None,
            cause: crate::store::CorruptCause::ChecksumMismatch { part: None },
        },
    ] {
        let (local, replica, _, job) = rejected_setup();
        replica.inject_failure_after_publish(mismatch);
        let ran = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
        assert!(
            matches!(ran, Outcome::Failed(MaintainError::Compact(_))),
            "{ran:?}"
        );
        assert!(load(&replica).level(level(1)).is_empty());
    }
}

#[test]
fn a_distrusted_output_holds_retention_until_deleted() {
    let (local, replica, mut state, job) = rejected_setup();
    replica.inject_failure_after_publish(corrupt_output_error(false));
    replica.inject_failure(StoreOp::Delete, delete_fails());
    let ran = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(
        matches!(ran, Outcome::Failed(MaintainError::OutputDistrusted { .. })),
        "{ran:?}"
    );
    let now = at(T0 + 3_600_000);
    state.record(now, &Job::Compact(job.clone()), &ran);
    let out = job.output_key();
    // The level's delete is the next job once its backoff has passed.
    let later = at(T0 + 3_700_000);
    let Due::Run(Job::Delete(delete)) =
        due_state(&load(&replica), later, &defaults(), None, &mut state)
    else {
        panic!("expected the output's delete")
    };
    assert_eq!(delete.keys(), [out]);
    // While it is listed, retention holds (level 1 is backing off, so it is reached).
    let held = due_state(
        &load(&replica),
        at(T0 + 3_605_000),
        &defaults(),
        None,
        &mut state,
    );
    assert!(
        matches!(&held, Due::Wait { held: Some(RetentionHold::OutputDistrusted { key }), .. } if *key == out),
        "{held:?}"
    );
    let done = block_on(run_delete(&delete, &replica));
    state.record(later, &Job::Delete(delete), &done);
    let after = due_state(&load(&replica), later, &defaults(), None, &mut state);
    assert!(
        !matches!(
            after,
            Due::Wait {
                held: Some(RetentionHold::OutputDistrusted { .. }),
                ..
            }
        ),
        "{after:?}"
    );
}

/// A level holding a distrusted output, after the delete that should remove it.
fn distrusted_setup() -> (MemStore, MaintainState, DeleteJob, Timestamp) {
    let (local, replica, mut state, job) = rejected_setup();
    replica.inject_failure_after_publish(corrupt_output_error(false));
    replica.inject_failure(StoreOp::Delete, delete_fails());
    let ran = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    state.record(at(T0 + 3_600_000), &Job::Compact(job), &ran);
    let later = at(T0 + 3_700_000);
    let Due::Run(Job::Delete(delete)) =
        due_state(&load(&replica), later, &defaults(), None, &mut state)
    else {
        panic!("expected the output's delete")
    };
    (replica, state, delete, later)
}

/// A failed delete of a distrusted output backs its level off, as a failed
/// compaction does: the second failure at the level waits twenty seconds.
#[test]
fn a_failed_delete_of_a_distrusted_output_backs_its_level_off() {
    let (_replica, mut state, delete, later) = distrusted_setup();
    let job = Job::Delete(delete);
    let failed = Outcome::Failed(MaintainError::Delete(delete_fails()));
    state.record(later, &job, &failed);
    assert_eq!(
        state.retry_at(&job, later),
        Some(at(T0 + 3_700_000 + 20_000))
    );
}

/// A distrusted output that is gone from the listing, however it went, is
/// forgotten: its delete is not planned again.
#[test]
fn a_distrusted_output_gone_from_the_listing_is_forgotten() {
    let (replica, mut state, delete, later) = distrusted_setup();
    block_on(replica.delete(delete.keys())).unwrap();
    let after = due_state(&load(&replica), later, &defaults(), None, &mut state);
    assert!(
        !matches!(&after, Due::Run(Job::Delete(job)) if job.keys() == delete.keys()),
        "{after:?}"
    );
}

#[test]
fn a_failing_level_backs_off_and_others_run() {
    let (_local, replica, mut state, job) = rejected_setup();
    let now = at(T0 + 3_600_000);
    let failed = Outcome::Failed(MaintainError::Compact(CompactError::Source(
        StoreError::Transient {
            op: StoreOp::Read,
            kind: std::io::ErrorKind::TimedOut,
        },
    )));
    let ask = |state: &mut MaintainState, now: Timestamp| {
        due_state(&load(&replica), now, &defaults(), None, state)
    };
    state.record(now, &Job::Compact(job.clone()), &failed);
    // Level 1 waits ten seconds; nothing else is ready, so the wake time is its retry.
    let Due::Wait { until, .. } = ask(&mut state, at(T0 + 3_605_000)) else {
        panic!("level 1 should be backing off")
    };
    assert_eq!(until, Some(at(T0 + 3_610_000)));
    assert!(matches!(
        ask(&mut state, at(T0 + 3_610_000)),
        Due::Run(Job::Compact(_))
    ));
    // Doubling, capped at MAX_BACKOFF.
    for _ in 0..12 {
        state.record(now, &Job::Compact(job.clone()), &failed);
    }
    let Due::Wait { until, .. } = ask(&mut state, now) else {
        panic!("backing off")
    };
    let cap = i64::try_from(crate::fault::MAX_BACKOFF.as_millis()).unwrap();
    assert_eq!(until, Some(at(T0 + 3_600_000 + cap)));
    // A success clears it.
    let ok = Outcome::Compacted {
        info: file(1, 2, 3, T0),
        bytes_read: 0,
    };
    state.record(now, &Job::Compact(job), &ok);
    assert!(matches!(ask(&mut state, now), Due::Run(Job::Compact(_))));
}

#[test]
fn a_throttled_job_waits_the_later_of_backoff_and_retry_after() {
    let (_local, _replica, mut state, job) = rejected_setup();
    let now = at(T0 + 3_600_000);
    let throttled = |secs: u64| {
        Outcome::Failed(MaintainError::Compact(CompactError::Dest(
            StoreError::Throttled {
                op: StoreOp::Upload,
                retry_after: Some(Duration::from_secs(secs)),
            },
        )))
    };
    let job = Job::Compact(job);
    // Longer than the ten-second backoff: the store's ask wins.
    state.record(now, &job, &throttled(30));
    assert_eq!(state.retry_at(&job, now), Some(at(T0 + 3_630_000)));
    // Shorter than the doubled backoff (twenty seconds): the backoff wins,
    // and it doubled on its own schedule although the last wait was longer.
    state.record(now, &job, &throttled(3));
    assert_eq!(state.retry_at(&job, now), Some(at(T0 + 3_620_000)));
}

#[test]
fn a_corrupt_local_input_falls_back_to_the_replica_copy() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    put(&replica, 0, 2, T0);
    put(&replica, 0, 3, T0);
    put(&local, 0, 2, T0);
    block_on(async {
        let mut up = local
            .begin_upload(&key(0, 3, 3), at(T0), WriteMode::Replace)
            .await
            .unwrap();
        up.write(&[0xAB; 200]).await.unwrap();
        up.commit().await.unwrap();
    });
    let inputs = CompactionInputs::new(vec![file(0, 2, 2, T0), file(0, 3, 3, T0)]).unwrap();
    let job = CompactJob::new(inputs, level(1)).unwrap();
    let ran = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
    assert!(matches!(ran, Outcome::Compacted { .. }), "{ran:?}");
}

/// Local errors a level-1 compaction can meet on the staged copies, and the fault each is:
/// what the staging store's errors mean everywhere else, never a replica's fault.
fn local_read_errors() -> [(StoreError, Fault); 3] {
    let key = key(0, 1, 1);
    [
        (
            StoreError::Denied {
                op: StoreOp::Read,
                why: crate::store::Denied::Permission,
            },
            Fault::LocalIo,
        ),
        (
            StoreError::Transient {
                op: StoreOp::Read,
                kind: std::io::ErrorKind::StorageFull,
            },
            Fault::LocalDiskFull,
        ),
        (
            StoreError::Corrupt {
                key: Some(key),
                cause: crate::store::CorruptCause::NotARegularFile,
            },
            Fault::StagedLost,
        ),
    ]
}

fn failed_fault(out: &Outcome) -> Fault {
    match out {
        Outcome::Failed(err) => Fault::from_maintain(err),
        Outcome::Compacted { .. } | Outcome::Deleted { .. } | Outcome::Replan { .. } => {
            panic!("not failed: {out:?}")
        }
    }
}

/// Comparing the local copies with the replica's: a local read error is a staging fault.
#[test]
fn a_local_read_error_comparing_copies_is_a_staging_fault() {
    for (err, want) in local_read_errors() {
        let (local, replica) = (MemStore::new(), MemStore::new());
        three_l0(&local, &replica);
        let job = l1_job(&replica);
        local.inject_failure(StoreOp::Read, err.clone());
        let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
        assert_eq!(failed_fault(&out), want, "{err}");
        assert!(listed(&replica, 1).is_empty());
    }
}

/// A store whose whole-file reads (from offset 0) fail with `err`; tail reads succeed.
struct FailsWholeReads {
    inner: MemStore,
    err: StoreError,
}

impl ObjectStore for FailsWholeReads {
    type Upload = <MemStore as ObjectStore>::Upload;

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl std::future::Future<Output = Result<crate::store::Listing, StoreError>> + Send {
        self.inner.list(level, after, limit)
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: crate::store::ReadLen,
    ) -> impl std::future::Future<Output = Result<Vec<u8>, StoreError>> + Send {
        let read = self.inner.read(key, offset, len);
        let err = (offset == 0).then(|| self.err.clone());
        async move {
            match err {
                Some(err) => Err(err),
                None => read.await,
            }
        }
    }

    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl std::future::Future<Output = Result<CreatedAt, StoreError>> + Send {
        self.inner.created_at(file)
    }

    fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: crate::store::ReadLen,
    ) -> impl std::future::Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send {
        let read = self.inner.read_with_created_at(file, len);
        let err = self.err.clone();
        async move {
            drop(read.await);
            Err(err)
        }
    }

    fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<Self::Upload, StoreError>> + Send {
        self.inner.begin_sized_upload(key, created_at, mode, len)
    }

    fn delete(
        &self,
        keys: &[FileKey],
    ) -> impl std::future::Future<Output = Result<(), StoreError>> + Send {
        self.inner.delete(keys)
    }
}

/// Reading the local copies once they matched the replica's: a local read error is a staging
/// fault.
#[test]
fn a_local_read_error_while_compacting_is_a_staging_fault() {
    for (err, want) in local_read_errors() {
        let (inner, replica) = (MemStore::new(), MemStore::new());
        three_l0(&inner, &replica);
        let job = l1_job(&replica);
        let local = FailsWholeReads {
            inner,
            err: err.clone(),
        };
        let out = block_on(run_compact(&job, &Staging(&local), &Replica(&replica)));
        assert_eq!(failed_fault(&out), want, "{err}");
        assert!(listed(&replica, 1).is_empty());
    }
}

// What a whole decision reports to the short listings that follow it.

/// The gate of the decision on a listing that holds a single level-0 file, when the caller has
/// published up to `published`.
fn gate_of(config: &Maintenance, published: Option<Txid>) -> Gate {
    let inventory = inv([file(0, 1, 1, T0)]);
    match due_full(
        &inventory,
        at(T0 + 1_000),
        config,
        published,
        &mut MaintainState::default(),
        &ResolvedCreatedAt::default(),
    ) {
        Resolve::Done(decided) => decided.gate,
        Resolve::Need(file) => panic!("a stored listing asked for a created-at time: {file:?}"),
    }
}

/// A listing behind what was published fails the retention gate, unless retention is off, when
/// there is nothing to gate: the short listings that follow rely on the answer.
#[test]
fn a_whole_decision_reports_whether_its_listing_passed_the_retention_gate() {
    let enforcing = defaults();
    assert_eq!(gate_of(&enforcing, None), Gate::Clear);
    assert_eq!(gate_of(&enforcing, Some(txid(1))), Gate::Clear);
    assert_eq!(gate_of(&enforcing, Some(txid(2))), Gate::Held);
    let off = defaults().with_retention(RetentionMode::Off);
    assert_eq!(gate_of(&off, Some(txid(2))), Gate::Clear);
}

#[test]
fn an_output_that_could_not_be_deleted_is_reported_as_distrusted_until_it_is_forgotten() {
    let mut state = MaintainState::default();
    assert!(!state.has_distrusted());
    state.distrust_snapshot(key(9, 1, 5));
    assert!(state.has_distrusted());
    state.forget_unlisted(|_| false);
    assert!(!state.has_distrusted());
}

/// The next window opens after an output created inside the current one, and not after one
/// created before the window began (nor at the very instant it began).
#[test]
fn an_output_closes_the_window_only_when_created_after_its_start() {
    let interval = iv(30);
    let now = at(T0 + 10_000);
    let before = file(1, 1, 1, T0 - 1);
    let on_the_start = file(1, 1, 1, T0);
    let inside = file(1, 1, 1, T0 + 1);
    assert_eq!(window_closed_by(&before, now, interval), None);
    assert_eq!(window_closed_by(&on_the_start, now, interval), None);
    assert_eq!(
        window_closed_by(&inside, now, interval),
        Some(at(T0 + 30_000))
    );
}

/// A one-off schedule is the same settings with the run switched off; nothing else changes,
/// since the retention pass a `-once` drain ends with reads them.
#[test]
fn a_one_off_schedule_keeps_its_settings_and_is_not_run() {
    let ordinary = defaults();
    assert!(!ordinary.is_one_off());
    let once = ordinary.clone().one_off();
    assert!(once.is_one_off());
    assert_eq!(once.top(), ordinary.top());
    assert_eq!(once.snapshot_retention(), ordinary.snapshot_retention());
    assert_eq!(once.retention(), ordinary.retention());
    assert_ne!(once, ordinary);
    assert!(!Maintenance::go_default().is_one_off());
}
