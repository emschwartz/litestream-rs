//! Planner, schedule and executor tests: one test per error variant, Go's
//! window arithmetic, hand-built inventories for each rule of `due`, a
//! property over generated write histories, and the executor on `MemStore`.

use std::collections::BTreeMap;
use std::time::Duration;

use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::layout::{ListedStamp, StoredStamp};
use crate::mem::MemStore;
use crate::plan::{PlanOutcome, RestoreTarget, plan};
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
        listed: ListedStamp::Stored(StoredStamp::mtime(at(created))),
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
    super::due(
        inventory,
        now,
        config,
        published,
        &mut MaintainState::default(),
    )
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
            through: txid(5)
        }))
    );
}

#[test]
fn level_one_compacts_the_whole_source_when_the_destination_is_empty() {
    let i = inv(l0_run(1, 3, T0 - 100_000));
    let job = run_job(due_at(&i, T0));
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
fn a_destination_written_this_window_waits_for_the_next_one() {
    let mut files = l0_run(1, 5, T0 - 100_000);
    files.push(file(1, 1, 3, T0 + 1));
    files.push(snap(5, T0 - 100_000));
    // Window of 30 s starting at T0; the next starts 30 s later. Level 2
    // has L1 input (1..3) with an empty destination and runs first.
    let job = run_job(due_at(&inv(files.clone()), T0 + 10_000));
    assert_eq!(compact_job(&job).level(), level(2));
    files.push(file(2, 1, 3, T0 + 1));
    files.push(file(3, 1, 3, T0 + 1));
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
        match due(&model.inv(), at(now), &cfg, None) {
            Due::Run(Job::Compact(job)) => {
                if job.level() == level(1) {
                    sizes.push(job.inputs().files().len());
                }
                model.apply(&Job::Compact(job), now);
            }
            Due::Run(job) => model.apply(&job, now),
            Due::Wait { until, .. } => {
                assert_eq!(until, None);
                break;
            }
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
        match due(&model.inv(), at(T0), &cfg, None) {
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
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: txid(3)
        }))
    );
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
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: None,
            through: txid(5)
        }))
    );
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
            through: txid(4)
        }))
    );
    let got = due(&i, at(T0 + 5000), &defaults(), Some(txid(9)));
    assert_eq!(
        got,
        Due::Run(Job::Snapshot(SnapshotJob {
            after: Some(txid(3)),
            through: txid(9)
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
    // Once that output is stamped inside the window, Go's level is too early
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
    // The newest snapshot (4) is below position 6, stamped inside today's
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
            through: txid(6)
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
                    listed: ListedStamp::Stored(StoredStamp::mtime(
                        last.exact_stamp().unwrap().at(),
                    )),
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
    /// Per level: the window start of the last job, and its output's stamp.
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
        let dst = before.level(c.level());
        if let Some(last) = dst.last() {
            assert!(
                c.inputs().range().min() > last.key.range().max(),
                "overlaps {last:?}"
            );
        }
        let interval = cfg.interval(c.level()).unwrap();
        let start = go_truncate(now, interval);
        if let Some((window, stamp)) = self.last.get(&c.level())
            && *window == start
        {
            assert!(
                *stamp <= start,
                "a second job in one window at {}",
                c.level()
            );
        }
        let stamp = files.last().unwrap().exact_stamp().unwrap().at();
        self.last.insert(c.level(), (start, stamp));
    }

    fn after(&self, model: &Model) {
        let Some(newest) = self.newest_l0 else { return };
        match plan(&model.inv(), RestoreTarget::Latest) {
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
            let max = last.map(|f| f.key.range().max());
            let pending = src
                .iter()
                .any(|f| max.is_none_or(|m| f.key.range().min() > m));
            if pending {
                let start = go_truncate(now, cfg.interval(dst).unwrap());
                assert!(
                    last.is_some_and(|f| f.exact_stamp().unwrap().at() > start),
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

// The executor.

const PAGE: u32 = 4096;

fn ltx(min: u64, max: u64, created: i64) -> Vec<u8> {
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
    let data = vec![u8::try_from(max % 251).unwrap(); page_size.bytes()];
    encoder
        .write_page(PageNo::new(1, page_size).unwrap(), &data)
        .unwrap();
    encoder.finish(None).unwrap().0
}

fn put(store: &MemStore, l: u8, n: u64, created: i64) {
    block_on(async {
        let k = key(l, n, n);
        let mut up = store.begin_upload(&k, at(created)).await.unwrap();
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
    let job = run_job(due_at(&load(replica), T0));
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
    let Outcome::Compacted { info, bytes_read } = block_on(run_compact(&job, &local, &replica))
    else {
        panic!("not compacted");
    };
    assert_eq!(info.key, job.output_key());
    assert!(bytes_read > 0);
    assert_eq!(listed(&replica, 1), vec![job.output_key()]);
    assert_eq!(listed(&replica, 0).len(), 3, "inputs are never deleted");
    no_level_one_job(&replica);
}

#[test]
fn level_one_reads_the_local_copy_first() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &MemStore::new());
    for n in 1..=3 {
        // The replica's listing names the files; its bytes are never read.
        put(&replica, 0, n, T0 - 100_000);
    }
    let job = l1_job(&replica);
    block_on(async {
        replica
            .delete(&[key(0, 1, 1), key(0, 2, 2), key(0, 3, 3)])
            .await
            .unwrap();
    });
    assert!(matches!(
        block_on(run_compact(&job, &local, &replica)),
        Outcome::Compacted { .. }
    ));
}

#[test]
fn level_one_falls_back_to_the_replica_when_the_local_copy_is_gone() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&MemStore::new(), &replica);
    let job = l1_job(&replica);
    let out = block_on(run_compact(&job, &local, &replica));
    assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
    assert_eq!(listed(&replica, 1), vec![key(1, 1, 3)]);
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
    let out = block_on(run_compact(&job, &local, &replica));
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
    let out = block_on(run_compact(job, &local, &replica));
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
    let out = block_on(run_compact(&job, &local, &replica));
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
fn a_source_read_error_is_failed_and_publishes_nothing() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    let fault = StoreError::Auth { op: StoreOp::Read };
    local.inject_failure(StoreOp::Read, fault);
    let out = block_on(run_compact(&job, &local, &replica));
    assert!(
        matches!(
            &out,
            Outcome::Failed(MaintainError::Compact(CompactError::Source(_)))
        ),
        "{out:?}"
    );
    assert!(listed(&replica, 1).is_empty());
}

#[test]
fn running_a_job_twice_leaves_one_file_at_the_key() {
    let (local, replica) = (MemStore::new(), MemStore::new());
    three_l0(&local, &replica);
    let job = l1_job(&replica);
    for _ in 0..2 {
        let out = block_on(run_compact(&job, &local, &replica));
        assert!(matches!(out, Outcome::Compacted { .. }), "{out:?}");
    }
    assert_eq!(listed(&replica, 1), vec![job.output_key()]);
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
        StoreError::Auth {
            op: StoreOp::Delete,
        },
    );
    let failed = block_on(run_delete(&job, &replica));
    assert!(
        matches!(
            &failed,
            Outcome::Failed(MaintainError::Delete(StoreError::Auth { .. }))
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
        let created = load(replica).level(level(1))[0].exact_stamp().unwrap().at();
        let mut up = replica.begin_upload(out, created).await.unwrap();
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
    let due = super::due(&load(&replica), now, &cfg, None, &mut state);
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
    let ran = block_on(run_compact(&job, &local, &replica));
    assert!(matches!(ran, Outcome::Failed(_)), "{ran:?}");
    state.record(now, &Job::Compact(job.clone()), &ran);
    // What the store holds after a size mismatch it could not remove.
    truncate_to_half(&replica, &out);
    let inputs: Vec<FileKey> = job.inputs().files().iter().map(|f| f.key).collect();
    for _ in 0..10 {
        match super::due(&load(&replica), now, &cfg, None, &mut state) {
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
                block_on(run_compact(&c, &local, &replica));
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
            let mut up = store.begin_upload(&k, at(T0 - 100_000)).await.unwrap();
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
            match super::due(&load(&replica), now, &defaults(), None, &mut state) {
                Due::Run(Job::Compact(job)) => {
                    let out = block_on(run_compact(&job, &local, &replica));
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
    let Due::Run(Job::Compact(job)) = super::due(
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
        let ran = block_on(run_compact(&job, &local, &replica));
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
    let ran = block_on(run_compact(&job, &local, &replica));
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
        super::due(&load(&replica), later, &defaults(), None, &mut state)
    else {
        panic!("expected the output's delete")
    };
    assert_eq!(delete.keys(), [out]);
    // While it is listed, retention holds (level 1 is backing off, so it is reached).
    let held = super::due(
        &load(&replica),
        at(T0 + 3_605_000),
        &defaults(),
        None,
        &mut state,
    );
    assert!(
        matches!(&held, Due::Wait { held: Some(Hold::OutputDistrusted { key }), .. } if *key == out),
        "{held:?}"
    );
    let done = block_on(run_delete(&delete, &replica));
    state.record(later, &Job::Delete(delete), &done);
    let after = super::due(&load(&replica), later, &defaults(), None, &mut state);
    assert!(
        !matches!(
            after,
            Due::Wait {
                held: Some(Hold::OutputDistrusted { .. }),
                ..
            }
        ),
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
        super::due(&load(&replica), now, &defaults(), None, state)
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
        let mut up = local.begin_upload(&key(0, 3, 3), at(T0)).await.unwrap();
        up.write(&[0xAB; 200]).await.unwrap();
        up.commit().await.unwrap();
    });
    let inputs = CompactionInputs::new(vec![file(0, 2, 2, T0), file(0, 3, 3, T0)]).unwrap();
    let job = CompactJob::new(inputs, level(1)).unwrap();
    let ran = block_on(run_compact(&job, &local, &replica));
    assert!(matches!(ran, Outcome::Compacted { .. }), "{ran:?}");
}
