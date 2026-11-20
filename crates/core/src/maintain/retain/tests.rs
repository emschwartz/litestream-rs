//! Retention tests: the trust gate one variant at a time, the rules on
//! hand-built ladders, Go's in-flight restore case, and the invariants as
//! properties over raw listings and over write histories driven through
//! `due` with a model executor that applies delete jobs.

use std::collections::BTreeSet;

use lsr_ltx::{Timestamp, Txid};
use proptest::prelude::*;

use super::*;
use crate::inventory::Inventory;
use crate::maintain::tests::{
    Model, T0, at, defaults, due, file, inv, key, level, secs, small, txid,
};
use crate::maintain::{Due, Job};

/// `retention` from a fresh state.
fn retention(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
) -> Retention {
    super::retention(inventory, now, config, published, &MaintainState::default())
}

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

/// One transaction every four hours.
fn stamp_of(n: u64) -> i64 {
    T0 + i64::try_from(n).unwrap() * 4 * HOUR
}

/// Snapshots at 10, 20 and 30, L1 over each tenth, L0 for 25 to 30: the
/// newest stamp is 120 h after T0, so with 24 h retention the first two
/// snapshots are old and the floor is 20.
fn ladder() -> Vec<FileInfo> {
    let mut files = Vec::new();
    for n in [10, 20, 30] {
        files.push(file(9, 1, n, stamp_of(n)));
    }
    for (a, b) in [(1, 10), (11, 20), (21, 30)] {
        files.push(file(1, a, b, stamp_of(b)));
    }
    for n in 25..=30 {
        files.push(file(0, n, n, stamp_of(n)));
    }
    files
}

fn newest_stamp(files: &[FileInfo]) -> i64 {
    files
        .iter()
        .map(|f| f.created_at.unix_millis())
        .max()
        .unwrap()
}

/// One level, so `due` plans nothing but retention on `ladder`.
fn one_level() -> Maintenance {
    Maintenance::new(
        vec![secs(30)],
        secs(24 * 3600),
        secs(24 * 3600),
        L0Retention::After(secs(300)),
    )
    .unwrap()
}

fn run(files: Vec<FileInfo>) -> Retention {
    let now = newest_stamp(&files);
    retention(&inv(files), at(now), &defaults(), None)
}

fn held(files: Vec<FileInfo>) -> Hold {
    match run(files) {
        Retention::Held(hold) => hold,
        other @ (Retention::Delete(_) | Retention::Wait { .. }) => panic!("not held: {other:?}"),
    }
}

fn keys(r: &Retention) -> Vec<FileKey> {
    match r {
        Retention::Delete(job) => job.keys().to_vec(),
        other @ (Retention::Wait { .. } | Retention::Held(_)) => panic!("not a delete: {other:?}"),
    }
}

fn expected_ladder_deletes() -> Vec<FileKey> {
    let mut want: Vec<FileKey> = (25..=29).map(|n| key(0, n, n)).collect();
    want.extend([key(1, 1, 10), key(9, 1, 10), key(9, 1, 20)]);
    want
}

// The rules.

#[test]
fn the_ladder_deletes_old_snapshots_what_they_floor_and_covered_level_zero() {
    assert_eq!(keys(&run(ladder())), expected_ladder_deletes());
}

#[test]
fn an_empty_replica_has_nothing_to_delete_and_no_hold() {
    let got = retention(&Inventory::default(), at(T0), &defaults(), None);
    assert_eq!(got, Retention::Wait { until: None });
}

#[test]
fn without_a_snapshot_nothing_is_deleted() {
    let files: Vec<FileInfo> = ladder()
        .into_iter()
        .filter(|f| !f.key.level().is_snapshot())
        .collect();
    assert!(matches!(run(files), Retention::Wait { until: None }));
}

#[test]
fn the_last_file_of_every_level_stays_even_when_all_are_old() {
    // Everything below the floor, every level a single old file.
    let files = vec![
        file(9, 1, 5, T0 + HOUR),
        file(9, 1, 50, T0 + 100 * DAY),
        file(1, 1, 5, T0 + HOUR),
        file(2, 1, 5, T0 + HOUR),
        file(0, 50, 50, T0 + 100 * DAY),
    ];
    let got = keys(&run(files));
    assert_eq!(got, vec![key(9, 1, 5)]);
}

#[test]
fn a_level_one_file_at_the_floor_is_kept_below_it_goes() {
    let got = keys(&run(ladder()));
    assert!(got.contains(&key(1, 1, 10)));
    assert!(
        !got.contains(&key(1, 11, 20)),
        "max equal to the floor stays"
    );
}

#[test]
fn level_zero_files_the_levels_do_not_cover_stay() {
    // L1 covers 21 to 30 only up to 27.
    let mut files = ladder();
    files.retain(|f| f.key != key(1, 21, 30));
    files.push(file(1, 21, 27, stamp_of(27)));
    let got = keys(&run(files));
    assert!(got.contains(&key(0, 25, 25)) && got.contains(&key(0, 27, 27)));
    assert!(!got.contains(&key(0, 28, 28)), "{got:?}");
}

#[test]
fn level_zero_skips_an_uncovered_file_and_deletes_the_covered_ones_above_it() {
    // 27 is uncovered and stays; 28 and 29 are covered and go (review R5; the
    // scan used to stop at 27 and keep them).
    let mut files = ladder();
    files.retain(|f| f.key != key(1, 21, 30));
    files.push(file(1, 21, 26, stamp_of(26)));
    files.push(file(1, 28, 30, stamp_of(30)));
    let got = keys(&run(files));
    assert!(got.contains(&key(0, 26, 26)));
    assert!(!got.contains(&key(0, 27, 27)));
    assert!(got.contains(&key(0, 28, 28)) && got.contains(&key(0, 29, 29)));
    assert!(!got.contains(&key(0, 30, 30)), "never the last file");
}

#[test]
fn a_clock_that_steps_back_deletes_less_and_one_that_returns_deletes_the_same() {
    // Two snapshots a day apart, retention a day. At the newest stamp the first
    // is old. A clock far in the past keeps it (less is deleted, never more),
    // and nothing is remembered: the clock's return plans the first job again.
    let files = vec![
        file(9, 1, 10, T0),
        file(9, 1, 20, T0 + DAY + HOUR),
        file(1, 1, 10, T0),
        file(1, 11, 20, T0 + DAY + HOUR),
    ];
    let cfg = Maintenance::new(
        vec![secs(30)],
        secs(24 * 3600),
        secs(24 * 3600),
        L0Retention::Keep,
    )
    .unwrap();
    let inventory = inv(files);
    let state = MaintainState::default();
    let newest = at(T0 + DAY + HOUR);
    let first = super::retention(&inventory, newest, &cfg, None, &state);
    assert!(keys(&first).contains(&key(9, 1, 10)), "{first:?}");
    let back = super::retention(&inventory, at(T0), &cfg, None, &state);
    assert!(matches!(back, Retention::Wait { .. }), "{back:?}");
    // A clock that jumped ahead and came back leaves no mark either.
    let ahead = super::retention(&inventory, at(T0 + 400 * DAY), &cfg, None, &state);
    assert_eq!(first, ahead, "the newest stamp caps a clock that is ahead");
    let again = super::retention(&inventory, newest, &cfg, None, &state);
    assert_eq!(first, again);
}

#[test]
fn level_zero_below_the_floor_goes_when_aged_though_nothing_covers_it() {
    // Left over from before L0 retention was switched on: L1 under it is
    // being deleted, and without this clause it would block every L0 delete.
    let mut files = ladder();
    files.push(file(0, 3, 3, stamp_of(3)));
    let got = keys(&run(files));
    assert!(got.contains(&key(0, 3, 3)));
}

#[test]
fn keep_never_deletes_level_zero() {
    let keep = Maintenance::new(
        vec![secs(30)],
        secs(24 * 3600),
        secs(24 * 3600),
        L0Retention::Keep,
    )
    .unwrap();
    let mut inventory = inv(ladder());
    let now = at(newest_stamp(inventory.files()));
    let mut deleted = BTreeSet::new();
    while let Retention::Delete(job) = retention(&inventory, now, &keep, None) {
        deleted.extend(job.keys().iter().copied());
        inventory = inv(inventory
            .files()
            .iter()
            .filter(|f| !job.keys().contains(&f.key))
            .copied()
            .collect::<Vec<_>>());
    }
    assert!(!deleted.is_empty());
    assert!(deleted.iter().all(|k| k.level() != Level::ZERO));
    assert_eq!(inventory.level(Level::ZERO).len(), 6);
}

/// Go's test, ported: the plan of an in-flight restore must keep its files.
/// `TestStore_EnforceSnapshotRetention_RetainsInFlightRestorePlanFiles`
/// (`db_test.go:851`): a snapshot 1-5 and L1 6-10 two hours old, then a
/// fresh snapshot 1-11 and L1 11-11, one hour of retention.
#[test]
fn the_files_after_the_previous_snapshot_survive() {
    let hour = secs(3600);
    let cfg = Maintenance::new(vec![secs(1800)], hour, hour, L0Retention::Keep).unwrap();
    let old = T0;
    let now = T0 + 2 * HOUR;
    let files = vec![
        file(9, 1, 5, old),
        file(1, 6, 10, old + 60_000),
        file(9, 1, 11, now),
        file(1, 11, 11, now),
    ];
    let got = retention(&inv(files), at(now), &cfg, None);
    assert_eq!(keys(&got), vec![key(9, 1, 5)], "L1 6-10 is not deleted");
}

/// Go deleted L0 file 6 after an L1 that did not cover it (it compares only
/// L1's max), leaving a hole behind the newest snapshot. That hole is lost
/// history, not a reason to hold.
fn go_hole() -> Vec<FileInfo> {
    let mut files = vec![
        file(9, 1, 10, stamp_of(10)),
        file(9, 1, 30, stamp_of(30)),
        file(1, 1, 5, stamp_of(5)),
        file(1, 11, 20, stamp_of(20)),
        file(1, 21, 30, stamp_of(30)),
    ];
    files.extend((21..=30).map(|n| file(0, n, n, stamp_of(n))));
    files
}

#[test]
fn a_hole_behind_the_newest_snapshot_does_not_hold() {
    let files = go_hole();
    let now = at(newest_stamp(&files));
    let before = inv(files);
    let Retention::Delete(job) = retention(&before, now, &defaults(), None) else {
        panic!("held on a hole behind the newest snapshot");
    };
    check_delete(&before, &job, now, &defaults(), true);
}

#[test]
fn a_hole_after_the_newest_snapshot_holds() {
    let mut files = vec![file(9, 1, 10, stamp_of(10)), file(9, 1, 20, stamp_of(20))];
    files.extend([(1, 10), (11, 20)].map(|(a, b)| file(1, a, b, stamp_of(b))));
    files.extend(
        (21..=30)
            .filter(|n| *n != 25)
            .map(|n| file(0, n, n, stamp_of(n))),
    );
    assert_eq!(
        held(files),
        Hold::Hole {
            after: txid(24),
            next: Some(txid(26))
        }
    );
}

// The trust gate, one variant each.

#[test]
fn foreign_names_hold() {
    let inventory = inv(ladder()).with_skipped(level(1), 2);
    let got = retention(&inventory, at(T0 + 200 * HOUR), &defaults(), None);
    assert_eq!(
        got,
        Retention::Held(Hold::ForeignNames {
            level: level(1),
            count: 2
        })
    );
}

#[test]
fn a_malformed_snapshot_holds() {
    let mut files = ladder();
    files.push(file(9, 5, 30, stamp_of(30)));
    assert_eq!(held(files), Hold::MalformedSnapshot { key: key(9, 5, 30) });
}

#[test]
fn a_listing_behind_what_was_published_holds() {
    let files = ladder();
    let now = at(newest_stamp(&files));
    let got = retention(&inv(files), now, &defaults(), Some(txid(31)));
    assert_eq!(
        got,
        Retention::Held(Hold::ListingBehind {
            listed: Some(txid(30)),
            published: txid(31)
        })
    );
    let empty = retention(&Inventory::default(), now, &defaults(), Some(txid(1)));
    assert_eq!(
        empty,
        Retention::Held(Hold::ListingBehind {
            listed: None,
            published: txid(1)
        })
    );
    let level = retention(&inv(ladder()), now, &defaults(), Some(txid(30)));
    assert!(
        matches!(level, Retention::Delete(_)),
        "published equal to newest is fine"
    );
}

#[test]
fn a_latest_that_does_not_plan_holds() {
    let files: Vec<FileInfo> = (5..=8).map(|n| file(0, n, n, stamp_of(n))).collect();
    assert_eq!(
        held(files),
        Hold::LatestUnplannable {
            outcome: PlanOutcome::NoSnapshot {
                oldest_snapshot: None,
                malformed_snapshots: vec![]
            }
        }
    );
}

#[test]
fn an_unstamped_snapshot_or_level_zero_file_holds_and_other_levels_do_not() {
    let mut l0 = ladder();
    l0.retain(|f| f.key != key(0, 26, 26));
    l0.push(file(0, 26, 26, 0));
    assert_eq!(
        held(l0),
        Hold::Unstamped {
            key: key(0, 26, 26)
        }
    );
    let mut snap = ladder();
    snap.retain(|f| f.key != key(9, 1, 10));
    snap.push(file(9, 1, 10, -5));
    assert_eq!(held(snap), Hold::Unstamped { key: key(9, 1, 10) });
    let mut l1 = ladder();
    l1.retain(|f| f.key != key(1, 1, 10));
    l1.push(file(1, 1, 10, 0));
    assert!(matches!(run(l1), Retention::Delete(_)));
}

#[test]
fn snapshots_whose_stamps_run_backwards_hold_with_a_text_about_stored_times() {
    let mut files = ladder();
    files.retain(|f| f.key != key(9, 1, 20));
    files.push(file(9, 1, 20, stamp_of(5)));
    let hold = held(files);
    assert_eq!(
        hold,
        Hold::SnapshotStampsBackwards {
            earlier: key(9, 1, 10),
            later: key(9, 1, 20)
        }
    );
    assert!(
        hold.to_string()
            .contains("stored times no longer follow the headers")
    );
}

#[test]
fn equal_snapshot_stamps_are_not_backwards() {
    let mut files = ladder();
    files.retain(|f| f.key != key(9, 1, 20));
    files.push(file(9, 1, 20, stamp_of(10)));
    assert!(matches!(run(files), Retention::Delete(_)));
}

/// Resets the test hook when dropped, so a failing assertion cannot leak it.
struct Broken;

impl Broken {
    fn on() -> Self {
        BREAK_L0_COVERAGE.set(true);
        Self
    }
}

impl Drop for Broken {
    fn drop(&mut self) {
        BREAK_L0_COVERAGE.set(false);
    }
}

#[test]
fn a_rule_that_breaks_the_newest_restore_is_caught_by_the_self_check() {
    // Level 0 only, uncovered: honest rules delete nothing.
    let mut files = vec![file(9, 1, 10, stamp_of(10))];
    files.extend((11..=14).map(|n| file(0, n, n, stamp_of(n))));
    assert!(matches!(run(files.clone()), Retention::Wait { .. }));
    let _hook = Broken::on();
    let got = run(files);
    assert!(
        matches!(
            got,
            Retention::Held(Hold::SelfCheck {
                outcome: PlanOutcome::Gap { .. }
            })
        ),
        "{got:?}"
    );
}

#[test]
fn the_gate_reports_the_first_failure_in_its_order() {
    let mut files = ladder();
    files.push(file(9, 5, 30, stamp_of(30)));
    let inventory = inv(files).with_skipped(level(0), 1);
    let got = retention(&inventory, at(T0 + 200 * HOUR), &defaults(), None);
    assert_eq!(
        got,
        Retention::Held(Hold::ForeignNames {
            level: level(0),
            count: 1
        })
    );
}

// Through `due`.

#[test]
fn due_hands_out_the_delete_job_and_then_waits_with_nothing_left() {
    let files = ladder();
    let now = at(newest_stamp(&files));
    let cfg = one_level();
    let mut inventory = inv(files);
    let Due::Run(Job::Delete(job)) = due(&inventory, now, &cfg, None) else {
        panic!("no delete");
    };
    assert_eq!(job.keys(), expected_ladder_deletes());
    inventory = inv(inventory
        .files()
        .iter()
        .filter(|f| !job.keys().contains(&f.key))
        .copied()
        .collect::<Vec<_>>());
    assert_eq!(
        due(&inventory, now, &cfg, None),
        Due::Wait {
            until: None,
            held: None
        }
    );
}

#[test]
fn a_hold_shows_in_the_wait_and_compaction_still_runs_while_held() {
    let cfg = one_level();
    let now = at(newest_stamp(&ladder()));
    let held_inv = inv(ladder()).with_skipped(level(2), 1);
    assert_eq!(
        due(&held_inv, now, &cfg, None),
        Due::Wait {
            until: None,
            held: Some(Hold::ForeignNames {
                level: level(2),
                count: 1
            })
        }
    );
    let mut files = ladder();
    files.push(file(0, 31, 31, stamp_of(30)));
    let behind = inv(files).with_skipped(level(2), 1);
    let got = due(&behind, now, &cfg, None);
    assert!(matches!(got, Due::Run(Job::Compact(_))), "{got:?}");
}

#[test]
fn a_ready_snapshot_goes_before_any_delete() {
    let mut files = ladder();
    files.push(file(0, 31, 31, stamp_of(31)));
    files.push(file(1, 31, 31, stamp_of(31)));
    let now = at(stamp_of(31));
    let got = due(&inv(files), now, &one_level(), None);
    assert!(matches!(got, Due::Run(Job::Snapshot(_))), "{got:?}");
}

#[test]
fn the_wake_times_are_when_the_next_file_ages_out() {
    let cfg = one_level();
    let files = ladder();
    // 60 h after T0: the first snapshot (stamped 40 h) is inside retention.
    let before = at(T0 + 60 * HOUR);
    let inventory = inv(files);
    let wake = T0 + 40 * HOUR + DAY + 1;
    assert_eq!(
        due(&inventory, before, &cfg, None),
        Due::Wait {
            until: Some(at(wake)),
            held: None
        }
    );
    let Due::Run(Job::Delete(job)) = due(&inventory, at(wake), &cfg, None) else {
        panic!("nothing at the wake time");
    };
    assert_eq!(job.keys(), [key(9, 1, 10)]);
    let Due::Wait { until, .. } = due(&inventory, at(wake - 1), &cfg, None) else {
        panic!("a delete a millisecond early");
    };
    assert_eq!(until, Some(at(wake)));
}

#[test]
fn a_covered_level_zero_file_wakes_when_it_is_old_enough() {
    let cfg = one_level();
    let files = vec![
        file(9, 1, 30, stamp_of(30)),
        file(1, 21, 30, stamp_of(30)),
        file(0, 25, 25, stamp_of(25)),
        file(0, 30, 30, stamp_of(30)),
    ];
    let inventory = inv(files);
    let wake = stamp_of(25) + 300_000;
    let early = at(stamp_of(25) + 60_000);
    assert_eq!(
        due(&inventory, early, &cfg, None),
        Due::Wait {
            until: Some(at(wake)),
            held: None
        }
    );
    let Due::Run(Job::Delete(job)) = due(&inventory, at(wake), &cfg, None) else {
        panic!("no delete when the file is old enough");
    };
    assert_eq!(job.keys(), [key(0, 25, 25)]);
}

#[test]
fn a_clock_far_ahead_deletes_what_the_newest_stamp_deletes_and_one_far_behind_nothing() {
    let files = ladder();
    let newest = at(newest_stamp(&files));
    let inventory = inv(files);
    let at_newest = retention(&inventory, newest, &defaults(), None);
    assert_eq!(
        retention(&inventory, at(i64::MAX), &defaults(), None),
        at_newest
    );
    let behind = retention(&inventory, at(i64::MIN / 2), &defaults(), None);
    assert!(!matches!(behind, Retention::Delete(_)), "{behind:?}");
}

#[test]
fn two_thousand_five_hundred_deletes_go_in_three_jobs_snapshots_last() {
    let mut files = vec![file(9, 1, 2500, T0), file(9, 1, 2600, T0 + 5 * DAY)];
    files.extend((1..=2600u64).map(|n| file(1, n, n, T0 + 5 * DAY)));
    let cfg = defaults();
    let now = at(T0 + 5 * DAY);
    let mut inventory = inv(files);
    let mut sizes = Vec::new();
    let mut last_job: Vec<FileKey> = Vec::new();
    while let Retention::Delete(job) = retention(&inventory, now, &cfg, None) {
        assert!(job.keys().is_sorted());
        if let Some(prev) = last_job.last() {
            assert!(prev < &job.keys()[0], "jobs continue in key order");
        }
        sizes.push(job.keys().len());
        last_job = job.keys().to_vec();
        inventory = inv(inventory
            .files()
            .iter()
            .filter(|f| !job.keys().contains(&f.key))
            .copied()
            .collect::<Vec<_>>());
    }
    assert_eq!(sizes, [1000, 1000, 500]);
    assert_eq!(
        last_job.last(),
        Some(&key(9, 1, 2500)),
        "the snapshot goes last"
    );
    assert_eq!(inventory.level(Level::SNAPSHOT).len(), 1);
}

// The invariants.

/// The files of a level-0-to-8 chain from `start`: how far they reach.
fn reaches_from(inventory: &Inventory, start: Txid) -> Txid {
    let mut reach = start;
    loop {
        let best = inventory
            .files()
            .iter()
            .filter(|f| {
                !f.key.level().is_snapshot()
                    && reach
                        .next()
                        .is_none_or(|one_past| f.key.range().min() <= one_past)
            })
            .map(|f| f.key.range().max())
            .max();
        match best {
            Some(best) if best > reach => reach = best,
            _ => return reach,
        }
    }
}

fn without(inventory: &Inventory, keys: &[FileKey]) -> Inventory {
    inv(inventory
        .files()
        .iter()
        .filter(|f| keys.binary_search(&f.key).is_err())
        .copied()
        .collect::<Vec<_>>())
}

/// The snapshots and the first retained one, recomputed from the rules'
/// statement rather than from the code under test.
fn retained_from(before: &Inventory, now: Timestamp, cfg: &Maintenance) -> Option<(Txid, Txid)> {
    let snaps: Vec<&FileInfo> = before
        .level(Level::SNAPSHOT)
        .iter()
        .filter(|f| is_snapshot_start(f))
        .collect();
    let newest = before.files().iter().map(|f| f.created_at).max()?;
    let anchor = now.min(newest).unix_millis();
    let cutoff =
        anchor.saturating_sub(i64::try_from(cfg.snapshot_retention().as_millis()).unwrap());
    let first = snaps
        .iter()
        .position(|s| s.created_at.unix_millis() >= cutoff)
        .unwrap_or(snaps.len().checked_sub(1)?);
    let floor = first
        .checked_sub(1)
        .map_or(Txid::MIN, |p| snaps[p].key.range().max());
    Some((snaps[first].key.range().max(), floor))
}

fn restores(outcome: &PlanOutcome, t: Txid) -> bool {
    matches!(outcome, PlanOutcome::Plan(chain) if chain.reaches() == t)
}

/// A kept file of level 1 to 8 that holds `t` strictly inside and whose own
/// end restores: the documented loss to compaction granularity.
fn inside_a_kept_file(after: &Inventory, t: Txid) -> bool {
    after.files().iter().any(|g| {
        let range = g.key.range();
        (1..=8).contains(&g.key.level().get())
            && range.min() <= t
            && t < range.max()
            && restores(&plan(after, RestoreTarget::Txid(range.max())), range.max())
    })
}

/// Invariants 1 to 4 for one delete job. `consistent`: stamps follow the
/// transactions, so instants can be checked too.
fn check_delete(
    before: &Inventory,
    job: &DeleteJob,
    now: Timestamp,
    cfg: &Maintenance,
    consistent: bool,
) {
    let newest = before.newest().unwrap();
    let after = without(before, job.keys());
    // 1
    assert!(
        restores(&plan(&after, RestoreTarget::Latest), newest),
        "latest after {job:?}"
    );
    // 2
    let (lo, floor_snapshot) = retained_from(before, now, cfg).unwrap();
    for raw in lo.get()..=newest.get() {
        let t = Txid::new(raw).unwrap();
        if restores(&plan(before, RestoreTarget::Txid(t)), t) {
            let now_plan = plan(&after, RestoreTarget::Txid(t));
            assert!(
                restores(&now_plan, t) || inside_a_kept_file(&after, t),
                "{t} no longer restores after {job:?}: {now_plan:?}"
            );
        }
    }
    if consistent {
        check_instants(before, &after, lo);
    }
    // 3
    for snap in after
        .level(Level::SNAPSHOT)
        .iter()
        .filter(|s| is_snapshot_start(s))
    {
        let max = snap.key.range().max();
        if max >= floor_snapshot && reaches_from(before, max) == newest {
            assert_eq!(
                reaches_from(&after, max),
                newest,
                "the chain of {} is cut",
                snap.key
            );
        }
    }
    // 4
    for l in Level::ALL {
        let was = before.level(l);
        let is = after.level(l);
        assert_eq!(is.is_empty(), was.is_empty(), "{l} emptied");
        let gone = was.len() - is.len();
        // Level 0 is judged file by file (review R5): the covered, aged files
        // go and an uncovered one in between stays, so only 1 to 8 are prefixes.
        if l != Level::ZERO {
            assert_eq!(&was[gone..], is, "{l} did not lose a prefix");
        }
    }
}

/// The same for `AsOf`: at each kept file's stamp plus a millisecond, an
/// instant that planned still plans to the same state, or says the instant is
/// inside a compacted file.
fn check_instants(before: &Inventory, after: &Inventory, lo: Txid) {
    let base_stamp = before
        .level(Level::SNAPSHOT)
        .iter()
        .find(|s| s.key.range().max() == lo && is_snapshot_start(s))
        .map(|s| s.created_at.unix_millis());
    for f in after.files() {
        let instant = at(f.created_at.unix_millis().saturating_add(1));
        let Some(base) = base_stamp else { continue };
        if f.created_at.unix_millis() < base {
            continue;
        }
        let target = RestoreTarget::AsOf(instant);
        if let PlanOutcome::Plan(was) = plan(before, target) {
            let wanted = was.reaches();
            let now = plan(after, target);
            assert!(
                restores(&now, wanted)
                    || matches!(now, PlanOutcome::AsOfInsideFile { .. })
                    || inside_a_kept_file(after, wanted),
                "instant {instant} lost: {now:?}"
            );
        }
    }
}

struct Bytes<'a> {
    data: &'a [u8],
    at: usize,
}

impl Bytes<'_> {
    fn next(&mut self) -> u64 {
        let b = self
            .data
            .get(self.at % self.data.len().max(1))
            .copied()
            .unwrap_or(0);
        self.at += 1;
        u64::from(b)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn percent(&mut self, p: u64) -> bool {
        self.below(100) < p
    }

    fn index(&mut self, n: usize) -> usize {
        usize::try_from(self.below(u64::try_from(n).unwrap())).unwrap()
    }
}

/// A generated listing and how it was made.
struct Listing {
    files: Vec<FileInfo>,
    skipped: Option<(Level, usize)>,
    published: Option<Txid>,
    /// No overlaps, no holes after the last snapshot, stamps follow the
    /// transactions: the shape a Rust or Go writer leaves.
    sane: bool,
}

/// Prefix-contiguous chunks of `from..=to` with fan-in 1 to `most`.
fn chunks(b: &mut Bytes, from: u64, to: u64, most: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut at = from;
    while at <= to {
        let end = (at + b.below(most)).min(to);
        out.push((at, end));
        at = end + 1;
    }
    out
}

fn build(data: &[u8], weird: bool) -> Listing {
    let mut b = Bytes { data, at: 0 };
    let n = 4 + b.below(36);
    let gaps = [1_000, 60_000, HOUR, 4 * HOUR, DAY];
    let mut stamps = vec![0i64];
    let mut t = T0;
    for _ in 1..=n {
        t += gaps[b.index(gaps.len())];
        stamps.push(t);
    }
    let stamp = |i: u64| stamps[usize::try_from(i).unwrap()];
    let mut files = Vec::new();
    let snaps: BTreeSet<u64> = if b.percent(8) {
        BTreeSet::new()
    } else {
        (0..=b.below(4)).map(|_| 2 + b.below(n - 1)).collect()
    };
    let last_snap = snaps.last().copied().unwrap_or(0);
    files.extend(snaps.iter().map(|m| file(9, 1, *m, stamp(*m))));
    for i in 1..=n {
        if i > last_snap || b.percent(70) {
            files.push(file(0, i, i, stamp(i)));
        }
    }
    let covered_to = if b.percent(15) { 0 } else { 1 + b.below(n) };
    let l1 = chunks(&mut b, 1, covered_to, 6);
    files.extend(l1.iter().map(|(a, z)| file(1, *a, *z, stamp(*z))));
    let l2_count = b.index(l1.len() + 1);
    let mut at = 0;
    while at < l2_count {
        let end = (at + b.index(3)).min(l2_count - 1);
        files.push(file(2, l1[at].0, l1[end].1, stamp(l1[end].1)));
        at = end + 1;
    }
    let mut listing = Listing {
        files,
        skipped: None,
        published: None,
        sane: !weird,
    };
    if weird {
        roughen(&mut b, &mut listing, n, &stamp);
    }
    listing
}

/// Holes, overlaps, backwards or zero stamps, foreign names, a listing behind.
fn roughen(b: &mut Bytes, listing: &mut Listing, n: u64, stamp: &dyn Fn(u64) -> i64) {
    for _ in 0..=b.below(3) {
        match b.below(7) {
            0 if !listing.files.is_empty() => {
                let at = b.index(listing.files.len());
                listing.files.remove(at);
            }
            1 => {
                let lo = 1 + b.below(n);
                let hi = lo + b.below(n - lo + 1);
                let l = u8::try_from(1 + b.below(3)).unwrap();
                listing.files.push(file(l, lo, hi, stamp(hi)));
            }
            2 if !listing.files.is_empty() => {
                let at = b.index(listing.files.len());
                let f = &mut listing.files[at];
                f.created_at = at_stamp(stamp(1 + b.below(n)));
            }
            3 if !listing.files.is_empty() => {
                let at = b.index(listing.files.len());
                listing.files[at].created_at = Timestamp::from_unix_millis(0);
            }
            4 => {
                let lo = 2 + b.below(n - 1);
                listing.files.push(file(9, lo, n, stamp(n)));
            }
            5 => listing.skipped = Some((level(u8::try_from(b.below(10)).unwrap()), 1)),
            6 => listing.published = Txid::new(n + 1 + b.below(2)),
            _ => {}
        }
    }
}

fn at_stamp(ms: i64) -> Timestamp {
    Timestamp::from_unix_millis(ms)
}

fn configs() -> [Maintenance; 4] {
    let wide = Maintenance::new(
        vec![secs(60), secs(3600)],
        secs(7 * 86_400),
        secs(7 * 86_400),
        L0Retention::After(secs(3600)),
    )
    .unwrap();
    let aging = Maintenance::new(
        vec![secs(10), secs(60), secs(600)],
        secs(3600),
        secs(3600),
        L0Retention::After(secs(120)),
    )
    .unwrap();
    [defaults(), small(), wide, aging]
}

fn pick_now(b: &mut Bytes, files: &[FileInfo]) -> i64 {
    let newest = newest_stamp(files);
    match b.below(5) {
        0 => newest,
        1 => newest + DAY,
        2 => i64::MAX,
        3 => newest - i64::try_from(b.below(120)).unwrap() * HOUR,
        _ => files[b.index(files.len())].created_at.unix_millis() + 1,
    }
}

/// Invariants 5 (as far as a property reaches it), 6, 7 and 8 and the wake
/// time, for one listing; 1 to 4 through `check_delete` when it deletes.
fn check_listing(
    inventory: &Inventory,
    now: Timestamp,
    cfg: &Maintenance,
    published: Option<Txid>,
    sane: bool,
    noise: &mut Bytes,
) {
    let first = retention(inventory, now, cfg, published);
    assert_eq!(
        first,
        retention(inventory, now, cfg, published),
        "not deterministic"
    );
    let Some(newest) = inventory.files().iter().map(|f| f.created_at).max() else {
        return;
    };
    // 6
    assert_eq!(
        retention(inventory, at(i64::MAX), cfg, published),
        retention(inventory, newest, cfg, published),
        "a clock far ahead"
    );
    let behind = retention(inventory, at(i64::MIN / 2), cfg, published);
    assert!(
        !matches!(behind, Retention::Delete(_)),
        "a clock far behind: {behind:?}"
    );
    match &first {
        Retention::Delete(job) => {
            check_delete(inventory, job, now, cfg, sane);
            if sane {
                omission(inventory, now, cfg, job.keys(), noise);
            }
        }
        Retention::Wait { until } => wake(inventory, cfg, published, *until, now),
        Retention::Held(_) => {}
    }
    if !matches!(first, Retention::Held(_)) {
        later_stamps(inventory, now, cfg, published, &first, noise);
    }
}

fn wake(
    inv: &Inventory,
    cfg: &Maintenance,
    published: Option<Txid>,
    until: Option<Timestamp>,
    now: Timestamp,
) {
    match until {
        Some(wake) => {
            let at_wake = retention(inv, wake, cfg, published);
            assert!(
                matches!(at_wake, Retention::Delete(_)),
                "nothing at the wake time: {at_wake:?}"
            );
            if wake > now {
                let before = retention(inv, at(wake.unix_millis() - 1), cfg, published);
                assert!(
                    !matches!(before, Retention::Delete(_)),
                    "a delete before the wake time"
                );
            }
        }
        None => {
            let ever = retention(inv, at(i64::MAX), cfg, published);
            assert!(
                !matches!(ever, Retention::Delete(_)),
                "idle, yet {ever:?} at the end of time"
            );
        }
    }
}

/// 7: a listing missing files never deletes a key the full one keeps.
fn omission(
    inventory: &Inventory,
    now: Timestamp,
    cfg: &Maintenance,
    full: &[FileKey],
    noise: &mut Bytes,
) {
    for _ in 0..3 {
        let chance = 10 + noise.below(60);
        let kept: Vec<FileInfo> = inventory
            .files()
            .iter()
            .filter(|_| !noise.percent(chance))
            .copied()
            .collect();
        if let Retention::Delete(job) = retention(&inv(kept), now, cfg, None) {
            for k in job.keys() {
                assert!(
                    full.contains(k),
                    "a listing without some files deletes {k}, the full one keeps it"
                );
            }
        }
    }
}

/// 8: stamps rewritten later (all older ones to one instant, then everything
/// by a constant) delete nothing that the true stamps keep. Stretching the
/// gaps between stamps is a different fault and a non-goal.
fn later_stamps(
    inventory: &Inventory,
    now: Timestamp,
    cfg: &Maintenance,
    published: Option<Txid>,
    truth: &Retention,
    noise: &mut Bytes,
) {
    let files = inventory.files();
    let floor = files[noise.index(files.len())].created_at.unix_millis();
    let shift = i64::try_from(noise.below(40)).unwrap() * HOUR;
    let moved: Vec<FileInfo> = files
        .iter()
        .map(|f| FileInfo {
            created_at: at(f.created_at.unix_millis().max(floor) + shift),
            ..*f
        })
        .collect();
    let allowed: Vec<FileKey> = match truth {
        Retention::Delete(job) => job.keys().to_vec(),
        Retention::Wait { .. } | Retention::Held(_) => Vec::new(),
    };
    if let Retention::Delete(job) = retention(&inv(moved), now, cfg, published) {
        for k in job.keys() {
            assert!(
                allowed.contains(k),
                "later stamps delete {k}, the true stamps keep it"
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    #[test]
    fn the_rules_hold_over_any_listing(
        data in prop::collection::vec(any::<u8>(), 96..400),
        weird in any::<bool>(),
        which in 0usize..4,
    ) {
        let listing = build(&data, weird);
        if listing.files.is_empty() {
            return Ok(());
        }
        let mut inventory = inv(listing.files.clone());
        if let Some((l, count)) = listing.skipped {
            inventory = inventory.with_skipped(l, count);
        }
        let mut b = Bytes { data: &data, at: 7 };
        let now = at(pick_now(&mut b, inventory.files()));
        let cfg = &configs()[which];
        check_listing(&inventory, now, cfg, listing.published, listing.sane, &mut b);
    }
}

/// Write histories run through `due`, every delete job checked.
fn history() -> impl Strategy<Value = (usize, Vec<(u8, u8)>)> {
    (0..2usize, prop::collection::vec((0u8..10, 0u8..5), 0..70))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn retention_holds_through_any_write_history((which, events) in history()) {
        let cfg = if which == 0 { configs()[1].clone() } else { configs()[3].clone() };
        let gaps = [1_000, 60_000, HOUR, 4 * HOUR, DAY];
        let mut model = Model::default();
        let (mut now, mut next) = (T0, 1u64);
        let mut noise = Bytes { data: &[3, 141, 59, 26, 53, 58, 97, 93], at: 0 };
        for (kind, gap) in events {
            now += gaps[usize::from(gap)];
            if kind < 6 {
                model.files.push(file(0, next, next, now));
                next += 1;
            } else {
                drive_checked(&mut model, now, &cfg, &mut noise);
            }
        }
        drive_checked(&mut model, now, &cfg, &mut noise);
    }
}

/// Runs `due` to a wait, checking every delete and the waiting listing.
fn drive_checked(model: &mut Model, now: i64, cfg: &Maintenance, noise: &mut Bytes) {
    for _ in 0..2000 {
        let before = model.inv();
        match due(&before, at(now), cfg, None) {
            Due::Run(Job::Delete(job)) => {
                check_delete(&before, &job, at(now), cfg, true);
                model.apply(&Job::Delete(job), now);
            }
            Due::Run(job) => model.apply(&job, now),
            Due::Wait { .. } => {
                check_listing(&before, at(now), cfg, None, true, noise);
                return;
            }
        }
    }
    panic!("due never settled");
}
