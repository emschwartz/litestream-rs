//! Retention tests: the trust gate one variant at a time, the rules on
//! hand-built ladders, Go's in-flight restore case, and the invariants as
//! properties over raw listings and over write histories driven through
//! `due` with a model executor that applies delete jobs.

use std::collections::BTreeSet;

use lsr_ltx::{Timestamp, Txid};
use proptest::prelude::*;

use super::*;
use crate::created_at::{Resolve, ResolvedCreatedAt, resolve};
use crate::inventory::Inventory;
use crate::layout::ListedTime;
use crate::maintain::tests::{
    Model, T0, at, defaults, due, file, inv, key, level, secs, small, txid,
};
use crate::maintain::{Due, Job};
use crate::mem::MemStore;
use crate::plan::RestoreTarget;
use crate::store::{ObjectStore, Upload};

/// `retention` from a fresh state.
fn set_created_at(file: &mut FileInfo, at: Timestamp) {
    file.listed = ListedTime::Created(CreatedAt::mtime(at));
}

fn retention(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
) -> Retention {
    retention_state(inventory, now, config, published, &MaintainState::default())
}

/// `super::retention` on a listing that carries every created-at time.
fn retention_state(
    inventory: &Inventory,
    now: Timestamp,
    config: &Maintenance,
    published: Option<Txid>,
    state: &MaintainState,
) -> Retention {
    match super::retention(
        inventory,
        now,
        config,
        published,
        state,
        &ResolvedCreatedAt::default(),
    ) {
        Resolve::Done(retention) => retention,
        Resolve::Need(file) => panic!("a stored listing asked for a created-at time: {file:?}"),
    }
}

const HOUR: i64 = 3_600_000;
const DAY: i64 = 24 * HOUR;

/// One transaction every four hours.
fn created_at_of(n: u64) -> i64 {
    T0 + i64::try_from(n).unwrap() * 4 * HOUR
}

/// Snapshots at 10, 20 and 30, L1 over each tenth, L0 for 25 to 30: the
/// newest created-at time is 120 h after T0, so with 24 h retention the first two
/// snapshots are old and the floor is 20.
fn ladder() -> Vec<FileInfo> {
    let mut files = Vec::new();
    for n in [10, 20, 30] {
        files.push(file(9, 1, n, created_at_of(n)));
    }
    for (a, b) in [(1, 10), (11, 20), (21, 30)] {
        files.push(file(1, a, b, created_at_of(b)));
    }
    for n in 25..=30 {
        files.push(file(0, n, n, created_at_of(n)));
    }
    files
}

fn newest_created_at(files: &[FileInfo]) -> i64 {
    files
        .iter()
        .map(|f| f.created_at().unwrap().at().unix_millis())
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
    let now = newest_created_at(&files);
    retention(&inv(files), at(now), &defaults(), None)
}

fn held(files: Vec<FileInfo>) -> RetentionHold {
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

fn pinned(restore_point: u64) -> MaintainState {
    let mut state = MaintainState::default();
    state.set_kept_restore_point(Some(txid(restore_point)));
    state
}

fn pinned_run(files: Vec<FileInfo>, restore_point: u64) -> Retention {
    let now = newest_created_at(&files);
    retention_state(
        &inv(files),
        at(now),
        &defaults(),
        None,
        &pinned(restore_point),
    )
}

/// A degraded task pins a restore point: its chain stays, and everything
/// else the rules delete still goes (the kept restore point never freezes retention).
#[test]
fn a_kept_restore_point_keeps_its_chain_and_the_rest_is_retained_as_usual() {
    for restore_point in [10, 20] {
        let got = keys(&pinned_run(ladder(), restore_point));
        let kept = key(9, 1, restore_point);
        let want: Vec<FileKey> = expected_ladder_deletes()
            .into_iter()
            .filter(|k| *k != kept)
            .collect();
        assert_eq!(got, want, "kept restore point {restore_point}");
        let rest = without(&inv(ladder()), &got);
        let restores = plan(&rest, KeyTarget::Txid(txid(restore_point)));
        assert!(
            matches!(&restores, PlanOutcome::Plan(chain) if chain.reaches() == txid(restore_point)),
            "kept restore point {restore_point}: {restores:?}"
        );
    }
}

/// A pinned transaction the listing cannot restore holds every delete: the
/// kept restore point cannot tell which files a later listing would need for it.
#[test]
fn a_kept_restore_point_that_does_not_plan_holds() {
    let got = pinned_run(ladder(), 27);
    assert!(
        matches!(
            &got,
            Retention::Held(RetentionHold::RestorePointUnplannable { restore_point, .. }) if *restore_point == txid(27)
        ),
        "{got:?}"
    );
}

/// For every transaction of the ladder as the kept restore point: either retention holds, or
/// the kept restore point still restores without the job and the newest state does too.
#[test]
fn no_kept_restore_point_is_ever_deleted_from_under_its_restore() {
    for restore_point in 1..=30 {
        match pinned_run(ladder(), restore_point) {
            Retention::Held(RetentionHold::RestorePointUnplannable { .. })
            | Retention::Wait { .. } => {}
            Retention::Delete(job) => {
                let rest = without(&inv(ladder()), job.keys());
                let at_restore_point = plan(&rest, KeyTarget::Txid(txid(restore_point)));
                assert!(
                    matches!(&at_restore_point, PlanOutcome::Plan(c) if c.reaches() == txid(restore_point)),
                    "kept restore point {restore_point}: {at_restore_point:?}"
                );
                let latest = plan(&rest, KeyTarget::Latest);
                assert!(
                    matches!(&latest, PlanOutcome::Plan(c) if c.reaches() == txid(30)),
                    "kept restore point {restore_point}: {latest:?}"
                );
            }
            other @ Retention::Held(_) => {
                panic!("kept restore point {restore_point}: {other:?}")
            }
        }
    }
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
    files.push(file(1, 21, 27, created_at_of(27)));
    let got = keys(&run(files));
    assert!(got.contains(&key(0, 25, 25)) && got.contains(&key(0, 27, 27)));
    assert!(!got.contains(&key(0, 28, 28)), "{got:?}");
}

#[test]
fn level_zero_skips_an_uncovered_file_and_deletes_the_covered_ones_above_it() {
    // 27 is uncovered and stays; 28 and 29 are covered and go (the
    // scan used to stop at 27 and keep them).
    let mut files = ladder();
    files.retain(|f| f.key != key(1, 21, 30));
    files.push(file(1, 21, 26, created_at_of(26)));
    files.push(file(1, 28, 30, created_at_of(30)));
    let got = keys(&run(files));
    assert!(got.contains(&key(0, 26, 26)));
    assert!(!got.contains(&key(0, 27, 27)));
    assert!(got.contains(&key(0, 28, 28)) && got.contains(&key(0, 29, 29)));
    assert!(!got.contains(&key(0, 30, 30)), "never the last file");
}

#[test]
fn a_clock_that_steps_back_deletes_less_and_one_that_returns_deletes_the_same() {
    // Two snapshots a day apart, retention a day. At the newest created-at time the first
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
    let first = retention_state(&inventory, newest, &cfg, None, &state);
    assert!(keys(&first).contains(&key(9, 1, 10)), "{first:?}");
    let back = retention_state(&inventory, at(T0), &cfg, None, &state);
    assert!(matches!(back, Retention::Wait { .. }), "{back:?}");
    // A clock that jumped ahead and came back leaves no mark either.
    let ahead = retention_state(&inventory, at(T0 + 400 * DAY), &cfg, None, &state);
    assert_eq!(
        first, ahead,
        "the newest created-at time caps a clock that is ahead"
    );
    let again = retention_state(&inventory, newest, &cfg, None, &state);
    assert_eq!(first, again);
}

#[test]
fn level_zero_below_the_floor_goes_when_aged_though_nothing_covers_it() {
    // Left over from before L0 retention was switched on: L1 under it is
    // being deleted, and without this clause it would block every L0 delete.
    let mut files = ladder();
    files.push(file(0, 3, 3, created_at_of(3)));
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
    let now = at(newest_created_at(inventory.files()));
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
        file(9, 1, 10, created_at_of(10)),
        file(9, 1, 30, created_at_of(30)),
        file(1, 1, 5, created_at_of(5)),
        file(1, 11, 20, created_at_of(20)),
        file(1, 21, 30, created_at_of(30)),
    ];
    files.extend((21..=30).map(|n| file(0, n, n, created_at_of(n))));
    files
}

#[test]
fn a_hole_behind_the_newest_snapshot_does_not_hold() {
    let files = go_hole();
    let now = at(newest_created_at(&files));
    let before = inv(files);
    let Retention::Delete(job) = retention(&before, now, &defaults(), None) else {
        panic!("held on a hole behind the newest snapshot");
    };
    check_delete(&before, &job, now, &defaults(), true);
}

#[test]
fn a_hole_after_the_newest_snapshot_holds() {
    let mut files = vec![
        file(9, 1, 10, created_at_of(10)),
        file(9, 1, 20, created_at_of(20)),
    ];
    files.extend([(1, 10), (11, 20)].map(|(a, b)| file(1, a, b, created_at_of(b))));
    files.extend(
        (21..=30)
            .filter(|n| *n != 25)
            .map(|n| file(0, n, n, created_at_of(n))),
    );
    assert_eq!(
        held(files),
        RetentionHold::Hole {
            after: txid(24),
            next: Some(txid(26))
        }
    );
}

// The trust gate, one variant each.

#[test]
fn unrecognized_names_hold() {
    let inventory = inv(ladder()).with_skipped(level(1), 2);
    let got = retention(&inventory, at(T0 + 200 * HOUR), &defaults(), None);
    assert_eq!(
        got,
        Retention::Held(RetentionHold::UnrecognizedNames {
            level: level(1),
            count: 2
        })
    );
}

#[test]
fn a_malformed_snapshot_holds() {
    let mut files = ladder();
    files.push(file(9, 5, 30, created_at_of(30)));
    assert_eq!(
        held(files),
        RetentionHold::MalformedSnapshot { key: key(9, 5, 30) }
    );
}

#[test]
fn a_listing_behind_what_was_published_holds() {
    let files = ladder();
    let now = at(newest_created_at(&files));
    let got = retention(&inv(files), now, &defaults(), Some(txid(31)));
    assert_eq!(
        got,
        Retention::Held(RetentionHold::ListingBehind {
            listed: Some(txid(30)),
            published: txid(31)
        })
    );
    let empty = retention(&Inventory::default(), now, &defaults(), Some(txid(1)));
    assert_eq!(
        empty,
        Retention::Held(RetentionHold::ListingBehind {
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
    let files: Vec<FileInfo> = (5..=8).map(|n| file(0, n, n, created_at_of(n))).collect();
    assert_eq!(
        held(files),
        RetentionHold::LatestUnplannable {
            outcome: PlanOutcome::NoSnapshot {
                oldest_snapshot: None,
                malformed_snapshots: vec![]
            }
        }
    );
}

#[test]
fn a_snapshot_or_level_zero_file_without_created_at_holds_and_other_levels_do_not() {
    let mut l0 = ladder();
    l0.retain(|f| f.key != key(0, 26, 26));
    l0.push(file(0, 26, 26, 0));
    assert_eq!(
        held(l0),
        RetentionHold::NoCreatedAt {
            key: key(0, 26, 26)
        }
    );
    let mut snap = ladder();
    snap.retain(|f| f.key != key(9, 1, 10));
    snap.push(file(9, 1, 10, -5));
    assert_eq!(
        held(snap),
        RetentionHold::NoCreatedAt { key: key(9, 1, 10) }
    );
    let mut l1 = ladder();
    l1.retain(|f| f.key != key(1, 1, 10));
    l1.push(file(1, 1, 10, 0));
    assert!(matches!(run(l1), Retention::Delete(_)));
}

/// A level-0 file that ends at the floor is not wholly below it: with no kept
/// level-1 file covering it, it stays.
#[test]
fn a_level_zero_file_ending_at_the_floor_is_not_below_it() {
    let mut files = ladder();
    files.retain(|f| f.key != key(1, 11, 20));
    files.push(file(1, 11, 19, created_at_of(19)));
    files.push(file(0, 20, 20, created_at_of(20)));
    let deleted = keys(&run(files));
    assert!(deleted.contains(&key(1, 11, 19)), "{deleted:?}");
    assert!(!deleted.contains(&key(0, 20, 20)), "{deleted:?}");
}

/// The same rule for the files that set the anchor: the last file of each
/// level. An without a created-at time last level-0 file holds; an without a created-at time last level-1
/// file does not.
#[test]
fn a_last_file_without_created_at_holds_only_at_level_zero_or_nine() {
    let mut l0 = ladder();
    l0.retain(|f| f.key != key(0, 30, 30));
    l0.push(file(0, 30, 30, 0));
    assert_eq!(
        held(l0),
        RetentionHold::NoCreatedAt {
            key: key(0, 30, 30)
        }
    );
    let mut l1 = ladder();
    l1.retain(|f| f.key != key(1, 21, 30));
    l1.push(file(1, 21, 30, 0));
    assert!(matches!(run(l1), Retention::Delete(_)));
}

/// A created-at time at or before the epoch is a writer that recorded no created-at time. Read as
/// an instant it would be a file from 1970, older than any retention, and go
/// first; it is a hold instead, however far ahead the clock is.
#[test]
fn a_file_with_a_zero_created_at_is_never_aged_as_from_1970() {
    for unset in [0, -1, i64::MIN] {
        let mut files = ladder();
        files.retain(|f| f.key != key(0, 25, 25));
        files.push(file(0, 25, 25, unset));
        match retention(&inv(files), Timestamp::MAX, &one_level(), None) {
            Retention::Held(hold) => assert_eq!(
                hold,
                RetentionHold::NoCreatedAt {
                    key: key(0, 25, 25)
                },
                "{unset}"
            ),
            other @ (Retention::Delete(_) | Retention::Wait { .. }) => {
                panic!("{unset}: {other:?}")
            }
        }
    }
    // The same ladder with a non-zero created-at time ages that file as usual.
    let deleted = keys(&retention(
        &inv(ladder()),
        Timestamp::MAX,
        &one_level(),
        None,
    ));
    assert!(deleted.contains(&key(0, 25, 25)), "{deleted:?}");
}

#[test]
fn snapshots_whose_created_at_run_backwards_hold_with_a_text_about_stored_times() {
    let mut files = ladder();
    files.retain(|f| f.key != key(9, 1, 20));
    files.push(file(9, 1, 20, created_at_of(5)));
    let hold = held(files);
    assert_eq!(
        hold,
        RetentionHold::SnapshotCreatedAtBackwards {
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
fn equal_snapshot_created_at_are_not_backwards() {
    let mut files = ladder();
    files.retain(|f| f.key != key(9, 1, 20));
    files.push(file(9, 1, 20, created_at_of(10)));
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
    let mut files = vec![file(9, 1, 10, created_at_of(10))];
    files.extend((11..=14).map(|n| file(0, n, n, created_at_of(n))));
    assert!(matches!(run(files.clone()), Retention::Wait { .. }));
    let _hook = Broken::on();
    let got = run(files);
    assert!(
        matches!(
            got,
            Retention::Held(RetentionHold::SelfCheck {
                outcome: PlanOutcome::Gap { .. }
            })
        ),
        "{got:?}"
    );
}

#[test]
fn the_gate_reports_the_first_failure_in_its_order() {
    let mut files = ladder();
    files.push(file(9, 5, 30, created_at_of(30)));
    let inventory = inv(files).with_skipped(level(0), 1);
    let got = retention(&inventory, at(T0 + 200 * HOUR), &defaults(), None);
    assert_eq!(
        got,
        Retention::Held(RetentionHold::UnrecognizedNames {
            level: level(0),
            count: 1
        })
    );
}

// Through `due`.

#[test]
fn due_hands_out_the_delete_job_and_then_waits_with_nothing_left() {
    let files = ladder();
    let now = at(newest_created_at(&files));
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
    let now = at(newest_created_at(&ladder()));
    let held_inv = inv(ladder()).with_skipped(level(2), 1);
    assert_eq!(
        due(&held_inv, now, &cfg, None),
        Due::Wait {
            until: None,
            held: Some(RetentionHold::UnrecognizedNames {
                level: level(2),
                count: 1
            })
        }
    );
    let mut files = ladder();
    files.push(file(0, 31, 31, created_at_of(30)));
    let behind = inv(files).with_skipped(level(2), 1);
    let got = due(&behind, now, &cfg, None);
    assert!(matches!(got, Due::Run(Job::Compact(_))), "{got:?}");
}

#[test]
fn a_ready_snapshot_goes_before_any_delete() {
    let mut files = ladder();
    files.push(file(0, 31, 31, created_at_of(31)));
    files.push(file(1, 31, 31, created_at_of(31)));
    let now = at(created_at_of(31));
    let got = due(&inv(files), now, &one_level(), None);
    assert!(matches!(got, Due::Run(Job::Snapshot(_))), "{got:?}");
}

#[test]
fn the_wake_times_are_when_the_next_file_ages_out() {
    let cfg = one_level();
    let files = ladder();
    // 60 h after T0: the first snapshot (recorded as created 40 h) is inside retention.
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
        file(9, 1, 30, created_at_of(30)),
        file(1, 21, 30, created_at_of(30)),
        file(0, 25, 25, created_at_of(25)),
        file(0, 30, 30, created_at_of(30)),
    ];
    let inventory = inv(files);
    let wake = created_at_of(25) + 300_000;
    let early = at(created_at_of(25) + 60_000);
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
fn a_clock_far_ahead_deletes_what_the_newest_created_at_deletes_and_one_far_behind_nothing() {
    let files = ladder();
    let newest = at(newest_created_at(&files));
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
    let newest = before
        .files()
        .iter()
        .map(|f| f.created_at().unwrap().at())
        .max()?;
    let anchor = now.min(newest).unix_millis();
    let cutoff =
        anchor.saturating_sub(i64::try_from(cfg.snapshot_retention().as_millis()).unwrap());
    let first = snaps
        .iter()
        .position(|s| s.created_at().unwrap().at().unix_millis() >= cutoff)
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
            && restores(&plan(after, KeyTarget::Txid(range.max())), range.max())
    })
}

/// Invariants 1 to 4 for one delete job. `consistent`: created-at times follow the
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
        restores(&plan(&after, KeyTarget::Latest), newest),
        "latest after {job:?}"
    );
    // 2
    let (lo, floor_snapshot) = retained_from(before, now, cfg).unwrap();
    for raw in lo.get()..=newest.get() {
        let t = Txid::new(raw).unwrap();
        if restores(&plan(before, KeyTarget::Txid(t)), t) {
            let now_plan = plan(&after, KeyTarget::Txid(t));
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
        // Level 0 is judged file by file: the covered, aged files
        // go and an uncovered one in between stays, so only 1 to 8 are prefixes.
        if l != Level::ZERO {
            assert_eq!(&was[gone..], is, "{l} did not lose a prefix");
        }
    }
}

/// The same for `AsOf`: at each kept file's created-at time plus a millisecond, an
/// instant that planned still plans to the same state, or says the instant is
/// inside a compacted file.
fn check_instants(before: &Inventory, after: &Inventory, lo: Txid) {
    let base_created_at = before
        .level(Level::SNAPSHOT)
        .iter()
        .find(|s| s.key.range().max() == lo && is_snapshot_start(s))
        .map(|s| s.created_at().unwrap().at().unix_millis());
    for f in after.files() {
        let instant = at(f.created_at().unwrap().at().unix_millis().saturating_add(1));
        let Some(base) = base_created_at else {
            continue;
        };
        if f.created_at().unwrap().at().unix_millis() < base {
            continue;
        }
        let target = RestoreTarget::AsOf(instant);
        if let PlanOutcome::Plan(was) = crate::plan::plan_reference(before, target) {
            let wanted = was.reaches();
            let now = crate::plan::plan_reference(after, target);
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
    /// No overlaps, no holes after the last snapshot, created-at times follow the
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
    let mut resolved = vec![0i64];
    let mut t = T0;
    for _ in 1..=n {
        t += gaps[b.index(gaps.len())];
        resolved.push(t);
    }
    let created_at = |i: u64| resolved[usize::try_from(i).unwrap()];
    let mut files = Vec::new();
    let snaps: BTreeSet<u64> = if b.percent(8) {
        BTreeSet::new()
    } else {
        (0..=b.below(4)).map(|_| 2 + b.below(n - 1)).collect()
    };
    let last_snap = snaps.last().copied().unwrap_or(0);
    files.extend(snaps.iter().map(|m| file(9, 1, *m, created_at(*m))));
    for i in 1..=n {
        if i > last_snap || b.percent(70) {
            files.push(file(0, i, i, created_at(i)));
        }
    }
    let covered_to = if b.percent(15) { 0 } else { 1 + b.below(n) };
    let l1 = chunks(&mut b, 1, covered_to, 6);
    files.extend(l1.iter().map(|(a, z)| file(1, *a, *z, created_at(*z))));
    let l2_count = b.index(l1.len() + 1);
    let mut at = 0;
    while at < l2_count {
        let end = (at + b.index(3)).min(l2_count - 1);
        files.push(file(2, l1[at].0, l1[end].1, created_at(l1[end].1)));
        at = end + 1;
    }
    let mut listing = Listing {
        files,
        skipped: None,
        published: None,
        sane: !weird,
    };
    if weird {
        roughen(&mut b, &mut listing, n, &created_at);
        // A store lists a key once; a roughened created-at time on one copy of a key
        // pushed twice is not a listing.
        let mut seen = BTreeSet::new();
        listing.files.retain(|f| seen.insert(f.key));
    }
    listing
}

/// Holes, overlaps, backwards or zero created-at times, unrecognized names, a listing behind.
fn roughen(b: &mut Bytes, listing: &mut Listing, n: u64, created_at: &dyn Fn(u64) -> i64) {
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
                listing.files.push(file(l, lo, hi, created_at(hi)));
            }
            2 if !listing.files.is_empty() => {
                let at = b.index(listing.files.len());
                let f = &mut listing.files[at];
                set_created_at(f, at_created_at(created_at(1 + b.below(n))));
            }
            3 if !listing.files.is_empty() => {
                let at = b.index(listing.files.len());
                set_created_at(&mut listing.files[at], Timestamp::from_unix_millis(0));
            }
            4 => {
                let lo = 2 + b.below(n - 1);
                listing.files.push(file(9, lo, n, created_at(n)));
            }
            5 => listing.skipped = Some((level(u8::try_from(b.below(10)).unwrap()), 1)),
            6 => listing.published = Txid::new(n + 1 + b.below(2)),
            _ => {}
        }
    }
}

fn at_created_at(ms: i64) -> Timestamp {
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
    let newest = newest_created_at(files);
    match b.below(5) {
        0 => newest,
        1 => newest + DAY,
        2 => i64::MAX,
        3 => newest - i64::try_from(b.below(120)).unwrap() * HOUR,
        _ => {
            files[b.index(files.len())]
                .created_at()
                .unwrap()
                .at()
                .unix_millis()
                + 1
        }
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
    let Some(newest) = inventory
        .files()
        .iter()
        .map(|f| f.created_at().unwrap().at())
        .max()
    else {
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
    // The level 0 scan stops at the first young file on the promise that
    // the files after it are young (see "Rules" in the module docs). Later
    // snapshot created-at times move the floor, which changes which files the scan
    // reads at all, so with level 0 created-at times running backwards the rewrite can
    // unblock a delete the true created-at times never reached. That is not a promise
    // of the design.
    if !matches!(first, Retention::Held(_)) && level_zero_created_at_follow_keys(inventory) {
        later_created_at(inventory, now, cfg, published, &first, noise);
    }
}

/// Whether the level 0 files' created-at times never decrease in key order.
fn level_zero_created_at_follow_keys(inventory: &Inventory) -> bool {
    let resolved: Vec<i64> = inventory
        .level(Level::ZERO)
        .iter()
        .map(|f| f.created_at().unwrap().at().unix_millis())
        .collect();
    resolved.is_sorted()
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
            // The level-0 scan stops at the first young file, so a level-0
            // file with no created-at time behind it is not read until that file
            // ages out. The wake is honest, and the hold it meets then
            // carries no wake time, so the task sleeps instead of spinning.
            let without_created_at_behind = matches!(
                &at_wake,
                Retention::Held(RetentionHold::NoCreatedAt { key })
                    if key.level() == Level::ZERO
                        && inv
                            .files()
                            .iter()
                            .any(|f| f.key == *key && f.created_at().unwrap().at().unix_millis() <= 0)
            );
            assert!(
                matches!(at_wake, Retention::Delete(_)) || without_created_at_behind,
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

/// 8: created-at times rewritten later (all older ones to one instant, then everything
/// by a constant) delete nothing that the true created-at times keep. Stretching the
/// gaps between created-at times is a different fault and a non-goal.
fn later_created_at(
    inventory: &Inventory,
    now: Timestamp,
    cfg: &Maintenance,
    published: Option<Txid>,
    truth: &Retention,
    noise: &mut Bytes,
) {
    let files = inventory.files();
    let floor = files[noise.index(files.len())]
        .created_at()
        .unwrap()
        .at()
        .unix_millis();
    let shift = i64::try_from(noise.below(40)).unwrap() * HOUR;
    let moved: Vec<FileInfo> = files
        .iter()
        .map(|f| FileInfo {
            listed: ListedTime::Created(CreatedAt::mtime(at(f
                .created_at()
                .unwrap()
                .at()
                .unix_millis()
                .max(floor)
                + shift))),
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
                "later created-at times delete {k}, the true created-at times keep it"
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
        check_generated(&data, weird, which);
    }
}

/// One generated listing, every rule checked: the body of the property, so a
/// shrunk case can be replayed by name.
fn check_generated(data: &[u8], weird: bool, which: usize) {
    let listing = build(data, weird);
    if listing.files.is_empty() {
        return;
    }
    let mut inventory = inv(listing.files.clone());
    if let Some((l, count)) = listing.skipped {
        inventory = inventory.with_skipped(l, count);
    }
    let mut b = Bytes { data, at: 7 };
    let now = at(pick_now(&mut b, inventory.files()));
    let cfg = &configs()[which];
    check_listing(
        &inventory,
        now,
        cfg,
        listing.published,
        listing.sane,
        &mut b,
    );
}

/// A shrunk failure of the property: a level 0 file with a young created-at time sorts
/// before an old one, and later snapshot created-at times move the floor so that the
/// young file stops blocking the old one's delete. Created-at times like that are not
/// a writer's, so the later-created-at times rule does not apply to them.
#[test]
fn a_level_zero_file_created_out_of_order_is_outside_the_later_created_at_rule() {
    let data = [
        107, 0, 0, 0, 0, 0, 0, 63, 209, 174, 13, 0, 0, 0, 0, 0, 0, 0, 0, 85, 10, 40, 73, 0, 38, 0,
        132, 128, 0, 92, 39, 24, 178, 233, 155, 133, 85, 227, 108, 64, 8, 106, 247, 0, 1, 0, 0, 70,
        0, 70, 0, 0, 0, 100, 70, 0, 0, 100, 0, 0, 0, 0, 0, 0, 100, 0, 100, 0, 53, 65, 2, 2, 92, 79,
        0, 0, 240, 203, 184, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    check_generated(&data, true, 0);
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

struct Xs(u64);

impl Xs {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(u64::try_from(len).unwrap())).unwrap()
    }
    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[self.index(xs.len())]
    }
}

/// Created-at time shapes for the generated listings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    /// One writer: L0 in single transactions, L1 and L2 compacted from whole
    /// L0 files, created-at times of the last transaction, snapshots at an L0 end.
    Writer,
    /// As Writer, but L0 files hold several transactions and L1 boundaries
    /// are drawn independently (a handoff or a Go-era replica): an L0 file can
    /// straddle a snapshot's end or an L1 boundary. Created-at times still follow
    /// transactions.
    Misaligned,
    /// Misaligned plus overlapping L0 files (same created-at time rule: the last
    /// transaction's).
    Overlapping,
    /// Misaligned with some created-at times moved earlier or later (a clock that
    /// stepped across a handoff).
    Backwards,
}

fn gen_listing(x: &mut Xs, shape: Shape) -> Vec<FileInfo> {
    let n = 6 + x.below(24);
    let gaps = [1_000, 60_000, HOUR, 4 * HOUR, DAY];
    let mut f = vec![T0];
    for _ in 0..=n {
        let last = *f.last().unwrap();
        f.push(last + x.pick(&gaps));
    }
    let st = |i: u64| f[usize::try_from(i).unwrap()];
    let mut files = Vec::new();
    let span = if shape == Shape::Writer { 1 } else { 3 };
    let l0 = chunks_x(x, 1, n, span);
    let snaps: BTreeSet<u64> = (0..=x.below(4))
        .map(|_| {
            if shape == Shape::Writer {
                l0[x.index(l0.len())].1
            } else {
                1 + x.below(n)
            }
        })
        .collect();
    let last_snap = snaps.last().copied().unwrap_or(0);
    for &(a, z) in &l0 {
        if z > last_snap || x.below(100) < 70 {
            files.push(file(0, a, z, st(z)));
        }
    }
    if shape == Shape::Overlapping {
        for _ in 0..=x.below(3) {
            let a = 1 + x.below(n);
            let z = a + x.below((n - a + 1).min(5));
            files.push(file(0, a, z, st(z)));
        }
    }
    for &m in &snaps {
        // A snapshot is recorded as created when taken: after its last transaction, before
        // the next one.
        let lag = if x.below(2) == 0 {
            0
        } else {
            (st(m + 1) - st(m)) / 2
        };
        files.push(file(9, 1, m, st(m) + lag));
    }
    let covered_to = if x.below(100) < 15 { 0 } else { 1 + x.below(n) };
    let l1: Vec<(u64, u64)> = if shape == Shape::Writer {
        // Whole L0 files.
        let mut out = Vec::new();
        let mut i = 0;
        while i < l0.len() && l0[i].1 <= covered_to {
            let j = (i + usize::try_from(x.below(4)).unwrap()).min(l0.len() - 1);
            out.push((l0[i].0, l0[j].1));
            i = j + 1;
        }
        out
    } else {
        chunks_x(x, 1, covered_to, 6)
    };
    files.extend(l1.iter().map(|(a, z)| file(1, *a, *z, st(*z))));
    if !l1.is_empty() && x.below(2) == 0 {
        let end = x.index(l1.len());
        files.push(file(2, l1[0].0, l1[end].1, st(l1[end].1)));
    }
    if shape == Shape::Backwards {
        for _ in 0..=x.below(3) {
            let i = x.index(files.len());
            let j = 1 + x.below(n);
            set_created_at(&mut files[i], at(st(j)));
        }
    }
    let mut seen = BTreeSet::new();
    files.retain(|f| seen.insert(f.key));
    files
}

fn chunks_x(x: &mut Xs, from: u64, to: u64, most: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut a = from;
    while a <= to {
        let z = (a + x.below(most)).min(to);
        out.push((a, z));
        a = z + 1;
    }
    out
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap()
}

/// The instants at which some rule changes: every created-at time plus each retention,
/// and one millisecond either side.
fn anchors(files: &[FileInfo], cfg: &Maintenance) -> Vec<i64> {
    let mut out = BTreeSet::new();
    let snap = millis(cfg.snapshot_retention());
    let l0 = match cfg.l0_retention() {
        L0Retention::After(age) => millis(age),
        L0Retention::Keep => 0,
    };
    for f in files {
        let s = f.created_at().unwrap().at().unix_millis();
        for base in [s, s + snap, s + l0] {
            out.extend([base - 1, base, base + 1]);
        }
    }
    out.into_iter().collect()
}

/// The rule is not monotone in the anchor: a lower anchor can delete a file
/// a higher one keeps. Floor 20 at the high anchor removes L1 15-19 and leaves
/// L0 18-21 (straddling the floor, uncovered). At the low anchor the floor is
/// 10, L1 15-19 stays and covers L0 18-21, which goes. Both jobs are
/// restore-safe for the range their anchor promises (the wobbling-clock test
/// checks that); this pins what the rules do.
#[test]
fn a_lower_anchor_can_delete_a_level_zero_file_a_higher_one_keeps() {
    let files = vec![
        file(9, 1, 10, T0),
        file(9, 1, 20, T0 + 2 * DAY),
        file(9, 1, 30, T0 + 4 * DAY),
        file(1, 11, 14, T0 + DAY),
        file(1, 15, 19, T0 + 2 * DAY - HOUR),
        file(1, 20, 30, T0 + 4 * DAY),
        file(0, 18, 21, T0 + 2 * DAY + HOUR),
        file(0, 31, 31, T0 + 4 * DAY + HOUR),
    ];
    let inventory = inv(files);
    let cfg = Maintenance::new(
        vec![secs(30)],
        secs(24 * 3600),
        secs(24 * 3600),
        L0Retention::After(secs(300)),
    )
    .unwrap();
    let del = |now: i64| match retention(&inventory, at(now), &cfg, None) {
        Retention::Delete(job) => job.keys().to_vec(),
        Retention::Wait { .. } | Retention::Held(_) => Vec::new(),
    };
    let high = del(T0 + 4 * DAY + HOUR);
    let low = del(T0 + 2 * DAY + 12 * HOUR);
    let l0 = key(0, 18, 21);
    assert!(!high.contains(&l0), "high anchor: {high:?}");
    assert!(high.contains(&key(1, 15, 19)), "high anchor: {high:?}");
    assert!(low.contains(&l0), "low anchor: {low:?}");
    assert!(!low.contains(&key(1, 15, 19)), "low anchor: {low:?}");
}

/// The property the module doc states: ticks against the current listing with
/// a clock that steps back and forth, each job applied only in part (a batch
/// delete that fails partway, a crash). After every tick the newest state
/// restores, and every transaction at or above the highest first-retained
/// snapshot seen so far that restored at the start still restores, or is
/// inside a kept file whose end restores. Instants are checked per tick
/// (`check_instants`) on sane created-at times.
#[test]
fn retention_r_prime_jobs_are_restore_safe() {
    let mut x = Xs(0x7a11_c10c);
    let mut deleted_total = 0usize;
    for case in 0..1500u64 {
        let shape = [
            Shape::Writer,
            Shape::Misaligned,
            Shape::Overlapping,
            Shape::Backwards,
        ][usize::try_from(case % 4).unwrap()];
        let files = gen_listing(&mut x, shape);
        let cfg = &configs()[usize::try_from(case / 4 % 4).unwrap()];
        let start = inv(files.clone());
        let Some(newest) = start.newest() else {
            continue;
        };
        let newest_ms = newest_created_at(&files);
        let mut lo_max = Txid::MIN;
        let mut current = start.clone();
        let nows = anchors(&files, cfg);
        for _tick in 0..12 {
            let now = x.pick(&nows).min(newest_ms + DAY);
            let Retention::Delete(job) = retention(&current, at(now), cfg, None) else {
                continue;
            };
            let (lo, _) = retained_from(&current, at(now), cfg).unwrap();
            lo_max = lo_max.max(lo);
            let part: Vec<FileKey> = job
                .keys()
                .iter()
                .copied()
                .filter(|_| x.below(3) != 0)
                .collect();
            deleted_total += part.len();
            let after = without(&current, &part);
            assert!(
                restores(&plan(&after, KeyTarget::Latest), newest),
                "latest lost: case {case}"
            );
            if shape != Shape::Backwards && shape != Shape::Overlapping {
                check_instants(&current, &after, lo);
            }
            current = after;
            for raw in lo_max.get()..=newest.get() {
                let t = Txid::new(raw).unwrap();
                if restores(&plan(&start, KeyTarget::Txid(t)), t) {
                    let p = plan(&current, KeyTarget::Txid(t));
                    assert!(
                        restores(&p, t) || inside_a_kept_file(&current, t),
                        "case {case} {shape:?}: {t} (>= {lo_max}) lost: {p:?}; start {:?}",
                        start
                            .files()
                            .iter()
                            .map(|f| (f.key, f.created_at().unwrap().at().unix_millis() - T0))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
    println!("deleted {deleted_total} keys");
    assert!(deleted_total > 1000);
}

// The clock from created-at times read on demand (R').

/// A lazy store holding `files` (each recorded as created as listed, one byte each) and
/// the retention run over its listing.
struct Lazy {
    store: MemStore,
}

impl Lazy {
    fn new(files: &[FileInfo]) -> Self {
        let store = MemStore::with_listed_last_modified();
        crate::conformance::block_on(async {
            for f in files {
                let created_at = f.created_at().unwrap().at();
                let mut up = store.begin_upload(&f.key, created_at).await.unwrap();
                up.write(b"x").await.unwrap();
                up.commit().await.unwrap();
            }
        });
        Self { store }
    }

    /// The decision and the keys whose created-at times it asked for, in order.
    fn run(&self, now: i64, cfg: &Maintenance) -> (Retention, Vec<FileKey>) {
        let inventory = crate::conformance::block_on(Inventory::load(&self.store)).unwrap();
        let mut asked = Vec::new();
        let mut step = |resolved: &ResolvedCreatedAt| {
            let got = super::retention(
                &inventory,
                at(now),
                cfg,
                None,
                &MaintainState::default(),
                resolved,
            );
            if let Resolve::Need(file) = &got {
                asked.push(file.first().key);
            }
            got
        };
        let done = crate::conformance::block_on(resolve(&self.store, &mut step)).unwrap();
        assert_eq!(done.created_at_read, u64::try_from(asked.len()).unwrap());
        (done.value, asked)
    }
}

fn key_set(keys: &[FileKey]) -> BTreeSet<FileKey> {
    keys.iter().copied().collect()
}

/// On one writer's listing the newest created-at time is on a last file or a snapshot
/// start, so the clock from them is main's, and so is every decision.
#[test]
fn retention_equals_exact_on_writer_listings() {
    let mut x = Xs(0x51ab_1e55);
    let mut deletes = 0;
    for case in 0..80u64 {
        let files = gen_listing(&mut x, Shape::Writer);
        let cfg = &configs()[usize::try_from(case % 4).unwrap()];
        let lazy = Lazy::new(&files);
        for now in anchors(&files, cfg) {
            let now = now.min(newest_created_at(&files) + DAY);
            let (got, _) = lazy.run(now, cfg);
            ANCHOR_FROM_EVERY_FILE.set(true);
            let want = retention(&inv(files.clone()), at(now), cfg, None);
            ANCHOR_FROM_EVERY_FILE.set(false);
            assert_eq!(got, want, "case {case} now {now}");
            deletes += usize::from(matches!(got, Retention::Delete(_)));
        }
    }
    assert!(deletes > 20, "only {deletes} delete decisions exercised");
}

/// The clock from the lasts and snapshot starts is never ahead of the clock
/// from every file, whatever the created-at times do.
#[test]
fn retention_r_prime_anchor_is_at_most_the_exact_anchor() {
    let mut x = Xs(0xa4c4_0123);
    for case in 0..600u64 {
        let shape = [
            Shape::Writer,
            Shape::Misaligned,
            Shape::Overlapping,
            Shape::Backwards,
        ][usize::try_from(case % 4).unwrap()];
        let files = gen_listing(&mut x, shape);
        let inventory = inv(files.clone());
        let newest_of = |files: &[FileInfo]| {
            files
                .iter()
                .map(|f| f.created_at().unwrap().at().unix_millis())
                .max()
        };
        let r_prime = newest_of(&anchor_files(&inventory));
        assert!(r_prime <= newest_of(&files), "case {case} {shape:?}");
        if shape == Shape::Writer {
            assert_eq!(r_prime, newest_of(&files), "case {case}");
        }
    }
}

#[test]
fn retention_reads_only_lasts_snaps_and_scanned_level_zero() {
    let files = ladder();
    let now = newest_created_at(&files);
    let (got, asked) = Lazy::new(&files).run(now, &defaults());
    assert_eq!(keys(&got), expected_ladder_deletes());
    let mut want = vec![key(0, 30, 30), key(1, 21, 30)];
    want.extend([key(9, 1, 10), key(9, 1, 20), key(9, 1, 30)]);
    want.extend((25..=29).map(|n| key(0, n, n)));
    assert_eq!(key_set(&asked), key_set(&want), "{asked:?}");
    assert_eq!(
        asked.len(),
        want.len(),
        "a created-at time was asked twice: {asked:?}"
    );
}

/// The level-0 scan ends at the first young file: later ones are not read.
#[test]
fn level_zero_reading_stops_at_the_first_young_file() {
    let mut files = ladder();
    let now = newest_created_at(&files);
    for f in &mut files {
        if (27..=29).contains(&f.key.range().max().get()) && f.key.level() == Level::ZERO {
            set_created_at(f, at(now - 1_000));
        }
    }
    let (got, asked) = Lazy::new(&files).run(now, &defaults());
    assert!(
        matches!(got, Retention::Delete(_) | Retention::Wait { .. }),
        "{got:?}"
    );
    let level_zero: BTreeSet<u64> = asked
        .iter()
        .filter(|k| k.level() == Level::ZERO)
        .map(|k| k.range().max().get())
        .collect();
    assert_eq!(level_zero, BTreeSet::from([25, 26, 27, 30]), "{asked:?}");
}

/// What a lazy listing calls last-modified is the store's clock, so moving it
/// changes neither the decision nor what is read.
#[test]
fn retention_ignores_last_modified() {
    let files = ladder();
    let now = newest_created_at(&files);
    let lazy = Lazy::new(&files);
    let base = lazy.run(now, &defaults());
    for moved in [0, i64::MAX / 2] {
        for f in &files {
            lazy.store.set_last_modified(&f.key, at(moved));
        }
        assert_eq!(lazy.run(now, &defaults()), base, "last-modified {moved}");
    }
}

/// Stored created-at times all young while last-modified says everything is ancient:
/// nothing may be deleted.
#[test]
fn retention_never_deletes_on_last_modified() {
    let mut files = ladder();
    let now = newest_created_at(&files);
    for f in &mut files {
        set_created_at(f, at(now));
    }
    let lazy = Lazy::new(&files);
    for f in &files {
        lazy.store.set_last_modified(&f.key, at(0));
    }
    let (got, _) = lazy.run(now, &defaults());
    assert!(!matches!(got, Retention::Delete(_)), "{got:?}");
}

/// A young level-0 file ahead of an without a created-at time one: the without a created-at time file is
/// behind the scan's stop, so the wake time is promised, and at it the hold
/// has no wake time of its own (the task sleeps, it does not spin).
#[test]
fn a_file_without_created_at_behind_a_young_one_is_a_hold_at_the_wake_not_a_spin() {
    let mut files = ladder();
    files.retain(|f| f.key != key(1, 1, 10) && f.key != key(9, 1, 10) && f.key != key(9, 1, 20));
    for f in &mut files {
        if f.key.level() == Level::ZERO {
            set_created_at(f, at(created_at_of(30) - 4 * HOUR));
        }
    }
    let now = newest_created_at(&files) - HOUR;
    for f in &mut files {
        match (f.key.level() == Level::ZERO).then(|| f.key.range().max().get()) {
            Some(25) => set_created_at(f, at(now - 1_000)),
            Some(26) => set_created_at(f, at(0)),
            _ => {}
        }
    }
    let cfg = one_level();
    let now = at(now);
    let Retention::Wait { until: Some(wake) } = retention(&inv(files.clone()), now, &cfg, None)
    else {
        panic!(
            "expected a wake time: {:?}",
            retention(&inv(files.clone()), now, &cfg, None)
        );
    };
    let at_wake = retention(&inv(files.clone()), wake, &cfg, None);
    assert_eq!(
        at_wake,
        Retention::Held(RetentionHold::NoCreatedAt {
            key: key(0, 26, 26)
        })
    );
    let due = crate::maintain::tests::due_state(
        &inv(files),
        wake,
        &cfg,
        None,
        &mut MaintainState::default(),
    );
    assert!(
        matches!(
            due,
            Due::Wait {
                until: None,
                held: Some(_)
            }
        ),
        "{due:?}"
    );
}

mod switch;
