use super::*;
use crate::layout::{CreatedAt, ListedTime};
use crate::spec_view;
use lsr_ltx::TxidRange;
use proptest::prelude::*;
use std::collections::HashSet;

fn txid(raw: u64) -> Txid {
    Txid::new(raw).unwrap()
}

fn key(level: u8, min: u64, max: u64) -> FileKey {
    let range = TxidRange::new(txid(min), txid(max)).unwrap();
    FileKey::new(Level::new(level).unwrap(), range)
}

/// A file created at [`created`] milliseconds.
fn file(level: u8, min: u64, max: u64, created: i64) -> FileInfo {
    FileInfo {
        key: key(level, min, max),
        size: 1,
        listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(created))),
    }
}

/// A file created at 100 ms per transaction, the usual shape.
fn at(level: u8, min: u64, max: u64) -> FileInfo {
    file(level, min, max, ms(max))
}

/// 100 ms per transaction.
fn ms(txn: u64) -> i64 {
    i64::try_from(txn).unwrap().saturating_mul(100)
}

fn inv(files: &[FileInfo]) -> Inventory {
    Inventory::from_files(files.iter().copied()).unwrap()
}

fn keys(outcome: &PlanOutcome) -> Vec<FileKey> {
    let PlanOutcome::Plan(chain) = outcome else {
        panic!("expected a plan, got {outcome:?}");
    };
    chain.files().iter().map(|f| f.key).collect()
}

const ALL_TARGETS: [RestoreTarget; 3] = [
    RestoreTarget::Latest,
    RestoreTarget::AsOf(Timestamp::from_unix_millis(1_000_000)),
    RestoreTarget::Txid(Txid::MIN),
];

fn targets_through(txn: u64) -> [RestoreTarget; 3] {
    let [latest, as_of, _] = ALL_TARGETS;
    [latest, as_of, RestoreTarget::Txid(txid(txn))]
}

fn gap(have_through: u64, next_starts: u64) -> PlanOutcome {
    PlanOutcome::Gap {
        have_through: txid(have_through),
        next_starts: txid(next_starts),
    }
}

/// A healthy writer-shaped replica after level-0 retention: L0 0x125 to
/// 0x12b are gone, the level 1 file that covers them is recorded as created after the
/// instant, and the stray L0 files 0x12c and 0x12d are recorded as created before it.
/// Go restores 0x124. The planner reaches it too, but says it lies inside
/// L1 0x125-0x12f; Latest still reaches 0x140.
fn retention_replica() -> Vec<FileInfo> {
    let mut files = vec![
        file(9, 1, 0x124, 360_086),
        file(1, 0x11b, 0x124, 360_086),
        file(1, 0x125, 0x12f, 361_086),
        file(1, 0x130, 0x13a, 362_086),
    ];
    for (step, t) in (0x12c..=0x140u64).enumerate() {
        let created_at =
            360_786_i64.saturating_add(100_i64.saturating_mul(i64::try_from(step).unwrap()));
        files.push(file(0, t, t, created_at));
    }
    files
}

#[test]
fn a_stray_file_before_the_instant_is_not_a_gap_and_the_straddler_still_shows() {
    let files = retention_replica();
    let as_of = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(360_900)),
    );
    let PlanOutcome::AsOfInsideFile { chain, file } = as_of else {
        panic!("expected AsOfInsideFile, got {as_of:?}");
    };
    assert_eq!(chain.reaches(), txid(0x124));
    assert_eq!(file, key(1, 0x125, 0x12f));
    let latest = plan_reference(&inv(&files), RestoreTarget::Latest);
    let PlanOutcome::Plan(chain) = latest else {
        panic!("expected a plan, got {latest:?}");
    };
    assert_eq!(chain.reaches(), txid(0x140));
}

#[test]
fn as_of_before_a_hole_reports_the_gap() {
    let files = [file(0, 1, 2, 100), file(0, 5, 6, 200)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
    );
    assert_eq!(
        outcome,
        PlanOutcome::Gap {
            have_through: txid(2),
            next_starts: txid(5)
        }
    );
    assert_eq!(
        outcome,
        plan_reference(&inv(&files), RestoreTarget::Latest),
        "the Latest planner reports the same hole"
    );
}

#[test]
fn as_of_before_a_hole_that_a_later_snapshot_covers_is_not_a_gap() {
    let files = [file(0, 1, 2, 100), file(0, 5, 6, 200), file(9, 1, 6, 300)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
    );
    assert!(
        matches!(outcome, PlanOutcome::AsOfInsideFile { ref file, .. } if file.level().get() == 9),
        "{outcome:?}"
    );
}

#[test]
fn as_of_with_a_linking_file_created_after_the_instant_keeps_its_outcome() {
    let files = [file(0, 1, 2, 100), file(0, 3, 6, 200)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
    );
    assert!(
        matches!(outcome, PlanOutcome::AsOfInsideFile { .. }),
        "{outcome:?}"
    );
}

#[test]
fn as_of_with_nothing_past_the_reach_plans() {
    let files = [file(0, 1, 2, 100)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(101)),
    );
    assert_eq!(keys(&outcome), vec![key(0, 1, 2)]);
}

#[test]
fn a_gap_at_level_zero_is_a_gap_for_every_target() {
    // Transaction 6 is gone from L0 and nothing covers it.
    let files = [
        at(9, 1, 3),
        at(0, 4, 4),
        at(0, 5, 5),
        at(0, 7, 7),
        at(0, 8, 8),
    ];
    for target in targets_through(8) {
        assert_eq!(
            plan_reference(&inv(&files), target),
            gap(5, 7),
            "{target:?}"
        );
    }
}

#[test]
fn a_gap_at_a_higher_level_is_a_gap_for_every_target() {
    // L1 4-6 is gone; L0 4 to 6 were compacted away long ago.
    let files = [at(9, 1, 3), at(1, 7, 9), at(0, 10, 10)];
    for target in targets_through(10) {
        assert_eq!(
            plan_reference(&inv(&files), target),
            gap(3, 7),
            "{target:?}"
        );
    }
}

#[test]
fn a_target_before_the_hole_still_plans() {
    let files = [at(9, 1, 3), at(0, 4, 4), at(0, 5, 5), at(0, 7, 7)];
    let outcome = plan_reference(&inv(&files), RestoreTarget::Txid(txid(5)));
    assert_eq!(
        keys(&outcome),
        vec![key(9, 1, 3), key(0, 4, 4), key(0, 5, 5)]
    );
    let as_of = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(600)),
    );
    // Transaction 6 is absent and may have committed before the instant.
    assert_eq!(as_of, gap(5, 7));
}

#[test]
fn a_timestamp_restore_that_would_stop_at_a_hole_is_a_gap() {
    // The upstream bug: files 1-3, 4, then a hole, then 6 and 7 created
    // before the instant. Go restores 1-4 and exits 0.
    let files = [at(9, 1, 3), at(0, 4, 4), at(0, 6, 6), at(0, 7, 7)];
    let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(750));
    assert_eq!(plan_reference(&inv(&files), target), gap(4, 6));
}

#[test]
fn a_higher_level_file_spanning_the_hole_heals_it() {
    let files = [at(9, 1, 3), at(0, 4, 4), at(0, 6, 6), at(1, 5, 7)];
    for target in targets_through(7) {
        let outcome = plan_reference(&inv(&files), target);
        assert_eq!(
            keys(&outcome),
            vec![key(9, 1, 3), key(0, 4, 4), key(1, 5, 7)],
            "{target:?}"
        );
    }
}

#[test]
fn an_overlapping_higher_file_replaces_the_small_ones() {
    let files = [at(9, 1, 3), at(0, 4, 4), at(0, 5, 5), at(2, 3, 8)];
    let outcome = plan_reference(&inv(&files), RestoreTarget::Latest);
    assert_eq!(keys(&outcome), vec![key(9, 1, 3), key(2, 3, 8)]);
}

#[test]
fn equal_reach_prefers_the_higher_level_then_the_earlier_start() {
    let files = [
        at(9, 1, 3),
        at(0, 4, 6),
        at(2, 4, 6),
        at(2, 3, 6),
        at(1, 4, 6),
    ];
    let outcome = plan_reference(&inv(&files), RestoreTarget::Latest);
    assert_eq!(keys(&outcome), vec![key(9, 1, 3), key(2, 3, 6)]);
}

#[test]
fn a_target_inside_a_compacted_file_names_the_file() {
    let files = [at(9, 1, 3), at(1, 4, 9)];
    assert_eq!(
        plan_reference(&inv(&files), RestoreTarget::Txid(txid(6))),
        PlanOutcome::InsideFile {
            requested: txid(6),
            file: key(1, 4, 9),
        }
    );
    // At the file's end it is a plan.
    let at_end = plan_reference(&inv(&files), RestoreTarget::Txid(txid(9)));
    assert_eq!(keys(&at_end), vec![key(9, 1, 3), key(1, 4, 9)]);
}

#[test]
fn a_target_inside_a_snapshot_names_it() {
    let files = [at(9, 1, 3), at(9, 1, 9), at(0, 10, 10)];
    assert_eq!(
        plan_reference(&inv(&files), RestoreTarget::Txid(txid(6))),
        PlanOutcome::InsideFile {
            requested: txid(6),
            file: key(9, 1, 9),
        }
    );
}

#[test]
fn a_target_with_the_finer_files_present_plans_to_it() {
    let files = [
        at(9, 1, 3),
        at(1, 4, 9),
        at(0, 4, 4),
        at(0, 5, 5),
        at(0, 6, 6),
    ];
    let outcome = plan_reference(&inv(&files), RestoreTarget::Txid(txid(6)));
    assert_eq!(
        keys(&outcome),
        vec![key(9, 1, 3), key(0, 4, 4), key(0, 5, 5), key(0, 6, 6)]
    );
    let PlanOutcome::Plan(chain) = outcome else {
        panic!("plan expected");
    };
    assert_eq!(chain.reaches(), txid(6));
    assert_eq!(chain.snapshot().key, key(9, 1, 3));
}

#[test]
fn the_newer_snapshot_is_used_when_eligible_and_the_older_for_earlier_targets() {
    let files = [
        at(9, 1, 3),
        at(9, 1, 6),
        at(0, 4, 4),
        at(0, 5, 5),
        at(0, 6, 6),
        at(0, 7, 7),
    ];
    let inventory = inv(&files);
    let latest = plan_reference(&inventory, RestoreTarget::Latest);
    assert_eq!(keys(&latest), vec![key(9, 1, 6), key(0, 7, 7)]);
    let earlier = plan_reference(&inventory, RestoreTarget::Txid(txid(5)));
    assert_eq!(
        keys(&earlier),
        vec![key(9, 1, 3), key(0, 4, 4), key(0, 5, 5)]
    );
    let instant = RestoreTarget::AsOf(Timestamp::from_unix_millis(450));
    assert_eq!(
        keys(&plan_reference(&inventory, instant)),
        vec![key(9, 1, 3), key(0, 4, 4)]
    );
}

#[test]
fn as_of_is_strictly_before_the_file_creation_time() {
    let files = [at(9, 1, 3), at(0, 4, 4)];
    let exactly = RestoreTarget::AsOf(Timestamp::from_unix_millis(400));
    assert_eq!(
        keys(&plan_reference(&inv(&files), exactly)),
        vec![key(9, 1, 3)]
    );
    let after = RestoreTarget::AsOf(Timestamp::from_unix_millis(401));
    assert_eq!(keys(&plan_reference(&inv(&files), after)).len(), 2);
}

#[test]
fn as_of_later_than_every_file_is_latest() {
    let files = [at(9, 1, 3), at(0, 4, 4), at(0, 5, 5)];
    let late = RestoreTarget::AsOf(Timestamp::from_unix_millis(i64::MAX));
    assert_eq!(
        plan_reference(&inv(&files), late),
        plan_reference(&inv(&files), RestoreTarget::Latest)
    );
}

#[test]
fn an_empty_replica_is_its_own_outcome() {
    for target in ALL_TARGETS {
        assert_eq!(
            plan_reference(&Inventory::default(), target),
            PlanOutcome::EmptyReplica
        );
    }
}

#[test]
fn without_an_eligible_snapshot_the_outcome_says_so() {
    let starts_late = inv(&[at(0, 2, 2), at(0, 3, 3)]);
    let none = PlanOutcome::NoSnapshot {
        oldest_snapshot: None,
        malformed_snapshots: Vec::new(),
    };
    assert_eq!(plan_reference(&starts_late, RestoreTarget::Latest), none);

    let files = inv(&[at(9, 1, 5), at(0, 6, 6)]);
    let older = PlanOutcome::NoSnapshot {
        oldest_snapshot: Some(txid(5)),
        malformed_snapshots: Vec::new(),
    };
    let before_all = RestoreTarget::AsOf(Timestamp::from_unix_millis(10));
    assert_eq!(plan_reference(&files, before_all), older);
}

/// With no older base, a transaction that exists only inside the sole
/// transaction-1 file is the same cause as one inside a later file: the
/// finer files are gone. It must not read as a replica that cannot start a
/// restore, since `Latest` restores from it.
#[test]
fn a_target_inside_the_only_transaction_one_file_names_it() {
    let files = inv(&[at(9, 1, 5), at(0, 6, 6)]);
    assert_eq!(
        plan_reference(&files, RestoreTarget::Txid(txid(4))),
        PlanOutcome::InsideFile {
            requested: txid(4),
            file: key(9, 1, 5),
        }
    );
    // The nearest-ending of several transaction-1 files is named.
    let several = inv(&[at(9, 1, 9), at(2, 1, 6), at(0, 7, 7)]);
    assert_eq!(
        plan_reference(&several, RestoreTarget::Txid(txid(3))),
        PlanOutcome::InsideFile {
            requested: txid(3),
            file: key(2, 1, 6),
        }
    );
    // A covering file that does not begin at transaction 1 does not link to
    // an empty chain, and a refused snapshot is no file at all: no snapshot.
    let no_start = inv(&[at(1, 2, 8), at(9, 2, 9)]);
    assert_eq!(
        plan_reference(&no_start, RestoreTarget::Txid(txid(4))),
        PlanOutcome::NoSnapshot {
            oldest_snapshot: None,
            malformed_snapshots: vec![key(9, 2, 9)],
        }
    );
}

#[test]
fn a_replica_with_only_level_zero_files_from_transaction_one_plans_latest() {
    let files = inv(&[at(0, 1, 1), at(0, 2, 2), at(0, 3, 3)]);
    let outcome = plan_reference(&files, RestoreTarget::Latest);
    assert_eq!(
        keys(&outcome),
        vec![key(0, 1, 1), key(0, 2, 2), key(0, 3, 3)]
    );
}

#[test]
fn a_level_one_file_from_transaction_one_is_a_base() {
    let files = inv(&[at(1, 1, 30), at(0, 31, 31), at(0, 32, 32)]);
    let outcome = plan_reference(&files, RestoreTarget::Latest);
    assert_eq!(
        keys(&outcome),
        vec![key(1, 1, 30), key(0, 31, 31), key(0, 32, 32)]
    );
    let PlanOutcome::Plan(chain) = outcome else {
        panic!("expected a plan");
    };
    assert_eq!(chain.snapshot().key, key(1, 1, 30));
}

#[test]
fn an_instant_older_than_the_first_level_nine_file_plans_from_level_zero() {
    let files = inv(&[
        at(0, 1, 1),
        at(0, 2, 2),
        at(0, 3, 3),
        at(0, 4, 4),
        at(9, 1, 4),
    ]);
    let instant = RestoreTarget::AsOf(Timestamp::from_unix_millis(ms(1) + 1));
    assert_eq!(keys(&plan_reference(&files, instant)), vec![key(0, 1, 1)]);
}

#[test]
fn the_further_reaching_base_wins_and_a_tie_goes_to_the_higher_level() {
    let files = inv(&[at(9, 1, 5), at(2, 1, 8), at(0, 9, 9)]);
    let outcome = plan_reference(&files, RestoreTarget::Latest);
    assert_eq!(keys(&outcome), vec![key(2, 1, 8), key(0, 9, 9)]);

    let tied = inv(&[at(9, 1, 5), at(2, 1, 5)]);
    assert_eq!(
        keys(&plan_reference(&tied, RestoreTarget::Latest)),
        vec![key(9, 1, 5)]
    );
}

#[test]
fn a_replica_whose_lowest_file_begins_at_two_has_no_snapshot() {
    let files = inv(&[at(0, 2, 2), at(1, 2, 4)]);
    assert_eq!(
        plan_reference(&files, RestoreTarget::Latest),
        PlanOutcome::NoSnapshot {
            oldest_snapshot: None,
            malformed_snapshots: Vec::new(),
        }
    );
}

#[test]
fn the_oldest_snapshot_counts_files_of_every_level_that_begin_at_one() {
    let files = inv(&[at(0, 1, 1), at(9, 1, 5), at(0, 2, 2)]);
    let before_all = RestoreTarget::AsOf(Timestamp::from_unix_millis(10));
    assert_eq!(
        plan_reference(&files, before_all),
        PlanOutcome::NoSnapshot {
            oldest_snapshot: Some(txid(1)),
            malformed_snapshots: Vec::new(),
        }
    );
}

#[test]
fn a_level_nine_file_not_starting_at_one_is_never_a_snapshot() {
    let files = inv(&[at(9, 2, 5), at(0, 2, 2)]);
    assert_eq!(
        plan_reference(&files, RestoreTarget::Latest),
        PlanOutcome::NoSnapshot {
            oldest_snapshot: None,
            malformed_snapshots: vec![key(9, 2, 5)],
        }
    );
}

#[test]
fn history_only_in_a_refused_snapshot_is_reported_not_dropped() {
    let files = inv(&[at(9, 1, 3), at(0, 4, 4), at(9, 2, 9)]);
    assert_eq!(
        plan_reference(&files, RestoreTarget::Latest),
        PlanOutcome::MalformedSnapshot {
            have_through: txid(4),
            file: key(9, 2, 9),
        }
    );
}

/// A refused snapshot past a hole is still the refused file, not a gap:
/// only a usable file marks where history resumes.
#[test]
fn a_refused_snapshot_past_a_hole_is_reported_as_refused() {
    let files = inv(&[at(9, 1, 3), at(9, 5, 9)]);
    let refused = PlanOutcome::MalformedSnapshot {
        have_through: txid(3),
        file: key(9, 5, 9),
    };
    assert_eq!(plan(&files, KeyTarget::Latest), refused);
    assert_eq!(plan_reference(&files, RestoreTarget::Latest), refused);
}

#[test]
fn a_transaction_beyond_the_newest_is_its_own_outcome() {
    let files = inv(&[at(9, 1, 3), at(0, 4, 4)]);
    assert_eq!(
        plan_reference(&files, RestoreTarget::Txid(txid(5))),
        PlanOutcome::BeyondNewest {
            requested: txid(5),
            newest: txid(4),
        }
    );
}

#[test]
fn a_transaction_past_the_hole_is_a_gap_not_beyond() {
    let files = inv(&[at(9, 1, 3), at(0, 20, 30)]);
    assert_eq!(
        plan_reference(&files, RestoreTarget::Txid(txid(25))),
        gap(3, 20)
    );
}

#[test]
fn an_instant_inside_a_compacted_file_is_its_own_outcome() {
    // L1 4-10 is recorded as created with its newest input's time; the L0 files are gone.
    let files = [file(9, 1, 3, 300), file(1, 4, 10, 1000)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
    );
    let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
        panic!("expected the inside-file outcome, got {outcome:?}");
    };
    assert_eq!(chain.reaches(), txid(3));
    assert_eq!(file, key(1, 4, 10));
    // At or after the file's created-at time the file is eligible and it is a plan.
    let after = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(1001)),
    );
    assert_eq!(keys(&after), vec![key(9, 1, 3), key(1, 4, 10)]);
}

#[test]
fn a_snapshot_can_straddle_the_instant_too() {
    let files = [file(9, 1, 3, 300), file(9, 1, 10, 1000)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
    );
    let PlanOutcome::AsOfInsideFile { file, .. } = outcome else {
        panic!("expected the inside-file outcome, got {outcome:?}");
    };
    assert_eq!(file, key(9, 1, 10));
}

#[test]
fn a_refused_snapshot_is_not_a_straddler() {
    let files = [file(9, 1, 3, 300), file(9, 2, 10, 1000)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
    );
    // Not AsOfInsideFile; the history past the chain is unusable, which is
    // what Latest reports for the same listing.
    assert_eq!(
        outcome,
        PlanOutcome::MalformedSnapshot {
            have_through: txid(3),
            file: key(9, 2, 10)
        }
    );
}

#[test]
fn a_file_that_pins_the_next_transaction_removes_the_doubt() {
    // L0 5 is recorded as created 500, after the instant, so transaction 5 happened
    // after it and so did everything the big L1 file adds.
    let files = [
        file(9, 1, 3, 300),
        file(0, 4, 4, 400),
        file(0, 5, 5, 500),
        file(1, 4, 9, 900),
    ];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(450)),
    );
    assert_eq!(keys(&outcome), vec![key(9, 1, 3), key(0, 4, 4)]);
}

#[test]
fn a_later_snapshot_ending_right_after_the_chain_does_not_date_that_transaction() {
    // L9 1-2 ends one past the chain, but a snapshot is recorded as created when it is
    // written, long after transaction 2 committed, so it says nothing about
    // when 2 committed. The instant stays inside a file, never a plan: the
    // nearest-ending straddler is the snapshot itself.
    let files = [file(0, 1, 1, 100), file(9, 1, 2, 1000), file(1, 2, 5, 500)];
    let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(300));
    let outcome = plan_reference(&inv(&files), target);
    let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
        panic!("expected AsOfInsideFile, got {outcome:?}");
    };
    assert_eq!(chain.reaches(), txid(1));
    assert_eq!(file, key(9, 1, 2));
    let without = plan_reference(&inv(&[files[0], files[2]]), target);
    let PlanOutcome::AsOfInsideFile { file, .. } = without else {
        panic!("expected AsOfInsideFile, got {without:?}");
    };
    assert_eq!(file, key(1, 2, 5));
}

/// A file whose store lost its created-at time, so the listing carries the
/// object's last-modified time ([`CreatedAt::fallback`]): its upload, not
/// its newest commit.
fn fallback(level: u8, min: u64, max: u64, uploaded: i64) -> FileInfo {
    FileInfo {
        listed: ListedTime::Created(CreatedAt::fallback(Timestamp::from_unix_millis(uploaded))),
        ..file(level, min, max, 0)
    }
}

#[test]
fn a_fallback_created_at_does_not_date_the_next_transaction() {
    // Transaction 3 reached level 0 at 300; the level-1 file that holds it was
    // uploaded at 900 by a writer that stored no created-at time. 900 is when
    // the object was written, not when 3 committed, so the instant 350 may
    // fall after 3: a plan to 2 would silently drop it.
    let files = [
        file(9, 1, 1, 100),
        file(0, 2, 2, 200),
        fallback(1, 2, 3, 900),
    ];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(350)),
    );
    let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
        panic!("expected AsOfInsideFile, got {outcome:?}");
    };
    assert_eq!(chain.reaches(), txid(2));
    assert_eq!(file, key(1, 2, 3));
    // The same file with its created-at time stored dates transaction 3.
    let stored = [files[0], files[1], at(1, 2, 3)];
    let outcome = plan_reference(
        &inv(&stored),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(250)),
    );
    assert_eq!(keys(&outcome), vec![key(9, 1, 1), key(0, 2, 2)]);
}

#[test]
fn a_fallback_created_at_before_the_instant_still_admits_the_file() {
    // An upload time before the instant bounds the commit before it too.
    let files = [file(9, 1, 1, 100), fallback(1, 2, 3, 300)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(350)),
    );
    assert_eq!(keys(&outcome), vec![key(9, 1, 1), key(1, 2, 3)]);
}

#[test]
fn a_snapshot_ending_right_after_the_chain_is_the_straddler_alone() {
    // L9 1-2 is written at 1000; transaction 2 may have committed before the
    // instant 300 or after it. Nothing else dates 2, so the chain at 1 is
    // not certified.
    let files = [file(0, 1, 1, 100), file(9, 1, 2, 1000)];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(300)),
    );
    let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
        panic!("expected AsOfInsideFile, got {outcome:?}");
    };
    assert_eq!(chain.reaches(), txid(1));
    assert_eq!(file, key(9, 1, 2));
}

#[test]
fn a_stray_past_a_straddling_file_is_still_inside_file() {
    // L0 20 is recorded as created before the instant but does not connect (a stray is
    // ignored, full stop), and L1 4-9 links, so the replica continues. The
    // outcome is the one without the stray: the state at 3, inside L1 4-9.
    let files = [
        file(9, 1, 3, 300),
        file(1, 4, 9, 1000),
        file(0, 20, 20, 500),
    ];
    let outcome = plan_reference(
        &inv(&files),
        RestoreTarget::AsOf(Timestamp::from_unix_millis(950)),
    );
    let PlanOutcome::AsOfInsideFile { chain, file } = outcome else {
        panic!("expected AsOfInsideFile, got {outcome:?}");
    };
    assert_eq!(chain.reaches(), txid(3));
    assert_eq!(file, key(1, 4, 9));
}

#[test]
fn the_other_targets_never_say_inside_file_for_a_timestamp() {
    let files = [file(9, 1, 3, 300), file(1, 4, 10, 1000)];
    for target in [RestoreTarget::Latest, RestoreTarget::Txid(txid(10))] {
        assert_eq!(
            keys(&plan_reference(&inv(&files), target)),
            vec![key(9, 1, 3), key(1, 4, 10)]
        );
    }
}

// The generated histories. Their oracle is the restore-plan spec (`lsr-spec`,
// through `crate::spec_view`), which cannot reach the planner's code.

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Made {
    level: u8,
    min: u64,
    max: u64,
    created: i64,
}

impl Made {
    fn info(self) -> FileInfo {
        file(self.level, self.min, self.max, self.created)
    }
}

#[derive(Clone, Copy, Debug)]
enum Goal {
    Latest,
    Txid(u64),
    AsOf(i64),
}

impl Goal {
    fn target(self) -> RestoreTarget {
        match self {
            Self::Latest => RestoreTarget::Latest,
            Self::Txid(n) => RestoreTarget::Txid(txid(n)),
            Self::AsOf(t) => RestoreTarget::AsOf(Timestamp::from_unix_millis(t)),
        }
    }
}

/// One generated replica: [`n`] transactions, compactions, snapshots, then a
/// random subset of files deleted. Ids start at [`base`] plus one, so some
/// histories sit against `Txid::MAX`.
#[derive(Clone, Debug)]
struct History {
    files: Vec<Made>,
    base: u64,
}

fn history() -> impl Strategy<Value = History> {
    // A compacted file is recorded as created long after its range, so an instant can
    // land inside it: the jitter reaches past the next transactions.
    let compaction = (1u8..=8, 1u64..=10, 1u64..=10, 0i64..1500);
    let snapshot = (1u64..=10, prop::bool::weighted(0.15), 1u64..=10, 0i64..1500);
    (
        prop_oneof![Just(0u64), Just(u64::MAX - 10)],
        1u64..=10,
        prop::collection::vec(compaction, 0..8),
        prop::collection::vec(snapshot, 0..4),
        prop::collection::vec(prop::bool::weighted(0.3), 40),
    )
        .prop_map(|(base, n, compactions, snapshots, deletions)| {
            let id = |idx: u64| base.saturating_add(idx);
            let mut files = Vec::new();
            for txn in 1..=n {
                files.push(Made {
                    level: 0,
                    min: id(txn),
                    max: id(txn),
                    created: ms(txn),
                });
            }
            for (level, a, b, jitter) in compactions {
                let (min, max) = (a.min(b).min(n), a.max(b).min(n));
                files.push(Made {
                    level,
                    min: id(min),
                    max: id(max),
                    created: ms(max).saturating_add(jitter),
                });
            }
            for (end, malformed, start, jitter) in snapshots {
                let max = end.min(n);
                let min = if malformed { id(start.min(max)) } else { 1 };
                files.push(Made {
                    level: 9,
                    min,
                    max: id(max),
                    created: ms(max).saturating_add(jitter),
                });
            }
            let mut seen = HashSet::new();
            files.retain(|s| seen.insert((s.level, s.min, s.max)));
            let mut flags = deletions.into_iter();
            files.retain(|_| !flags.next().unwrap_or(false));
            History { files, base }
        })
}

fn goal(base: u64) -> impl Strategy<Value = Goal> {
    prop_oneof![
        Just(Goal::Latest),
        (1u64..=10).prop_map(move |d| Goal::Txid(base.saturating_add(d))),
        (-50i64..=2600).prop_map(Goal::AsOf),
    ]
}

fn case() -> impl Strategy<Value = (History, Goal)> {
    history().prop_flat_map(|history| {
        let base = history.base;
        (Just(history), goal(base))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]
    #[test]
    fn as_of_never_plans_across_a_hole_latest_reports(history in history(), t in -50i64..=2600) {
        let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
        let inventory = Inventory::from_files(infos).unwrap();
        let reach = match plan_reference(&inventory, RestoreTarget::AsOf(Timestamp::from_unix_millis(t))) {
            PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. } => chain.reaches(),
            PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::Gap { .. }
            | PlanOutcome::InsideFile { .. }
            | PlanOutcome::MalformedSnapshot { .. } => return Ok(()),
        };
        match plan_reference(&inventory, RestoreTarget::Latest) {
            PlanOutcome::Gap { have_through, .. }
            | PlanOutcome::MalformedSnapshot { have_through, .. } => {
                prop_assert!(have_through > reach, "planned at {reach}, Latest stops at {have_through}");
            }
            PlanOutcome::Plan(_)
            | PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::AsOfInsideFile { .. }
            | PlanOutcome::InsideFile { .. } => {}
        }
    }

    #[test]
    fn as_of_never_reaches_further_than_latest_and_applies_only_older_files(
        history in history(),
        t in -50i64..=2600,
    ) {
        let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
        let inventory = Inventory::from_files(infos).unwrap();
        let (PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. }) =
            plan_reference(&inventory, RestoreTarget::AsOf(Timestamp::from_unix_millis(t)))
        else {
            return Ok(());
        };
        for info in chain.files() {
            prop_assert!(info.created_at().expect("an exact created-at time").at() < Timestamp::from_unix_millis(t), "{info:?} is not older than {t}");
        }
        if let PlanOutcome::Plan(latest) = plan_reference(&inventory, RestoreTarget::Latest) {
            prop_assert!(chain.reaches() <= latest.reaches());
        }
    }

    /// Strays are unobservable: when a usable file links past the reach, the
    /// outcome is the same with every eligible file that does not connect to
    /// the chain removed. (Without a linking file, the files past the reach
    /// are the hole rule's evidence, so removing them would change the answer.)
    #[test]
    fn removing_every_stray_leaves_a_continuing_outcome_unchanged(
        history in history(),
        t in -50i64..=2600,
    ) {
        let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
        let inventory = Inventory::from_files(infos.clone()).unwrap();
        let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(t));
        let outcome = plan_reference(&inventory, target);
        let (PlanOutcome::Plan(chain) | PlanOutcome::AsOfInsideFile { chain, .. }) = &outcome else {
            return Ok(());
        };
        let reach = chain.reaches().get();
        let continues = infos.iter().any(|i| spec_view::key(i.key).passes(reach));
        if !continues {
            return Ok(());
        }
        let instant = Timestamp::from_unix_millis(t);
        let kept: Vec<FileInfo> = infos
            .into_iter()
            .filter(|i| {
                let key = spec_view::key(i.key);
                let stray = i.created_at().expect("an exact created-at time").at() < instant
                    && !key.links(reach)
                    && key.max > reach;
                !stray
            })
            .collect();
        let without = plan_reference(&Inventory::from_files(kept).unwrap(), target);
        prop_assert_eq!(without, outcome);
    }

    #[test]
    fn plan_is_what_the_contract_allows(
        (history, goal) in case(),
        order in prop::collection::vec(any::<u32>(), 40),
    ) {
        let infos: Vec<FileInfo> = history.files.iter().map(|s| s.info()).collect();
        let inventory = Inventory::from_files(infos.clone()).unwrap();
        let outcome = plan_reference(&inventory, goal.target());
        let judgement = spec_view::judge(&inventory, goal.target()).expect("the spec takes every inventory");
        prop_assert!(!judgement.allowed.is_empty(), "the contract is silent: {:?}", judgement.by);
        prop_assert!(
            judgement.admits(&spec_view::outcome(&outcome)),
            "{:?}: the contract allows {:?} ({:?})",
            outcome,
            judgement.allowed,
            judgement.by
        );
        // A real permutation of the input (keys are random sort keys): the
        // inventory is the same value and so is the plan.
        let mut shuffled: Vec<(u32, FileInfo)> = order
            .iter()
            .copied()
            .cycle()
            .zip(infos)
            .collect();
        shuffled.sort_by_key(|(rank, _)| *rank);
        let shuffled = Inventory::from_files(shuffled.into_iter().map(|(_, info)| info)).unwrap();
        prop_assert_eq!(&shuffled, &inventory);
        prop_assert_eq!(plan_reference(&shuffled, goal.target()), outcome);
    }
}

/// The `verify` re-check accepts what the planner builds and rejects a chain
/// with a gap, which no other test could build through `plan`.
#[test]
fn verify_recheck_rejects_a_chain_with_a_gap() {
    let listing = inv(&[at(9, 1, 3), at(0, 4, 6)]);
    let PlanOutcome::Plan(chain) = plan(&listing, KeyTarget::Latest) else {
        panic!("expected a plan");
    };
    assert!(chain.is_contiguous(&listing));
    let mut broken = chain.clone();
    broken.files.push(at(0, 9, 12));
    broken.reaches = txid(12);
    assert!(!broken.is_contiguous(&listing));
}

/// The re-check also refuses a chain that is contiguous but names a file the
/// listing does not hold: each half of the check must be able to fail alone.
#[test]
fn verify_recheck_rejects_a_contiguous_chain_with_an_unlisted_file() {
    let listing = inv(&[at(9, 1, 3), at(0, 4, 6)]);
    let PlanOutcome::Plan(chain) = plan(&listing, KeyTarget::Latest) else {
        panic!("expected a plan");
    };
    let mut extended = chain.clone();
    extended.files.push(at(0, 7, 9));
    extended.reaches = txid(9);
    assert!(!extended.is_contiguous(&listing));
    assert!(extended.is_contiguous(&inv(&[at(9, 1, 3), at(0, 4, 6), at(0, 7, 9)])));
}
