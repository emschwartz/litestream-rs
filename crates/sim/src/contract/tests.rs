use lsr_core::{CreatedAt, FileKey, Level, ListedTime};
use lsr_ltx::TxidRange;

use super::*;

fn tx(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn file(level: u8, min: u64, max: u64, at: i64) -> FileInfo {
    FileInfo {
        key: FileKey::new(
            Level::new(level).unwrap(),
            TxidRange::new(tx(min), tx(max)).unwrap(),
        ),
        size: 1,
        listed: ListedTime::Created(CreatedAt::metadata(Timestamp::from_unix_millis(at))),
    }
}

fn expect_txid(files: &[FileInfo], target: u64) -> Expected {
    txid(files, &Reach::over(files, |_| true), tx(target))
}

fn at(millis: i64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

#[test]
fn a_txid_that_a_chain_reaches_plans() {
    let files = [file(0, 1, 1, 10), file(0, 2, 2, 20), file(1, 1, 2, 20)];
    assert_eq!(expect_txid(&files, 1), Expected::Plan(tx(1)));
    assert_eq!(expect_txid(&files, 2), Expected::Plan(tx(2)));
}

#[test]
fn a_txid_inside_a_compacted_file_is_inside_it() {
    let files = [file(0, 1, 1, 10), file(1, 2, 3, 30)];
    let want = Expected::InsideFile {
        reach: Some(tx(1)),
        target: tx(2),
    };
    assert_eq!(expect_txid(&files, 2), want);
    let refused = |key: FileKey| {
        Answer::Refused(PlanOutcome::InsideFile {
            requested: tx(2),
            file: key,
        })
    };
    assert!(allows(&want, &files, &refused(files[1].key)));
    // The base does not pass the target.
    assert!(!allows(&want, &files, &refused(files[0].key)));
}

#[test]
fn a_txid_below_the_only_base_is_inside_it() {
    let files = [file(9, 1, 5, 50), file(0, 6, 6, 60)];
    let want = Expected::InsideFile {
        reach: None,
        target: tx(3),
    };
    assert_eq!(expect_txid(&files, 3), want);
    assert_eq!(expect_txid(&files, 6), Expected::Plan(tx(6)));
}

#[test]
fn no_file_at_transaction_one_is_no_snapshot() {
    let files = [file(0, 2, 2, 20)];
    assert_eq!(expect_txid(&files, 2), Expected::NoSnapshot);
}

#[test]
fn a_hole_is_a_gap() {
    let files = [file(0, 1, 1, 10), file(0, 3, 3, 30)];
    assert_eq!(expect_txid(&files, 3), Expected::Gap);
    assert_eq!(as_of(&files, at(35)), Expected::Gap);
}

#[test]
fn only_a_refused_snapshot_past_the_reach_is_malformed() {
    let files = [file(0, 1, 1, 10), file(9, 2, 3, 30)];
    assert_eq!(expect_txid(&files, 3), Expected::MalformedSnapshot);
}

#[test]
fn as_of_plans_to_the_last_file_created_before_the_instant() {
    let files = [file(0, 1, 1, 10), file(0, 2, 2, 20), file(0, 3, 3, 30)];
    assert_eq!(as_of(&files, at(5)), Expected::NoSnapshot);
    assert_eq!(as_of(&files, at(20)), Expected::Plan(tx(1)));
    assert_eq!(as_of(&files, at(25)), Expected::Plan(tx(2)));
    assert_eq!(as_of(&files, at(31)), Expected::Plan(tx(3)));
}

#[test]
fn an_instant_inside_a_compacted_file_with_its_finer_files_gone_straddles() {
    let files = [file(0, 1, 1, 10), file(1, 2, 4, 40)];
    let want = Expected::AsOfInside { reach: tx(1) };
    assert_eq!(as_of(&files, at(25)), want);
    let inside = Answer::Reached {
        txid: tx(1),
        inside: Some(files[1].key),
    };
    assert!(allows(&want, &files, &inside));
    let plain = Answer::Reached {
        txid: tx(1),
        inside: None,
    };
    assert!(!allows(&want, &files, &plain));
}

#[test]
fn a_level_zero_file_ending_right_after_the_chain_dates_it() {
    let files = [file(0, 1, 1, 10), file(1, 2, 4, 40), file(0, 2, 2, 30)];
    assert_eq!(as_of(&files, at(25)), Expected::Plan(tx(1)));
}

#[test]
fn a_snapshot_alone_past_the_chain_straddles() {
    let files = [file(0, 1, 1, 10), file(9, 1, 4, 40)];
    let want = Expected::AsOfInside { reach: tx(1) };
    assert_eq!(as_of(&files, at(25)), want);
    let plain = Answer::Reached {
        txid: tx(1),
        inside: None,
    };
    assert!(!allows(&want, &files, &plain));
}

#[test]
fn a_snapshot_ending_right_after_the_chain_straddles() {
    let files = [file(0, 1, 1, 10), file(9, 1, 2, 40)];
    let want = Expected::AsOfInside { reach: tx(1) };
    assert_eq!(as_of(&files, at(25)), want);
    let inside = Answer::Reached {
        txid: tx(1),
        inside: Some(files[1].key),
    };
    assert!(allows(&want, &files, &inside));
}

#[test]
fn a_fallback_created_at_does_not_date_the_next_transaction() {
    let mut fell_back = file(1, 2, 2, 40);
    fell_back.listed = ListedTime::Created(CreatedAt::fallback(at(40)));
    let files = [file(0, 1, 1, 10), fell_back];
    assert_eq!(as_of(&files, at(25)), Expected::AsOfInside { reach: tx(1) });
    // Stored, the same time dates transaction 2.
    let files = [file(0, 1, 1, 10), file(1, 2, 2, 40)];
    assert_eq!(as_of(&files, at(25)), Expected::Plan(tx(1)));
}

#[test]
fn a_later_answer_is_never_allowed() {
    let files = [file(0, 1, 1, 10), file(0, 2, 2, 20)];
    let later = Answer::Reached {
        txid: tx(2),
        inside: None,
    };
    assert!(!allows(&as_of(&files, at(15)), &files, &later));
}
