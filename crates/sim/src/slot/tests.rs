//! The checker can fail: what the folding makes of each thing a slot's file goes through.

use lsr_core::{FileKey, Level, WriteMode};
use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::{Counts, LEVEL, as_staged};
use crate::store::RecorderEvent;

fn key(level: u8, txid: u64) -> FileKey {
    FileKey::new(
        Level::new(level).unwrap(),
        TxidRange::single(Txid::new(txid).unwrap()),
    )
}

fn put(level: u8, txid: u64, digest: u64) -> RecorderEvent {
    let zero = Timestamp::from_unix_millis(0);
    RecorderEvent::Put {
        key: key(level, txid),
        mode: WriteMode::New,
        header: None,
        origin: None,
        created_at: zero,
        size: 1,
        digest,
        at: zero,
        high: zero,
    }
}

fn delete(keys: &[FileKey]) -> RecorderEvent {
    let zero = Timestamp::from_unix_millis(0);
    RecorderEvent::Delete {
        keys: keys.to_vec(),
        at: zero,
        high: zero,
    }
}

/// A file staged in the slot, moved up and deleted from the slot was staged once, where the
/// slot took it: the move stages nothing and the delete gives nothing up.
#[test]
fn a_file_moved_up_was_staged_when_the_slot_took_it() {
    let raw = [
        put(0, 2, 20),
        put(LEVEL, 3, 30),
        put(0, 3, 30),
        delete(&[key(LEVEL, 3)]),
    ];
    let (staged, counts) = as_staged(&raw, 0);
    assert_eq!(
        staged,
        [put(0, 2, 20), put(0, 3, 30), delete(&[]), delete(&[])]
    );
    assert_eq!(
        counts,
        Counts {
            staged: 1,
            moved_up: 1,
            given_up: 0
        }
    );
}

/// A slot deleted with no move gave its files up, each under the key it would have had.
#[test]
fn a_file_deleted_from_the_slot_unmoved_was_given_up() {
    let raw = [
        put(LEVEL, 3, 30),
        put(LEVEL, 4, 40),
        delete(&[key(LEVEL, 3), key(LEVEL, 4), key(0, 1)]),
    ];
    let (staged, counts) = as_staged(&raw, 0);
    assert_eq!(
        staged.last(),
        Some(&delete(&[key(0, 3), key(0, 4), key(0, 1)]))
    );
    assert_eq!(
        counts,
        Counts {
            staged: 2,
            moved_up: 0,
            given_up: 2
        }
    );
}

/// A level-0 put of other bytes than the slot's is no move: it is a staging of its own, and
/// the slot's file deleted after it was given up.
#[test]
fn other_bytes_at_level_zero_are_no_move() {
    let raw = [put(LEVEL, 3, 30), put(0, 3, 31), delete(&[key(LEVEL, 3)])];
    let (staged, counts) = as_staged(&raw, 0);
    assert_eq!(staged, [put(0, 3, 30), put(0, 3, 31), delete(&[key(0, 3)])]);
    assert_eq!(counts.given_up, 1);
    assert_eq!(counts.moved_up, 0);
}

/// A file staged at level 0 again after the slot's was deleted is a staging, whatever its
/// bytes: the slot no longer holds what it would have moved.
#[test]
fn a_put_after_the_slots_file_is_gone_is_a_staging() {
    let raw = [put(LEVEL, 3, 30), delete(&[key(LEVEL, 3)]), put(0, 3, 30)];
    let (staged, counts) = as_staged(&raw, 0);
    assert_eq!(staged.last(), Some(&put(0, 3, 30)));
    assert_eq!(counts.moved_up, 0);
}

/// The counts are of the events from the given one on; the folding is of them all.
#[test]
fn counts_start_where_asked() {
    let raw = [put(LEVEL, 3, 30), put(0, 3, 30), delete(&[key(LEVEL, 3)])];
    let (staged, counts) = as_staged(&raw, 1);
    assert_eq!(staged.len(), 3);
    assert_eq!(
        counts,
        Counts {
            staged: 0,
            moved_up: 1,
            given_up: 0
        }
    );
}

/// Other levels and every level-0 event of a run with no slot pass through as they are.
#[test]
fn events_outside_the_slot_pass_through() {
    let raw = [
        put(0, 1, 10),
        put(9, 1, 11),
        delete(&[key(0, 1)]),
        put(0, 1, 10),
    ];
    let (staged, counts) = as_staged(&raw, 0);
    assert_eq!(staged, raw);
    assert_eq!(counts, Counts::default());
}
