//! The monotonic rule a record must keep over the one before it.

use lsr_journal::{Floor, Journal, Lowered, ReplicaId, Unproven, WouldLower};
use lsr_ltx::Txid;

fn floor(replica: u8, bound: u64) -> Option<Floor> {
    Some(Floor::new(
        ReplicaId([replica; 8]),
        Txid::new(bound).unwrap(),
        None,
    ))
}

fn with(floor: Option<Floor>, unproven: Unproven) -> Journal {
    Journal {
        floor,
        unproven,
        ..Journal::ABSENT
    }
}

#[test]
fn the_same_destination_s_bound_never_falls() {
    let old = with(floor(1, 5), Unproven::No);
    assert_eq!(old.monotonic(&with(floor(1, 5), Unproven::No)), Ok(()));
    assert_eq!(old.monotonic(&with(floor(1, 6), Unproven::No)), Ok(()));
    assert_eq!(
        old.monotonic(&with(floor(1, 4), Unproven::No)),
        Err(Lowered::Floor(WouldLower {
            from: Txid::new(5).unwrap(),
            to: Txid::new(4).unwrap()
        }))
    );
}

#[test]
fn another_destination_starts_again() {
    let old = with(floor(1, 5), Unproven::No);
    assert_eq!(old.monotonic(&with(floor(2, 1), Unproven::No)), Ok(()));
}

#[test]
fn a_floor_is_never_dropped() {
    let old = with(floor(1, 5), Unproven::No);
    assert_eq!(
        old.monotonic(&with(None, Unproven::No)),
        Err(Lowered::FloorDropped)
    );
}

#[test]
fn the_debt_is_never_forgiven() {
    use Unproven::{No, Snapshot, Truncate};
    for (from, to, ok) in [
        (No, No, true),
        (No, Truncate, true),
        (No, Snapshot, false),
        (Truncate, Truncate, true),
        (Truncate, Snapshot, true),
        (Truncate, No, false),
        (Snapshot, Snapshot, true),
        (Snapshot, No, true),
        // A new WAL fsync failure while the paying snapshot is still owed.
        (Snapshot, Truncate, true),
    ] {
        let got = with(None, from).monotonic(&with(None, to));
        assert_eq!(got.is_ok(), ok, "{from:?} -> {to:?}");
    }
}

#[test]
fn withdrawing_the_anchor_keeps_the_bound() {
    let anchored = floor(1, 5).unwrap();
    assert_eq!(anchored.withdraw_anchor().bound(), anchored.bound());
    assert_eq!(anchored.withdraw_anchor().anchor(), None);
}
