use std::num::NonZeroU32;

use lsr_ltx::{PageChecksum, PageNo, PageSize, Timestamp, Txid};
use lsr_wal::{Salts, WalOffset, WalReader};

use crate::source::{FakeWal, WalSource};

use super::*;

fn ps() -> PageSize {
    PageSize::new(4096).unwrap()
}

#[test]
fn a_file_covers_the_synced_end_exactly() {
    let end = WalOffset::from_frame_index(3, ps());
    let synced = SyncedOffset::new(end);
    assert!(WalFileLen::new(end.get()).covers(synced));
    assert!(WalFileLen::new(end.get() + 1).covers(synced));
}

#[test]
fn a_file_one_byte_short_does_not_cover() {
    let end = WalOffset::from_frame_index(3, ps());
    assert!(!WalFileLen::new(end.get() - 1).covers(SyncedOffset::new(end)));
}

#[test]
fn a_longer_file_than_synced_is_not_the_synced_offset() {
    // Go's bug #997 compared these as one integer type; here only covers() asks.
    let synced = SyncedOffset::new(WalOffset::from_frame_index(1, ps()));
    let file = WalFileLen::new(synced.offset().get() * 10);
    assert!(file.covers(synced));
    assert_eq!(file.bytes(), synced.offset().get() * 10);
}

#[test]
fn epochs_count_up_within_one_holder() {
    let first = ReadLockHold::first();
    assert_eq!(first.count(), 1);
    assert_eq!(first.next().count(), 2);
    assert_ne!(first.next(), first);
    assert_eq!(first.next(), first.next());
}

fn position(continuity: Continuity) -> Position {
    let mut fake = FakeWal::new(ps(), Salts { salt1: 1, salt2: 2 });
    let reader = WalReader::open(fake.open().unwrap().unwrap()).unwrap();
    Position::new(
        Txid::new(5).unwrap(),
        reader.cursor_at_start(),
        NonZeroU32::MIN,
        LastPage::new(
            PageNo::new(2, ps()).unwrap(),
            PageChecksum::of(PageNo::new(2, ps()).unwrap(), &[]),
        ),
        Timestamp::from_unix_millis(9),
        continuity,
    )
}

/// Two opens each start at their first hold, and the two are never equal: a
/// read lock from one cannot pass for a read lock under the other.
#[test]
fn the_first_epochs_of_two_holders_differ() {
    let (one, two) = (ReadLockHold::first(), ReadLockHold::first());
    assert_eq!(one.count(), two.count());
    assert_ne!(one, two);
    assert_ne!(one.next(), two.next());
}

/// Two fakes standing in for two opens of one database.
#[test]
fn two_sources_never_report_the_same_epoch() {
    let salts = Salts { salt1: 1, salt2: 2 };
    let (one, two) = (FakeWal::new(ps(), salts), FakeWal::new(ps(), salts));
    assert_ne!(one.epoch(), two.epoch());
}

#[test]
fn a_position_takes_its_synced_end_and_page_size_from_its_cursor() {
    let at = position(Continuity::Broken);
    assert_eq!(at.synced().offset(), at.cursor().next());
    assert_eq!(at.page_size(), ps());
    assert_eq!(at.txid(), Txid::new(5).unwrap());
}

#[test]
fn replacing_continuity_changes_nothing_else() {
    let epoch = ReadLockHold::first().next().next();
    let pinned = position(Continuity::Broken).with_continuity(Continuity::Held(epoch));
    assert_eq!(pinned.continuity(), Continuity::Held(epoch));
    assert_eq!(
        pinned.with_continuity(Continuity::Broken),
        position(Continuity::Broken)
    );
}

/// A step's end is a proof only once the epoch is read again and is the same:
/// then the position is pinned to it.
#[test]
fn a_reached_end_is_proven_only_at_the_epoch_it_was_reached_in() {
    let epoch = ReadLockHold::first();
    let wal = crate::source::FakeWal::new(
        lsr_ltx::PageSize::MIN,
        lsr_wal::Salts { salt1: 1, salt2: 2 },
    );
    wal.set_epoch(epoch);
    let at = position(Continuity::Held(ReadLockHold::first()));
    let reached = ReachedEnd::new(at, epoch);
    let unpinned = at.with_continuity(Continuity::Broken);
    assert_eq!(
        reached.position(),
        &unpinned,
        "nothing is pinned before the second read"
    );
    let proof = reached.confirm(&wal).unwrap();
    let pinned = at.with_continuity(Continuity::Held(epoch));
    assert_eq!(proof.position(), &pinned);
    assert_eq!(proof.into_position(), pinned);
}

/// The read transaction was released and retaken since the step began: no
/// proof, and the position comes back unpinned.
#[test]
fn a_reached_end_whose_epoch_moved_is_no_proof() {
    let epoch = ReadLockHold::first();
    let wal = crate::source::FakeWal::new(
        lsr_ltx::PageSize::MIN,
        lsr_wal::Salts { salt1: 1, salt2: 2 },
    );
    wal.set_epoch(epoch.next());
    let at = position(Continuity::Broken);
    let reached = ReachedEnd::new(at, epoch);
    assert_eq!(reached.confirm(&wal), Err(at));
    assert_eq!(ReachedEnd::new(at, epoch).into_position(), at);
}
