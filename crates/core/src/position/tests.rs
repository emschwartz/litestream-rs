use std::num::NonZeroU32;

use lsr_ltx::{Checksum, PageNo, PageSize, Timestamp, Txid};
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
fn epochs_count_up() {
    assert_eq!(ReadEpoch::new(4).next(), ReadEpoch::new(5));
    assert_eq!(ReadEpoch::new(u64::MAX).next().get(), u64::MAX);
    assert!(ReadEpoch::new(1) < ReadEpoch::new(2));
}

fn position(continuity: Continuity) -> Position {
    let mut fake = FakeWal::new(ps(), Salts { salt1: 1, salt2: 2 });
    let reader = WalReader::open(fake.open().unwrap().unwrap()).unwrap();
    Position::new(
        Txid::new(5).unwrap(),
        reader.cursor_at_start(),
        NonZeroU32::MIN,
        LastPage::new(PageNo::new(2, ps()).unwrap(), Checksum::EMPTY),
        Timestamp::from_unix_millis(9),
        continuity,
    )
}

#[test]
fn a_position_takes_its_synced_end_and_page_size_from_its_cursor() {
    let at = position(Continuity::Unpinned);
    assert_eq!(at.synced().offset(), at.cursor().next());
    assert_eq!(at.page_size(), ps());
    assert_eq!(at.txid(), Txid::new(5).unwrap());
}

#[test]
fn replacing_continuity_changes_nothing_else() {
    let epoch = ReadEpoch::new(3);
    let pinned = position(Continuity::Unpinned).with_continuity(Continuity::Pinned(epoch));
    assert_eq!(pinned.continuity(), Continuity::Pinned(epoch));
    assert_eq!(
        pinned.with_continuity(Continuity::Unpinned),
        position(Continuity::Unpinned)
    );
}

#[test]
fn a_through_proof_gives_back_its_position() {
    let at = position(Continuity::Pinned(ReadEpoch::new(1)));
    let proof = SyncedThrough::new(at);
    assert_eq!(proof.position(), &at);
    assert_eq!(proof.into_position(), at);
}
