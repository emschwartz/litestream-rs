//! The page map: newest frame wins, shrinking drops pages, order is enforced.

use std::num::NonZeroU32;

use lsr_wal::{PageMap, PageMapError, ScanLimit};

use crate::common::{Wal, open, scan, size};

const PS: u32 = 512;
const SALTS: (u32, u32) = (1, 2);

fn map_of(wal: &Wal) -> PageMap {
    let mut map = PageMap::new(size(PS));
    for tx in &scan(&wal.bytes, ScanLimit::FirstInvalidFrame).txs {
        map.apply(tx).unwrap();
    }
    map
}

fn listing(map: &PageMap) -> Vec<(u32, u32)> {
    map.pages()
        .map(|f| (f.pgno().get(), f.offset().frame_index()))
        .collect()
}

#[test]
fn an_empty_map_spans_nothing() {
    let map = PageMap::new(size(PS));
    assert_eq!(map.commit(), None);
    assert_eq!(map.pages().count(), 0);
    let (start, end) = map.range();
    assert_eq!(start, end);
    assert_eq!(map.page_size(), size(PS));
}

#[test]
fn the_newest_frame_of_a_page_wins() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1, 2], 2, 0);
    wal.tx(&[2, 3], 3, 0);
    wal.tx(&[2], 3, 0);
    let map = map_of(&wal);
    assert_eq!(listing(&map), [(1, 0), (2, 4), (3, 3)]);
    assert_eq!(map.commit(), NonZeroU32::new(3));
    let (start, end) = map.range();
    assert_eq!((start.frame_index(), end.frame_index()), (0, 5));
    assert!(map.get(map.pages().next().unwrap().pgno()).is_some());
}

#[test]
fn shrinking_drops_pages_above_the_new_size_including_earlier_ones() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1, 2, 3, 4], 4, 0);
    wal.tx(&[1], 2, 0); // the database shrinks to two pages
    let map = map_of(&wal);
    assert_eq!(listing(&map), [(1, 4), (2, 1)]);
    assert_eq!(map.commit(), NonZeroU32::new(2));
}

#[test]
fn a_page_above_its_own_commit_is_dropped() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1, 3], 2, 0);
    assert_eq!(listing(&map_of(&wal)), [(1, 0)]);
}

#[test]
fn a_page_that_returns_after_shrinking_comes_from_its_new_frame() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1, 2, 3], 3, 0);
    wal.tx(&[1], 1, 0);
    wal.tx(&[1, 2, 3], 3, 0);
    assert_eq!(listing(&map_of(&wal)), [(1, 4), (2, 5), (3, 6)]);
}

#[test]
fn the_range_starts_at_the_first_frame_applied() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1], 1, 0);
    wal.tx(&[2], 2, 0);
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut map = PageMap::new(size(PS));
    map.apply(&scanned.txs[1]).unwrap();
    assert_eq!(map.range().0.frame_index(), 1);
}

#[test]
fn page_map_page_size_mismatch() {
    let mut wal = Wal::new(1024, false, SALTS);
    wal.tx(&[1], 1, 0);
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let err = PageMap::new(size(PS)).apply(&scanned.txs[0]).unwrap_err();
    assert_eq!(
        err,
        PageMapError::PageSizeMismatch {
            expected: size(PS),
            found: size(1024)
        }
    );
}

#[test]
fn page_map_not_after_end() {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1], 1, 0);
    wal.tx(&[2], 2, 0);
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut map = PageMap::new(size(PS));
    map.apply(&scanned.txs[1]).unwrap();
    let err = map.apply(&scanned.txs[0]).unwrap_err();
    assert!(matches!(err, PageMapError::NotAfterEnd { .. }), "{err}");
    assert_eq!(open(&wal.bytes).header().page_size(), size(PS));
}
