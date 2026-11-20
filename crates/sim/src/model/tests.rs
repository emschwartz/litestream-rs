use lsr_wal::Salts;

use super::*;
use crate::testkit::{PAGE, SALTS, pgno, ps, wal_end};

fn app(frames: u32) -> AppCommit {
    AppCommit {
        salts: SALTS,
        end: wal_end(frames),
    }
}

fn page(fill: u8) -> Vec<u8> {
    vec![fill; usize::try_from(PAGE).unwrap()]
}

fn byte(image: &PageImage, pgno: u32) -> Option<u8> {
    image.page(pgno).map(|p| p[0])
}

#[test]
fn an_image_reads_through_its_parents_and_survives_flattening() {
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    model.commit(&[(pgno(2), page(5)), (pgno(1), page(1))], 2, app(1), 0);
    for n in 2..100_u32 {
        model.commit(&[(pgno(1), page(u8::try_from(n).unwrap()))], 2, app(n), 0);
    }
    let tip = model.tip_image();
    assert_eq!(byte(&tip, 1), Some(99));
    assert_eq!(byte(&tip, 2), Some(5));
    assert_eq!(byte(&tip, 3), None);
    let first = model.rec(CommitId(1)).unwrap();
    assert_eq!(byte(&first.image, 1), Some(1));
}

#[test]
fn a_shrink_forgets_pages_and_growing_again_reads_zeros() {
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    model.commit(&[(pgno(1), page(1)), (pgno(2), page(2))], 2, app(1), 0);
    model.commit(&[], 1, app(2), 0);
    model.commit(&[(pgno(1), page(3))], 2, app(3), 0);
    let tip = model.tip_image();
    assert_eq!(byte(&tip, 2), Some(0));
    assert_eq!(byte(&tip, 1), Some(3));
}

#[test]
fn a_power_loss_keeps_durable_commits_and_the_synced_prefix() {
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    let c0 = model.commit(&[(pgno(1), page(1))], 1, app(1), 0);
    model.on_checkpoint();
    let c1 = model.commit(&[(pgno(1), page(2))], 1, app(2), 0);
    let c2 = model.commit(&[(pgno(1), page(3))], 1, app(3), 0);
    assert!(!model.rewound());
    model.on_power_loss(SALTS, 2);
    assert!(model.rewound());
    assert_eq!(model.tip(), Some(c1));
    assert_eq!(byte(&model.tip_image(), 1), Some(2));
    // The lost commit stays on record: a file may already cover it.
    assert!(model.rec(c2).is_some());
    assert!(model.rec(c0).unwrap().durable);
}

#[test]
fn a_power_loss_drops_commits_of_an_older_generation_that_were_never_synced() {
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    let c0 = model.commit(&[(pgno(1), page(1))], 1, app(1), 0);
    let other = Salts {
        salt1: SALTS.salt1.wrapping_add(1),
        salt2: 3,
    };
    model.on_power_loss(other, 10);
    assert_eq!(model.tip(), Some(CommitId(0)));
    assert!(model.rec(c0).is_some());
}

#[test]
fn the_database_before_any_commit_is_commit_zero() {
    let model = Model::new(PageImage::from_bytes(ps(), &[]));
    assert_eq!(model.tip(), Some(CommitId(0)));
    assert_eq!(model.find(SALTS, wal_end(0).get(), usize::MAX), None);
}

#[test]
fn find_picks_the_newest_commit_made_by_the_stamp() {
    let mut model = Model::new(PageImage::from_bytes(ps(), &[]));
    let early = model.commit(&[(pgno(1), page(1))], 1, app(1), 2);
    model.on_power_loss(SALTS, 0);
    let late = model.commit(&[(pgno(1), page(9))], 1, app(1), 7);
    let end = wal_end(1).get();
    assert_eq!(model.find(SALTS, end, 1), None);
    assert_eq!(model.find(SALTS, end, 5), Some(early));
    assert_eq!(model.find(SALTS, end, 9), Some(late));
    assert_eq!(model.find(SALTS, end.saturating_add(1), 9), None);
}
