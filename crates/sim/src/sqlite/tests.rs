use lsr_wal::ReadPageError;
use proptest::prelude::*;

use super::*;

const PS: u32 = 512;

fn ps() -> PageSize {
    PageSize::new(PS).unwrap()
}

fn pgno(n: u32) -> PageNo {
    PageNo::new(n, ps()).unwrap()
}

fn tx(pgnos: &[u32], fill: u8) -> Vec<(PageNo, Vec<u8>)> {
    pgnos.iter().map(|n| (pgno(*n), vec![fill; 512])).collect()
}

fn sim() -> SimSqlite {
    SimSqlite::new(ps(), 4, 7)
}

/// Every committed transaction, read back through `SimSource` up to the committed end.
fn scan_committed(source: &mut SimSource) -> u32 {
    let end = source.committed().unwrap().end();
    if end.frame_count() == 0 {
        return 0;
    }
    let mut reader = WalReader::open(source.open().unwrap().unwrap()).unwrap();
    let mut cursor = reader.cursor_at_start();
    while let TxOutcome::Tx(tx) = reader.next_tx(&cursor, ScanLimit::Committed(end)).unwrap() {
        cursor = *tx.end();
    }
    cursor.next().frame_index()
}

#[derive(Clone, Debug)]
enum Step {
    Commit(Vec<u32>, u8),
    Spill(u32),
    Rollback,
    Reader(bool),
    Ours(bool),
    Checkpoint(StrangerMode),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (prop::collection::vec(1_u32..9, 1..4), any::<u8>()).prop_map(|(p, f)| Step::Commit(p, f)),
        1 => (1_u32..9).prop_map(Step::Spill),
        1 => Just(Step::Rollback),
        1 => any::<bool>().prop_map(Step::Reader),
        1 => any::<bool>().prop_map(Step::Ours),
        1 => Just(Step::Checkpoint(StrangerMode::Passive)),
        1 => Just(Step::Checkpoint(StrangerMode::Restart)),
        1 => Just(Step::Checkpoint(StrangerMode::Truncate)),
    ]
}

/// Runs `steps`; returns the image after each commit and the first one a power crash must keep.
fn history(db: &SimSqlite, steps: Vec<Step>) -> (Vec<Vec<u8>>, usize) {
    let mut images = vec![db.image()];
    let mut floor = 0;
    let mut reader = None;
    for step in steps {
        match step {
            Step::Commit(pages, fill) => {
                // A commit that grows the database writes every page it adds, as SQLite's
                // does (`sqlite_rules::a_growing_commit_writes_every_page_it_adds`). Without
                // them a page above an earlier shrink would read an old frame that no
                // checkpoint copies.
                let old = db.db_size();
                let mut pages = pages;
                pages.extend(old.saturating_add(1)..=pages.iter().copied().max().unwrap());
                pages.sort_unstable();
                pages.dedup();
                let size = old.max(*pages.last().unwrap());
                if db.commit(&tx(&pages, fill), size).is_some() {
                    images.push(db.image());
                }
            }
            Step::Spill(n) => {
                db.spill(&tx(&[n], 0xee));
            }
            Step::Rollback => {
                db.rollback();
            }
            Step::Reader(true) => reader = reader.or_else(|| Some(db.stranger_reader_begin())),
            Step::Reader(false) => {
                reader.take().map(|id| db.stranger_reader_end(id));
            }
            Step::Ours(true) => {
                db.our_reader_begin();
            }
            Step::Ours(false) => {
                db.our_reader_end();
            }
            Step::Checkpoint(mode) => {
                let before = (db.backfilled(), db.mx_frame());
                if let Some(run) = db.stranger_checkpoint(mode)
                    && (run.checkpointed > before.0 || (run.log == 0 && before.1 > 0))
                {
                    floor = images.len().saturating_sub(1);
                }
            }
        }
    }
    (images, floor)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn power_crash_keeps_checkpointed_pages(steps in prop::collection::vec(step(), 0..40)) {
        let db = sim();
        let (images, floor) = history(&db, steps);
        db.crash(CrashKind::Power);
        let after = db.image();
        let kept = images.iter().rposition(|image| *image == after);
        prop_assert!(kept.is_some_and(|at| at >= floor), "kept {kept:?}, floor {floor}");
        prop_assert_eq!(db.mx_frame(), scan_committed(&mut db.source()));
    }

    #[test]
    fn sources_pass_conformance(steps in prop::collection::vec(step(), 0..40)) {
        let db = sim();
        history(&db, steps);
        let mut source = db.source();
        let read = source.committed().unwrap();
        prop_assert!(read.backfilled() <= read.end().frame_count());
        prop_assert_eq!(scan_committed(&mut source), read.end().frame_count());
        let len = db.wal_file().map(|wal| WalFileLen::new(u64::try_from(wal.len()).unwrap()));
        prop_assert_eq!(source.len().unwrap(), len);
        let mut pages = db.pages();
        let count = pages.page_count().unwrap();
        prop_assert_eq!(u64::from(count).saturating_mul(u64::from(PS)), u64::try_from(db.db_file().len()).unwrap());
        let mut buf = vec![0; 512];
        let beyond = pgno(count.saturating_add(1));
        prop_assert_eq!(pages.read_page(beyond, &mut buf), Err(SourceError::Short { pgno: beyond }));
        let epoch = source.epoch();
        db.our_reader_begin();
        prop_assert!(db.our_reader_end());
        prop_assert_eq!(source.epoch(), epoch.next());
        let run = db.checkpointer().checkpoint(CheckpointMode::Passive).unwrap();
        prop_assert_eq!(run.epoch, source.epoch());
        prop_assert!(run.checkpointed <= run.log);
    }
}

#[test]
fn injections_fire_once() {
    let db = sim();
    db.inject(SourceOp::Committed, SourceError::DiskFull);
    db.inject(SourceOp::ReadPage, SourceError::FsyncFailed);
    let mut source = db.source();
    assert_eq!(source.committed(), Err(SourceError::DiskFull));
    source.committed().unwrap();
    let mut buf = vec![0; 512];
    assert_eq!(
        db.pages().read_page(pgno(1), &mut buf),
        Err(SourceError::FsyncFailed)
    );
    db.pages().read_page(pgno(1), &mut buf).unwrap();
    assert_eq!(buf, page(7, pgno(1), ps()));
}

#[test]
fn barrier_and_write_lock_hold_the_writer() {
    let db = sim();
    let mut ck = db.checkpointer();
    let barrier = ck.begin_barrier().unwrap();
    assert_eq!(ck.begin_write_lock(), Err(SourceError::Busy));
    assert_eq!(db.commit(&tx(&[2], 1), 4), None);
    assert_eq!(ck.bump_seq(), Err(SourceError::Busy));
    let run = ck.checkpoint(CheckpointMode::Truncate).unwrap();
    assert!(
        run.busy,
        "TRUNCATE degrades to PASSIVE under another writer"
    );
    ck.end_barrier(barrier).unwrap();
    let held = ck.begin_write_lock().unwrap();
    assert_eq!(db.commit(&tx(&[2], 1), 4), None);
    ck.end_write_lock(held).unwrap();
    assert!(db.commit(&tx(&[2], 1), 4).is_some());
    ck.bump_seq().unwrap();
    assert_eq!(db.mx_frame(), 2);
}

#[test]
fn spill_rollback_and_total_methods() {
    let db = sim();
    assert!(!db.rollback(), "nothing to roll back");
    assert!(!db.our_reader_end());
    assert_eq!(db.commit(&[], 4), None);
    assert!(db.spill(&tx(&[2], 1)));
    assert_eq!(
        db.stranger_checkpoint(StrangerMode::Passive),
        None,
        "the app is in a transaction"
    );
    assert!(db.rollback());
    assert_eq!(db.mx_frame(), 0);
    let commit = db.commit(&tx(&[3], 2), 4).unwrap();
    assert_eq!(commit.end, WalOffset::from_frame_index(1, ps()));
    assert_eq!(scan_committed(&mut db.source()), 1);
    assert_eq!(db.shrink(4), None);
    db.shrink(2).unwrap();
    db.stranger_checkpoint(StrangerMode::Passive).unwrap();
    assert_eq!(db.pages().page_count(), Ok(2));
}

#[test]
fn wal_index_faults() {
    let db = sim();
    db.commit(&tx(&[1, 2], 1), 4).unwrap();
    let mut source = db.source();
    assert!(db.shm_mid_update());
    assert_eq!(source.committed(), Err(SourceError::Busy));
    assert!(db.shm_end_inside_tx());
    assert_eq!(source.committed().unwrap().end().frame_count(), 1);
    assert_eq!(source.committed().unwrap().end().frame_count(), 1);
    assert_eq!(source.committed().unwrap().end().frame_count(), 2);
}

#[test]
fn overwrite_between_scan_and_read_is_caught() {
    let db = sim();
    db.commit(&tx(&[2], 1), 4).unwrap();
    assert!(db.overwrite_frame_between_scan_and_read(1));
    let mut source = db.source();
    let end = source.committed().unwrap().end();
    let mut reader = WalReader::open(source.open().unwrap().unwrap()).unwrap();
    let TxOutcome::Tx(tx) = reader
        .next_tx(&reader.cursor_at_start(), ScanLimit::Committed(end))
        .unwrap()
    else {
        panic!("one transaction");
    };
    let mut buf = vec![0; 512];
    let err = reader.read_page(&tx.frames()[0], &mut buf).unwrap_err();
    assert!(
        matches!(err, ReadPageError::ChangedSinceScan { .. }),
        "{err:?}"
    );
}

fn first_tx_checksum(db: &SimSqlite) -> WalChecksum {
    let mut reader = WalReader::open(db.source().open().unwrap().unwrap()).unwrap();
    let start = reader.cursor_at_start();
    match reader
        .next_tx(&start, ScanLimit::FirstInvalidFrame)
        .unwrap()
    {
        TxOutcome::Tx(tx) => tx.end().checksum(),
        TxOutcome::End { .. } => panic!("no transaction"),
    }
}

#[test]
fn file_faults() {
    let db = sim();
    let first = db.commit(&tx(&[2], 1), 4).unwrap();
    let salts = first.salts;
    let before = first_tx_checksum(&db);
    assert!(db.regrow_same_salts(9));
    assert_ne!(
        first_tx_checksum(&db),
        before,
        "same salts, a new valid chain"
    );
    assert_eq!((db.salts(), db.mx_frame()), (salts, 2));
    let len = db.wal_file().unwrap().len();
    assert!(db.rewrite_same_length(3));
    let wal = db.wal_file().unwrap();
    assert_eq!(wal.len(), len);
    assert_ne!(
        WalReader::open(Cursor::new(wal)).unwrap().header().salts(),
        salts
    );
    assert!(db.flip_bit(8 * 40));
    assert!(!db.flip_bit(8 * u64::try_from(len).unwrap()));
    assert!(db.tear_wal_header());
    WalReader::open(Cursor::new(db.wal_file().unwrap())).unwrap_err();
    assert!(db.truncate_wal_at(10));
    assert!(!db.truncate_wal_at(10));
    assert!(db.unlink_wal());
    assert_eq!(db.source().open().err(), Some(SourceError::Unlinked));
    assert_eq!(db.source().len(), Err(SourceError::Unlinked));
}

#[test]
fn delete_wal_needs_every_connection_closed() {
    let db = sim();
    db.commit(&tx(&[2], 1), 4).unwrap();
    let image = db.image();
    db.our_reader_begin();
    assert!(!db.delete_wal());
    db.crash(CrashKind::Process);
    assert!(db.delete_wal());
    assert!(db.source().open().unwrap().is_none());
    assert_eq!(db.image(), image);
    db.commit(&tx(&[3], 2), 4).unwrap();
    let wal = db.wal_file().unwrap();
    assert_eq!(
        WalReader::open(Cursor::new(wal))
            .unwrap()
            .header()
            .checkpoint_seq(),
        0
    );
}

#[test]
fn rewrite_page_size_keeps_the_bytes() {
    let db = sim();
    db.commit(&tx(&[2], 1), 4).unwrap();
    let image = db.image();
    let bigger = PageSize::new(1024).unwrap();
    assert!(!db.rewrite_page_size(ps()));
    assert!(db.rewrite_page_size(bigger));
    assert_eq!(
        (db.page_size(), db.db_size(), db.image()),
        (bigger, 2, image)
    );
    assert_eq!(db.wal_file(), Some(Vec::new()));
}

#[test]
fn process_crash_drops_our_connections_only() {
    let db = sim();
    let mut ck = db.checkpointer();
    let _barrier = ck.begin_barrier().unwrap();
    db.our_reader_begin();
    let stranger = db.stranger_reader_begin();
    let epoch = db.source().epoch();
    db.crash(CrashKind::Process);
    assert_eq!(db.source().epoch(), epoch.next());
    assert!(db.commit(&tx(&[2], 1), 4).is_some(), "the barrier is gone");
    assert!(
        db.stranger_reader_end(stranger),
        "a stranger survives our crash"
    );
}
#[test]
fn forced_salts_replace_every_random_draw_but_not_the_increment() {
    let db = sim();
    let named = Salts {
        salt1: 0xaaaa_0001,
        salt2: 0xbbbb_0002,
    };
    db.force_random_salts(Some(named));
    db.commit(&tx(&[1], 1), 4).unwrap();
    assert_eq!(db.salts(), named, "a header written at count 0 takes both");
    db.force_random_salts(Some(Salts {
        salt1: 0,
        salt2: 0xcccc_0003,
    }));
    assert!(!db.stranger_checkpoint(StrangerMode::Truncate).unwrap().busy);
    db.commit(&tx(&[1], 2), 4).unwrap();
    assert_eq!(
        db.salts(),
        Salts {
            salt1: named.salt1.wrapping_add(1),
            salt2: 0xcccc_0003
        },
        "a restart keeps salt1 + 1 and takes salt2"
    );
}

#[test]
fn a_page_rewritten_above_the_safe_frame_is_not_copied() {
    let db = sim();
    db.commit(&tx(&[1, 2], 1), 4).unwrap();
    assert!(db.our_reader_begin());
    db.commit(&tx(&[2], 2), 4).unwrap();
    let before = db.db_file();
    let numbers = db.stranger_checkpoint(StrangerMode::Passive).unwrap();
    assert_eq!(numbers.checkpointed, 2, "backfilled up to our mark");
    let after = db.db_file();
    assert_eq!(
        after[..512],
        vec![1; 512][..],
        "page 1's newest frame is below the mark"
    );
    assert_eq!(
        after[512..1024],
        before[512..1024],
        "page 2's newest frame is above it, so SQLite leaves the page alone"
    );
}
