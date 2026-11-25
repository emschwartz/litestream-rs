//! The first start on an empty replica reads the database once: the level-9 snapshot is built
//! from the level-0 file the start just wrote, not from a second pass over the database file
//! (a database of tens of gigabytes beside a live application would be read twice).

use super::*;
use crate::layout::CreatedAt;

/// A database of this many pages, all in the file but page 1, which one commit rewrote in
/// the WAL: the engine reads every other page from the file.
const PAGES: u32 = 40;

fn page_bytes(page: u32, commit: u8) -> Vec<u8> {
    let fill = if page == 1 {
        commit
    } else {
        u8::try_from(page).unwrap()
    };
    vec![fill; PS.bytes()]
}

/// A quiet database of [`PAGES`] pages with one committed transaction in its WAL.
fn quiet() -> Rig {
    let wal = FakeWal::new(
        PS,
        Salts {
            salt1: 10,
            salt2: 20,
        },
    );
    let mut db = FakeDb::new(PS);
    db.set_page_count(PAGES);
    for n in 1..=PAGES {
        db.write_page(pg(n), &page_bytes(n, 0));
    }
    db.link(&wal);
    let ck = FakeCheckpointer::new(wal.clone());
    let rig = Rig {
        wal,
        db,
        ck,
        staging: Counting::default(),
        remote: Counting::default(),
        takeover: None,
        announced: Vec::new(),
    };
    rig.wal.append_tx(&[(pg(1), page_bytes(1, 1))], nz(PAGES));
    rig
}

fn bytes_at(rig: &Rig, level: Level) -> u64 {
    Rig::files(&rig.remote, level).iter().map(|f| f.size).sum()
}

/// What a restore to `target` leaves, every page.
fn restored_pages(store: &MemStore, target: RestoreTarget) -> Vec<Vec<u8>> {
    let sink = MemSink::new();
    block_on(restore(store, target, sink.clone())).unwrap();
    let database = sink.published().unwrap();
    (1..=PAGES).map(|n| database.page(n).unwrap()).collect()
}

fn expected(commit: u8) -> Vec<Vec<u8>> {
    (1..=PAGES).map(|n| page_bytes(n, commit)).collect()
}

/// Every page the database file holds is read once for the first start, and the replica holds
/// the database twice at most: once at level 0, once at level 9.
#[test]
fn the_first_start_reads_each_page_once() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    let level_9 = Rig::files(&rig.remote, Level::SNAPSHOT);
    let [snapshot] = level_9.as_slice() else {
        panic!("one snapshot expected: {level_9:?}");
    };
    assert_eq!(snapshot.key.range().min().get(), 1);
    assert_eq!(snapshot.key.range().max().get(), 1);
    assert_eq!(page_count(&rig, snapshot), PAGES);
    assert_eq!(
        rig.db.reads(),
        u64::from(PAGES - 1),
        "pages the file supplied (page 1 is in the WAL): one read each"
    );
    // The level-0 file and the snapshot are the only uploads: the database's pages twice.
    let uploaded = bytes_at(&rig, Level::ZERO) + bytes_at(&rig, Level::SNAPSHOT);
    let held: u64 = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .chain(&level_9)
        .map(|f| f.size)
        .sum();
    assert_eq!(uploaded, held);
    assert_eq!(Rig::files(&rig.remote, Level::ZERO).len(), 1);
}

/// A first start moves the database twice, to level 0 and to level 9: the levels between hold
/// nothing (they start above the snapshot), and the only download is the snapshot's own check.
/// Before, the level-0 file was carried into levels 1, 2 and 3 (five uploads, two downloads).
#[test]
fn the_first_start_uploads_the_database_twice_and_downloads_none() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    for level in [1, 2, 3] {
        let files = Rig::files(&rig.remote, Level::new(level).unwrap());
        assert!(files.is_empty(), "level {level} holds {files:?}");
    }
    let held: u64 = (0..=9)
        .filter_map(Level::new)
        .flat_map(|level| Rig::files(&rig.remote, level))
        .map(|f| f.size)
        .sum();
    assert_eq!(
        held,
        bytes_at(&rig, Level::ZERO) + bytes_at(&rig, Level::SNAPSHOT),
        "every uploaded byte is at level 0 or 9"
    );
    // The one read is the snapshot's check after it was written.
    let reads = rig.remote.reads.lock().unwrap().clone();
    assert!(
        reads.iter().all(|key| key.level().is_snapshot()),
        "downloaded {reads:?}"
    );
}

/// The pages the file holds, by its own trailer.
fn page_count(rig: &Rig, info: &FileInfo) -> u32 {
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bytes = block_on(rig.remote.inner.read(&info.key, 0, all)).unwrap();
    let mut decoder = ReadDecoder::new(&bytes[..]);
    while decoder.next_event().unwrap().is_some() {}
    u32::try_from(decoder.finish().unwrap().page_count()).unwrap()
}

/// A snapshot built from level 0 that cannot be written leaves the first snapshot to be read
/// from the database: the replica still gets one.
#[test]
fn a_failed_first_snapshot_from_level_zero_is_read_from_the_database_next() {
    let (mut rig, mut actor) = super::rejected_snapshot::before_first_snapshot();
    let reads = rig.db.reads();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let seen = rig.follow(&mut actor, T0, 200);
    let level_9 = Rig::files(&rig.remote, Level::SNAPSHOT);
    assert!(!level_9.is_empty(), "no snapshot followed: {seen:?}");
    assert_eq!(
        rig.db.reads().saturating_sub(reads),
        2,
        "the second try read pages 2 and 3, the two the file supplies"
    );
}

/// The snapshot built from the level-0 file restores to the database: at the latest state, at
/// its transaction, at an instant, and alone (the level-0 file gone).
#[test]
fn the_snapshot_of_a_first_start_restores_the_database() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    rig.wal.append_tx(&[(pg(1), page_bytes(1, 2))], nz(PAGES));
    rig.follow(&mut actor, T0 + 1000, 20);
    let first = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .find(|f| f.key.range().max().get() == 1)
        .unwrap();
    let second = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .find(|f| f.key.range().max().get() == 2)
        .unwrap();
    let store = &rig.remote.inner;
    assert!(restored_pages(store, RestoreTarget::Latest) == expected(2));
    assert!(restored_pages(store, RestoreTarget::Txid(first.key.range().max())) == expected(1));
    let instant: CreatedAt = second.created_at().unwrap();
    assert!(restored_pages(store, RestoreTarget::AsOf(instant.at())) == expected(1));
    // The snapshot alone: a replica holding nothing else restores the first state.
    let others: Vec<FileKey> = Level::ALL
        .iter()
        .flat_map(|level| Rig::files(&rig.remote, *level))
        .filter(|f| !f.key.level().is_snapshot() || f.key.range().max().get() > 1)
        .map(|f| f.key)
        .collect();
    block_on(store.delete(&others)).unwrap();
    assert!(restored_pages(store, RestoreTarget::Latest) == expected(1));
}

/// The other full copies of the database file, on the three-page database of the rig: pages 2
/// and 3 come from the file when page 1 is in the WAL.
mod other_copies {
    use super::super::vouch::redeployed_with_a_new_meta_directory;
    use super::*;

    fn reads_after(rig: &mut Rig, run: impl FnOnce(&mut Rig)) -> u64 {
        let before = rig.db.reads();
        run(rig);
        rig.db.reads().saturating_sub(before)
    }

    /// A WAL that restarted under a running engine costs a snapshot (the next level-0 file
    /// holds every page): one read of the file.
    #[test]
    fn a_resnapshot_reads_each_page_once() {
        let (mut rig, mut actor) = caught_up();
        let reads = reads_after(&mut rig, |rig| {
            rig.commit(2);
            rig.wal.force_restart();
            let seen = rig.settle(&mut actor, T0 + 1);
            assert!(
                seen.iter()
                    .any(|tick| matches!(event(tick), Some(Event::Resnapshot(_)))),
                "{seen:?}"
            );
        });
        assert_eq!(reads, 3);
    }

    /// A scheduled verify reads the live pages once.
    #[test]
    fn a_scheduled_verify_reads_each_page_once() {
        let (mut rig, mut actor) = caught_up();
        let reads = reads_after(&mut rig, |rig| {
            actor.read_live_pages();
            rig.settle(&mut actor, T0 + 1);
        });
        assert!(actor.take_live_pages().is_some());
        assert_eq!(reads, 2);
    }

    /// A start with an empty working folder, after the WAL restarted, checks the database
    /// against the replica and then starts a chain above it: the check reads every page of the
    /// file (page 1 included: the WAL has none), and the snapshot that follows reads them
    /// again. The check also looks for the replicator's counter before it compares: one more
    /// read of page 1, the schema's root, which here holds no b-tree, so the walk ends there. A
    /// schema with more pages costs one read of each, never of the data pages. Only a start
    /// that runs the check (a fresh working folder over a replica it cannot account for) pays
    /// it; a start that continues from its own staging reads nothing extra.
    #[test]
    fn a_start_with_an_empty_working_folder_reads_each_page_twice_and_the_schema_once() {
        let mut rig = redeployed_with_a_new_meta_directory();
        let mut actor = Actor::new(go_like());
        let reads = reads_after(&mut rig, |rig| {
            rig.settle(&mut actor, T0 + 2);
        });
        let (pages, schema_reads) = (3, 1);
        assert_eq!(reads, 2 * pages + schema_reads);
    }
}
