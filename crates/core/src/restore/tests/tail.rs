//! A restore whose planned files the writer's compaction and retention delete while it reads:
//! the part already built is kept and only the files after it are planned again.

use super::*;
use crate::mem::wrapped::Compaction;

/// The restore of `target` over `store`, with every [Replan] it reported.
fn run_noting(
    store: &impl ObjectStore,
    target: RestoreTarget,
) -> (Result<Restored, RestoreError>, MemSink, Vec<Replan>) {
    let sink = MemSink::new();
    let mut notes = Vec::new();
    let result = block_on(restore_noting(
        store,
        target,
        RestoreOptions::default(),
        sink.clone(),
        |replan| notes.push(replan.clone()),
    ));
    (result, sink, notes)
}

/// The writer compacts level-0 files 2 and 3 into one level-1 file while the restore reads the
/// snapshot: the files the first plan named after the snapshot are gone.
fn compacting_during_the_snapshot(store: MemStore) -> Wrapped {
    let store = Wrapped::new(store);
    let merged = Spec::plain(512, 2, 2, 3, &[(1, 5), (2, 9)]).encode();
    store.compactions.lock().unwrap().push(Compaction {
        trigger: key(9, 1, 1),
        from: 0,
        put: vec![(key(1, 2, 3), merged)],
        delete: vec![key(0, 2, 2), key(0, 3, 3)],
    });
    store
}

/// Found restoring a database of tens of GB with steady writes: the snapshot took most of an hour,
/// the writer's compaction deleted the level-0 files planned after it, and the restore started
/// over from the snapshot three times, then failed. The snapshot is read once.
#[test]
fn a_file_compacted_away_while_the_snapshot_downloads_keeps_the_snapshot() {
    let store = compacting_during_the_snapshot(four_file_replica());
    let (result, sink, notes) = run_noting(&store, RestoreTarget::Latest);
    let restored = result.unwrap();
    assert_eq!(seeds(&sink.published().unwrap()), [5, 9, 0, 7]);
    assert_eq!(store.reads_of(key(9, 1, 1)), 1);
    assert_eq!(sink.calls(SinkOp::Reset), 0);
    assert_eq!((restored.reached(), restored.attempts()), (txid(4), 2));
    assert_eq!(
        notes,
        [Replan::Tail {
            missing: key(0, 2, 2),
            kept: txid(1),
        }]
    );
}

#[test]
fn a_txid_restore_keeps_the_snapshot_when_files_after_it_are_compacted() {
    let store = compacting_during_the_snapshot(four_file_replica());
    let (result, sink, notes) = run_noting(&store, RestoreTarget::Txid(txid(3)));
    assert_eq!(result.unwrap().reached(), txid(3));
    assert_eq!(seeds(&sink.published().unwrap()), [5, 9]);
    assert_eq!(store.reads_of(key(9, 1, 1)), 1);
    assert_eq!(notes.len(), 1);
}

/// An instant's plan is fixed to the transaction it reached, so the rest stops there even though
/// the compacted file's own created-at time is not before the instant.
#[test]
fn an_as_of_restore_keeps_the_snapshot_and_the_transaction_its_plan_reached() {
    let store = compacting_during_the_snapshot(fill_four(MemStore::with_listed_last_modified()));
    let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(3500));
    let (result, sink, notes) = run_noting(&store, target);
    assert_eq!(result.unwrap().reached(), txid(3));
    assert_eq!(seeds(&sink.published().unwrap()), [5, 9]);
    assert_eq!(store.reads_of(key(9, 1, 1)), 1);
    assert_eq!(notes.len(), 1);
}

/// Retention removed the level-0 files after the kept snapshot under a newer snapshot: nothing
/// leads on from the kept part, so the restore starts over, on the listing that showed it.
#[test]
fn files_after_the_kept_part_gone_under_a_newer_snapshot_start_over_on_that_listing() {
    let wrap = |store| {
        let store = Wrapped::new(store);
        let newer = Spec::plain(512, 2, 1, 3, &[(1, 5), (2, 9)]).encode();
        store.compactions.lock().unwrap().push(Compaction {
            trigger: key(9, 1, 1),
            from: 0,
            put: vec![(key(9, 1, 3), newer)],
            delete: vec![key(0, 2, 2), key(0, 3, 3)],
        });
        store
    };
    let store = wrap(four_file_replica());
    let (result, sink, notes) = run_noting(&store, RestoreTarget::Latest);
    assert_eq!(result.unwrap().reached(), txid(4));
    assert_eq!(seeds(&sink.published().unwrap()), [5, 9, 0, 7]);
    assert_eq!(sink.calls(SinkOp::Reset), 1);
    let [tail, no_tail] = notes.as_slice() else {
        panic!("{notes:?}");
    };
    assert_eq!(
        *tail,
        Replan::Tail {
            missing: key(0, 2, 2),
            kept: txid(1),
        }
    );
    assert!(
        matches!(
            no_tail,
            Replan::NoTail { missing, kept, outcome: PlanOutcome::Gap { .. } }
                if *missing == key(0, 2, 2) && *kept == txid(1)
        ),
        "{no_tail:?}"
    );
    // Two listings, the first plan's and the one the rest was planned on: the start over
    // plans on the second instead of listing a third time.
    let once = Wrapped::new(four_file_replica());
    block_on(Inventory::load(&once)).unwrap();
    let per_listing = once.batches.lock().unwrap().len();
    assert_eq!(store.batches.lock().unwrap().len(), 2 * per_listing);
}

/// Pages that do not compress, so a file of them is read in more than one chunk.
fn noise(ps: PageSize, pgno: u32) -> Vec<u8> {
    let mut state = u64::from(pgno).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..ps.bytes())
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A file of pages 1 to `commit` covering `min` to `max`: page 1 is `first` repeated and the
/// rest are [noise].
fn noisy_file(ps: PageSize, commit: u32, min: u64, max: u64, first: u8) -> Vec<u8> {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps,
        commit,
        txids: range(min, max),
        timestamp: Timestamp::from_unix_millis(i64::try_from(max).unwrap() * 1000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for pgno in 1..=commit {
        let page = if pgno == 1 {
            page_of(ps, first)
        } else {
            noise(ps, pgno)
        };
        encoder
            .write_page(PageNo::new(pgno, ps).unwrap(), &page)
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// A file gone after part of it was written leaves pages no verified file vouches for: the
/// database is emptied and the restore starts over from the snapshot.
#[test]
fn a_file_gone_part_way_through_starts_over() {
    let ps = size(65536);
    let commit = 80;
    let inner = MemStore::new();
    put_spec(&inner, 9, &Spec::plain(65536, 1, 1, 1, &[(1, 1)]));
    let big = noisy_file(ps, commit, 2, 2, 2);
    assert!(big.len() > usize::try_from(CHUNK_BYTES).unwrap());
    put(&inner, 0, 2, 2, &big);
    put_spec(&inner, 0, &Spec::plain(65536, commit, 3, 3, &[(1, 5)]));
    let store = Wrapped::new(inner);
    store.compactions.lock().unwrap().push(Compaction {
        trigger: key(0, 2, 2),
        from: 1,
        put: vec![(key(1, 2, 3), noisy_file(ps, commit, 2, 3, 5))],
        delete: vec![key(0, 2, 2), key(0, 3, 3)],
    });
    let (result, sink, notes) = run_noting(&store, RestoreTarget::Latest);
    assert_eq!(result.unwrap().reached(), txid(3));
    assert_eq!(
        notes,
        [Replan::Restart {
            missing: key(0, 2, 2),
        }]
    );
    assert_eq!(store.reads_of(key(9, 1, 1)), 2);
    assert_eq!(sink.calls(SinkOp::Reset), 1);
    let db = sink.published().unwrap();
    assert_eq!(db.page(1).unwrap(), page_of(ps, 5).as_slice());
    for pgno in 2..=commit {
        assert_eq!(
            db.page(pgno).unwrap(),
            noise(ps, pgno).as_slice(),
            "page {pgno}"
        );
    }
}

/// A snapshot and a file for transaction 2 at each level from 1 to 8 that vanish when read, the
/// highest level first (the planner prefers it), and one at level 0 that stays.
fn vanishing_levels(levels: std::ops::RangeInclusive<u8>) -> Wrapped {
    let inner = MemStore::new();
    put_spec(&inner, 9, &Spec::plain(512, 1, 1, 1, &[(1, 1)]));
    for level in 0..=8 {
        put_spec(&inner, level, &Spec::plain(512, 1, 2, 2, &[(1, 2)]));
    }
    let keys: Vec<FileKey> = levels.map(|level| key(level, 2, 2)).collect();
    Wrapped::vanishing(inner, &keys)
}

#[test]
fn the_rest_is_planned_again_up_to_the_bound() {
    let store = vanishing_levels(2..=8);
    let (result, sink, notes) = run_noting(&store, RestoreTarget::Latest);
    let restored = result.unwrap();
    assert_eq!(restored.attempts(), MAX_TAIL_PLANS);
    assert_eq!(seeds(&sink.published().unwrap()), [2]);
    assert_eq!(store.reads_of(key(9, 1, 1)), 1);
    assert_eq!(notes.len(), usize::from(MAX_TAIL_PLANS - 1));
}

#[test]
fn files_vanishing_past_the_bound_are_unstable() {
    let store = vanishing_levels(1..=8);
    let (result, sink, notes) = run_noting(&store, RestoreTarget::Latest);
    assert!(
        matches!(
            result,
            Err(RestoreError::Unstable { attempts, missing })
                if attempts == MAX_TAIL_PLANS && missing == key(1, 2, 2)
        ),
        "{result:?}"
    );
    assert_eq!(sink.published(), None);
    assert_eq!(notes.len(), usize::from(MAX_TAIL_PLANS - 1));
}

#[test]
fn a_vanished_first_file_is_reported_as_a_restart() {
    let store = Wrapped::vanishing(three_snapshots(), &[key(9, 1, 3)]);
    let (result, _, notes) = run_noting(&store, RestoreTarget::Latest);
    assert_eq!(result.unwrap().reached(), txid(2));
    assert_eq!(
        notes,
        [Replan::Restart {
            missing: key(9, 1, 3),
        }]
    );
}
