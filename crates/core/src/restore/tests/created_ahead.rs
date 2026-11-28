//! An instant's plan reads the created-at times of its chain a store-width at a time: the files it
//! will ask for after the one it needs now are predicted ([`AsOfPlanner`]'s `expected_after`), so
//! a chain of N files costs about N over the width round trips, not N of them, and the files read
//! are the files of the plan.

use std::collections::BTreeSet;

use super::*;

const WIDTH: usize = 4;

/// A store listing last-modified times only, whose files (level, first and last transaction)
/// were created one second a transaction, run `WIDTH` wide.
fn replica_of(files: &[(u8, u64, u64)]) -> Wrapped {
    let inner = MemStore::with_listed_last_modified();
    for (level, min, max) in files {
        let seed = u8::try_from(*max).unwrap();
        let pages: &[(u32, u8)] = if *level == 9 {
            &[(1, 1), (2, 2), (3, 3)]
        } else {
            &[(2, seed)]
        };
        let spec = Spec::plain(512, 3, *min, *max, pages);
        put_spec(&inner, *level, &spec);
    }
    let mut store = Wrapped::new(inner);
    store.width = WIDTH;
    store
}

/// A restore as of `second`, and the keys whose created-at time the store was asked for.
fn asked_as_of(
    store: &Wrapped,
    second: i64,
) -> (Result<Restored, RestoreError>, BTreeSet<FileKey>) {
    let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(second * 1000));
    let (result, _) = run(store, target);
    let asked = store.created_asked.lock().unwrap();
    if result.is_ok() {
        assert_eq!(
            asked.len(),
            asked.iter().collect::<BTreeSet<_>>().len(),
            "a file asked twice"
        );
    }
    (result, asked.iter().copied().collect())
}

fn keys(files: &[(u8, u64, u64)]) -> BTreeSet<FileKey> {
    files
        .iter()
        .map(|(l, min, max)| key(*l, *min, *max))
        .collect()
}

#[test]
fn a_chain_of_single_transaction_files_is_read_a_width_at_a_time() {
    let mut files = vec![(9, 1, 1)];
    files.extend((2..=21).map(|n| (0, n, n)));
    let store = replica_of(&files);
    let (result, asked) = asked_as_of(&store, 100);
    assert_eq!(result.unwrap().reached(), txid(21));
    // The base is asked alone; the twenty files after it in five batches of four.
    assert_eq!(*store.created_batches.lock().unwrap(), [WIDTH; 5]);
    assert_eq!(asked, keys(&files));
}

#[test]
fn the_files_read_ahead_are_the_ones_the_chain_takes() {
    // A level-1 file covers level-0 files 2 to 9, which the chain passes over unread.
    let mut files = vec![(9, 1, 1), (1, 2, 9)];
    files.extend((2..=9).map(|n| (0, n, n)));
    files.extend((10..=13).map(|n| (0, n, n)));
    // The most preferred of two that start at 10: the one that reaches furthest.
    files.push((1, 10, 13));
    let store = replica_of(&files);
    let (result, asked) = asked_as_of(&store, 100);
    assert_eq!(result.unwrap().reached(), txid(13));
    let mut taken = vec![(9, 1, 1), (1, 2, 9), (1, 10, 13)];
    taken.sort_unstable();
    assert_eq!(asked, keys(&taken));
}

#[test]
fn nothing_is_read_ahead_past_a_gap() {
    let mut files = vec![(9, 1, 1)];
    files.extend((2..=5).map(|n| (0, n, n)));
    files.extend((8..=10).map(|n| (0, n, n)));
    let store = replica_of(&files);
    let (result, asked) = asked_as_of(&store, 100);
    assert!(result.is_err(), "{result:?}");
    let mut before = vec![(9, 1, 1)];
    before.extend((2..=5).map(|n| (0, n, n)));
    assert_eq!(asked, keys(&before));
}

#[test]
fn files_created_after_the_instant_cost_about_a_batch_of_reads() {
    let mut files = vec![(9, 1, 1)];
    files.extend((2..=40).map(|n| (0, n, n)));
    let store = replica_of(&files);
    // Files 2 to 9 were created before second 10, which is not before it.
    let (result, asked) = asked_as_of(&store, 10);
    assert_eq!(result.unwrap().reached(), txid(9));
    let wanted = keys(
        &files
            .iter()
            .copied()
            .filter(|f| f.2 <= 10)
            .collect::<Vec<_>>(),
    );
    assert!(wanted.is_subset(&asked), "the first late file is asked");
    assert!(asked.len() < wanted.len() + WIDTH, "{}", asked.len());
}
