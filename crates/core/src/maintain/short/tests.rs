//! The remembered files, the cursors they give and the cover cursor.

use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::maintain::L0Retention;
use crate::maintain::tests::{T0, at, defaults, file, level, txid};
use crate::mem::MemStore;
use crate::store::{Upload, WriteMode};

fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(txid(min), txid(max)).unwrap()
}

/// The cursor of the level `n` in `starts`.
fn after(starts: &[ListAt], n: u8) -> Option<Option<TxidRange>> {
    starts
        .iter()
        .find(|at| at.level == level(n))
        .map(|at| at.after)
}

#[test]
fn a_replica_nothing_is_known_of_is_listed_from_the_first_file_at_every_level() {
    let starts = Remembered::default().starts(&defaults());
    let levels: Vec<u8> = starts.iter().map(|at| at.level.get()).collect();
    assert_eq!(levels, vec![0, 1, 2, 3, 9]);
    assert!(starts.iter().all(|at| at.after.is_none()));
}

/// A level is listed after the lower of the newest file it holds and, for the level below a
/// compaction, the last transaction of the level above; level 1 also after the cover cursor.
#[test]
fn each_level_is_listed_after_the_lowest_key_its_decisions_need() {
    let mut known = Remembered::default();
    known.note(file(1, 91, 100, T0));
    known.note(file(2, 51, 80, T0));
    known.note(file(3, 1, 40, T0));
    known.cover = Some(range(61, 70));
    let starts = known.starts(&defaults());
    // Level 1: its newest (91, 100), the cover (61, 70), and level 2's last transaction (80, 80).
    assert_eq!(after(&starts, 1), Some(Some(range(61, 70))));
    // Level 2: its newest (51, 80) and level 3's last transaction (40, 40).
    assert_eq!(after(&starts, 2), Some(Some(range(40, 40))));
    // Level 3: its newest file; nothing is compacted from it.
    assert_eq!(after(&starts, 3), Some(Some(range(1, 40))));
    // Level 0 and level 9 are always whole.
    assert_eq!(after(&starts, 0), Some(None));
    assert_eq!(after(&starts, 9), Some(None));
}

#[test]
fn the_cover_cursor_is_left_out_when_level_zero_is_kept() {
    let keep = Maintenance::new(
        vec![std::time::Duration::from_secs(30)],
        std::time::Duration::from_secs(86_400),
        std::time::Duration::from_secs(86_400),
        L0Retention::Keep,
    )
    .unwrap();
    let mut known = Remembered::default();
    known.note(file(1, 91, 100, T0));
    known.cover = Some(range(1, 10));
    assert_eq!(after(&known.starts(&keep), 1), Some(Some(range(91, 100))));
}

/// The cursor is the key of the last file of the prefix that ends below `first_zero`.
#[test]
fn the_cover_cursor_stops_before_the_first_file_that_could_cover() {
    let files: Vec<FileInfo> = [(1, 10), (11, 20), (21, 30), (31, 40)]
        .into_iter()
        .map(|(a, b)| file(1, a, b, T0))
        .collect();
    let got = advance_cover(None, &files, Some(txid(25)), None);
    assert_eq!(got, Some(range(11, 20)));
    // Nothing below it: the whole level is listed.
    assert_eq!(advance_cover(None, &files, Some(txid(5)), None), None);
    // No level-0 file left: the newest level-1 file known.
    assert_eq!(
        advance_cover(None, &files, None, files.last()),
        Some(range(31, 40))
    );
    // It never moves back.
    assert_eq!(
        advance_cover(Some(range(21, 30)), &files[3..], Some(txid(25)), None),
        Some(range(21, 30))
    );
}

proptest! {
    /// Every level-1 file that could cover a level-0 file from `first_zero` on sorts after the
    /// cursor, so a listing after it returns all of them, whatever the files look like.
    #[test]
    fn every_file_that_could_cover_sorts_after_the_cover_cursor(
        mut ends in prop::collection::vec(1u64..400, 0..40),
        spans in prop::collection::vec(0u64..20, 40),
        first_zero in 1u64..450,
    ) {
        ends.sort_unstable();
        ends.dedup();
        let mut files: Vec<FileInfo> = ends
            .iter()
            .zip(&spans)
            .map(|(end, span)| file(1, end.saturating_sub(*span).max(1), *end, T0))
            .collect();
        // A listing is ascending by key, which overlapping files do not keep in order of their ends.
        files.sort_by_key(|info| info.key);
        let cursor = advance_cover(None, &files, Some(txid(first_zero)), None);
        for info in &files {
            if let Some(cursor) = cursor
                && info.key.range().max() >= txid(first_zero)
            {
                prop_assert!(cmp_range(info.key.range(), cursor).is_gt(), "{info:?} {cursor:?}");
            }
        }
    }
}

/// A store holding `files`, each created as the listing says.
fn store_of(files: &[FileInfo]) -> MemStore {
    let store = MemStore::default();
    block_on(async {
        for f in files {
            let created_at = f.created_at().unwrap().at();
            let mut up = store
                .begin_upload(&f.key, created_at, WriteMode::Replace)
                .await
                .unwrap();
            up.write(b"x").await.unwrap();
            up.commit().await.unwrap();
        }
    });
    store
}

/// The ladder of the retention tests without its level-1 files: snapshots at 10, 20 and 30
/// and level 0 from 25 to 30, one every four hours.
fn without_level_one() -> Vec<FileInfo> {
    let created = |n: u64| T0 + i64::try_from(n).unwrap() * 4 * 3_600_000;
    let mut files: Vec<FileInfo> = [10, 20, 30]
        .into_iter()
        .map(|n| file(9, 1, n, created(n)))
        .collect();
    files.extend((25..=30).map(|n| file(0, n, n, created(n))));
    files
}

fn decided(store: &MemStore, known: &Remembered, now: i64) -> ShortDecided {
    // One level, so only level 0's retention is left to plan.
    let cfg = Maintenance::new(
        vec![std::time::Duration::from_secs(30)],
        std::time::Duration::from_secs(86_400),
        std::time::Duration::from_secs(86_400),
        L0Retention::After(std::time::Duration::from_secs(300)),
    )
    .unwrap();
    let tail = block_on(Tail::load(store, known, &cfg)).unwrap();
    let state = MaintainState::default();
    let got = block_on(crate::created_at::resolve(
        store,
        &mut |resolved: &ResolvedCreatedAt| due_short(&tail, at(now), &cfg, None, &state, resolved),
    ))
    .unwrap();
    got.value
}

/// A remembered file may not be there any more, so it is never what covers a level-0
/// file: with the level-1 file over 21 to 30 only remembered, nothing is deleted.
#[test]
fn a_remembered_file_is_never_a_cover() {
    let files = without_level_one();
    let now = files
        .iter()
        .map(|f| f.created_at().unwrap().at().unix_millis())
        .max()
        .unwrap();
    let store = store_of(&files);
    let mut known = Remembered::default();
    known.note(file(1, 21, 30, now));
    let got = decided(&store, &known, now);
    assert!(
        matches!(got.due, Due::Wait { .. }),
        "deleted from a remembered cover: {got:?}"
    );
    // The same file listed does cover them.
    let mut listed = files;
    listed.push(file(1, 21, 30, now));
    let store = store_of(&listed);
    let got = decided(&store, &Remembered::default(), now);
    assert!(matches!(got.due, Due::Run(Job::Delete(_))), "{got:?}");
}

/// Remembering the output of a compaction moves the level's cursor past it.
#[test]
fn a_compaction_output_is_the_newest_file_of_its_level_and_an_older_one_is_not() {
    let mut known = Remembered::default();
    known.note(file(1, 11, 20, T0));
    known.note(file(1, 1, 10, T0));
    assert_eq!(known.newest_txid(1), Some(txid(20)));
}

/// Dropping the memory makes the next pass whole.
#[test]
fn forgetting_makes_the_next_pass_whole() {
    let mut known = Remembered::default();
    let inventory = Inventory::from_files([file(0, 1, 1, T0)]).unwrap();
    let decided = Decided {
        due: Due::Wait {
            until: None,
            held: None,
        },
        gate: Gate::Clear,
        retention_wake: None,
    };
    known.learn_full(&inventory, at(T0), &defaults(), &decided);
    assert!(known.short_ok(at(T0 + 1)));
    // The backstop: a whole listing at the latest after the interval.
    let interval = i64::try_from(FULL_LISTING_INTERVAL.as_millis()).unwrap();
    assert!(!known.short_ok(at(T0 + interval)));
    known.forget();
    assert!(!known.short_ok(at(T0 + 1)));
    // A held gate, and a delete above level 0, each owe a whole listing.
    let held = Decided {
        gate: Gate::Held,
        ..decided.clone()
    };
    known.learn_full(&inventory, at(T0), &defaults(), &held);
    assert!(!known.short_ok(at(T0 + 1)));
    known.learn_full(&inventory, at(T0), &defaults(), &decided);
    known.owe_full();
    assert!(!known.short_ok(at(T0 + 1)));
    // A retention wake that has come is a whole listing too.
    let waking = Decided {
        retention_wake: Some(at(T0 + 5_000)),
        ..decided
    };
    known.learn_full(&inventory, at(T0), &defaults(), &waking);
    assert!(known.short_ok(at(T0 + 4_999)));
    assert!(!known.short_ok(at(T0 + 5_000)));
}
