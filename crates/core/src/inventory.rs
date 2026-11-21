//! One complete picture of a replica: every level's files, listed once.
//!
//! The planner works on an [`Inventory`], never on the store, so a plan is a
//! pure function of what one paging saw. A part-way listing failure is an
//! error here; an inventory with a missing level or a short level cannot be
//! built, which keeps "the history looks shorter than it is" out of every
//! later decision.

use std::collections::BTreeMap;

use lsr_ltx::{Txid, TxidRange};
use thiserror::Error;

use crate::layout::{FileInfo, FileKey, Level, ListedStamp, cmp_range};
use crate::store::{ListAt, ObjectStore, PageLimit, StoreError};

/// The page size a full scan asks for. A backend that proves "more follows"
/// with one key past the page (S3 caps a request at [`PageLimit::MAX`] keys)
/// needs a second request for a page of exactly that size, and the next page
/// then fetches the same keys again. One short of the cap fills a request.
const SCAN_PAGE: PageLimit = match PageLimit::new(PageLimit::MAX_RAW - 1) {
    Some(limit) => limit,
    None => PageLimit::MAX,
};

/// Why an inventory could not be built.
#[derive(Clone, PartialEq, Eq, Debug, Error)]
pub enum InventoryError {
    /// A page of a listing failed; the inventory would have been short.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// One key appeared twice with different sizes or timestamps. A real
    /// listing cannot do that, and which copy is right would decide an `AsOf`
    /// plan, so there is no inventory rather than a guess.
    #[error("key {key} listed twice with different size or timestamp")]
    ConflictingDuplicate {
        /// The key that appeared twice.
        key: FileKey,
    },
    /// A store returned a cursor that does not move forward, which would page
    /// forever. A store that does this is broken, not busy.
    #[error("listing of level {level} returned cursor {cursor:?} that does not advance")]
    ListingStalled {
        /// The level being listed.
        level: Level,
        /// The cursor that did not advance.
        cursor: TxidRange,
    },
}

/// Whether a file can be the start of a restore chain: its range begins at
/// transaction 1, at any level. LTX defines a snapshot as a file that starts
/// at transaction 1, and the decoder proves such a file holds every page, so a
/// young replica's level-0 file from transaction 1 is as good a base as a
/// level-9 one (Go's `CalcRestorePlan` does the same). A level-9 file that
/// begins later is still never a base: it skips transactions, so it cannot be
/// proven to hold every page.
#[must_use]
pub fn is_snapshot_start(info: &FileInfo) -> bool {
    info.key.range().min() == Txid::MIN
}

/// Every file of a replica, ascending by key, plus the count of foreign names
/// each level held.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Inventory {
    files: Vec<FileInfo>,
    skipped: BTreeMap<Level, usize>,
}

impl Inventory {
    /// Lists every level to the end. The only async code in the planner.
    ///
    /// Up to the store's [`ObjectStore::batch_width`] levels are listed at
    /// once: each round asks for the next page of the first that many levels
    /// still going, in level order, so at width one this is level 0 to the
    /// end, then level 1, as one loop would do it.
    ///
    /// # Errors
    /// The first failed page in level order, or a cursor that does not
    /// advance. Never a partial inventory.
    pub async fn load<S: ObjectStore>(store: &S) -> Result<Self, InventoryError> {
        let mut files = Vec::new();
        let mut skipped = BTreeMap::new();
        let width = store.batch_width().get();
        let mut going: Vec<ListAt> = Level::ALL
            .iter()
            .map(|&level| ListAt { level, after: None })
            .collect();
        while !going.is_empty() {
            let round: Vec<ListAt> = going.iter().take(width).copied().collect();
            let pages = store.list_many(&round, SCAN_PAGE).await;
            let mut next = Vec::with_capacity(going.len());
            for (at, page) in round.iter().zip(pages) {
                let page = page?;
                let seen: &mut usize = skipped.entry(at.level).or_default();
                *seen = seen.saturating_add(page.skipped);
                files.extend(page.files);
                let Some(cursor) = page.next else { continue };
                if at.after.is_some_and(|prev| cmp_range(cursor, prev).is_le()) {
                    return Err(InventoryError::ListingStalled {
                        level: at.level,
                        cursor,
                    });
                }
                next.push(ListAt {
                    level: at.level,
                    after: Some(cursor),
                });
            }
            next.extend(going.into_iter().skip(round.len()));
            going = next;
        }
        Self::from_parts(files, skipped)
    }

    /// An inventory of exactly these files, for tests and callers that
    /// already hold a listing. The order of the input never matters. A key
    /// given twice with the same size and timestamp keeps one entry.
    ///
    /// # Errors
    /// [`InventoryError::ConflictingDuplicate`] when a key is given twice with
    /// a different size or timestamp.
    pub fn from_files(files: impl IntoIterator<Item = FileInfo>) -> Result<Self, InventoryError> {
        Self::from_parts(files.into_iter().collect(), BTreeMap::new())
    }

    /// Records the foreign names a level held, as a store's listings counted them.
    #[must_use]
    pub fn with_skipped(mut self, level: Level, count: usize) -> Self {
        self.skipped.insert(level, count);
        self
    }

    /// The same inventory with every file's stamp read from the store, one
    /// `stamp` call (a HEAD on S3) per file, so each is `Stored` afterwards.
    ///
    /// # Errors
    /// The first failed read; never a partly stamped inventory.
    pub async fn stamped<S: ObjectStore>(mut self, store: &S) -> Result<Self, StoreError> {
        for info in &mut self.files {
            info.listed = ListedStamp::Stored(store.stamp(info).await?);
        }
        Ok(self)
    }

    fn from_parts(
        mut files: Vec<FileInfo>,
        skipped: BTreeMap<Level, usize>,
    ) -> Result<Self, InventoryError> {
        files.sort_by_key(|info| info.key);
        // Equal keys are adjacent, and any set of copies that is not all
        // identical has an adjacent pair that differs.
        if let Some(pair) = files.windows(2).find(|pair| {
            pair.first().map(|a| a.key) == pair.get(1).map(|b| b.key) && pair.first() != pair.get(1)
        }) && let Some(first) = pair.first()
        {
            return Err(InventoryError::ConflictingDuplicate { key: first.key });
        }
        files.dedup_by_key(|info| info.key);
        Ok(Self { files, skipped })
    }

    /// Every file, ascending by key.
    #[must_use]
    pub fn files(&self) -> &[FileInfo] {
        &self.files
    }

    /// The files of one level, ascending by key.
    #[must_use]
    pub fn level(&self, level: Level) -> &[FileInfo] {
        let start = self.files.partition_point(|info| info.key.level() < level);
        let end = self.files.partition_point(|info| info.key.level() <= level);
        self.files.get(start..end).unwrap_or_default()
    }

    /// Whether the replica holds no LTX file at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// The last transaction any file covers.
    #[must_use]
    pub fn newest(&self) -> Option<Txid> {
        self.files.iter().map(|info| info.key.range().max()).max()
    }

    /// Foreign names seen in one level.
    #[must_use]
    pub fn skipped(&self, level: Level) -> usize {
        self.skipped.get(&level).copied().unwrap_or(0)
    }

    /// Foreign names seen in every level together.
    #[must_use]
    pub fn skipped_total(&self) -> usize {
        self.skipped
            .values()
            .fold(0, |sum, n| sum.saturating_add(*n))
    }

    /// Level-9 files that do not start at transaction 1: never used as a
    /// snapshot, always worth telling an operator about.
    #[must_use]
    pub fn malformed_snapshots(&self) -> Vec<FileKey> {
        self.files
            .iter()
            .filter(|info| info.key.level().is_snapshot() && !is_snapshot_start(info))
            .map(|info| info.key)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::block_on;
    use crate::layout::ListedStamp;
    use crate::layout::StoredStamp;
    use crate::mem::{MemStore, MemUpload};
    use crate::store::{Listing, ReadLen, StoreOp, Upload};
    use lsr_ltx::Timestamp;
    use std::future::{Future, ready};

    fn key(level: u8, min: u64, max: u64) -> FileKey {
        let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
        FileKey::new(Level::new(level).unwrap(), range)
    }

    fn info(level: u8, min: u64, max: u64) -> FileInfo {
        FileInfo {
            key: key(level, min, max),
            size: 1,
            listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(0))),
        }
    }

    async fn put(store: &MemStore, k: FileKey) {
        let at = Timestamp::from_unix_millis(7);
        let mut up = store.begin_upload(&k, at).await.unwrap();
        up.write(b"x").await.unwrap();
        up.commit().await.unwrap();
    }

    #[test]
    fn from_files_sorts_and_dedups() {
        let inv = Inventory::from_files([info(1, 5, 6), info(0, 2, 2), info(0, 2, 2)]).unwrap();
        let keys: Vec<FileKey> = inv.files().iter().map(|f| f.key).collect();
        assert_eq!(keys, vec![key(0, 2, 2), key(1, 5, 6)]);
        assert_eq!(inv.newest(), Txid::new(6));
        assert!(!inv.is_empty() && Inventory::default().is_empty());
    }

    #[test]
    fn a_key_given_twice_with_different_details_is_an_error_in_any_order() {
        let early = info(0, 4, 4);
        let late = FileInfo {
            listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(900))),
            ..early
        };
        let bigger = FileInfo { size: 9, ..early };
        let other = info(1, 5, 6);
        let conflict = Err(InventoryError::ConflictingDuplicate { key: early.key });
        for order in [
            [early, late, other],
            [late, early, other],
            [other, early, late],
            [early, other, late],
            [early, bigger, early],
        ] {
            assert_eq!(Inventory::from_files(order), conflict, "{order:?}");
        }
    }

    #[test]
    fn input_order_never_changes_the_inventory() {
        let files = [info(9, 1, 3), info(0, 4, 4), info(1, 4, 9), info(0, 5, 5)];
        let want = Inventory::from_files(files).unwrap();
        let mut rotated = files;
        for _ in 0..files.len() {
            rotated.rotate_left(1);
            assert_eq!(Inventory::from_files(rotated).unwrap(), want);
            let mut reversed = rotated;
            reversed.reverse();
            assert_eq!(Inventory::from_files(reversed).unwrap(), want);
        }
    }

    #[test]
    fn level_is_the_slice_of_that_level_in_key_order() {
        let inv = Inventory::from_files([
            info(9, 1, 3),
            info(1, 4, 9),
            info(0, 5, 5),
            info(0, 4, 4),
            info(1, 1, 3),
        ])
        .unwrap();
        let keys = |l: u8| -> Vec<FileKey> {
            inv.level(Level::new(l).unwrap())
                .iter()
                .map(|f| f.key)
                .collect()
        };
        assert_eq!(keys(0), vec![key(0, 4, 4), key(0, 5, 5)]);
        assert_eq!(keys(1), vec![key(1, 1, 3), key(1, 4, 9)]);
        assert_eq!(keys(9), vec![key(9, 1, 3)]);
        assert!(keys(2).is_empty() && keys(5).is_empty());
        assert!(Inventory::default().level(Level::ZERO).is_empty());
    }

    #[test]
    fn malformed_snapshots_are_level_nine_not_starting_at_one() {
        let inv = Inventory::from_files([info(9, 1, 4), info(9, 2, 6), info(1, 2, 6)]).unwrap();
        assert_eq!(inv.malformed_snapshots(), vec![key(9, 2, 6)]);
    }

    #[test]
    fn load_reads_every_level_across_pages_and_counts_foreign_names() {
        let store = MemStore::new();
        block_on(async {
            for n in 1..=1005u64 {
                put(&store, key(0, n, n)).await;
            }
            put(&store, key(9, 1, 3)).await;
            put(&store, key(4, 1, 8)).await;
        });
        store.plant_foreign(Level::ZERO, "zzz.tmp");
        store.plant_foreign(Level::SNAPSHOT, "lock.json");
        let inv = block_on(Inventory::load(&store)).unwrap();
        assert_eq!(inv.files().len(), 1007);
        assert_eq!(inv.skipped(Level::ZERO), 1);
        assert_eq!(inv.skipped(Level::SNAPSHOT), 1);
        assert_eq!(inv.skipped_total(), 2);
        assert_eq!(inv.newest(), Txid::new(1005));
    }

    #[test]
    fn stamped_keeps_every_file_and_gives_each_its_stored_stamp() {
        let store = MemStore::with_listed_last_modified();
        block_on(async {
            for n in 1..=3u64 {
                put(&store, key(0, n, n)).await;
            }
            put(&store, key(9, 1, 3)).await;
        });
        let listed = block_on(Inventory::load(&store)).unwrap();
        assert!(listed.files().iter().all(|f| f.exact_stamp().is_none()));
        let keys: Vec<FileKey> = listed.files().iter().map(|f| f.key).collect();
        let stamped = block_on(listed.stamped(&store)).unwrap();
        assert_eq!(
            stamped.files().iter().map(|f| f.key).collect::<Vec<_>>(),
            keys
        );
        for file in stamped.files() {
            let stamp = file.exact_stamp().unwrap();
            assert_eq!(stamp.at(), Timestamp::from_unix_millis(7), "{}", file.key);
        }
        assert_eq!(store.stamp_calls(), 4);
    }

    #[test]
    fn a_failed_page_is_an_error_not_a_short_inventory() {
        let store = MemStore::new();
        block_on(put(&store, key(0, 1, 1)));
        let fault = StoreError::Transient {
            op: StoreOp::List,
            kind: std::io::ErrorKind::TimedOut,
        };
        store.inject_failure(StoreOp::List, fault.clone());
        let got = block_on(Inventory::load(&store));
        assert_eq!(got, Err(InventoryError::Store(fault)));
    }

    /// A store whose cursor never moves.
    #[derive(Debug)]
    struct Stuck;

    impl ObjectStore for Stuck {
        type Upload = MemUpload;

        fn list(
            &self,
            _level: Level,
            _after: Option<TxidRange>,
            _limit: PageLimit,
        ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
            ready(Ok(Listing {
                files: vec![info(0, 1, 1)],
                next: Some(key(0, 1, 1).range()),
                skipped: 0,
            }))
        }

        fn read(
            &self,
            key: &FileKey,
            _offset: u64,
            _len: ReadLen,
        ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
            ready(Err(StoreError::NotFound { key: *key }))
        }

        fn stamp(
            &self,
            file: &FileInfo,
        ) -> impl Future<Output = Result<StoredStamp, StoreError>> + Send {
            ready(Err(StoreError::NotFound { key: file.key }))
        }

        fn read_with_stamp(
            &self,
            file: &FileInfo,
            _len: ReadLen,
        ) -> impl Future<Output = Result<(Vec<u8>, StoredStamp), StoreError>> + Send {
            ready(Err(StoreError::NotFound { key: file.key }))
        }

        fn begin_upload(
            &self,
            key: &FileKey,
            _created_at: Timestamp,
        ) -> impl Future<Output = Result<MemUpload, StoreError>> + Send {
            ready(Err(StoreError::NotFound { key: *key }))
        }

        fn delete(&self, _keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
            ready(Ok(()))
        }
    }

    #[test]
    fn a_cursor_that_does_not_advance_is_a_typed_give_up() {
        let got = block_on(Inventory::load(&Stuck));
        assert_eq!(
            got,
            Err(InventoryError::ListingStalled {
                level: Level::ZERO,
                cursor: key(0, 1, 1).range(),
            })
        );
    }
}
