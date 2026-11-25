//! The local course's view of staging (`actor/apart.rs`) and the move of its files up to
//! level 0.

use super::second_writer::{put_whole, read_whole};
use super::*;
use crate::actor::apart::{Apart, raise_course};
use crate::fault::role::Staging;

fn key(level: Level, n: u64) -> FileKey {
    FileKey::new(level, TxidRange::single(Txid::new(n).unwrap()))
}

fn l0(n: u64) -> FileKey {
    key(Level::ZERO, n)
}

fn slot(n: u64) -> FileKey {
    key(Level::LOCAL_COURSE, n)
}

fn base(n: u64) -> Option<Txid> {
    Txid::new(n)
}

/// A store with `level_zero` and `in_slot` files, each holding its place and number as bytes.
fn store(level_zero: &[u64], in_slot: &[u64]) -> MemStore {
    let store = MemStore::new();
    for &n in level_zero {
        put_whole(&store, &l0(n), format!("level 0, {n}").as_bytes());
    }
    for &n in in_slot {
        put_whole(&store, &slot(n), format!("slot, {n}").as_bytes());
    }
    store
}

fn held(store: &MemStore, level: Level) -> Vec<u64> {
    block_on(store.list(level, None, PageLimit::MAX))
        .unwrap()
        .files
        .iter()
        .map(|file| file.key.range().max().get())
        .collect()
}

/// A store that counts the writes aborted in it; everything else is the store it wraps.
struct Aborts {
    inner: MemStore,
    aborted: Arc<AtomicU32>,
}

struct CountedUpload {
    inner: <MemStore as ObjectStore>::Upload,
    aborted: Arc<AtomicU32>,
}

impl Upload for CountedUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.inner.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        self.inner.commit().await
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.aborted.fetch_add(1, Ordering::SeqCst);
        self.inner.abort().await
    }
}

impl ObjectStore for Aborts {
    type Upload = CountedUpload;

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.inner.read(key, offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.inner.read_with_created_at(file, len).await
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<CountedUpload, StoreError> {
        Ok(CountedUpload {
            inner: self
                .inner
                .begin_sized_upload(key, created_at, mode, len)
                .await?,
            aborted: Arc::clone(&self.aborted),
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// Every page of level 0 through `view`, `limit` files at a time, following its cursors.
fn paged(view: &Apart<'_, MemStore>, limit: u16) -> Vec<FileKey> {
    let limit = PageLimit::new(limit).unwrap();
    let (mut after, mut all) = (None, Vec::new());
    loop {
        let page = block_on(view.list(Level::ZERO, after, limit)).unwrap();
        assert!(page.files.len() <= limit.get());
        all.extend(page.files.iter().map(|file| file.key));
        match page.next {
            Some(next) => {
                assert!(
                    after.is_none_or(|was: TxidRange| next.max() > was.max()),
                    "the cursor advances"
                );
                after = Some(next);
            }
            None => return all,
        }
    }
}

/// Level 0 through the base, then the slot's files above it, each under a level-0 key, at
/// any page size. A slot file at or below the base (a copy a boot left after it moved the
/// file up) is not the course's.
#[test]
fn level_zero_is_the_base_and_below_then_the_slot_above_it() {
    let store = store(&[1, 2, 3], &[2, 3, 4, 5]);
    let view = Apart::over(&store, base(3));
    let want: Vec<FileKey> = (1..=5).map(l0).collect();
    for limit in [1, 2, 3, 4, 1000] {
        assert_eq!(paged(&view, limit), want, "pages of {limit}");
    }
    let read = |n| block_on(view.read(&l0(n), 0, ReadLen::CHUNK)).unwrap();
    assert_eq!(read(3), b"level 0, 3");
    assert_eq!(read(4), b"slot, 4");
}

/// The last page of level 0 names a next page only when the slot holds a file above the
/// base: a caller never fetches an empty trailing page.
#[test]
fn a_slot_with_nothing_above_the_base_adds_no_page() {
    for in_slot in [&[][..], &[1, 2][..]] {
        let store = store(&[1, 2], in_slot);
        let view = Apart::over(&store, base(2));
        let page = block_on(view.list(Level::ZERO, None, PageLimit::MAX)).unwrap();
        assert_eq!(page.files.len(), 2);
        assert_eq!(page.next, None, "{in_slot:?}");
    }
    // A full page of level 0 with more of it to come is level 0's own page, cursor and all.
    let store = store(&[1, 2], &[3]);
    let view = Apart::over(&store, base(2));
    let page = block_on(view.list(Level::ZERO, None, PageLimit::ONE)).unwrap();
    assert_eq!(
        page.next,
        Some(TxidRange::single(Txid::new(1).unwrap())),
        "{page:?}"
    );
}

/// With no level-0 file as the course began, every file is the slot's.
#[test]
fn with_no_base_every_file_is_the_slots() {
    let store = store(&[], &[1, 2]);
    let view = Apart::over(&store, None);
    assert_eq!(paged(&view, 1), vec![l0(1), l0(2)]);
    assert_eq!(
        block_on(view.read(&l0(1), 0, ReadLen::CHUNK)).unwrap(),
        b"slot, 1"
    );
    assert!(paged(&Apart::over(&MemStore::new(), None), 1).is_empty());
}

/// An abort reaches the store, which is where a real one frees what the write took.
#[test]
fn an_abort_reaches_the_store() {
    let store = Aborts {
        inner: store(&[1], &[]),
        aborted: Arc::default(),
    };
    let view = Apart::over(&store, base(1));
    block_on(async {
        let mut upload = view
            .begin_upload(&l0(2), ts(T0), WriteMode::New)
            .await
            .unwrap();
        upload.write(b"gone").await.unwrap();
        upload.abort().await.unwrap();
    });
    assert_eq!(store.aborted.load(Ordering::SeqCst), 1);
    assert_eq!(held(&store.inner, Level::LOCAL_COURSE), Vec::<u64>::new());
}

/// A write, a stat and a delete above the base go to the slot, and the commit names the file
/// as the course does; at or below the base they are level 0's.
#[test]
fn calls_above_the_base_go_to_the_slot() {
    let store = store(&[1, 2, 3], &[]);
    let view = Apart::over(&store, base(3));
    let stamp = ts(T0 + 5);
    let put = |key: FileKey| {
        block_on(async {
            let mut upload = view.begin_upload(&key, stamp, WriteMode::New).await?;
            upload.write(b"new").await?;
            upload.commit().await
        })
    };
    let info = put(l0(4)).unwrap();
    assert_eq!(info.key, l0(4));
    assert_eq!(
        (held(&store, Level::ZERO), held(&store, Level::LOCAL_COURSE)),
        (vec![1, 2, 3], vec![4])
    );
    assert_eq!(block_on(view.created_at(&info)).unwrap().at(), stamp);
    let (bytes, at) = block_on(view.read_with_created_at(&info, ReadLen::CHUNK)).unwrap();
    assert_eq!((bytes.as_slice(), at.at()), (&b"new"[..], stamp));
    // At the base: level 0's key, where a file is already.
    put(l0(3)).unwrap_err();
    assert_eq!(read_whole(&store, &l0(3)), b"level 0, 3");
    // An aborted write leaves nothing.
    block_on(async {
        let mut upload = view
            .begin_upload(&l0(5), stamp, WriteMode::New)
            .await
            .unwrap();
        upload.write(b"gone").await.unwrap();
        upload.abort().await.unwrap();
    });
    assert_eq!(held(&store, Level::LOCAL_COURSE), vec![4]);
    block_on(view.delete(&[l0(4), l0(3)])).unwrap();
    assert_eq!(
        (held(&store, Level::ZERO), held(&store, Level::LOCAL_COURSE)),
        (vec![1, 2], vec![])
    );
}

/// Every other level passes through as it is, the slot's own included.
#[test]
fn other_levels_pass_through() {
    let store = store(&[1], &[2]);
    put_whole(&store, &key(Level::SNAPSHOT, 7), b"snapshot");
    let view = Apart::over(&store, base(1));
    for level in [Level::SNAPSHOT, Level::LOCAL_COURSE] {
        assert_eq!(
            block_on(view.list(level, None, PageLimit::MAX)).unwrap(),
            block_on(store.list(level, None, PageLimit::MAX)).unwrap()
        );
    }
    assert_eq!(
        block_on(view.read(&key(Level::SNAPSHOT, 7), 0, ReadLen::CHUNK)).unwrap(),
        b"snapshot"
    );
    store.plant_scratch(3);
    assert_eq!(block_on(view.remove_scratch()).unwrap(), 3);
    assert_eq!(store.scratch(), 0);
}

/// The slot's files move up to level 0 with their bytes and the time they were staged
/// under, whatever their size against the read chunk, and the slot is left empty.
#[test]
fn the_slots_files_move_up_with_their_bytes_and_times() {
    let chunk = usize::try_from(ReadLen::CHUNK.get()).unwrap();
    let store = store(&[1], &[]);
    let files = [
        (2, vec![2_u8; 10]),
        (3, vec![3_u8; chunk]),
        (4, vec![4_u8; chunk + 7]),
        (5, Vec::new()),
    ];
    let view = Apart::over(&store, base(1));
    for (n, bytes) in &files {
        block_on(async {
            let at = ts(T0 + i64::try_from(*n).unwrap());
            let mut upload = view
                .begin_upload(&l0(*n), at, WriteMode::New)
                .await
                .unwrap();
            upload.write(bytes).await.unwrap();
            upload.commit().await.unwrap();
        });
    }
    assert_eq!(held(&store, Level::LOCAL_COURSE), vec![2, 3, 4, 5]);
    block_on(raise_course(&Staging(&store))).unwrap();
    assert_eq!(held(&store, Level::ZERO), vec![1, 2, 3, 4, 5]);
    assert!(held(&store, Level::LOCAL_COURSE).is_empty());
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    for file in block_on(store.list(Level::ZERO, None, PageLimit::MAX))
        .unwrap()
        .files
    {
        let n = file.key.range().max().get();
        let Some((_, bytes)) = files.iter().find(|(m, _)| *m == n) else {
            continue;
        };
        assert_eq!(
            &block_on(store.read(&file.key, 0, all)).unwrap(),
            bytes,
            "{n}"
        );
        let at = block_on(store.created_at(&file)).unwrap().at();
        assert_eq!(at, ts(T0 + i64::try_from(n).unwrap()), "{n}");
    }
}

/// A boot attempt that died between the copy and the delete left the file in both places:
/// the next one compares them and deletes the slot's. A level-0 copy that differs is not the
/// file: it is deleted, the attempt fails, and the next one copies again.
#[test]
fn a_copy_left_behind_is_compared_before_the_slots_file_goes() {
    let store = store(&[1], &[2, 3]);
    put_whole(&store, &l0(2), b"slot, 2");
    put_whole(&store, &l0(3), b"not it");
    assert_eq!(
        block_on(raise_course(&Staging(&store))),
        Err(Fault::LocalIo)
    );
    assert_eq!(
        (held(&store, Level::ZERO), held(&store, Level::LOCAL_COURSE)),
        (vec![1, 2], vec![3])
    );
    block_on(raise_course(&Staging(&store))).unwrap();
    assert_eq!(
        (held(&store, Level::ZERO), held(&store, Level::LOCAL_COURSE)),
        (vec![1, 2, 3], vec![])
    );
    assert_eq!(read_whole(&store, &l0(3)), b"slot, 3");
}

/// A store call that fails part way leaves the slot's file where it was and no part of a
/// copy, and the next attempt finishes.
#[test]
fn a_failed_move_leaves_the_slots_file() {
    for op in [
        StoreOp::Read,
        StoreOp::Upload,
        StoreOp::Delete,
        StoreOp::List,
    ] {
        let store = store(&[1], &[2]);
        store.inject_failure(op, transient(op));
        assert!(block_on(raise_course(&Staging(&store))).is_err(), "{op:?}");
        assert_eq!(held(&store, Level::LOCAL_COURSE), vec![2], "{op:?}");
        block_on(raise_course(&Staging(&store))).unwrap();
        assert_eq!(
            (held(&store, Level::ZERO), held(&store, Level::LOCAL_COURSE)),
            (vec![1, 2], vec![])
        );
        assert_eq!(read_whole(&store, &l0(2)), b"slot, 2", "{op:?}");
    }
}
