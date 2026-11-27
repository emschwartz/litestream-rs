//! A restore file read through a window of spans (`restore::window`): the pages are the ones one
//! stream gives, every byte is read once and in order, at most the store's width of spans is open,
//! and damage fails the file as it fails one stream.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::super::window::{SPAN_BYTES, Window};
use super::*;
use crate::layout::CreatedAt;
use crate::store::{BatchKind, ListAt, Listing, PageLimit, ReadStream};

const PS: u32 = 4096;
/// Enough 4 KiB pages for four spans and a part of a fifth.
const PAGES: u32 = 8_400;

/// Pages LZ4 cannot shrink, so the file is as large as the database.
fn noise(pgno: u32, salt: u32) -> Vec<u8> {
    let mut state = u64::from(pgno) << 32 | u64::from(salt) | 1;
    (0..PS)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            u8::try_from(state & 0xff).unwrap()
        })
        .collect()
}

/// A tracked file of `pages` (each page's bytes from `salt`), at transactions `min` to `max`.
fn encode(
    min: u64,
    max: u64,
    pages: &[u32],
    salt: u32,
    pre: Option<DbChecksum>,
) -> (Vec<u8>, DbChecksum) {
    let ps = size(PS);
    let header = Header::new(HeaderFields {
        tracked: true,
        page_size: ps,
        commit: PAGES,
        txids: range(min, max),
        timestamp: Timestamp::from_unix_millis(i64::try_from(max).unwrap() * 1000),
        pre_apply: pre,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut post = pre.unwrap_or(DbChecksum::EMPTY);
    for &pgno in pages {
        let page = PageNo::new(pgno, ps).unwrap();
        let data = noise(pgno, salt);
        if pre.is_some() {
            // The page it replaces: the snapshot's.
            post = post.toggle(PageChecksum::of(page, &noise(pgno, 0)));
        }
        post = post.toggle(PageChecksum::of(page, &data));
        encoder.write_page(page, &data).unwrap();
    }
    (encoder.finish(Some(post)).unwrap().0, post)
}

/// The snapshot every test restores: pages 1 to [`PAGES`].
fn snapshot() -> &'static [u8] {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    BYTES.get_or_init(|| encode(1, 1, &(1..=PAGES).collect::<Vec<_>>(), 0, None).0)
}

/// A memory store that reads up to `width` spans at once, logs each span it opens, counts the
/// spans open at once, and can patch bytes into a span the first `times` it is opened over them.
struct Spans {
    inner: MemStore,
    width: usize,
    opened: Mutex<Vec<(FileKey, u64, u64)>>,
    live: Arc<AtomicUsize>,
    most_live: Arc<AtomicUsize>,
    patches: Mutex<Vec<(u64, Vec<u8>, usize)>>,
    /// Added to every listed size: a listing that disagrees with the object.
    size_lie: i64,
}

impl Spans {
    fn new(inner: MemStore, width: usize) -> Self {
        Self {
            inner,
            width,
            opened: Mutex::new(Vec::new()),
            live: Arc::new(AtomicUsize::new(0)),
            most_live: Arc::new(AtomicUsize::new(0)),
            patches: Mutex::new(Vec::new()),
            size_lie: 0,
        }
    }

    fn opened(&self, key: FileKey) -> Vec<(u64, u64)> {
        let log = self.opened.lock().unwrap();
        log.iter()
            .filter(|o| o.0 == key)
            .map(|o| (o.1, o.2))
            .collect()
    }
}

/// A span with bytes patched in where it covers them.
struct Patched<'a> {
    inner: Box<dyn ReadStream + 'a>,
    offset: u64,
    patches: Vec<(u64, Vec<u8>)>,
    live: Arc<AtomicUsize>,
}

impl Drop for Patched<'_> {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl ReadStream for Patched<'_> {
    fn next(&mut self, len: ReadLen) -> crate::store::BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let mut bytes = self.inner.next(len).await?;
            for (at, patch) in &self.patches {
                for (i, byte) in patch.iter().enumerate() {
                    let pos = at + u64::try_from(i).unwrap();
                    if pos >= self.offset && pos < self.offset + u64::try_from(bytes.len()).unwrap()
                    {
                        bytes[usize::try_from(pos - self.offset).unwrap()] = *byte;
                    }
                }
            }
            self.offset += u64::try_from(bytes.len()).unwrap();
            Ok(bytes)
        })
    }
}

impl ObjectStore for Spans {
    type Upload = <MemStore as ObjectStore>::Upload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let mut listing = self.inner.list(level, after, limit).await?;
        for file in &mut listing.files {
            file.size = file.size.checked_add_signed(self.size_lie).unwrap();
        }
        Ok(listing)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.inner.read(key, offset, len).await
    }

    fn read_span(&self, key: FileKey, start: u64, end: u64) -> Box<dyn ReadStream + '_> {
        self.opened.lock().unwrap().push((key, start, end));
        let mut patches = Vec::new();
        for (at, bytes, times) in self.patches.lock().unwrap().iter_mut() {
            if *at >= start && *at < end && *times > 0 {
                *times -= 1;
                patches.push((*at, bytes.clone()));
            }
        }
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_live.fetch_max(live, Ordering::SeqCst);
        Box::new(Patched {
            inner: self.inner.read_span(key, start, end),
            offset: start,
            patches,
            live: Arc::clone(&self.live),
        })
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
    ) -> Result<Self::Upload, StoreError> {
        self.inner
            .begin_sized_upload(key, created_at, mode, len)
            .await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        match kind {
            BatchKind::Spans => NonZeroUsize::new(self.width).unwrap(),
            BatchKind::Lists | BatchKind::ResolvedCreatedAt => NonZeroUsize::MIN,
        }
    }

    async fn list_many(
        &self,
        pages: &[ListAt],
        limit: PageLimit,
    ) -> Vec<Result<Listing, StoreError>> {
        let mut out = Vec::new();
        for at in pages {
            out.push(self.list(at.level, at.after, limit).await);
        }
        out
    }
}

fn replica(files: &[(u8, u64, u64, &[u8])]) -> MemStore {
    let store = MemStore::new();
    for (level, min, max, bytes) in files {
        put(&store, *level, *min, *max, bytes);
    }
    store
}

fn restored(store: &impl ObjectStore) -> (Result<Restored, RestoreError>, Option<Database>) {
    let sink = MemSink::new();
    let result = block_on(restore(store, RestoreTarget::Latest, sink.clone()));
    (result, sink.published())
}

/// The spans opened over `key`, and that they run back to back from byte 0 in steps of a span.
fn tiled(store: &Spans, key: FileKey) -> usize {
    let opened = store.opened(key);
    for (index, (start, end)) in opened.iter().enumerate() {
        assert_eq!(
            *start,
            u64::try_from(index).unwrap() * SPAN_BYTES,
            "{opened:?}"
        );
        assert_eq!(end - start, SPAN_BYTES, "{opened:?}");
    }
    opened.len()
}

#[test]
fn a_window_gives_the_pages_one_stream_gives() {
    let bytes = snapshot();
    let one = Spans::new(replica(&[(9, 1, 1, bytes)]), 1);
    let (whole, whole_db) = restored(&one);
    whole.unwrap();
    assert!(
        one.opened(key(9, 1, 1)).is_empty(),
        "width one reads one stream"
    );
    let many = Spans::new(replica(&[(9, 1, 1, bytes)]), 8);
    let (windowed, windowed_db) = restored(&many);
    let windowed = windowed.unwrap();
    assert_eq!(windowed_db, whole_db);
    assert_eq!(windowed_db.unwrap().page(PAGES).unwrap(), noise(PAGES, 0));
    // Four whole spans, then the rest read to the object's end as one stream.
    let size = u64::try_from(bytes.len()).unwrap();
    assert_eq!(
        tiled(&many, key(9, 1, 1)),
        usize::try_from(size / SPAN_BYTES).unwrap()
    );
    assert_eq!(windowed.bytes_read(), size, "every byte read once");
}

#[test]
fn a_window_holds_at_most_the_stores_width_of_spans() {
    let bytes = snapshot();
    for width in [2, 3] {
        let store = Spans::new(replica(&[(9, 1, 1, bytes)]), width);
        restored(&store).0.unwrap();
        assert_eq!(store.most_live.load(Ordering::SeqCst), width);
        assert_eq!(store.live.load(Ordering::SeqCst), 0);
        assert_eq!(tiled(&store, key(9, 1, 1)), 4);
    }
}

#[test]
fn a_file_under_two_spans_is_one_stream() {
    let every: Vec<u32> = (1..=PAGES).collect();
    let pre = encode(1, 1, &every, 0, None).1;
    let few: Vec<u32> = (1..4_000).collect();
    let (bytes, _) = encode(2, 2, &few, 5, Some(pre));
    assert!(u64::try_from(bytes.len()).unwrap() < 2 * SPAN_BYTES);
    let store = Spans::new(replica(&[(9, 1, 1, snapshot()), (1, 2, 2, &bytes)]), 8);
    let (result, db) = restored(&store);
    result.unwrap();
    assert!(store.opened(key(1, 2, 2)).is_empty());
    assert_eq!(db.unwrap().page(7).unwrap(), noise(7, 5));
}

#[test]
fn a_file_of_scattered_pages_is_read_through_a_window_too() {
    let base = snapshot();
    let every: Vec<u32> = (1..=PAGES).collect();
    let pre = encode(1, 1, &every, 0, None).1;
    let odd: Vec<u32> = (1..=PAGES).filter(|p| p % 2 == 1).collect();
    let (scattered, _) = encode(2, 2, &odd, 3, Some(pre));
    let files: [(u8, u64, u64, &[u8]); 2] = [(9, 1, 1, base), (1, 2, 2, &scattered)];
    let reference = restored(&Spans::new(replica(&files), 1)).1.unwrap();
    let store = Spans::new(replica(&files), 4);
    let (result, db) = restored(&store);
    result.unwrap();
    assert_eq!(tiled(&store, key(1, 2, 2)), 2);
    let db = db.unwrap();
    assert_eq!(db, reference);
    assert_eq!(db.page(3).unwrap(), noise(3, 3));
    assert_eq!(db.page(4).unwrap(), noise(4, 0));
}

#[test]
fn a_damaged_byte_in_a_span_fails_the_file_as_one_stream_does() {
    let bytes = snapshot();
    let at = SPAN_BYTES + 12_345;
    let flipped = bytes[usize::try_from(at).unwrap()] ^ 0x5a;
    let mut damaged = bytes.to_vec();
    damaged[usize::try_from(at).unwrap()] = flipped;
    let (alone, _) = restored(&Spans::new(replica(&[(9, 1, 1, &damaged)]), 1));
    let store = Spans::new(replica(&[(9, 1, 1, bytes)]), 4);
    store.patches.lock().unwrap().push((at, vec![flipped], 1));
    let (result, db) = restored(&store);
    assert!(
        matches!(&result, Err(RestoreError::File { .. })),
        "{result:?}"
    );
    assert_eq!(format!("{result:?}"), format!("{alone:?}"));
    assert_eq!(db, None);
}

/// The listed size only plans the spans: an object longer or shorter than listed, by less or more
/// than a span, restores as one stream restores it.
#[test]
fn an_object_that_is_not_its_listed_size_restores_whole() {
    let bytes = snapshot();
    let reference = restored(&Spans::new(replica(&[(9, 1, 1, bytes)]), 1))
        .1
        .unwrap();
    let span = i64::try_from(SPAN_BYTES).unwrap();
    for lie in [-1, 1, -span - 5, span + 5] {
        let mut store = Spans::new(replica(&[(9, 1, 1, bytes)]), 3);
        store.size_lie = lie;
        let (result, db) = restored(&store);
        result.unwrap();
        assert_eq!(db.unwrap(), reference, "listed {lie} off");
    }
}

/// The window's bytes, taken `len` at a time, until a short piece.
fn drain(window: &mut Window<'_, MemStore>, len: u32) -> Vec<u8> {
    let len = ReadLen::new(len).unwrap();
    let mut out = Vec::new();
    loop {
        let piece = block_on(window.next(len)).unwrap();
        let short = u64::try_from(piece.len()).unwrap() < len.get();
        out.extend(piece);
        if short {
            return out;
        }
    }
}

proptest! {
    /// Whatever the object's size, the listed size, the span, the width and the pieces asked, the
    /// window hands out the object's bytes in order and nothing else.
    #[test]
    fn a_window_hands_out_the_object_in_order(
        size in 1_usize..3_000,
        listed in 1_u64..3_200,
        span in 1_u64..400,
        width in 1_usize..6,
        len in 1_u32..700,
    ) {
        let bytes: Vec<u8> = (0..size).map(|i| u8::try_from(i % 251).unwrap()).collect();
        let store = replica(&[(9, 1, 1, &bytes)]);
        let mut window = Window::new(&store, key(9, 1, 1), listed, width, span);
        prop_assert_eq!(drain(&mut window, len), bytes);
    }
}
