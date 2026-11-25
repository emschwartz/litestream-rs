use std::convert::Infallible;

use lsr_ltx::{
    DecodeError, Encoder, Event, Header, HeaderFields, PageNo, PageSize, Section, Timestamp, Txid,
    TxidRange,
};

use super::*;
use crate::conformance::block_on;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::store::WriteMode;
use crate::store::{StoreOp, Upload};

const PS: PageSize = PageSize::MIN;

fn key() -> FileKey {
    FileKey::new(Level::SNAPSHOT, TxidRange::single(Txid::MIN))
}

/// A snapshot of `pages` pages, page n filled with byte n.
fn snapshot(pages: u32) -> Vec<u8> {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PS,
        commit: pages,
        txids: TxidRange::single(Txid::MIN),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in 1..=pages {
        let fill = u8::try_from(n).unwrap();
        encoder
            .write_page(PageNo::new(n, PS).unwrap(), &[fill; 512])
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

fn stored(bytes: &[u8]) -> MemStore {
    let store = MemStore::new();
    block_on(async {
        let mut upload = store
            .begin_upload(&key(), Timestamp::from_unix_millis(1), WriteMode::Replace)
            .await
            .unwrap();
        upload.write(bytes).await.unwrap();
        upload.commit().await.unwrap();
    });
    store
}

fn len(n: usize) -> ReadLen {
    ReadLen::new(u32::try_from(n).unwrap()).unwrap()
}

/// Every page number the read handed out, and how it ended.
fn pages_of(
    store: &MemStore,
    read: &mut LtxRead,
) -> (Vec<u32>, Result<Verified, ReadFault<Infallible>>) {
    let mut pages = Vec::new();
    let end = block_on(read.read_verified(store, |event| {
        if let Event::Page(page) = event {
            pages.push(page.pgno().get());
        }
        Ok(())
    }));
    (pages, end)
}

#[test]
fn a_short_chunk_is_the_end_of_the_object() {
    let bytes = snapshot(3);
    let store = stored(&bytes);
    // The whole file as a chunk shorter than asked: no read follows, so the
    // failure queued for the next read is still there afterwards.
    store.inject_failure(StoreOp::Read, StoreError::NotFound { key: key() });
    let mut read = LtxRead::with_first(key(), len(bytes.len() + 1), bytes.clone());
    let (pages, end) = pages_of(&store, &mut read);
    assert_eq!(pages, [1, 2, 3]);
    assert_eq!(end.unwrap().page_count(), 3);
    assert!(
        block_on(store.read(&key(), 0, len(1))).is_err(),
        "the read stopped at the short chunk"
    );
    assert_eq!(read.bytes_read(), u64::try_from(bytes.len()).unwrap());

    // The same bytes as a full chunk are not the end: the reader asks for
    // more, and here the store has no such object.
    let mut read = LtxRead::with_first(key(), len(bytes.len()), bytes.clone());
    let (pages, end) = pages_of(&MemStore::new(), &mut read);
    assert_eq!(pages, [1, 2, 3]);
    assert!(matches!(
        end,
        Err(ReadFault::Store(StoreError::NotFound { .. }))
    ));
}

#[test]
fn small_chunks_read_the_whole_object_and_count_every_byte() {
    let bytes = snapshot(5);
    let store = stored(&bytes);
    for chunk in [1, 7, 512, bytes.len()] {
        let mut read = LtxRead::new(key(), len(chunk));
        let (pages, end) = pages_of(&store, &mut read);
        assert_eq!(pages, [1, 2, 3, 4, 5], "chunk {chunk}");
        assert_eq!(end.unwrap().page_count(), 5);
        assert_eq!(read.bytes_read(), u64::try_from(bytes.len()).unwrap());
        assert_eq!(read.held(), 0, "the last chunk is dropped at the end");
    }
}

#[test]
fn pages_of_a_corrupt_file_come_out_but_no_verified_end_does() {
    let mut bytes = snapshot(3);
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    let store = stored(&bytes);
    let mut read = LtxRead::new(key(), len(64));
    let (pages, end) = pages_of(&store, &mut read);
    assert_eq!(pages, [1, 2, 3], "staged, never to be committed");
    assert!(matches!(
        end,
        Err(ReadFault::Decode(DecodeError::FileChecksum { .. }))
    ));
    assert_eq!(read.header(), None, "a decode fault forgets the header");
}

#[test]
fn a_truncated_file_keeps_its_header_and_has_no_verified_end() {
    let bytes = snapshot(2);
    let store = stored(&bytes[..bytes.len() - 3]);
    let mut read = LtxRead::new(key(), len(64));
    let (pages, end) = pages_of(&store, &mut read);
    assert_eq!(pages, [1, 2]);
    assert!(matches!(
        end,
        Err(ReadFault::Decode(DecodeError::Truncated {
            section: Section::Trailer
        }))
    ));
    assert_eq!(read.header().map(Header::commit), Some(2));
}

#[test]
fn a_rejected_event_stops_the_read() {
    let store = stored(&snapshot(4));
    let mut read = LtxRead::new(key(), len(512));
    let mut seen = 0_u32;
    let end = block_on(read.read_verified(&store, |event| match event {
        Event::Header(_) => Ok(()),
        Event::Page(page) => {
            seen += 1;
            if page.pgno().get() == 2 {
                Err("no")
            } else {
                Ok(())
            }
        }
    }));
    assert!(matches!(end, Err(ReadFault::Rejected("no"))));
    assert_eq!(seen, 2);
    assert_eq!(read.key(), key());
}

#[test]
fn a_first_chunk_in_hand_is_not_read_again() {
    let bytes = snapshot(2);
    // Nothing is stored: any read is NotFound.
    let empty = MemStore::new();
    let mut read = LtxRead::with_first(key(), len(bytes.len() + 1), bytes.clone());
    let (pages, end) = pages_of(&empty, &mut read);
    assert_eq!(pages, [1, 2]);
    end.unwrap();
    assert_eq!(read.bytes_read(), u64::try_from(bytes.len()).unwrap());

    let store = stored(&bytes);
    let head = bytes[..100].to_vec();
    let mut read = LtxRead::with_first(key(), len(100), head);
    let (pages, end) = pages_of(&store, &mut read);
    assert_eq!(pages, [1, 2], "a full first chunk reads on from its end");
    end.unwrap();
}

#[test]
fn a_vanished_object_is_a_store_fault() {
    let mut read = LtxRead::new(key(), len(64));
    let (pages, end) = pages_of(&MemStore::new(), &mut read);
    assert!(pages.is_empty());
    assert!(matches!(
        end,
        Err(ReadFault::Store(StoreError::NotFound { .. }))
    ));
}

/// A store that counts the streams it opens and fails the read at `fail_at`, whether a
/// stream or a lone read asks it.
struct Streams {
    inner: MemStore,
    fail_at: Option<u64>,
    opened: std::sync::atomic::AtomicUsize,
    reads: std::sync::atomic::AtomicUsize,
}

impl Streams {
    fn new(inner: MemStore, fail_at: Option<u64>) -> Self {
        Self {
            inner,
            fail_at,
            opened: std::sync::atomic::AtomicUsize::new(0),
            reads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn opened(&self) -> usize {
        self.opened.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl ObjectStore for Streams {
    type Upload = <MemStore as ObjectStore>::Upload;

    /// Forwards to the store it wraps.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: crate::store::PageLimit,
    ) -> Result<crate::store::Listing, StoreError> {
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail_at == Some(offset) {
            return Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: std::io::ErrorKind::ConnectionReset,
            });
        }
        self.inner.read(key, offset, len).await
    }

    fn read_stream(&self, key: FileKey, offset: u64) -> Box<dyn ReadStream + '_> {
        self.opened
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::new(Pieces {
            store: self,
            key,
            offset,
        })
    }

    async fn created_at(
        &self,
        file: &crate::layout::FileInfo,
    ) -> Result<crate::layout::CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &crate::layout::FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, crate::layout::CreatedAt), StoreError> {
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
}

/// [`Streams`]'s stream: its pieces are its reads, in order.
struct Pieces<'a> {
    store: &'a Streams,
    key: FileKey,
    offset: u64,
}

impl ReadStream for Pieces<'_> {
    fn next(&mut self, len: ReadLen) -> crate::store::BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let bytes = self.store.read(&self.key, self.offset, len).await?;
            self.offset = self
                .offset
                .checked_add(u64::try_from(bytes.len()).unwrap())
                .unwrap();
            Ok(bytes)
        })
    }
}

fn streamed_pages(
    store: &Streams,
    read: &mut LtxRead,
) -> (Vec<u32>, Result<Verified, ReadFault<Infallible>>) {
    let mut pages = Vec::new();
    let end = block_on(read.read_streamed(store, |event| {
        if let Event::Page(page) = event {
            pages.push(page.pgno().get());
        }
        Ok(())
    }));
    (pages, end)
}

#[test]
fn a_streamed_read_opens_one_stream_and_feeds_the_decoder_what_reads_would() {
    let bytes = snapshot(40);
    let size = u64::try_from(bytes.len()).unwrap();
    let store = Streams::new(stored(&bytes), None);
    let mut read = LtxRead::new(key(), len(100));
    let (pages, end) = streamed_pages(&store, &mut read);
    assert_eq!(pages, (1..=40).collect::<Vec<_>>());
    assert_eq!(end.unwrap().page_count(), 40);
    assert_eq!(read.bytes_read(), size);
    assert!(size > 800, "the file spans many chunks");
    assert_eq!(store.opened(), 1, "one stream for the whole file");
    assert_eq!(
        store.reads(),
        usize::try_from(size.div_ceil(100)).unwrap(),
        "a piece per chunk, none past the short last one"
    );
    assert_eq!(read.held(), 0, "the last chunk is dropped at the end");
}

#[test]
fn a_read_a_chunk_at_a_time_opens_no_stream() {
    let bytes = snapshot(40);
    let store = Streams::new(stored(&bytes), None);
    let mut read = LtxRead::new(key(), len(100));
    let end = block_on(read.read_verified(&store, |_| Ok::<(), Infallible>(())));
    end.unwrap();
    assert_eq!(store.opened(), 0);
}

#[test]
fn a_stream_that_fails_part_way_is_a_store_fault_after_the_pages_before_it() {
    let bytes = snapshot(40);
    let store = Streams::new(stored(&bytes), Some(500));
    let mut read = LtxRead::new(key(), len(100));
    let (pages, end) = streamed_pages(&store, &mut read);
    assert!(matches!(
        end,
        Err(ReadFault::Store(StoreError::Transient { .. }))
    ));
    assert!(!pages.is_empty() && pages.len() < 40, "{pages:?}");
    assert_eq!(read.bytes_read(), 500);
}
