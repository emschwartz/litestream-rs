//! The store both request gates restore from: `gates` here runs it under the retry layer, and
//! `crates/replica/tests/replica/gates.rs` includes this file to run it through the public
//! restore entry.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    BoxFuture, CreatedAt, FileInfo, FileKey, Level, Listing, MemStore, ObjectStore, PageLimit,
    ReadLen, ReadStream, StoreError, Upload,
};
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};

/// A store that counts the requests a bucket would bill: a lone read is one, and a stream is
/// one however many pieces are taken from it.
pub struct Waves {
    inner: MemStore,
    /// Requests: lone reads and streams opened.
    pub requests: Arc<AtomicUsize>,
    /// Pieces taken from streams.
    pub pieces: Arc<AtomicUsize>,
}

/// Bytes of one restore read: core's `restore::CHUNK_BYTES`.
pub const RESTORE_CHUNK: usize = 4 * 1024 * 1024;

impl Waves {
    pub fn new(inner: MemStore) -> Self {
        Self {
            inner,
            requests: Arc::default(),
            pieces: Arc::default(),
        }
    }
}

struct Stream<'a> {
    store: &'a Waves,
    key: FileKey,
    offset: u64,
}

impl ReadStream for Stream<'_> {
    fn next(&mut self, len: ReadLen) -> BoxFuture<'_, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            self.store.pieces.fetch_add(1, Ordering::SeqCst);
            let bytes = self.store.inner.read(&self.key, self.offset, len).await?;
            self.offset = self
                .offset
                .checked_add(u64::try_from(bytes.len()).unwrap())
                .unwrap();
            Ok(bytes)
        })
    }
}

impl ObjectStore for Waves {
    type Upload = <MemStore as ObjectStore>::Upload;

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
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.read(key, offset, len).await
    }

    fn read_stream(&self, key: FileKey, offset: u64) -> Box<dyn ReadStream + '_> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        Box::new(Stream {
            store: self,
            key,
            offset,
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
}

/// Stores, under level 0, the file of transaction `n` of a 2600-page database holding its first
/// `pages` pages, each filled with `n`.
pub fn put_increment(store: &MemStore, n: u64, pages: u32) {
    let size = PageSize::new(16384).unwrap();
    let txid = Txid::new(n).unwrap();
    let range = TxidRange::single(txid);
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit: 2600,
        txids: range,
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let fill = vec![u8::try_from(n).unwrap(); size.bytes()];
    for at in 1..=pages {
        encoder
            .write_page(PageNo::new(at, size).unwrap(), &fill)
            .unwrap();
    }
    let bytes = encoder.finish(None).unwrap().0;
    block_on(async {
        let mut up = store
            .begin_upload(
                &FileKey::new(Level::ZERO, range),
                Timestamp::from_unix_millis(1_700_000_000_000),
                WriteMode::Replace,
            )
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap();
    });
}

/// One snapshot of 2600 random 16 KiB pages (about 42 MiB, 11 restore chunks), stored.
pub fn snapshot_store() -> (MemStore, usize) {
    let size = PageSize::new(16384).unwrap();
    let key = FileKey::snapshot(Txid::new(1).unwrap());
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit: 2600,
        txids: TxidRange::new(Txid::new(1).unwrap(), Txid::new(1).unwrap()).unwrap(),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for at in 1..=2600_u32 {
        let mut page = Vec::with_capacity(size.bytes());
        while page.len() < size.bytes() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            page.extend_from_slice(&state.to_le_bytes());
        }
        encoder
            .write_page(PageNo::new(at, size).unwrap(), &page)
            .unwrap();
    }
    let bytes = encoder.finish(None).unwrap().0;
    let store = MemStore::new();
    block_on(async {
        let mut up = store
            .begin_upload(
                &key,
                Timestamp::from_unix_millis(1_700_000_000_000),
                WriteMode::Replace,
            )
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap();
    });
    (store, bytes.len())
}
