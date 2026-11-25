//! The store both read-ahead gates restore from: `gates` here runs it under the retry layer, and
//! `crates/replica/tests/replica/gates.rs` includes this file to run it through the public
//! restore entry.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    BatchKind, CreatedAt, FileInfo, FileKey, Level, Listing, MemStore, ObjectStore, PageLimit,
    ReadAt, ReadLen, StoreError, Upload,
};
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};

/// A store that models a link on a virtual clock: a lone read is one round trip, and a
/// batch of `n` reads at the store's width is `ceil(n / width)` waves. It answers from
/// memory, so the run is deterministic and nothing sleeps.
pub struct Waves {
    inner: MemStore,
    pub rounds: Arc<AtomicUsize>,
    pub reads: Arc<AtomicUsize>,
}

/// Ranged reads in flight at once, as the S3 store designs it.
pub const WIDTH: usize = 8;

/// Bytes of one restore read: core's `restore::CHUNK_BYTES`.
pub const RESTORE_CHUNK: usize = 4 * 1024 * 1024;

impl Waves {
    pub fn new(inner: MemStore) -> Self {
        Self {
            inner,
            rounds: Arc::default(),
            reads: Arc::default(),
        }
    }

    fn wave(&self, requests: usize) {
        self.rounds
            .fetch_add(requests.div_ceil(WIDTH), Ordering::SeqCst);
        self.reads.fetch_add(requests, Ordering::SeqCst);
    }
}

impl ObjectStore for Waves {
    type Upload = <MemStore as ObjectStore>::Upload;

    /// Forwards to the store it wraps.
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
        self.rounds.fetch_add(1, Ordering::SeqCst);
        self.reads.fetch_add(1, Ordering::SeqCst);
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

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        self.inner.begin_upload(key, created_at, mode).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        match kind {
            BatchKind::Reads => NonZeroUsize::new(WIDTH).unwrap(),
            BatchKind::Lists | BatchKind::ResolvedCreatedAt => NonZeroUsize::MIN,
        }
    }

    async fn read_many(&self, reads: &[ReadAt]) -> Vec<Result<Vec<u8>, StoreError>> {
        self.wave(reads.len());
        let mut out = Vec::new();
        for at in reads {
            out.push(self.inner.read(&at.key, at.offset, at.len).await);
        }
        out
    }
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
