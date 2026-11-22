//! Read-ahead survives the store wrappers.
//!
//! A restore reads a file's chunks side by side only if every layer between it and
//! the store forwards the store's batch width and its batch read. A wrapper that
//! forgets either one turns a restore serial without changing a single answer, so
//! nothing but the critical-path depth shows it. Here a store that models a link on a
//! virtual clock sits under the retry layer, and the restore's waves are held to
//! `ceil(chunks / width)`. A serial restore is one wave per chunk (11 here).

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    BatchKind, CreatedAt, FileInfo, FileKey, Level, Listing, MemSink, MemStore, ObjectStore,
    PageLimit, ReadAt, ReadLen, RecordingSleeper, RestoreTarget, RetryPolicy, Retrying,
    SeededEntropy, StoreError, Upload, restore,
};
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};

/// A store that models a link on a virtual clock: a lone read is one round trip, and a
/// batch of `n` reads at the store's width is `ceil(n / width)` waves. It answers from
/// memory, so the run is deterministic and nothing sleeps.
struct Waves {
    inner: MemStore,
    rounds: Arc<AtomicUsize>,
    reads: Arc<AtomicUsize>,
}

/// Ranged reads in flight at once, as the S3 store designs it.
const WIDTH: usize = 8;

/// Bytes of one restore read: core's `restore::CHUNK_BYTES`.
const RESTORE_CHUNK: usize = 4 * 1024 * 1024;

impl Waves {
    fn new(inner: MemStore) -> Self {
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
fn snapshot_store() -> (MemStore, usize) {
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

#[test]
fn restore_through_the_retry_layer_reads_side_by_side() {
    let (inner, size) = snapshot_store();
    let waves = Waves::new(inner);
    let (rounds, reads) = (Arc::clone(&waves.rounds), Arc::clone(&waves.reads));
    let store = Retrying::new(
        waves,
        RecordingSleeper::new(),
        SeededEntropy::new(1),
        RetryPolicy::DEFAULT,
    );
    let restored = block_on(restore(&store, RestoreTarget::Latest, MemSink::new())).unwrap();
    assert_eq!(restored.files(), 1);
    let chunks = size.div_ceil(RESTORE_CHUNK);
    assert_eq!(reads.load(Ordering::SeqCst), chunks, "one read per chunk");
    let bound = chunks.div_ceil(WIDTH);
    let seen = rounds.load(Ordering::SeqCst);
    assert!(
        seen <= bound,
        "{seen} waves, bound {bound} for {chunks} chunks"
    );
}
