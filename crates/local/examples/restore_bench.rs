//! Restore benchmark: `restore_bench <mode> [pages] [latency_ms] [mb_per_s]`.
//! Modes: full (MemStore to FileSink), decode, crc, lz4, slow (latency store, null sink).
//! The last stdout line is the run's milliseconds.
#![allow(clippy::all, clippy::pedantic, clippy::restriction, missing_docs)]

use std::io;
use std::time::{Duration, Instant};

use lsr_core::conformance::block_on;
use lsr_core::{
    DatabaseSink, FileInfo, FileKey, Level, Listing, MemStore, ObjectStore, PageLimit, ReadLen,
    RestoreTarget, StoreError, StoredStamp, Upload, restore,
};
use lsr_ltx::*;
use lsr_local::FileSink;

const PS: u32 = 16384;

fn page_bytes(n: u32) -> Vec<u8> {
    // ~2.3x compressible: half pseudo-text, a quarter zeros, quarter random.
    let mut state = u64::from(n).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut p = vec![0_u8; PS as usize];
    let q = PS as usize / 4;
    for b in &mut p[q..3 * q] {
        *b = b"etaoin shrdlu cmfwyp vbgkqj"[(next() % 27) as usize];
    }
    for b in &mut p[3 * q..] {
        *b = next() as u8;
    }
    p
}

fn build(pages: u32, tracked: bool) -> Vec<u8> {
    let ps = PageSize::new(PS).unwrap();
    let h = Header::new(HeaderFields {
        tracked,
        page_size: ps,
        commit: pages,
        txids: TxidRange::new(Txid::new(1).unwrap(), Txid::new(1).unwrap()).unwrap(),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut enc = Encoder::new(Vec::new(), h).unwrap();
    let mut db = DbChecksum::EMPTY;
    for n in 1..=pages {
        let Some(pgno) = PageNo::new(n, ps) else { continue };
        let data = page_bytes(n);
        enc.write_page(pgno, &data).unwrap();
        db = db.toggle(PageChecksum::of(pgno, &data));
    }
    enc.finish(tracked.then_some(db)).unwrap().0
}

struct Slow {
    inner: MemStore,
    latency: Duration,
    ns_per_byte: f64,
}
impl ObjectStore for Slow {
    type Upload = <MemStore as ObjectStore>::Upload;
    async fn list(&self, l: Level, a: Option<TxidRange>, p: PageLimit) -> Result<Listing, StoreError> {
        self.inner.list(l, a, p).await
    }
    async fn read(&self, k: &FileKey, o: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let v = self.inner.read(k, o, len).await?;
        std::thread::sleep(self.latency + Duration::from_nanos((v.len() as f64 * self.ns_per_byte) as u64));
        Ok(v)
    }
    async fn stamp(&self, f: &FileInfo) -> Result<StoredStamp, StoreError> { self.inner.stamp(f).await }
    async fn read_with_stamp(&self, f: &FileInfo, l: ReadLen) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        self.inner.read_with_stamp(f, l).await
    }
    async fn begin_upload(&self, k: &FileKey, c: Timestamp) -> Result<Self::Upload, StoreError> {
        self.inner.begin_upload(k, c).await
    }
    async fn delete(&self, k: &[FileKey]) -> Result<(), StoreError> { self.inner.delete(k).await }
}

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, mpsc};

type Chunk = Result<Vec<u8>, StoreError>;
struct Stream { key: FileKey, len: ReadLen, next: u64, pending: VecDeque<(u64, mpsc::Receiver<Chunk>)> }
struct ReadAhead<S> { inner: Arc<S>, window: usize, state: Mutex<Option<Stream>> }
impl<S: ObjectStore + 'static> ReadAhead<S> {
    fn top_up(&self, st: &mut Stream) {
        while st.pending.len() < self.window {
            let (tx, rx) = mpsc::sync_channel(1);
            let (inner, key, off, len) = (Arc::clone(&self.inner), st.key, st.next, st.len);
            std::thread::spawn(move || { let _ = tx.send(block_on(inner.read(&key, off, len))); });
            st.pending.push_back((off, rx));
            st.next += u64::from(len.get());
        }
    }
}
impl<S: ObjectStore + 'static> ObjectStore for ReadAhead<S> {
    type Upload = S::Upload;
    async fn list(&self, l: Level, a: Option<TxidRange>, p: PageLimit) -> Result<Listing, StoreError> { self.inner.list(l, a, p).await }
    async fn read(&self, k: &FileKey, o: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let mut guard = self.state.lock().unwrap();
        let fresh = match &*guard {
            Some(st) => !(st.key == *k && st.len == len && st.pending.front().map(|f| f.0) == Some(o)),
            None => true,
        };
        if fresh {
            let mut st = Stream { key: *k, len, next: o, pending: VecDeque::new() };
            self.top_up(&mut st);
            *guard = Some(st);
        }
        let st = guard.as_mut().unwrap();
        let (_, rx) = st.pending.pop_front().unwrap();
        let got = rx.recv().unwrap()?;
        if got.len() < len.get() as usize { *guard = None } else { let st = guard.as_mut().unwrap(); self.top_up(st); }
        Ok(got)
    }
    async fn stamp(&self, f: &FileInfo) -> Result<StoredStamp, StoreError> { self.inner.stamp(f).await }
    async fn read_with_stamp(&self, f: &FileInfo, l: ReadLen) -> Result<(Vec<u8>, StoredStamp), StoreError> { self.inner.read_with_stamp(f, l).await }
    async fn begin_upload(&self, k: &FileKey, c: Timestamp) -> Result<Self::Upload, StoreError> { self.inner.begin_upload(k, c).await }
    async fn delete(&self, k: &[FileKey]) -> Result<(), StoreError> { self.inner.delete(k).await }
}

struct Null;
impl DatabaseSink for Null {
    fn write_page(&mut self, _: PageRef<'_>) -> io::Result<()> { Ok(()) }
    fn truncate(&mut self, _: DbPages, _: PageSize) -> io::Result<()> { Ok(()) }
    fn reset(&mut self) -> io::Result<()> { Ok(()) }
    fn checksum(&mut self, _: DbPages, _: PageSize) -> io::Result<DbChecksum> { Ok(DbChecksum::EMPTY) }
    type PublishError = std::convert::Infallible;
    fn publish(self) -> Result<(), Self::PublishError> { Ok(()) }
}

fn put(store: &MemStore, bytes: &[u8]) {
    block_on(async {
        let key = FileKey::snapshot(Txid::new(1).unwrap());
        let mut up = store.begin_upload(&key, Timestamp::from_unix_millis(1_700_000_000_000)).await.unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap();
    });
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let mode = a[1].as_str();
    let pages: u32 = a.get(2).map_or(32768, |s| s.parse().unwrap());
    let lat: u64 = a.get(3).map_or(0, |s| s.parse().unwrap());
    let mbps: f64 = a.get(4).map_or(100.0, |s| s.parse().unwrap());
    let tracked = std::env::var("TRACKED").is_ok();
    let bytes = build(pages, tracked);
    eprintln!("file {} MiB, db {} MiB", bytes.len() >> 20, (u64::from(pages) * u64::from(PS)) >> 20);
    let store = MemStore::new();
    put(&store, &bytes);
    let t = Instant::now();
    match mode {
        "full" => {
            let dir = std::env::var("BENCH_DIR").unwrap_or("/dev/shm".into());
            let target = std::path::Path::new(&dir).join(format!("rb-{}", std::process::id()));
            let sink = FileSink::create(&target).unwrap();
            block_on(restore(&store, RestoreTarget::Latest, sink)).unwrap();
            let _ = std::fs::remove_file(&target);
        }
        "null" => {
            block_on(restore(&store, RestoreTarget::Latest, Null)).unwrap();
        }
        "slow" => {
            let s = Slow { inner: store.clone(), latency: Duration::from_millis(lat), ns_per_byte: 1e3 / mbps };
            block_on(restore(&s, RestoreTarget::Latest, Null)).unwrap();
        }
        "slowra" => {
            let w: usize = std::env::var("WINDOW").map_or(4, |v| v.parse().unwrap());
            let slow = Slow { inner: store.clone(), latency: Duration::from_millis(lat), ns_per_byte: 1e3 / mbps };
            let s = ReadAhead { inner: Arc::new(slow), window: w, state: Mutex::new(None) };
            block_on(restore(&s, RestoreTarget::Latest, Null)).unwrap();
        }
        "decode" => {
            let mut d = Decoder::new();
            for c in bytes.chunks(4 << 20) {
                let mut rest = c;
                while !rest.is_empty() {
                    let (n, _) = d.feed(rest).unwrap();
                    rest = &rest[n..];
                }
            }
            d.finish().unwrap();
        }
        _ => panic!("mode"),
    }
    println!("{}", t.elapsed().as_secs_f64() * 1e3);
}

