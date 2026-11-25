//! Memory gates: peak heap as a number a test can hold a path to.
//!
//! A counting allocator ([`crate::heap`]) measures the most heap a restore, a snapshot or
//! a plan holds at once, and each gate asserts the law the code documents rather than
//! a figure from one machine:
//!
//! - Restore memory is one read window (the store's read width times the 4 MiB
//!   chunk), whatever the database's size and however long the chain. The gate runs a
//!   size N and a size 4N, so a buffer that grows with the file shows as a difference,
//!   and also caps the peak by the constants the window is made of.
//! - A snapshot is linear in pages and in nothing else: the page checksum table and the
//!   page plan cost a fixed number of bytes a page. The gate pins that number (issue #64
//!   asks whether the table should be bounded) and the largest chunk handed to the
//!   upload.
//! - Planning borrows the inventory it is given: a plan over 10^5 files holds a
//!   constant, not another copy of the list.
//!
//! The stores are files on disk and the sinks discard: `MemStore` and `MemSink` hold
//! everything, so against them every peak would be the database. The files are written
//! by the real encoder before a measurement starts, so only the code under test is
//! counted. The small sizes run in `just check`; `mem_big` holds the 1 to 4 GiB cases
//! (`just mem-big`).

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fs::File;
use std::io::{self, BufWriter, Cursor, Write};
use std::num::{NonZeroU32, NonZeroUsize};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::heap::{Counting, Peak, peak_of};
use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    AsOfPlanner, BatchKind, CreatedAt, DatabaseSink, FakeWal, FileInfo, FileKey, Inventory,
    KeyTarget, Level, ListedTime, Listing, ObjectStore, PageChecksumTable, PageLimit, PageSource,
    PlanOutcome, ReadLen, RestoreTarget, SourceError, Step, StepEnd, StoreError, SyncConfig,
    Synced, Upload, build_listing, plan, restore, sync_step,
};
use lsr_ltx::{
    DbChecksum, DbPages, Encoder, Header, HeaderFields, PageNo, PageRef, PageSize, Timestamp, Txid,
    TxidRange, lock_page,
};
use lsr_wal::{Salts, WalReader};

#[global_allocator]
static HEAP: Counting = Counting;

/// A kibibyte, a mebibyte.
pub const KIB: usize = 1024;
pub const MIB: usize = 1024 * KIB;

/// Bytes one restore read asks of the store: core's `restore::CHUNK_BYTES`. A change
/// there changes the window this file bounds, which is the point of repeating it.
pub const RESTORE_CHUNK: usize = 4 * MIB;

/// The page size every fixture uses.
pub fn page_size() -> PageSize {
    PageSize::new(4096).unwrap()
}

/// A xorshift stream: pages that do not compress, so a file on disk is as large as the
/// database it holds and a buffer the size of the file is visible.
pub struct Rng(pub u64);

impl Rng {
    pub fn fill(&mut self, buf: &mut [u8]) {
        for word in buf.chunks_mut(8) {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            let bytes = self.0.to_le_bytes();
            word.copy_from_slice(bytes.get(..word.len()).unwrap());
        }
    }
}

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(txid(min), txid(max)).unwrap()
}

fn stamp() -> Timestamp {
    Timestamp::from_unix_millis(1_700_000_000_000)
}

/// The pages of `pgnos` a database file can hold: all but SQLite's lock page, which a
/// database past 1 GiB skips and `PageNo` refuses, so a fixture that size never names it.
pub fn stored_pages(pgnos: impl Iterator<Item = u32>) -> impl Iterator<Item = PageNo> {
    pgnos.filter_map(|pgno| PageNo::new(pgno, page_size()))
}

/// Writes one LTX file with the real encoder, streaming to `path`, holding only the
/// encoder's own state (its page index) until it returns. Also returns how many pages
/// it wrote, which is how many a restore of it writes.
fn write_file(
    path: &Path,
    key: FileKey,
    commit: u32,
    pgnos: impl Iterator<Item = u32>,
    rng: &mut Rng,
) -> (FileInfo, u64) {
    let size = page_size();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit,
        txids: key.range(),
        timestamp: stamp(),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let out = BufWriter::with_capacity(MIB, File::create(path).unwrap());
    let mut encoder = Encoder::new(out, header).unwrap();
    let mut page = vec![0_u8; size.bytes()];
    let mut written = 0;
    for pgno in stored_pages(pgnos) {
        rng.fill(&mut page);
        encoder.write_page(pgno, &page).unwrap();
        written += 1;
    }
    let (mut out, _trailer) = encoder.finish(None).unwrap();
    out.flush().unwrap();
    let info = FileInfo {
        key,
        size: std::fs::metadata(path).unwrap().len(),
        listed: ListedTime::Created(CreatedAt::mtime(stamp())),
    };
    (info, written)
}

/// A replica on disk that answers ranged reads without holding a byte of it: the
/// stand-in for the object store the memory gates restore from.
pub struct Spool {
    _dir: tempfile::TempDir,
    files: Vec<(FileInfo, File)>,
    width: usize,
    pages: u64,
}

impl Spool {
    /// A snapshot of `pages` pages at transaction 1, then `overlays` level-0 files
    /// (transactions 2, 3, ...) that each rewrite `per_overlay` pages spread over the
    /// database. `width` is the read width the store reports, so the restore's window
    /// is `width` chunks.
    pub fn build(pages: u32, overlays: u64, per_overlay: u32, width: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut files = Vec::new();
        let mut written = 0;
        let mut put = |path: std::path::PathBuf, (info, pages): (FileInfo, u64)| {
            files.push((info, File::open(path).unwrap()));
            written += pages;
        };
        let key = FileKey::snapshot(txid(1));
        let path = dir.path().join("snapshot");
        put(
            path.clone(),
            write_file(&path, key, pages, 1..=pages, &mut rng),
        );
        let step = (pages / per_overlay.max(1)).max(1);
        for n in 0..overlays {
            let key = FileKey::new(Level::ZERO, range(n + 2, n + 2));
            let path = dir.path().join(format!("l0-{n}"));
            let pgnos = (0..per_overlay)
                .map(|i| 1 + i * step)
                .filter(|p| *p <= pages);
            put(path.clone(), write_file(&path, key, pages, pgnos, &mut rng));
        }
        Self {
            _dir: dir,
            files,
            width,
            pages: written,
        }
    }

    /// Pages in every file together: what a restore of the whole chain writes.
    pub fn pages(&self) -> u64 {
        self.pages
    }

    /// Bytes on disk, every file together.
    pub fn bytes(&self) -> u64 {
        self.files.iter().map(|(info, _)| info.size).sum()
    }

    fn file(&self, key: &FileKey) -> Result<&(FileInfo, File), StoreError> {
        self.files
            .iter()
            .find(|(info, _)| info.key == *key)
            .ok_or(StoreError::NotFound { key: *key })
    }
}

/// A store that never takes an upload; the restore gates only read.
#[derive(Debug)]
pub struct NoUpload;

impl Upload for NoUpload {
    async fn write(&mut self, _chunk: &[u8]) -> Result<(), StoreError> {
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        Err(StoreError::Refused {
            op: lsr_core::StoreOp::Upload,
            status: 501,
        })
    }

    async fn abort(self) -> Result<(), StoreError> {
        Ok(())
    }
}

impl ObjectStore for Spool {
    type Upload = NoUpload;

    /// Leaves no scratch.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let level_files = self
            .files
            .iter()
            .filter(|(info, _)| info.key.level() == level)
            .map(|(info, _)| *info)
            .collect();
        Ok(build_listing(level_files, &[], after, limit))
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let (info, file) = self.file(key)?;
        let left = info.size.saturating_sub(offset);
        let take = usize::try_from(left.min(len.get())).unwrap();
        let mut buf = vec![0_u8; take];
        file.read_exact_at(&mut buf, offset).unwrap();
        Ok(buf)
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        let _ = file;
        Ok(CreatedAt::mtime(stamp()))
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        Ok((
            self.read(&file.key, 0, len).await?,
            CreatedAt::mtime(stamp()),
        ))
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        _created_at: Timestamp,
        _mode: WriteMode,
    ) -> Result<NoUpload, StoreError> {
        Err(StoreError::NotFound { key: *key })
    }

    async fn delete(&self, _keys: &[FileKey]) -> Result<(), StoreError> {
        Ok(())
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        match kind {
            BatchKind::Reads => NonZeroUsize::new(self.width).unwrap(),
            BatchKind::Lists | BatchKind::ResolvedCreatedAt => NonZeroUsize::MIN,
        }
    }
}

/// A destination that keeps a page count and nothing else.
pub struct Discard {
    pages: Arc<AtomicU64>,
}

impl DatabaseSink for Discard {
    fn write_page(&mut self, _page: PageRef<'_>) -> io::Result<()> {
        self.pages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn truncate(&mut self, _pages: DbPages, _page_size: PageSize) -> io::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn checksum(&mut self, _pages: DbPages, _page_size: PageSize) -> io::Result<DbChecksum> {
        Err(io::Error::other(
            "untracked files never ask for the end state",
        ))
    }

    type PublishError = Infallible;

    fn publish(self) -> Result<(), Infallible> {
        Ok(())
    }
}

/// The peak heap of restoring the latest state of `store`, and the pages the sink saw.
pub fn restore_peak(store: &Spool) -> (Peak, u64) {
    let pages = Arc::new(AtomicU64::new(0));
    let sink = Discard {
        pages: Arc::clone(&pages),
    };
    let (restored, peak) = peak_of(|| block_on(restore(store, RestoreTarget::Latest, sink)));
    restored.unwrap();
    (peak, pages.load(Ordering::Relaxed))
}

/// What restore may hold beyond the read window (`width` chunks, one per read in
/// flight): the decoder's one page, the index entry being checked and the result.
pub const RESTORE_MARGIN: usize = MIB;

/// How much two peaks may differ and still be "flat": allocator rounding and the
/// bookkeeping of a longer run, never a buffer.
pub const FLAT_SLACK: usize = 256 * KIB;

/// The most a restore of `width` chunks may hold, from the constants it is made of.
pub const fn restore_cap(width: usize) -> usize {
    width * RESTORE_CHUNK + RESTORE_MARGIN
}

/// Checks the restore law over two databases of `small` and `4 * small` pages, built
/// one after the other so the disk holds one at a time.
pub fn assert_restore_flat_in_size(small: u32, width: usize) {
    let measure = |pages: u32| {
        let spool = Spool::build(pages, 3, 16, width);
        assert!(
            spool.bytes() > u64::try_from(restore_cap(width)).unwrap(),
            "the database must be larger than the window, or the gate cannot see a buffer"
        );
        let (peak, written) = restore_peak(&spool);
        assert_eq!(written, spool.pages());
        eprintln!(
            "restore width {width}: {} MiB -> {} KiB",
            spool.bytes() >> 20,
            peak.extra >> 10
        );
        peak.extra
    };
    let few = measure(small);
    let many = measure(small * 4);
    assert!(
        few <= restore_cap(width) && many <= restore_cap(width),
        "{few} and {many} bytes, over the cap {}",
        restore_cap(width)
    );
    assert!(
        many <= few + FLAT_SLACK,
        "restore grew with the database: {few} bytes for {small} pages, {many} for {}",
        small * 4
    );
}

#[test]
fn restore_peak_heap_is_flat_in_database_size() {
    assert_restore_flat_in_size(4096, 2);
}

/// The same bytes in one file and in sixty-four: a chain holds no per-file state that
/// piles up.
#[test]
fn restore_peak_heap_is_flat_in_chain_length() {
    let pages = 4096;
    let one = Spool::build(pages, 0, 16, 2);
    let long = Spool::build(pages, 63, 64, 2);
    let (one_peak, _) = restore_peak(&one);
    let (long_peak, long_pages) = restore_peak(&long);
    eprintln!(
        "restore chain: 1 file {} KiB, 64 files {} KiB",
        one_peak.extra >> 10,
        long_peak.extra >> 10
    );
    assert_eq!(long_pages, long.pages());
    assert!(
        long_peak.extra <= one_peak.extra + FLAT_SLACK,
        "a longer chain held more: {} bytes for 1 file, {} for 64",
        one_peak.extra,
        long_peak.extra
    );
    assert!(long_peak.extra <= restore_cap(2));
}

/// A database past 1 GiB has no lock page, so the fixtures past it must leave it out:
/// naming it made the 4 GiB restore gate panic while building its replica.
#[test]
fn a_fixture_past_one_gib_never_names_the_lock_page() {
    let lock = lock_page(page_size());
    let stored: Vec<u32> = stored_pages(lock - 1..=lock + 1).map(PageNo::get).collect();
    assert_eq!(stored, [lock - 1, lock + 1]);
}

/// A store that takes an upload and keeps only its length and its largest write.
#[derive(Default)]
struct Void {
    largest: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

struct VoidUpload {
    key: FileKey,
    largest: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl Upload for VoidUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        let len = u64::try_from(chunk.len()).unwrap();
        self.largest.fetch_max(len, Ordering::Relaxed);
        self.bytes.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        Ok(FileInfo {
            key: self.key,
            size: self.bytes.load(Ordering::Relaxed),
            listed: ListedTime::Created(CreatedAt::mtime(stamp())),
        })
    }

    async fn abort(self) -> Result<(), StoreError> {
        Ok(())
    }
}

impl ObjectStore for Void {
    type Upload = VoidUpload;

    /// Leaves no scratch.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    async fn list(
        &self,
        _level: Level,
        _after: Option<TxidRange>,
        _limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        Ok(Listing::empty())
    }

    async fn read(&self, key: &FileKey, _: u64, _: ReadLen) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::NotFound { key: *key })
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        Err(StoreError::NotFound { key: file.key })
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        _: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        Err(StoreError::NotFound { key: file.key })
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        _created_at: Timestamp,
        _mode: WriteMode,
    ) -> Result<VoidUpload, StoreError> {
        Ok(VoidUpload {
            key: *key,
            largest: Arc::clone(&self.largest),
            bytes: Arc::clone(&self.bytes),
        })
    }

    async fn delete(&self, _keys: &[FileKey]) -> Result<(), StoreError> {
        Ok(())
    }
}

/// A database file whose pages are computed when read: no page is held.
struct GeneratedDb {
    pages: u32,
}

impl PageSource for GeneratedDb {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        Ok(DbPages::new(self.pages))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        Rng(u64::from(pgno.get()) | 1 << 40).fill(buf);
        Ok(())
    }
}

/// What a first snapshot of a `pages`-page database cost.
pub struct SnapshotRun {
    pub peak: Peak,
    pub bytes: u64,
    pub largest_write: u64,
}

/// Snapshots a database of `pages` pages (page 1 in the WAL, the rest in the file) into
/// a store that discards what it is given.
pub fn snapshot_run(pages: u32) -> SnapshotRun {
    let size = page_size();
    let wal = FakeWal::new(
        size,
        Salts {
            salt1: 10,
            salt2: 20,
        },
    );
    let commit = NonZeroU32::new(pages).unwrap();
    wal.append_tx(
        &[(PageNo::new(1, size).unwrap(), vec![7; size.bytes()])],
        commit,
    );
    let bytes = wal.file_bytes();
    let committed =
        lsr_core::CommittedRead::new(wal.committed_end(), wal.backfilled(), wal.epoch()).unwrap();
    let mut reader = WalReader::open(Cursor::new(bytes)).unwrap();
    let mut db = GeneratedDb { pages };
    let mut table = PageChecksumTable::new();
    let void = Void::default();
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: lsr_core::SnapshotCause::FirstSync,
    };
    let (synced, peak) = peak_of(|| {
        block_on(sync_step(
            step,
            &mut reader,
            committed,
            &mut db,
            &mut table,
            &void,
            &SyncConfig::default(),
            stamp(),
        ))
    });
    assert!(matches!(
        synced.unwrap(),
        Synced::Wrote {
            end: StepEnd::Through(_),
            ..
        }
    ));
    SnapshotRun {
        peak,
        bytes: void.bytes.load(Ordering::Relaxed),
        largest_write: void.largest.load(Ordering::Relaxed),
    }
}

/// Bytes a snapshot holds for each page of the database, in the page checksum table
/// and the encoder's page index. The table is one 8-byte slot a page and the index about
/// nine varint bytes, so the law is linear in the database's pages and its floor is 17
/// bytes a page (74 MiB measured at 16 GiB of 4 KiB pages: 18). The plan holds nothing a page costs: it
/// is walked, never listed (it was 32 bytes a page, 48 while sorted); the table was a map
/// (about 20), and the index was copied twice at the end of the file. Measured: 16 to 18
/// bytes a page, from 86; the ceiling is that plus about 15%, so any of those again, or holding
/// page contents, which is 4096 bytes a page, breaks it.
pub const SNAPSHOT_BYTES_PER_PAGE: usize = 20;

/// The most one `Upload::write` may carry, the last included: a snapshot streams its pages
/// and then its index in pieces of about 256 KiB (measured: 271 KiB), so no write is a file
/// or an index. The index is about nine bytes a page, so one that went out whole would pass
/// this at 60,000 pages.
pub const SNAPSHOT_WRITE_CEILING: u64 = 512 * 1024;

/// What a snapshot holds that does not grow with the pages: its buffers and
/// bookkeeping, and the parts an upload buffers, which are the store's to bound (and which
/// `lsr-store-s3`'s memory gates hold to `concurrency x part size` through the real client).
pub const SNAPSHOT_FIXED: usize = 2 * MIB;

/// The snapshot law: peak = fixed + bytes-per-page times pages, checked on N and 4N.
pub fn assert_snapshot_law(small: u32) {
    let few = snapshot_run(small);
    let many = snapshot_run(small * 4);
    let (a, b) = (few.peak.extra, many.peak.extra);
    let slope = b.saturating_sub(a) / usize::try_from(small * 3).unwrap();
    let fixed = a.saturating_sub(slope * usize::try_from(small).unwrap());
    eprintln!(
        "snapshot {small} pages: {} KiB, {} pages: {} KiB, {slope} B/page, fixed {} KiB, largest write {} KiB",
        a >> 10,
        small * 4,
        b >> 10,
        fixed >> 10,
        many.largest_write >> 10
    );
    assert!(
        slope <= SNAPSHOT_BYTES_PER_PAGE,
        "{slope} bytes a page, ceiling {SNAPSHOT_BYTES_PER_PAGE} (issue #64)"
    );
    assert!(
        fixed <= SNAPSHOT_FIXED,
        "{fixed} bytes held whatever the size, ceiling {SNAPSHOT_FIXED}"
    );
    assert!(few.bytes >= u64::from(small) * 4096 && many.bytes >= u64::from(small * 4) * 4096);
    for (run, pages) in [(&few, small), (&many, small * 4)] {
        let ceiling = SNAPSHOT_WRITE_CEILING;
        assert!(
            run.largest_write <= ceiling,
            "one write carried {} bytes for {pages} pages, ceiling {ceiling}",
            run.largest_write
        );
    }
}

#[test]
fn snapshot_peak_heap_is_linear_in_pages_with_a_pinned_coefficient() {
    assert_snapshot_law(16 * 1024);
}

/// The inventory of a replica in the writer's shape: level-0 singletons, a level-1 file
/// per ten, a level-2 file per hundred, and ten snapshots, `files` in all.
pub fn inventory_of(files: usize) -> Inventory {
    let tx = |n: usize| u64::try_from(n).unwrap();
    let l0 = files * 9 / 10;
    let info = |level: u8, min: u64, max: u64| FileInfo {
        key: FileKey::new(Level::new(level).unwrap(), range(min, max)),
        size: 1,
        listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(
            i64::try_from(max).unwrap() * MS_PER_TX,
        ))),
    };
    let mut all = Vec::with_capacity(files);
    all.extend((1..=l0).map(|n| info(0, tx(n), tx(n))));
    all.extend((0..l0 / 10).map(|n| info(1, tx(n * 10 + 1), tx(n * 10 + 10))));
    all.extend((0..l0 / 100).map(|n| info(2, tx(n * 100 + 1), tx(n * 100 + 100))));
    all.extend((1..=10).map(|n| info(9, 1, tx(l0 / 10 * n))));
    Inventory::from_files(all).unwrap()
}

/// What planning may hold beyond the inventory itself: the answer and a few locals.
pub const PLAN_CAP: usize = 64 * KIB;

/// The bytes the as-of planner may hold for each file: a pointer into the inventory,
/// whatever its sorts need, and nothing else.
pub const AS_OF_BYTES_PER_FILE: usize = 12;

/// Checks that planning `files` files copies none of them, and returns the inventory's
/// own size for the report.
pub fn assert_planning_borrows(files: usize) {
    let (inventory, built) = peak_of(|| inventory_of(files));
    let held = built.kept;
    let (latest, a) = peak_of(|| plan(&inventory, KeyTarget::Latest));
    let (_, b) = peak_of(|| {
        plan(
            &inventory,
            KeyTarget::Txid(txid(u64::try_from(files / 3).unwrap())),
        )
    });
    let (planner, c) =
        peak_of(|| AsOfPlanner::new(&inventory, Timestamp::from_unix_millis(1 << 40)));
    drop(planner);
    eprintln!(
        "planning {files} files: inventory {} KiB; Latest +{} KiB, Txid +{} KiB, AsOf +{} KiB",
        held >> 10,
        a.extra >> 10,
        b.extra >> 10,
        c.extra >> 10
    );
    assert!(matches!(latest, PlanOutcome::Plan(_)));
    assert!(
        a.extra <= PLAN_CAP && b.extra <= PLAN_CAP,
        "planning copied the inventory: Latest +{} bytes, Txid +{} bytes of {held}",
        a.extra,
        b.extra
    );
    assert!(
        c.extra <= PLAN_CAP + AS_OF_BYTES_PER_FILE * files,
        "the as-of planner held {} bytes for {files} files",
        c.extra
    );
}

#[test]
fn planning_a_hundred_thousand_files_copies_none_of_them() {
    assert_planning_borrows(100_000);
}

/// One minute of created-at time per transaction, so 10^5 files span about 70 days.
const MS_PER_TX: i64 = 60_000;

/// The bytes retention may hold for each file: the list of keys it chose to delete before
/// the cut to one job, which is a key (24 bytes) for each deletable file at levels 1 to 8,
/// and nothing that scales with the whole inventory.
pub const RETENTION_BYTES_PER_FILE: usize = 12;

/// Checks that retention over `files` files, down to the self-check that the delete
/// leaves a restorable history, copies none of them.
pub fn assert_retention_borrows(files: usize) {
    use lsr_core::{Due, Job, L0Retention, MaintainState, Maintenance, ResolvedCreatedAt, due};
    let day = std::time::Duration::from_secs(86_400);
    let hour = std::time::Duration::from_secs(3_600);
    let config = Maintenance::new(vec![hour], day, day, L0Retention::After(hour)).unwrap();
    let inventory = inventory_of(files);
    let newest = inventory
        .files()
        .iter()
        .filter_map(|f| f.created_at())
        .map(|c| c.at())
        .max()
        .unwrap();
    let now = Timestamp::from_unix_millis(newest.unix_millis() + 7 * 86_400_000);
    let mut state = MaintainState::default();
    let (decided, peak) = peak_of(|| {
        due(
            &inventory,
            now,
            &config,
            None,
            &mut state,
            &ResolvedCreatedAt::default(),
        )
    });
    eprintln!("retention over {files} files: +{} KiB", peak.extra >> 10);
    assert!(
        matches!(decided, lsr_core::Resolve::Done(Due::Run(Job::Delete(_)))),
        "{decided:?}"
    );
    assert!(
        peak.extra <= PLAN_CAP + RETENTION_BYTES_PER_FILE * files,
        "retention held {} bytes for {files} files",
        peak.extra
    );
}

#[test]
fn retention_over_a_hundred_thousand_files_copies_none_of_them() {
    assert_retention_borrows(100_000);
}

/// What a test run builds an inventory out of, so a test of the generator itself can
/// see it is the shape claimed.
#[test]
fn the_planning_fixture_has_the_shape_it_claims() {
    let inventory = inventory_of(1000);
    let count = |level: u8| inventory.level(Level::new(level).unwrap()).len();
    assert_eq!((count(0), count(1), count(2), count(9)), (900, 90, 9, 10));
    let by_level: BTreeMap<u8, usize> = [0, 1, 2, 9].into_iter().map(|l| (l, count(l))).collect();
    assert_eq!(by_level.values().sum::<usize>(), inventory.files().len());
}
