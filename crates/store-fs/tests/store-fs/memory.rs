//! The memory gate for compaction reading its inputs from a directory, as the engine's
//! level-1 compaction reads the staged level-0 files.
//!
//! A merge holds every input at once (it takes the next page from whichever input has the
//! lowest one), so its memory is the inputs it merges times what each one holds: the read
//! chunk the store hands back, plus one page. `MAX_INPUTS` files of a little over a
//! mebibyte each are the busy case the engine meets: a second of writes per file. The
//! files are written by the real encoder through the store before a measurement starts,
//! and the output goes to a second directory, so only the compaction is counted.
//!
//! Both roles are measured: a replica's inputs are read a mebibyte at a time (the public
//! `compact`, what a file replica's levels above 1 get), and the engine's level-1
//! compaction reads the staged level-0 files in a 128 KiB window (`compact_staged`).
//!
//! A compaction also holds one output buffer for the file, not one per flush: the count of
//! 256 KiB-and-over allocations is pinned (`a_compaction_allocates_few_large_blocks`).
//! The upload of a staged file is measured too. It goes to the replica a slice a step
//! (`docs/decisions/D217.md`), and between two slices the engine keeps the decoder that
//! verifies the file and the upload it has open. Neither grows with the file, so a file four
//! times the size, sent in four times the slices, must peak where the small one does. The
//! whole-file upload it replaced peaked at 533,268 bytes on this gate (two read chunks and
//! 9 KiB); the send's own state adds 504.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "bounds arithmetic on small counts in test code"
)]

use std::cell::Cell;
use std::io::Cursor;
use std::num::NonZeroU32;

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    CommittedRead, CompactionInputs, Did, FakeWal, FileKey, Level, MAX_INPUTS, ObjectStore,
    PageChecksumTable, PageLimit, PageSource, SnapshotCause, SourceError, Step, SyncConfig, Synced,
    Upload, UploadState, Witnesses, compact, compact_staged, sync_step, upload_next,
};
use lsr_ltx::{
    DbPages, Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange,
};
use lsr_store_fs::FsStore;
use lsr_wal::{Salts, WalReader};

#[path = "../../../core/tests/core/heap.rs"]
#[expect(
    dead_code,
    reason = "the file is shared with the other memory gates; this binary measures one thread"
)]
mod heap;

use heap::{Counting, large_allocs_of, peak_of};

#[global_allocator]
static HEAP: Counting = Counting;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Pages every input holds: a little over one mebibyte of incompressible 4 KiB pages.
const PAGES: u32 = 300;

/// What one read of a replica's input asks for, and of a staged one, restated so a change
/// to either shows here.
/// A replica's input gets its share of the 16 MiB budget: 512 KiB at `MAX_INPUTS`.
const REPLICA_WINDOW: usize = 16 * MIB / MAX_INPUTS;
const STAGED_WINDOW: usize = 128 * KIB;

/// A page's size, and the encoder's output buffer that compaction flushes at.
const PAGE: usize = 4 * KIB;
const FLUSH: usize = 256 * KIB;

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

/// A xorshift stream: pages that do not compress, so a file is as large as its pages.
fn fill(state: &Cell<u64>, buf: &mut [u8]) {
    for word in buf.chunks_mut(8) {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        word.copy_from_slice(x.to_le_bytes().get(..word.len()).unwrap());
    }
}

/// One level-0 file of `PAGES` pages for transaction `n`, as the encoder writes it.
fn level_zero_file(n: u64, rng: &Cell<u64>) -> Vec<u8> {
    let size = PageSize::new(4096).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit: PAGES,
        txids: TxidRange::new(txid(n), txid(n)).unwrap(),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut page = vec![0_u8; size.bytes()];
    for pgno in 1..=PAGES {
        fill(rng, &mut page);
        encoder
            .write_page(PageNo::new(pgno, size).unwrap(), &page)
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// `MAX_INPUTS` level-0 files in a directory, and the inputs naming them.
fn staged_inputs() -> (tempfile::TempDir, FsStore, CompactionInputs) {
    let from = tempfile::tempdir().unwrap();
    let source = FsStore::new(from.path());
    let rng = Cell::new(0x9E37_79B9_7F4A_7C15);
    let stamp = Timestamp::from_unix_millis(1_700_000_000_000);
    for n in 1..=u64::try_from(MAX_INPUTS).unwrap() {
        let key = FileKey::new(Level::ZERO, TxidRange::new(txid(n), txid(n)).unwrap());
        let bytes = level_zero_file(n, &rng);
        assert!(
            bytes.len() > MIB,
            "an input holds more than one read window"
        );
        block_on(async {
            let mut upload = source
                .begin_upload(&key, stamp, WriteMode::New)
                .await
                .unwrap();
            upload.write(&bytes).await.unwrap();
            upload.commit().await.unwrap();
        });
    }
    let files = block_on(source.list(Level::ZERO, None, PageLimit::MAX))
        .unwrap()
        .files;
    assert_eq!(files.len(), MAX_INPUTS);
    (from, source, CompactionInputs::new(files).unwrap())
}

/// Asserts `held` is at most the output's flush buffer and slack plus `window` and a page
/// for each of the `MAX_INPUTS` inputs.
fn assert_holds_a_window_per_input(held: usize, window: usize) {
    let bound = MAX_INPUTS * (window + PAGE) + FLUSH + MIB;
    assert!(
        held <= bound,
        "compaction held {held} bytes, over the bound of {bound} ({MAX_INPUTS} inputs x ({window} + {PAGE}) + {FLUSH} + 1 MiB)"
    );
}

/// A compaction reading its inputs as a replica's holds its share of the budget per input.
#[test]
fn a_compaction_holds_its_share_of_the_budget_per_replica_input() {
    let (_from, source, inputs) = staged_inputs();
    let to = tempfile::tempdir().unwrap();
    let dest = FsStore::new(to.path());

    let (done, peak) =
        peak_of(|| block_on(compact(&source, &dest, &inputs, Level::new(1).unwrap())));

    assert_eq!(done.unwrap().pages(), PAGES);
    assert_holds_a_window_per_input(peak.extra, REPLICA_WINDOW);
}

/// The engine's level-1 compaction reads the staged files in the staging window.
#[test]
fn a_level_one_compaction_holds_a_staging_window_per_input() {
    let (_from, source, inputs) = staged_inputs();
    let to = tempfile::tempdir().unwrap();
    let dest = FsStore::new(to.path());

    let (done, peak) = peak_of(|| {
        block_on(compact_staged(
            &source,
            &dest,
            &inputs,
            Level::new(1).unwrap(),
        ))
    });

    assert_eq!(done.unwrap().pages(), PAGES);
    assert_holds_a_window_per_input(peak.extra, STAGED_WINDOW);
}

/// Blocks of 256 KiB or more a level-1 compaction of `MAX_INPUTS` staged files may allocate: the
/// output buffer, once (the output is the 300 pages every input holds, about five flushes).
/// With a fresh buffer at every flush, each growing from empty by doubling, it is one block per
/// flush.
const LARGE_BLOCKS: usize = 2;

#[test]
fn a_compaction_allocates_few_large_blocks() {
    let (_from, source, inputs) = staged_inputs();
    let to = tempfile::tempdir().unwrap();
    let dest = FsStore::new(to.path());

    let (done, large) = large_allocs_of(|| {
        block_on(compact_staged(
            &source,
            &dest,
            &inputs,
            Level::new(1).unwrap(),
        ))
    });

    assert_eq!(done.unwrap().pages(), PAGES);
    eprintln!("compaction: {large} blocks of 256 KiB or more");
    assert!(
        large <= LARGE_BLOCKS,
        "{large} large blocks, over {LARGE_BLOCKS}"
    );
}

/// A database whose pages are computed when read, and do not compress: the staged file is as
/// large as the database.
struct GeneratedDb {
    pages: u32,
}

impl PageSource for GeneratedDb {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        Ok(DbPages::new(self.pages))
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        fill(&Cell::new(u64::from(pgno.get()) | 1 << 40), buf);
        Ok(())
    }
}

/// A database of `pages` pages staged in a directory by the sync step, as the engine stages
/// its first file, with the token the uploader needs for it.
fn staged_database(pages: u32) -> (tempfile::TempDir, FsStore, Witnesses, u64) {
    let size = PageSize::new(4096).unwrap();
    let wal = FakeWal::new(
        size,
        Salts {
            salt1: 10,
            salt2: 20,
        },
    );
    wal.append_tx(
        &[(PageNo::new(1, size).unwrap(), vec![7; size.bytes()])],
        NonZeroU32::new(pages).unwrap(),
    );
    let committed = CommittedRead::new(wal.committed_end(), wal.backfilled(), wal.epoch()).unwrap();
    let mut reader = WalReader::open(Cursor::new(wal.file_bytes())).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let staging = FsStore::new(dir.path());
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: SnapshotCause::FirstSync,
    };
    let synced = block_on(sync_step(
        step,
        &mut reader,
        committed,
        &mut GeneratedDb { pages },
        &mut PageChecksumTable::new(),
        &staging,
        &SyncConfig::default(),
        Timestamp::from_unix_millis(1_700_000_000_000),
    ));
    let Synced::Wrote { file, witness, .. } = synced.unwrap() else {
        panic!("the sync step staged no file");
    };
    let mut witnesses = Witnesses::new();
    witnesses.insert(witness);
    (dir, staging, witnesses, file.size)
}

/// The most heap an upload of a staged database of `pages` pages holds, and the file's size.
fn upload_peak(pages: u32) -> (usize, u64) {
    let (_dir, staging, witnesses, size) = staged_database(pages);
    let to = tempfile::tempdir().unwrap();
    let replica = FsStore::new(to.path());
    let (done, peak) = peak_of(|| {
        block_on(upload_next(
            UploadState::At(None),
            &staging,
            &replica,
            Some(txid(1)),
            &witnesses,
        ))
    });
    assert!(matches!(done.unwrap().did, Did::Put(_)));
    (peak.extra, size)
}

/// What one read of a staged file for its upload asks for, and the bytes one step sends:
/// core's `upload::CHUNK` and `upload::SLICE`, restated so a change to either shows here.
const UPLOAD_CHUNK: usize = 256 * KIB;
const UPLOAD_SLICE: u64 = 8 * 1024 * 1024;

/// A staged file goes up a slice a step, and what the engine keeps between two slices does
/// not grow with the file: a file sent in two slices and one four times its size peak at the
/// same heap, which is what one whole-file upload held.
#[test]
fn an_upload_holds_the_same_however_large_the_file_and_however_many_its_slices() {
    let (small, small_size) = upload_peak(3 * 1024);
    let (large, large_size) = upload_peak(12 * 1024);
    eprintln!(
        "upload of {} MiB: {} KiB; of {} MiB: {} KiB",
        small_size >> 20,
        small >> 10,
        large_size >> 20,
        large >> 10
    );
    assert!(small_size > UPLOAD_SLICE, "the small file takes two slices");
    assert!(
        large_size > 4 * UPLOAD_SLICE,
        "the large file takes more than four"
    );
    let bound = 2 * UPLOAD_CHUNK + 16 * KIB;
    assert!(
        small <= bound && large <= bound,
        "an upload held {small} and {large} bytes, over the bound of {bound}"
    );
    assert!(
        large <= small + 16 * KIB,
        "an upload four times the size held {} bytes more",
        large - small
    );
}
