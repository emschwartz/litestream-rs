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

#![expect(
    clippy::arithmetic_side_effects,
    reason = "bounds arithmetic on small counts in test code"
)]

use std::cell::Cell;

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    CompactionInputs, FileKey, Level, MAX_INPUTS, ObjectStore, PageLimit, Upload, compact,
    compact_staged,
};
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};
use lsr_store_fs::FsStore;

#[path = "../../../core/tests/core/heap.rs"]
#[expect(
    dead_code,
    reason = "the file is shared with the other memory gates; this binary measures one thread"
)]
mod heap;

use heap::{Counting, peak_of};

#[global_allocator]
static HEAP: Counting = Counting;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Pages every input holds: a little over one mebibyte of incompressible 4 KiB pages.
const PAGES: u32 = 300;

/// What one read of a replica's input asks for, and of a staged one, restated so a change
/// to either shows here.
const CHUNK: usize = MIB;
const STAGED_WINDOW: usize = 128 * KIB;

/// A page's size, and the encoder's output buffer that compaction flushes at.
const PAGE: usize = 4 * KIB;
const FLUSH: usize = MIB;

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
            bytes.len() > CHUNK,
            "an input holds more than one read chunk"
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
    let bound = MAX_INPUTS * (window + PAGE) + FLUSH + 2 * MIB;
    assert!(
        held <= bound,
        "compaction held {held} bytes, over the bound of {bound} ({MAX_INPUTS} inputs x ({window} + {PAGE}) + {FLUSH} + 2 MiB)"
    );
}

/// A compaction reading its inputs as a replica's holds a chunk per input, not the doubled
/// capacity a read grown from empty ends with.
#[test]
fn a_compaction_holds_a_chunk_per_replica_input() {
    let (_from, source, inputs) = staged_inputs();
    let to = tempfile::tempdir().unwrap();
    let dest = FsStore::new(to.path());

    let (done, peak) =
        peak_of(|| block_on(compact(&source, &dest, &inputs, Level::new(1).unwrap())));

    assert_eq!(done.unwrap().pages(), PAGES);
    assert_holds_a_window_per_input(peak.extra, CHUNK);
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
