//! Request-shape gates: how many requests an operation sends, and how many
//! sequential round trips are on its critical path.
//!
//! Every slow path the store has won back came from the same mistake in
//! reverse: independent requests sent one after another (ranged reads of a
//! restore, parts of a multipart upload, the levels of a listing, the HEADs
//! that read created-at times). Wall time is round trips times latency, so a
//! serial path sends the same requests and costs many more round trips. A
//! count alone cannot see it. These tests run each representative operation
//! through a [`Link`] (a virtual clock, nothing sleeps) and hold the requests
//! per kind and the waves to a bound derived from the design, stated beside
//! each test. Cutting a batch to one request (`READ_CONCURRENCY`,
//! `CREATION_TIME_CONCURRENCY` or an upload's concurrency set to 1) breaks the
//! matching gate.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "bounds arithmetic on small counts in test code"
)]

use lsr_core::conformance::block_on;
use lsr_core::{
    AsOfPlanner, Due, FakeClock, FileKey, Inventory, Job, L0Retention, Level, MAX_DELETE,
    MaintainState, MemSink, ObjectStore, Outcome, RestoreTarget, Upload, due, resolve, restore,
    run_delete,
};
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, TxidRange};
use lsr_store_s3::{CREATION_TIME_CONCURRENCY, PartSize, READ_CONCURRENCY, S3Store};

use crate::common::*;
use crate::latency::Link;
use crate::request_counts::cases::{
    EPOCH_MS, Rig, Shared, TXNS, instant_back, now_ms, retention_config, txid,
};
use lsr_core::WriteMode;

/// Entries one listing page returns: the page limit minus the one-key look-ahead
/// that proves the listing goes on (see `a_full_scan_sends_one_list_per_page`).
const LIST_PER_PAGE: usize = 999;

/// The levels a replica has: 0 to 8 and the snapshot level, all listed.
const LEVELS: usize = 10;

/// Bytes of one restore read: core's `restore::CHUNK_BYTES`, restated so a
/// change of either shows as a failing count here.
const RESTORE_CHUNK: u64 = 4 * 1024 * 1024;

/// Ranged reads and listing pages in flight at once, as designed. Restated, not
/// read from the store's constants, so shrinking a constant cannot shrink the bound
/// with it; the first test pins the two together.
const READ_WIDTH: usize = 8;

/// Created-at HEADs in flight at once, as designed.
const HEAD_WIDTH: usize = 16;

/// The waves a batch of `n` takes at `width` in flight.
fn waves(n: usize, width: usize) -> usize {
    n.div_ceil(width)
}

type LinkStore = S3Store<Link<Shared>>;

fn linked(rig: &Rig) -> (LinkStore, Link<Shared>) {
    let link = Link::new(rig.shared());
    let store =
        S3Store::with_transport(config(), credentials(), link.clone(), FakeClock::new(now()))
            .unwrap();
    (store, link)
}

// The bounds below assume these widths; changing one is a design change.
#[test]
fn the_designed_widths_are_the_stores() {
    assert_eq!(READ_CONCURRENCY.get(), READ_WIDTH);
    assert_eq!(CREATION_TIME_CONCURRENCY.get(), HEAD_WIDTH);
}

// A listing asks for the next page of up to `READ_CONCURRENCY` levels per
// wave, so the levels that fit on one page ride along with the long level's
// pages. Pages: ceil(files / 999) per level, at least one. Depth: the longest
// level's pages, plus one wave for the levels past the first eight. A serial
// listing is the sum of the pages (19 here) and not 11.
#[test]
fn inventory_listing_is_levels_side_by_side() {
    let rig = Rig::build();
    let (store, link) = linked(&rig);
    let (inventory, tally) = link.measure(|| block_on(Inventory::load(&store)).unwrap());
    let per_level: Vec<usize> = Level::ALL
        .iter()
        .map(|level| inventory.level(*level).len())
        .collect();
    let pages: Vec<usize> = per_level
        .iter()
        .map(|n| waves(*n, LIST_PER_PAGE).max(1))
        .collect();
    let longest = pages.iter().copied().max().unwrap();
    println!("listing {tally:?} pages {pages:?}");
    assert_eq!(tally.list, pages.iter().sum::<usize>(), "one LIST per page");
    assert_eq!(tally.requests(), tally.list, "a listing sends only LISTs");
    let depth = longest + 1;
    assert!(
        tally.rounds <= depth,
        "{} round trips, bound {depth} ({tally:?})",
        tally.rounds
    );
}

/// A random page: incompressible, so a file's size is its page count times the page size.
fn page(state: &mut u64, size: PageSize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size.bytes());
    while bytes.len() < size.bytes() {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes
}

/// One untracked LTX file of `pages` random pages at every `stride`th page number from 1,
/// in a database of `commit` pages, for transaction `n`.
fn ltx_file(n: u64, commit: u32, pages: u32, stride: u32) -> Vec<u8> {
    let size = PageSize::new(16384).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit,
        txids: TxidRange::new(txid(n), txid(n)).unwrap(),
        timestamp: Timestamp::from_unix_millis(EPOCH_MS),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut state = 0x9E37_79B9_7F4A_7C15 ^ n;
    for at in 0..pages {
        let no = PageNo::new(1 + at * stride, size).unwrap();
        encoder.write_page(no, &page(&mut state, size)).unwrap();
    }
    encoder.finish(None).unwrap().0
}

fn put(store: &impl ObjectStore, key: &FileKey, bytes: &[u8]) {
    block_on(async {
        let mut up = store
            .begin_upload(
                key,
                Timestamp::from_unix_millis(EPOCH_MS),
                WriteMode::Replace,
            )
            .await
            .unwrap();
        for chunk in bytes.chunks(1 << 20) {
            up.write(chunk).await.unwrap();
        }
        up.commit().await.unwrap();
    });
}

// A restore reads each file in chunks of RESTORE_CHUNK, up to READ_CONCURRENCY
// of one file's chunks side by side. Requests: one GET per chunk of every
// file in the chain, nothing more (no empty read to find an end). Depth: the
// listing, then ceil(chunks / width) waves per file; files are applied in
// order, so their waves add. Serial reading costs the chunk count (14 here).
#[test]
fn restore_reads_chunks_side_by_side() {
    let link = Link::new(fake());
    let store =
        S3Store::with_transport(config(), credentials(), link.clone(), FakeClock::new(now()))
            .unwrap();
    // 2600 pages of 16 KiB: 11 chunks; the overlays 700 pages (3 chunks) and 40 (1).
    let files = [
        (key(9, 1, 1), ltx_file(1, 2600, 2600, 1)),
        (key(0, 2, 2), ltx_file(2, 2600, 700, 3)),
        (key(0, 3, 3), ltx_file(3, 2600, 40, 7)),
    ];
    for (key, bytes) in &files {
        put(&store, key, bytes);
    }
    let chunks: Vec<usize> = files
        .iter()
        .map(|(_, bytes)| waves(bytes.len(), usize::try_from(RESTORE_CHUNK).unwrap()))
        .collect();
    let sink = MemSink::new();
    let (restored, tally) =
        link.measure(|| block_on(restore(&store, RestoreTarget::Latest, sink.clone())).unwrap());
    assert_eq!(restored.files(), 3);
    println!("restore {tally:?} chunks {chunks:?}");
    let total: usize = chunks.iter().sum();
    assert_eq!(tally.get, total, "one GET per chunk");
    assert_eq!(tally.head, 0, "Latest reads no created-at time");
    let width = READ_WIDTH;
    let reads: usize = chunks.iter().map(|n| waves(*n, width)).sum();
    let listing = waves(LEVELS, width);
    let depth = listing + reads;
    assert!(
        tally.rounds <= depth,
        "{} round trips, bound {depth} = {listing} listing + {reads} reads ({tally:?})",
        tally.rounds
    );
    assert!(tally.widest <= width, "{} in flight", tally.widest);
}

// A multipart upload keeps up to `concurrency` parts in flight while it fills the next.
// Requests: create, one per part, complete. Depth: create, ceil(parts / concurrency)
// waves of parts (the link lands every part in flight when the upload has to wait for
// one), complete. Memory: never more than `concurrency` parts in flight and one more.
// A serial upload is parts + 2 round trips (11 here).
#[test]
fn multipart_upload_keeps_parts_in_flight() {
    let concurrency = std::num::NonZeroUsize::new(4).unwrap();
    let part = PartSize::MIN;
    let link = Link::new(fake());
    let store = S3Store::with_transport(
        config().with_part_size(part).with_concurrency(concurrency),
        credentials(),
        link.clone(),
        FakeClock::new(now()),
    )
    .unwrap();
    let part_bytes = 5 * 1024 * 1024;
    let parts = 9;
    let bytes = vec![7_u8; part_bytes * (parts - 1) + part_bytes / 2];
    let key = key(9, 1, 1);
    let (peak, tally) = link.measure(|| {
        block_on(async {
            let mut up = store
                .begin_upload(
                    &key,
                    Timestamp::from_unix_millis(EPOCH_MS),
                    WriteMode::Replace,
                )
                .await
                .unwrap();
            let mut peak = 0;
            for chunk in bytes.chunks(1 << 20) {
                up.write(chunk).await.unwrap();
                peak = peak.max(up.buffer_capacity());
            }
            up.commit().await.unwrap();
            peak
        })
    });
    println!("upload {tally:?} peak {peak}");
    assert_eq!(tally.part, parts, "one request per part");
    assert_eq!(tally.other, 2, "create and complete");
    let depth = 2 + waves(parts, concurrency.get());
    assert!(
        tally.rounds <= depth,
        "{} round trips, bound {depth} ({tally:?})",
        tally.rounds
    );
    assert!(
        tally.widest <= concurrency.get(),
        "{} in flight",
        tally.widest
    );
    assert!(
        peak <= (concurrency.get() + 1) * part_bytes,
        "{peak} bytes buffered, bound {} parts of {part_bytes}",
        concurrency.get() + 1
    );
}

// An AsOf plan reads the created-at time of the one file its decision needs
// and of up to `CREATION_TIME_CONCURRENCY` files it expects to need next, as
// one batch (one wave). The decisions depend on one another, so the depth is
// the length of that chain, not the number of HEADs. The ceilings are the
// chain lengths of the 10^4-file replica's planner today; the HEAD count
// bound is the one in `as_of_heads_are_bounded`. At width 1 the planner reads fewer
// files (it does not speculate) but each is a wave: 60 minutes back is 11
// waves against a ceiling of 8 (the 1 minute case has the same chain either
// way, so the other two carry the gate).
#[test]
fn as_of_created_at_reads_go_side_by_side() {
    let rig = Rig::build();
    let (store, link) = linked(&rig);
    let inventory = block_on(Inventory::load(&store)).unwrap();
    let minute = std::time::Duration::from_secs(60);
    // (minutes back, HEAD ceiling, wave ceiling)
    for (age, heads, rounds) in [(1_u32, 27_usize, 17_usize), (60, 18, 8), (120, 22, 12)] {
        let mut planner = AsOfPlanner::new(&inventory, instant_back(minute * age));
        let (resolved, tally) = link.measure(|| block_on(resolve(&store, &mut planner)).unwrap());
        let read = usize::try_from(resolved.created_at_read).unwrap();
        assert_eq!(tally.head, read, "{age} min: one HEAD per created-at read");
        assert_eq!(tally.requests(), tally.head, "{age} min: only HEADs");
        assert!(
            tally.head <= heads,
            "{age} min: {} HEADs, ceiling {heads}",
            tally.head
        );
        assert!(
            tally.rounds <= rounds,
            "{age} min: {} round trips, ceiling {rounds} ({tally:?})",
            tally.rounds
        );
        assert!(tally.widest <= HEAD_WIDTH);
    }
}

// A level-0 retention tick: list every level, read the created-at times the
// decision needs (the 300 level-0 files, among others), then delete the
// chosen keys. DeleteObjects takes 1000 keys, so a job is one request.
// Depth: the listing's, plus the HEAD waves (ceil(HEADs / width), with slack
// for batches the decision cuts short), plus the delete. A serial tick is
// every request in a row (331 here).
#[test]
fn level_zero_retention_tick_is_shallow() {
    let rig = Rig::build();
    let (store, link) = linked(&rig);
    let now = Timestamp::from_unix_millis(now_ms());
    let config = retention_config(L0Retention::After(std::time::Duration::from_secs(1)));
    let (keys, tally) = link.measure(|| {
        block_on(async {
            let inventory = Inventory::load(&store).await.unwrap();
            let mut state = MaintainState::default();
            let mut step = |resolved: &lsr_core::ResolvedCreatedAt| {
                due(
                    &inventory,
                    now,
                    &config,
                    Some(txid(TXNS)),
                    &mut state,
                    resolved,
                )
            };
            let resolved = resolve(&store, &mut step).await.unwrap();
            let Due::Run(Job::Delete(job)) = resolved.value else {
                panic!("expected a delete job");
            };
            let outcome = run_delete(&job, &store).await;
            assert!(matches!(outcome, Outcome::Deleted { .. }), "{outcome:?}");
            job.keys().len()
        })
    });
    assert_eq!(
        tally.delete,
        waves(keys, MAX_DELETE),
        "one DeleteObjects per 1000 keys"
    );
    assert_eq!(tally.list, 19, "one LIST per page");
    let slack = 8;
    let depth = 10 + waves(tally.head, HEAD_WIDTH) + slack + tally.delete;
    assert!(
        tally.rounds <= depth,
        "{} round trips, bound {depth} ({tally:?})",
        tally.rounds
    );
}

// DeleteObjects takes 1000 keys: 1000 keys are one request, 1001 are two,
// and each runs on its own round trip (deletes are not sent side by side).
#[test]
fn delete_splits_at_the_s3_cap() {
    for (count, requests) in [(1000_u64, 1_usize), (1001, 2)] {
        let link = Link::new(fake());
        let store =
            S3Store::with_transport(config(), credentials(), link.clone(), FakeClock::new(now()))
                .unwrap();
        let keys: Vec<FileKey> = (1..=count).map(|n| key(0, n, n)).collect();
        let ((), tally) = link.measure(|| block_on(store.delete(&keys)).unwrap());
        assert_eq!(tally.delete, requests, "{count} keys");
        assert_eq!(tally.rounds, requests, "{count} keys");
    }
}
