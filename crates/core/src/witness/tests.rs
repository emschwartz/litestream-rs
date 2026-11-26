//! Witness token tests: what the set keeps, and that a token matches only the
//! bytes it was minted for; and what the boot's pass makes of the staged files it is
//! handed. The pass inside a boot is driven through the actor (`crate::actor` tests,
//! `witness`) and the simulator.

use std::io::Cursor;

use lsr_ltx::{Encoder, HeaderFields, PageNo, PageSize, Timestamp, TxidRange, WalOrigin};

use super::*;
use crate::conformance::block_on;
use crate::fault::role::{Replica, Role, Staging};
use crate::layout::Level;
use crate::mem::MemStore;

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn key(n: u64) -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(txid(n)))
}

fn file(n: u64, fill: u8) -> Vec<u8> {
    file_from(n, fill, None)
}

/// The WAL span of `frames` frames from frame `first` (counting from 0) of the generation
/// salted `salt`.
fn span(first: u64, frames: u64, salt: u32) -> Option<WalOrigin> {
    // A frame header and a page of `PageSize::MIN`; the first frame follows the WAL header.
    const FRAME: u64 = 536;
    WalOrigin::new(
        first.saturating_mul(FRAME).saturating_add(32),
        frames.saturating_mul(FRAME),
        salt,
        salt,
    )
}

/// A whole file for transaction `n` whose header names the WAL span `wal`.
fn file_from(n: u64, fill: u8, wal: Option<WalOrigin>) -> Vec<u8> {
    let page_size = PageSize::MIN;
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: 1,
        txids: TxidRange::single(txid(n)),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    encoder
        .write_page(
            PageNo::new(1, page_size).unwrap(),
            &vec![fill; page_size.bytes()],
        )
        .unwrap();
    encoder.finish(None).unwrap().0
}

fn token(n: u64, fill: u8) -> Witnessed {
    Witnessed::of_bytes(key(n), &file(n, fill)).unwrap()
}

#[test]
fn a_token_matches_only_the_bytes_it_was_minted_for() {
    let ours = token(1, 1);
    let theirs = token(1, 2);
    assert!(ours.matches_sealed(ours.bytes, ours.checksum));
    // Another file, or the same content in other bytes, is not the file.
    assert!(!ours.matches_sealed(theirs.bytes, theirs.checksum));
    assert!(!ours.matches_sealed(theirs.bytes, ours.checksum));
    assert!(!ours.matches_sealed(ours.bytes, theirs.checksum));
}

/// The tally tells streams apart by length and by bytes, however they are
/// cut into chunks.
#[test]
fn a_byte_tally_is_the_stream_not_its_chunks() {
    let mut whole = ByteTally::new();
    whole.add(b"abcdef");
    let mut parts = ByteTally::new();
    parts.add(b"ab");
    parts.add(b"");
    parts.add(b"cdef");
    assert_eq!(whole, parts);
    assert_eq!(whole.seal(), parts.seal());
    let mut flipped = ByteTally::new();
    flipped.add(b"abcdeg");
    assert_ne!(whole, flipped);
    let mut longer = ByteTally::new();
    longer.add(b"abcdef\0");
    assert_ne!(whole, longer);
}

#[test]
fn a_token_is_found_only_under_its_own_key() {
    let mut all = Witnesses::new();
    all.insert(token(3, 1));
    assert!(all.get(&key(3)).is_some());
    assert!(all.get(&key(4)).is_none());
    let wider = FileKey::new(Level::ZERO, TxidRange::new(txid(2), txid(3)).unwrap());
    assert!(all.get(&wider).is_none());
    let higher = FileKey::new(Level::new(1).unwrap(), TxidRange::single(txid(3)));
    assert!(all.get(&higher).is_none());
}

#[test]
fn confirmation_and_drops_let_tokens_go() {
    let mut all: Witnesses = (1..=5).map(|n| token(n, 1)).collect();
    all.confirmed(Some(txid(2)));
    assert_eq!(all.len(), 3);
    all.drop_from(txid(5));
    assert_eq!(all.len(), 2);
    assert!(all.get(&key(3)).is_some() && all.get(&key(4)).is_some());
    all.confirmed(None);
    assert_eq!(all.len(), 2);
    assert!(!all.is_empty());
    all.drop_from(txid(1));
    assert!(all.is_empty());
    assert!(Witnesses::new().is_empty());
    // A file staged again under a key replaces its token.
    all.insert(token(4, 9));
    assert_eq!(all.get(&key(4)), Some(&token(4, 9)));
}

#[test]
fn room_runs_out_at_the_cap() {
    let one = token(1, 1);
    let mut all = Witnesses::new();
    let cap = u64::try_from(MAX_WITNESSED).unwrap();
    for n in 1..cap {
        all.insert(Witnessed { key: key(n), ..one });
    }
    assert!(all.has_room(1));
    assert!(!all.has_room(2));
    all.insert(Witnessed {
        key: key(cap),
        ..one
    });
    assert_eq!(all.len(), MAX_WITNESSED);
    assert!(!all.has_room(1));
    assert!(all.has_room(0));
    // Replacing a held token is not growth.
    all.insert(token(1, 2));
    assert_eq!(all.get(&key(1)), Some(&token(1, 2)));
    assert_eq!(all.len(), MAX_WITNESSED);
    all.confirmed(Some(txid(1)));
    assert!(all.has_room(1));
}

/// A staged file longer than one read decodes whole: only a short read ends
/// the file.
#[test]
fn a_file_longer_than_one_read_is_decoded_whole() {
    let page_size = PageSize::new(4096).unwrap();
    let count = 300_u32;
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: count,
        txids: TxidRange::single(txid(1)),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    // Bytes LZ4 cannot shrink, so the file outgrows one read.
    let mut state = 0x9e37_79b9_u32;
    let mut noise = || {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        state.to_le_bytes()[0]
    };
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in 1..=count {
        let data: Vec<u8> = (0..page_size.bytes()).map(|_| noise()).collect();
        encoder
            .write_page(PageNo::new(n, page_size).unwrap(), &data)
            .unwrap();
    }
    let bytes = encoder.finish(None).unwrap().0;
    assert!(
        bytes.len() > usize::try_from(READ.get()).unwrap(),
        "{}",
        bytes.len()
    );
    let store = crate::mem::MemStore::new();
    crate::conformance::block_on(async {
        let mut up = store
            .begin_upload(
                &key(1),
                Timestamp::from_unix_millis(1),
                crate::store::WriteMode::Replace,
            )
            .await
            .unwrap();
        crate::store::Upload::write(&mut up, &bytes).await.unwrap();
        crate::store::Upload::commit(up).await.unwrap();
    });
    let decoded = crate::conformance::block_on(walk(
        &store,
        key(1),
        Staging::<crate::mem::MemStore>::fault,
        |_| Ok(()),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(decoded.header.fields().commit, 300);
    let mut tally = ByteTally::new();
    tally.add(&bytes);
    assert_eq!(decoded.bytes, tally);
}

fn put(store: &MemStore, key: FileKey, bytes: &[u8]) {
    block_on(async {
        let mut up = store
            .begin_upload(
                &key,
                Timestamp::from_unix_millis(1),
                crate::store::WriteMode::Replace,
            )
            .await
            .unwrap();
        crate::store::Upload::write(&mut up, bytes).await.unwrap();
        crate::store::Upload::commit(up).await.unwrap();
    });
}

/// A store holding one file per `(transaction, span)`.
fn holding(files: &[(u64, Option<WalOrigin>)]) -> MemStore {
    let store = MemStore::new();
    for (n, wal) in files {
        put(&store, key(*n), &file_from(*n, 7, *wal));
    }
    store
}

/// The boot's pass with no WAL to compare with.
fn pass(staging: &MemStore, remote: &MemStore, confirmed: Option<u64>) -> Scan {
    block_on(scan(
        &Staging(staging),
        Base::Replica(&Replica(remote), confirmed.map(txid)),
        None::<BootWal<'_, Cursor<Vec<u8>>>>,
    ))
    .unwrap()
}

/// The transactions the pass minted tokens for, in order.
fn witnessed(scan: &Scan) -> Vec<u64> {
    scan.tokens
        .iter()
        .map(|token| token.key().range().max().get())
        .collect()
}

/// Files that each start where the one before ended get a token apiece, oldest first, and
/// every one is kept for recovery and seeds the page table.
#[test]
fn pending_files_that_chain_are_all_witnessed() {
    let staging = holding(&[(1, span(0, 1, 5)), (2, span(1, 2, 5)), (3, span(3, 1, 5))]);
    let found = pass(&staging, &MemStore::new(), None);
    assert_eq!(found.bad, None);
    assert_eq!(witnessed(&found), vec![1, 2, 3]);
    let kept: Vec<u64> = found.scanned.keys().map(|txid| txid.get()).collect();
    assert_eq!(kept, vec![1, 2, 3]);
    assert_eq!(found.seeding.finish().table.len(), 1);
}

/// The key is part of what a read verifies: a whole, valid file for another transaction
/// under this key is no file of this key's.
#[test]
fn a_file_under_another_transactions_key_does_not_read() {
    let staging = MemStore::new();
    put(&staging, key(1), &file_from(2, 7, span(0, 1, 5)));
    let found = pass(&staging, &MemStore::new(), None);
    assert_eq!(found.bad, Some((key(1), Bad::Unreadable)));
    assert!(found.tokens.is_empty());
    assert!(found.scanned.is_empty());
}

/// A pending file must say where in the WAL it came from.
#[test]
fn a_pending_file_that_names_no_wal_range_does_not_chain() {
    let staging = holding(&[(1, None)]);
    let found = pass(&staging, &MemStore::new(), None);
    assert_eq!(found.bad, Some((key(1), Bad::Unchained)));
    assert!(found.tokens.is_empty());
}

/// A file that starts neither where the one before it ended nor at the first frame is
/// refused with all after it, and the files before it keep their tokens. The same offsets
/// in another generation do not chain either.
#[test]
fn a_pending_file_from_elsewhere_in_the_wal_ends_the_run() {
    for second in [span(4, 1, 5), span(1, 1, 6), span(2, 1, 5)] {
        let staging = holding(&[(1, span(0, 1, 5)), (2, second), (3, span(5, 1, 5))]);
        let found = pass(&staging, &MemStore::new(), None);
        assert_eq!(found.bad, Some((key(2), Bad::Unchained)), "{second:?}");
        assert_eq!(witnessed(&found), vec![1]);
        assert_eq!(found.scanned.len(), 1);
    }
}

/// A file at the first frame starts a generation or a snapshot, whatever came before it.
#[test]
fn a_pending_file_at_the_first_frame_starts_a_run() {
    let staging = holding(&[(1, span(0, 3, 5)), (2, span(0, 1, 6))]);
    let found = pass(&staging, &MemStore::new(), None);
    assert_eq!(found.bad, None);
    assert_eq!(witnessed(&found), vec![1, 2]);
}

/// A transaction with no staged file leaves a gap no upload may cross.
#[test]
fn a_gap_in_the_pending_transactions_ends_the_run() {
    let staging = holding(&[(1, span(0, 1, 5)), (3, span(1, 1, 5))]);
    let found = pass(&staging, &MemStore::new(), None);
    assert_eq!(found.bad, Some((key(3), Bad::Unchained)));
    assert_eq!(witnessed(&found), vec![1]);
    let staging = holding(&[(2, span(0, 1, 5))]);
    let found = pass(&staging, &MemStore::new(), None);
    assert_eq!(found.bad, Some((key(2), Bad::Unchained)));
}

/// The staged copy of the replica's newest file gives the first pending file its link and
/// gets no token itself: the replica, which holds nothing here, is not asked.
#[test]
fn the_confirmed_files_staged_copy_links_the_first_pending_file() {
    let staging = holding(&[(1, span(0, 1, 5)), (2, span(1, 1, 5))]);
    let found = pass(&staging, &MemStore::new(), Some(1));
    assert_eq!(found.bad, None);
    assert_eq!(witnessed(&found), vec![2]);
    assert_eq!(found.scanned.len(), 2);
}

/// With the replica's newest file gone from staging, its link is read from the replica: a
/// pending file that follows it chains, one that does not is refused.
#[test]
fn the_replicas_newest_file_links_the_first_pending_file_when_staging_lacks_it() {
    let remote = holding(&[(1, span(0, 1, 5))]);
    let found = pass(&holding(&[(2, span(1, 1, 5))]), &remote, Some(1));
    assert_eq!((found.bad, witnessed(&found)), (None, vec![2]));
    let found = pass(&holding(&[(2, span(2, 1, 5))]), &remote, Some(1));
    assert_eq!(found.bad, Some((key(2), Bad::Unchained)));
}

/// A replica's newest file that does not decode links nothing: only a file at the first
/// frame can follow it.
#[test]
fn a_replica_tail_that_does_not_decode_links_only_a_first_frame_file() {
    let remote = MemStore::new();
    put(&remote, key(1), b"not an ltx file");
    let found = pass(&holding(&[(2, span(1, 1, 5))]), &remote, Some(1));
    assert_eq!(found.bad, Some((key(2), Bad::Unchained)));
    let found = pass(&holding(&[(2, span(0, 1, 5))]), &remote, Some(1));
    assert_eq!((found.bad, witnessed(&found)), (None, vec![2]));
}

/// The pass with no replica to ask.
fn pass_from_oldest(staging: &MemStore) -> Scan {
    block_on(scan(
        &Staging(staging),
        Base::<Replica<'_, MemStore>>::Oldest,
        None::<BootWal<'_, Cursor<Vec<u8>>>>,
    ))
    .unwrap()
}

/// With no replica asked, the oldest staged file stands where the replica's newest would:
/// it gets no token and may start anywhere, and every file after it is pending.
#[test]
fn with_no_replica_the_oldest_staged_file_is_the_base() {
    let staging = holding(&[(3, span(4, 1, 5)), (4, span(5, 1, 5)), (5, span(6, 2, 5))]);
    let found = pass_from_oldest(&staging);
    assert_eq!(found.bad, None);
    assert_eq!(witnessed(&found), vec![4, 5]);
    assert_eq!(found.scanned.len(), 3);
}

/// The files after the base must chain from it, with no gap, and the base must read whole.
#[test]
fn with_no_replica_the_files_after_the_oldest_must_follow_it() {
    let found = pass_from_oldest(&holding(&[(3, span(4, 1, 5)), (5, span(5, 1, 5))]));
    assert_eq!(found.bad, Some((key(5), Bad::Unchained)));
    let found = pass_from_oldest(&holding(&[(3, span(4, 1, 5)), (4, span(7, 1, 5))]));
    assert_eq!(found.bad, Some((key(4), Bad::Unchained)));
    let staging = holding(&[(4, span(5, 1, 5))]);
    put(&staging, key(3), b"not an ltx file");
    let found = pass_from_oldest(&staging);
    assert_eq!(found.bad, Some((key(3), Bad::Unreadable)));
    assert!(pass_from_oldest(&MemStore::new()).scanned.is_empty());
}

/// The definition the incremental tally must meet: whole blocks into the lanes by position, then
/// the length, the lanes and the last few bytes folded one by one.
fn reference_seal(bytes: &[u8]) -> Sealed {
    let mut lanes = SEEDS;
    let (blocks, remainder) = bytes.as_chunks::<BLOCK>();
    for block in blocks {
        for (lane, word) in lanes.iter_mut().zip(block.as_chunks::<WORD>().0) {
            *lane = (*lane ^ u64::from_le_bytes(*word))
                .wrapping_mul(MULTIPLIER)
                .rotate_left(31);
        }
    }
    let size = u64::try_from(bytes.len()).unwrap();
    let mut digest = size;
    for lane in lanes {
        digest = (digest ^ lane).wrapping_mul(MULTIPLIER).rotate_left(29);
    }
    for byte in remainder {
        digest = (digest ^ u64::from(*byte))
            .wrapping_mul(MULTIPLIER)
            .rotate_left(29);
    }
    Sealed { size, digest }
}

fn tally_in(bytes: &[u8], cuts: &[usize]) -> ByteTally {
    let mut tally = ByteTally::new();
    let mut from = 0;
    for cut in cuts {
        let to = (*cut).min(bytes.len()).max(from);
        tally.add(&bytes[from..to]);
        from = to;
    }
    tally.add(&bytes[from..]);
    tally
}

#[test]
fn the_empty_stream_and_each_length_around_a_block_match_the_definition() {
    let data: Vec<u8> = (0..=255).cycle().take(4 * BLOCK + 1).collect();
    for len in 0..=data.len() {
        let bytes = &data[..len];
        assert_eq!(tally_in(bytes, &[]).seal(), reference_seal(bytes), "{len}");
        // One byte at a time is the worst split for the carried block.
        let cuts: Vec<usize> = (0..len).collect();
        assert_eq!(
            tally_in(bytes, &cuts).seal(),
            reference_seal(bytes),
            "{len}"
        );
    }
}

#[test]
fn every_two_way_split_of_a_stream_across_three_blocks_seals_alike() {
    let data: Vec<u8> = (0..=255).rev().cycle().take(3 * BLOCK + 7).collect();
    let want = reference_seal(&data);
    for cut in 0..=data.len() {
        assert_eq!(tally_in(&data, &[cut]).seal(), want, "cut at {cut}");
    }
}

#[test]
fn a_tally_differs_for_one_flipped_bit_one_swapped_word_and_a_trailing_zero() {
    let data: Vec<u8> = (0..=255).cycle().take(3 * BLOCK + 5).collect();
    let base = reference_seal(&data);
    for at in 0..data.len() {
        let mut flipped = data.clone();
        flipped[at] ^= 1;
        assert_ne!(tally_in(&flipped, &[]).seal(), base, "bit at {at}");
    }
    let mut swapped = data.clone();
    swapped.copy_within(0..WORD, WORD);
    assert_ne!(tally_in(&swapped, &[]).seal(), base);
    let mut longer = data.clone();
    longer.push(0);
    assert_ne!(tally_in(&longer, &[]).seal(), base);
}

proptest::proptest! {
    // No shrinking: the direct tests above name the small failing cases, and shrinking a failure
    // under a mutant outlasts the mutation run's timeout.
    #![proptest_config(proptest::prelude::ProptestConfig {
        cases: 64,
        max_shrink_iters: 0,
        ..proptest::prelude::ProptestConfig::default()
    })]
    #[test]
    fn any_split_of_any_bytes_seals_as_the_definition(
        bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..400),
        cuts in proptest::collection::vec(0_usize..400, 0..8),
    ) {
        let mut cuts = cuts;
        cuts.sort_unstable();
        proptest::prop_assert_eq!(tally_in(&bytes, &cuts).seal(), reference_seal(&bytes));
    }
}
