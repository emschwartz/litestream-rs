//! Witness token tests: what the set keeps, and that a token matches only the
//! bytes it was minted for. The boot pass is driven through the actor
//! (`crate::actor` tests, `witness`) and the simulator.

use lsr_ltx::{Encoder, HeaderFields, PageSize, Timestamp};

use super::*;

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

fn key(n: u64) -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(txid(n)))
}

fn file(n: u64, fill: u8) -> Vec<u8> {
    let page_size = PageSize::MIN;
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: 1,
        txids: TxidRange::single(txid(n)),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: None,
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
    assert!(ours.matches(ours.bytes, ours.checksum));
    // Another file, or the same content in other bytes, is not the file.
    assert!(!ours.matches(theirs.bytes, theirs.checksum));
    assert!(!ours.matches(theirs.bytes, ours.checksum));
    assert!(!ours.matches(ours.bytes, theirs.checksum));
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
