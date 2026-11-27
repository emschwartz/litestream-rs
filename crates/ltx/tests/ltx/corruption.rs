//! A corrupt file never verifies with different pages, and almost never
//! verifies at all: every truncation of a small file is an error, and a
//! single changed byte is an error unless it lies inside a compressed LZ4
//! block (and is not that block's last token) and the file still decodes to
//! the same header and pages.
//!
//! That one exception is LZ4 aliasing: a block is one of many encodings of
//! its page, and a changed offset or match length can copy equal bytes from
//! elsewhere. The file checksum covers decompressed pages, as Go's encoder
//! defines it, so no reader can see such a change; restore is unaffected.
//! Every other byte is checked: the header, page headers and block lengths and
//! the index and trailer by the file checksum, a legacy frame's descriptor and
//! checksums by its xxHash32 checksums, each block's last token by
//! [`lsr_ltx::FrameError::BlockEnd`], and literals by the page they produce.

use std::fs;
use std::ops::Range;

use lsr_ltx::testing::compressed_block_spans;
use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange};
use proptest::prelude::*;

use crate::common::{Decoded, decode, fixtures_dir};

/// Asserts the property for `flipped`, `original` with byte `at` changed.
fn assert_fails_or_aliases(original: &Decoded, spans: &[Range<usize>], flipped: &[u8], at: usize) {
    if let Ok(decoded) = decode(flipped) {
        assert_eq!(
            (decoded.header, &decoded.pages),
            (original.header, &original.pages),
            "a change at {at} verified with other pages"
        );
        assert!(
            spans.iter().any(|span| span.contains(&at)),
            "a change at {at}, outside every LZ4 block, verified; blocks {spans:?}"
        );
    }
}

fn flip_all(bytes: &[u8], masks: &[u8]) -> usize {
    let original = decode(bytes).unwrap();
    let spans = compressed_block_spans(bytes).expect("the file's blocks parse");
    let mut silent = 0;
    for at in 0..bytes.len() {
        for mask in masks {
            let mut flipped = bytes.to_vec();
            flipped[at] ^= mask;
            assert_fails_or_aliases(&original, &spans, &flipped, at);
            silent += usize::from(decode(&flipped).is_ok());
        }
    }
    silent
}

fn small_fixtures() -> Vec<Vec<u8>> {
    ["ltx-v0.5.1", "ltx-v0.5.3"]
        .iter()
        .flat_map(|version| {
            [
                "0000000000000003-0000000000000003.ltx",
                "0000000000000004-0000000000000005.ltx",
            ]
            .map(|name| fs::read(fixtures_dir().join(version).join("512").join(name)).unwrap())
        })
        .collect()
}

#[test]
fn every_truncation_is_an_error() {
    for bytes in small_fixtures() {
        for len in 0..bytes.len() {
            assert!(
                decode(&bytes[..len]).is_err(),
                "prefix of {len} bytes verified"
            );
        }
    }
}

#[test]
fn every_bit_flip_of_a_go_file_fails_or_aliases() {
    let bits: Vec<u8> = (0..8).map(|bit| 1 << bit).collect();
    let mut silent = 0;
    for bytes in small_fixtures() {
        silent += flip_all(&bytes, &bits);
    }
    // Aliasing exists in these files (runs of equal bytes), so the walk
    // above did meet the exception it forgives.
    assert!(silent > 0);
}

/// A small file the strict encoder writes: 512-byte pages that are random,
/// periodic or zero, so some blocks alias and some do not.
fn rust_file() -> impl Strategy<Value = Vec<u8>> {
    let page = prop_oneof![
        prop::collection::vec(any::<u8>(), 512),
        (any::<u8>(), 1..64_usize).prop_map(|(seed, period)| (0..512)
            .map(|i| seed.wrapping_add(u8::try_from(i % period).unwrap()))
            .collect()),
        Just(vec![0; 512]),
    ];
    (prop::collection::vec(page, 1..4), any::<bool>()).prop_map(|(pages, tracked)| {
        let page_size = PageSize::new(512).unwrap();
        let commit = u32::try_from(pages.len()).unwrap();
        let header = Header::new(HeaderFields {
            tracked,
            page_size,
            commit,
            txids: TxidRange::new(Txid::new(2).unwrap(), Txid::new(3).unwrap()).unwrap(),
            timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
            pre_apply: tracked.then_some(lsr_ltx::DbChecksum::EMPTY),
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for (pgno, data) in (1..).zip(&pages) {
            encoder
                .write_page(PageNo::new(pgno, page_size).unwrap(), data)
                .unwrap();
        }
        let post = tracked.then(|| lsr_ltx::DbChecksum::from_raw(1 << 63 | 7).unwrap());
        encoder.finish(post).unwrap().0
    })
}

proptest! {
    /// Any one byte of a Rust-written file, changed to any other value.
    #[test]
    fn any_byte_change_to_a_rust_file_fails_or_aliases(
        bytes in rust_file(),
        at in any::<prop::sample::Index>(),
        mask in 1..=255_u8,
    ) {
        let original = decode(&bytes).unwrap();
        let spans = compressed_block_spans(&bytes).expect("the file's blocks parse");
        let at = at.index(bytes.len());
        let mut flipped = bytes.clone();
        flipped[at] ^= mask;
        assert_fails_or_aliases(&original, &spans, &flipped, at);
    }
}

/// Every byte of one small Rust-written file, changed to every other value:
/// a zero page and a periodic one.
#[test]
fn every_byte_change_to_a_small_rust_file_fails_or_aliases() {
    let page_size = PageSize::new(512).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit: 2,
        txids: TxidRange::new(Txid::new(1).unwrap(), Txid::new(1).unwrap()).unwrap(),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let periodic: Vec<u8> = (0..512).map(|i| u8::try_from(i % 13).unwrap()).collect();
    for (pgno, data) in [(1, vec![0; 512]), (2, periodic)] {
        encoder
            .write_page(PageNo::new(pgno, page_size).unwrap(), &data)
            .unwrap();
    }
    let bytes = encoder.finish(None).unwrap().0;
    let masks: Vec<u8> = (1..=255).collect();
    flip_all(&bytes, &masks);
}
