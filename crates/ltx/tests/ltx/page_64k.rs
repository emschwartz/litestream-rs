//! The largest SQLite page (65536 bytes) holding random bytes LZ4 cannot shrink, in both page
//! encodings the reader accepts: the sized LZ4 block Rust (and Go from ltx v0.5.3) writes, and
//! the LZ4 frame ltx v0.5.1 wrote, whose writer stores an incompressible block raw.
//! Upstream `TestDecoder_64KBPageSize/Incompressible`.

use lsr_ltx::{
    Checksum, Encoder, Header, HeaderFields, PageNo, PageSize, ReadDecoder, Timestamp, Txid,
    TxidRange, WalOrigin,
};

use crate::common::{Legacy, RawHeader, assemble, decode, decode_chunked};

const PS: u32 = 65_536;

/// `len` bytes from splitmix64: deterministic, and as incompressible as random bytes.
fn random_page(len: u32, seed: u64) -> Vec<u8> {
    let mut state = seed;
    let mut out = Vec::with_capacity(usize::try_from(len).unwrap());
    while out.len() < out.capacity() {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        out.extend_from_slice(&z.to_le_bytes());
    }
    out.truncate(out.capacity());
    out
}

fn page_size() -> PageSize {
    PageSize::new(PS).unwrap()
}

fn post_apply(page: &[u8]) -> Checksum {
    Checksum::EMPTY.toggle(Checksum::of_page(
        PageNo::new(1, page_size()).unwrap(),
        page,
    ))
}

/// The header upstream's test writes: a one-page snapshot at TXID 1, checksums tracked.
fn header() -> Header {
    header_at(1000)
}

fn header_at(millis: i64) -> Header {
    Header::new(HeaderFields {
        tracked: true,
        page_size: page_size(),
        commit: 1,
        txids: TxidRange::single(Txid::MIN),
        timestamp: Timestamp::from_unix_millis(millis),
        pre_apply: None,
        wal: WalOrigin::new(0, 0, 0, 0),
        node_id: 0,
    })
    .unwrap()
}

#[test]
fn the_page_is_incompressible() {
    let page = random_page(PS, 1);
    assert!(lz4_flex::block::compress(&page).len() > page.len());
}

#[test]
fn an_incompressible_64k_page_round_trips_block_encoded() {
    let page = random_page(PS, 1);
    let post = post_apply(&page);
    let mut encoder = Encoder::new(Vec::new(), header()).unwrap();
    encoder
        .write_page(PageNo::new(1, page_size()).unwrap(), &page)
        .unwrap();
    let (bytes, trailer) = encoder.finish(Some(post)).unwrap();
    // The first page header's flags: 1 is the sized LZ4 block, even when it is larger than
    // the page (upstream asserts the same of Go's encoder).
    assert_eq!(&bytes[100..106], &[0, 0, 0, 1, 0, 1]);
    assert!(bytes.len() > 100 + usize::try_from(PS).unwrap());

    let decoded = decode(&bytes).unwrap();
    assert_eq!(decoded.header, header());
    assert_eq!(decoded.pages, vec![(1, page.clone())]);
    assert_eq!(decoded.verified.trailer(), &trailer);
    assert_eq!(trailer.post_apply(), Some(post));
    assert_eq!(decode_chunked(&bytes, &[1, 4093, 65_537]).unwrap(), decoded);

    let mut reader = ReadDecoder::new(bytes.as_slice());
    let mut pages = Vec::new();
    while let Some(event) = reader.next_event().unwrap() {
        if let lsr_ltx::Event::Page { pgno, data } = event {
            pages.push((pgno.get(), data.to_vec()));
        }
    }
    assert_eq!(reader.finish().unwrap().trailer(), &trailer);
    assert_eq!(pages, vec![(1, page)]);
}

#[test]
fn an_incompressible_64k_page_decodes_from_an_lz4_frame() {
    let page = random_page(PS, 2);
    let post = post_apply(&page);
    // The hand-built header has no timestamp field: it is zero.
    let raw = RawHeader {
        flags: 0,
        ..RawHeader::untracked(PS, 1, 1)
    };
    // What ltx v0.5.1's pierrec writer emits for a block it cannot shrink: 64 KiB blocks
    // (BD 0x40), the block stored raw under the high bit of its size.
    let frame = Legacy::stored(&page).frame(1, &page);
    assert_eq!(&frame.bytes[..6], &[0, 0, 0, 1, 0, 0], "an LZ4 frame page");
    let bytes = assemble(&raw, &[frame], post.get());

    let decoded = decode(&bytes).unwrap();
    assert_eq!(decoded.header, header_at(0));
    assert_eq!(decoded.pages, vec![(1, page.clone())]);
    assert_eq!(decoded.verified.trailer().post_apply(), Some(post));
    assert_eq!(decode_chunked(&bytes, &[7, 65_536, 3]).unwrap(), decoded);

    // Rust re-encodes it as a block, and the page survives that too.
    let mut encoder = Encoder::new(Vec::new(), decoded.header).unwrap();
    encoder
        .write_page(PageNo::new(1, page_size()).unwrap(), &page)
        .unwrap();
    let (again, _) = encoder.finish(Some(post)).unwrap();
    assert_eq!(decode(&again).unwrap().pages, vec![(1, page)]);
}
