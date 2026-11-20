//! Behaviour a reader of the replica can observe that mutation testing found
//! unpinned: see docs/mutants.md.

use lsr_ltx::{
    Checksum, Decoder, Event, Header, HeaderFields, IndexError, PageIndex, PageSize, TRAILER_LEN,
    Timestamp, Trailer, Txid, TxidRange,
};

use crate::common::{RawHeader, assemble, block_frame, page, uvarint};

const PS: u32 = 512;

#[test]
fn decoder_header_appears_with_the_header_event_and_stays() {
    let header = RawHeader::untracked(PS, 2, 1);
    let file = assemble(
        &header,
        &[block_frame(1, &page(PS, 1)), block_frame(2, &page(PS, 2))],
        0,
    );
    let mut decoder = Decoder::new();
    assert_eq!(decoder.header(), None);
    let mut rest = file.as_slice();
    let mut seen = None;
    while !rest.is_empty() {
        let (used, event) = decoder.feed(rest).unwrap();
        if let Some(Event::Header(header)) = event {
            seen = Some(header);
        }
        rest = &rest[used..];
        if seen.is_some() {
            assert_eq!(decoder.header(), seen.as_ref());
        }
    }
    let seen = seen.expect("a header event");
    assert_eq!(seen.fields().commit, 2);
    assert_eq!(decoder.header(), Some(&seen), "after the trailer too");
}

fn header(tracked: bool) -> Header {
    Header::new(HeaderFields {
        tracked,
        page_size: PageSize::new(PS).unwrap(),
        commit: 3,
        txids: TxidRange::single(Txid::MIN),
        timestamp: Timestamp::from_unix_millis(1),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap()
}

#[test]
fn trailer_encodes_post_apply_then_file_checksum_big_endian() {
    let flag = 1_u64 << 63;
    let mut bytes = [0_u8; TRAILER_LEN];
    bytes[8..].copy_from_slice(&(flag | 0x0102_0304_0506_0708).to_be_bytes());
    let untracked = Trailer::decode(&bytes, &header(false)).unwrap();
    assert_eq!(untracked.encode(), bytes);

    bytes[..8].copy_from_slice(&(flag | 0x1122_3344_5566_7788).to_be_bytes());
    let tracked = Trailer::decode(&bytes, &header(true)).unwrap();
    assert_eq!(
        tracked.post_apply(),
        Checksum::from_raw(flag | 0x1122_3344_5566_7788)
    );
    assert_eq!(tracked.encode(), bytes);
}

fn index_region(size: u64) -> Vec<u8> {
    let mut out = Vec::new();
    for value in [1, 100, size, 0] {
        uvarint(&mut out, value);
    }
    let len = u64::try_from(out.len()).unwrap();
    out.extend_from_slice(&len.to_be_bytes());
    out
}

/// The shortest frame is a page header, a block size and one block byte; the
/// longest is what one page can need at worst. Both bounds are inclusive.
#[test]
fn index_frame_sizes_are_accepted_at_both_bounds_and_refused_beyond() {
    let snapshot = Header::decode(&RawHeader::untracked(PS, 1, 1).bytes()).unwrap();
    // 6 page header + 15 descriptor (magic, FLG, BD, content size, checksum)
    // + 4 block size + the LZ4 bound + 4 block checksum + 4 end mark + 4
    // content checksum.
    let bound = u64::try_from(lz4_flex::block::get_maximum_output_size(512)).unwrap();
    let (min, max) = (11, 37 + bound);
    for size in [min, max] {
        PageIndex::decode(&index_region(size), &snapshot).unwrap();
    }
    for size in [min - 1, max + 1] {
        assert_eq!(
            PageIndex::decode(&index_region(size), &snapshot),
            Err(IndexError::FrameSize { pgno: 1, size })
        );
    }
}
