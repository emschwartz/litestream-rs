//! Every error variant, each produced by the smallest input that causes it.

use std::io::{self, Read, Write};

use lsr_ltx::{
    Checksum, DecodeError, Decoder, EncodeError, Encoder, FrameError, Header, HeaderError,
    HeaderFields, IndexEntry, IndexError, OrderError, PageIndex, PageNo, PageSize, ReadDecoder,
    Section, Timestamp, Trailer, TrailerError, Txid, TxidRange, decode_frame, lock_page,
};

use crate::common::{
    Frame, Legacy, RawHeader, STORED, assemble, assemble_with_index, block_frame, block_frame_raw,
    decode, index_for, page, page_header, uvarint,
};

const PS: u32 = 512;

fn snapshot_frames() -> Vec<Frame> {
    vec![block_frame(1, &page(PS, 1)), block_frame(2, &page(PS, 2))]
}

fn snapshot() -> (RawHeader, Vec<u8>) {
    let header = RawHeader::untracked(PS, 2, 1);
    let bytes = assemble(&header, &snapshot_frames(), 0);
    (header, bytes)
}

fn err(bytes: &[u8]) -> DecodeError {
    decode(bytes).expect_err("the file is corrupt")
}

/// Decodes a file holding just page 1 of a one-page snapshot, in this frame.
fn frame_err(frame: &Frame) -> FrameError {
    let got = err(&assemble(
        &RawHeader::untracked(PS, 1, 1),
        std::slice::from_ref(frame),
        0,
    ));
    let DecodeError::Frame(inner) = got else {
        panic!("expected a frame error, got {got:?}")
    };
    inner
}

fn parsed(header: &RawHeader) -> Header {
    Header::decode(&header.bytes()).unwrap()
}

/// The index entry for page `pgno` of the two-page snapshot.
fn entry(pgno: u32) -> IndexEntry {
    let mut region = index_for(&snapshot_frames());
    region.extend_from_slice(&u64::try_from(region.len()).unwrap().to_be_bytes());
    let index = PageIndex::decode(&region, &parsed(&RawHeader::untracked(PS, 2, 1))).unwrap();
    *index
        .get(PageNo::new(pgno, PageSize::new(PS).unwrap()).unwrap())
        .unwrap()
}

#[test]
fn hand_built_files_decode() {
    let (_, bytes) = snapshot();
    assert_eq!(decode(&bytes).unwrap().pages.len(), 2);
    let legacy = Legacy::stored(&page(PS, 1)).frame(1, &page(PS, 1));
    assert_eq!(
        decode(&assemble(&RawHeader::untracked(PS, 1, 1), &[legacy], 0))
            .unwrap()
            .pages
            .len(),
        1
    );
}

#[test]
fn header_error() {
    let (_, mut bytes) = snapshot();
    bytes[0] = b'X';
    assert!(matches!(
        err(&bytes),
        DecodeError::Header(HeaderError::Magic { .. })
    ));
}

#[test]
fn frame_page_header_flags() {
    let mut frame = block_frame(1, &page(PS, 1));
    frame.bytes[5] = 3;
    assert_eq!(frame_err(&frame), FrameError::PageHeaderFlags { flags: 3 });
}

#[test]
fn frame_zero_page_number() {
    let frame = Frame {
        pgno: 1,
        bytes: page_header(0, 1),
        hashed: vec![],
    };
    assert_eq!(frame_err(&frame), FrameError::ZeroPageNumber);
}

#[test]
fn frame_end_marker_where_a_frame_belongs() {
    let header = parsed(&RawHeader::untracked(PS, 1, 1));
    assert!(matches!(
        decode_frame(&[0; 6], &header, &entry(1), &mut Vec::new()),
        Err(DecodeError::Frame(FrameError::EndMarker))
    ));
}

#[test]
fn frame_block_len_zero() {
    let max = u64::try_from(lz4_flex::block::get_maximum_output_size(512)).unwrap();
    assert_eq!(
        frame_err(&block_frame_raw(1, &[], &page(PS, 1))),
        FrameError::BlockLen { len: 0, max }
    );
}

#[test]
fn huge_claimed_sizes_fail_before_buffering() {
    // Only the length field is fed: the error cannot have waited for the data.
    let mut bytes = RawHeader::untracked(PS, 1, 1).bytes().to_vec();
    bytes.extend_from_slice(&page_header(1, 1));
    bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    let mut decoder = Decoder::new();
    let (used, _) = decoder.feed(&bytes).unwrap();
    assert_eq!(used, 100);
    assert!(matches!(
        decoder.feed(&bytes[100..]),
        Err(DecodeError::Frame(FrameError::BlockLen {
            len: 0xffff_ffff,
            ..
        }))
    ));

    let mut legacy = Legacy::stored(&page(PS, 1));
    legacy.blocks = vec![(STORED | 0x7fff_ffff, vec![])];
    let frame = legacy.frame(1, &page(PS, 1));
    let mut decoder = Decoder::new();
    let header = RawHeader::untracked(PS, 1, 1).bytes();
    decoder.feed(&header).unwrap();
    let result = decoder.feed(&frame.bytes[..6 + 7 + 4]);
    assert!(matches!(
        result,
        Err(DecodeError::Frame(FrameError::BlockLen {
            len: 0x7fff_ffff,
            max: 512
        }))
    ));
}

#[test]
fn frame_stored_block_longer_than_the_page() {
    let mut legacy = Legacy::stored(&page(PS, 1));
    legacy.blocks.push((STORED | 1, vec![9]));
    assert_eq!(
        frame_err(&legacy.frame(1, &page(PS, 1))),
        FrameError::BlockLen { len: 1, max: 0 }
    );
}

#[test]
fn frame_magic() {
    let mut legacy = Legacy::stored(&page(PS, 1));
    legacy.magic = 0x184d_2a50_u32.to_le_bytes(); // a skippable frame
    assert_eq!(
        frame_err(&legacy.frame(1, &page(PS, 1))),
        FrameError::FrameMagic {
            found: legacy.magic
        }
    );
}

#[test]
fn frame_descriptor() {
    for (flg, bd) in [
        (0x65, 0x40),
        (0x66, 0x40),
        (0x24, 0x40),
        (0xa4, 0x40),
        (0x64, 0x30),
        (0x64, 0x41),
        (0x64, 0xc0),
    ] {
        let legacy = Legacy {
            flg,
            bd,
            ..Legacy::stored(&page(PS, 1))
        };
        assert_eq!(
            frame_err(&legacy.frame(1, &page(PS, 1))),
            FrameError::FrameDescriptor { flg, bd },
            "{flg:#x} {bd:#x}"
        );
    }
}

#[test]
fn frame_descriptor_options_go_may_write_are_accepted() {
    let data = page(PS, 1);
    let variants = [
        Legacy {
            flg: 0x6c,
            content_size: Some(512),
            ..Legacy::stored(&data)
        }, // content size
        Legacy {
            flg: 0x70,
            ..Legacy::compressed(&data)
        }, // block checksums, no content checksum
        Legacy {
            bd: 0x70,
            ..Legacy::compressed(&data)
        }, // 4 MiB blocks
    ];
    for legacy in variants {
        let file = assemble(
            &RawHeader::untracked(PS, 1, 1),
            &[legacy.frame(1, &data)],
            0,
        );
        assert_eq!(
            decode(&file).unwrap().pages,
            [(1, data.clone())],
            "{legacy:?}"
        );
    }
}

#[test]
fn frame_with_several_blocks_and_dependent_blocks_decodes() {
    let data = page(PS, 3);
    let (first, second) = data.split_at(200);
    let independent = Legacy {
        blocks: vec![
            (STORED | 200, first.to_vec()),
            (
                u32::try_from(lz4_flex::block::compress(second).len()).unwrap(),
                lz4_flex::block::compress(second),
            ),
        ],
        ..Legacy::stored(&data)
    };
    let dependent_block = lz4_flex::block::compress_with_dict(second, first);
    let dependent = Legacy {
        flg: 0x44,
        blocks: vec![
            (STORED | 200, first.to_vec()),
            (
                u32::try_from(dependent_block.len()).unwrap(),
                dependent_block,
            ),
        ],
        ..Legacy::stored(&data)
    };
    for legacy in [independent, dependent] {
        let file = assemble(
            &RawHeader::untracked(PS, 1, 1),
            &[legacy.frame(1, &data)],
            0,
        );
        assert_eq!(decode(&file).unwrap().pages, [(1, data.clone())]);
    }
}

#[test]
fn frame_content_size() {
    let legacy = Legacy {
        flg: 0x6c,
        content_size: Some(511),
        ..Legacy::stored(&page(PS, 1))
    };
    assert_eq!(
        frame_err(&legacy.frame(1, &page(PS, 1))),
        FrameError::ContentSize { found: 511 }
    );
}

#[test]
fn frame_too_long() {
    let data = page(PS, 1);
    let legacy = Legacy {
        blocks: data.iter().map(|byte| (STORED | 1, vec![*byte])).collect(),
        ..Legacy::stored(&data)
    };
    assert!(matches!(
        frame_err(&legacy.frame(1, &data)),
        FrameError::FrameTooLong { .. }
    ));
}

#[test]
fn frame_lz4() {
    assert!(matches!(
        frame_err(&block_frame_raw(1, &[0xf0], &page(PS, 1))),
        FrameError::Lz4 { .. }
    ));
}

#[test]
fn frame_page_len() {
    let data = page(PS, 1);
    let half = lz4_flex::block::compress(&data[..256]);
    assert_eq!(
        frame_err(&block_frame_raw(1, &half, &data)),
        FrameError::PageLen {
            found: 256,
            expected: 512
        }
    );
    let legacy = Legacy::stored(&data[..256]);
    assert_eq!(
        frame_err(&legacy.frame(1, &data)),
        FrameError::PageLen {
            found: 256,
            expected: 512
        }
    );
}

#[test]
fn order_error() {
    let header = RawHeader::untracked(PS, 2, 5);
    let bytes = assemble(&header, &[block_frame(3, &page(PS, 1))], 0);
    assert!(matches!(
        err(&bytes),
        DecodeError::Order(OrderError::AboveCommit { pgno: 3, commit: 2 })
    ));
    let short = assemble(&RawHeader::untracked(PS, 3, 1), &snapshot_frames(), 0);
    assert!(matches!(
        err(&short),
        DecodeError::Order(OrderError::SnapshotShort { last: 2, commit: 3 })
    ));
}

#[test]
fn lock_page_is_rejected_at_each_page_size() {
    for shift in 9..=16 {
        let size = PageSize::new(1 << shift).unwrap();
        let lock = lock_page(size);
        let raw = RawHeader::untracked(size.get(), lock + 1, 5);
        let frame = block_frame(lock, &page(size.get(), 1));
        let want = OrderError::LockPage { pgno: lock };
        let bytes = assemble(&raw, std::slice::from_ref(&frame), 0);
        assert!(
            matches!(err(&bytes), DecodeError::Order(got) if got == want),
            "{size:?}"
        );
        let header = parsed(&raw);
        assert!(
            matches!(decode_frame(&frame.bytes, &header, &entry(1), &mut Vec::new()), Err(DecodeError::EntryMismatch { found, .. }) if found == lock)
        );
        let mut region = index_for(&[frame]);
        region.extend_from_slice(&u64::try_from(region.len()).unwrap().to_be_bytes());
        assert_eq!(
            PageIndex::decode(&region, &header),
            Err(IndexError::Order(want))
        );
        // A page number valid at another page size is still the lock page here.
        let other = PageSize::new(if shift == 9 { 1024 } else { 512 }).unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        let result = encoder.write_page(PageNo::new(lock, other).unwrap(), &page(size.get(), 1));
        assert!(matches!(result, Err(EncodeError::Order(got)) if got == want));
    }
}

#[test]
fn index_varint() {
    let index = [[0xff_u8; 9].as_slice(), &[0x02]].concat();
    let bytes = assemble_with_index(
        &RawHeader::untracked(PS, 2, 1),
        &snapshot_frames(),
        &index,
        0,
    );
    assert!(matches!(
        err(&bytes),
        DecodeError::Index(IndexError::Varint)
    ));
}

#[test]
fn index_page_number() {
    let mut index = Vec::new();
    uvarint(&mut index, 1 << 33);
    let bytes = assemble_with_index(
        &RawHeader::untracked(PS, 2, 1),
        &snapshot_frames(),
        &index,
        0,
    );
    assert!(matches!(
        err(&bytes),
        DecodeError::Index(IndexError::PageNumber {
            value: 0x2_0000_0000
        })
    ));
}

#[test]
fn index_size_field() {
    let (_, mut bytes) = snapshot();
    let at = bytes.len() - 24;
    bytes[at + 7] += 1;
    assert!(matches!(
        err(&bytes),
        DecodeError::Index(IndexError::SizeField { .. })
    ));
}

#[test]
fn index_mismatch() {
    let frames = snapshot_frames();
    let entries = |list: &[(u64, u64, u64)]| {
        let mut index = Vec::new();
        for (a, b, c) in list {
            uvarint(&mut index, *a);
            uvarint(&mut index, *b);
            uvarint(&mut index, *c);
        }
        uvarint(&mut index, 0);
        index
    };
    let (first, second) = (
        u64::try_from(frames[0].bytes.len()).unwrap(),
        u64::try_from(frames[1].bytes.len()).unwrap(),
    );
    for list in [
        vec![(1, 100, first), (2, 101 + first, second)],
        vec![(1, 100, first)],
        vec![
            (1, 100, first),
            (2, 100 + first, second),
            (3, 100 + first + second, 11),
        ],
        vec![(2, 100, first), (1, 100 + first, second)],
    ] {
        let bytes =
            assemble_with_index(&RawHeader::untracked(PS, 2, 1), &frames, &entries(&list), 0);
        assert!(
            matches!(err(&bytes), DecodeError::IndexMismatch),
            "{list:?}"
        );
    }
}

#[test]
fn trailer_post_apply() {
    let bytes = assemble(
        &RawHeader::untracked(PS, 2, 1),
        &snapshot_frames(),
        (1 << 63) | 1,
    );
    assert!(matches!(
        err(&bytes),
        DecodeError::Trailer(TrailerError::PostApply { .. })
    ));
}

#[test]
fn trailer_rules() {
    let tracked_empty = parsed(&RawHeader {
        flags: 0,
        pre_apply: (1 << 63) | 4,
        ..RawHeader::untracked(PS, 0, 5)
    });
    let trailer = |post: u64| {
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&post.to_be_bytes());
        bytes[8..].copy_from_slice(&((1_u64 << 63) | 9).to_be_bytes());
        bytes
    };
    Trailer::decode(&trailer(1 << 63), &tracked_empty).unwrap();
    assert_eq!(
        Trailer::decode(&trailer((1 << 63) | 3), &tracked_empty),
        Err(TrailerError::PostApply { raw: (1 << 63) | 3 })
    );
    assert_eq!(
        Trailer::decode(&trailer(0), &tracked_empty),
        Err(TrailerError::PostApply { raw: 0 })
    );
    assert_eq!(
        Trailer::decode(&trailer(3), &tracked_empty),
        Err(TrailerError::PostApply { raw: 3 })
    );
}

#[test]
fn trailer_file_checksum() {
    let (_, mut bytes) = snapshot();
    let at = bytes.len() - 8;
    bytes[at..].fill(0);
    assert!(matches!(
        err(&bytes),
        DecodeError::Trailer(TrailerError::FileChecksum { raw: 0 })
    ));
}

#[test]
fn file_checksum() {
    let (_, mut bytes) = snapshot();
    bytes[79] ^= 1; // node id: valid either way, covered only by the checksum
    assert!(matches!(err(&bytes), DecodeError::FileChecksum { .. }));
}

#[test]
fn post_apply_mismatch() {
    let header = RawHeader {
        flags: 0,
        ..RawHeader::untracked(PS, 2, 1)
    };
    let ps = PageSize::new(PS).unwrap();
    let right = Checksum::EMPTY
        .toggle(Checksum::of_page(PageNo::new(1, ps).unwrap(), &page(PS, 1)))
        .toggle(Checksum::of_page(PageNo::new(2, ps).unwrap(), &page(PS, 2)));
    decode(&assemble(&header, &snapshot_frames(), right.get())).unwrap();
    let wrong = assemble(&header, &snapshot_frames(), right.get() ^ 1);
    assert!(
        matches!(err(&wrong), DecodeError::PostApplyMismatch { computed, .. } if computed == right)
    );
}

#[test]
fn truncated_in_every_section() {
    let (_, bytes) = snapshot();
    let index_start = bytes.len() - 16 - 8 - 7;
    for (cut, section) in [
        (0, Section::Header),
        (99, Section::Header),
        (100, Section::Page),
        (105, Section::Page),
        (index_start - 3, Section::Page),
        (index_start + 1, Section::Index),
        (bytes.len() - 17, Section::Index),
        (bytes.len() - 1, Section::Trailer),
    ] {
        assert!(
            matches!(err(&bytes[..cut]), DecodeError::Truncated { section: got } if got == section),
            "{cut}"
        );
    }
    let header = parsed(&RawHeader::untracked(PS, 2, 1));
    let frame = &snapshot_frames()[0].bytes;
    assert!(matches!(
        decode_frame(
            &frame[..frame.len() - 1],
            &header,
            &entry(1),
            &mut Vec::new()
        ),
        Err(DecodeError::Truncated {
            section: Section::Page
        })
    ));
}

#[test]
fn trailing_bytes() {
    let (_, mut bytes) = snapshot();
    bytes.push(0);
    assert!(matches!(err(&bytes), DecodeError::TrailingBytes));
    let header = parsed(&RawHeader::untracked(PS, 2, 1));
    let mut frame = snapshot_frames()[0].bytes.clone();
    frame.push(0);
    assert!(matches!(
        decode_frame(&frame, &header, &entry(1), &mut Vec::new()),
        Err(DecodeError::TrailingBytes)
    ));
}

#[test]
fn poisoned_after_any_error() {
    let mut decoder = Decoder::new();
    decoder.feed(&[0; 100]).unwrap_err();
    assert!(matches!(
        decoder.feed(&[0; 100]),
        Err(DecodeError::Poisoned)
    ));
    assert!(matches!(decoder.finish(), Err(DecodeError::Poisoned)));
}

struct Flaky<'a> {
    bytes: &'a [u8],
    interrupted: bool,
    fail_at: Option<usize>,
    pos: usize,
}

impl Read for Flaky<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.fail_at.is_some_and(|at| self.pos >= at) {
            return Err(io::Error::other("disk on fire"));
        }
        let n = buf.len().min(self.bytes.len() - self.pos).min(7);
        buf[..n].copy_from_slice(&self.bytes[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

#[test]
fn io_error_from_the_reader() {
    let (_, bytes) = snapshot();
    let drain = |fail_at| {
        let mut reader = ReadDecoder::new(Flaky {
            bytes: &bytes,
            interrupted: false,
            fail_at,
            pos: 0,
        });
        loop {
            match reader.next_event() {
                Ok(Some(_)) => {}
                Ok(None) => return reader.finish().map(|_| ()),
                Err(err) => return Err(err),
            }
        }
    };
    assert!(drain(None).is_ok(), "an interrupted read is retried");
    assert!(matches!(drain(Some(110)), Err(DecodeError::Io(_))));
}

// ---- Encoder ----

fn header(tracked: bool, commit: u32, min: u64) -> Header {
    Header::new(HeaderFields {
        tracked,
        page_size: PageSize::new(PS).unwrap(),
        commit,
        txids: TxidRange::single(Txid::new(min).unwrap()),
        timestamp: Timestamp::from_unix_millis(0),
        pre_apply: (tracked && min > 1).then_some(Checksum::EMPTY),
        wal: None,
        node_id: 0,
    })
    .unwrap()
}

fn pgno(n: u32) -> PageNo {
    PageNo::new(n, PageSize::new(PS).unwrap()).unwrap()
}

#[test]
fn encode_page_len() {
    let mut encoder = Encoder::new(Vec::new(), header(false, 2, 1)).unwrap();
    assert!(matches!(
        encoder.write_page(pgno(1), &[0; 511]),
        Err(EncodeError::PageLen {
            found: 511,
            expected: 512
        })
    ));
    encoder.write_page(pgno(1), &[0; 512]).unwrap();
}

#[test]
fn encode_order() {
    let mut encoder = Encoder::new(Vec::new(), header(false, 2, 1)).unwrap();
    assert!(matches!(
        encoder.write_page(pgno(2), &[0; 512]),
        Err(EncodeError::Order(OrderError::SnapshotGap { .. }))
    ));
    assert!(matches!(
        encoder.write_page(pgno(3), &[0; 512]),
        Err(EncodeError::Order(OrderError::AboveCommit { .. }))
    ));
    encoder.write_page(pgno(1), &[0; 512]).unwrap();
    assert!(matches!(
        encoder.finish(None),
        Err(EncodeError::Order(OrderError::SnapshotShort {
            last: 1,
            commit: 2
        }))
    ));
}

#[test]
fn encode_trailer() {
    let some = Some(Checksum::EMPTY);
    let finish = |header: Header, post| Encoder::new(Vec::new(), header).unwrap().finish(post);
    assert!(matches!(
        finish(header(false, 1, 5), some),
        Err(EncodeError::Trailer(TrailerError::PostApply { .. }))
    ));
    assert!(matches!(
        finish(header(true, 1, 5), None),
        Err(EncodeError::Trailer(TrailerError::PostApply { raw: 0 }))
    ));
    finish(header(true, 1, 5), some).unwrap();
}

/// SQLite never commits a zero-page database in WAL mode; Go writes one only
/// tracked. The encoder is the strict side, the decoder still reads one.
#[test]
fn encode_refuses_commit_zero() {
    for tracked in [false, true] {
        let got = Encoder::new(Vec::new(), header(tracked, 0, 5));
        assert!(matches!(got, Err(EncodeError::ZeroCommit)), "{tracked}");
    }
}

/// A frame for another page than its entry, even one the header allows, is
/// refused and leaves nothing of it in the caller's buffer.
#[test]
fn decode_frame_checks_the_entry_and_clears_out_on_error() {
    let header = parsed(&RawHeader::untracked(PS, 2, 1));
    let frames = snapshot_frames();
    let mut out = Vec::new();
    decode_frame(&frames[0].bytes, &header, &entry(1), &mut out).unwrap();
    assert_eq!(out, page(PS, 1));
    let got = decode_frame(&frames[1].bytes, &header, &entry(1), &mut out);
    assert!(
        matches!(
            got,
            Err(DecodeError::EntryMismatch {
                expected: 1,
                found: 2
            })
        ),
        "{got:?}"
    );
    assert!(out.iter().all(|byte| *byte == 0), "page 1 left in out");
    let mut long = frames[0].bytes.clone();
    long.push(0);
    out.extend_from_slice(&page(PS, 9));
    decode_frame(&long, &header, &entry(1), &mut out).unwrap_err();
    assert!(out.iter().all(|byte| *byte == 0));
}

#[test]
fn encode_post_apply_mismatch() {
    let mut encoder = Encoder::new(Vec::new(), header(true, 1, 1)).unwrap();
    encoder.write_page(pgno(1), &[7; 512]).unwrap();
    let wrong = Checksum::EMPTY;
    assert!(
        matches!(encoder.finish(Some(wrong)), Err(EncodeError::PostApplyMismatch { given, .. }) if given == wrong)
    );
}

struct Full(usize);

impl Write for Full {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.0 == 0 {
            return Err(io::Error::other("disk full"));
        }
        let n = buf.len().min(self.0);
        self.0 -= n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn encode_io_then_poisoned() {
    assert!(matches!(
        Encoder::new(Full(10), header(false, 2, 1)),
        Err(EncodeError::Io(_))
    ));
    let mut encoder = Encoder::new(Full(110), header(false, 2, 1)).unwrap();
    assert!(matches!(
        encoder.write_page(pgno(1), &[0; 512]),
        Err(EncodeError::Io(_))
    ));
    assert!(matches!(
        encoder.write_page(pgno(1), &[0; 512]),
        Err(EncodeError::Poisoned)
    ));
    assert!(matches!(encoder.finish(None), Err(EncodeError::Poisoned)));
}

// ---- PageIndex ----

fn region(entries: &[(u64, u64, u64)], extra: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for (a, b, c) in entries {
        uvarint(&mut out, *a);
        uvarint(&mut out, *b);
        uvarint(&mut out, *c);
    }
    uvarint(&mut out, 0);
    out.extend_from_slice(extra);
    out.extend_from_slice(&u64::try_from(out.len()).unwrap().to_be_bytes());
    out
}

#[test]
fn page_index_errors() {
    let snap = parsed(&RawHeader::untracked(PS, 2, 1));
    let inc = parsed(&RawHeader::untracked(PS, 9, 4));
    PageIndex::decode(&region(&[(1, 100, 20), (2, 120, 20)], &[]), &snap).unwrap();
    let cases: [(&Header, Vec<u8>, IndexError); 10] = [
        (
            &snap,
            region(&[(1, 100, 20), (3, 120, 20)], &[]),
            IndexError::Order(OrderError::AboveCommit { pgno: 3, commit: 2 }),
        ),
        (
            &snap,
            region(&[(1, 100, 20)], &[]),
            IndexError::Order(OrderError::SnapshotShort { last: 1, commit: 2 }),
        ),
        (
            &inc,
            region(&[(4, 99, 20)], &[]),
            IndexError::Offset {
                pgno: 4,
                offset: 99,
            },
        ),
        (
            &inc,
            region(&[(4, 100, 20), (5, 119, 20)], &[]),
            IndexError::Offset {
                pgno: 5,
                offset: 119,
            },
        ),
        (
            &inc,
            region(&[(4, 100, 10)], &[]),
            IndexError::FrameSize { pgno: 4, size: 10 },
        ),
        (
            &inc,
            region(&[(4, 100, 1 << 20)], &[]),
            IndexError::FrameSize {
                pgno: 4,
                size: 1 << 20,
            },
        ),
        (
            &inc,
            region(&[(4, 100, 20)], &[0]),
            IndexError::TrailingBytes,
        ),
        (&inc, vec![0; 7], IndexError::Truncated),
        (
            &inc,
            [&[4_u8, 100][..], &2_u64.to_be_bytes()].concat(),
            IndexError::Truncated,
        ),
        (
            &inc,
            [&[0_u8][..], &2_u64.to_be_bytes()].concat(),
            IndexError::SizeField {
                found: 2,
                expected: 1,
            },
        ),
    ];
    for (header, bytes, want) in cases {
        assert_eq!(PageIndex::decode(&bytes, header), Err(want));
    }
}

#[test]
fn page_index_bad_varints() {
    let inc = parsed(&RawHeader::untracked(PS, 9, 4));
    let varint = [[0x80_u8; 10].as_slice(), &[0], &11_u64.to_be_bytes()].concat();
    assert_eq!(PageIndex::decode(&varint, &inc), Err(IndexError::Varint));
    let mut big = Vec::new();
    uvarint(&mut big, 1 << 32);
    big.extend_from_slice(&5_u64.to_be_bytes());
    assert_eq!(
        PageIndex::decode(&big, &inc),
        Err(IndexError::PageNumber { value: 1 << 32 })
    );
}

/// Go's `pageIndexValidator` rejects `offset > MaxInt64 - size`; an entry whose
/// end passes `i64::MAX` (or wraps `u64`) names no byte of any real file.
#[test]
fn page_index_rejects_offset_past_i64_max() {
    let snap = parsed(&RawHeader::untracked(PS, 1, 1));
    let past = u64::try_from(i64::MAX).unwrap() - 10;
    for offset in [past, u64::MAX - 5] {
        let got = PageIndex::decode(&region(&[(1, offset, 20)], &[]), &snap);
        assert!(
            matches!(got, Err(IndexError::Offset { .. })),
            "offset {offset} + 20 accepted: {got:?}"
        );
    }
}
