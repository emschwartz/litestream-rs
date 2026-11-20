//! Hand-built WALs: every error variant and every tail, each from the smallest input that causes it.

use std::cell::Cell;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::rc::Rc;

use lsr_ltx::{Checksum, lock_page};
use lsr_wal::{
    ByteOrder, CommittedEnd, FrameFault, HEADER_LEN, HeaderError, OpenError, ReadPageError,
    ResumeError, Salts, ScanLimit, Tail, WalChecksum, WalError, WalHeader, WalOffset, WalReader,
};

use crate::common::{Failing, Wal, Watching, header_bytes, open, page, scan, scan_from, size, sum};

const PS: u32 = 512;
const PSU: usize = 512;
const SALTS: (u32, u32) = (0xaaaa_0001, 0xbbbb_0002);

fn salts(pair: (u32, u32)) -> Salts {
    Salts {
        salt1: pair.0,
        salt2: pair.1,
    }
}

/// Two committed transactions: pages 1 and 2 (size 2), then pages 2 and 3 (size 3).
fn two_txs() -> Wal {
    let mut wal = Wal::new(PS, false, SALTS);
    wal.tx(&[1, 2], 2, 10);
    wal.tx(&[2, 3], 3, 20);
    wal
}

fn header_err(bytes: &[u8]) -> HeaderError {
    WalHeader::decode(bytes.try_into().unwrap()).unwrap_err()
}

#[test]
fn header_decodes_every_field() {
    let wal = Wal::new(4096, false, SALTS);
    let header = WalHeader::decode(wal.bytes.as_slice().try_into().unwrap()).unwrap();
    assert_eq!(header.byte_order(), ByteOrder::Little);
    assert_eq!(header.page_size(), size(4096));
    assert_eq!(header.checkpoint_seq(), 0);
    assert_eq!(header.salts(), salts(SALTS));
    assert_eq!(
        Some(header.checksum()),
        WalChecksum::ZERO.extend(ByteOrder::Little, &wal.bytes[..24])
    );
    let big = Wal::new(512, true, SALTS);
    let header = WalHeader::decode(big.bytes.as_slice().try_into().unwrap()).unwrap();
    assert_eq!(header.byte_order(), ByteOrder::Big);
}

#[test]
fn header_bad_magic() {
    let bytes = header_bytes(0x377f_0684, 3_007_000, PS, SALTS, false);
    assert_eq!(
        header_err(&bytes),
        HeaderError::BadMagic { found: 0x377f_0684 }
    );
}

#[test]
fn header_checksum_mismatch() {
    let mut bytes = Wal::new(PS, false, SALTS).bytes;
    bytes[HEADER_LEN - 1] ^= 1;
    assert_eq!(header_err(&bytes), HeaderError::ChecksumMismatch);
}

#[test]
fn header_unsupported_version() {
    let bytes = header_bytes(0x377f_0682, 3_007_001, PS, SALTS, false);
    assert_eq!(
        header_err(&bytes),
        HeaderError::UnsupportedVersion { found: 3_007_001 }
    );
}

#[test]
fn header_bad_page_size() {
    for found in [0, 1, 511, 513, 131_072] {
        let bytes = header_bytes(0x377f_0682, 3_007_000, found, SALTS, false);
        assert_eq!(header_err(&bytes), HeaderError::BadPageSize { found });
    }
}

#[test]
fn open_truncated() {
    for len in [0, 1, 31] {
        let err = WalReader::open(Cursor::new(
            Wal::new(PS, false, SALTS).bytes[..len].to_vec(),
        ))
        .unwrap_err();
        assert!(
            matches!(err, OpenError::Truncated { found } if found == len),
            "{err}"
        );
    }
}

#[test]
fn open_header() {
    let mut bytes = Wal::new(PS, false, SALTS).bytes;
    bytes[0] = 0;
    let err = WalReader::open(Cursor::new(bytes)).unwrap_err();
    assert!(
        matches!(err, OpenError::Header(HeaderError::BadMagic { .. })),
        "{err}"
    );
}

#[test]
fn open_io() {
    let failing = Failing {
        inner: Cursor::new(Wal::new(PS, false, SALTS).bytes),
        ok: 0,
    };
    assert!(matches!(
        WalReader::open(failing).unwrap_err(),
        OpenError::Io(_)
    ));
}

#[test]
fn scan_yields_each_transaction_then_a_clean_end() {
    let wal = two_txs();
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    assert_eq!(scanned.tail, Tail::Clean);
    let sizes: Vec<_> = scanned
        .txs
        .iter()
        .map(|tx| (tx.frames().len(), tx.commit()))
        .collect();
    assert_eq!(sizes, [(2, 2), (2, 3)]);
    let first = &scanned.txs[0].frames()[0];
    assert_eq!(first.pgno().get(), 1);
    assert_eq!(first.offset(), WalOffset::start(size(PS)));
    assert_eq!(
        first.digest(),
        Checksum::of_page(first.pgno(), &page(PS, 11))
    );
    assert_eq!(scanned.at, *scanned.txs[1].end());
    assert_eq!(
        scanned.at.next().get(),
        u64::try_from(wal.bytes.len()).unwrap()
    );
}

#[test]
fn big_endian_wal_scans_like_little_endian() {
    let mut big = Wal::new(PS, true, SALTS);
    big.tx(&[1, 2], 2, 10);
    big.tx(&[2, 3], 3, 20);
    let (le, be) = (two_txs(), big);
    let digests = |wal: &Wal| -> Vec<_> {
        scan(&wal.bytes, ScanLimit::FirstInvalidFrame)
            .txs
            .iter()
            .flat_map(|tx| tx.frames().iter().map(|f| (f.pgno(), f.digest())))
            .collect()
    };
    assert_eq!(digests(&le), digests(&be));
    assert_eq!(open(&be.bytes).header().byte_order(), ByteOrder::Big);
}

#[test]
fn tail_clean_on_an_empty_wal() {
    let wal = Wal::new(PS, false, SALTS);
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    assert!(scanned.txs.is_empty());
    assert_eq!(scanned.tail, Tail::Clean);
}

#[test]
fn tail_partial_frame() {
    let wal = two_txs();
    for cut in [1, 10, 24, 25, wal.frame_len() - 1] {
        let bytes = &wal.bytes[..wal.bytes.len() - wal.frame_len() + cut];
        let scanned = scan(bytes, ScanLimit::FirstInvalidFrame);
        assert_eq!(scanned.tail, Tail::PartialFrame, "cut {cut}");
        assert_eq!(
            scanned.txs.len(),
            1,
            "the half-written transaction is not returned"
        );
    }
}

#[test]
fn tail_uncommitted() {
    let mut wal = two_txs();
    wal.frame(4, 0, 1);
    wal.frame(5, 0, 2);
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    assert_eq!(scanned.txs.len(), 2);
    assert_eq!(scanned.tail, Tail::Uncommitted { frames: 2 });
    assert_eq!(
        scanned.at,
        *scanned.txs[1].end(),
        "the open transaction is not consumed"
    );
}

#[test]
fn tail_invalid_salt_mismatch() {
    let mut wal = two_txs();
    wal.frame_with(1, 1, (1, 2), &page(PS, 1));
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    assert_eq!(scanned.txs.len(), 2);
    assert_eq!(
        scanned.tail,
        Tail::InvalidFrame(FrameFault::SaltMismatch {
            found: salts((1, 2))
        })
    );
}

#[test]
fn tail_invalid_checksum_mismatch() {
    let mut wal = two_txs();
    wal.frame(1, 3, 5);
    let at = wal.bytes.len() - 1;
    wal.bytes[at] ^= 0x80;
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    assert_eq!(
        scanned.tail,
        Tail::InvalidFrame(FrameFault::ChecksumMismatch)
    );
}

#[test]
fn tail_invalid_bad_page_number() {
    let lock = lock_page(size(PS));
    for raw in [0, lock] {
        let mut wal = two_txs();
        wal.frame(raw, 3, 5);
        let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
        assert_eq!(scanned.txs.len(), 2);
        assert_eq!(
            scanned.tail,
            Tail::InvalidFrame(FrameFault::BadPageNumber { raw })
        );
    }
}

#[test]
fn committed_limit_stops_at_the_end_and_never_reads_past_it() {
    let mut wal = two_txs();
    wal.frame(4, 4, 9); // a third commit the limit excludes
    let end = CommittedEnd::new(4, salts(SALTS), size(PS));
    let highest = Rc::new(Cell::new(0));
    let watching = Watching {
        inner: Cursor::new(wal.bytes.clone()),
        highest: Rc::clone(&highest),
    };
    let mut reader = WalReader::open(watching).unwrap();
    let scanned = scan_from(&mut reader, None, ScanLimit::Committed(end)).unwrap();
    assert_eq!(scanned.txs.len(), 2);
    assert_eq!(scanned.tail, Tail::Clean);
    assert_eq!(scanned.at.next(), end.offset());
    assert_eq!(
        highest.get(),
        end.offset().get(),
        "no byte at or past the end was read"
    );
}

#[test]
fn committed_limit_corrupt_below() {
    let mut wal = two_txs();
    let at = 32 + wal.frame_len() * 2 + 40;
    wal.bytes[at] ^= 1;
    let end = CommittedEnd::new(4, salts(SALTS), size(PS));
    let err = scan_from(&mut open(&wal.bytes), None, ScanLimit::Committed(end)).unwrap_err();
    assert!(
        matches!(
            err,
            WalError::CorruptBelowCommitted { offset, fault: FrameFault::ChecksumMismatch }
                if offset.frame_index() == 2
        ),
        "{err}"
    );
}

#[test]
fn committed_limit_truncated_below() {
    let wal = two_txs();
    let end = CommittedEnd::new(4, salts(SALTS), size(PS));
    for len in [wal.bytes.len() - wal.frame_len(), wal.bytes.len() - 1] {
        let err = scan_from(
            &mut open(&wal.bytes[..len]),
            None,
            ScanLimit::Committed(end),
        )
        .unwrap_err();
        assert!(
            matches!(err, WalError::TruncatedBelowCommitted { .. }),
            "{err}"
        );
    }
}

#[test]
fn committed_limit_end_not_at_commit() {
    let wal = two_txs();
    for frames in [1, 3] {
        let end = CommittedEnd::new(frames, salts(SALTS), size(PS));
        let err = scan_from(&mut open(&wal.bytes), None, ScanLimit::Committed(end));
        let scanned = match (frames, err) {
            (1, Err(WalError::EndNotAtCommit { end: at })) => {
                assert_eq!(at, end.offset());
                continue;
            }
            (3, Err(WalError::EndNotAtCommit { .. })) => continue,
            (_, other) => other,
        };
        panic!("expected EndNotAtCommit, got {:?}", scanned.map(|s| s.tail));
    }
}

#[test]
fn committed_limit_zero_is_an_empty_wal() {
    let wal = two_txs();
    let end = CommittedEnd::new(0, salts(SALTS), size(PS));
    let scanned = scan_from(&mut open(&wal.bytes), None, ScanLimit::Committed(end)).unwrap();
    assert!(scanned.txs.is_empty());
    assert_eq!(scanned.tail, Tail::Clean);
}

#[test]
fn cursor_past_end() {
    let wal = two_txs();
    let mut reader = open(&wal.bytes);
    let cursor = *scan_from(&mut reader, None, ScanLimit::FirstInvalidFrame)
        .unwrap()
        .txs[1]
        .end();
    let end = CommittedEnd::new(2, salts(SALTS), size(PS));
    let err = reader
        .next_tx(&cursor, ScanLimit::Committed(end))
        .unwrap_err();
    assert!(matches!(err, WalError::CursorPastEnd { .. }), "{err}");
}

#[test]
fn restarted_generation_is_refused_up_front() {
    let old = two_txs();
    let cursor = open(&old.bytes).cursor_at_start();
    let mut restarted = Wal::new(PS, false, (SALTS.0 + 1, 77));
    restarted.tx(&[1], 1, 1);
    let err = open(&restarted.bytes)
        .next_tx(&cursor, ScanLimit::FirstInvalidFrame)
        .unwrap_err();
    assert!(
        matches!(err, WalError::Restarted { cursor, header }
            if cursor == salts(SALTS) && header == salts((SALTS.0 + 1, 77))),
        "{err}"
    );
}

#[test]
fn page_size_mismatch_in_cursor_and_end() {
    let small = two_txs();
    let mut big = Wal::new(1024, false, SALTS);
    big.tx(&[1], 1, 1);
    let mut reader = open(&big.bytes);
    let foreign = open(&small.bytes).cursor_at_start();
    let err = reader
        .next_tx(&foreign, ScanLimit::FirstInvalidFrame)
        .unwrap_err();
    assert!(
        matches!(err, WalError::PageSizeMismatch { expected, found }
            if expected == size(1024) && found == size(PS)),
        "{err}"
    );
    let own = reader.cursor_at_start();
    let end = CommittedEnd::new(1, salts(SALTS), size(PS));
    let err = reader.next_tx(&own, ScanLimit::Committed(end)).unwrap_err();
    assert!(matches!(err, WalError::PageSizeMismatch { .. }), "{err}");
}

#[test]
fn scan_io() {
    let failing = Failing {
        inner: Cursor::new(two_txs().bytes),
        ok: 1,
    };
    let mut reader = WalReader::open(failing).unwrap();
    let cursor = reader.cursor_at_start();
    let err = reader
        .next_tx(&cursor, ScanLimit::FirstInvalidFrame)
        .unwrap_err();
    assert!(matches!(err, WalError::Io(_)), "{err}");
}

/// Serves three frames at the very top of the 32-bit frame range and nothing else.
struct Top {
    header: Vec<u8>,
    frames: Vec<u8>,
    flen: u64,
    pos: u64,
}

impl Read for Top {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let source: &[u8] = if self.pos < 32 {
            &self.header[usize::try_from(self.pos).map_err(io::Error::other)?..]
        } else {
            let rel = self.pos - 32;
            let first = u64::from(u32::MAX) - 2;
            match rel.checked_sub(first * self.flen) {
                Some(into) if into < 3 * self.flen => {
                    &self.frames[usize::try_from(into).map_err(io::Error::other)?..]
                }
                _ => return Ok(0),
            }
        };
        let n = source.len().min(buf.len());
        buf[..n].copy_from_slice(&source[..n]);
        self.pos += u64::try_from(n).map_err(io::Error::other)?;
        Ok(n)
    }
}

impl Seek for Top {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let SeekFrom::Start(at) = pos else {
            return Err(io::Error::other("only absolute seeks"));
        };
        self.pos = at;
        Ok(at)
    }
}

/// A frame header-and-page continuing from `running`, and the checksum after it.
fn chained_frame(pgno: u32, commit: u32, running: (u32, u32)) -> (Vec<u8>, (u32, u32)) {
    let data = page(PS, pgno);
    let mut head = Vec::new();
    for field in [pgno, commit, SALTS.0, SALTS.1] {
        head.extend(field.to_be_bytes());
    }
    let after = sum(false, sum(false, running, &head[..8]), &data);
    head.extend(after.0.to_be_bytes());
    head.extend(after.1.to_be_bytes());
    head.extend(data);
    (head, after)
}

#[test]
fn too_many_frames() {
    // Frame MAX-2 is only a stored checksum for MAX-1 to chain from; MAX-1 is a
    // commit frame, so resume(MAX) verifies; the scan then reads frame MAX.
    let start: (u32, u32) = (11, 12);
    let (mut frames, _) = chained_frame(1, 1, (0, 0));
    frames[16..20].copy_from_slice(&start.0.to_be_bytes());
    frames[20..24].copy_from_slice(&start.1.to_be_bytes());
    let (second, running) = chained_frame(1, 1, start);
    let (third, _) = chained_frame(2, 0, running);
    frames.extend(second);
    frames.extend(third);
    let wal = Wal::new(PS, false, SALTS);
    let top = Top {
        header: wal.bytes[..32].to_vec(),
        frames,
        flen: u64::try_from(wal.frame_len()).unwrap(),
        pos: 0,
    };
    let mut reader = WalReader::open(top).unwrap();
    let cursor = reader
        .resume(
            WalOffset::from_frame_index(u32::MAX, size(PS)),
            salts(SALTS),
        )
        .unwrap();
    let err = reader
        .next_tx(&cursor, ScanLimit::FirstInvalidFrame)
        .unwrap_err();
    assert!(matches!(err, WalError::TooManyFrames), "{err}");
}

#[test]
fn resume_continues_where_a_scan_stopped() {
    let mut wal = two_txs();
    wal.tx(&[1, 4], 4, 30);
    let full = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut reader = open(&wal.bytes);
    let after_first = *full.txs[0].end();
    let cursor = reader.resume(after_first.next(), salts(SALTS)).unwrap();
    assert_eq!(
        cursor, after_first,
        "stored checksum equals the verified one"
    );
    let rest = scan_from(&mut reader, Some(cursor), ScanLimit::FirstInvalidFrame).unwrap();
    assert_eq!(rest.txs, full.txs[1..]);
}

#[test]
fn resume_at_the_start_is_the_start() {
    let mut reader = open(&two_txs().bytes);
    assert_eq!(
        reader
            .resume(WalOffset::start(size(PS)), salts(SALTS))
            .unwrap(),
        reader.cursor_at_start()
    );
}

#[test]
fn resume_prev_frame_of_another_generation() {
    let mut wal = two_txs();
    wal.frame_with(1, 1, (9, 9), &page(PS, 1));
    let mut reader = open(&wal.bytes);
    let err = reader
        .resume(WalOffset::from_frame_index(5, size(PS)), salts(SALTS))
        .unwrap_err();
    assert!(
        matches!(err, ResumeError::PrevFrame { fault: FrameFault::SaltMismatch { found }, .. }
            if found == salts((9, 9))),
        "{err}"
    );
}

#[test]
fn resume_truncated() {
    let mut reader = open(&two_txs().bytes);
    let err = reader
        .resume(WalOffset::from_frame_index(9, size(PS)), salts(SALTS))
        .unwrap_err();
    assert!(matches!(err, ResumeError::Truncated { .. }), "{err}");
}

#[test]
fn frame_before_is_the_commit_frame_the_scan_saw() {
    let wal = two_txs();
    let full = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut reader = open(&wal.bytes);
    for tx in &full.txs {
        let frame = reader.frame_before(tx.end().next(), salts(SALTS)).unwrap();
        let last = tx.frames().last().unwrap();
        assert_eq!(frame, *last);
        assert_eq!(frame.offset(), last.offset());
    }
}

#[test]
fn frame_before_refuses_what_resume_refuses() {
    let mut wal = two_txs();
    wal.frame_with(1, 1, (9, 9), &page(PS, 1));
    let mut reader = open(&wal.bytes);
    let at = |index| WalOffset::from_frame_index(index, size(PS));
    let err = reader.frame_before(at(5), salts(SALTS)).unwrap_err();
    assert!(matches!(err, ResumeError::PrevFrame { .. }), "{err}");
    let err = reader.frame_before(at(9), salts(SALTS)).unwrap_err();
    assert!(matches!(err, ResumeError::Truncated { .. }), "{err}");
    let err = reader.frame_before(at(1), salts((1, 2))).unwrap_err();
    assert!(matches!(err, ResumeError::Restarted { .. }), "{err}");
    let err = reader
        .frame_before(WalOffset::from_frame_index(1, size(1024)), salts(SALTS))
        .unwrap_err();
    assert!(matches!(err, ResumeError::PageSizeMismatch { .. }), "{err}");
}

#[test]
fn frame_before_the_start_or_inside_a_transaction_is_not_a_commit() {
    let mut reader = open(&two_txs().bytes);
    for at in [
        WalOffset::start(size(PS)),
        WalOffset::from_frame_index(1, size(PS)),
    ] {
        let err = reader.frame_before(at, salts(SALTS)).unwrap_err();
        assert!(matches!(err, ResumeError::NotAtCommit { .. }), "{err}");
    }
}

#[test]
fn resume_page_size_mismatch() {
    let mut reader = open(&two_txs().bytes);
    let err = reader
        .resume(WalOffset::from_frame_index(1, size(1024)), salts(SALTS))
        .unwrap_err();
    assert!(matches!(err, ResumeError::PageSizeMismatch { .. }), "{err}");
}

#[test]
fn resume_io() {
    let failing = Failing {
        inner: Cursor::new(two_txs().bytes),
        ok: 1,
    };
    let mut reader = WalReader::open(failing).unwrap();
    let err = reader
        .resume(WalOffset::from_frame_index(1, size(PS)), salts(SALTS))
        .unwrap_err();
    assert!(matches!(err, ResumeError::Io(_)), "{err}");
}

#[test]
fn resume_refuses_a_wrong_stored_checksum_in_the_frame_before_or_before_that() {
    // Frame 1 is the frame before the offset; frame 0 supplies the checksum it chains from.
    for (flipped, at) in [(1, 2), (0, 2)] {
        let mut wal = two_txs();
        let flen = wal.frame_len();
        wal.bytes[32 + flen * flipped + 16] ^= 1;
        let err = open(&wal.bytes)
            .resume(WalOffset::from_frame_index(at, size(PS)), salts(SALTS))
            .unwrap_err();
        assert!(
            matches!(
                err,
                ResumeError::PrevFrame {
                    fault: FrameFault::ChecksumMismatch,
                    ..
                }
            ),
            "frame {flipped}: {err}"
        );
    }
}

#[test]
fn resume_restarted_names_both_generations() {
    let mut reader = open(&two_txs().bytes);
    let other = salts((1, 2));
    for at in [
        WalOffset::start(size(PS)),
        WalOffset::from_frame_index(2, size(PS)),
    ] {
        let err = reader.resume(at, other).unwrap_err();
        assert!(
            matches!(err, ResumeError::Restarted { expected, found }
                if expected == other && found == salts(SALTS)),
            "{err}"
        );
    }
}

#[test]
fn resume_not_at_commit() {
    let mut reader = open(&two_txs().bytes);
    let err = reader
        .resume(WalOffset::from_frame_index(1, size(PS)), salts(SALTS))
        .unwrap_err();
    assert!(matches!(err, ResumeError::NotAtCommit { .. }), "{err}");
}

#[test]
fn a_committed_end_of_another_generation_is_restarted_before_any_read() {
    let wal = two_txs();
    let highest = Rc::new(Cell::new(0));
    let watching = Watching {
        inner: Cursor::new(wal.bytes.clone()),
        highest: Rc::clone(&highest),
    };
    let mut reader = WalReader::open(watching).unwrap();
    let after_open = highest.get();
    let end = CommittedEnd::new(4, salts((1, 2)), size(PS));
    let cursor = reader.cursor_at_start();
    let err = reader
        .next_tx(&cursor, ScanLimit::Committed(end))
        .unwrap_err();
    assert!(
        matches!(err, WalError::Restarted { cursor, header }
            if cursor == salts((1, 2)) && header == salts(SALTS)),
        "{err}"
    );
    assert_eq!(highest.get(), after_open, "no frame was read");
}

#[test]
fn read_page_returns_the_scanned_bytes() {
    let wal = two_txs();
    let mut reader = open(&wal.bytes);
    let scanned = scan_from(&mut reader, None, ScanLimit::FirstInvalidFrame).unwrap();
    let mut buf = vec![0; PSU];
    reader
        .read_page(&scanned.txs[1].frames()[1], &mut buf)
        .unwrap();
    assert_eq!(buf, page(PS, 23));
}

#[test]
fn read_page_changed_since_scan() {
    let wal = two_txs();
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut changed = wal.bytes.clone();
    changed[32 + 24 + 100] ^= 1; // a byte of frame 0's page
    let mut reader = open(&changed);
    let mut buf = vec![7; PSU];
    let err = reader
        .read_page(&scanned.txs[0].frames()[0], &mut buf)
        .unwrap_err();
    assert!(
        matches!(err, ReadPageError::ChangedSinceScan { .. }),
        "{err}"
    );
    assert!(
        buf.iter().all(|b| *b == 0),
        "unverified bytes are not left for the caller"
    );
}

#[test]
fn read_page_buffer_size() {
    let wal = two_txs();
    let mut reader = open(&wal.bytes);
    let scanned = scan_from(&mut reader, None, ScanLimit::FirstInvalidFrame).unwrap();
    let err = reader
        .read_page(&scanned.txs[0].frames()[0], &mut [0; 100])
        .unwrap_err();
    assert!(
        matches!(
            err,
            ReadPageError::BufferSize {
                expected: 512,
                found: 100
            }
        ),
        "{err}"
    );
}

#[test]
fn read_page_page_size_mismatch() {
    let scanned = scan(&two_txs().bytes, ScanLimit::FirstInvalidFrame);
    let mut other = Wal::new(1024, false, SALTS);
    other.tx(&[1], 1, 1);
    let err = open(&other.bytes)
        .read_page(&scanned.txs[0].frames()[0], &mut vec![0; 1024])
        .unwrap_err();
    assert!(
        matches!(err, ReadPageError::PageSizeMismatch { .. }),
        "{err}"
    );
}

#[test]
fn read_page_truncated() {
    let wal = two_txs();
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut reader = open(&wal.bytes[..32 + 24 + 10]);
    let err = reader
        .read_page(&scanned.txs[0].frames()[0], &mut vec![0; PSU])
        .unwrap_err();
    assert!(matches!(err, ReadPageError::Truncated { .. }), "{err}");
}

#[test]
fn read_page_io() {
    let wal = two_txs();
    let scanned = scan(&wal.bytes, ScanLimit::FirstInvalidFrame);
    let mut reader = WalReader::open(Failing {
        inner: Cursor::new(wal.bytes),
        ok: 1,
    })
    .unwrap();
    let err = reader
        .read_page(&scanned.txs[0].frames()[0], &mut vec![0; PSU])
        .unwrap_err();
    assert!(matches!(err, ReadPageError::Io(_)), "{err}");
}

#[test]
fn checksums_cross_check_with_the_crate() {
    let bytes = page(PS, 3);
    for big in [false, true] {
        let order = if big {
            ByteOrder::Big
        } else {
            ByteOrder::Little
        };
        let ours = WalChecksum::ZERO.extend(order, &bytes).unwrap();
        assert_eq!((ours.s0(), ours.s1()), sum(big, (0, 0), &bytes));
    }
}

#[test]
fn builder_matches_sqlite_header_checksum() {
    use lsr_wal::testing::WalBuilder;
    for (big, order) in [(false, ByteOrder::Little), (true, ByteOrder::Big)] {
        let built = WalBuilder::new(size(4096), big, salts(SALTS));
        let header = WalHeader::decode(built.bytes().try_into().unwrap()).unwrap();
        assert_eq!(header.byte_order(), order);
        assert_eq!(header.page_size(), size(4096));
        assert_eq!(header.salts(), salts(SALTS));
        assert_eq!(
            Some(header.checksum()),
            WalChecksum::ZERO.extend(order, &built.bytes()[..24])
        );
    }
}

#[test]
fn builder_transactions_scan_as_committed() {
    use lsr_ltx::PageNo;
    use lsr_wal::testing::{WalBuilder, page};
    let ps = size(PS);
    let pg = |n| PageNo::new(n, ps).unwrap();
    let mut built = WalBuilder::new(ps, false, salts(SALTS));
    built.tx(&[pg(1), pg(2)], 2, 7);
    built.tx(&[pg(2)], 2, 8);
    assert_eq!(built.bytes().len(), 32 + 3 * built.frame_len());
    let bytes = built.into_bytes();
    let scanned = scan(&bytes, ScanLimit::FirstInvalidFrame);
    assert_eq!(scanned.txs.len(), 2);
    assert_ne!(page(7, pg(1), ps), page(7, pg(2), ps));
    assert_ne!(page(7, pg(1), ps), page(8, pg(1), ps));
    assert_eq!(page(7, pg(1), ps), page(7, pg(1), ps));
    let frame_start = 32 + 24;
    assert_eq!(&bytes[frame_start..frame_start + PSU], page(7, pg(1), ps));
}
