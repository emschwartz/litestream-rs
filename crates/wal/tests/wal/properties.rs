//! Properties over generated WALs: nothing panics, and a remembered position is only ever resumed in its own generation.

use lsr_wal::{
    CommittedEnd, PageMap, ResumeError, Salts, ScanLimit, TxOutcome, WalOffset, WalReader,
};
use proptest::prelude::*;
use std::hint::black_box;
use std::io::Cursor;

use crate::common::{Wal, open, scan_from, size};

fn salts(pair: (u32, u32)) -> Salts {
    Salts {
        salt1: pair.0,
        salt2: pair.1,
    }
}

type Txs = Vec<Vec<u32>>;

fn txs() -> impl Strategy<Value = Txs> {
    proptest::collection::vec(proptest::collection::vec(1u32..6, 1..4), 1..6)
}

/// A WAL of the given transactions; each commits at its highest page number.
fn build(salt_pair: (u32, u32), txs: &Txs, seed: u32) -> Wal {
    let mut wal = Wal::new(512, false, salt_pair);
    for (i, pgnos) in txs.iter().enumerate() {
        let commit = pgnos.iter().copied().max().unwrap_or(1);
        wal.tx(pgnos, commit, seed + u32::try_from(i).unwrap());
    }
    wal
}

/// Frame indexes after each transaction, starting with 0.
fn boundaries(txs: &Txs) -> Vec<u32> {
    let mut at = 0;
    let mut out = vec![0];
    for tx in txs {
        at += u32::try_from(tx.len()).unwrap();
        out.push(at);
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn nothing_panics_on_arbitrary_frames_flips_positions_and_ends(
        big in any::<bool>(),
        ps in prop::sample::select(vec![512u32, 4096]),
        frames in proptest::collection::vec((0u32..8, 0u32..8, any::<bool>()), 0..8),
        flips in proptest::collection::vec((any::<usize>(), 1u8..=255), 0..4),
        resume_at in 0u32..12,
        remembered in any::<(u32, u32)>(),
        use_header_salts in any::<bool>(),
        mx in 0u32..12,
        end_salts in any::<(u32, u32)>(),
    ) {
        let header = (7, 9);
        let mut wal = Wal::new(ps, big, header);
        for (pgno, commit, good) in frames {
            let data = crate::common::page(ps, pgno);
            wal.frame_with(pgno, commit, if good { header } else { (1, 2) }, &data);
        }
        let mut bytes = wal.bytes;
        for (at, mask) in flips {
            let len = bytes.len();
            bytes[at % len] ^= mask;
        }
        let Ok(mut reader) = WalReader::open(Cursor::new(bytes)) else { return Ok(()) };
        let page_size = reader.header().page_size();
        let expected = if use_header_salts { reader.header().salts() } else { salts(remembered) };
        black_box(reader.resume(WalOffset::from_frame_index(resume_at, page_size), expected, lsr_wal::testing::unbounded(expected)).is_ok());
        let ends = [
            ScanLimit::FirstInvalidFrame,
            ScanLimit::Committed(CommittedEnd::new(mx, reader.header().salts(), page_size)),
            ScanLimit::Committed(CommittedEnd::new(mx, salts(end_salts), page_size)),
        ];
        let mut buf = vec![0; page_size.bytes()];
        for limit in ends {
            let mut cursor = reader.cursor_at_start();
            let mut map = PageMap::new(page_size);
            for _ in 0..16 {
                match reader.next_tx(&cursor, limit) {
                    Ok(TxOutcome::Tx(tx)) => {
                        black_box(map.apply(&tx).is_ok());
                        for frame in tx.frames() {
                            black_box(reader.read_page(frame, &mut buf).is_ok());
                        }
                        cursor = *tx.end();
                    }
                    Ok(TxOutcome::End { .. }) | Err(_) => break,
                }
            }
        }
    }

    #[test]
    fn a_position_is_resumed_only_in_its_own_generation(
        a in txs(),
        b in txs(),
        salt2 in any::<u32>(),
    ) {
        let salts_a = (0xaaaa_0001, 0xbbbb_0002);
        let salts_b = (salts_a.0 + 1, salt2);
        let (wal_a, wal_b) = (build(salts_a, &a, 10), build(salts_b, &b, 90));
        let bounds = boundaries(&a);
        let frames = *bounds.last().unwrap();
        let full = scan_from(&mut open(&wal_a.bytes), None, ScanLimit::FirstInvalidFrame).unwrap().txs;
        for index in 0..=frames {
            let at = WalOffset::from_frame_index(index, size(512));
            let resumed = open(&wal_a.bytes).resume(at, salts(salts_a), lsr_wal::testing::unbounded(salts(salts_a)));
            match bounds.iter().position(|b| *b == index) {
                Some(done) => {
                    let cursor = resumed.unwrap();
                    let mut reader = open(&wal_a.bytes);
                    let rest = scan_from(&mut reader, Some(cursor), ScanLimit::FirstInvalidFrame).unwrap();
                    prop_assert_eq!(&rest.txs[..], &full[done..]);
                }
                None => prop_assert!(matches!(resumed, Err(ResumeError::NotAtCommit { .. })), "frame {}: {:?}", index, resumed),
            }
        }
        // After a restart and regrowth of any length, no position of generation A resumes.
        for index in bounds {
            let at = WalOffset::from_frame_index(index, size(512));
            let resumed = open(&wal_b.bytes).resume(at, salts(salts_a), lsr_wal::testing::unbounded(salts(salts_a)));
            prop_assert!(matches!(resumed, Err(ResumeError::Restarted { .. })), "frame {}: {:?}", index, resumed);
        }
    }
}
