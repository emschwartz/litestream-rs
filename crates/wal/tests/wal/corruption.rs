//! Damage anywhere: an error or a prefix of the clean result, never a different page.

use std::io::Cursor;

use lsr_wal::{CommittedEnd, ScanLimit, TxOutcome, WalError, WalReader};
use proptest::prelude::*;

use crate::common::{Wal, scan, scan_from, size};

const PS: u32 = 512;

fn baseline() -> Wal {
    let mut wal = Wal::new(PS, false, (0x1111, 0x2222));
    wal.tx(&[1, 2], 2, 10);
    wal.tx(&[2, 3], 3, 20);
    wal.tx(&[1], 3, 30);
    wal
}

fn committed_end(wal: &Wal) -> CommittedEnd {
    let frames = (wal.bytes.len() - 32) / wal.frame_len();
    CommittedEnd::from_frame_count(u32::try_from(frames).unwrap(), size(PS))
}

/// The scan result under a limit, flattened: `None` when the WAL does not even open.
fn run(bytes: &[u8], limit: ScanLimit) -> Option<Result<Vec<lsr_wal::CommittedTx>, WalError>> {
    let mut reader = WalReader::open(Cursor::new(bytes.to_vec())).ok()?;
    Some(scan_from(&mut reader, None, limit).map(|s| s.txs))
}

#[test]
fn every_bit_flip_is_an_error_or_a_strict_prefix() {
    let wal = baseline();
    let clean = scan(&wal.bytes, ScanLimit::FirstInvalidFrame).txs;
    let end = ScanLimit::Committed(committed_end(&wal));
    let mut opened = 0;
    for byte in 0..wal.bytes.len() {
        for bit in 0..8 {
            let mut bytes = wal.bytes.clone();
            bytes[byte] ^= 1 << bit;
            let Some(first) = run(&bytes, ScanLimit::FirstInvalidFrame) else {
                continue; // a damaged header is refused at open
            };
            opened += 1;
            let txs = first.unwrap();
            assert!(txs.len() < clean.len(), "flip {byte}:{bit} lost nothing");
            assert_eq!(
                txs[..],
                clean[..txs.len()],
                "flip {byte}:{bit} changed a page"
            );
            let below = run(&bytes, end).unwrap();
            assert!(
                matches!(below, Err(WalError::CorruptBelowCommitted { .. })),
                "flip {byte}:{bit} under the committed limit: {below:?}"
            );
        }
    }
    assert_eq!(opened, (wal.bytes.len() - 32) * 8);
}

#[test]
fn every_truncation_is_an_error_or_a_strict_prefix() {
    let wal = baseline();
    let clean = scan(&wal.bytes, ScanLimit::FirstInvalidFrame).txs;
    let end = ScanLimit::Committed(committed_end(&wal));
    for len in 0..wal.bytes.len() {
        let bytes = &wal.bytes[..len];
        let Some(first) = run(bytes, ScanLimit::FirstInvalidFrame) else {
            assert!(len < 32, "a whole header failed to open at {len}");
            continue;
        };
        let txs = first.unwrap();
        assert!(txs.len() < clean.len(), "truncation to {len} lost nothing");
        assert_eq!(
            txs[..],
            clean[..txs.len()],
            "truncation to {len} changed a page"
        );
        let below = run(bytes, end).unwrap();
        assert!(
            matches!(below, Err(WalError::TruncatedBelowCommitted { .. })),
            "truncation to {len} under the committed limit: {below:?}"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..700)) {
        let _ = run(&bytes, ScanLimit::FirstInvalidFrame);
    }

    #[test]
    fn a_valid_header_over_arbitrary_frames_never_panics_or_overreads(
        ps in prop::sample::select(vec![512u32, 4096, 65536]),
        tail in proptest::collection::vec(any::<u8>(), 0..1500),
        frames in 0u32..4,
    ) {
        let mut bytes = Wal::new(ps, false, (5, 6)).bytes;
        bytes.extend(tail);
        let mut reader = WalReader::open(Cursor::new(bytes)).unwrap();
        let end = ScanLimit::Committed(CommittedEnd::from_frame_count(frames, size(ps)));
        for limit in [ScanLimit::FirstInvalidFrame, end] {
            let cursor = reader.cursor_at_start();
            match reader.next_tx(&cursor, limit) {
                Ok(TxOutcome::Tx(_) | TxOutcome::End { .. }) | Err(_) => {}
            }
        }
    }
}
