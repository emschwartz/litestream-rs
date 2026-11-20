use lsr_ltx::{PageNo, PageSize};
use lsr_wal::testing::WalBuilder;

use super::*;

const SALTS: Salts = Salts {
    salt1: 7,
    salt2: 0x1234_5678,
};

fn size(bytes: u32) -> PageSize {
    PageSize::new(bytes).unwrap()
}

fn pgnos(raw: &[u32], page_size: PageSize) -> Vec<PageNo> {
    raw.iter()
        .map(|n| PageNo::new(*n, page_size).unwrap())
        .collect()
}

/// Three transactions, a shrink among them, then an uncommitted frame.
fn three_txs(page_size: PageSize, big_endian: bool) -> Vec<u8> {
    let mut wal = WalBuilder::new(page_size, big_endian, SALTS);
    wal.tx(&pgnos(&[1, 2, 3], page_size), 3, 1);
    wal.tx(&pgnos(&[2, 4], page_size), 4, 2);
    wal.tx(&pgnos(&[1], page_size), 2, 3);
    wal.frame(PageNo::new(5, page_size).unwrap(), 0, 4);
    wal.into_bytes()
}

fn input(params: [u8; WAL_PREFIX_LEN], file: &[u8]) -> Vec<u8> {
    let mut data = params.to_vec();
    data.extend(file);
    data
}

/// The whole scan of `file` after the mode's fix-ups.
fn whole_of(mode: u8, file: &[u8]) -> Scan {
    let mut file = file.to_vec();
    if mode & WAL_REWRITE_HEADER != 0 {
        rewrite_header(&mut file, mode);
    }
    if mode & WAL_RECHAIN != 0 {
        rechain(&mut file);
    }
    let shared = Shared::new(file);
    let mut reader = open(&shared).expect("a valid header");
    let frames = frames_in(shared.len(), reader.header().page_size());
    whole_scan(&mut reader, &shared, frames)
}

#[test]
fn short_and_garbage_input_do_not_panic() {
    wal(&[]);
    wal(&[0; WAL_PREFIX_LEN]);
    wal(&[0xff; 300]);
    let mut rewritten = vec![WAL_REWRITE_HEADER | WAL_RECHAIN];
    rewritten.extend([0xa5; 2000]);
    wal(&rewritten);
}

#[test]
fn a_built_wal_holds_under_every_parameter() {
    for (page_size, big) in [(512, false), (1024, true), (4096, false)] {
        let file = three_txs(size(page_size), big);
        let whole = whole_of(0, &file);
        assert_eq!(whole.txs.len(), 3);
        assert_eq!(
            whole.stopped().map(|(_, tail)| tail),
            Some(Tail::Uncommitted { frames: 1 })
        );
        for seed in 0_u8..64 {
            let spread = |k: u8| seed.wrapping_mul(k);
            let params = [
                0,
                spread(37),
                spread(11),
                spread(3),
                spread(5),
                seed,
                spread(7),
                seed,
                spread(13),
                spread(17),
                spread(19),
                spread(23),
            ];
            wal(&input(params, &file));
        }
    }
}

#[test]
fn a_rewritten_header_reads_with_the_other_byte_order() {
    let file = three_txs(size(512), false);
    let mode = WAL_BIG_ENDIAN | WAL_REWRITE_HEADER | WAL_RECHAIN;
    let whole = whole_of(mode, &file);
    assert_eq!(whole.txs.len(), 3);
    // Without re-chaining, the frames no longer match the new header's checksum.
    assert!(
        whole_of(WAL_BIG_ENDIAN | WAL_REWRITE_HEADER, &file)
            .txs
            .is_empty()
    );
    wal(&input([mode, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], &file));
}

#[test]
fn rechaining_lets_a_changed_commit_field_through() {
    let mut file = three_txs(size(512), false);
    // The commit field of the first frame: the first transaction is now one frame.
    let at = HEADER_LEN + 4;
    file[at..at + 4].copy_from_slice(&5_u32.to_be_bytes());
    assert!(whole_of(0, &file).txs.is_empty());
    let whole = whole_of(WAL_RECHAIN, &file);
    assert_eq!(whole.txs.len(), 4);
    assert_eq!(whole.txs.first().map(CommittedTx::commit), Some(5));
    wal(&input(
        [WAL_RECHAIN, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        &file,
    ));
}

#[test]
fn old_generation_frames_end_the_scan() {
    let page_size = size(512);
    let old = Salts { salt1: 6, salt2: 1 };
    let mut stale = WalBuilder::new(page_size, false, old);
    stale.tx(&pgnos(&[1, 2, 3, 4], page_size), 4, 9);
    let mut file = WalBuilder::new(page_size, false, SALTS);
    file.tx(&pgnos(&[1], page_size), 4, 1);
    let mut bytes = file.into_bytes();
    let stale = stale.into_bytes();
    let frame_len = 512 + FRAME_HEADER_LEN;
    bytes.extend(&stale[HEADER_LEN + frame_len..]);
    let whole = whole_of(0, &bytes);
    assert_eq!(whole.txs.len(), 1);
    assert!(matches!(
        whole.stopped(),
        Some((_, Tail::InvalidFrame(FrameFault::SaltMismatch { found }))) if found == old
    ));
    for pick in 0..4 {
        wal(&input(
            [0, 0xff, 0xff, 0, 3, pick, 0, 2, pick, 0, 9, 0x80],
            &bytes,
        ));
    }
}

#[test]
fn a_page_size_the_file_cannot_hold_still_opens() {
    let file = three_txs(size(512), false);
    // 65536-byte pages: the file holds no whole frame.
    let whole = whole_of(WAL_REWRITE_HEADER | 7, &file);
    assert!(whole.txs.is_empty());
    assert_eq!(
        whole.stopped().map(|(_, tail)| tail),
        Some(Tail::PartialFrame)
    );
}
