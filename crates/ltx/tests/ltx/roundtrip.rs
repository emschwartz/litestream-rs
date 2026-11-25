//! Property tests: what the encoder writes, the decoder reads back exactly,
//! however the input is chunked.

use lsr_ltx::{
    DbChecksum, Encoder, Header, HeaderFields, PageChecksum, PageNo, PageSize, Timestamp, Txid,
    TxidRange, WalOrigin,
};
use proptest::prelude::*;

use crate::common::{decode, decode_chunked};

#[derive(Debug, Clone)]
struct Case {
    header: Header,
    pages: Vec<(u32, Vec<u8>)>,
    post: Option<DbChecksum>,
    chunks: Vec<usize>,
}

fn page_bytes(size: usize) -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), size),
        (any::<u8>(), 1..64_usize).prop_map(move |(seed, period)| (0..size)
            .map(|i| seed.wrapping_add(u8::try_from(i % period).unwrap()))
            .collect()),
        Just(vec![0; size]),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (
        prop::sample::select(vec![512_u32, 1024, 4096]),
        any::<bool>(),
        any::<bool>(),
        1..12_u32,
        any::<u64>(),
        any::<i64>(),
        any::<(u32, u32)>(),
    )
        .prop_flat_map(|(size, snapshot, tracked, commit, pre, timestamp, salts)| {
            let pgnos = if snapshot {
                Just((1..=commit).collect::<Vec<u32>>()).boxed()
            } else {
                prop::sample::subsequence(
                    (1..=commit).collect::<Vec<u32>>(),
                    0..=usize::try_from(commit).unwrap(),
                )
                .boxed()
            };
            let bytes = usize::try_from(size).unwrap();
            (
                pgnos,
                prop::collection::vec(page_bytes(bytes), usize::try_from(commit).unwrap()),
                prop::collection::vec(1..700_usize, 1..6),
            )
                .prop_map(move |(pgnos, datas, chunks)| {
                    let min = if snapshot { 1 } else { 2 + pre % 1000 };
                    let page_size = PageSize::new(size).unwrap();
                    let header = Header::new(HeaderFields {
                        tracked,
                        page_size,
                        commit,
                        txids: TxidRange::new(
                            Txid::new(min).unwrap(),
                            Txid::new(min + pre % 7).unwrap(),
                        )
                        .unwrap(),
                        timestamp: Timestamp::from_unix_millis(timestamp),
                        pre_apply: (tracked && !snapshot)
                            .then(|| DbChecksum::from_raw(pre | (1 << 63)).unwrap()),
                        wal: WalOrigin::new(32 + pre % 100, pre % 5000, salts.0, salts.1),
                        node_id: pre,
                    })
                    .unwrap();
                    let pages: Vec<(u32, Vec<u8>)> = pgnos.into_iter().zip(datas).collect();
                    let post = tracked.then(|| {
                        if snapshot {
                            pages.iter().fold(DbChecksum::EMPTY, |sum, (pgno, data)| {
                                sum.toggle(PageChecksum::of(
                                    PageNo::new(*pgno, page_size).unwrap(),
                                    data,
                                ))
                            })
                        } else {
                            DbChecksum::from_raw(pre.rotate_left(7) | (1 << 63)).unwrap()
                        }
                    });
                    Case {
                        header,
                        pages,
                        post,
                        chunks,
                    }
                })
        })
}

fn encode(case: &Case) -> Vec<u8> {
    let mut encoder = Encoder::new(Vec::new(), case.header).unwrap();
    for (pgno, data) in &case.pages {
        encoder
            .write_page(PageNo::new(*pgno, case.header.page_size()).unwrap(), data)
            .unwrap();
    }
    encoder.finish(case.post).unwrap().0
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn encode_then_decode_is_identity(case in case()) {
        let bytes = encode(&case);
        let decoded = decode(&bytes).unwrap();
        prop_assert_eq!(decoded.header, case.header);
        prop_assert_eq!(&decoded.pages, &case.pages);
        prop_assert_eq!(decoded.verified.trailer().post_apply(), case.post);
        prop_assert_eq!(decode_chunked(&bytes, &case.chunks).unwrap(), decoded);
    }

    #[test]
    fn one_byte_at_a_time_matches_one_shot(case in case()) {
        let bytes = encode(&case);
        prop_assert_eq!(decode_chunked(&bytes, &[1]).unwrap(), decode(&bytes).unwrap());
    }

    #[test]
    fn header_round_trips(case in case()) {
        prop_assert_eq!(Header::decode(&case.header.encode()), Ok(case.header));
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..2048), chunks in prop::collection::vec(1..300_usize, 1..4)) {
        let mut file = b"LTX1".to_vec();
        file.extend_from_slice(&bytes);
        // Any verdict is fine; reaching it without a panic is the property.
        drop(decode_chunked(&file, &chunks));
    }
}
/// An index longer than one chunk (16 KiB, about 1,800 pages), written out piece by piece
/// after the last page, is the same file as one finish writes, and no page fits after it.
#[test]
fn an_index_written_in_pieces_is_the_file_finish_alone_writes() {
    let page_size = PageSize::new(512).unwrap();
    let commit = 4_000_u32;
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size,
        commit,
        txids: TxidRange::new(Txid::new(1).unwrap(), Txid::new(1).unwrap()).unwrap(),
        timestamp: Timestamp::from_unix_millis(7),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let page = |n: u32| {
        let seed = u8::try_from(n % 251).unwrap();
        (0..512_usize)
            .map(|i| seed.wrapping_add(u8::try_from(i % 97).unwrap()))
            .collect::<Vec<u8>>()
    };
    let fill = |encoder: &mut Encoder<Vec<u8>>| {
        for n in 1..=commit {
            encoder
                .write_page(PageNo::new(n, page_size).unwrap(), &page(n))
                .unwrap();
        }
    };
    let mut whole = Encoder::new(Vec::new(), header).unwrap();
    fill(&mut whole);
    let one_shot = whole.finish(None).unwrap().0;

    let mut pieces = Encoder::new(Vec::new(), header).unwrap();
    fill(&mut pieces);
    let mut calls = 0_u32;
    while pieces.write_index_chunk().unwrap() {
        calls += 1;
    }
    assert!(
        calls >= 1,
        "the index took {calls} calls, so it was not several chunks"
    );
    assert!(
        pieces
            .write_page(PageNo::new(1, page_size).unwrap(), &page(1))
            .is_err(),
        "a page after the index began"
    );
    let piecewise = pieces.finish(None).unwrap().0;
    assert_eq!(piecewise, one_shot);
    assert_eq!(decode(&piecewise).unwrap().pages.len(), 4_000);
}
