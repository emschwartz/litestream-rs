use lsr_ltx::{Encoder, Header, PageSize, WalOrigin};

use super::super::tests::{
    commit, encode_newest_staged, fresh, is_rewound, nz, pg, put, reader, regrown, salts, ts, txid,
    view,
};
use super::*;
use crate::fault::SnapshotCause;
use crate::layout::{FileKey, Level};
use crate::mem::MemStore;
use crate::recover::{LastPageCheck, PositionSource, Recovered, recover};
use crate::source::FakeWal;
use crate::store::{ObjectStore, Upload};

const PS: PageSize = PageSize::MIN;
/// A WAL frame: its 24-byte header and one page.
const FRAME: u64 = 24 + 512;

/// The replica's newest staged file for transaction 2, loaded against `wal`.
fn newest_replica_file(store: &MemStore, wal: &FakeWal) -> Result<Option<NewestStagedFile>, Fault> {
    crate::conformance::block_on(async {
        let mut r = reader(wal.file_bytes());
        NewestStagedFile::load_remote(store, Some(&mut r), txid(2)).await
    })
}

fn newest_staged(store: &MemStore, wal: &FakeWal) -> NewestStagedFile {
    crate::conformance::block_on(async {
        let mut r = reader(wal.file_bytes());
        NewestStagedFile::load(store, Some(&mut r))
            .await
            .unwrap()
            .unwrap()
    })
}

/// One store holding `bytes` as transaction 2.
fn holding(bytes: &[u8]) -> MemStore {
    let store = MemStore::new();
    crate::conformance::block_on(put(&store, 2, bytes));
    store
}

#[test]
fn a_wiped_meta_continues_from_the_replicas_newest_staged_file() {
    let wal = fresh();
    let remote = holding(&encode_newest_staged(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3));
    let newest_staged = newest_replica_file(&remote, &wal).unwrap().unwrap();
    assert!(
        matches!(
            newest_staged,
            NewestStagedFile::Readable {
                last_page: LastPageCheck::Match { .. },
                ..
            }
        ),
        "{newest_staged:?}"
    );
    let recovered = recover(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&wal),
        Some(txid(2)),
    );
    assert!(matches!(recovered, Recovered::Continue(_)), "{recovered:?}");
}

/// A WAL as it stands at boot, the newest staged file's pages and its commit size.
type Case = (FakeWal, Vec<(u32, u8)>, u32);

fn cases() -> Vec<Case> {
    let full = vec![(1, 1), (2, 2), (3, 3)];
    let restarted = fresh();
    restarted.force_restart();
    commit(&restarted, &[1], 3);
    let other_size = FakeWal::new(PS, salts(10));
    commit(&other_size, &[1, 2], 2);
    commit(&other_size, &[2, 3], 5);
    let grown = fresh();
    commit(&grown, &[1], 3);
    vec![
        (fresh(), full.clone(), 3),
        (grown, full.clone(), 3),
        (regrown(), full.clone(), 3),
        (other_size, full.clone(), 3),
        (fresh(), vec![(2, 2), (3, 3)], 3),
        (fresh(), vec![(1, 1), (2, 2), (3, 9)], 3),
        (restarted, full, 3),
    ]
}

#[test]
fn the_replicas_newest_staged_file_decides_as_the_same_file_staged_would() {
    let mut causes = Vec::new();
    for (now, pages, size) in cases() {
        let bytes = encode_newest_staged(&fresh(), 2, &pages, size);
        let store = holding(&bytes);
        let staged = newest_staged(&store, &now);
        let remote = newest_replica_file(&store, &now).unwrap().unwrap();
        assert_eq!(remote, staged);
        let seen = view(&now);
        let from_remote = recover(Some(PositionSource::Boot(&remote)), &seen, Some(txid(2)));
        let from_staged = recover(Some(PositionSource::Boot(&staged)), &seen, Some(txid(2)));
        assert_eq!(from_remote, from_staged);
        causes.push(from_remote);
    }
    // The cases reach a continue and each snapshot a staged newest file can give.
    assert!(matches!(causes[0], Recovered::Continue(_)), "{causes:?}");
    assert!(matches!(causes[1], Recovered::Continue(_)), "{causes:?}");
    assert!(is_rewound(causes[2]), "{causes:?}");
    assert!(is_rewound(causes[3]), "{causes:?}");
    assert!(is_rewound(causes[4]), "{causes:?}");
    let cause = |at: usize| match causes[at] {
        Recovered::Snapshot { cause, .. } => Some(cause),
        Recovered::Continue(_) | Recovered::WalRestarted(_) | Recovered::Stop(_) => None,
    };
    assert_eq!(cause(5), Some(SnapshotCause::LastPageMismatch));
    assert_eq!(cause(6), Some(SnapshotCause::GenerationChanged));
}

#[test]
fn a_replica_newest_staged_file_whose_header_names_another_transaction_is_corrupt() {
    let wal = fresh();
    let remote = holding(&encode_newest_staged(&wal, 3, &[(1, 1), (2, 2), (3, 3)], 3));
    assert_eq!(
        newest_replica_file(&remote, &wal),
        Err(Fault::RemoteCorrupt)
    );
}

#[test]
fn a_replica_newest_staged_file_that_does_not_verify_is_corrupt() {
    let wal = fresh();
    let mut bytes = encode_newest_staged(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    let at = bytes.len() / 2;
    bytes[at] ^= 0x01;
    assert_eq!(
        newest_replica_file(&holding(&bytes), &wal),
        Err(Fault::RemoteCorrupt)
    );
    let short = encode_newest_staged(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    let cut = holding(&short[..50]);
    assert_eq!(newest_replica_file(&cut, &wal), Err(Fault::RemoteCorrupt));
}

#[test]
fn no_level_zero_file_at_the_newest_transaction_is_no_newest_staged_file() {
    // The replica's newest file is above level 0: its level-0 files were
    // retained away, so there is no position, and boot takes a first sync.
    let wal = fresh();
    let remote = MemStore::new();
    let bytes = encode_newest_staged(&wal, 2, &[(1, 1), (2, 2), (3, 3)], 3);
    let key = FileKey::new(
        Level::new(1).unwrap(),
        lsr_ltx::TxidRange::new(txid(1), txid(2)).unwrap(),
    );
    crate::conformance::block_on(async {
        let mut up = remote.begin_upload(&key, ts(7)).await.unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap();
    });
    assert_eq!(newest_replica_file(&remote, &wal), Ok(None));
}

#[test]
fn a_replica_newest_staged_file_without_a_wal_origin_is_no_newest_staged_file() {
    let wal = fresh();
    let origin = WalOrigin::new(32, 0, 1, 2).unwrap();
    let mut fields = crate::sync::header_fields(txid(2), nz(3), PS, origin, ts(7), 0);
    fields.wal = None;
    let mut enc = Encoder::new(Vec::new(), Header::new(fields).unwrap()).unwrap();
    enc.write_page(pg(1), &vec![1; PS.bytes()]).unwrap();
    let bytes = enc.finish(None).unwrap().0;
    assert_eq!(newest_replica_file(&holding(&bytes), &wal), Ok(None));
}

/// Pages no compressor shrinks, so the file spans several reads.
fn noise(seed: u32) -> Vec<u8> {
    let mut state = u64::from(seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..PS.bytes())
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_be_bytes()[0]
        })
        .collect()
}

#[test]
fn a_replica_newest_staged_file_over_one_read_is_checked_whole() {
    let wal = FakeWal::new(PS, salts(10));
    let count = 2_400_u32;
    let pages: Vec<_> = (1..=count).map(|n| (pg(n), noise(n))).collect();
    wal.append_tx(&pages, nz(count));
    let end = 32 + u64::from(count) * FRAME;
    let head = *reader(wal.file_bytes()).header();
    let origin = WalOrigin::new(32, end - 32, head.salts().salt1, head.salts().salt2).unwrap();
    let fields = crate::sync::header_fields(txid(2), nz(count), PS, origin, ts(7), 0);
    let mut enc = Encoder::new(Vec::new(), Header::new(fields).unwrap()).unwrap();
    for (pgno, data) in &pages {
        enc.write_page(*pgno, data).unwrap();
    }
    let bytes = enc.finish(None).unwrap().0;
    assert!(bytes.len() > 1 << 20, "{}", bytes.len());
    let newest_staged = newest_replica_file(&holding(&bytes), &wal)
        .unwrap()
        .unwrap();
    let recovered = recover(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&wal),
        Some(txid(2)),
    );
    assert!(matches!(recovered, Recovered::Continue(_)), "{recovered:?}");
}

/// A rewind that drops the newest staged file and the transaction before it, then regrows
/// both: the first with other bytes, the newest staged file byte for byte. The compare of the whole
/// newest staged file sees only the newest staged file's range, which is identical, so boot continues
/// although the replica's first transaction is not the database's. Closing it
/// needs local state that ties the WAL's checksum chain to the newest staged file, which the
/// replica format has no field for.
#[test]
#[ignore = "review: issue #12, a rewind of two or more transactions that regrows a byte-identical newest staged file passes the compare of the whole newest staged file"]
fn review_a_rewind_of_two_transactions_regrowing_an_identical_newest_staged_file_is_caught() {
    let before = fresh();
    let after = FakeWal::new(PS, salts(10));
    after.append_tx(
        &[
            (pg(1), vec![0xAA; PS.bytes()]),
            (pg(2), vec![0xAA; PS.bytes()]),
        ],
        nz(2),
    );
    commit(&after, &[2, 3], 3);
    let head = *reader(before.file_bytes()).header();
    let origin = WalOrigin::new(
        32 + 2 * FRAME,
        2 * FRAME,
        head.salts().salt1,
        head.salts().salt2,
    )
    .unwrap();
    let fields = crate::sync::header_fields(txid(2), nz(3), PS, origin, ts(7), 0);
    let mut enc = Encoder::new(Vec::new(), Header::new(fields).unwrap()).unwrap();
    enc.write_page(pg(2), &vec![2; PS.bytes()]).unwrap();
    enc.write_page(pg(3), &vec![3; PS.bytes()]).unwrap();
    let remote = holding(&enc.finish(None).unwrap().0);
    let newest_staged = newest_replica_file(&remote, &after).unwrap().unwrap();
    let recovered = recover(
        Some(PositionSource::Boot(&newest_staged)),
        &view(&after),
        Some(txid(2)),
    );
    assert!(
        matches!(recovered, Recovered::Snapshot { .. }),
        "{recovered:?}"
    );
}
