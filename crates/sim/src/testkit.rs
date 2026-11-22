//! Builders the unit tests share: small untracked LTX files and WAL origins.

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{FileKey, Level, ObjectStore, Upload};
use lsr_ltx::{
    Encoder, Header, HeaderFields, PageNo, PageSize, Timestamp, Txid, TxidRange, WalOrigin,
};
use lsr_wal::{Salts, WalOffset};

pub(crate) const PAGE: u32 = 512;

pub(crate) fn ps() -> PageSize {
    PageSize::new(PAGE).unwrap()
}

pub(crate) fn txid(raw: u64) -> Txid {
    Txid::new(raw).unwrap()
}

pub(crate) fn fkey(level: u8, min: u64, max: u64) -> FileKey {
    FileKey::new(
        Level::new(level).unwrap(),
        TxidRange::new(txid(min), txid(max)).unwrap(),
    )
}

pub(crate) fn pgno(n: u32) -> PageNo {
    PageNo::new(n, ps()).unwrap()
}

pub(crate) const SALTS: Salts = Salts { salt1: 7, salt2: 9 };

/// The offset just past the `frames`th frame.
pub(crate) fn wal_end(frames: u32) -> WalOffset {
    WalOffset::from_frame_index(frames, ps())
}

/// The WAL range of frames `first..=last` (1-based) of the generation `SALTS`.
pub(crate) fn origin(first: u32, last: u32) -> WalOrigin {
    let start = wal_end(first.saturating_sub(1)).get();
    let end = wal_end(last).get();
    WalOrigin::new(start, end.saturating_sub(start), SALTS.salt1, SALTS.salt2).unwrap()
}

/// A file of `commit` pages, page 1 filled with `fill`.
pub(crate) fn ltx(min: u64, max: u64, commit: u32, fill: u8, wal: Option<WalOrigin>) -> Vec<u8> {
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps(),
        commit,
        txids: TxidRange::new(txid(min), txid(max)).unwrap(),
        timestamp: Timestamp::from_unix_millis(
            1_000_i64.saturating_add(i64::try_from(max).unwrap()),
        ),
        pre_apply: None,
        wal,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    encoder
        .write_page(pgno(1), &vec![fill; ps().bytes()])
        .unwrap();
    encoder.finish(None).unwrap().0
}

pub(crate) fn put<S: ObjectStore>(store: &S, level: u8, min: u64, max: u64, bytes: &[u8]) {
    block_on(async {
        let created =
            Timestamp::from_unix_millis(1_000_i64.saturating_add(i64::try_from(max).unwrap()));
        let mut up = store
            .begin_upload(&fkey(level, min, max), created, WriteMode::Replace)
            .await
            .unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap();
    });
}
