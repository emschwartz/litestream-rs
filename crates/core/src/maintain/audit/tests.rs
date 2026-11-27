//! The header-and-tail check of the boot read-back, one case per way a file fails it.

use lsr_ltx::{
    Encoder, HEADER_LEN, Header, HeaderFields, INDEX_TAIL_LEN, PageNo, PageSize, Timestamp, Txid,
    TxidRange,
};

use super::*;
use crate::conformance::block_on;
use crate::fault::role::Replica;
use crate::mem::MemStore;
use crate::store::{ObjectStore, Upload, WriteMode};

fn range(min: u64, max: u64) -> TxidRange {
    TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap()
}

fn key(min: u64, max: u64) -> FileKey {
    FileKey::new(Level::new(1).unwrap(), range(min, max))
}

/// A file for transactions `1..=2` of `pages` pages.
fn file(pages: u32) -> Vec<u8> {
    let ps = PageSize::new(4096).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: ps,
        commit: pages,
        txids: range(1, 2),
        timestamp: Timestamp::from_unix_millis(1_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for n in 1..=pages {
        let page = vec![u8::try_from(n % 251).unwrap(); ps.bytes()];
        encoder
            .write_page(PageNo::new(n, ps).unwrap(), &page)
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// Whether `bytes` stored at `key` pass, as the listing shows them.
fn sound_at(key: FileKey, bytes: &[u8]) -> bool {
    let store = MemStore::new();
    block_on(async {
        let mut upload = store
            .begin_upload(&key, Timestamp::from_unix_millis(1_000), WriteMode::Replace)
            .await
            .unwrap();
        upload.write(bytes).await.unwrap();
        upload.commit().await.unwrap();
        let info = store.stored().into_iter().next().unwrap();
        ends_sound(&Replica(&store), &info).await.unwrap()
    })
}

fn sound(bytes: &[u8]) -> bool {
    sound_at(key(1, 2), bytes)
}

/// `bytes` with the index size field at the tail set to `size`.
fn index_size(mut bytes: Vec<u8>, size: u64) -> Vec<u8> {
    let at = bytes.len().saturating_sub(INDEX_TAIL_LEN);
    bytes[at..at.saturating_add(8)].copy_from_slice(&size.to_be_bytes());
    bytes
}

#[test]
fn the_reads_are_a_header_and_the_tail_past_the_index() {
    assert_eq!(HEADER_READ.get(), u64::try_from(HEADER_LEN).unwrap());
    assert_eq!(TAIL.get(), u64::try_from(INDEX_TAIL_LEN + 1).unwrap());
}

#[test]
fn a_whole_file_is_sound() {
    assert!(sound(&file(3)));
}

#[test]
fn a_header_that_does_not_decode_fails() {
    let mut bytes = file(3);
    bytes[..4].fill(0);
    assert!(!sound(&bytes));
}

#[test]
fn a_header_that_names_another_range_fails() {
    assert!(!sound_at(key(1, 3), &file(3)));
}

#[test]
fn a_file_too_short_for_a_header_and_a_tail_fails() {
    let bytes = file(3);
    assert!(!sound(&bytes[..HEADER_LEN + INDEX_TAIL_LEN]));
    assert!(!sound(&bytes[..HEADER_LEN]));
}

#[test]
fn a_file_cut_short_fails() {
    let bytes = file(3);
    assert!(!sound(&bytes[..bytes.len() - 1]));
    assert!(!sound(&bytes[..bytes.len() / 2]));
}

#[test]
fn a_tail_with_no_index_terminator_fails() {
    let mut bytes = file(3);
    let at = bytes.len() - INDEX_TAIL_LEN - 1;
    bytes[at] = 1;
    assert!(!sound(&bytes));
}

#[test]
fn an_index_size_that_is_zero_or_runs_into_the_header_fails() {
    let bytes = file(3);
    // The index may reach back to the header's end, and no further.
    let room = u64::try_from(bytes.len() - HEADER_LEN - INDEX_TAIL_LEN).unwrap();
    assert!(sound(&index_size(bytes.clone(), room)));
    assert!(!sound(&index_size(bytes.clone(), room + 1)));
    assert!(!sound(&index_size(bytes.clone(), 0)));
    assert!(!sound(&index_size(bytes, u64::MAX)));
}

#[test]
fn a_trailer_that_does_not_decode_fails() {
    let mut bytes = file(3);
    let at = bytes.len() - 8;
    bytes[at..].fill(0);
    assert!(!sound(&bytes));
}

#[test]
fn a_file_the_store_no_longer_holds_is_the_replicas_fault() {
    let store = MemStore::new();
    let info = FileInfo {
        key: key(1, 2),
        size: 1_000,
        listed: crate::layout::ListedTime::LastModified(Timestamp::from_unix_millis(1_000)),
    };
    block_on(ends_sound(&Replica(&store), &info)).unwrap_err();
}

#[test]
fn an_object_shorter_than_listed_fails() {
    let store = MemStore::new();
    let bytes = file(3);
    block_on(async {
        let mut upload = store
            .begin_upload(
                &key(1, 2),
                Timestamp::from_unix_millis(1_000),
                WriteMode::Replace,
            )
            .await
            .unwrap();
        upload.write(&bytes).await.unwrap();
        upload.commit().await.unwrap();
        let mut info = store.stored().into_iter().next().unwrap();
        info.size += 10;
        assert!(!ends_sound(&Replica(&store), &info).await.unwrap());
    });
}
