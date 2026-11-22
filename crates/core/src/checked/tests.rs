use std::io::ErrorKind;

use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::*;
use crate::conformance::block_on;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::mem::short::SizeLiar;
use crate::store::{PageLimit, StoreOp};

fn key() -> FileKey {
    let txid = Txid::new(1).unwrap();
    FileKey::new(Level::ZERO, TxidRange::new(txid, txid).unwrap())
}

fn put<S: ObjectStore>(store: &S, bytes: &[u8]) -> Result<FileInfo, StoreError> {
    block_on(async {
        let mut up = Checked::begin(store, &key(), Timestamp::from_unix_millis(1), WriteMode::Replace).await?;
        up.write(&bytes[..2]).await?;
        up.write(&bytes[2..]).await?;
        up.commit().await
    })
}

fn listed(store: &MemStore) -> usize {
    block_on(store.list(Level::ZERO, None, PageLimit::MAX))
        .unwrap()
        .files
        .len()
}

#[test]
fn a_commit_holding_what_was_written_passes_through() {
    let store = MemStore::new();
    assert_eq!(put(&store, b"abcd").unwrap().size, 4);
    assert_eq!(listed(&store), 1);
}

#[test]
fn a_commit_acknowledged_short_is_deleted_and_reported() {
    let store = SizeLiar(MemStore::new());
    assert_eq!(
        put(&store, b"abcd"),
        Err(StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 4,
                stored: 3,
                removed: true,
            },
        })
    );
    assert_eq!(listed(&store.0), 0);
}

#[test]
fn a_short_commit_whose_delete_fails_says_the_object_may_remain() {
    let store = SizeLiar(MemStore::new());
    store.0.inject_failure(
        StoreOp::Delete,
        StoreError::Transient {
            op: StoreOp::Delete,
            kind: ErrorKind::TimedOut,
        },
    );
    assert_eq!(
        put(&store, b"abcd"),
        Err(StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 4,
                stored: 3,
                removed: false,
            },
        })
    );
}
