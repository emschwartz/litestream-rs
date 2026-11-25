use std::io::ErrorKind;

use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::*;
use crate::conformance::block_on;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::mem::short::{DigestLiar, SizeLiar};
use crate::store::{PageLimit, StoreOp};

fn key() -> FileKey {
    let txid = Txid::new(1).unwrap();
    FileKey::new(Level::ZERO, TxidRange::new(txid, txid).unwrap())
}

fn write<S: ObjectStore>(
    store: &S,
    bytes: &[u8],
    mode: WriteMode,
) -> Result<Result<Published, Unpublished>, StoreError> {
    block_on(async {
        let mut up =
            Writer::begin(store, &key(), Timestamp::from_unix_millis(1), mode, None).await?;
        up.write(&bytes[..2]).await?;
        up.write(&bytes[2..]).await?;
        Ok(up.commit(store).await)
    })
}

fn put<S: ObjectStore>(store: &S, bytes: &[u8]) -> Result<FileInfo, StoreError> {
    write(store, bytes, WriteMode::Replace)?
        .map(|published| published.info())
        .map_err(Unpublished::into_error)
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
fn a_create_only_commit_acknowledged_short_is_deleted_and_reported() {
    let store = SizeLiar(MemStore::new());
    let got = write(&store, b"abcd", WriteMode::New)
        .unwrap()
        .map_err(Unpublished::into_error);
    assert_eq!(
        got.map(|published| published.info()),
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

/// A replace may land on a key that held an acknowledged object: deleting what the short commit
/// left would make the replica end a transaction early with no error (seed 0x1dc). The object is
/// left, and the caller is told it was not removed.
#[test]
fn a_replace_acknowledged_short_is_reported_and_never_deleted() {
    let store = SizeLiar(MemStore::new());
    put(&store.0, b"abcd").unwrap();
    assert_eq!(
        put(&store, b"wxyz"),
        Err(StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::CommitSizeMismatch {
                wrote: 4,
                stored: 3,
                removed: false,
            },
        })
    );
    assert_eq!(
        listed(&store.0),
        1,
        "a replace's short commit deleted the key"
    );
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
    let got = write(&store, b"abcd", WriteMode::New)
        .unwrap()
        .map_err(Unpublished::into_error);
    assert_eq!(
        got.map(|published| published.info()),
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

#[test]
fn a_create_only_write_into_a_free_key_is_created_and_a_replace_replaced() {
    let store = MemStore::new();
    let created = write(&store, b"abcd", WriteMode::New).unwrap().unwrap();
    assert_eq!(created.how, HowPublished::Created);
    let replaced = write(&store, b"wxyz", WriteMode::Replace).unwrap().unwrap();
    assert_eq!(replaced.how, HowPublished::Replaced);
}

#[test]
fn a_create_only_write_into_a_taken_key_is_refused_and_leaves_the_object() {
    let store = MemStore::new();
    write(&store, b"abcd", WriteMode::New).unwrap().unwrap();
    let refused = write(&store, b"wxyz", WriteMode::New).unwrap();
    assert!(matches!(refused, Err(Unpublished::Taken(_))), "{refused:?}");
    assert_eq!(listed(&store), 1);
}

/// Only a rejected create-only write hands back its object, and removing it
/// takes that key alone.
#[test]
fn only_a_rejected_create_only_write_hands_back_its_object() {
    let store = DigestLiar(MemStore::new());
    let rejected = write(&store, b"abcd", WriteMode::New).unwrap();
    let Err(Unpublished::Rejected(_, Some(own))) = rejected else {
        panic!("{rejected:?}");
    };
    assert_eq!(own.0, key());
    block_on(remove_rejected(&store, own)).unwrap();
    assert_eq!(listed(&store.0), 0);

    let replace = write(&store, b"abcd", WriteMode::Replace).unwrap();
    assert!(
        matches!(replace, Err(Unpublished::Rejected(_, None))),
        "{replace:?}"
    );
    assert_eq!(listed(&store.0), 1);
}

/// A part the store reports wrong ended a multipart upload before anything was published:
/// the commit never asked for the key, so a create-only write has no object of its own.
#[test]
fn a_rejected_part_hands_back_no_object() {
    let part = StoreError::Corrupt {
        key: Some(key()),
        cause: CorruptCause::ChecksumMismatch {
            part: crate::PartNumber::new(2),
        },
    };
    for mode in [WriteMode::New, WriteMode::Replace] {
        let left = unpublished(part.clone(), key(), mode);
        assert!(
            matches!(left, Unpublished::Rejected(_, None)),
            "{mode:?}: {left:?}"
        );
    }
    let whole = StoreError::Corrupt {
        key: Some(key()),
        cause: CorruptCause::ChecksumMismatch { part: None },
    };
    let left = unpublished(whole, key(), WriteMode::New);
    assert!(
        matches!(left, Unpublished::Rejected(_, Some(_))),
        "{left:?}"
    );
}

#[test]
fn a_removal_deletes_the_keys_it_names() {
    let store = MemStore::new();
    write(&store, b"abcd", WriteMode::New).unwrap().unwrap();
    let keys = [key()];
    let removal = Removal::new(&keys, Why::Retention);
    assert_eq!(removal.why, Why::Retention);
    block_on(remove(&store, &removal)).unwrap();
    assert_eq!(listed(&store), 0);
}

#[test]
fn an_open_upload_prints_its_key_what_it_wrote_and_its_mode() {
    let store = MemStore::new();
    let printed = block_on(async {
        let at = Timestamp::from_unix_millis(1);
        let mut up = Writer::begin(&store, &key(), at, WriteMode::New, None)
            .await
            .unwrap();
        up.write(b"abcd").await.unwrap();
        format!("{up:?}")
    });
    assert!(printed.starts_with("Writer {"), "{printed}");
    assert!(printed.contains(&format!("key: {:?}", key())), "{printed}");
    assert!(printed.contains("written: 4"), "{printed}");
    assert!(printed.contains("mode: New"), "{printed}");
}
