use std::io::ErrorKind;

use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::*;
use crate::conformance::block_on;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::mem::reread::Rereads;
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

/// A rejected write, in either mode, is reported as rejected and the commit deletes nothing:
/// the refusal says nothing about what the key holds (`docs/decisions/D280.md`).
#[test]
fn a_rejected_write_is_reported_and_deletes_nothing() {
    let store = DigestLiar(MemStore::new());
    for mode in [WriteMode::New, WriteMode::Replace] {
        let rejected = write(&store, b"abcd", mode).unwrap();
        assert!(
            matches!(rejected, Err(Unpublished::Rejected(_))),
            "{mode:?}: {rejected:?}"
        );
        assert_eq!(listed(&store.0), 1, "{mode:?}");
    }
    for part in [None, crate::PartNumber::new(2)] {
        let err = StoreError::Corrupt {
            key: Some(key()),
            cause: CorruptCause::ChecksumMismatch { part },
        };
        let left = unpublished(err, key(), WriteMode::New);
        assert!(matches!(left, Unpublished::Rejected(_)), "{left:?}");
    }
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
fn open_upload(store: &Rereads) -> Open {
    block_on(Writer::begin(
        store,
        &key(),
        Timestamp::from_unix_millis(1),
        WriteMode::Replace,
        None,
    ))
    .unwrap()
    .boxed()
}

/// A slice asks the upload's parts before it hands bytes over, and once more before it
/// publishes; `waits` says the last answer stopped it, and a patient slice never asks.
#[test]
fn a_handoff_asks_the_parts_unless_patient_and_notes_when_it_waits() {
    let store = Rereads::default();
    store.asked().flights = [
        Ok(Flight::Room),
        Ok(Flight::Full),
        Ok(Flight::Room),
        Ok(Flight::Idle),
    ]
    .into();
    let mut open = open_upload(&store);
    let mut handoff = Handoff::default();
    assert!(block_on(handoff.clear(&mut open, Rest::More, true)).unwrap());
    assert!(store.asked().rests.is_empty());
    assert!(block_on(handoff.clear(&mut open, Rest::More, false)).unwrap());
    assert!(!handoff.waits, "room for a write");
    assert!(!block_on(handoff.clear(&mut open, Rest::More, false)).unwrap());
    assert!(handoff.waits, "a whole part waits");
    handoff.waits = false;
    assert!(!block_on(handoff.clear(&mut open, Rest::None, false)).unwrap());
    assert!(handoff.waits, "a part is still out at the end");
    handoff.waits = false;
    assert!(block_on(handoff.clear(&mut open, Rest::None, false)).unwrap());
    assert!(!handoff.waits);
    assert_eq!(
        store.asked().rests,
        [Rest::More, Rest::More, Rest::None, Rest::None]
    );
}

/// A send hands over at most the room, drains what went, and answers the room left.
#[test]
fn a_handoff_sends_at_most_the_room_and_drains_what_went() {
    let store = Rereads::default();
    let mut open = open_upload(&store);
    let mut handoff = Handoff::default();
    let mut out = vec![1_u8; 10];
    let room = NonZeroU64::new(4).unwrap();
    assert_eq!(
        block_on(handoff.send(&mut open, &mut out, room)).unwrap(),
        None
    );
    assert_eq!((out.len(), handoff.sent), (6, 4));
    let room = NonZeroU64::new(9).unwrap();
    let left = block_on(handoff.send(&mut open, &mut out, room)).unwrap();
    assert_eq!(left, NonZeroU64::new(3));
    assert_eq!((out.len(), handoff.sent), (0, 10));
}
