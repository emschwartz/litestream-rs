//! Reads that check what a listing says. A store may list behind its writes (an eventually
//! consistent index, a cache in front of a listing) while its reads are exact, so a listing's
//! absence of a key is not evidence and neither is its presence; a read's is. Every place that
//! decides from a listing (the uploader's boot and relist, maintenance, a restore of the newest
//! state) reads through here, so there is one rule for which keys can be named without a
//! listing and one cost for naming them.
//!
//! A file whose newest transaction is `t` can be named without a listing in three ways: its
//! level-0 file (`t-t`), a level-0 file that starts at the first transaction (`1-t`, a young
//! replica's start), and the snapshot that ends at it (level 9, `1-t`). A level 1 to 8 file
//! has a start no read can guess. The three are read in one batch, so a store that reads side
//! by side asks for them together.

use lsr_ltx::{Txid, TxidRange};

use crate::layout::{FileKey, Level};
use crate::store::{ObjectStore, ReadAt, ReadLen, StoreError};

/// A probe asks only whether an object exists, so it reads one byte.
const PROBE_LEN: ReadLen = ReadLen::or_chunk(1);

/// The keys a reader can name without a listing for a file whose newest transaction is
/// `txid`, the level-0 file first.
fn named(txid: Txid) -> Vec<FileKey> {
    let single = FileKey::new(Level::ZERO, TxidRange::single(txid));
    let from_start = FileKey::new(Level::ZERO, TxidRange::from_start(txid));
    let mut keys = vec![single];
    if from_start != single {
        keys.push(from_start);
    }
    keys.push(FileKey::snapshot(txid));
    keys
}

/// The first of the keys named for `txid` that a read finds, if any.
///
/// # Errors
/// The store's error for a read that fails other than as not found.
pub(crate) async fn holder<S: ObjectStore>(
    store: &S,
    txid: Txid,
) -> Result<Option<FileKey>, StoreError> {
    let reads: Vec<ReadAt> = named(txid)
        .into_iter()
        .map(|key| ReadAt {
            key,
            offset: 0,
            len: PROBE_LEN,
        })
        .collect();
    let answers = store.read_many(&reads).await;
    let mut found = None;
    for (at, read) in reads.iter().zip(answers) {
        match read {
            Ok(_) => {
                found = found.or(Some(at.key));
            }
            Err(StoreError::NotFound { .. }) => {}
            Err(err) => return Err(err),
        }
    }
    Ok(found)
}

/// How many reads [`holder`] makes for `txid`: three, two for the first transaction.
#[must_use]
pub fn reads_per_transaction(txid: Txid) -> u64 {
    u64::try_from(named(txid).len()).unwrap_or(u64::MAX)
}

/// Whether the store holds an object under `key`, by a read.
///
/// # Errors
/// The store's error for a read that fails other than as not found.
pub(crate) async fn exists<S: ObjectStore>(store: &S, key: &FileKey) -> Result<bool, StoreError> {
    match store.read(key, 0, PROBE_LEN).await {
        Ok(_) => Ok(true),
        Err(StoreError::NotFound { .. }) => Ok(false),
        Err(err) => Err(err),
    }
}
