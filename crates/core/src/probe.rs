//! Reads that check what a listing says. A store may list behind its writes (an eventually
//! consistent index, a cache in front of a listing) while its reads are exact, so a listing's
//! absence of a key is not evidence and neither is its presence; a read's is. Every place that
//! decides from a listing (the uploader's boot and relist, maintenance, a restore of the newest
//! state) reads through here, so there is one rule for which keys can be named without a
//! listing and one cost for naming them.
//!
//! A file whose newest transaction is `t` can be named without a listing in these ways: its
//! level-0 file (`t-t`), a level-0 file that starts at the first transaction (`1-t`, a young
//! replica's start), the snapshot that ends at it (level 9, `1-t`), and a level 1 to 8 file
//! at one of the starts the listing implies ([`Starts`]). A compaction into level `k` takes
//! the run of level `k - 1` files above level `k`'s newest file, so its output starts at the
//! first of those files, which is the transaction after level `k`'s newest file unless the
//! level below has a hole there. Both are named. A listing that hides level `k`'s newest
//! file as well hides the start, so a file above it stays unnamed: that residue is the
//! "known wrong" row BOOT-25 in `docs/BEHAVIOR.md`. The keys are read in one batch, so a
//! store that reads side by side asks for them together.

use lsr_ltx::{Txid, TxidRange};

use crate::inventory::Inventory;
use crate::layout::{FileKey, Level};
use crate::store::{ObjectStore, ReadAt, ReadLen, StoreError};

/// A probe asks only whether an object exists, so it reads one byte.
const PROBE_LEN: ReadLen = ReadLen::or_chunk(1);

/// Where a level 1 to 8 file that a listing does not show can start, as the listing implies:
/// for each such level, the transaction after its newest listed file and the start of the
/// first file of the level below above that file. A compaction written after the listing, or
/// hidden by one that lags, starts at one of these.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Starts(Vec<(Level, Txid)>);

impl Starts {
    /// The starts `inventory` implies.
    #[must_use]
    pub fn of(inventory: &Inventory) -> Self {
        let mut starts = Vec::new();
        for (below, level) in Level::ALL.iter().zip(Level::ALL.iter().skip(1)) {
            if level.is_snapshot() {
                break;
            }
            let newest = inventory
                .level(*level)
                .last()
                .map(|info| info.key.range().max());
            let after = newest.and_then(Txid::next);
            let run = inventory
                .level(*below)
                .iter()
                .map(|info| info.key.range().min())
                .find(|min| newest.is_none_or(|max| *min > max));
            for start in [after, run].into_iter().flatten() {
                if !starts.contains(&(*level, start)) {
                    starts.push((*level, start));
                }
            }
        }
        Self(starts)
    }
}

/// The keys a reader can name without a listing for a file whose newest transaction is
/// `txid`, the level-0 file first and the snapshot last.
fn named(txid: Txid, starts: &Starts) -> Vec<FileKey> {
    let single = FileKey::new(Level::ZERO, TxidRange::single(txid));
    let from_start = FileKey::new(Level::ZERO, TxidRange::from_start(txid));
    let mut keys = vec![single];
    if from_start != single {
        keys.push(from_start);
    }
    for (level, start) in &starts.0 {
        if let Some(range) = TxidRange::new(*start, txid) {
            keys.push(FileKey::new(*level, range));
        }
    }
    keys.push(FileKey::snapshot(txid));
    keys
}

/// Whether the probe of `key`'s newest transaction, past a listing that implies `starts`,
/// reads `key`: the contract with compaction that the maintenance planner's property test
/// checks for every job it plans.
#[cfg(test)]
pub(crate) fn names(starts: &Starts, key: &FileKey) -> bool {
    named(key.range().max(), starts).contains(key)
}

/// The first of the keys named for `txid` that a read finds, if any.
///
/// # Errors
/// The store's error for a read that fails other than as not found.
pub(crate) async fn holder<S: ObjectStore>(
    store: &S,
    txid: Txid,
    starts: &Starts,
) -> Result<Option<FileKey>, StoreError> {
    let reads: Vec<ReadAt> = named(txid, starts)
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

/// How many reads [`holder`] makes for `txid`: three (two for the first transaction), and
/// one for each of `starts` at or below `txid`.
#[must_use]
pub fn reads_per_transaction(txid: Txid, starts: &Starts) -> u64 {
    u64::try_from(named(txid, starts).len()).unwrap_or(u64::MAX)
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
