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
//! "known wrong" row BOOT-32 in `docs/behavior.toml`. A level-0 file that spans several
//! transactions (Go may write one) starts after the listed level-0 files, so it is named
//! from there too. The keys are read in one batch, so a store that reads side by side asks
//! for them together.
//!
//! [`walk`] looks for these keys transaction by transaction past a listing, through a
//! transaction the caller knows was written, then until [`PROBE_GAP`] transactions in a row
//! are missing, never past [`PROBE_CAP`]. Its reach is the limit of what reads can correct:
//! a hole in the replica wider than the gap right after the listing's end (a bucket rolled
//! back while the writer went on above it) hides every newer file from a lagging listing.

use lsr_ltx::{Txid, TxidRange};

use crate::inventory::Inventory;
use crate::layout::{FileKey, Level};
use crate::store::{ObjectStore, ReadAt, ReadLen, StoreError};

/// The most transactions past a listing's newest that one [`walk`] looks for. A listing that
/// hides more uploads than this is far outside what an S3-compatible store does; past it,
/// the bucket's refusal of a taken key still stops a second upload under the same key.
pub const PROBE_CAP: u32 = 64;

/// The most missing transactions in a row a [`walk`] reads past before it stops. Keys past a
/// replica's newest are never taken, but one below it can be free: another writer's file an
/// operator removed, or a bucket rolled back, leaves a gap under the files uploaded after it.
pub const PROBE_GAP: u32 = 3;

/// The most keys [`holder`] names for one transaction: its level-0 file, a level-0 file from
/// the first transaction, a level-0 file from where the listed level 0 ends, two starts for
/// each of levels 1 to 8, and the snapshot.
pub const NAMED_KEYS_MAX: u64 = 3 + 1 + 2 * 8;

/// The most keys named for one transaction past a walk's gap window: its level-0 file, a
/// level-0 file from the first transaction and one from where the listed level 0 ends, and
/// the snapshot.
const NAMED_KEYS_PAST_GAP: u64 = 4;

/// The most reads one [`walk`] makes: every key for the transactions inside its gap window,
/// the level-0 and snapshot keys for the rest, up to [`PROBE_CAP`] transactions.
#[expect(
    clippy::as_conversions,
    reason = "u32 constants widen to u64 losslessly; `From` is not const"
)]
pub const WALK_READS_MAX: u64 = {
    let window = PROBE_GAP as u64 + 1;
    let rest = PROBE_CAP as u64 - window;
    window * NAMED_KEYS_MAX + rest * NAMED_KEYS_PAST_GAP
};

/// A probe asks only whether an object exists, so it reads one byte.
const PROBE_LEN: ReadLen = ReadLen::or_chunk(1);

/// Where a level 1 to 8 file that a listing does not show can start, as the listing implies:
/// for each such level, the transaction after its newest listed file and the start of the
/// first file of the level below above that file. A compaction written after the listing, or
/// hidden by one that lags, starts at one of these.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Starts(Vec<(Level, Txid)>);

impl Starts {
    /// These starts without the level 1 to 8 ones: what a walk names past its gap window.
    pub(crate) fn level_zero_only(&self) -> Self {
        Self(
            self.0
                .iter()
                .copied()
                .filter(|(level, _)| *level == Level::ZERO)
                .collect(),
        )
    }

    /// The starts `inventory` implies.
    #[must_use]
    pub fn of(inventory: &Inventory) -> Self {
        let mut starts = Vec::new();
        let level_zero = inventory.level(Level::ZERO).last();
        if let Some(after) = level_zero.and_then(|info| info.key.range().max().next()) {
            starts.push((Level::ZERO, after));
        }
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
            let key = FileKey::new(*level, range);
            if !keys.contains(&key) {
                keys.push(key);
            }
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

/// The keys reads find past a listing, ascending: each transaction from `first` is looked
/// for under the keys [`holder`] names, through `through` (a gap below it does not end the
/// walk), then until more than [`PROBE_GAP`] transactions in a row past the last one found
/// are missing, at most [`PROBE_CAP`] transactions. With `first_only` it stops at the first
/// key found, for a caller that only needs to know the listing is behind.
///
/// # Errors
/// The store's error for a read that fails other than as not found.
pub(crate) async fn walk<S: ObjectStore>(
    store: &S,
    first: Option<Txid>,
    through: Option<Txid>,
    starts: &Starts,
    first_only: bool,
) -> Result<Vec<FileKey>, StoreError> {
    let mut found = Vec::new();
    let level_zero_only = starts.level_zero_only();
    let (mut at, mut missed) = (first, 0_u32);
    for _ in 0..PROBE_CAP {
        let Some(txid) = at.filter(|txid| missed <= PROBE_GAP || Some(*txid) <= through) else {
            break;
        };
        // Past the gap window, only the level-0 and snapshot keys: a level 1 to 8 file starts
        // where the listing implies, so one that ends this far past it is a run long enough
        // to have left level-0 files the window finds first.
        let named = if missed <= PROBE_GAP {
            starts
        } else {
            &level_zero_only
        };
        match holder(store, txid, named).await? {
            Some(key) => {
                found.push(key);
                if first_only {
                    break;
                }
                missed = 0;
            }
            None => missed = missed.saturating_add(1),
        }
        at = txid.next();
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
