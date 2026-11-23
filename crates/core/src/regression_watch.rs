//! Seeing a replica lose files while the engine runs.
//!
//! In steady state the uploader makes no listing: each upload is the confirmed
//! transaction plus one. So a bucket rolled back to an older version, or files
//! deleted by someone else, went unseen unless a commit failed and the uploader
//! listed: the engine kept uploading above a hole, and `Latest` refused with a
//! gap until the next snapshot, with no signal.
//!
//! A [`RegressionWatch`] names objects this process is sure stand on the replica: one
//! anchor from the last maintenance listing (the last file of a level whose
//! range holds the published transaction), every level-0 file an upload
//! confirmed since, and the last snapshot this process wrote since. Nothing this engine does deletes any of them before the
//! next maintenance listing: deletes run only on the job planned from a
//! listing, in that listing's tick, retention never deletes a level's last
//! file, and a read-back repair that deletes the anchor forgets it
//! ([`RegressionWatch::forget`]). So the next listing must show them all.
//!
//! One that is missing ([`RegressionWatch::missing`]) is read, because a listing may lag
//! the bucket and a read may not: `NotFound` is a regression
//! (`Fault::RemoteRegressed`, whose configured response relists, or stops);
//! a read that finds it says the listing lags, and nothing is decided: maintenance plans
//! nothing from that listing and lists again later. A snapshot the listing hides would
//! otherwise be planned and written again under its own key with other bytes. A
//! rollback to any earlier version removes every object written after that
//! version, so it removes the anchor (written before the last listing) or
//! the uploads after it, whichever it reaches.
//!
//! What it does not see: a loss between a boot and the first maintenance
//! listing that removes only what the boot listed (the boot has no anchor),
//! and anything while the engine is down. Both need a record that outlives
//! the process. The anchor is that record's seam: the engine journal
//! (`docs/notes/2026-11-22/journal-design-v4.md` on the analysis branch) is
//! designed to keep a floor whose anchor names the confirmed bytes, and a boot
//! that reads it can run the same check.

use lsr_ltx::{Txid, TxidRange};

use crate::inventory::Inventory;
use crate::layout::{FileKey, Level};

/// Objects this process is sure stand on the replica until its next
/// maintenance listing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RegressionWatch {
    /// The last file of a level whose range held the published transaction
    /// at the last maintenance listing.
    anchor: Option<FileKey>,
    /// The level-0 transactions uploads confirmed since, first and last:
    /// one key each, consecutive, as the uploader puts them.
    uploaded: Option<(Txid, Txid)>,
    /// The last snapshot maintenance wrote since the last listing.
    made: Option<FileKey>,
}

impl RegressionWatch {
    /// What a maintenance listing that passed [`RegressionWatch::missing`] lets the
    /// engine watch until the next one: the first level, from 0 up, whose
    /// last file holds `published`. `None` with nothing to watch.
    pub(crate) fn after_listing(inventory: &Inventory, published: Option<Txid>) -> Option<Self> {
        let published = published?;
        let anchor = Level::ALL.iter().find_map(|level| {
            let last = inventory.level(*level).last()?;
            let range = last.key.range();
            (range.min() <= published && published <= range.max()).then_some(last.key)
        })?;
        Some(Self {
            anchor: Some(anchor),
            uploaded: None,
            made: None,
        })
    }

    /// Adds the snapshot maintenance wrote: nothing deletes the newest snapshot before
    /// the next listing shows it, so that listing must.
    pub(crate) fn made(watch: Option<Self>, key: FileKey) -> Option<Self> {
        let (anchor, uploaded) = watch.map_or((None, None), |watch| (watch.anchor, watch.uploaded));
        Some(Self {
            anchor,
            uploaded,
            made: Some(key),
        })
    }

    /// Adds a level-0 file an upload confirmed. A key that does not follow the
    /// last one starts the run of uploads again from it, dropping the older
    /// run: watching less is never wrong.
    pub(crate) fn put(watch: Option<Self>, key: FileKey) -> Option<Self> {
        let range = key.range();
        if key.level() != Level::ZERO || range.min() != range.max() {
            return watch;
        }
        let txid = range.max();
        let anchor = watch.and_then(|watch| watch.anchor);
        let made = watch.and_then(|watch| watch.made);
        let uploaded = match watch.and_then(|watch| watch.uploaded) {
            Some((first, last)) if last.next() == Some(txid) => (first, txid),
            Some(_) | None => (txid, txid),
        };
        Some(Self {
            anchor,
            uploaded: Some(uploaded),
            made,
        })
    }

    /// Stops watching `key`, which this engine itself deleted.
    pub(crate) fn forget(watch: Option<Self>, key: FileKey) -> Option<Self> {
        let watch = watch?;
        let anchor = watch.anchor.filter(|anchor| *anchor != key);
        let uploaded = watch.uploaded.filter(|(first, last)| {
            let range = key.range();
            key.level() != Level::ZERO
                || range.min() != range.max()
                || range.max() < *first
                || range.max() > *last
        });
        let made = watch.made.filter(|made| *made != key);
        (anchor.is_some() || uploaded.is_some() || made.is_some()).then_some(Self {
            anchor,
            uploaded,
            made,
        })
    }

    /// The first watched object `inventory` does not list, anchor first.
    pub(crate) fn missing(&self, inventory: &Inventory) -> Option<FileKey> {
        let listed = |key: &FileKey| {
            inventory
                .level(key.level())
                .binary_search_by_key(key, |info| info.key)
                .is_ok()
        };
        if let Some(anchor) = self.anchor.filter(|anchor| !listed(anchor)) {
            return Some(anchor);
        }
        if let Some(made) = self.made.filter(|made| !listed(made)) {
            return Some(made);
        }
        let (first, last) = self.uploaded?;
        let mut txid = first;
        loop {
            let key = FileKey::new(Level::ZERO, TxidRange::single(txid));
            if !listed(&key) {
                return Some(key);
            }
            if txid >= last {
                return None;
            }
            txid = txid.next()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::layout::{FileInfo, ListedTime};

    fn txid(n: u64) -> Txid {
        Txid::new(n).unwrap()
    }

    fn key(level: u8, min: u64, max: u64) -> FileKey {
        FileKey::new(
            Level::new(level).unwrap(),
            TxidRange::new(txid(min), txid(max)).unwrap(),
        )
    }

    fn inventory(keys: &[FileKey]) -> Inventory {
        let files = keys.iter().map(|key| FileInfo {
            key: *key,
            size: 100,
            listed: ListedTime::LastModified(lsr_ltx::Timestamp::from_unix_millis(1)),
        });
        Inventory::from_files(files).unwrap()
    }

    /// Each clause of `forget`'s keep rule alone keeps the run of uploads, and each bound
    /// is exact: only a single-transaction level-0 key from `first` to `last` drops it.
    #[test]
    fn forget_drops_the_uploads_only_for_one_of_their_own_keys() {
        let watch = RegressionWatch {
            anchor: None,
            uploaded: Some((txid(3), txid(7))),
            made: None,
        };
        let cases = [
            (key(1, 5, 5), true, "another level"),
            (key(0, 4, 5), true, "a range of transactions"),
            (key(0, 2, 2), true, "below the first"),
            (key(0, 8, 8), true, "above the last"),
            (key(0, 3, 3), false, "the first"),
            (key(0, 7, 7), false, "the last"),
            (key(0, 5, 5), false, "inside"),
        ];
        for (deleted, kept, why) in cases {
            let left = RegressionWatch::forget(Some(watch), deleted);
            assert_eq!(left, kept.then_some(watch), "{why}");
        }
    }

    #[test]
    fn the_anchor_is_the_lowest_level_whose_last_file_holds_the_published_transaction() {
        let listed = inventory(&[key(0, 3, 3), key(0, 4, 4), key(1, 1, 4), key(9, 1, 2)]);
        let watch = RegressionWatch::after_listing(&listed, Some(txid(4))).unwrap();
        assert_eq!(watch.anchor, Some(key(0, 4, 4)));
        let compacted = inventory(&[key(1, 1, 4), key(9, 1, 2)]);
        let watch = RegressionWatch::after_listing(&compacted, Some(txid(4))).unwrap();
        assert_eq!(watch.anchor, Some(key(1, 1, 4)));
        assert_eq!(
            RegressionWatch::after_listing(&compacted, Some(txid(5))),
            None
        );
        assert_eq!(RegressionWatch::after_listing(&compacted, None), None);
    }

    /// A snapshot maintenance wrote is watched until a listing shows it: a listing that
    /// hides it names it missing, and forgetting it (the engine deleted it) stops the watch.
    #[test]
    fn a_snapshot_written_since_the_listing_is_missing_until_listed() {
        let before = inventory(&[key(0, 3, 3), key(1, 1, 3)]);
        let watch = RegressionWatch::after_listing(&before, Some(txid(3)));
        let snapshot = key(9, 1, 3);
        let watch = RegressionWatch::made(watch, snapshot).unwrap();
        assert_eq!(watch.missing(&before), Some(snapshot));
        let after = inventory(&[key(0, 3, 3), key(1, 1, 3), snapshot]);
        assert_eq!(watch.missing(&after), None);
        let alone = RegressionWatch::made(None, snapshot);
        assert_eq!(RegressionWatch::forget(alone, snapshot), None);
    }

    #[test]
    fn a_rolled_back_anchor_or_upload_is_missing() {
        let before = inventory(&[key(0, 3, 3), key(1, 1, 3)]);
        let mut watch = RegressionWatch::after_listing(&before, Some(txid(3)));
        for n in 4..=6 {
            watch = RegressionWatch::put(watch, key(0, n, n));
        }
        let watch = watch.unwrap();
        let all = inventory(&[key(0, 3, 3), key(0, 4, 4), key(0, 5, 5), key(0, 6, 6)]);
        assert_eq!(watch.missing(&all), None);
        // Rolled back to before the last listing: the anchor went.
        let rolled = inventory(&[key(0, 4, 4), key(0, 5, 5), key(0, 6, 6)]);
        assert_eq!(watch.missing(&rolled), Some(key(0, 3, 3)));
        // Rolled back after it, then one more upload above the hole.
        let hole = inventory(&[key(0, 3, 3), key(0, 4, 4), key(0, 6, 6)]);
        assert_eq!(watch.missing(&hole), Some(key(0, 5, 5)));
    }

    #[test]
    fn a_key_out_of_sequence_starts_the_run_again_and_others_are_ignored() {
        let watch = RegressionWatch::put(None, key(0, 4, 4));
        let watch = RegressionWatch::put(watch, key(0, 9, 9));
        assert_eq!(watch.and_then(|w| w.uploaded), Some((txid(9), txid(9))));
        assert_eq!(RegressionWatch::put(None, key(1, 4, 4)), None);
        assert_eq!(RegressionWatch::put(None, key(0, 1, 4)), None);
    }

    #[test]
    fn a_key_this_engine_deleted_is_forgotten() {
        let listed = inventory(&[key(1, 1, 4)]);
        let watch = RegressionWatch::after_listing(&listed, Some(txid(4)));
        assert_eq!(RegressionWatch::forget(watch, key(1, 1, 4)), None);
        let watch = RegressionWatch::put(watch, key(0, 5, 5));
        let watch = RegressionWatch::forget(watch, key(0, 5, 5));
        assert_eq!(
            watch.map(|w| (w.anchor, w.uploaded)),
            Some((Some(key(1, 1, 4)), None))
        );
        assert_eq!(RegressionWatch::forget(None, key(0, 5, 5)), None);
    }

    proptest! {
        /// Whatever subset of the watched objects a listing drops, the first
        /// one dropped is the one reported, and a listing that keeps them all
        /// reports none.
        #[test]
        fn missing_names_exactly_the_first_dropped_object(
            anchor_max in 1u64..50,
            uploads in 0u64..40,
            dropped in proptest::collection::vec(any::<bool>(), 41),
        ) {
            let anchor = key(0, anchor_max, anchor_max);
            let mut watch = RegressionWatch::after_listing(&inventory(&[anchor]), Some(txid(anchor_max)));
            let mut watched = vec![anchor];
            for n in 1..=uploads {
                let at = anchor_max.saturating_add(n);
                let k = key(0, at, at);
                watch = RegressionWatch::put(watch, k);
                watched.push(k);
            }
            let kept: Vec<FileKey> = watched
                .iter()
                .zip(&dropped)
                .filter(|(_, drop)| !**drop)
                .map(|(k, _)| *k)
                .collect();
            let first_dropped = watched.iter().zip(&dropped).find(|(_, drop)| **drop).map(|(k, _)| *k);
            prop_assert_eq!(watch.unwrap().missing(&inventory(&kept)), first_dropped);
        }
    }
}
