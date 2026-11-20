//! A small model of Go's restore chain, used only to name the hole in the mid-chain-gap
//! fixture. TEMPORARY: the planner of `lsr-core` (task 4) replaces it, and its brute-force
//! model tests supersede the tests here.
//!
//! The model: the newest snapshot at or below the target, then repeatedly the file that starts
//! at or before the next TXID and reaches furthest, within the target.

use crate::ltxkey::LocalKey;

/// What the chain reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
    /// No snapshot at or below the target.
    NoSnapshot,
    /// Contiguous to this TXID with nothing beyond it.
    Complete {
        /// Last TXID reached.
        through: u64,
    },
    /// Contiguous to `have_through` and a file exists that starts beyond `have_through + 1`.
    Gap {
        /// Last TXID reached contiguously.
        have_through: u64,
        /// Lowest start of a file beyond the hole.
        next_starts: u64,
    },
}

/// The chain for a target TXID (`None` is the latest).
pub fn chain(keys: &[LocalKey], target: Option<u64>) -> Chain {
    let limit = target.unwrap_or(u64::MAX);
    let snapshot = keys
        .iter()
        .filter(|k| k.is_snapshot() && k.max <= limit)
        .max_by_key(|k| k.max);
    let Some(snapshot) = snapshot else {
        return Chain::NoSnapshot;
    };
    let files: Vec<&LocalKey> = keys
        .iter()
        .filter(|k| !k.is_snapshot() && k.max <= limit)
        .collect();
    let mut cur = snapshot.max;
    while let Some(next) = files
        .iter()
        .filter(|k| k.min <= cur.saturating_add(1) && k.max > cur)
        .max_by_key(|k| k.max)
    {
        cur = next.max;
    }
    match files
        .iter()
        .filter(|k| k.min > cur.saturating_add(1))
        .map(|k| k.min)
        .min()
    {
        Some(next_starts) => Chain::Gap {
            have_through: cur,
            next_starts,
        },
        None => Chain::Complete { through: cur },
    }
}

/// A TXID whose covering files can be deleted to leave a hole strictly inside the chain of the
/// newest snapshot: some progress before the hole, at least two TXIDs of history after it.
pub fn find_hole(keys: &[LocalKey]) -> Option<u64> {
    let newest = keys
        .iter()
        .filter(|k| k.is_snapshot())
        .map(|k| k.max)
        .max()?;
    let head = keys.iter().map(|k| k.max).max()?;
    (newest.saturating_add(2)..head).find(|&h| {
        let remaining = without(keys, h);
        matches!(
            chain(&remaining, None),
            Chain::Gap { have_through, next_starts }
                if have_through > newest && head.saturating_sub(next_starts) >= 2
        )
    })
}

/// The keys with every non-snapshot file covering `txid` removed.
pub fn without(keys: &[LocalKey], txid: u64) -> Vec<LocalKey> {
    keys.iter()
        .copied()
        .filter(|k| k.is_snapshot() || !(k.min <= txid && txid <= k.max))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(level: u8, min: u64, max: u64) -> LocalKey {
        LocalKey { level, min, max }
    }

    #[test]
    fn contiguous_chain_is_complete() {
        let keys = [k(9, 1, 4), k(0, 5, 5), k(0, 6, 6)];
        assert_eq!(chain(&keys, None), Chain::Complete { through: 6 });
    }

    #[test]
    fn a_hole_is_named_by_both_sides() {
        let keys = [k(9, 1, 4), k(0, 5, 5), k(0, 7, 7), k(0, 8, 8)];
        assert_eq!(
            chain(&keys, None),
            Chain::Gap {
                have_through: 5,
                next_starts: 7
            }
        );
        assert_eq!(
            chain(&keys, Some(8)),
            Chain::Gap {
                have_through: 5,
                next_starts: 7
            }
        );
        assert_eq!(chain(&keys, Some(5)), Chain::Complete { through: 5 });
    }

    #[test]
    fn no_snapshot_below_the_target() {
        assert_eq!(
            chain(&[k(9, 5, 9), k(0, 10, 10)], Some(3)),
            Chain::NoSnapshot
        );
    }

    #[test]
    fn a_higher_level_file_bridges_overlapping_ranges() {
        let keys = [k(9, 1, 3), k(1, 2, 6), k(0, 4, 4), k(0, 7, 7)];
        assert_eq!(chain(&keys, None), Chain::Complete { through: 7 });
    }

    #[test]
    fn finds_a_hole_with_room_on_both_sides() {
        let keys = [
            k(9, 1, 3),
            k(0, 4, 4),
            k(0, 5, 5),
            k(0, 6, 6),
            k(0, 7, 7),
            k(0, 8, 8),
        ];
        let hole = find_hole(&keys).expect("a hole exists");
        assert!(matches!(
            chain(&without(&keys, hole), None),
            Chain::Gap { .. }
        ));
    }

    #[test]
    fn no_hole_without_history_after_the_snapshot() {
        assert_eq!(find_hole(&[k(9, 1, 3), k(0, 4, 4)]), None);
    }
}
