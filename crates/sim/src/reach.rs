//! Reachability: how often runs reached each state the generator and the
//! oracle exist to exercise.
//!
//! A random history passes or fails, and a pass says nothing about what it
//! did: a fault kind the generator never draws, a refusal the oracle never
//! judges and a compaction level never reached all pass. Each run counts
//! what it reached under a short name (`fault.remote.Delete.Error`,
//! `asof.inside`, `put.level.3`), and `just sim-tally` sums the counts over
//! many seeds, so a zero shows a state nothing reaches.

use std::collections::BTreeMap;

/// Counts of the states one run, or many summed, reached.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reach(BTreeMap<String, u64>);

impl Reach {
    /// Counts `key` once.
    pub fn bump(&mut self, key: impl Into<String>) {
        self.add(key, 1);
    }

    /// Counts `key` `n` times.
    pub fn add(&mut self, key: impl Into<String>, n: u64) {
        let slot = self.0.entry(key.into()).or_insert(0);
        *slot = slot.saturating_add(n);
    }

    /// Adds every count of `other`.
    pub fn merge(&mut self, other: &Self) {
        for (key, n) in &other.0 {
            self.add(key.clone(), *n);
        }
    }

    /// How often `key` was reached.
    #[must_use]
    pub fn get(&self, key: &str) -> u64 {
        self.0.get(key).copied().unwrap_or(0)
    }

    /// Every count, by name.
    pub fn iter(&self) -> impl Iterator<Item = (&str, u64)> {
        self.0.iter().map(|(key, n)| (key.as_str(), *n))
    }
}

/// The name of an enum value's variant: its debug text up to the first
/// character that cannot be part of a name.
#[must_use]
pub fn variant(value: &impl std::fmt::Debug) -> String {
    format!("{value:?}")
        .chars()
        .take_while(char::is_ascii_alphanumeric)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_sum_and_merge() {
        let mut one = Reach::default();
        one.bump("a");
        one.add("b", 2);
        let mut two = Reach::default();
        two.bump("a");
        two.merge(&one);
        assert_eq!((two.get("a"), two.get("b"), two.get("c")), (2, 2, 0));
    }

    #[test]
    fn a_variant_is_named_without_its_fields() {
        assert_eq!(variant(&Some(3)), "Some");
        assert_eq!(variant(&lsr_core::StoreOp::Delete), "Delete");
    }
}
