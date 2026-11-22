//! Reachability: how often runs reached each state the generator and the
//! oracle exist to exercise.
//!
//! A random history passes or fails, and a pass says nothing about what it
//! did: a fault kind the generator never draws, a refusal the oracle never
//! judges and a compaction level never reached all pass. Each run counts
//! what it reached under a short name (`fault.remote.Delete.Error`,
//! `asof.inside`, `put.level.3`), and `just sim-tally` sums the counts over
//! many seeds, so a zero shows a state nothing reaches.
//!
//! A key nothing bumps never appears in the map, so [`REQUIRED`] and
//! [`WATCHED`] name the keys that must be printed even at zero. The required
//! ones are asserted: `tests/sim/tally.rs` fails when the fixed seed set
//! (seeds 1 to 64, the same histories `tests/sim/fixed_seeds.rs` runs) leaves
//! one at zero, so a generator change that stops reaching a state the oracle
//! or the planted-bug ledger depends on is a red test, not a missing line.

use std::collections::BTreeMap;

/// States the fixed seed set must reach, each with the check that needs it.
pub const REQUIRED: &[&str] = &[
    // The oracle's own judgments: a check that never runs passes vacuously.
    "check.runs",            // any restore was judged at all
    "check.txid.restored",   // RestoreMismatch, per transaction
    "check.txid.inside",     // a target inside a compacted file (ledger: inside-file-sole-base)
    "check.txid.below_base", // refusals below the retention base (RestoreRefused, WrongAnswer)
    "check.latest",          // LatestLostAck
    "check.lost",            // acknowledged transactions the replica legitimately lost (roll back)
    "check.asof.planned",    // AsOfTooNew, NotLatest (ledger: asof-straddle)
    "check.asof.inside",     // as-of inside a file's range (ledger: asof-straddle)
    "check.asof.refused",    // as-of refusals the planner's contract judges
    "check.stamps",          // WrongCreatedAt (ledger: time-provenance, time-provenance-sim)
    "check.deletes", // RetentionTooEager, DeletedOnOneRead (ledger: delete-on-one-failed-read-*)
    // Crashes: power loss is the only source of rewinds, torn staged files and kept unsynced
    // frames, the shapes the boot rules and the liveness oracle's map exist for.
    "crash.Process",
    "crash.Power",
    "power.kept_unsynced_wal",
    "power.kept_cut_frame",
    "staged.Torn", // a torn newest staged file at boot (ledger: boot-page-size-stale-staged)
    // Every snapshot cause the liveness oracle maps to its justifying faults; a cause no run
    // reaches is a row of that map nothing tests. CheckpointBoundary and OperatorRequested have
    // no event of their own and are counted when staged (`snapshot.staged.*`).
    "snapshot.FirstSync",
    "snapshot.BehindRemote",
    "snapshot.NewestStagedFileUnreadable",
    "snapshot.WalMissing",
    "snapshot.GenerationChanged",
    "snapshot.PrevFrameMismatch",
    "snapshot.LastPageMismatch",
    "snapshot.LocalRewound",
    // The liveness oracle's other two judgments: an operator's snapshot is
    // judged when staged, and every staged file given up is judged.
    "snapshot.staged.OperatorRequested",
    "liveness.given_up",
    // Another writer: OtherWriterMissed (ledger: maintenance-another-writer).
    "remote.second_writer",
    "stop.OtherWriter",
    // Compaction levels and retention deletes the planner and the retention oracle judge;
    // level 9 is the snapshot level.
    "put.level.1",
    "put.level.2",
    "put.level.3",
    "put.level.9",
    "delete.calls",
    // Retried faults the ledger's fixes handle: a staged file read again before it is
    // deleted (delete-on-one-failed-read-boot), an upload whose outcome is unknown
    // (relist-after-failed-drop, relist-acknowledged-mark), the WAL moving under a read
    // (sim-read-lock-retake).
    "retrying.StagedLost",
    "retrying.UploadAmbiguous",
    "retrying.WalMoved",
    "fault.remote.Upload.ErrorAfterPublish",
    "fault.remote.Upload.CorruptPublish",
    "fault.remote.Read.FlipByteOnRead", // ledger: verifying-object-written-over
    "fault.staging.Read.FlipByteOnRead",
    // WAL damage the oracle adopts as the database's new truth, one key per drawn shape
    // (RewriteSameLength is not drawn; see `generate.rs`).
    "wal.TearHeader",
    "wal.TruncateAtFrame",
    "wal.RegrowSameSalts",
    "wal.RegrowWithoutCommitMarks",
    "wal.FlipLastFrameBit",
    // Persistent faults: a corrupt WAL, and a page size change (ledger: page-size-after-vacuum).
    "degraded.WalCorrupt",
    "degraded.PageSizeChanged",
    "commit.shrinks", // a database that loses pages: the restore truncates
    "event.Clamped",  // a clock that went back (ledger: time-provenance)
];

/// States printed even at zero but not asserted: reached rarely or never by
/// the fixed seed set today, each a listed generator gap.
pub const WATCHED: &[&str] = &[
    "check.asof.fallback",                // an as-of with no file at or before it
    "check.damaged",                      // a restore stopped by a file a corrupt publish damaged
    "snapshot.staged.CheckpointBoundary", // the engine's own TRUNCATE checkpoint, never judged
    "staged.Dropped",                     // a staged file gone whole after a power loss
];

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

    /// Every count, by name, with each [`REQUIRED`] and [`WATCHED`] key
    /// present: a state nothing reached prints as a zero, not as a missing line.
    #[must_use]
    pub fn listing(&self) -> Vec<(String, u64)> {
        let mut all = self.0.clone();
        for key in REQUIRED.iter().chain(WATCHED) {
            all.entry((*key).to_owned()).or_insert(0);
        }
        all.into_iter().collect()
    }

    /// The [`REQUIRED`] keys this reach left at zero.
    #[must_use]
    pub fn missing(&self) -> Vec<&'static str> {
        REQUIRED
            .iter()
            .copied()
            .filter(|key| self.get(key) == 0)
            .collect()
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
    fn a_required_key_left_at_zero_is_missing_and_listed() {
        let mut reach = Reach::default();
        for key in REQUIRED.iter().skip(1) {
            reach.bump(*key);
        }
        assert_eq!(reach.missing(), REQUIRED.get(..1).unwrap_or_default());
        let listing = reach.listing();
        for key in REQUIRED.iter().chain(WATCHED) {
            assert!(
                listing.iter().any(|(name, _)| name == key),
                "{key} is not listed"
            );
        }
    }

    #[test]
    fn no_key_is_both_required_and_watched() {
        let mut all: Vec<&str> = REQUIRED.iter().chain(WATCHED).copied().collect();
        let before = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), before);
    }

    #[test]
    fn a_variant_is_named_without_its_fields() {
        assert_eq!(variant(&Some(3)), "Some");
        assert_eq!(variant(&lsr_core::StoreOp::Delete), "Delete");
    }
}
