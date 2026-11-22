//! Budgets over a whole run: what the replica cost, against what a correct
//! engine needs. None of these breaks a restore, so the data oracle cannot
//! see them, but each is real money on a bucket: a compaction that re-merges
//! a whole level each interval, a read per file per listing, a rewrite loop.
//!
//! The arithmetic, done once here:
//!
//! - Compaction merges each file of a level into the next level once: a run
//!   above the destination's newest file becomes one output, and an output
//!   holds each page of its inputs at most once (newest wins). So the bytes
//!   of the distinct keys put at level `k` (1 to 8) are at most those put at
//!   level `k - 1`, plus one header and trailer per output ([`PER_FILE`]).
//!   A key put twice counts once, at its largest: a retry after a crash
//!   rewrites the same output (the cost of a crash, not amplification), and
//!   a rewind or a rollback puts a key again over a history the levels below
//!   were rewritten with too, from inputs no larger than their largest.
//! - A compaction reads each input once, in chunks larger than any file the
//!   simulator writes; a boot reads back the newest file of each level and
//!   the replica's newest level-0 file, a header first then the file. So
//!   replica reads are at most two per compaction input, plus
//!   [`READS_PER_BOOT`] per boot, plus one per fault that failed a call
//!   (each such failure is retried).
//! - Each file needs one commit; a failed or ambiguous commit is retried. So
//!   replica commits are at most two per distinct key put plus one per
//!   fault hit and crash.

use std::collections::BTreeMap;

use lsr_core::FileKey;

use crate::oracle::{Bound, Violation, within};
use crate::store::{RecorderCounts, RecorderEvent};

/// The header, page index and trailer of one LTX file, with slack for the
/// compressor's framing.
pub const PER_FILE: u64 = 1024;

/// Reads one boot may make of the replica: a header and a body for the
/// newest file of each of the ten levels, and the upload lane's read-back.
pub const READS_PER_BOOT: u64 = 24;

/// What a run did besides its recorder's events.
#[derive(Clone, Copy, Debug, Default)]
pub struct Spent {
    /// Boots and crashes (each crash is followed by a boot).
    pub boots: u64,
    /// Calls a scheduled fault failed or changed.
    pub fault_hits: u64,
}

/// Judges the run's replica costs.
///
/// # Errors
/// The first [`Violation::ResourceBound`].
pub fn check(
    events: &[RecorderEvent],
    counts: RecorderCounts,
    spent: Spent,
) -> Result<(), Violation> {
    let mut last: BTreeMap<FileKey, u64> = BTreeMap::new();
    for event in events {
        if let RecorderEvent::Put { key, size, .. } = event {
            let slot = last.entry(*key).or_insert(0);
            *slot = (*slot).max(*size);
        }
    }
    let mut bytes = [0_u64; 10];
    let mut files = [0_u64; 10];
    for (key, size) in &last {
        let level = usize::from(key.level().get());
        if let (Some(b), Some(f)) = (bytes.get_mut(level), files.get_mut(level)) {
            *b = b.saturating_add(*size);
            *f = f.saturating_add(1);
        }
    }
    for level in 1..=8_usize {
        let below = bytes.get(level.saturating_sub(1)).copied().unwrap_or(0);
        let outputs = files.get(level).copied().unwrap_or(0);
        let limit = below.saturating_add(outputs.saturating_mul(PER_FILE));
        within(
            Bound::LevelBytes,
            bytes.get(level).copied().unwrap_or(0),
            limit,
        )?;
    }
    let inputs = compaction_inputs(&last);
    let reads = inputs
        .saturating_mul(2)
        .saturating_add(spent.boots.saturating_add(1).saturating_mul(READS_PER_BOOT))
        .saturating_add(spent.fault_hits);
    within(Bound::ReadCalls, counts.reads, reads)?;
    let keys = u64::try_from(last.len()).unwrap_or(u64::MAX);
    let commits = keys
        .saturating_mul(2)
        .saturating_add(spent.fault_hits)
        .saturating_add(spent.boots);
    within(Bound::CommitCalls, counts.commits, commits)
}

/// For every key put at levels 1 to 8, the keys one level down that lie
/// inside its range: the inputs it could have been merged from.
fn compaction_inputs(last: &BTreeMap<FileKey, u64>) -> u64 {
    let mut inputs = 0_u64;
    for output in last.keys().filter(|k| (1..=8).contains(&k.level().get())) {
        let below = output.level().get().saturating_sub(1);
        let range = output.range();
        let within_range = last
            .keys()
            .filter(|k| k.level().get() == below)
            .filter(|k| range.min() <= k.range().min() && k.range().max() <= range.max())
            .count();
        inputs = inputs.saturating_add(u64::try_from(within_range).unwrap_or(u64::MAX));
    }
    inputs
}
