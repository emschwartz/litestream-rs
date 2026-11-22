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
//!   were rewritten with too, from inputs no larger than their largest. A
//!   key ending at a transaction a bucket rollback took is left out at every
//!   level: the history rebuilt past the loss re-merges what the lost files
//!   held under other keys (`1-1` after `1-2` was rolled back), which is the
//!   cost of the loss, not amplification. Such a key still counts when a
//!   counted file one level up was merged from it (level-1 `2-5` from level-0 `2`
//!   to `5` before a rollback took `2` to `4`): that file's bytes came from it.
//! - A compaction reads each input once, in chunks larger than any file the
//!   simulator writes (a level-1 compaction first reads each input's trailer
//!   from the replica to compare it with the staged copy, then the whole
//!   input from one of the two); a boot reads back the newest file of each level and
//!   the replica's newest level-0 file, a header first then the file, and
//!   its listing is followed by reads of the missing level-0 keys past it
//!   ([`PROBE_READS`]). So replica reads are at most two per compaction
//!   input, plus [`READS_PER_BOOT`] per boot, plus one per fault that failed
//!   a call (each such failure is retried) and [`PROBE_READS`] more for the
//!   listing that may follow it. A key a lag hid and a probe found is a
//!   listing the lag changed, counted as a fault hit.
//! - Each file needs one commit; a failed or ambiguous commit is retried. So
//!   replica commits are at most two per distinct key put plus one per
//!   fault hit and crash. A listing a lag changed counts as a fault hit: what
//!   it hid (a compaction output, say) may be written again once.

use std::collections::{BTreeMap, BTreeSet};

use lsr_core::FileKey;
use lsr_ltx::Txid;

use crate::oracle::{Bound, Violation, within};
use crate::store::{RecorderCounts, RecorderEvent};

/// The header, page index and trailer of one LTX file, with slack for the
/// compressor's framing.
pub const PER_FILE: u64 = 1024;

/// Reads one boot may make of the replica: a header and a body for the
/// newest file of each of the ten levels, and the upload lane's read-back.
pub const READS_PER_BOOT: u64 = 24 + PROBE_READS;

/// The reads that end a probe past a listing: two keys (the level-0 file and
/// the snapshot) for each missing transaction it reads past before it stops.
#[expect(
    clippy::as_conversions,
    reason = "a u32 widens to a u64 losslessly; `From` is not const"
)]
pub const PROBE_READS: u64 = 2 * (lsr_core::PROBE_GAP as u64 + 1);

/// What a run did besides its recorder's events.
#[derive(Clone, Copy, Debug, Default)]
pub struct Spent {
    /// Boots and crashes (each crash is followed by a boot), boots that
    /// listed the replica and failed (each is tried again), and changes that
    /// took the replica's newest file away (each costs reads past a listing
    /// and a relist, no more than a boot).
    pub boots: u64,
    /// Calls a scheduled fault failed or changed, and listings a lag changed.
    pub fault_hits: u64,
}

/// Judges the run's replica costs.
///
/// # Errors
/// The first [`Violation::ResourceBound`].
///
/// `foreign` is every file another writer put straight into the replica, with
/// its size: no recorder saw it, but the engine may compact it, so it counts
/// toward the bytes of its level.
pub fn check(
    events: &[RecorderEvent],
    counts: RecorderCounts,
    spent: Spent,
    lost: &BTreeSet<Txid>,
    foreign: &[(FileKey, u64)],
) -> Result<(), Violation> {
    let mut last: BTreeMap<FileKey, u64> = BTreeMap::new();
    for event in events {
        if let RecorderEvent::Put { key, size, .. } = event {
            let slot = last.entry(*key).or_insert(0);
            *slot = (*slot).max(*size);
        }
    }
    let mut sized = last.clone();
    for (key, size) in foreign {
        let slot = sized.entry(*key).or_insert(0);
        *slot = (*slot).max(*size);
    }
    level_bytes(&sized, lost)?;
    let inputs = compaction_inputs(&sized);
    let reads = inputs
        .saturating_mul(2)
        .saturating_add(spent.boots.saturating_add(1).saturating_mul(READS_PER_BOOT))
        .saturating_add(spent.fault_hits.saturating_mul(1 + PROBE_READS));
    within(Bound::ReadCalls, counts.reads, reads)?;
    let keys = u64::try_from(last.len()).unwrap_or(u64::MAX);
    let commits = keys
        .saturating_mul(2)
        .saturating_add(spent.fault_hits)
        .saturating_add(spent.boots);
    within(Bound::CommitCalls, counts.commits, commits)
}

/// Each compaction level holds at most the bytes of the level below plus one
/// header and trailer per output ([`PER_FILE`]), over the files in `sized`.
fn level_bytes(sized: &BTreeMap<FileKey, u64>, lost: &BTreeSet<Txid>) -> Result<(), Violation> {
    let mut bytes = [0_u64; 10];
    let mut files = [0_u64; 10];
    for (key, size) in sized.iter().filter(|(key, _)| counted(key, sized, lost)) {
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
    Ok(())
}

/// Whether `key` counts toward its level's bytes: it ends at a transaction no
/// rollback took, or a counted file one level up was merged from it (its
/// range lies inside that file's), so the bytes that file holds are charged
/// to the inputs it read.
fn counted(key: &FileKey, last: &BTreeMap<FileKey, u64>, lost: &BTreeSet<Txid>) -> bool {
    if !lost.contains(&key.range().max()) {
        return true;
    }
    let above = key.level().get().saturating_add(1);
    (1..=8).contains(&above)
        && last.keys().any(|output| {
            output.level().get() == above
                && output.range().min() <= key.range().min()
                && key.range().max() <= output.range().max()
                && counted(output, last, lost)
        })
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

#[cfg(test)]
mod tests {
    use lsr_core::Level;
    use lsr_ltx::{Timestamp, TxidRange};

    use super::*;

    fn key(level: u8, min: u64, max: u64) -> FileKey {
        let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
        FileKey::new(Level::new(level).unwrap(), range)
    }

    fn put(key: FileKey, size: u64) -> RecorderEvent {
        RecorderEvent::Put {
            key,
            header: None,
            origin: None,
            created_at: Timestamp::from_unix_millis(0),
            size,
            digest: 0,
            at: Timestamp::from_unix_millis(0),
            high: Timestamp::from_unix_millis(0),
        }
    }

    /// Another writer's file reaches level 0 without passing a recorder; a
    /// level 1 compacted from it is as large as it, not a level grown from
    /// nothing.
    #[test]
    fn another_writers_file_counts_toward_its_level() {
        let big = PER_FILE * 8;
        let events = [put(key(0, 1, 1), 100), put(key(1, 1, 2), big)];
        let lost = BTreeSet::new();
        let spent = Spent::default();
        let counts = RecorderCounts::default();
        assert!(check(&events, counts, spent, &lost, &[]).is_err());
        let foreign = [(key(0, 2, 2), big)];
        assert_eq!(check(&events, counts, spent, &lost, &foreign), Ok(()));
    }
}
