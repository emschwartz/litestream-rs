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
//!   the replica's newest level-0 file, a header first then the file. So replica reads
//!   are at most two per compaction input, plus [`READS_PER_BOOT`] per boot, plus one per
//!   fault that failed a call (each such failure is retried), plus the restore a boot with an
//!   empty staging directory may make to compare the database with the replica
//!   ([`Spent::restores`]), plus
//!   [`READS_PER_DEGRADED_RETRY`] per retry of a degraded task (a retry of a persistent
//!   cause such as a page size change compares the replica's newest file again; the
//!   doubling wait bounds how often).
//! - Each file needs one commit; a failed or ambiguous commit is retried. So
//!   replica commits are at most two per distinct key put plus one per
//!   fault hit and crash.

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
pub const READS_PER_BOOT: u64 = 24;

/// Reads one retry of a degraded task may make of the replica: a header and
/// the newest file it names.
pub const READS_PER_DEGRADED_RETRY: u64 = 2;

/// Reads of one replica file by a restore of the replica's state: a header and the body.
pub const READS_PER_RESTORED_FILE: u64 = 2;

/// What a run did besides its recorder's events.
#[derive(Clone, Copy, Debug, Default)]
pub struct Spent {
    /// Boots and crashes (each crash is followed by a boot), boots that
    /// listed the replica and failed (each is tried again), and changes that
    /// took the replica's newest file away (each costs a relist, no more than a boot).
    pub boots: u64,
    /// Calls a scheduled fault failed or changed.
    pub fault_hits: u64,
    /// Ticks that reported a task degraded: each is one retry.
    pub degraded_retries: u64,
    /// The replica files a restore of its whole state could read, summed over every tick that
    /// began unbooted with nothing staged. Such a boot may compare the database with the replica
    /// (`docs/decisions/D170.md`), which restores the replica's latest state: each file is read
    /// at most [`READS_PER_RESTORED_FILE`] times, and a boot that fails or stops makes the
    /// comparison again at its next start.
    pub restores: u64,
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
        .saturating_add(spent.fault_hits)
        .saturating_add(
            spent
                .degraded_retries
                .saturating_mul(READS_PER_DEGRADED_RETRY),
        )
        .saturating_add(spent.restores.saturating_mul(READS_PER_RESTORED_FILE));
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
    // A wider key at the same level from the same start replaced it: the history rebuilt past
    // the loss merged its range again (level-1 `1-2` over a rolled-back `1-1`), and an output
    // one level up that holds its range was merged from the replacement, not from it.
    let superseded = last.keys().any(|other| {
        other != key
            && other.level() == key.level()
            && other.range().min() == key.range().min()
            && other.range().max() > key.range().max()
    });
    if superseded {
        return false;
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
            mode: lsr_core::WriteMode::New,
            header: None,
            origin: None,
            created_at: Timestamp::from_unix_millis(0),
            size,
            digest: 0,
            begun: Timestamp::from_unix_millis(0),
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

    /// Swarm seed 0x70f: a rollback took transaction 1, the level-1 `1-1` merged before it was
    /// replaced by `1-2` after it, and level 2 holds `1-2`. The rolled-back `1-1` was counted at
    /// level 1 beside its replacement, as if `1-2` at level 2 had been merged from it, and level
    /// 1 came out over level 0's bytes.
    #[test]
    fn a_rolled_back_output_replaced_at_its_level_counts_once() {
        let events = [
            put(key(0, 1, 1), 4370),
            put(key(0, 2, 2), 660),
            put(key(1, 1, 1), 4370),
            put(key(1, 1, 2), 3874),
            put(key(2, 1, 2), 3874),
        ];
        let lost = BTreeSet::from([Txid::new(1).unwrap()]);
        let counts = RecorderCounts::default();
        assert_eq!(check(&events, counts, Spent::default(), &lost, &[]), Ok(()));
    }

    /// A boot with nothing staged may restore the replica's state to compare it with the database
    /// (D170): reads within the files it could read pass, one more fails.
    #[test]
    fn restores_for_an_identity_check_are_charged_per_file() {
        let spent = Spent {
            restores: 130,
            ..Spent::default()
        };
        let allowed = READS_PER_BOOT + 130 * READS_PER_RESTORED_FILE;
        let lost = BTreeSet::new();
        let at = |reads| RecorderCounts {
            reads,
            ..RecorderCounts::default()
        };
        assert_eq!(check(&[], at(allowed), spent, &lost, &[]), Ok(()));
        assert!(check(&[], at(allowed + 1), spent, &lost, &[]).is_err());
    }

    /// Boots that fail one after another each read the replica. Reads within what they allow
    /// pass; one more fails.
    #[test]
    fn repeated_failed_boots_pass_within_their_reads_and_fail_past_them() {
        let boots = 5;
        let spent = Spent {
            boots,
            ..Spent::default()
        };
        let allowed = (boots + 1) * READS_PER_BOOT;
        let lost = BTreeSet::new();
        let at = |reads| RecorderCounts {
            reads,
            ..RecorderCounts::default()
        };
        assert_eq!(check(&[], at(allowed), spent, &lost, &[]), Ok(()));
        assert!(check(&[], at(allowed + 1), spent, &lost, &[]).is_err());
    }
}
