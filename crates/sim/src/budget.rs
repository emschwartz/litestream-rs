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
//!   fault that failed a call (each such failure is retried), plus
//!   [`READS_PER_DEGRADED_RETRY`] per retry of a degraded task (a retry of a persistent
//!   cause such as a page size change compares the replica's newest file again; the
//!   doubling wait bounds how often), plus the probes past each listing ([`Walked`]).
//! - Every listing (a boot's, a relist's, maintenance's, a restore's) lists each level,
//!   so a tick's listing calls over the level count, rounded up, bound the probes past a
//!   listing it made. Each probe looks for transactions from the one after its listing's
//!   newest, through the newest that staging or the replica holds and a gap past that, or
//!   up to its cap when nothing is staged to bound it; never more than the cap. So it looks
//!   for at most the reach the simulator sees around the tick plus the gap, capped. For each
//!   transaction in its gap window it names the level-0 file, the level-0 file from the
//!   first transaction and from where the listed level 0 ends, the snapshot, and two starts
//!   for each level 1 to 8; past the window, the level-1 to 8 starts go. Only a level the
//!   replica was ever written at can have a start, so the levels counted are those a put
//!   or another writer reached, over the whole run. A key a lag hid and a probe found is a
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

/// Reads one boot may make of the replica besides its probe: a header and a body for the
/// newest file of each of the ten levels, and the upload lane's read-back.
pub const READS_PER_BOOT: u64 = 24;

/// Keys a probe names for each transaction whatever the replica holds: the level-0 file, the
/// level-0 file from the first transaction, the level-0 file from where the listed level 0
/// ends, and the snapshot.
pub const KEYS_PER_TRANSACTION: u64 = 4;

/// Keys a probe names inside its gap window for each level 1 to 8 the replica holds: the
/// start after the level's newest file and the start of the level below's run above it.
pub const KEYS_PER_LEVEL: u64 = 2;

/// Transactions the probes past a run's listings may have looked for, split at each probe's
/// gap window: inside it every start is named, past it only the level-0 and snapshot keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Walked {
    /// Transactions inside a gap window.
    pub window: u64,
    /// Transactions past one.
    pub rest: u64,
}

impl Walked {
    /// The most transactions one probe looks for.
    #[expect(
        clippy::as_conversions,
        reason = "a u32 widens to a u64 losslessly; `From` is not const"
    )]
    pub const CAP: u64 = lsr_core::PROBE_CAP as u64;

    /// Transactions a probe looks for inside its gap window: the first one past the listing
    /// and the gap after it.
    #[expect(
        clippy::as_conversions,
        reason = "a u32 widens to a u64 losslessly; `From` is not const"
    )]
    pub const WINDOW: u64 = lsr_core::PROBE_GAP as u64 + 1;

    /// Adds a tick that made `lists` listing calls, each probe reaching at most `reach`, the
    /// newest transaction staging or the replica held around the tick ([`Self::CAP`] when
    /// nothing was staged).
    pub fn add(&mut self, lists: u64, reach: u64) {
        let levels = u64::try_from(lsr_core::Level::ALL.len()).unwrap_or(u64::MAX);
        let probes = lists.div_ceil(levels);
        let looked = reach.saturating_add(Self::WINDOW).min(Self::CAP);
        let window = looked.min(Self::WINDOW);
        self.window = self.window.saturating_add(probes.saturating_mul(window));
        self.rest = self
            .rest
            .saturating_add(probes.saturating_mul(looked.saturating_sub(window)));
    }

    /// The most reads these probes made, with `levels` of levels 1 to 8 ever written.
    #[must_use]
    pub fn reads(self, levels: u64) -> u64 {
        let in_window = KEYS_PER_TRANSACTION.saturating_add(KEYS_PER_LEVEL.saturating_mul(levels));
        self.window
            .saturating_mul(in_window)
            .saturating_add(self.rest.saturating_mul(KEYS_PER_TRANSACTION))
    }
}

/// Reads one retry of a degraded task may make of the replica: a header and
/// the newest file it names.
pub const READS_PER_DEGRADED_RETRY: u64 = 2;

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
    /// Ticks that reported a task degraded: each is one retry.
    pub degraded_retries: u64,
    /// The probes past the run's listings.
    pub walked: Walked,
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
        .saturating_add(spent.walked.reads(merged_levels(&sized)))
        .saturating_add(
            spent
                .degraded_retries
                .saturating_mul(READS_PER_DEGRADED_RETRY),
        );
    within(Bound::ReadCalls, counts.reads, reads)?;
    let keys = u64::try_from(last.len()).unwrap_or(u64::MAX);
    let commits = keys
        .saturating_mul(2)
        .saturating_add(spent.fault_hits)
        .saturating_add(spent.boots);
    within(Bound::CommitCalls, counts.commits, commits)
}

/// The levels 1 to 8 any file in `sized` was written at.
fn merged_levels(sized: &BTreeMap<FileKey, u64>) -> u64 {
    let levels: BTreeSet<u8> = sized
        .keys()
        .map(|key| key.level().get())
        .filter(|level| (1..=8).contains(level))
        .collect();
    u64::try_from(levels.len()).unwrap_or(u64::MAX)
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

    /// One listing lists every level, so a tick's listing calls over the level count, rounded
    /// up, are the probes it made.
    #[test]
    fn listing_calls_count_one_probe_per_listing_of_every_level() {
        let levels = u64::try_from(Level::ALL.len()).unwrap();
        let mut one = Walked::default();
        one.add(levels, 0);
        let mut two = Walked::default();
        two.add(levels + 1, 0);
        assert_eq!(two.window, 2 * one.window);
        let mut none = Walked::default();
        none.add(0, Walked::CAP);
        assert_eq!(none, Walked::default());
    }

    /// With nothing staged, nothing bounds the walk: a probe is charged its cap, every key in
    /// its gap window and the level-0 and snapshot keys past it.
    #[test]
    fn a_probe_with_nothing_staged_is_charged_to_the_cap() {
        let levels = u64::try_from(Level::ALL.len()).unwrap();
        let mut walked = Walked::default();
        walked.add(levels, Walked::CAP);
        assert_eq!(walked.window, Walked::WINDOW);
        assert_eq!(walked.rest, Walked::CAP - Walked::WINDOW);
        assert_eq!(walked.reads(0), Walked::CAP * KEYS_PER_TRANSACTION);
        assert_eq!(
            walked.reads(2),
            Walked::CAP * KEYS_PER_TRANSACTION + Walked::WINDOW * 2 * KEYS_PER_LEVEL
        );
    }

    /// Behind a staged backlog a probe reads through the newest staged transaction and its
    /// gap, never past the cap: the charge follows the backlog, not the worst case.
    #[test]
    fn a_probe_behind_a_backlog_is_charged_through_the_backlog_and_its_gap() {
        let levels = u64::try_from(Level::ALL.len()).unwrap();
        let mut short = Walked::default();
        short.add(levels, 10);
        assert_eq!(short.window + short.rest, 10 + Walked::WINDOW);
        let mut long = Walked::default();
        long.add(levels, Walked::CAP * 2);
        assert_eq!(long.window + long.rest, Walked::CAP);
    }

    /// Boots that fail one after another each list and walk their backlog. Reads within what
    /// those listings allow pass; twice as many, a walk repeated per listing, fail.
    #[test]
    fn repeated_boots_walking_their_backlog_pass_and_a_doubled_walk_fails() {
        let levels = u64::try_from(Level::ALL.len()).unwrap();
        let boots = 5;
        let mut walked = Walked::default();
        for _ in 0..boots {
            walked.add(levels, 40);
        }
        let spent = Spent {
            boots,
            walked,
            ..Spent::default()
        };
        let allowed = (boots + 1) * READS_PER_BOOT + walked.reads(0);
        let lost = BTreeSet::new();
        let at = |reads| RecorderCounts {
            reads,
            ..RecorderCounts::default()
        };
        assert_eq!(check(&[], at(allowed), spent, &lost, &[]), Ok(()));
        let doubled = allowed + walked.reads(0);
        assert!(check(&[], at(doubled), spent, &lost, &[]).is_err());
    }
}
