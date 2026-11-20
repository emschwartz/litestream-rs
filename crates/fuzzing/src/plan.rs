//! The `plan` target: the planner's answer must be a valid chain or a reason.

use lsr_core::{
    ContiguousChain, FileInfo, FileKey, Inventory, Level, PlanOutcome, RestoreTarget,
    plan as plan_restore,
};
use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::bytes::{Reader, violation};
use lsr_core::{ListedStamp, StoredStamp};

/// Bytes of one file record in the input.
pub const PLAN_RECORD_LEN: usize = 4;

/// Most files an input may hold; the planner is quadratic at worst.
const MAX_FILES: usize = 64;

/// Builds the inventory and target an input describes.
///
/// Input: a target byte (low two bits: 0 latest, 1 transaction, 2 instant;
/// bit 7: count the transaction back from the largest id), a big-endian
/// `u16` target value, a shift byte, then records of [`PLAN_RECORD_LEN`]
/// bytes: level (mod 10), first transaction (mod 64, plus one), span (mod 32,
/// shifted left by the shift byte mod 58 so ranges reach large ids), creation
/// time in milliseconds. Small ids make overlaps, gaps and snapshots likely.
#[must_use]
pub fn plan_input(data: &[u8]) -> Option<(Inventory, RestoreTarget)> {
    let mut reader = Reader::new(data);
    let kind = reader.u8()?;
    let value = reader.u16()?;
    let shift = u32::from(reader.u8()? % 58);
    let target = match kind & 3 {
        0 => RestoreTarget::Latest,
        1 => {
            let raw = if kind & 0x80 == 0 {
                u64::from(value % 96).checked_add(1)?
            } else {
                u64::MAX.checked_sub(u64::from(value))?
            };
            RestoreTarget::Txid(Txid::new(raw)?)
        }
        _ => RestoreTarget::AsOf(Timestamp::from_unix_millis(i64::from(value % 300))),
    };
    let mut files = Vec::new();
    while files.len() < MAX_FILES
        && let Some([level, first, span, created]) = reader.array::<PLAN_RECORD_LEN>()
    {
        let min = Txid::new(u64::from(first % 64).checked_add(1)?)?;
        let width = u64::from(span % 32).checked_shl(shift)?;
        let max = Txid::new(min.get().saturating_add(width))?;
        let Some(range) = TxidRange::new(min, max) else {
            continue;
        };
        let Some(level) = Level::new(level % 10) else {
            continue;
        };
        files.push(FileInfo {
            key: FileKey::new(level, range),
            size: 0,
            listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(
                i64::from(created),
            ))),
        });
    }
    // A key listed twice with different details is not an inventory (see
    // InventoryError::ConflictingDuplicate); keep the first, as a store never lists a key twice.
    let mut keys = std::collections::BTreeSet::new();
    files.retain(|info| keys.insert(info.key));
    Some((Inventory::from_files(files).ok()?, target))
}

fn eligible(info: &FileInfo, target: RestoreTarget) -> bool {
    match target {
        RestoreTarget::Latest => true,
        RestoreTarget::Txid(wanted) => info.key.range().max() <= wanted,
        RestoreTarget::AsOf(instant) => {
            info.exact_stamp().is_some_and(|stamp| stamp.at() < instant)
        }
    }
}

fn check_chain(inventory: &Inventory, target: RestoreTarget, chain: &ContiguousChain) {
    let Some((first, later)) = chain.files().split_first() else {
        violation("a plan holds no file");
    };
    if first.key.range().min() != Txid::MIN {
        violation("a plan does not start with a file at transaction 1");
    }
    let mut reach = first.key.range().max();
    for info in later {
        let range = info.key.range();
        let touches = reach.next().is_none_or(|one_past| range.min() <= one_past);
        if info.key.level().is_snapshot() || !touches || range.max() <= reach {
            violation("a plan is not contiguous and advancing");
        }
        reach = range.max();
    }
    if chain.reaches() != reach {
        violation("reaches() disagrees with the last file");
    }
    for info in chain.files() {
        if !inventory.files().contains(info) || !eligible(info, target) {
            violation("a plan uses a file that is absent or ineligible for the target");
        }
    }
    let eligible_files = || {
        inventory
            .files()
            .iter()
            .filter(|info| eligible(info, target))
    };
    let wanted = match target {
        RestoreTarget::Txid(wanted) => Some(wanted),
        RestoreTarget::Latest => eligible_files().map(|info| info.key.range().max()).max(),
        // Ruling A1: an as-of plan ends where the eligible chain can reach, and an
        // eligible file that does not connect is not a reason to stop earlier.
        RestoreTarget::AsOf(_) => furthest_reach(eligible_files().map(|info| info.key)),
    };
    if Some(reach) != wanted {
        violation("a plan does not end exactly where the target says");
    }
}

/// The furthest transaction a chain over `files` can end at: bases start at
/// transaction 1 and each later file below level 9 must touch the end so far.
fn furthest_reach(files: impl Iterator<Item = FileKey>) -> Option<Txid> {
    let files: Vec<FileKey> = files.collect();
    let mut reach = files
        .iter()
        .filter(|key| key.range().min().get() == 1)
        .map(|key| key.range().max())
        .max()?;
    loop {
        let Some(next) = files
            .iter()
            .filter(|key| key.level().get() < 9)
            .filter(|key| {
                reach
                    .next()
                    .is_none_or(|one_past| key.range().min() <= one_past)
            })
            .map(|key| key.range().max())
            .max()
        else {
            return Some(reach);
        };
        if next <= reach {
            return Some(reach);
        }
        reach = next;
    }
}

/// An `AsOfInsideFile` carries a valid chain, and a listed file that links to
/// it, reaches at least two transactions past it and was created at or after
/// the instant.
fn check_inside_file(
    inventory: &Inventory,
    target: RestoreTarget,
    chain: &ContiguousChain,
    file: FileKey,
) {
    let RestoreTarget::AsOf(instant) = target else {
        violation("AsOfInsideFile for a target that is not AsOf");
    };
    check_chain(inventory, target, chain);
    let Some(info) = inventory.files().iter().find(|info| info.key == file) else {
        violation("AsOfInsideFile names a file the inventory lacks");
    };
    let reach = chain.reaches();
    let links = reach
        .next()
        .is_none_or(|one_past| file.range().min() <= one_past);
    let past = reach
        .next()
        .is_some_and(|one_past| file.range().max() > one_past);
    if info.exact_stamp().is_none_or(|stamp| stamp.at() < instant)
        || !links
        || !past
        || chain.files().iter().any(|f| f.key == file)
    {
        violation("AsOfInsideFile's file does not straddle the instant past the chain");
    }
}

fn check_reason(inventory: &Inventory, target: RestoreTarget, outcome: &PlanOutcome) {
    match outcome {
        PlanOutcome::Plan(_) => {}
        PlanOutcome::EmptyReplica => {
            if !inventory.is_empty() {
                violation("EmptyReplica for a replica that holds files");
            }
        }
        PlanOutcome::NoSnapshot { .. } => {}
        PlanOutcome::AsOfInsideFile { chain, file } => {
            check_inside_file(inventory, target, chain, *file);
        }
        PlanOutcome::BeyondNewest { requested, newest } => {
            if requested <= newest || inventory.newest() != Some(*newest) {
                violation("BeyondNewest names a request the replica covers");
            }
        }
        PlanOutcome::Gap {
            have_through,
            next_starts,
        } => {
            let hole = have_through
                .next()
                .is_some_and(|one_past| *next_starts > one_past);
            if !hole {
                violation("Gap where the next file touches the chain");
            }
        }
        PlanOutcome::InsideFile { requested, file } => {
            let listed = inventory.files().iter().any(|info| info.key == *file);
            if !listed || !file.range().contains(*requested) || file.range().max() == *requested {
                violation("InsideFile names a file that does not strictly cover the request");
            }
        }
        PlanOutcome::MalformedSnapshot { file, .. } => {
            if !inventory.files().iter().any(|info| info.key == *file) {
                violation("MalformedSnapshot names a file the inventory lacks");
            }
        }
    }
}

/// Plans a restore over a fuzz-built inventory and checks the answer: a plan
/// is a contiguous chain of listed, eligible files from a snapshot at
/// transaction 1 that ends exactly at the target; every other outcome must be
/// consistent with the inventory it explains.
pub fn plan(data: &[u8]) {
    let Some((inventory, target)) = plan_input(data) else {
        return;
    };
    let outcome = plan_restore(&inventory, target);
    if let PlanOutcome::Plan(chain) = &outcome {
        check_chain(&inventory, target, chain);
    }
    check_reason(&inventory, target, &outcome);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot 1..=3 at level 9 and L0 files 4 and 5, newest state wanted.
    const CHAIN: [u8; 3 + 1 + 3 * PLAN_RECORD_LEN] = [
        0, 0, 0, 0, //
        9, 0, 2, 1, //
        0, 3, 0, 2, //
        0, 4, 0, 3,
    ];

    #[test]
    fn a_chain_input_plans() {
        let (inventory, target) = plan_input(&CHAIN).expect("input parses");
        assert_eq!(inventory.files().len(), 3);
        let PlanOutcome::Plan(chain) = plan_restore(&inventory, target) else {
            panic!("expected a plan");
        };
        assert_eq!(chain.reaches().get(), 5);
        plan(&CHAIN);
    }

    #[test]
    fn an_instant_inside_a_later_file_is_checked() {
        let data = [2, 0, 40, 0, 9, 0, 2, 1, 1, 3, 2, 50];
        let (inventory, target) = plan_input(&data).expect("input parses");
        assert!(matches!(
            plan_restore(&inventory, target),
            PlanOutcome::AsOfInsideFile { .. }
        ));
        plan(&data);
    }

    #[test]
    fn a_hole_is_a_gap_not_a_check_failure() {
        let mut data = CHAIN.to_vec();
        data.splice(8..12, []);
        let (inventory, target) = plan_input(&data).expect("input parses");
        assert!(matches!(
            plan_restore(&inventory, target),
            PlanOutcome::Gap { .. }
        ));
        plan(&data);
    }

    #[test]
    fn short_and_garbage_input_do_not_panic() {
        plan(&[]);
        plan(&[1, 0xff, 0xff, 57]);
        plan(&[0x81, 0xff, 0xff, 57, 9, 0, 255, 0, 0, 0, 255, 0]);
        plan(&[2, 0, 0, 0, 9, 0, 0, 0, 0, 1, 0, 1]);
    }
}
