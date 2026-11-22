//! Bounded proofs of the key-target planner, run by `just kani`.
//!
//! Kani explores every inventory of up to [`MAX_FILES`] files whose levels run 0 to 9 and whose
//! transactions lie in 1 to [`DOMAIN`], against both key targets. [`reachable`] is an independent
//! brute-force oracle: a fixpoint over the transactions some chain can end at, written without
//! any of the planner's ordering or preference logic. The harnesses check that a returned plan is
//! a valid chain to the target (soundness), that a plan exists exactly when the oracle finds a
//! chain (completeness), and, since Kani checks overflow and panics by default, that `plan` never
//! panics on those inputs.

use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::inventory::Inventory;
use crate::layout::{FileInfo, FileKey, Level, ListedStamp, StoredStamp};
use crate::plan::{KeyTarget, PlanOutcome, plan};

/// Files per inventory.
const MAX_FILES: usize = 2;
/// Transactions range over 1 to this.
const DOMAIN: u64 = 8;

/// One symbolic file: any level, any valid range inside the domain.
fn any_file() -> FileInfo {
    let level: u8 = kani::any();
    let min: u64 = kani::any();
    let max: u64 = kani::any();
    kani::assume(level <= 9);
    kani::assume(1 <= min && min <= max && max <= DOMAIN);
    let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
    FileInfo {
        key: FileKey::new(Level::new(level).unwrap(), range),
        size: 1,
        listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(0))),
    }
}

/// A symbolic inventory of exactly [`MAX_FILES`] files, duplicates possible. Smaller inventories
/// are covered by duplicates (a repeated key collapses to one entry); the empty one by
/// `plan_empty`.
fn any_inventory() -> (Inventory, [FileInfo; MAX_FILES]) {
    let files: [FileInfo; MAX_FILES] = core::array::from_fn(|_| any_file());
    let inventory = Inventory::from_files(files).unwrap();
    (inventory, files)
}

fn raw(info: &FileInfo) -> (u8, u64, u64) {
    let range = info.key.range();
    (info.key.level().get(), range.min().get(), range.max().get())
}

/// Brute force: can some chain of eligible files end exactly at `goal`? Bit `t` of the
/// accumulator means a chain ends at transaction `t`.
fn reachable(files: &[FileInfo], limit: Option<u64>, goal: u64) -> bool {
    let eligible = |max: u64| limit.is_none_or(|wanted| max <= wanted);
    let mut ends: u16 = 0;
    for info in files {
        let (_, min, max) = raw(info);
        if eligible(max) && min == 1 {
            ends |= 1 << max;
        }
    }
    for _ in 0..=MAX_FILES {
        for info in files {
            let (level, min, max) = raw(info);
            if !eligible(max) || level == 9 {
                continue;
            }
            for end in 1..=DOMAIN {
                if ends & (1 << end) != 0 && min <= end + 1 && max > end {
                    ends |= 1 << max;
                }
            }
        }
    }
    goal <= DOMAIN && ends & (1 << goal) != 0
}

/// A stable insertion sort standing in for the standard library's stable sort, which is a
/// driftsort that CBMC cannot unwind at useful sizes. Both sort stably by the same comparison,
/// so the planner sees the same order; the stub lives in the proofs, not the planner.
#[allow(clippy::arithmetic_side_effects, reason = "proof stub; indices stay below the length")]
fn insertion_sort<T, F: FnMut(&T, &T) -> bool>(v: &mut [T], mut is_less: F) {
    for i in 1..v.len() {
        let mut j = i;
        while j > 0 && is_less(&v[j], &v[j - 1]) {
            v.swap(j, j - 1);
            j -= 1;
        }
    }
}

fn check(target: KeyTarget) {
    let (inventory, files) = any_inventory();
    let (limit, goal) = match target {
        KeyTarget::Txid(wanted) => (Some(wanted.get()), wanted.get()),
        KeyTarget::Latest => (
            None,
            files.iter().map(|f| raw(f).2).max().unwrap_or(0),
        ),
    };
    let found = reachable(&files, limit, goal);
    match plan(&inventory, target) {
        PlanOutcome::Plan(chain) => {
            assert!(found, "plan returned but the oracle finds no chain");
            assert_eq!(chain.reaches().get(), goal);
            let mut reach = 0;
            for (index, info) in chain.files().iter().enumerate() {
                let (level, min, max) = raw(info);
                assert!(inventory.files().contains(info));
                assert!(limit.is_none_or(|wanted| max <= wanted));
                if index == 0 {
                    assert_eq!(min, 1);
                } else {
                    assert!(level != 9 && min <= reach + 1 && max > reach);
                }
                reach = max;
            }
            assert_eq!(reach, goal);
        }
        PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::Gap { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. } => {
            assert!(!found, "the oracle finds a chain but plan returned none");
        }
    }
}

#[kani::proof]
#[kani::unwind(8)]
#[kani::stub(alloc::slice::stable_sort, insertion_sort)]
fn plan_latest() {
    check(KeyTarget::Latest);
}

#[kani::proof]
#[kani::unwind(8)]
#[kani::stub(alloc::slice::stable_sort, insertion_sort)]
fn plan_txid() {
    let wanted: u64 = kani::any();
    kani::assume(1 <= wanted && wanted <= DOMAIN + 1);
    check(KeyTarget::Txid(Txid::new(wanted).unwrap()));
}

#[kani::proof]
#[kani::unwind(8)]
#[kani::stub(alloc::slice::stable_sort, insertion_sort)]
fn probe_inventory() {
    let (inventory, _files) = any_inventory();
    assert!(inventory.files().len() <= MAX_FILES);
}
