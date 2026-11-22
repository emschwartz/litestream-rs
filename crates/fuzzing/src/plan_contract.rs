//! The `plan_contract` and `plan_lazy` targets: the planner's whole outcome,
//! payloads included, against the restore-plan spec (`lsr-spec`), which is
//! written from the contract in plan.rs's module doc and cannot reach the
//! planner's code.

use arbitrary::Unstructured;
use lsr_core::conformance::block_on;
use lsr_core::{
    AsOfPlanner, CreatedAtStep, FileInfo, Inventory, KeyTarget, MemStore, ObjectStore, PlanOutcome,
    Resolve, ResolvedCreatedAt, RestoreTarget, Upload, plan, plan_reference, resolve, spec_view,
};
use lsr_ltx::Timestamp;
use lsr_spec::restore_plan::{self as spec, Judgement};

use crate::bytes::violation;
use crate::listing::{ArbFile, ArbListing};

/// Holds an outcome to a judgement: the contract decides the case, and allows
/// this outcome.
fn hold(judgement: &Judgement, outcome: &PlanOutcome, case: &dyn Fn() -> String) {
    if judgement.allowed.is_empty() {
        violation(&format!(
            "the contract is silent ({:?}): {}",
            judgement.by,
            case()
        ));
    }
    if !judgement.admits(&spec_view::outcome(outcome)) {
        violation(&format!(
            "the contract allows {:?} ({:?}), the planner said {outcome:?}: {}",
            judgement.allowed,
            judgement.by,
            case()
        ));
    }
}

/// T1: a listing and a `Latest` or `Txid` target from `arbitrary`; [`plan`]'s
/// outcome must be one the spec allows, payloads and chain included.
pub fn plan_contract(data: &[u8]) {
    let mut u = Unstructured::new(data);
    let Ok(listing) = u.arbitrary::<ArbListing>() else {
        return;
    };
    let target = match u.int_in_range(0_u8..=3) {
        Ok(0) | Err(_) => KeyTarget::Latest,
        Ok(_) => match listing.txid(&mut u) {
            Ok(Some(txid)) => KeyTarget::Txid(txid),
            Ok(None) | Err(_) => return,
        },
    };
    let Ok(inventory) = Inventory::from_files(listing.exact()) else {
        violation("a deduplicated listing is not an inventory");
    };
    let outcome = plan(&inventory, target);
    let Some(judgement) = spec_view::judge(&inventory, target.into()) else {
        violation("the spec refuses an inventory");
    };
    hold(&judgement, &outcome, &|| {
        format!("{target:?} over {:?}", inventory.files())
    });
}

/// A store holding the files, listed with last-modified times only, each
/// with its created-at time stored or lost as the listing says.
fn store_of(files: &[ArbFile]) -> MemStore {
    let store = MemStore::with_listed_last_modified();
    block_on(async {
        for file in files {
            let created = Timestamp::from_unix_millis(file.created);
            let Ok(mut up) = store.begin_upload(&file.key, created).await else {
                violation("the memory store refused an upload");
            };
            if up.write(b"x").await.is_err() || up.commit().await.is_err() {
                violation("the memory store refused an upload");
            }
            store.set_last_modified(&file.key, Timestamp::from_unix_millis(file.last_modified));
            if file.lost {
                store.drop_created_at(&file.key);
            }
        }
    });
    store
}

/// One file of the lazy listing, exact if the input says so.
fn relisted(files: &[ArbFile], info: &FileInfo) -> FileInfo {
    match files.iter().find(|file| file.key == info.key) {
        Some(file) if file.listed_exact => file.exact(),
        Some(_) => *info,
        None => violation("the listing holds a file nobody stored"),
    }
}

/// The store's lazy listing with the files the input marks listed exact.
fn mixed(store: &MemStore, files: &[ArbFile]) -> Inventory {
    let Ok(lazy) = block_on(Inventory::load(store)) else {
        violation("the memory store's listing failed");
    };
    let listed = lazy.files().iter().map(|info| relisted(files, info));
    let Ok(inventory) = Inventory::from_files(listed) else {
        violation("a mixed listing is not an inventory");
    };
    inventory
}

/// What a lazy run decided, and how it asked.
struct Run {
    outcome: PlanOutcome,
    asked: Vec<FileInfo>,
    created_at_read: u64,
}

fn run_lazily(store: &MemStore, inventory: &Inventory, instant: Timestamp) -> Run {
    let mut planner = AsOfPlanner::new(inventory, instant);
    let mut asked = Vec::new();
    let mut step = |resolved: &ResolvedCreatedAt| {
        let next = planner.advance(resolved);
        if let Resolve::Need(need) = &next {
            asked.push(*need.first());
        }
        next
    };
    let Ok(got) = block_on(resolve(store, &mut step)) else {
        violation("resolving the as-of plan failed (a repeated ask fails closed)");
    };
    Run {
        outcome: got.value,
        asked,
        created_at_read: got.created_at_read,
    }
}

/// T2: a listing from `arbitrary` in a store that lists last-modified times
/// (some files exact, some with their created-at time lost) and an instant
/// that ties with the times. [`AsOfPlanner`] resolves created-at times on
/// demand; its outcome must be one the spec allows over the true times, and
/// equal [`plan_reference`]'s over the exact listing. It asks only for files
/// listed without their time, each once, and reads no more than those.
pub fn plan_lazy(data: &[u8]) {
    let mut u = Unstructured::new(data);
    let Ok(listing) = u.arbitrary::<ArbListing>() else {
        return;
    };
    let Ok(instant) = listing.instant(&mut u) else {
        return;
    };
    let store = store_of(&listing.files);
    let inventory = mixed(&store, &listing.files);
    let run = run_lazily(&store, &inventory, instant);
    check_asks(&listing, &run);
    let Ok(exact) = Inventory::from_files(listing.exact()) else {
        violation("a deduplicated listing is not an inventory");
    };
    let target = RestoreTarget::AsOf(instant);
    let eager = plan_reference(&exact, target);
    if spec_view::outcome(&eager) != spec_view::outcome(&run.outcome) {
        violation(&format!(
            "lazy {:?} against eager {eager:?} at {instant:?} over {:?}",
            run.outcome,
            exact.files()
        ));
    }
    let truths = listing
        .files
        .iter()
        .map(|file| spec_view::file_at(&file.exact(), file.truth()));
    let Some(truths) = spec::Listing::new(truths) else {
        violation("the spec refuses an inventory");
    };
    let judgement = spec::judge(&truths, spec::Target::AsOf(instant.unix_millis()));
    hold(&judgement, &run.outcome, &|| {
        format!("{instant:?} over {:?}", exact.files())
    });
}

/// Each ask is for a file listed without its time, no file is asked twice,
/// and the store read no more created-at times than such files exist.
fn check_asks(listing: &ArbListing, run: &Run) {
    let lazy: Vec<&ArbFile> = listing
        .files
        .iter()
        .filter(|file| !file.listed_exact)
        .collect();
    for (at, info) in run.asked.iter().enumerate() {
        if !lazy.iter().any(|file| file.key == info.key) {
            violation("the as-of planner asked for a time the listing already had");
        }
        if run
            .asked
            .iter()
            .skip(at.saturating_add(1))
            .any(|later| later.key == info.key)
        {
            violation("the as-of planner asked for one file twice");
        }
    }
    let most = u64::try_from(lazy.len()).unwrap_or(u64::MAX);
    if run.created_at_read > most {
        violation("more created-at times read than files listed without one");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    /// Deterministic pseudo-random inputs of up to 1 KiB (a 64-bit LCG), so
    /// `just check` runs the targets over more than the committed corpus.
    fn inputs(count: u64) -> impl Iterator<Item = Vec<u8>> {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state.to_be_bytes()[0]
        };
        (0..count).map(move |_| {
            let len = usize::from(next()) * 4;
            (0..len).map(|_| next()).collect()
        })
    }

    #[test]
    fn random_inputs_hold_to_the_contract() {
        for data in inputs(1500) {
            plan_contract(&data);
            plan_lazy(&data);
        }
    }

    #[test]
    fn short_and_empty_inputs_do_nothing_wrong() {
        plan_contract(&[]);
        plan_lazy(&[]);
        plan_contract(&[0xff; 3]);
        plan_lazy(&[0xff; 3]);
    }

    /// Not vacuous: the generator reaches the lazy reads and every clause an
    /// `AsOf` judgement can be decided by.
    #[test]
    fn the_lazy_target_reaches_every_as_of_clause() {
        let mut by = BTreeSet::new();
        let mut asked = 0_u32;
        for data in inputs(1500) {
            let mut u = Unstructured::new(&data);
            let Ok(listing) = u.arbitrary::<ArbListing>() else {
                continue;
            };
            let Ok(instant) = listing.instant(&mut u) else {
                continue;
            };
            let store = store_of(&listing.files);
            let run = run_lazily(&store, &mixed(&store, &listing.files), instant);
            asked += u32::from(!run.asked.is_empty());
            let truths = listing
                .files
                .iter()
                .map(|file| spec_view::file_at(&file.exact(), file.truth()));
            let truths = spec::Listing::new(truths).unwrap();
            let judgement = spec::judge(&truths, spec::Target::AsOf(instant.unix_millis()));
            by.insert(format!("{:?}", judgement.by));
        }
        assert!(asked > 100, "only {asked} runs asked for a created-at time");
        for clause in [
            "EmptyReplica",
            "NoSnapshot",
            "AsOfChainEnd",
            "AsOfHole",
            "AsOfStraddle",
            "AsOfDatesNext",
        ] {
            assert!(by.contains(clause), "no input decided by {clause}: {by:?}");
        }
    }
}
