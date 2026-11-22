//! Adversarial planner checks written by the blind review: ids near both ends
//! of the range, refused snapshots, lost created-at times and instants that tie
//! with them, each outcome held whole to the restore-plan spec (`lsr-spec`).

#[cfg(test)]
mod review {

    use lsr_core::{
        AsOfPlanner, CreatedAtStep, FileInfo, FileKey, Inventory, InventoryError, Level,
        PlanOutcome, Resolve, ResolvedCreatedAt, RestoreTarget, plan_reference, spec_view,
    };
    use lsr_core::{CreatedAt, ListedTime};
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use proptest::prelude::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct F {
        level: u8,
        min: u64,
        max: u64,
        created: i64,
        /// The store lost the created-at time and reports the upload time.
        uploaded: bool,
    }

    fn info(f: F) -> FileInfo {
        let r = TxidRange::new(Txid::new(f.min).unwrap(), Txid::new(f.max).unwrap()).unwrap();
        let at = Timestamp::from_unix_millis(f.created);
        let created_at = if f.uploaded {
            CreatedAt::fallback(at)
        } else {
            CreatedAt::mtime(at)
        };
        FileInfo {
            key: FileKey::new(Level::new(f.level).unwrap(), r),
            size: 1,
            listed: ListedTime::Created(created_at),
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum G {
        Latest,
        Txid(u64),
        AsOf(i64),
    }

    fn spec(base: u64) -> impl Strategy<Value = F> {
        (
            0u8..=9,
            0u64..12,
            0u64..12,
            0i64..12,
            prop::bool::weighted(0.6),
            prop::bool::weighted(0.2),
        )
            .prop_map(move |(level, a, b, created, start_at_one, uploaded)| {
                let (lo, hi) = (a.min(b), a.max(b));
                let min = if level == 9 && start_at_one {
                    1
                } else {
                    base.saturating_add(lo).max(1)
                };
                let max = base.saturating_add(hi).max(min);
                F {
                    level,
                    min,
                    max,
                    created,
                    uploaded,
                }
            })
    }

    fn case() -> impl Strategy<Value = (Vec<F>, G)> {
        prop_oneof![Just(1u64), Just(u64::MAX - 11)].prop_flat_map(|base| {
            (
                prop::collection::vec(spec(base), 0..14),
                prop_oneof![
                    Just(G::Latest),
                    (0u64..13).prop_map(move |d| G::Txid(base.saturating_add(d).max(1))),
                    (0i64..14).prop_map(G::AsOf),
                ],
            )
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20_000))]
        #[test]
        fn plan_is_what_the_contract_allows((files, g) in case()) {
            // The generator may repeat a key with another created-at time; a real listing
            // cannot, and the inventory refuses it, so keep the first of each.
            let mut seen = std::collections::HashSet::new();
            let kept: Vec<F> = files
                .iter()
                .copied()
                .filter(|f| seen.insert((f.level, f.min, f.max)))
                .collect();
            let inv = Inventory::from_files(kept.iter().copied().map(info)).unwrap();
            let target = match g {
                G::Latest => RestoreTarget::Latest,
                G::Txid(n) => RestoreTarget::Txid(Txid::new(n).unwrap()),
                G::AsOf(t) => RestoreTarget::AsOf(Timestamp::from_unix_millis(t)),
            };
            let out = plan_reference(&inv, target);
            if let RestoreTarget::AsOf(instant) = target {
                let lazy = AsOfPlanner::new(&inv, instant).advance(&ResolvedCreatedAt::default());
                prop_assert_eq!(lazy, Resolve::Done(out.clone()));
            }
            // The whole outcome, payloads included, against the contract.
            let judgement = spec_view::judge(&inv, target).expect("the spec takes every inventory");
            prop_assert!(
                !judgement.allowed.is_empty(),
                "the contract is silent ({:?}) on {:?} {:?}",
                judgement.by,
                kept,
                g
            );
            prop_assert!(
                judgement.admits(&spec_view::outcome(&out)),
                "{:?} for {:?} {:?}: the contract allows {:?} ({:?})",
                out,
                kept,
                g,
                judgement.allowed,
                judgement.by
            );
        }
    }

    fn at(level: u8, min: u64, max: u64, created: i64) -> FileInfo {
        info(F {
            level,
            min,
            max,
            created,
            uploaded: false,
        })
    }

    /// Go records a compacted file's created-at time as its newest input's time, so an L1 4-10 made
    /// from L0 files created at 400..=1000 is created at 1000. With those L0 files
    /// gone, the state as of 950 lies somewhere in 4..=9 and nothing in the
    /// replica can say where. Planning to 3 claims no transaction happened between
    /// 300 and 950: a silently short restore, the AsOf twin of `InsideFile`.
    #[test]
    fn as_of_inside_a_compacted_file_is_not_a_plan_to_the_older_state() {
        let inv = Inventory::from_files([at(9, 1, 3, 300), at(1, 4, 10, 1000)]).unwrap();
        let out = plan_reference(&inv, RestoreTarget::AsOf(Timestamp::from_unix_millis(950)));
        assert!(
            matches!(out, PlanOutcome::AsOfInsideFile { .. }),
            "AsOf(950) planned to {out:?} although L1 4-10 proves later history the planner cannot place"
        );
    }

    /// Two listings merged into one inventory can hold one key twice (rewritten
    /// between them). Which created_at survives would depend on input order, so
    /// an AsOf plan would flip with it: the inventory refuses instead.
    #[test]
    fn a_key_given_twice_is_refused_whatever_the_order() {
        let early = at(0, 4, 4, 100);
        let late = at(0, 4, 4, 900);
        let snap = at(9, 1, 3, 50);
        let a = Inventory::from_files([snap, early, late]);
        let b = Inventory::from_files([snap, late, early]);
        assert!(matches!(
            a,
            Err(InventoryError::ConflictingDuplicate { .. })
        ));
        assert_eq!(a, b);
    }
}
