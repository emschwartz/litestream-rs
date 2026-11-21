//! Adversarial planner checks written by the blind review.

#[cfg(test)]
mod review {

    use lsr_core::{
        AsOfPlanner, FileInfo, FileKey, Inventory, InventoryError, Level, PlanOutcome, Resolve,
        RestoreTarget, StampStep, Stamps, plan_reference,
    };
    use lsr_core::{ListedStamp, StoredStamp};
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use proptest::prelude::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct F {
        level: u8,
        min: u64,
        max: u64,
        created: i64,
    }

    fn info(f: F) -> FileInfo {
        let r = TxidRange::new(Txid::new(f.min).unwrap(), Txid::new(f.max).unwrap()).unwrap();
        FileInfo {
            key: FileKey::new(Level::new(f.level).unwrap(), r),
            size: 1,
            listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(f.created))),
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum G {
        Latest,
        Txid(u64),
        AsOf(i64),
    }

    fn admits(g: G, f: &F) -> bool {
        match g {
            G::Latest => true,
            G::Txid(n) => f.max <= n,
            G::AsOf(t) => f.created < t,
        }
    }

    /// Reachable-set fixpoint: every transaction a chain can end at.
    fn reachable_ends(files: &[F], g: G) -> std::collections::BTreeSet<u64> {
        let ok: Vec<F> = files.iter().copied().filter(|f| admits(g, f)).collect();
        let mut ends: std::collections::BTreeSet<u64> =
            ok.iter().filter(|f| f.min == 1).map(|f| f.max).collect();
        loop {
            let mut grew = false;
            for f in ok.iter().filter(|f| f.level < 9) {
                if ends
                    .iter()
                    .any(|&e| f.min <= e.saturating_add(1) && f.max > e)
                    && ends.insert(f.max)
                {
                    grew = true;
                }
            }
            if !grew {
                return ends;
            }
        }
    }

    fn want_end(files: &[F], g: G) -> Option<u64> {
        match g {
            G::Txid(n) => Some(n),
            G::Latest | G::AsOf(_) => files.iter().filter(|f| admits(g, f)).map(|f| f.max).max(),
        }
    }

    /// Files later than the instant that cut through the state at `end`: usable,
    /// linking, two or more transactions past it, and with no file dating the
    /// transaction right after `end`. Only a level 0-8 file ending there dates
    /// it: a snapshot is stamped when written, any time after its newest
    /// transaction. Written from the reachability argument, not from the
    /// planner.
    fn straddlers(files: &[F], g: G, end: u64) -> Vec<F> {
        let Some(one_past) = end.checked_add(1) else {
            return Vec::new();
        };
        if files.iter().any(|f| f.level < 9 && f.max == one_past) {
            return Vec::new();
        }
        files
            .iter()
            .copied()
            .filter(|f| {
                !admits(g, f)
                    && (f.level < 9 || f.min == 1)
                    && f.min <= one_past
                    && f.max > one_past
            })
            .collect()
    }

    fn spec(base: u64) -> impl Strategy<Value = F> {
        (
            0u8..=9,
            0u64..12,
            0u64..12,
            0i64..12,
            prop::bool::weighted(0.6),
        )
            .prop_map(move |(level, a, b, created, start_at_one)| {
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
        fn plan_matches_the_reachable_set((files, g) in case()) {
            // The generator may repeat a key with another stamp; a real listing
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
                let lazy = AsOfPlanner::new(&inv, instant).advance(&Stamps::default());
                prop_assert_eq!(lazy, Resolve::Done(out.clone()));
            }
            let ends = reachable_ends(&kept, g);
            let want = match g {
                G::AsOf(_) => ends.iter().max().copied(),
                G::Latest | G::Txid(_) => want_end(&kept, g),
            };
            // An instant before a hole: files of any stamp lie past the end and
            // none of them starts at or before one past it.
            let before_a_hole = |w: u64| {
                matches!(g, G::AsOf(_))
                    && kept.iter().any(|f| f.max > w)
                    && !kept.iter().any(|f| {
                        (f.level < 9 || f.min == 1) && f.min <= w.saturating_add(1) && f.max > w
                    })
            };
            let exists = want.is_some_and(|w| ends.contains(&w) && !before_a_hole(w));
            let straddlers: Vec<F> = match (exists, want, g) {
                (true, Some(w), G::AsOf(_)) => straddlers(&kept, g, w),
                _ => Vec::new(),
            };
            let inside = !straddlers.is_empty();
            prop_assert_eq!(matches!(out, PlanOutcome::Plan(_)), exists && !inside, "{:?}", out);
            prop_assert_eq!(
                matches!(out, PlanOutcome::AsOfInsideFile { .. }),
                inside,
                "{:?}",
                out
            );
            if let PlanOutcome::AsOfInsideFile { file, .. } = &out {
                prop_assert!(
                    straddlers.iter().any(|s| s.level == file.level().get()
                        && s.min == file.range().min().get()
                        && s.max == file.range().max().get()),
                    "{:?} is not a straddler",
                    file
                );
            }
            if let PlanOutcome::BeyondNewest { requested, newest } = &out {
                prop_assert_eq!(Some(newest.get()), kept.iter().map(|f| f.max).max());
                prop_assert!(requested > newest);
            }
            if let PlanOutcome::InsideFile { requested, file } = &out {
                prop_assert!(file.range().max() > *requested);
                prop_assert!(inv.files().iter().any(|i| i.key == *file));
            }
            if let PlanOutcome::Plan(c) | PlanOutcome::AsOfInsideFile { chain: c, .. } = &out {
                prop_assert_eq!(Some(c.reaches().get()), want);
                prop_assert_eq!(c.files().last().map(|f| f.key.range().max()), Some(c.reaches()));
                let mut reach = 0u64;
                for (i, f) in c.files().iter().enumerate() {
                    prop_assert!(inv.files().contains(f));
                    let k = kept.iter().find(|k| k.level == f.key.level().get() && k.min == f.key.range().min().get() && k.max == f.key.range().max().get()).unwrap();
                    prop_assert!(admits(g, k));
                    if i == 0 { prop_assert!(k.min == 1); } else {
                        prop_assert!(k.level < 9 && k.min <= reach.saturating_add(1) && k.max > reach);
                    }
                    reach = k.max;
                }
            }
            if let PlanOutcome::Gap { have_through, .. } = &out {
                prop_assert_eq!(ends.iter().max().copied(), Some(have_through.get()), "have_through is not the furthest reach");
            }
        }
    }

    fn at(level: u8, min: u64, max: u64, created: i64) -> FileInfo {
        info(F {
            level,
            min,
            max,
            created,
        })
    }

    /// Go stamps a compacted file with its newest input's time, so an L1 4-10 made
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
