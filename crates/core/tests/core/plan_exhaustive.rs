//! Every small inventory, not a sample: the planner against the contract.
//!
//! Proptest samples; this enumerates. The key-target sweep builds every
//! inventory of up to N files over transactions 1..=D and a few levels (the
//! snapshot level 9 among them), plans each for `Latest` and for every `Txid`
//! target up to one past the end, and holds the whole outcome, payloads
//! included, to the restore-plan spec (`lsr-spec`), which is written from the
//! contract in the `plan` module doc and cannot reach the planner's code. The
//! `AsOf` sweep enumerates smaller inventories under every assignment of
//! distinct created-at times, stored or lost to a fallback, and every instant:
//! the exact planner is held to the spec, and the lazy planner (over lazy,
//! exact and mixed listings) to the exact planner's whole outcome.
//!
//! Two scopes. The gate (`just check`) runs the small one, a few CPU-seconds in debug; the
//! large one (`just plan-exhaustive`, the nightly `plan-exhaustive` job) is a strict superset of it
//! and takes minutes. Before the split the gate swept every inventory of up to four files over
//! transactions 1..=5 with levels {0,1,8,9} (523,686 inventories, 3.67 million cases, about
//! 50 CPU-seconds and 124 s of wall time under load) and the as-of sweep over D=4 (about 19
//! CPU-seconds); under load they outran nextest's 180 s limit.
//!
//! The spec must decide every case (an empty judgement fails); the tally
//! printed with `--no-capture` counts the cases per outcome and deciding
//! clause, so an empty bucket shows.

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the enumeration and oracle do arithmetic on tiny bounded transaction ids and indexes"
)]
mod exhaustive {
    use std::collections::BTreeMap;

    use lsr_core::WriteMode;
    use lsr_core::conformance::block_on;
    use lsr_core::{
        AsOfPlanner, CreatedAt, CreatedAtStep, FileInfo, FileKey, Inventory, KeyTarget, Level,
        ListedTime, MemStore, ObjectStore, PlanOutcome, Resolve, ResolvedCreatedAt, RestoreTarget,
        Upload, plan, plan_reference, resolve, spec_view,
    };
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use lsr_spec::restore_plan::Judgement;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct R {
        level: u8,
        min: u64,
        max: u64,
    }

    impl R {
        fn key(&self) -> FileKey {
            let range =
                TxidRange::new(Txid::new(self.min).unwrap(), Txid::new(self.max).unwrap()).unwrap();
            FileKey::new(Level::new(self.level).unwrap(), range)
        }

        fn is(&self, key: &FileKey) -> bool {
            self.key() == *key
        }
    }

    fn info(r: &R, created_at: i64) -> FileInfo {
        FileInfo {
            key: r.key(),
            size: 1,
            listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(created_at))),
        }
    }

    /// Every key over transactions 1..=`d` at each level.
    fn universe(d: u64, levels: &[u8]) -> Vec<R> {
        let mut keys = Vec::new();
        for &level in levels {
            for min in 1..=d {
                for max in min..=d {
                    keys.push(R { level, min, max });
                }
            }
        }
        keys
    }

    /// Calls `f` with every subset of `keys` of at most `n` elements, the empty one first.
    fn subsets(keys: &[R], n: usize, f: &mut impl FnMut(&[R])) {
        fn go(keys: &[R], from: usize, n: usize, held: &mut Vec<R>, f: &mut impl FnMut(&[R])) {
            f(held);
            if held.len() == n {
                return;
            }
            for i in from..keys.len() {
                held.push(keys[i]);
                go(keys, i + 1, n, held, f);
                held.pop();
            }
        }
        go(keys, 0, n, &mut Vec::new(), f);
    }

    // ---- the key-target sweep, judged by the spec ----

    #[derive(Clone, Copy, Debug)]
    enum T {
        Latest,
        Txid(u64),
    }

    impl T {
        fn key_target(self) -> KeyTarget {
            match self {
                Self::Latest => KeyTarget::Latest,
                Self::Txid(n) => KeyTarget::Txid(Txid::new(n).unwrap()),
            }
        }
    }

    type Tally = BTreeMap<String, u64>;

    fn note(tally: &mut Tally, what: String) {
        *tally.entry(what).or_default() += 1;
    }

    /// The outcome's variant, for the tally.
    fn variant(outcome: &PlanOutcome) -> String {
        format!("{outcome:?}")
            .split([' ', '(', '{'])
            .next()
            .unwrap()
            .to_owned()
    }

    /// Holds one outcome to the spec: the contract allows it, and decides the
    /// case at all (an empty judgement is a hole in the text, never a pass).
    fn judged(inv: &Inventory, target: RestoreTarget, out: &PlanOutcome, case: &str) -> Judgement {
        let judgement = spec_view::judge(inv, target).expect("the spec takes every inventory");
        assert!(
            !judgement.allowed.is_empty(),
            "the contract is silent ({:?}): {case}",
            judgement.by
        );
        assert!(
            judgement.admits(&spec_view::outcome(out)),
            "the contract allows {:?} ({:?}): {case}",
            judgement.allowed,
            judgement.by
        );
        judgement
    }

    /// Holds one `plan` outcome to the spec.
    fn verify(inv: &Inventory, files: &[R], t: T, tally: &mut Tally) {
        let out = plan(inv, t.key_target());
        let case = format!("{files:?} {t:?} -> {out:?}");
        let judgement = judged(inv, t.key_target().into(), &out, &case);
        note(tally, format!("{} by {:?}", variant(&out), judgement.by));
    }

    /// One sweep: every inventory of up to `n` files over `1..=d` with `levels`.
    /// Returns (inventories, cases).
    fn sweep_keys(d: u64, levels: &[u8], n: usize, tally: &mut Tally) -> (u64, u64) {
        let keys = universe(d, levels);
        let (mut inventories, mut cases) = (0u64, 0u64);
        subsets(&keys, n, &mut |files| {
            inventories += 1;
            let inv = Inventory::from_files(files.iter().map(|r| info(r, 0))).unwrap();
            verify(&inv, files, T::Latest, tally);
            cases += 1;
            for target in 1..=d + 1 {
                verify(&inv, files, T::Txid(target), tally);
                cases += 1;
            }
        });
        (inventories, cases)
    }

    fn report(name: &str, counts: (u64, u64), tally: &Tally) {
        println!("{name}: {} inventories, {} cases", counts.0, counts.1);
        for (what, count) in tally {
            println!("  {count:>9}  {what}");
        }
    }

    /// The gate's sweeps: up to three files over 1..=5 with levels 0, 1, 8 and 9 (every shape of
    /// chain, gap, overlap and snapshot of three files), and up to four files over 1..=4 with
    /// levels 0, 1 and 9 (the four-file interactions: a snapshot, a compacted file, and two
    /// level-0 files around it).
    #[test]
    fn every_small_inventory_plans_as_the_contract_says() {
        let mut tally = Tally::new();
        let counts = sweep_keys(5, &[0, 1, 8, 9], 3, &mut tally);
        report("key targets D=5 levels {0,1,8,9} N<=3", counts, &tally);
        let mut tally = Tally::new();
        let counts = sweep_keys(4, &[0, 1, 9], 4, &mut tally);
        report("key targets D=4 levels {0,1,9} N<=4", counts, &tally);
    }

    /// The large sweep (`just plan-exhaustive`, nightly): five files, and a wider range. It
    /// includes everything the gate runs and the old gate's four files over 1..=5.
    #[test]
    #[ignore = "the large sweep: minutes in debug; run with `just plan-exhaustive`"]
    fn every_larger_inventory_plans_as_the_contract_says() {
        let mut tally = Tally::new();
        let counts = sweep_keys(5, &[0, 1, 8, 9], 5, &mut tally);
        report("key targets D=5 levels {0,1,8,9} N<=5", counts, &tally);
        let mut tally = Tally::new();
        let counts = sweep_keys(6, &[0, 1, 8, 9], 4, &mut tally);
        report("key targets D=6 levels {0,1,8,9} N<=4", counts, &tally);
    }

    // ---- AsOf: the spec over exact listings, then lazy against eager ----

    /// Calls `f` with every injective assignment of `resolved` to `n` files.
    fn assignments(resolved: &[i64], n: usize, held: &mut Vec<i64>, f: &mut impl FnMut(&[i64])) {
        if held.len() == n {
            f(held);
            return;
        }
        for &s in resolved {
            if !held.contains(&s) {
                held.push(s);
                assignments(resolved, n, held, f);
                held.pop();
            }
        }
    }

    /// One file's created-at time as its store reports it: stored, or lost
    /// (bit `i` of `fallback`) so the store falls back to the object's
    /// last-modified time, set to the same instant. The two date a commit
    /// differently (the provenance table), so both matter.
    fn created_at(fallback: u32, i: usize, at: i64) -> CreatedAt {
        let at = Timestamp::from_unix_millis(at);
        if fallback >> i & 1 == 1 {
            CreatedAt::fallback(at)
        } else {
            CreatedAt::mtime(at)
        }
    }

    fn exact(r: &R, created_at: CreatedAt) -> FileInfo {
        FileInfo {
            key: r.key(),
            size: 1,
            listed: ListedTime::Created(created_at),
        }
    }

    fn store_of(files: &[R], resolved: &[i64], fallback: u32) -> MemStore {
        let store = MemStore::with_listed_last_modified();
        block_on(async {
            for (i, (r, &created_at)) in files.iter().zip(resolved).enumerate() {
                let at = Timestamp::from_unix_millis(created_at);
                let mut up = store
                    .begin_upload(&r.key(), at, WriteMode::Replace)
                    .await
                    .unwrap();
                up.write(b"x").await.unwrap();
                up.commit().await.unwrap();
                store.set_last_modified(&r.key(), at);
                if fallback >> i & 1 == 1 {
                    store.drop_created_at(&r.key());
                }
            }
        });
        store
    }

    /// One inventory under one created-at assignment and one choice of lost
    /// metadata: for every instant the exact planner is held to the spec and,
    /// when `lazily`, the lazy planner, over every mix of exact and
    /// last-modified-only listings, to the exact planner's whole outcome.
    fn check_as_of(
        files: &[R],
        resolved: &[i64],
        (fallback, lazily): (u32, bool),
        instants: &[i64],
        tally: &mut Tally,
    ) {
        let times: Vec<CreatedAt> = (0..files.len())
            .map(|i| created_at(fallback, i, resolved[i]))
            .collect();
        let exact_inv =
            Inventory::from_files(files.iter().zip(&times).map(|(r, c)| exact(r, *c))).unwrap();
        let store = store_of(files, resolved, fallback);
        let lazy = block_on(Inventory::load(&store)).unwrap();
        for &t in instants {
            let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(t));
            let eager = plan_reference(&exact_inv, target);
            let case = format!("t={t} files={files:?} created-at times={times:?} -> {eager:?}");
            let judgement = judged(&exact_inv, target, &eager, &case);
            note(tally, format!("{} by {:?}", variant(&eager), judgement.by));
            let direct = AsOfPlanner::new(&exact_inv, Timestamp::from_unix_millis(t))
                .advance(&ResolvedCreatedAt::default());
            assert_eq!(direct, Resolve::Done(eager.clone()), "{case}");
            let want = spec_view::outcome(&eager);
            let masks = if lazily { 1u32 << files.len() } else { 0 };
            for mask in 0..masks {
                // Bit i set: file i is listed exact; clear: last-modified only.
                let listed: Vec<FileInfo> = lazy
                    .files()
                    .iter()
                    .map(|l| {
                        let i = files.iter().position(|r| r.is(&l.key)).unwrap();
                        if mask >> i & 1 == 1 {
                            exact(&files[i], times[i])
                        } else {
                            *l
                        }
                    })
                    .collect();
                let mixed = Inventory::from_files(listed).unwrap();
                let mut planner = AsOfPlanner::new(&mixed, Timestamp::from_unix_millis(t));
                let got = block_on(resolve(&store, &mut planner)).unwrap();
                assert_eq!(
                    spec_view::outcome(&got.value),
                    want,
                    "lazy against eager, mask={mask:b}: {case}"
                );
            }
        }
    }

    fn sweep_as_of(
        d: u64,
        levels: &[u8],
        n: usize,
        resolved: &[i64],
        tally: &mut Tally,
    ) -> (u64, u64) {
        let mut instants: Vec<i64> = resolved.to_vec();
        instants.push(resolved.iter().max().unwrap() + 100);
        let keys = universe(d, levels);
        let (mut inventories, mut cases) = (0u64, 0u64);
        subsets(&keys, n, &mut |files| {
            inventories += 1;
            // Lost metadata only matters below level 9: a snapshot's time
            // dates no commit either way. The lazy planner reads times through
            // the store whatever their origin, so it runs with none lost and
            // with all lost; the spec judges every mix.
            let droppable: u32 = files
                .iter()
                .enumerate()
                .filter(|(_, r)| r.level < 9)
                .map(|(i, _)| 1 << i)
                .sum();
            assignments(resolved, files.len(), &mut Vec::new(), &mut |assigned| {
                for fallback in (0..=droppable).filter(|m| m & !droppable == 0) {
                    let lazily = fallback == 0 || fallback == droppable;
                    check_as_of(files, assigned, (fallback, lazily), &instants, tally);
                    let runs = if lazily { 1 + (1u64 << files.len()) } else { 1 };
                    cases += u64::try_from(instants.len()).unwrap() * runs;
                }
            });
        });
        (inventories, cases)
    }

    #[test]
    fn every_small_inventory_plans_as_of_the_same_lazily() {
        let mut tally = Tally::new();
        let counts = sweep_as_of(3, &[0, 1, 9], 3, &[100, 200, 300], &mut tally);
        report(
            "as-of D=3 levels {0,1,9} N<=3, 3 created-at times",
            counts,
            &tally,
        );
        let mut tally = Tally::new();
        let counts = sweep_as_of(4, &[0, 1, 9], 2, &[100, 200, 300], &mut tally);
        report(
            "as-of D=4 levels {0,1,9} N<=2, 3 created-at times",
            counts,
            &tally,
        );
    }

    #[test]
    #[ignore = "the large sweep: minutes in debug; run with `just plan-exhaustive`"]
    fn every_larger_inventory_plans_as_of_the_same_lazily() {
        let mut tally = Tally::new();
        let counts = sweep_as_of(5, &[0, 1, 8, 9], 3, &[100, 200, 300, 400], &mut tally);
        report(
            "as-of D=5 levels {0,1,8,9} N<=3, 4 created-at times",
            counts,
            &tally,
        );
        let mut tally = Tally::new();
        let counts = sweep_as_of(4, &[0, 1, 9], 3, &[100, 200, 300], &mut tally);
        report(
            "as-of D=4 levels {0,1,9} N<=3, 3 created-at times",
            counts,
            &tally,
        );
    }
}
