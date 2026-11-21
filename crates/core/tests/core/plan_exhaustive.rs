//! Every small inventory, not a sample: the planner against brute-force oracles.
//!
//! Proptest samples; this enumerates. The key-target sweep builds every
//! inventory of up to N files over transactions 1..=D and a few levels (the
//! snapshot level 9 among them), plans each for `Latest` and for every `Txid`
//! target up to one past the end, and holds the outcome to an oracle written
//! from the contract in the `plan` module doc, not from the planner's code:
//! the set of transactions some chain of eligible files can end at, found by
//! fixpoint, and for a refusal the whole outcome, payloads included. The `AsOf` sweep enumerates smaller inventories under every
//! assignment of distinct exact created-at times and every instant, and holds the lazy
//! planner (over lazy, exact and mixed listings) to the exact one.
//!
//! The contract decides every case, so the oracle demands exactly the one
//! outcome it names; the tally printed with `--no-capture` counts the cases per
//! outcome so an empty bucket shows.

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the enumeration and oracle do arithmetic on tiny bounded transaction ids and indexes"
)]
mod exhaustive {
    use std::cmp::Reverse;
    use std::collections::BTreeMap;

    use lsr_core::conformance::block_on;
    use lsr_core::{
        AsOfPlanner, ContiguousChain, CreatedAt, CreatedAtStep, FileInfo, FileKey, Inventory,
        KeyTarget, Level, ListedTime, MemStore, ObjectStore, PlanOutcome, Resolve,
        ResolvedCreatedAt, RestoreTarget, Upload, plan, plan_reference, resolve,
    };
    use lsr_ltx::{Timestamp, Txid, TxidRange};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct R {
        level: u8,
        min: u64,
        max: u64,
    }

    impl R {
        /// A level-9 file that does not start at transaction 1 is refused as a
        /// snapshot and can never be applied.
        fn usable(&self) -> bool {
            self.level < 9 || self.min == 1
        }

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

    // ---- the key-target oracle, from the module doc's contract ----

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

        /// Latest admits every file; Txid admits a last transaction at or below it.
        fn admits(self, f: &R) -> bool {
            match self {
                Self::Latest => true,
                Self::Txid(n) => f.max <= n,
            }
        }
    }

    /// Every transaction some chain of admitted files can end at: a base is an
    /// admitted file beginning at 1 (any level); a step is an admitted file of
    /// levels 0 to 8 that starts at most one past an end and passes it.
    fn chain_ends(files: &[R], t: T) -> [bool; 32] {
        let mut ends = [false; 32];
        for f in files.iter().filter(|f| t.admits(f) && f.min == 1) {
            ends[usize::try_from(f.max).unwrap()] = true;
        }
        loop {
            let mut grew = false;
            for f in files.iter().filter(|f| t.admits(f) && f.level < 9) {
                let reachable =
                    (0..f.max).any(|e| ends[usize::try_from(e).unwrap()] && f.min <= e + 1);
                let slot = &mut ends[usize::try_from(f.max).unwrap()];
                if reachable && !*slot {
                    *slot = true;
                    grew = true;
                }
            }
            if !grew {
                return ends;
            }
        }
    }

    fn furthest(ends: &[bool; 32]) -> Option<u64> {
        (0..32u64)
            .rev()
            .find(|e| ends[usize::try_from(*e).unwrap()])
    }

    type Tally = BTreeMap<String, u64>;

    fn note(tally: &mut Tally, what: String) {
        *tally.entry(what).or_default() += 1;
    }

    fn assert_chain(chain: &ContiguousChain, files: &[R], t: T, goal: u64, inv: &Inventory) {
        assert_eq!(chain.snapshot(), &chain.files()[0]);
        assert_eq!(chain.reaches().get(), goal, "ends at the target");
        let mut reach = 0u64;
        for (i, applied) in chain.files().iter().enumerate() {
            assert!(inv.files().contains(applied), "a file the replica holds");
            let r = files.iter().find(|r| r.is(&applied.key)).unwrap();
            assert!(t.admits(r), "eligible: {r:?} for {t:?}");
            if i == 0 {
                assert_eq!(r.min, 1, "the base begins at transaction 1");
            } else {
                assert!(r.level < 9, "no snapshot extends a chain");
                assert!(r.min <= reach + 1, "contiguous: {r:?} after {reach}");
                assert!(r.max > reach, "each step advances");
            }
            reach = r.max;
        }
        assert_eq!(reach, goal);
    }

    /// The outcome the contract demands when it is not a plan, computed from
    /// the files alone: the first refusal in precedence order, with every
    /// payload (requirement and design tier alike). `None` means a plan is due.
    fn expected_refusal(files: &[R], t: T) -> Option<PlanOutcome> {
        let txid = |n: u64| Txid::new(n).unwrap();
        let Some(newest) = files.iter().map(|f| f.max).max() else {
            return Some(PlanOutcome::EmptyReplica);
        };
        let goal = match t {
            T::Latest => newest,
            T::Txid(n) if n > newest => {
                return Some(PlanOutcome::BeyondNewest {
                    requested: txid(n),
                    newest: txid(newest),
                });
            }
            T::Txid(n) => n,
        };
        let ends = chain_ends(files, t);
        if ends[usize::try_from(goal).unwrap()] {
            return None;
        }
        // The reach: the furthest any chain gets, 0 when no chain starts.
        let r = furthest(&ends).unwrap_or(0);
        if let T::Txid(n) = t
            && let Some(f) = files
                .iter()
                .filter(|f| f.usable() && f.min <= r + 1 && f.max > n)
                .min_by_key(|f| (f.max, Reverse(f.level), f.min))
        {
            return Some(PlanOutcome::InsideFile {
                requested: txid(n),
                file: f.key(),
            });
        }
        if !files.iter().any(|f| t.admits(f) && f.min == 1) {
            let mut malformed: Vec<&R> = files.iter().filter(|f| !f.usable()).collect();
            malformed.sort_by_key(|f| (f.level, f.min, f.max));
            return Some(PlanOutcome::NoSnapshot {
                oldest_snapshot: files
                    .iter()
                    .filter(|f| f.min == 1)
                    .map(|f| txid(f.max))
                    .min(),
                malformed_snapshots: malformed.into_iter().map(R::key).collect(),
            });
        }
        let past = || files.iter().filter(|f| f.max > r);
        if let Some(next) = past()
            .filter(|f| f.usable() && f.min > r + 1)
            .map(|f| f.min)
            .min()
        {
            return Some(PlanOutcome::Gap {
                have_through: txid(r),
                next_starts: txid(next),
            });
        }
        let refused = past()
            .filter(|f| !f.usable())
            .min_by_key(|f| (f.level, f.min, f.max))
            .unwrap_or_else(|| panic!("no refusal applies: {files:?} {t:?}"));
        Some(PlanOutcome::MalformedSnapshot {
            have_through: txid(r),
            file: refused.key(),
        })
    }

    /// Holds one `plan` outcome to the contract: a plan exactly when a chain
    /// reaches the target, else the whole outcome the contract names.
    fn verify(files: &[R], t: T, tally: &mut Tally) {
        let inv = Inventory::from_files(files.iter().map(|r| info(r, 0))).unwrap();
        let out = plan(&inv, t.key_target());
        let case = format!("{files:?} {t:?} -> {out:?}");
        let name = if let Some(want) = expected_refusal(files, t) {
            assert_eq!(out, want, "{case}");
            format!("{out:?}")
                .split([' ', '('])
                .next()
                .unwrap()
                .to_owned()
        } else {
            let PlanOutcome::Plan(chain) = &out else {
                panic!("a chain to the target exists but the outcome is not a plan: {case}");
            };
            let goal = match t {
                T::Latest => files.iter().map(|f| f.max).max().unwrap(),
                T::Txid(n) => n,
            };
            assert_chain(chain, files, t, goal, &inv);
            "Plan".to_owned()
        };
        note(tally, name);
    }

    /// One sweep: every inventory of up to `n` files over `1..=d` with `levels`.
    /// Returns (inventories, cases).
    fn sweep_keys(d: u64, levels: &[u8], n: usize, tally: &mut Tally) -> (u64, u64) {
        let keys = universe(d, levels);
        let (mut inventories, mut cases) = (0u64, 0u64);
        subsets(&keys, n, &mut |files| {
            inventories += 1;
            verify(files, T::Latest, tally);
            cases += 1;
            for target in 1..=d + 1 {
                verify(files, T::Txid(target), tally);
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

    /// The default-suite sweep: levels 0, 1, 8 and 9, up to four files over 1..=5.
    #[test]
    fn every_small_inventory_plans_as_the_contract_says() {
        let mut tally = Tally::new();
        let counts = sweep_keys(5, &[0, 1, 8, 9], 4, &mut tally);
        report("key targets D=5 levels {0,1,8,9} N<=4", counts, &tally);
    }

    /// The large sweep (`just plan-exhaustive`): five files, and a wider range.
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

    // ---- AsOf: the lazy planner against the exact one ----

    /// A listing's outcome reduced to keys, so a lazy listing's compares with an exact one's.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum O {
        Plan(Vec<FileKey>),
        Inside(Vec<FileKey>, FileKey),
        Other(String),
    }

    fn reduce(outcome: &PlanOutcome) -> O {
        let keys = |chain: &ContiguousChain| chain.files().iter().map(|i| i.key).collect();
        match outcome {
            PlanOutcome::Plan(chain) => O::Plan(keys(chain)),
            PlanOutcome::AsOfInsideFile { chain, file } => O::Inside(keys(chain), *file),
            other @ (PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::Gap { .. }
            | PlanOutcome::InsideFile { .. }
            | PlanOutcome::MalformedSnapshot { .. }) => O::Other(format!("{other:?}")),
        }
    }

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

    fn store_of(files: &[R], resolved: &[i64]) -> MemStore {
        let store = MemStore::with_listed_last_modified();
        block_on(async {
            for (r, &created_at) in files.iter().zip(resolved) {
                let at = Timestamp::from_unix_millis(created_at);
                let mut up = store.begin_upload(&r.key(), at).await.unwrap();
                up.write(b"x").await.unwrap();
                up.commit().await.unwrap();
            }
        });
        store
    }

    /// One inventory under one created-at time assignment: every instant, every mix of
    /// exact and last-modified-only listings.
    fn check_as_of(files: &[R], resolved: &[i64], instants: &[i64]) {
        let exact =
            Inventory::from_files(files.iter().zip(resolved).map(|(r, s)| info(r, *s))).unwrap();
        let store = store_of(files, resolved);
        let lazy = block_on(Inventory::load(&store)).unwrap();
        for mask in 0u32..1 << files.len() {
            // Bit i set: file i is listed exact; clear: last-modified only.
            let listed: Vec<FileInfo> = lazy
                .files()
                .iter()
                .map(|l| {
                    let i = files.iter().position(|r| r.is(&l.key)).unwrap();
                    if mask >> i & 1 == 1 {
                        info(&files[i], resolved[i])
                    } else {
                        *l
                    }
                })
                .collect();
            let mixed = Inventory::from_files(listed).unwrap();
            for &t in instants {
                let instant = Timestamp::from_unix_millis(t);
                let want = reduce(&plan_reference(&exact, RestoreTarget::AsOf(instant)));
                let mut planner = AsOfPlanner::new(&mixed, instant);
                let got = block_on(resolve(&store, &mut planner)).unwrap();
                assert_eq!(
                    reduce(&got.value),
                    want,
                    "mask={mask:b} t={t} files={files:?} created-at times={resolved:?}"
                );
                if mask == (1 << files.len()) - 1 {
                    assert_eq!(
                        got.created_at_read, 0,
                        "an exact listing reads no created-at time"
                    );
                    let direct =
                        AsOfPlanner::new(&mixed, instant).advance(&ResolvedCreatedAt::default());
                    assert_eq!(
                        direct.clone(),
                        Resolve::Done(plan_reference(&mixed, RestoreTarget::AsOf(instant)))
                    );
                }
            }
        }
    }

    fn sweep_as_of(d: u64, levels: &[u8], n: usize, resolved: &[i64]) -> (u64, u64) {
        let mut instants: Vec<i64> = resolved.to_vec();
        instants.push(resolved.iter().max().unwrap() + 100);
        let keys = universe(d, levels);
        let (mut inventories, mut cases) = (0u64, 0u64);
        subsets(&keys, n, &mut |files| {
            inventories += 1;
            assignments(resolved, files.len(), &mut Vec::new(), &mut |assigned| {
                check_as_of(files, assigned, &instants);
                cases += u64::try_from(instants.len()).unwrap() << files.len();
            });
        });
        (inventories, cases)
    }

    #[test]
    fn every_small_inventory_plans_as_of_the_same_lazily() {
        let counts = sweep_as_of(4, &[0, 1, 9], 3, &[100, 200, 300]);
        report(
            "as-of D=4 levels {0,1,9} N<=3, 3 created-at times",
            counts,
            &Tally::new(),
        );
    }

    #[test]
    #[ignore = "the large sweep: minutes in debug; run with `just plan-exhaustive`"]
    fn every_larger_inventory_plans_as_of_the_same_lazily() {
        let counts = sweep_as_of(5, &[0, 1, 8, 9], 3, &[100, 200, 300, 400]);
        report(
            "as-of D=5 levels {0,1,8,9} N<=3, 3 created-at times",
            counts,
            &Tally::new(),
        );
    }
}
