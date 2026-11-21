//! Every small inventory, not a sample: the planner against brute-force oracles.
//!
//! Proptest samples; this enumerates. The key-target sweep builds every
//! inventory of up to N files over transactions 1..=D and a few levels (the
//! snapshot level 9 among them), plans each for `Latest` and for every `Txid`
//! target up to one past the end, and holds the outcome to an oracle written
//! from the contract in the `plan` module doc, not from the planner's code:
//! the set of transactions some chain of eligible files can end at, found by
//! fixpoint. The `AsOf` sweep enumerates smaller inventories under every
//! assignment of distinct exact stamps and every instant, and holds the lazy
//! planner (over lazy, exact and mixed listings) to the exact one.
//!
//! Where the module doc does not say which refusal wins, the oracle accepts
//! every refusal whose own text applies and tallies the case, so the code's
//! choice is neither encoded nor hidden (see the notes printed with
//! `--no-capture`).

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
        AsOfPlanner, ContiguousChain, FileInfo, FileKey, Inventory, KeyTarget, Level, ListedStamp,
        MemStore, ObjectStore, PlanOutcome, Resolve, RestoreTarget, StampStep, Stamps, StoredStamp,
        Upload, plan, plan_reference, resolve,
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

    fn info(r: &R, stamp: i64) -> FileInfo {
        FileInfo {
            key: r.key(),
            size: 1,
            listed: ListedStamp::Stored(StoredStamp::mtime(Timestamp::from_unix_millis(stamp))),
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

    /// The refusals whose own doc text applies, for a case with a base but no plan.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum Refusal {
        Inside,
        Gap,
        Malformed,
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

    /// Checks one `plan` outcome against the contract; panics with the case on failure.
    fn verify(files: &[R], t: T, tally: &mut Tally) {
        let inv = Inventory::from_files(files.iter().map(|r| info(r, 0))).unwrap();
        let out = plan(&inv, t.key_target());
        let case = format!("{files:?} {t:?} -> {out:?}");
        let Some(newest) = files.iter().map(|f| f.max).max() else {
            assert_eq!(out, PlanOutcome::EmptyReplica, "{case}");
            return;
        };
        if let T::Txid(n) = t
            && n > newest
        {
            // A request past everything held. NoSnapshot's own text ("none exists")
            // also applies when no file begins at 1; the doc does not rank the two.
            let beyond = PlanOutcome::BeyondNewest {
                requested: Txid::new(n).unwrap(),
                newest: Txid::new(newest).unwrap(),
            };
            if files.iter().any(|f| f.min == 1) {
                assert_eq!(out, beyond, "{case}");
            } else {
                let is_no_snapshot = matches!(out, PlanOutcome::NoSnapshot { .. });
                assert!(out == beyond || is_no_snapshot, "{case}");
                let chose = if is_no_snapshot {
                    "NoSnapshot"
                } else {
                    "BeyondNewest"
                };
                note(
                    tally,
                    format!("past the end, no base: [BeyondNewest, NoSnapshot] -> {chose}"),
                );
            }
            if let PlanOutcome::NoSnapshot { .. } = &out {
                assert_no_snapshot(&out, files, &case);
            }
            return;
        }
        let goal = match t {
            T::Latest => newest,
            T::Txid(n) => n,
        };
        let ends = chain_ends(files, t);
        let has_base = files.iter().any(|f| t.admits(f) && f.min == 1);
        if ends[usize::try_from(goal).unwrap()] {
            let PlanOutcome::Plan(chain) = &out else {
                panic!("a chain to {goal} exists but the outcome is not a plan: {case}");
            };
            assert_chain(chain, files, t, goal, &inv);
            return;
        }
        assert!(
            !matches!(out, PlanOutcome::Plan(_)),
            "a plan with no chain: {case}"
        );
        assert!(
            !matches!(
                out,
                PlanOutcome::AsOfInsideFile { .. } | PlanOutcome::EmptyReplica
            ),
            "{case}"
        );
        if !has_base {
            verify_no_base(&out, files, goal, tally, &case);
            return;
        }
        verify_refusal(&out, files, t, furthest(&ends).unwrap(), tally, &case);
    }

    /// No eligible base. A transaction-1 file that covers the target is
    /// InsideFile (the chain is empty, reach 0); otherwise no file begins at 1.
    fn verify_no_base(out: &PlanOutcome, files: &[R], goal: u64, tally: &mut Tally, case: &str) {
        let covers = |f: &&R| f.min == 1 && f.max > goal;
        let nearest = files
            .iter()
            .filter(covers)
            .min_by_key(|f| (f.max, Reverse(f.level), f.key()));
        if let Some(nearest) = nearest {
            let want = PlanOutcome::InsideFile {
                requested: Txid::new(goal).unwrap(),
                file: nearest.key(),
            };
            assert_eq!(out, &want, "{case}");
            note(
                tally,
                "no eligible base, a transaction-1 file covers the target: InsideFile".into(),
            );
        } else {
            assert_no_snapshot(out, files, case);
        }
    }

    fn assert_no_snapshot(out: &PlanOutcome, files: &[R], case: &str) {
        let PlanOutcome::NoSnapshot {
            oldest_snapshot,
            malformed_snapshots,
        } = out
        else {
            panic!("no eligible base must be NoSnapshot: {case}");
        };
        let oldest = files.iter().filter(|f| f.min == 1).map(|f| f.max).min();
        assert_eq!(oldest_snapshot.map(Txid::get), oldest, "{case}");
        let mut want: Vec<FileKey> = files
            .iter()
            .filter(|f| f.level == 9 && f.min != 1)
            .map(R::key)
            .collect();
        let mut got = malformed_snapshots.clone();
        want.sort();
        got.sort();
        assert_eq!(got, want, "{case}");
    }

    /// InsideFile's own text (Txid only): a usable file holds the requested
    /// transaction strictly before its end. Whether it must also link to the
    /// chain is not said; both readings are accepted.
    fn covering(files: &[R], t: T) -> Vec<&R> {
        match t {
            T::Txid(n) => files
                .iter()
                .filter(|f| f.usable() && f.min <= n && f.max > n)
                .collect(),
            T::Latest => Vec::new(),
        }
    }

    /// A base exists, no chain reaches the goal: the outcome is one of the
    /// refusals whose doc text applies, with the fields the doc defines.
    fn verify_refusal(out: &PlanOutcome, files: &[R], t: T, r: u64, tally: &mut Tally, case: &str) {
        let eligible_usable_beyond = |f: &&R| t.admits(f) && f.usable() && f.max > r;
        let inside = covering(files, t);
        // Gap: a usable file past the chain starts beyond a hole. Eligible-only
        // and all-files readings differ for Txid; either value is accepted.
        let hole = |f: &&R| f.usable() && f.max > r && f.min > r + 1;
        let gap_all: Option<u64> = files.iter().filter(hole).map(|f| f.min).min();
        let gap_eligible: Option<u64> = files
            .iter()
            .filter(|f| t.admits(f))
            .filter(hole)
            .map(|f| f.min)
            .min();
        // Malformed: the only history past the chain is in refused level-9 files.
        let refused: Vec<&R> = files.iter().filter(|f| !f.usable() && f.max > r).collect();
        let malformed = !refused.is_empty() && !files.iter().any(|f| eligible_usable_beyond(&f));

        let allowed: Vec<Refusal> = [
            (!inside.is_empty(), Refusal::Inside),
            (gap_all.is_some(), Refusal::Gap),
            (malformed, Refusal::Malformed),
        ]
        .into_iter()
        .filter_map(|(applies, refusal)| applies.then_some(refusal))
        .collect();
        assert!(
            !allowed.is_empty(),
            "no refusal applies, yet no plan: {case}"
        );
        let chose = match out {
            PlanOutcome::InsideFile { requested, file } => {
                assert!(inside.iter().any(|f| f.is(file)), "{case}");
                assert_eq!(
                    Some(requested.get()),
                    match t {
                        T::Txid(n) => Some(n),
                        T::Latest => None,
                    },
                    "{case}"
                );
                Refusal::Inside
            }
            PlanOutcome::Gap {
                have_through,
                next_starts,
            } => {
                assert_eq!(have_through.get(), r, "{case}");
                let next = Some(next_starts.get());
                assert!(next == gap_all || next == gap_eligible, "{case}");
                Refusal::Gap
            }
            PlanOutcome::MalformedSnapshot { have_through, file } => {
                assert_eq!(have_through.get(), r, "{case}");
                assert!(refused.iter().any(|f| f.is(file)), "{case}");
                Refusal::Malformed
            }
            other @ (PlanOutcome::Plan(_)
            | PlanOutcome::EmptyReplica
            | PlanOutcome::NoSnapshot { .. }
            | PlanOutcome::BeyondNewest { .. }
            | PlanOutcome::AsOfInsideFile { .. }) => {
                panic!("not a refusal for a case with a base: {other:?} {case}")
            }
        };
        assert!(
            allowed.contains(&chose),
            "{chose:?} not among {allowed:?}: {case}"
        );
        if allowed.len() > 1 {
            note(
                tally,
                format!("refusals applying together: {allowed:?} -> {chose:?}"),
            );
        }
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

    /// Calls `f` with every injective assignment of `stamps` to `n` files.
    fn assignments(stamps: &[i64], n: usize, held: &mut Vec<i64>, f: &mut impl FnMut(&[i64])) {
        if held.len() == n {
            f(held);
            return;
        }
        for &s in stamps {
            if !held.contains(&s) {
                held.push(s);
                assignments(stamps, n, held, f);
                held.pop();
            }
        }
    }

    fn store_of(files: &[R], stamps: &[i64]) -> MemStore {
        let store = MemStore::with_listed_last_modified();
        block_on(async {
            for (r, &stamp) in files.iter().zip(stamps) {
                let at = Timestamp::from_unix_millis(stamp);
                let mut up = store.begin_upload(&r.key(), at).await.unwrap();
                up.write(b"x").await.unwrap();
                up.commit().await.unwrap();
            }
        });
        store
    }

    /// One inventory under one stamp assignment: every instant, every mix of
    /// exact and last-modified-only listings.
    fn check_as_of(files: &[R], stamps: &[i64], instants: &[i64]) {
        let exact =
            Inventory::from_files(files.iter().zip(stamps).map(|(r, s)| info(r, *s))).unwrap();
        let store = store_of(files, stamps);
        let lazy = block_on(Inventory::load(&store)).unwrap();
        for mask in 0u32..1 << files.len() {
            // Bit i set: file i is listed exact; clear: last-modified only.
            let listed: Vec<FileInfo> = lazy
                .files()
                .iter()
                .map(|l| {
                    let i = files.iter().position(|r| r.is(&l.key)).unwrap();
                    if mask >> i & 1 == 1 {
                        info(&files[i], stamps[i])
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
                    "mask={mask:b} t={t} files={files:?} stamps={stamps:?}"
                );
                if mask == (1 << files.len()) - 1 {
                    assert_eq!(got.stamps_read, 0, "an exact listing reads no stamp");
                    let direct = AsOfPlanner::new(&mixed, instant).advance(&Stamps::default());
                    assert_eq!(
                        direct.clone(),
                        Resolve::Done(plan_reference(&mixed, RestoreTarget::AsOf(instant)))
                    );
                }
            }
        }
    }

    fn sweep_as_of(d: u64, levels: &[u8], n: usize, stamps: &[i64]) -> (u64, u64) {
        let mut instants: Vec<i64> = stamps.to_vec();
        instants.push(stamps.iter().max().unwrap() + 100);
        let keys = universe(d, levels);
        let (mut inventories, mut cases) = (0u64, 0u64);
        subsets(&keys, n, &mut |files| {
            inventories += 1;
            assignments(stamps, files.len(), &mut Vec::new(), &mut |assigned| {
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
            "as-of D=4 levels {0,1,9} N<=3, 3 stamps",
            counts,
            &Tally::new(),
        );
    }

    #[test]
    #[ignore = "the large sweep: minutes in debug; run with `just plan-exhaustive`"]
    fn every_larger_inventory_plans_as_of_the_same_lazily() {
        let counts = sweep_as_of(5, &[0, 1, 8, 9], 3, &[100, 200, 300, 400]);
        report(
            "as-of D=5 levels {0,1,8,9} N<=3, 3 stamps",
            counts,
            &Tally::new(),
        );
    }
}
