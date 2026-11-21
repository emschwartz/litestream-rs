//! The as-of planner in query form: [`AsOfPlanner`] over a listing that carries
//! only last-modified times, driven by [`resolve`], against [`plan_reference`] over the same
//! files with every stamp exact.

#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the generator and fixtures do arithmetic on small bounded transaction ids and stamps"
)]
mod lazy {
    use std::cmp::Reverse;
    use std::collections::BTreeSet;

    use lsr_core::conformance::block_on;
    use lsr_core::{
        AsOfPlanner, FileInfo, FileKey, Inventory, Level, ListedStamp, MemStore, ObjectStore,
        PlanOutcome, Resolve, RestoreTarget, StampStep, Stamps, StoredStamp, Upload,
        plan_reference, resolve,
    };
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use proptest::prelude::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct F {
        level: u8,
        min: u64,
        max: u64,
        stamp: i64,
    }

    fn f(level: u8, min: u64, max: u64, stamp: i64) -> F {
        F {
            level,
            min,
            max,
            stamp,
        }
    }

    fn key(f: &F) -> FileKey {
        let r = TxidRange::new(Txid::new(f.min).unwrap(), Txid::new(f.max).unwrap()).unwrap();
        FileKey::new(Level::new(f.level).unwrap(), r)
    }

    fn at(ms: i64) -> Timestamp {
        Timestamp::from_unix_millis(ms)
    }

    fn stored(f: &F) -> FileInfo {
        FileInfo {
            key: key(f),
            size: 1,
            listed: ListedStamp::Stored(StoredStamp::mtime(at(f.stamp))),
        }
    }

    /// The outcome with each file reduced to its key, so a lazy listing's
    /// outcome compares with an exact one's.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum O {
        Plan(Vec<FileKey>),
        Inside(Vec<FileKey>, FileKey),
        Other(String),
    }

    fn reduce(outcome: &PlanOutcome) -> O {
        let keys =
            |chain: &lsr_core::ContiguousChain| chain.files().iter().map(|i| i.key).collect();
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

    fn reference(files: &[F], t: i64) -> O {
        let inv = Inventory::from_files(files.iter().map(stored)).unwrap();
        reduce(&plan_reference(&inv, RestoreTarget::AsOf(at(t))))
    }

    /// A store holding the files, listed lazily or exactly.
    fn store(files: &[F], lazy: bool) -> MemStore {
        let store = if lazy {
            MemStore::with_listed_last_modified()
        } else {
            MemStore::new()
        };
        block_on(async {
            for f in files {
                let mut up = store.begin_upload(&key(f), at(f.stamp)).await.unwrap();
                up.write(b"x").await.unwrap();
                up.commit().await.unwrap();
            }
        });
        store
    }

    /// What one lazy run decided and which stamps it asked for, in order.
    struct Run {
        outcome: O,
        asked: Vec<FileKey>,
        stamps_read: u64,
        comparisons: u64,
    }

    fn run_on(store: &MemStore, t: i64) -> Run {
        let inv = block_on(Inventory::load(store)).unwrap();
        let mut planner = AsOfPlanner::new(&inv, at(t));
        let mut asked = Vec::new();
        let (outcome, stamps_read) = {
            let mut step = |stamps: &Stamps| {
                let next = planner.advance(stamps);
                if let Resolve::Need(info) = &next {
                    asked.push(info.key);
                }
                next
            };
            let got = block_on(resolve(store, &mut step)).unwrap();
            (reduce(&got.value), got.stamps_read)
        };
        Run {
            outcome,
            asked,
            stamps_read,
            comparisons: planner.comparisons(),
        }
    }

    /// Runs the lazy planner and checks it against the exact one.
    fn check(files: &[F], t: i64) -> Run {
        let store = store(files, true);
        let run = run_on(&store, t);
        assert_eq!(run.outcome, reference(files, t), "t={t} {files:?}");
        assert_eq!(run.stamps_read, u64::try_from(run.asked.len()).unwrap());
        assert_eq!(store.stamp_calls(), run.stamps_read);
        run
    }

    /// The bound: transaction-1 files plus the level 0-8 files that touch and
    /// pass the chain's end at some step, the chain modelled here from keys
    /// and exact stamps.
    fn bound(files: &[F], t: i64) -> BTreeSet<FileKey> {
        let mut out: BTreeSet<FileKey> = files.iter().filter(|f| f.min == 1).map(key).collect();
        let pref = |f: &F| (f.max, f.level, Reverse(f.min));
        let base = files
            .iter()
            .filter(|f| f.min == 1 && f.stamp < t)
            .max_by_key(|f| (f.max, f.level, Reverse(key(f))));
        let Some(base) = base else { return out };
        if base.level != 9 && base.min != 1 {
            return out;
        }
        let mut reach = base.max;
        loop {
            let passing: Vec<&F> = files
                .iter()
                .filter(|f| f.level < 9 && f.min <= reach.saturating_add(1) && f.max > reach)
                .collect();
            out.extend(passing.iter().map(|f| key(f)));
            match passing
                .iter()
                .filter(|f| f.stamp < t)
                .max_by_key(|f| pref(f))
            {
                Some(next) => reach = next.max,
                None => return out,
            }
        }
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }

        fn stamp(&mut self, n: u64) -> i64 {
            i64::try_from(self.below(n)).unwrap()
        }
    }

    /// Up to [`count`] files over transactions 1..=[`n`]. Mode 0: stamps at
    /// random; mode 1: stamps non-decreasing in the last transaction; mode 2:
    /// the writer's shape, level 0-8 files stamped as their last transaction
    /// and a snapshot any time after it.
    fn listing(rng: &mut Rng, n: u64, count: u64, mode: u8) -> Vec<F> {
        let levels = [0u8, 0, 0, 1, 1, 2, 9, 9];
        let mut files: Vec<F> = Vec::new();
        for _ in 0..1 + rng.below(count) {
            let level = levels[usize::try_from(rng.below(8)).unwrap()];
            let min = if level == 9 || rng.below(5) == 0 {
                1
            } else {
                1 + rng.below(n)
            };
            let max = min + rng.below(n - min + 1);
            let file = f(level, min, max, 0);
            if !files.iter().any(|g| key(g) == key(&file)) {
                files.push(file);
            }
        }
        match mode {
            2 => {
                let mut of = vec![0i64];
                for _ in 0..=n + 1 {
                    let last = *of.last().unwrap();
                    of.push(last + rng.stamp(3) * 100);
                }
                for file in &mut files {
                    let base = of[usize::try_from(file.max).unwrap()];
                    file.stamp = if file.level == 9 {
                        base + rng.stamp(4) * 100
                    } else {
                        base
                    };
                }
            }
            1 => {
                files.sort_by_key(|file| file.max);
                let mut s = 0;
                for file in &mut files {
                    s += rng.stamp(3) * 100;
                    file.stamp = s;
                }
            }
            _ => {
                for file in &mut files {
                    file.stamp = rng.stamp(6) * 100;
                }
            }
        }
        files
    }

    fn instants(files: &[F]) -> Vec<i64> {
        let mut v: BTreeSet<i64> = BTreeSet::from([-1, 1_000_000]);
        for file in files {
            v.extend([file.stamp - 1, file.stamp, file.stamp + 1]);
        }
        v.into_iter().collect()
    }

    /// One generated listing at every instant that matters: the lazy plan is
    /// the exact one, asks each stamp once and stays within the bound.
    fn survey_case(seed: u64, n: u64, count: u64, mode: u8) {
        let files = listing(&mut Rng(seed | 1), n, count, mode);
        for t in instants(&files) {
            let run = check(&files, t);
            let distinct: BTreeSet<FileKey> = run.asked.iter().copied().collect();
            assert_eq!(distinct.len(), run.asked.len(), "a stamp read twice");
            let allowed = bound(&files, t);
            assert!(
                distinct.is_subset(&allowed),
                "read outside S + E at t={t}: {files:?}"
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]

        #[test]
        fn plan_as_of_equals_the_exact_planner(
            seed in any::<u64>(),
            n in 2u64..8,
            count in 2u64..8,
            mode in 0u8..3,
        ) {
            survey_case(seed, n, count, mode);
        }

        /// Stamps the planner did not read are set to the extremes; it asks for
        /// the same stamps and decides the same.
        #[test]
        fn unread_stamps_cannot_change_the_outcome(
            seed in any::<u64>(),
            n in 2u64..8,
            count in 2u64..8,
            mode in 0u8..3,
        ) {
            let files = listing(&mut Rng(seed | 1), n, count, mode);
            for t in instants(&files) {
                let first = check(&files, t);
                let read: BTreeSet<FileKey> = first.asked.iter().copied().collect();
                for extreme in [i64::MIN, i64::MAX] {
                    let moved: Vec<F> = files
                        .iter()
                        .map(|file| if read.contains(&key(file)) { *file } else { F { stamp: extreme, ..*file } })
                        .collect();
                    let again = run_on(&store(&moved, true), t);
                    prop_assert_eq!(&again.asked, &first.asked);
                    prop_assert_eq!(&again.outcome, &first.outcome);
                }
            }
        }
    }

    /// Many more cases than the suite runs; for a soak.
    #[test]
    #[ignore = "a soak: 600k listings"]
    fn plan_as_of_soak() {
        for mode in 0u8..3 {
            for case in 0..200_000u64 {
                let seed = case.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ u64::from(mode);
                survey_case(seed, 2 + case % 6, 2 + (case / 6) % 6, mode);
            }
        }
    }

    #[test]
    fn plan_as_of_reads_within_the_bound() {
        let files = [
            f(9, 1, 2, 100),
            f(2, 3, 3, 300),
            f(0, 4, 4, 500),
            f(2, 3, 4, 600),
            f(1, 3, 4, 600),
            f(0, 1, 4, 700),
        ];
        let run = check(&files, 501);
        let asked: BTreeSet<FileKey> = run.asked.iter().copied().collect();
        assert!(asked.is_subset(&bound(&files, 501)));
    }

    #[test]
    fn plan_as_of_reads_each_stamp_once() {
        // The straddle rule's candidates are the files the last step read.
        let files = [f(9, 1, 3, 300), f(1, 4, 10, 1000), f(0, 4, 4, 1000)];
        let run = check(&files, 950);
        let distinct: BTreeSet<FileKey> = run.asked.iter().copied().collect();
        assert_eq!(distinct.len(), run.asked.len());
    }

    #[test]
    fn plan_as_of_ignores_last_modified() {
        let files = [f(9, 1, 2, 100), f(0, 3, 3, 200), f(0, 4, 4, 300)];
        let store = store(&files, true);
        // Last-modified times that would flip every file if read as stamps.
        store.set_last_modified(&key(&files[0]), at(1_000));
        store.set_last_modified(&key(&files[1]), at(0));
        store.set_last_modified(&key(&files[2]), at(0));
        let run = run_on(&store, 250);
        assert_eq!(run.outcome, reference(&files, 250));
        assert_eq!(run.outcome, O::Plan(vec![key(&files[0]), key(&files[1])]));
    }

    #[test]
    fn as_of_on_a_stored_listing_reads_nothing() {
        let files = [f(9, 1, 2, 100), f(0, 3, 3, 200), f(1, 3, 6, 600)];
        let store = store(&files, false);
        let run = run_on(&store, 250);
        assert_eq!((run.stamps_read, store.stamp_calls()), (0, 0));
        assert_eq!(run.outcome, reference(&files, 250));
        // A planner given an exact listing decides on its first advance.
        let inv = Inventory::from_files(files.iter().map(stored)).unwrap();
        let mut planner = AsOfPlanner::new(&inv, at(250));
        let got = planner.advance(&Stamps::default());
        assert_eq!(
            got,
            Resolve::Done(plan_reference(&inv, RestoreTarget::AsOf(at(250))))
        );
        assert_eq!(planner.advance(&Stamps::default()), got);
    }

    #[test]
    fn an_empty_replica_reads_nothing() {
        let run = run_on(&MemStore::with_listed_last_modified(), 0);
        assert_eq!(
            run.outcome,
            O::Other(format!("{:?}", PlanOutcome::EmptyReplica))
        );
        assert_eq!(run.stamps_read, 0);
    }

    #[test]
    fn an_older_snapshot_with_a_later_one_beyond_plans() {
        let files = [f(9, 1, 1, 100), f(9, 1, 2, 300)];
        assert_eq!(check(&files, 101).outcome, O::Plan(vec![key(&files[0])]));
    }

    #[test]
    fn an_instant_inside_a_file_before_a_later_snapshot() {
        let files = [f(9, 1, 3, 300), f(1, 4, 10, 1000), f(9, 1, 10, 1100)];
        let run = check(&files, 950);
        // Both later files straddle and end at 10; the higher level is named.
        assert_eq!(run.outcome, O::Inside(vec![key(&files[0])], key(&files[2])));
    }

    /// A snapshot ending one past the chain is stamped when written, not when
    /// that transaction committed, so it does not lift the straddle: the lazy
    /// planner names the compacted file, as it does without the snapshot.
    #[test]
    fn a_later_snapshot_ending_right_after_the_chain_does_not_hide_a_straddle() {
        let files = [f(0, 1, 1, 100), f(9, 1, 2, 1000), f(1, 2, 5, 500)];
        let run = check(&files, 300);
        assert_eq!(run.outcome, O::Inside(vec![key(&files[0])], key(&files[2])));
        assert_eq!(check(&[files[0], files[2]], 300).outcome, run.outcome);
    }

    /// Ruling A1: an eligible file past the end that does not connect is a
    /// stray; it neither extends the chain nor removes the straddle.
    #[test]
    fn a_stray_does_not_hide_a_straddle() {
        let files = [f(9, 1, 3, 300), f(1, 4, 10, 1000), f(0, 12, 12, 100)];
        let run = check(&files, 950);
        assert_eq!(run.outcome, O::Inside(vec![key(&files[0])], key(&files[1])));
        assert!(
            !run.asked.contains(&key(&files[2])),
            "a stray's stamp was read"
        );
    }

    #[test]
    fn a_stray_does_not_stop_a_plan() {
        let files = [
            f(9, 1, 2, 100),
            f(0, 3, 3, 200),
            f(0, 4, 4, 900),
            f(0, 6, 6, 150),
        ];
        let run = check(&files, 250);
        assert_eq!(run.outcome, O::Plan(vec![key(&files[0]), key(&files[1])]));
    }

    /// The hole fix: nothing usable touches the end, so the instant cannot be
    /// certified and the answer is what Latest reports.
    #[test]
    fn as_of_before_a_hole_reports_the_gap() {
        let files = [f(0, 1, 2, 100), f(0, 5, 6, 200)];
        let run = check(&files, 101);
        assert!(
            matches!(run.outcome, O::Other(ref s) if s.starts_with("Gap")),
            "{:?}",
            run.outcome
        );
    }

    /// About 10^4 files in the writer's shape: comparisons stay within a
    /// constant of N log N, and the reads are the snapshots plus a handful.
    #[test]
    fn plan_as_of_is_linear() {
        let mut files = Vec::new();
        let stamp = |max: u64| i64::try_from(max).unwrap() * 10;
        for n in 1..=9_000u64 {
            files.push(f(0, n, n, stamp(n)));
        }
        for n in 0..900u64 {
            files.push(f(1, n * 10 + 1, n * 10 + 10, stamp(n * 10 + 10)));
        }
        for n in 0..90u64 {
            files.push(f(2, n * 100 + 1, n * 100 + 100, stamp(n * 100 + 100)));
        }
        for n in 1..=9u64 {
            files.push(f(9, 1, n * 1_000, stamp(n * 1_000) + 5));
        }
        let t = stamp(1_003) + 1;
        let run = check(&files, t);
        let chain = match &run.outcome {
            O::Plan(chain) => chain.len(),
            other @ (O::Inside(..) | O::Other(_)) => panic!("{other:?}"),
        };
        assert_eq!(chain, 4);
        let big_n = u64::try_from(files.len()).unwrap();
        let log = u64::from(big_n.ilog2()) + 1;
        assert!(
            run.comparisons < 40 * big_n * log,
            "{} comparisons",
            run.comparisons
        );
        let starts = files.iter().filter(|f| f.min == 1).count();
        let starts = u64::try_from(starts).unwrap();
        assert!(
            run.stamps_read < 2 * 4 + starts + 10,
            "{} stamps read",
            run.stamps_read
        );
    }

    /// The count is a measurement, not a placeholder: sorting k files takes at
    /// least k - 1 comparisons, and every file that enters the heap adds more.
    #[test]
    fn comparisons_count_both_sorts_and_every_heap_step() {
        let mut files = vec![f(9, 1, 4, 45)];
        files.extend((1..=8u64).map(|n| f(0, n, n, i64::try_from(n).unwrap() * 10)));
        let store = store(&files, false);
        let inv = block_on(Inventory::load(&store)).unwrap();
        let mut planner = AsOfPlanner::new(&inv, at(1_000));
        let sorted = planner.comparisons();
        // Two transaction-1 files (one comparison at least) and eight
        // level-0 files (seven at least).
        let least = 1 + 7;
        assert!(sorted >= least, "{sorted} comparisons for two sorts");
        let mut step = |stamps: &Stamps| planner.advance(stamps);
        let got = block_on(resolve(&store, &mut step)).unwrap();
        assert_eq!(
            reduce(&got.value),
            O::Plan(vec![
                key(&files[0]),
                key(&f(0, 5, 5, 0)),
                key(&f(0, 6, 6, 0)),
                key(&f(0, 7, 7, 0)),
                key(&f(0, 8, 8, 0))
            ])
        );
        assert!(
            planner.comparisons() > sorted,
            "the heap steps added nothing to {sorted}"
        );
    }

    /// The step contract seen from outside: an exact listing is decided on the
    /// first advance whichever way the planner is driven.
    #[test]
    fn the_planner_is_a_stamp_step() {
        fn decide<S: StampStep>(step: &mut S) -> Resolve<S::Output> {
            step.advance(&Stamps::default())
        }
        let files = [f(9, 1, 2, 100)];
        let inv = Inventory::from_files(files.iter().map(stored)).unwrap();
        let got = decide(&mut AsOfPlanner::new(&inv, at(101)));
        assert!(matches!(got, Resolve::Done(PlanOutcome::Plan(_))));
    }
}
