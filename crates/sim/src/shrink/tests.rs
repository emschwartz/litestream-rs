use std::mem::discriminant;

use proptest::prelude::*;

use super::*;
use crate::oracle::bug_fault;

/// A synthetic failure: the list holds an operation of the kind of $first, and a later one of the
/// kind of $second.
fn holds_in_order(ops: &[Op], first: &Op, second: &Op) -> bool {
    let mut seen_first = false;
    for op in ops {
        if seen_first && discriminant(op) == discriminant(second) {
            return true;
        }
        seen_first |= discriminant(op) == discriminant(first);
    }
    false
}

fn drawn(seed: u64, steps: u32) -> Vec<Op> {
    history(Seed::new(seed), steps, &Weights::default())
}

proptest! {
    /// Whatever the predicate and the list, the output still fails, is never longer than the
    /// input, and no simpler form of any operation left still fails: ddmin's fixpoint.
    #[test]
    fn shrinking_keeps_the_failure_and_never_lengthens(
        seed in any::<u64>(),
        steps in 2_u32..200,
        a in any::<prop::sample::Index>(),
        b in any::<prop::sample::Index>(),
    ) {
        let ops = drawn(seed, steps);
        let (i, j) = (a.index(ops.len()), b.index(ops.len()));
        let (first, second) = (ops[i].clone(), ops[j].clone());
        let fails = |list: &[Op]| holds_in_order(list, &first, &second);
        prop_assume!(fails(&ops));
        let (kept, runs, capped) = minimize(&ops, ops.len(), 1, fails);
        prop_assert!(fails(&kept));
        prop_assert!(kept.len() <= ops.len());
        prop_assert!(runs <= MAX_RUNS);
        prop_assert!(!capped);
        for (index, op) in kept.iter().enumerate() {
            for form in simpler(op) {
                let mut other = kept.clone();
                other[index] = form;
                prop_assert!(!fails(&other), "{op:?} has a simpler form that still fails");
            }
        }
        // Removing any one operation breaks an in-order pair, so the pair is what is left.
        prop_assert!(kept.len() <= 2, "{kept:?}");
    }
}

#[test]
fn a_budget_stops_the_search_and_the_result_still_fails() {
    // Two hundred operations are needed of a thousand: ddmin cannot finish inside the budget.
    let ops: Vec<Op> = (0..1000_u16)
        .map(|n| if n % 5 == 0 { Op::Heal } else { Op::Tick })
        .collect();
    let needed = |list: &[Op]| list.iter().filter(|op| **op == Op::Heal).count() >= 150;
    let (kept, runs, capped) = minimize(&ops, ops.len(), 1, needed);
    assert!(needed(&kept));
    assert!(runs <= MAX_RUNS);
    assert!(kept.len() < ops.len());
    assert_eq!(capped, runs >= MAX_RUNS);
}

#[test]
fn fields_shrink_toward_their_minimum() {
    let ops = [
        Op::Commit {
            pages: 200,
            grow: true,
        },
        Op::ClockJump(-93_600_000),
    ];
    let (kept, _, _) = minimize(&ops, ops.len(), 1, |_| true);
    assert!(kept.is_empty());
    let needs_big = |list: &[Op]| {
        list.iter()
            .any(|op| matches!(op, Op::Commit { pages, .. } if *pages >= 7))
    };
    let (kept, _, _) = minimize(&ops, ops.len(), 1, needs_big);
    assert_eq!(
        kept,
        vec![Op::Commit {
            pages: 7,
            grow: false
        }]
    );
}

#[test]
fn the_class_is_the_check_and_its_kind_never_the_text() {
    let bound = |what, seen| Violation::ResourceBound {
        what,
        seen,
        limit: 2,
    };
    assert_eq!(
        Class::of(&bound(Bound::WalLength, 9)),
        Class::of(&bound(Bound::WalLength, 40))
    );
    assert_ne!(
        Class::of(&bound(Bound::WalLength, 9)),
        Class::of(&bound(Bound::ListCalls, 9))
    );
    assert_eq!(
        Class::of(&bug_fault("one")),
        Class::of(&bug_fault("another"))
    );
    assert_ne!(
        Class::of(&bug_fault("one")),
        Class::of(&bound(Bound::WalLength, 9))
    );
}

fn planted(step: u32) -> Opts {
    Opts {
        plant: Some((step, bug_fault("planted"))),
        ..Opts::default()
    }
}

/// A real run: the planted violation is raised at the twentieth-first operation, so that many
/// are the least that fail; the shrinker finds exactly them, and the list it prints fails again.
#[test]
fn a_real_failing_run_shrinks_to_the_operations_that_reach_the_failure() {
    let weights = Weights::default();
    let shrunk = shrink(Seed::new(2), 200, &weights, &planted(20)).expect("fails");
    assert_eq!(shrunk.before, 200);
    assert_eq!(shrunk.ops.len(), 21);
    assert!(!shrunk.capped && shrunk.runs <= MAX_RUNS);
    assert_eq!(
        Class::of(&shrunk.violation),
        Class::of(&bug_fault("planted"))
    );
    let again = run_ops(Seed::new(2), &shrunk.ops, &shrunk.opts).expect_err("still fails");
    assert_eq!(
        Class::of(&again.violation),
        Class::of(&bug_fault("planted"))
    );
    assert_eq!(
        shrink(Seed::new(2), 200, &weights, &Opts::default()).map(|s| s.ops),
        None
    );
    let source = scenario_source(Seed::new(2), &shrunk).expect("printable");
    assert!(source.contains("let ops = ["), "{source}");
    assert!(source.contains("drawn: true"), "{source}");
    assert!(
        source.contains("run_ops(Seed::new(0x0000000000000002)"),
        "{source}"
    );
}

#[test]
fn the_same_failure_shrinks_to_the_same_list() {
    let weights = Weights::default();
    let run = || shrink(Seed::new(3), 80, &weights, &planted(9)).expect("fails");
    let (a, b) = (run(), run());
    assert_eq!((a.ops, a.runs), (b.ops, b.runs));
}
