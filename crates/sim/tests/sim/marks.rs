//! The soak's gate bites: it fails a soak whose generator stopped drawing a class.

use lsr_sim::marks::{CANARY, gate};
use lsr_sim::{Reach, Seed, Weights, run};

/// Sums the reach of `seeds` under their mixes, with `edit` applied to each mix.
fn reach_of(seeds: std::ops::RangeInclusive<u64>, edit: impl Fn(&mut Weights)) -> Reach {
    let mut sum = Reach::default();
    for n in seeds {
        let seed = Seed::new(n);
        let mut weights = Weights::mixed(seed);
        edit(&mut weights);
        if let Ok(report) = run(seed, 300, &weights) {
            sum.merge(&report.reach);
        }
    }
    sum
}

/// A generator that stops drawing a class fails the gate: the soak counterpart of a planted
/// bug. With the second writer's weight at 0 the same seeds leave its class at zero, which the
/// gate reports; with it as drawn, they do not. The canary is a zero in both.
#[test]
fn a_weight_set_to_zero_fails_the_gate() {
    let key = "fault.SecondWriter.drawn";
    let drawn = gate(&reach_of(1..=16, |_| ()));
    assert!(!drawn.zeros.iter().any(|zero| zero == key), "{drawn:?}");
    let silenced = gate(&reach_of(1..=16, |weights| weights.second_writer = 0));
    assert!(
        silenced.zeros.iter().any(|zero| zero == key),
        "{silenced:?}"
    );
    assert!(!silenced.passed());
    for judged in [drawn, silenced] {
        assert!(judged.zeros.iter().any(|zero| zero == CANARY));
    }
}

/// Each coverage scenario reaches the marks it names, every one of them is a mark the gate
/// requires (a scenario for an exempt mark would hide that the exemption went stale), and no two
/// name the same mark.
#[test]
fn the_coverage_scenarios_reach_their_required_marks() {
    let required = lsr_sim::marks::required_keys();
    let mut named = Vec::new();
    for scenario in lsr_sim::scenario::coverage() {
        let reached = scenario.run().unwrap_or_else(|why| panic!("{why}"));
        assert!(!scenario.keys().is_empty(), "{}", scenario.name());
        for key in scenario.keys() {
            assert!(required.contains(key), "{key} is not a required mark");
            assert!(reached.get(key) > 0, "{key}");
            assert!(!named.contains(key), "{key} is named twice");
            named.push(key.clone());
        }
    }
}

/// The whole required population, closed: every mark the gate requires is named by a coverage
/// scenario, or falls in a family `LEFT_TO_THE_SEEDS` gives a reason for, never both. A new
/// mark of a family with no reason fails here until it has a scenario or one is written.
#[test]
fn every_required_mark_has_a_scenario_or_a_reason() {
    let covered: Vec<String> = lsr_sim::scenario::coverage()
        .iter()
        .flat_map(|scenario| scenario.keys().to_vec())
        .collect();
    let family = |key: &str| {
        lsr_sim::scenario::LEFT_TO_THE_SEEDS
            .iter()
            .find(|(prefix, _)| key.starts_with(prefix))
    };
    let required: Vec<String> = lsr_sim::marks::required_keys()
        .into_iter()
        .filter(|key| key != CANARY)
        .collect();
    assert!(!required.is_empty(), "the gate requires nothing");
    let orphans: Vec<&String> = required
        .iter()
        .filter(|key| !covered.contains(key) && family(key).is_none())
        .collect();
    assert!(
        orphans.is_empty(),
        "no scenario and no reason for {orphans:?}"
    );
    let both: Vec<&String> = covered.iter().filter(|key| family(key).is_some()).collect();
    assert!(both.is_empty(), "a scenario and a reason for {both:?}");
    for (prefix, why) in lsr_sim::scenario::LEFT_TO_THE_SEEDS {
        assert!(!why.is_empty(), "{prefix}");
        assert!(
            required.iter().any(|key| key.starts_with(prefix)),
            "{prefix} names no required mark"
        );
    }
}
