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
