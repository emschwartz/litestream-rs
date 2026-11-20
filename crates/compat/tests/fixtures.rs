//! Fixture self-consistency: what is committed under `fixtures/` is complete, within budget
//! and internally coherent. Needs no Go binary, so it runs in `just check`.

use lsr_compat::{
    GoVersion, KeyLayout, LocalKey, Manifest, Outcome, SNAPSHOT_LEVEL, Scenario, Target,
    all_fixtures, fixture_dir,
};

const FIXTURE_BUDGET: u64 = 1024 * 1024;
const TOTAL_BUDGET: u64 = 12 * 1024 * 1024;

#[test]
fn every_version_and_scenario_has_a_fixture() {
    let have = all_fixtures().expect("list");
    assert_eq!(have.len(), GoVersion::ALL.len() * Scenario::ALL.len());
    for version in GoVersion::ALL {
        for scenario in Scenario::ALL {
            let dir = fixture_dir(version.name(), scenario.name());
            assert!(have.contains(&dir), "missing {}", dir.display());
            let m = Manifest::load(&dir).expect("load");
            assert_eq!(
                (m.go_version.as_str(), m.scenario.as_str()),
                (version.name(), scenario.name())
            );
            assert_eq!(
                (m.page_size, m.seed),
                (scenario.page_size(), scenario.seed())
            );
        }
    }
}

#[test]
fn manifests_match_their_replicas() {
    for dir in all_fixtures().expect("list") {
        Manifest::load(&dir)
            .expect("load")
            .check(&dir)
            .expect("self-consistent");
    }
}

#[test]
fn replicas_hold_every_level_and_two_snapshots() {
    for dir in all_fixtures().expect("list") {
        let m = Manifest::load(&dir).expect("load");
        let keys: Vec<LocalKey> = m
            .files
            .iter()
            .map(|f| LocalKey::parse(&f.key, KeyLayout::File).expect("key"))
            .collect();
        let at = |pred: fn(&LocalKey) -> bool| keys.iter().filter(|k| pred(k)).count();
        assert!(at(|k| k.level == 0) >= 1, "{}: no L0", dir.display());
        assert!(
            at(|k| k.level > 0 && k.level < SNAPSHOT_LEVEL) >= 1,
            "{}: no compacted level",
            dir.display()
        );
        assert!(
            at(|k| k.is_snapshot()) >= 2,
            "{}: fewer than two snapshots",
            dir.display()
        );
    }
}

#[test]
fn fixtures_stay_within_budget() {
    let mut total = 0;
    for dir in all_fixtures().expect("list") {
        let m = Manifest::load(&dir).expect("load");
        let size: u64 = m.files.iter().map(|f| f.size).sum::<u64>()
            + std::fs::metadata(dir.join("manifest.json"))
                .expect("meta")
                .len();
        assert!(size <= FIXTURE_BUDGET, "{} is {size} bytes", dir.display());
        total += size;
    }
    assert!(total <= TOTAL_BUDGET, "fixtures total {total} bytes");
}

#[test]
fn scenarios_leave_their_mark_in_the_manifest() {
    for dir in all_fixtures().expect("list") {
        let m = Manifest::load(&dir).expect("load");
        let gap = m
            .targets
            .iter()
            .any(|t| matches!(t.outcome, Outcome::Gap { .. }));
        assert_eq!(
            gap,
            m.scenario == Scenario::MidChainGap.name(),
            "{}",
            dir.display()
        );
        if m.scenario == Scenario::VacuumShrink.name() {
            assert!(
                m.final_page_count < m.max_page_count,
                "{}: vacuum did not shrink",
                dir.display()
            );
        }
        if !gap {
            let latest = m
                .targets
                .iter()
                .find(|t| t.target == Target::Latest)
                .expect("latest");
            let Outcome::Restored { page_count, .. } = &latest.outcome else {
                panic!("latest not restored")
            };
            assert_eq!(*page_count, m.final_page_count, "{}", dir.display());
        }
    }
}

#[test]
fn the_gap_fixture_records_the_hole_not_just_gos_answer() {
    for dir in all_fixtures().expect("list") {
        let m = Manifest::load(&dir).expect("load");
        if m.scenario != Scenario::MidChainGap.name() {
            continue;
        }
        for t in &m.targets {
            let Outcome::Gap {
                have_through,
                next_starts,
                ..
            } = t.outcome
            else {
                panic!("not a gap outcome")
            };
            assert!(
                have_through + 1 < next_starts,
                "{}: no hole between {have_through} and {next_starts}",
                dir.display()
            );
        }
    }
}
