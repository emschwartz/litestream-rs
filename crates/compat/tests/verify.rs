//! The verifier against every Go-written fixture: no damage found where there is
//! none, and exactly the recorded hole where there is one. Needs no Go binary.

#![expect(
    clippy::expect_used,
    reason = "helpers of an integration test fail the test by panicking"
)]

use lsr_compat::{Manifest, Outcome, Target, all_fixtures};
use lsr_core::conformance::block_on;
use lsr_core::{Depth, Finding, PlanOutcome, Report, Severity, verify};
use lsr_store_fs::FsStore;

fn run(fixture: &std::path::Path, depth: Depth) -> Report {
    let manifest = Manifest::load(fixture).expect("manifest");
    // A git checkout resets mtimes, and a file replica reads creation time from them.
    manifest.apply_mtimes(fixture).expect("mtimes");
    let store = FsStore::new(Manifest::replica_dir(fixture));
    block_on(verify(&store, depth)).expect("verify walks the whole replica")
}

fn unrestorable(report: &Report) -> Vec<&Finding> {
    report
        .findings()
        .iter()
        .filter(|finding| finding.severity() == Severity::Unrestorable)
        .collect()
}

#[test]
fn every_fixture_verifies_at_both_depths_except_the_recorded_gap() {
    let fixtures = all_fixtures().expect("list");
    assert!(!fixtures.is_empty());
    let (mut checked, mut gaps) = (0_usize, 0_usize);
    for fixture in fixtures {
        let manifest = Manifest::load(&fixture).expect("manifest");
        let files = manifest.files.len();
        let bytes: u64 = manifest.files.iter().map(|file| file.size).sum();
        assert!(files > 0 && bytes > 0, "{}", fixture.display());
        let names = run(&fixture, Depth::Names);
        let contents = run(&fixture, Depth::Contents);
        assert_eq!((names.files_read(), names.bytes_read()), (0, 0));
        assert_eq!(
            (contents.files_read(), contents.bytes_read()),
            (files, bytes),
            "{}: every file is read, whole",
            fixture.display()
        );
        let total: usize = contents.levels().iter().map(|level| level.files()).sum();
        assert_eq!(total, files, "{}", fixture.display());
        for report in [&names, &contents] {
            let latest = manifest
                .targets
                .iter()
                .find(|entry| entry.target == Target::Latest)
                .expect("latest target");
            match &latest.outcome {
                Outcome::Gap {
                    have_through,
                    next_starts,
                    ..
                } => {
                    gaps += 1;
                    let [
                        Finding::Gap {
                            have_through: through,
                            next_starts: starts,
                            at_tip: true,
                            ..
                        },
                    ] = unrestorable(report).as_slice()
                    else {
                        panic!("{}: {:?}", fixture.display(), report.findings());
                    };
                    assert_eq!(
                        (through.get(), starts.get()),
                        (*have_through, *next_starts),
                        "{}",
                        fixture.display()
                    );
                    assert!(matches!(report.latest(), PlanOutcome::Gap { .. }));
                }
                Outcome::Restored { .. } => {
                    assert!(
                        unrestorable(report).is_empty(),
                        "{}: {:?}",
                        fixture.display(),
                        report.findings()
                    );
                    assert!(matches!(report.latest(), PlanOutcome::Plan(_)));
                }
            }
        }
        checked += 1;
    }
    assert!(checked >= 12, "only {checked} fixtures were checked");
    assert!(gaps >= 4, "only {gaps} gap fixtures were checked");
}
