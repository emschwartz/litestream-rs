//! The verifier against every Go-written fixture: no damage found where there is
//! none, and exactly the recorded hole where there is one. Needs no Go binary.

use lsr_compat::{Manifest, Outcome, Target, all_fixtures};
use lsr_core::conformance::block_on;
use lsr_core::{
    Depth, Finding, MemSink, PlanOutcome, Report, RestoreTarget, Severity, restore, verify,
};
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

/// The walk reads every file whole; the latest restore then reads its plan
/// again, and there is no plan for a gap.
fn assert_reads(fixture: &std::path::Path, contents: &Report, files: usize, bytes: u64) {
    let reads = (contents.files_read(), contents.bytes_read());
    if matches!(contents.latest(), PlanOutcome::Plan(_)) {
        assert!(
            reads.0 > files && reads.1 > bytes,
            "{}: {reads:?}",
            fixture.display()
        );
    } else {
        assert_eq!(reads, (files, bytes), "{}", fixture.display());
    }
}

/// Unrestorable exactly when the real latest restore fails.
fn assert_agrees_with_restore(fixture: &std::path::Path, contents: &Report) {
    let restored = block_on(restore(
        &FsStore::new(Manifest::replica_dir(fixture)),
        RestoreTarget::Latest,
        MemSink::new(),
    ));
    assert_eq!(
        contents.worst() == Some(Severity::Unrestorable),
        restored.is_err(),
        "{}: restore {restored:?}, verify {:?}",
        fixture.display(),
        contents.findings()
    );
}

/// What a report must say about the manifest's recorded latest outcome;
/// true for a gap.
fn assert_latest(fixture: &std::path::Path, manifest: &Manifest, report: &Report) -> bool {
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
            assert!(
                matches!(
                    unrestorable(report).as_slice(),
                    [Finding::Gap { have_through: through, next_starts: starts, at_newest: true, .. }]
                        if (through.get(), starts.get()) == (*have_through, *next_starts)
                ),
                "{}: {:?}",
                fixture.display(),
                report.findings()
            );
            assert!(matches!(report.latest(), PlanOutcome::Gap { .. }));
            true
        }
        Outcome::Restored { .. } => {
            assert!(
                unrestorable(report).is_empty(),
                "{}: {:?}",
                fixture.display(),
                report.findings()
            );
            assert!(matches!(report.latest(), PlanOutcome::Plan(_)));
            false
        }
    }
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
        assert_reads(&fixture, &contents, files, bytes);
        assert_agrees_with_restore(&fixture, &contents);
        let total: usize = contents.levels().iter().map(|level| level.files()).sum();
        assert_eq!(total, files, "{}", fixture.display());
        let restored = block_on(restore(
            &FsStore::new(Manifest::replica_dir(&fixture)),
            RestoreTarget::Latest,
            MemSink::new(),
        ));
        assert_eq!(
            contents.worst() == Some(Severity::Unrestorable),
            restored.is_err(),
            "{}: restore {restored:?}, verify {:?}",
            fixture.display(),
            contents.findings()
        );
        for report in [&names, &contents] {
            if assert_latest(&fixture, &manifest, report) {
                gaps += 1;
            }
        }
        checked += 1;
    }
    assert!(checked >= 12, "only {checked} fixtures were checked");
    assert!(gaps >= 4, "only {gaps} gap fixtures were checked");
}
