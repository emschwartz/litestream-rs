//! Live checks against the pinned Go binaries: `just compat` builds them and runs these
//! (`--run-ignored only`). They are not part of `just check`, which has no Go toolchain.

use std::fs;
use std::path::Path;

use lsr_compat::{
    CompatError, GoRestore, GoVersion, Manifest, Outcome, Scenario, Target, all_fixtures, generate,
    go_binary, hash_pages, integrity_check, restore_with_go, write_config,
};

fn copy_tree(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let dest = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &dest)?;
        } else {
            fs::copy(entry.path(), dest)?;
        }
    }
    Ok(())
}

fn latest_pages(fixture: &Path) -> Result<Vec<String>, CompatError> {
    let m = Manifest::load(fixture)?;
    m.targets
        .into_iter()
        .find_map(|t| match t.outcome {
            Outcome::Restored { page_hashes, .. } if t.target == Target::Latest => {
                Some(page_hashes)
            }
            Outcome::Restored { .. } | Outcome::Gap { .. } => None,
        })
        .ok_or_else(|| CompatError::Fixture("no restored latest target".to_owned()))
}

/// Regenerating twice yields the same final database: timing changes how many L0 files the
/// replica holds, never the pages the latest restore returns.
#[test]
#[ignore = "runs the pinned Go binary; just compat"]
fn regenerating_twice_gives_identical_pages() {
    let version = GoVersion::V0_5_18;
    let scenario = Scenario::Small4096;
    let (a, b) = (
        tempfile::tempdir().expect("a"),
        tempfile::tempdir().expect("b"),
    );
    let first = generate(version, scenario, a.path()).expect("first run");
    let second = generate(version, scenario, b.path()).expect("second run");
    assert_eq!(first.final_page_count, second.final_page_count);
    let dir = |root: &Path| root.join(version.name()).join(scenario.name());
    let fresh = latest_pages(&dir(a.path())).expect("pages");
    assert_eq!(fresh, latest_pages(&dir(b.path())).expect("pages"));
    let committed = lsr_compat::fixture_dir(version.name(), scenario.name());
    assert_eq!(
        fresh,
        latest_pages(&committed).expect("pages"),
        "the committed fixture drifted from a fresh run"
    );
}

/// Every committed fixture still restores, with the Go version that wrote it, to exactly what
/// its manifest records (a refusal for the gap fixture).
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_still_answers_what_the_manifests_say() {
    for source in all_fixtures().expect("list") {
        let work = tempfile::tempdir().expect("tempdir");
        let fixture = work.path().join("fixture");
        copy_tree(&source, &fixture).expect("copy");
        let m = Manifest::load(&fixture).expect("load");
        m.apply_mtimes(&fixture).expect("mtimes");
        let version = GoVersion::parse(&m.go_version).expect("version");
        let binary = go_binary(version).expect("binary");
        let (config, db_path) = (work.path().join("restore.yml"), work.path().join("from.db"));
        write_config(&config, &db_path, &Manifest::replica_dir(&fixture)).expect("config");
        for (i, entry) in m.targets.iter().enumerate() {
            let out = work.path().join(format!("out-{i}.db"));
            let got =
                restore_with_go(&binary, &config, &db_path, &out, entry.target).expect("restore");
            let label = format!("{} {} {:?}", m.go_version, m.scenario, entry.target);
            match &entry.outcome {
                Outcome::Restored { page_hashes, .. } => {
                    assert_eq!(got, GoRestore::Restored, "{label}");
                    integrity_check(&out).expect("integrity");
                    assert_eq!(
                        &hash_pages(&out).expect("hash").page_hashes,
                        page_hashes,
                        "{label}"
                    );
                }
                Outcome::Gap { go_refused, .. } => {
                    assert_eq!(
                        matches!(got, GoRestore::Refused { .. }),
                        *go_refused,
                        "{label}"
                    );
                }
            }
        }
    }
}
