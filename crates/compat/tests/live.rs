//! Live checks against the pinned Go binaries: `just compat` builds them and runs these
//! (`--run-ignored only`). They are not part of `just check`, which has no Go toolchain.

use std::path::Path;

use lsr_compat::{
    CompatError, GoRestore, GoVersion, Manifest, Outcome, Scenario, Target, all_fixtures,
    build_verify_helper, copy_tree, generate, go_binary, go_verify, hash_pages, integrity_check,
    restore_replica, restore_with_go, stage, write_config,
};
use lsr_core::{PlanOutcome, RestoreError, RestoreTarget};
use lsr_ltx::Timestamp;

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

/// The Go versions whose restore reads the block page encoding Rust writes.
const READS_BLOCKS: [GoVersion; 3] = [GoVersion::V0_5_16, GoVersion::V0_5_18, GoVersion::Head];
/// The versions that read only LZ4 frames: the one-directional break the matrix records.
const READS_FRAMES: [GoVersion; 2] = [GoVersion::V0_5_0, GoVersion::V0_5_15];

/// Every replica file of every fixture, rewritten by `lsr_ltx::Encoder`, restores under each
/// Go version from v0.5.16 up to the pages the manifest records; v0.5.0 and v0.5.15 refuse.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_restores_what_rust_encoded() {
    for source in all_fixtures().expect("list") {
        let manifest = Manifest::load(&source).expect("load");
        let staged = stage(&source, &manifest).expect("stage");
        staged.reencode(&manifest).expect("re-encode");
        let (config, db_path) = (staged.output("restore.yml"), staged.output("from.db"));
        write_config(&config, &db_path, &staged.replica()).expect("config");
        let mut n = 0_u32;
        let mut run = |version: GoVersion, target: Target| {
            n = n.saturating_add(1);
            let out = staged.output(&format!("go-{n}.db"));
            let binary = go_binary(version).expect("binary");
            let got = restore_with_go(&binary, &config, &db_path, &out, target).expect("restore");
            (got, out)
        };
        for entry in &manifest.targets {
            let Outcome::Restored { page_hashes, .. } = &entry.outcome else {
                continue;
            };
            for version in READS_BLOCKS {
                let label = format!(
                    "{} reading {} {} {:?}",
                    version.name(),
                    manifest.go_version,
                    manifest.scenario,
                    entry.target
                );
                let (got, out) = run(version, entry.target);
                assert_eq!(got, GoRestore::Restored, "{label}");
                integrity_check(&out).expect("integrity");
                assert_eq!(
                    &hash_pages(&out).expect("hash").page_hashes,
                    page_hashes,
                    "{label}"
                );
            }
        }
        if manifest
            .targets
            .iter()
            .any(|t| matches!(t.outcome, Outcome::Restored { .. }))
        {
            for version in READS_FRAMES {
                let (got, _) = run(version, Target::Latest);
                assert!(
                    matches!(got, GoRestore::Refused { .. }),
                    "{} unexpectedly read a block-encoded replica of {} {}",
                    version.name(),
                    manifest.go_version,
                    manifest.scenario
                );
            }
        }
    }
}

/// The reference reader, `ltx.Decoder.Verify` from the pinned library, accepts every file
/// Rust re-encoded.
#[test]
#[ignore = "builds and runs the Go helper; just compat"]
fn go_verifies_every_file_rust_encoded() {
    let tools = tempfile::tempdir().expect("tempdir");
    let helper = build_verify_helper(tools.path()).expect("build the helper");
    for source in all_fixtures().expect("list") {
        let manifest = Manifest::load(&source).expect("load");
        let staged = stage(&source, &manifest).expect("stage");
        staged.reencode(&manifest).expect("re-encode");
        let files: Vec<_> = manifest
            .files
            .iter()
            .map(|f| staged.replica().join(&f.key))
            .collect();
        go_verify(&helper, &files)
            .unwrap_or_else(|e| panic!("{} {}: {e}", manifest.go_version, manifest.scenario));
    }
}

/// Instants the manifests do not record, between every two file times: where Rust restores,
/// Go (the version that wrote the fixture) restores the same pages; where Rust reports the
/// instant inside a compacted file, Go's answer equals Rust's restore of the chain's end;
/// where nothing is old enough, Go refuses.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_rust_agree_on_instants_between_files() {
    for source in all_fixtures().expect("list") {
        let manifest = Manifest::load(&source).expect("load");
        if manifest.scenario == "mid-chain-gap" {
            continue;
        }
        let staged = stage(&source, &manifest).expect("stage");
        let version = GoVersion::parse(&manifest.go_version).expect("version");
        let binary = go_binary(version).expect("binary");
        let (config, db_path) = (staged.output("restore.yml"), staged.output("from.db"));
        write_config(&config, &db_path, &staged.replica()).expect("config");
        for (i, at) in manifest.midpoints().into_iter().enumerate() {
            let label = format!("{} {} as-of {at}", manifest.go_version, manifest.scenario);
            let go_out = staged.output(&format!("go-{i}.db"));
            let go = restore_with_go(
                &binary,
                &config,
                &db_path,
                &go_out,
                Target::AsOf { unix_ms: at },
            )
            .expect("go");
            let rs_out = staged.output(&format!("rs-{i}.db"));
            let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(at));
            let rs = restore_replica(&staged.replica(), target, &rs_out).expect("sink");
            let (compare, want) = match (&rs, &go) {
                (Ok(_), GoRestore::Restored) => (true, rs_out),
                (
                    Err(RestoreError::NoPlan(PlanOutcome::AsOfInsideFile { chain, .. })),
                    GoRestore::Restored,
                ) => {
                    let end = staged.output(&format!("rs-chain-{i}.db"));
                    restore_replica(
                        &staged.replica(),
                        RestoreTarget::Txid(chain.reaches()),
                        &end,
                    )
                    .expect("sink")
                    .expect("the chain's own end restores");
                    (true, end)
                }
                (
                    Err(RestoreError::NoPlan(PlanOutcome::NoSnapshot { .. })),
                    GoRestore::Refused { .. },
                ) => (false, rs_out),
                // Deliberately stricter: with no snapshot old enough Go starts a chain at an L0
                // file that begins at transaction 1; the planner requires a level-9 base
                // (plan.rs, `only_l0`). Go's result must still be a sound database.
                (
                    Err(RestoreError::NoPlan(PlanOutcome::NoSnapshot { .. })),
                    GoRestore::Restored,
                ) => {
                    integrity_check(&go_out).expect("Go's snapshot-less restore is sound");
                    (false, rs_out)
                }
                other => panic!("{label}: Rust and Go disagree: {other:?}"),
            };
            if compare {
                assert_eq!(
                    hash_pages(&go_out).expect("hash").page_hashes,
                    hash_pages(&want).expect("hash").page_hashes,
                    "{label}"
                );
            }
        }
    }
}
