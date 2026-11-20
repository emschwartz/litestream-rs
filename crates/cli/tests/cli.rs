//! The built `lsr` binary against the committed Go-written fixtures. Needs no Go.
//! Restores are compared with the page hashes each fixture's manifest holds, and
//! the JSON outputs with what the library reports for the same replica.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
mod cli {
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::ffi::OsStringExt;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use lsr_compat::{Manifest, Outcome, Staged, Target, all_fixtures, hash_pages, stage};
    use lsr_core::conformance::block_on;
    use lsr_core::{Depth, Inventory, PlanOutcome, Report, Severity, verify};
    use lsr_ltx::Timestamp;
    use lsr_store_fs::FsStore;
    use serde_json::{Value, json};

    struct Run {
        code: i32,
        stdout: String,
        stderr: String,
    }

    fn lsr_os(args: &[OsString]) -> Run {
        let out = Command::new(env!("CARGO_BIN_EXE_lsr"))
            .args(args)
            .output()
            .expect("run lsr");
        Run {
            code: out.status.code().expect("exited with a code"),
            stdout: String::from_utf8(out.stdout).expect("stdout is UTF-8"),
            stderr: String::from_utf8(out.stderr).expect("stderr is UTF-8"),
        }
    }

    fn lsr(args: &[&str]) -> Run {
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        lsr_os(&args)
    }

    fn path_str(path: &Path) -> &str {
        path.to_str().expect("UTF-8 path")
    }

    fn fixtures() -> Vec<(PathBuf, Manifest)> {
        let list = all_fixtures().expect("list");
        assert!(!list.is_empty());
        list.into_iter()
            .map(|fixture| {
                let manifest = Manifest::load(&fixture).expect("manifest");
                (fixture, manifest)
            })
            .collect()
    }

    fn target_flags(target: Target) -> Vec<String> {
        match target {
            Target::Latest => Vec::new(),
            Target::Txid { txid } => vec!["--txid".into(), format!("{txid:016x}")],
            Target::AsOf { unix_ms } => vec![
                "--timestamp".into(),
                Timestamp::from_unix_millis(unix_ms).to_string(),
            ],
        }
    }

    fn restore(staged: &Staged, output: &Path, extra: &[String]) -> Run {
        let mut args = vec![
            "restore".to_owned(),
            "--replica".into(),
            path_str(&staged.replica()).to_owned(),
            "--output".into(),
            path_str(output).to_owned(),
        ];
        args.extend_from_slice(extra);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        lsr(&args)
    }

    fn dir_is_empty(dir: &Path) -> bool {
        fs::read_dir(dir).expect("read dir").next().is_none()
    }

    fn assert_pages_equal(path: &Path, want: &Outcome, label: &str) {
        let Outcome::Restored {
            page_size,
            page_count,
            page_hashes,
        } = want
        else {
            panic!("{label}: not a restored outcome")
        };
        let got = hash_pages(path).expect("hash the restored pages");
        assert_eq!(got.page_size, *page_size, "{label}: page size");
        assert_eq!(got.page_count, *page_count, "{label}: pages");
        assert_eq!(&got.page_hashes, page_hashes, "{label}: page hashes");
    }

    /// Every target of every fixture: the pages Go wrote, or a refusal that leaves nothing.
    #[test]
    fn every_target_matches_the_manifest() {
        let (mut restored, mut refused) = (0_usize, 0_usize);
        for (fixture, manifest) in fixtures() {
            let staged = stage(&fixture, &manifest).expect("stage");
            for entry in &manifest.targets {
                let label = format!(
                    "{} {} {:?}",
                    manifest.go_version, manifest.scenario, entry.target
                );
                let dir = tempfile::tempdir().expect("tempdir");
                let output = dir.path().join("out.db");
                let run = restore(&staged, &output, &target_flags(entry.target));
                match &entry.outcome {
                    Outcome::Restored { .. } if run.code == 0 => {
                        assert_pages_equal(&output, &entry.outcome, &label);
                        assert!(
                            run.stdout.contains("database checksum"),
                            "{label}: {}",
                            run.stdout
                        );
                        assert_eq!(run.stderr, "", "{label}");
                        restored += 1;
                    }
                    // An instant inside a compacted file has no exact plan; Go answered anyway.
                    Outcome::Restored { .. }
                        if run.code == 2 && matches!(entry.target, Target::AsOf { .. }) =>
                    {
                        assert!(
                            run.stderr.contains("falls inside"),
                            "{label}: {}",
                            run.stderr
                        );
                        assert!(dir_is_empty(dir.path()), "{label}");
                        refused += 1;
                    }
                    Outcome::Gap {
                        have_through,
                        next_starts,
                        ..
                    } if run.code == 2 => {
                        assert_eq!(run.stdout, "", "{label}");
                        assert_eq!(run.stderr.lines().count(), 1, "{label}: {}", run.stderr);
                        assert!(
                            run.stderr.contains(&format!("{have_through:016x}"))
                                && run.stderr.contains(&format!("{next_starts:016x}")),
                            "{label}: {}",
                            run.stderr
                        );
                        assert!(
                            dir_is_empty(dir.path()),
                            "{label}: left {:?}",
                            fs::read_dir(dir.path()).unwrap().collect::<Vec<_>>()
                        );
                        refused += 1;
                    }
                    want @ (Outcome::Restored { .. } | Outcome::Gap { .. }) => panic!(
                        "{label}: manifest {want:?}, lsr exited {} with {}",
                        run.code, run.stderr
                    ),
                }
            }
        }
        assert!(
            restored > 0 && refused > 0,
            "restored {restored}, refused {refused}"
        );
    }

    #[test]
    fn a_gap_target_exits_2_names_the_hole_and_leaves_no_file() {
        let mut seen = 0_usize;
        for (fixture, manifest) in fixtures() {
            let Some(entry) = manifest
                .targets
                .iter()
                .find(|e| e.target == Target::Latest && matches!(e.outcome, Outcome::Gap { .. }))
            else {
                continue;
            };
            let staged = stage(&fixture, &manifest).expect("stage");
            let dir = tempfile::tempdir().expect("tempdir");
            let output = dir.path().join("out.db");
            let run = restore(&staged, &output, &[]);
            assert_eq!((run.code, run.stdout.as_str()), (2, ""), "{}", run.stderr);
            assert!(run.stderr.contains("gap"), "{}", run.stderr);
            assert!(
                !output.exists() && dir_is_empty(dir.path()),
                "{:?}",
                entry.target
            );
            seen += 1;
        }
        assert!(seen > 0);
    }

    fn small_fixture() -> (Staged, Manifest) {
        let (fixture, manifest) = fixtures()
            .into_iter()
            .find(|(_, m)| m.scenario == "small-4096")
            .expect("a small fixture");
        (stage(&fixture, &manifest).expect("stage"), manifest)
    }

    fn latest_pages(manifest: &Manifest) -> &Outcome {
        &manifest
            .targets
            .iter()
            .find(|e| e.target == Target::Latest)
            .expect("latest")
            .outcome
    }

    #[test]
    fn an_existing_output_exits_3_and_stays_byte_identical() {
        let (staged, _) = small_fixture();
        let dir = tempfile::tempdir().expect("tempdir");
        let output = dir.path().join("out.db");
        fs::write(&output, b"not a database").expect("plant");
        let run = restore(&staged, &output, &[]);
        assert_eq!((run.code, run.stdout.as_str()), (3, ""));
        assert!(run.stderr.contains("already exists"), "{}", run.stderr);
        assert_eq!(fs::read(&output).expect("read"), b"not a database");
        assert_eq!(fs::read_dir(dir.path()).expect("dir").count(), 1);
    }

    #[test]
    fn a_stale_sidecar_refuses_until_clear_sidecars() {
        let (staged, manifest) = small_fixture();
        let dir = tempfile::tempdir().expect("tempdir");
        let output = dir.path().join("out.db");
        let journal = dir.path().join("out.db-journal");
        fs::write(&journal, b"stale").expect("plant");
        let refused = restore(&staged, &output, &[]);
        assert_eq!((refused.code, refused.stdout.as_str()), (3, ""));
        assert!(
            refused.stderr.contains("out.db-journal"),
            "{}",
            refused.stderr
        );
        assert!(
            refused.stderr.contains("--clear-sidecars"),
            "{}",
            refused.stderr
        );
        assert!(journal.exists() && !output.exists());

        let cleared = restore(&staged, &output, &["--clear-sidecars".to_owned()]);
        assert_eq!(cleared.code, 0, "{}", cleared.stderr);
        assert!(
            cleared.stdout.contains("out.db-journal"),
            "{}",
            cleared.stdout
        );
        assert!(!journal.exists());
        assert_pages_equal(&output, latest_pages(&manifest), "after clearing");
    }

    #[test]
    fn clear_sidecars_never_touches_an_existing_target() {
        let (staged, _) = small_fixture();
        let dir = tempfile::tempdir().expect("tempdir");
        let output = dir.path().join("out.db");
        let wal = dir.path().join("out.db-wal");
        fs::write(&output, b"live").expect("plant");
        fs::write(&wal, b"belongs to it").expect("plant");
        let run = restore(&staged, &output, &["--clear-sidecars".to_owned()]);
        assert_eq!(run.code, 3, "{}", run.stderr);
        assert_eq!(fs::read(&wal).expect("read"), b"belongs to it");
        assert_eq!(fs::read(&output).expect("read"), b"live");
    }

    fn latest_is_gap(manifest: &Manifest) -> bool {
        matches!(latest_pages(manifest), Outcome::Gap { .. })
    }

    #[test]
    fn verify_exit_codes_follow_the_latest_outcome() {
        let (mut clean, mut gaps) = (0_usize, 0_usize);
        for (fixture, manifest) in fixtures() {
            let staged = stage(&fixture, &manifest).expect("stage");
            let replica = staged.replica();
            for depth in ["names", "contents"] {
                let run = lsr(&["verify", "--replica", path_str(&replica), "--depth", depth]);
                let label = format!("{} {} {depth}", manifest.go_version, manifest.scenario);
                assert_eq!(run.stderr, "", "{label}");
                if latest_is_gap(&manifest) {
                    assert_eq!(run.code, 2, "{label}:\n{}", run.stdout);
                    assert!(
                        run.stdout.contains("unrestorable: gap"),
                        "{label}:\n{}",
                        run.stdout
                    );
                    gaps += 1;
                } else {
                    // Skewed header times are a note, so a restorable replica exits 0.
                    assert_eq!(run.code, 0, "{label}:\n{}", run.stdout);
                    assert!(
                        run.stdout.contains("restorable through"),
                        "{label}:\n{}",
                        run.stdout
                    );
                    clean += 1;
                }
            }
        }
        assert!(clean > 0 && gaps > 0);
    }

    fn severity_name(severity: Severity) -> &'static str {
        match severity {
            Severity::Note => "note",
            Severity::Degraded => "degraded",
            Severity::Unrestorable => "unrestorable",
        }
    }

    fn assert_levels_agree(levels: &[Value], report: &Report, label: &str) {
        for (index, (got, want)) in levels.iter().zip(report.levels()).enumerate() {
            assert_eq!(got["level"], json!(index), "{label}");
            assert_eq!(got["files"], json!(want.files()), "{label} level {index}");
            assert_eq!(got["bytes"], json!(want.bytes()), "{label} level {index}");
            assert_eq!(got["foreign_names"], json!(want.skipped()), "{label}");
            assert_eq!(
                got["first"],
                json!(want.range().map(|r| r.min().to_string())),
                "{label}"
            );
            assert_eq!(
                got["last"],
                json!(want.range().map(|r| r.max().to_string())),
                "{label}"
            );
        }
    }

    #[test]
    fn verify_json_agrees_with_the_library_report() {
        for (fixture, manifest) in fixtures() {
            let staged = stage(&fixture, &manifest).expect("stage");
            let replica = staged.replica();
            for (flag, depth) in [("names", Depth::Names), ("contents", Depth::Contents)] {
                let label = format!("{} {} {flag}", manifest.go_version, manifest.scenario);
                let run = lsr(&[
                    "verify",
                    "--replica",
                    path_str(&replica),
                    "--depth",
                    flag,
                    "--json",
                ]);
                let report = block_on(verify(&FsStore::new(&replica), depth)).expect("verify");
                let want_exit = match report.worst() {
                    None | Some(Severity::Note) => 0,
                    Some(Severity::Degraded) => 1,
                    Some(Severity::Unrestorable) => 2,
                };
                assert_eq!(run.code, want_exit, "{label}");
                let doc: Value =
                    serde_json::from_str(&run.stdout).expect("stdout is one JSON value");
                assert_eq!(doc["depth"], json!(flag), "{label}");
                assert_eq!(
                    doc["worst"],
                    json!(report.worst().map(severity_name)),
                    "{label}"
                );
                assert_eq!(doc["files_read"], json!(report.files_read()), "{label}");
                assert_eq!(doc["bytes_read"], json!(report.bytes_read()), "{label}");
                let reaches = if let PlanOutcome::Plan(chain) = report.latest() {
                    Some(chain.reaches().to_string())
                } else {
                    None
                };
                assert_eq!(
                    doc["latest"]["restorable"],
                    json!(reaches.is_some()),
                    "{label}"
                );
                assert_eq!(doc["latest"]["reaches"], json!(reaches), "{label}");
                let levels = doc["levels"].as_array().expect("levels");
                assert_eq!(levels.len(), 10, "{label}");
                assert_levels_agree(levels, &report, &label);
                let findings = doc["findings"].as_array().expect("findings");
                let severities: Vec<&str> = report
                    .findings()
                    .iter()
                    .map(|f| severity_name(f.severity()))
                    .collect();
                let got: Vec<&str> = findings
                    .iter()
                    .map(|f| f["severity"].as_str().expect("severity"))
                    .collect();
                assert_eq!(got, severities, "{label}");
            }
        }
    }

    #[test]
    fn files_json_agrees_with_the_inventory_and_text_lists_the_same() {
        for (fixture, manifest) in fixtures() {
            let staged = stage(&fixture, &manifest).expect("stage");
            let replica = staged.replica();
            let label = format!("{} {}", manifest.go_version, manifest.scenario);
            let inventory = block_on(Inventory::load(&FsStore::new(&replica))).expect("load");
            let run = lsr(&["files", "--replica", path_str(&replica), "--json"]);
            assert_eq!((run.code, run.stderr.as_str()), (0, ""), "{label}");
            let doc: Value = serde_json::from_str(&run.stdout).expect("JSON");
            let files = doc["files"].as_array().expect("files");
            assert_eq!(files.len(), manifest.files.len(), "{label}");
            assert_eq!(files.len(), inventory.files().len(), "{label}");
            for (got, want) in files.iter().zip(inventory.files()) {
                let name = format!("ltx/{}/{}", want.key.level(), want.key.file_name());
                assert_eq!(got["key"], json!(name), "{label}");
                assert_eq!(got["level"], json!(want.key.level().get()), "{label}");
                assert_eq!(
                    got["min"],
                    json!(want.key.range().min().to_string()),
                    "{label}"
                );
                assert_eq!(
                    got["max"],
                    json!(want.key.range().max().to_string()),
                    "{label}"
                );
                assert_eq!(got["size"], json!(want.size), "{label}");
                assert_eq!(
                    got["created_at_ms"],
                    json!(want.exact_stamp().unwrap().at().unix_millis()),
                    "{label}"
                );
                assert_eq!(
                    got["created_at"],
                    json!(want.exact_stamp().unwrap().at().to_string()),
                    "{label}"
                );
            }
            let text = lsr(&["files", "--replica", path_str(&replica)]);
            assert_eq!(text.code, 0, "{label}");
            assert_eq!(text.stdout.lines().count(), files.len(), "{label}");
            let first = inventory.files().first().expect("a file");
            assert!(
                text.stdout.starts_with(&format!(
                    "ltx/{}/{} {} {}\n",
                    first.key.level(),
                    first.key.file_name(),
                    first.size,
                    first.exact_stamp().unwrap().at()
                )),
                "{label}:\n{}",
                text.stdout
            );
        }
    }

    #[test]
    fn usage_errors_exit_64_with_one_stderr_line_and_no_stdout() {
        let cases: [&[&str]; 6] = [
            &[],
            &["frobnicate"],
            &["restore", "--replica", "r"],
            &["verify", "--replica", "r", "--replica", "s"],
            &["verify", "--replica", "https://bucket/path"],
            &[
                "restore",
                "--replica",
                "r",
                "--output",
                "o",
                "--txid",
                "1",
                "--timestamp",
                "2026-01-01T00:00:00Z",
            ],
        ];
        for args in cases {
            let run = lsr(args);
            assert_eq!((run.code, run.stdout.as_str()), (64, ""), "{args:?}");
            assert_eq!(run.stderr.lines().count(), 1, "{args:?}: {}", run.stderr);
        }
        let url = lsr(&["verify", "--replica", "https://bucket/path"]);
        assert!(url.stderr.contains("not supported"), "{}", url.stderr);
    }

    #[test]
    fn an_argument_that_is_not_utf8_is_a_usage_error() {
        let bad = OsString::from_vec(vec![b'v', 0xff]);
        let run = lsr_os(&["verify".into(), "--replica".into(), bad]);
        assert_eq!((run.code, run.stdout.as_str()), (64, ""));
        assert!(run.stderr.contains("UTF-8"), "{}", run.stderr);
    }

    #[test]
    fn a_directory_without_ltx_is_an_environment_error_not_an_empty_replica() {
        let dir = tempfile::tempdir().expect("tempdir");
        for command in ["verify", "files"] {
            let run = lsr(&[command, "--replica", path_str(dir.path())]);
            assert_eq!((run.code, run.stdout.as_str()), (3, ""), "{command}");
            assert!(run.stderr.contains("not a file replica"), "{}", run.stderr);
        }
    }

    #[test]
    fn help_and_version_print_to_stdout_and_exit_0() {
        let help = lsr(&["--help"]);
        assert_eq!((help.code, help.stderr.as_str()), (0, ""));
        for word in [
            "restore",
            "verify",
            "files",
            "--replica",
            "--clear-sidecars",
            "64",
        ] {
            assert!(help.stdout.contains(word), "{word}");
        }
        let version = lsr(&["--version"]);
        assert_eq!(version.code, 0);
        assert!(version.stdout.starts_with("lsr "));
    }
}
