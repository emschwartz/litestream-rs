//! The built `lsr` binary against the committed Go-written fixtures. Needs no Go.
//! Restores are compared with the page hashes each fixture's manifest holds, and
//! the JSON outputs with what the library reports for the same replica.

use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use lsr_compat::{Manifest, Outcome, Staged, Target, all_fixtures, hash_pages, stage};
use lsr_core::WriteMode;
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

/// The refusal names the way out, and `--allow-earlier` then restores the pages Go wrote for
/// the same instant, with the warning on stderr and the file in the JSON.
fn assert_accepting_matches_go(
    staged: &Staged,
    entry: &lsr_compat::TargetEntry,
    refusal: &str,
    label: &str,
) {
    assert!(
        refusal.contains("; rerun with --allow-earlier to restore that state, or with --txid "),
        "{label}: {refusal}"
    );
    let mut flags = target_flags(entry.target);
    flags.extend(["--allow-earlier".to_owned(), "--json".to_owned()]);
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let run = restore(staged, &output, &flags);
    assert_eq!(run.code, 0, "{label}: {}", run.stderr);
    assert_pages_equal(&output, &entry.outcome, label);
    let json = json_of(&run);
    let file = json["inside_file"].as_str().expect("inside_file");
    assert!(file.starts_with("ltx/"), "{label}: {file}");
    let want = format!(
        "warning: the instant falls inside {file}; restored the state at transaction {}, which may leave out transactions committed before the instant\n",
        json["txid"].as_str().expect("txid")
    );
    assert_eq!(run.stderr, want, "{label}");
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
                    assert_accepting_matches_go(&staged, entry, &run.stderr, &label);
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
fn a_stale_journal_file_refuses_until_clear_journal_files() {
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
        refused.stderr.contains("--clear-journal-files"),
        "{}",
        refused.stderr
    );
    assert!(journal.exists() && !output.exists());

    let cleared = restore(&staged, &output, &["--clear-journal-files".to_owned()]);
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
fn clear_journal_files_never_touches_an_existing_target() {
    let (staged, _) = small_fixture();
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let wal = dir.path().join("out.db-wal");
    fs::write(&output, b"live").expect("plant");
    fs::write(&wal, b"belongs to it").expect("plant");
    let run = restore(&staged, &output, &["--clear-journal-files".to_owned()]);
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
        assert_eq!(got["unrecognized_names"], json!(want.skipped()), "{label}");
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
            let doc: Value = serde_json::from_str(&run.stdout).expect("stdout is one JSON value");
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
                json!(want.created_at().unwrap().at().unix_millis()),
                "{label}"
            );
            assert_eq!(
                got["created_at"],
                json!(want.created_at().unwrap().at().to_string()),
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
                first.created_at().unwrap().at()
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

/// `verify` and `files` read a replica's contents, so a path with no `ltx/` is a wrong
/// path, never an empty replica (a restore treats it as Go does; see
/// `if_replica_exists_skips_a_path_without_ltx_as_go_does`).
#[test]
fn verify_and_files_refuse_a_directory_without_ltx() {
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
        "--clear-journal-files",
        "--integrity-check",
        "none|quick|full",
        "64",
    ] {
        assert!(help.stdout.contains(word), "{word}");
    }
    let version = lsr(&["--version"]);
    assert_eq!(version.code, 0);
    assert!(version.stdout.starts_with("lsr "));
}

/// A WAL-mode SQLite database of 512-byte pages with a table and an index.
fn sqlite_pages() -> Vec<u8> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("source.db");
    let conn = rusqlite::Connection::open(&path).expect("open");
    conn.execute_batch(
        "PRAGMA page_size = 512; PRAGMA journal_mode = WAL;
             CREATE TABLE t (x TEXT); CREATE INDEX t_x ON t (x);",
    )
    .expect("schema");
    for n in 0..200 {
        conn.execute("INSERT INTO t VALUES (?1)", [format!("{n:0>40}")])
            .expect("insert");
    }
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .expect("checkpoint");
    drop(conn);
    fs::read(&path).expect("read")
}

/// A directory replica whose one snapshot holds exactly [db]'s pages:
/// valid LTX, whether or not the pages are a valid SQLite database.
fn replica_of(dir: &Path, db: &[u8]) {
    use lsr_core::{FileKey, Level, ObjectStore, Upload};
    use lsr_ltx::{Encoder, Header, HeaderFields, PageNo, PageSize, Txid, TxidRange};
    let size = PageSize::new(512).expect("page size");
    let one = TxidRange::single(Txid::new(1).expect("txid"));
    let commit = u32::try_from(db.len() / 512).expect("pages");
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit,
        txids: one,
        timestamp: Timestamp::from_unix_millis(1000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .expect("header");
    let mut encoder = Encoder::new(Vec::new(), header).expect("encoder");
    for (n, page) in (1..=commit).zip(db.as_chunks::<512>().0) {
        let pgno = PageNo::new(n, size).expect("pgno");
        encoder.write_page(pgno, page).expect("page");
    }
    let bytes = encoder.finish(None).expect("finish").0;
    fs::create_dir_all(dir.join("ltx")).expect("ltx dir");
    let store = FsStore::new(dir);
    block_on(async {
        let key = FileKey::new(Level::SNAPSHOT, one);
        let mut upload = store
            .begin_upload(&key, Timestamp::from_unix_millis(1000), WriteMode::Replace)
            .await
            .expect("begin");
        upload.write(&bytes).await.expect("write");
        upload.commit().await.expect("commit");
    });
}

fn restore_checked(replica: &Path, output: &Path, check: &str) -> Run {
    lsr(&[
        "restore",
        "--replica",
        path_str(replica),
        "--output",
        path_str(output),
        "--integrity-check",
        check,
    ])
}

#[test]
fn integrity_check_publishes_a_valid_database_in_every_mode() {
    let db = sqlite_pages();
    let work = tempfile::tempdir().expect("tempdir");
    let replica = work.path().join("replica");
    replica_of(&replica, &db);
    for (check, line) in [
        ("none", None),
        ("quick", Some("SQLite quick_check passed")),
        ("full", Some("SQLite integrity_check passed")),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let output = dir.path().join("out.db");
        let run = restore_checked(&replica, &output, check);
        assert_eq!((run.code, run.stderr.as_str()), (0, ""), "{check}");
        assert_eq!(fs::read(&output).expect("read"), db, "{check}");
        assert_eq!(run.stdout.contains("passed"), line.is_some(), "{check}");
        if let Some(line) = line {
            assert!(run.stdout.contains(line), "{check}: {}", run.stdout);
        }
        // The database and its meta directory, which holds only the takeover record a
        // restore leaves for the replicator started on it.
        assert_eq!(fs::read_dir(dir.path()).expect("dir").count(), 2, "{check}");
        let meta = dir.path().join(".out.db-litestream");
        assert_eq!(fs::read_dir(&meta).expect("meta").count(), 1, "{check}");
        let record = fs::read(meta.join("lsr-takeover")).expect("takeover record");
        assert!(lsr_core::TakeoverRecord::decode(&record).is_ok(), "{check}");
        assert!(run.stdout.contains("takeover recorded through"), "{check}");
    }
}

/// Valid LTX, broken b-tree: quick and full refuse with exit 2 (a refused
/// restore) naming SQLite's problem and leave nothing; none publishes it.
#[test]
fn integrity_check_refuses_a_corrupt_b_tree_with_exit_2() {
    let mut db = sqlite_pages();
    db[512..1024].fill(0xDE);
    let work = tempfile::tempdir().expect("tempdir");
    let replica = work.path().join("replica");
    replica_of(&replica, &db);
    for check in ["quick", "full"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let output = dir.path().join("out.db");
        let run = restore_checked(&replica, &output, check);
        assert_eq!((run.code, run.stdout.as_str()), (2, ""), "{check}");
        assert!(run.stderr.contains("page 2"), "{check}: {}", run.stderr);
        assert!(
            run.stderr.contains("nothing was published"),
            "{check}: {}",
            run.stderr
        );
        assert_eq!(run.stderr.lines().count(), 1, "{check}: {}", run.stderr);
        assert!(dir_is_empty(dir.path()), "{check}");
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let run = restore_checked(&replica, &output, "none");
    assert_eq!(run.code, 0, "{}", run.stderr);
    assert_eq!(fs::read(&output).expect("read"), db);
}

fn json_of(run: &Run) -> Value {
    assert_eq!(
        run.stdout.lines().count(),
        1,
        "one JSON line: {}",
        run.stdout
    );
    serde_json::from_str(&run.stdout).expect("stdout is JSON")
}

fn strings(extra: &[&str]) -> Vec<String> {
    extra.iter().map(|s| (*s).to_owned()).collect()
}

/// A directory replica that holds no LTX file: what a fresh replica looks like.
fn empty_replica(root: &Path) -> PathBuf {
    let replica = root.join("replica");
    fs::create_dir_all(replica.join("ltx")).expect("ltx dir");
    replica
}

fn restore_from(replica: &Path, output: &Path, extra: &[&str]) -> Run {
    let mut args = vec![
        "restore",
        "--replica",
        path_str(replica),
        "--output",
        path_str(output),
    ];
    args.extend_from_slice(extra);
    lsr(&args)
}

fn latest_chain(staged: &Staged) -> lsr_core::ContiguousChain {
    let inventory = block_on(Inventory::load(&FsStore::new(staged.replica()))).expect("list");
    let PlanOutcome::Plan(chain) = lsr_core::plan(&inventory, lsr_core::KeyTarget::Latest) else {
        panic!("the small fixture has a plan")
    };
    chain
}

/// Go's `-if-db-not-exists`: an existing output ends the command with 0 before
/// the replica is opened, and nothing is touched.
#[test]
fn if_db_not_exists_skips_an_existing_output_without_reading_the_replica() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    fs::write(&output, b"live").expect("plant");
    let nowhere = dir.path().join("no-such-replica");
    let run = restore_from(&nowhere, &output, &["--if-db-not-exists"]);
    assert_eq!((run.code, run.stderr.as_str()), (0, ""), "{}", run.stdout);
    assert!(run.stdout.contains("already exists"), "{}", run.stdout);
    assert_eq!(fs::read(&output).expect("read"), b"live");
    assert_eq!(fs::read_dir(dir.path()).expect("dir").count(), 1);
    let json = json_of(&restore_from(
        &nowhere,
        &output,
        &["--if-db-not-exists", "--json"],
    ));
    assert_eq!(json["skipped"], json!("output_exists"), "{json}");
    assert_eq!(json["db_path"], json!(path_str(&output)), "{json}");

    // Absent output: the flag changes nothing and the restore runs.
    let (staged, manifest) = small_fixture();
    let fresh = dir.path().join("fresh.db");
    let run = restore(&staged, &fresh, &strings(&["--if-db-not-exists"]));
    assert_eq!(run.code, 0, "{}", run.stderr);
    assert_pages_equal(&fresh, latest_pages(&manifest), "absent output");
}

/// Go's `-if-replica-exists`: a replica with no LTX file ends with 0 and no file;
/// without the flag the same replica is a refused restore.
#[test]
fn if_replica_exists_exits_0_on_an_empty_replica_and_writes_nothing() {
    let work = tempfile::tempdir().expect("tempdir");
    let replica = empty_replica(work.path());
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let refused = restore_from(&replica, &output, &[]);
    assert_eq!((refused.code, refused.stdout.as_str()), (2, ""));
    for extra in [
        &["--if-replica-exists"][..],
        &["--if-replica-exists", "--txid", "0000000000000005"],
        &["--if-replica-exists", "--timestamp", "2026-01-01T00:00:00Z"],
    ] {
        let run = restore_from(&replica, &output, extra);
        assert_eq!((run.code, run.stderr.as_str()), (0, ""), "{extra:?}");
        assert!(
            run.stdout.contains("no backup"),
            "{extra:?}: {}",
            run.stdout
        );
        assert!(dir_is_empty(dir.path()), "{extra:?}");
    }
    let json = json_of(&restore_from(
        &replica,
        &output,
        &["--if-replica-exists", "--json"],
    ));
    assert_eq!(json["skipped"], json!("empty_replica"), "{json}");
    assert!(dir_is_empty(dir.path()));
}

/// Go's file replica lists a missing level directory as empty
/// (`file/replica_client.go:98-100`), so a path with no `ltx/` folder, or no
/// directory at all, is an empty replica: `--if-replica-exists` skips it with exit 0
/// and says why (Go's `TestRestoreCommand_RunIfReplicaExists`); without the flag it is
/// refused as an empty replica is, and nothing is written either way.
#[test]
fn if_replica_exists_skips_a_path_without_ltx_as_go_does() {
    let work = tempfile::tempdir().expect("tempdir");
    let bare = work.path().join("bare");
    fs::create_dir(&bare).expect("bare dir");
    let missing = work.path().join("no-such-replica");
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    for replica in [&bare, &missing] {
        let skipped = restore_from(replica, &output, &["--if-replica-exists"]);
        assert_eq!(
            (skipped.code, skipped.stderr.as_str()),
            (0, ""),
            "{replica:?}"
        );
        assert!(
            skipped.stdout.contains("no ltx/ folder"),
            "{replica:?}: {}",
            skipped.stdout
        );
        let json = json_of(&restore_from(
            replica,
            &output,
            &["--if-replica-exists", "--json"],
        ));
        assert_eq!(json["skipped"], json!("no_ltx_folder"), "{json}");
        let refused = restore_from(replica, &output, &[]);
        assert_eq!(
            (refused.code, refused.stdout.as_str()),
            (2, ""),
            "{replica:?}"
        );
        assert!(
            refused.stderr.contains("no ltx/ folder"),
            "{replica:?}: {}",
            refused.stderr
        );
        assert!(dir_is_empty(dir.path()), "{replica:?}");
    }
    assert!(!missing.exists());
}

/// Go's `-dry-run` returns before `-if-replica-exists` is looked at
/// (`cmd/litestream/restore.go:119-122`): an empty replica, or a path with no `ltx/`,
/// is refused with or without the flag.
#[test]
fn dry_run_refuses_an_empty_replica_even_with_if_replica_exists() {
    let work = tempfile::tempdir().expect("tempdir");
    let empty = empty_replica(work.path());
    let bare = work.path().join("bare");
    fs::create_dir(&bare).expect("bare dir");
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    for (replica, says) in [(&empty, "no LTX file"), (&bare, "no ltx/ folder")] {
        for extra in [&["--dry-run"][..], &["--dry-run", "--if-replica-exists"]] {
            let run = restore_from(replica, &output, extra);
            assert_eq!((run.code, run.stdout.as_str()), (2, ""), "{extra:?}");
            assert!(run.stderr.contains(says), "{extra:?}: {}", run.stderr);
        }
    }
    assert!(dir_is_empty(dir.path()));
}

/// Stricter than Go, which exits 0 on every "transaction not available": the
/// flag covers an empty replica only. A replica with files that cannot meet the
/// target is still refused.
#[test]
fn if_replica_exists_still_refuses_what_a_non_empty_replica_cannot_meet() {
    let (staged, _) = small_fixture();
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let beyond = restore(
        &staged,
        &output,
        &strings(&["--if-replica-exists", "--txid", "7fffffffffffffff"]),
    );
    assert_eq!((beyond.code, beyond.stdout.as_str()), (2, ""));
    assert!(
        beyond.stderr.contains("beyond the newest"),
        "{}",
        beyond.stderr
    );
    let mut gaps = 0_usize;
    for (fixture, manifest) in fixtures() {
        if !latest_is_gap(&manifest) {
            continue;
        }
        let staged = stage(&fixture, &manifest).expect("stage");
        let gap = restore(&staged, &output, &strings(&["--if-replica-exists"]));
        assert_eq!((gap.code, gap.stdout.as_str()), (2, ""), "{}", gap.stderr);
        gaps += 1;
    }
    assert!(gaps > 0);
    assert!(dir_is_empty(dir.path()));
}

/// Go's `-dry-run` text: the source, target, replica kind, transaction range
/// and one row per file the restore would fetch, in order. Nothing is written.
#[test]
fn dry_run_prints_the_plan_and_writes_nothing() {
    let (staged, _) = small_fixture();
    let chain = latest_chain(&staged);
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let run = restore(&staged, &output, &strings(&["--dry-run"]));
    assert_eq!((run.code, run.stderr.as_str()), (0, ""), "{}", run.stdout);
    assert!(dir_is_empty(dir.path()));
    let lines: Vec<&str> = run.stdout.lines().collect();
    assert_eq!(lines.first(), Some(&"Restore plan:"), "{}", run.stdout);
    for want in [
        format!("  source: {}", path_str(&staged.replica())),
        format!("  target: {}", path_str(&output)),
        "  replica: file".to_owned(),
        format!(
            "  txid range: {} - {}",
            chain.snapshot().key.range().min(),
            chain.reaches()
        ),
        "Files to fetch:".to_owned(),
    ] {
        assert!(lines.contains(&want.as_str()), "{want:?} in {}", run.stdout);
    }
    let header = lines
        .iter()
        .position(|line| line.starts_with("level"))
        .expect("table header");
    let rows: Vec<Vec<&str>> = lines[header + 1..]
        .iter()
        .map(|line| line.split_whitespace().collect())
        .collect();
    assert_eq!(rows.len(), chain.files().len(), "{}", run.stdout);
    for (row, info) in rows.iter().zip(chain.files()) {
        assert_eq!(row[0], info.key.level().to_string());
        assert_eq!(row[1], info.key.file_name());
        assert_eq!(row[2], info.key.range().min().to_string());
        assert_eq!(row[3], info.key.range().max().to_string());
        assert_eq!(row[4], info.size.to_string());
    }
}

/// Every target of every fixture: the dry run refuses exactly when the restore
/// does, with the same message, and otherwise names the chain the restore
/// applies. The JSON keys are Go's.
#[test]
fn dry_run_json_lists_the_chain_the_restore_applies() {
    let mut planned = 0_usize;
    for (fixture, manifest) in fixtures() {
        let staged = stage(&fixture, &manifest).expect("stage");
        for entry in &manifest.targets {
            let label = format!("{} {:?}", manifest.scenario, entry.target);
            let target = target_flags(entry.target);
            let dir = tempfile::tempdir().expect("tempdir");
            let output = dir.path().join("out.db");
            let mut dry = target.clone();
            dry.extend(strings(&["--dry-run", "--json"]));
            let plan = restore(&staged, &output, &dry);
            assert!(dir_is_empty(dir.path()), "{label}");
            let mut real = target.clone();
            real.push("--json".to_owned());
            let done = restore(&staged, &output, &real);
            assert_eq!(plan.code, done.code, "{label}: {}", plan.stderr);
            if done.code != 0 {
                assert_eq!(
                    (plan.stdout.as_str(), &plan.stderr),
                    ("", &done.stderr),
                    "{label}"
                );
                continue;
            }
            let (plan, done) = (json_of(&plan), json_of(&done));
            assert_eq!(
                plan["source"],
                json!(path_str(&staged.replica())),
                "{label}"
            );
            assert_eq!(plan["target_path"], json!(path_str(&output)), "{label}");
            assert_eq!(plan["replica"], json!("file"), "{label}");
            assert_eq!(plan["min_txid"], json!("0000000000000001"), "{label}");
            assert_eq!(plan["max_txid"], done["txid"], "{label}");
            let files = plan["files"].as_array().expect("files");
            assert_eq!(
                Some(u64::try_from(files.len()).expect("count")),
                done["files"].as_u64(),
                "{label}"
            );
            for file in files {
                for key in ["level", "name", "min_txid", "max_txid", "size", "timestamp"] {
                    assert!(!file[key].is_null(), "{label}: {key} in {file}");
                }
            }
            planned += 1;
        }
    }
    assert!(planned > 0);
}

/// Stricter than Go, whose dry run never looks at the output: it checks the
/// output as the restore would, and lists the journal files --clear-journal-files would
/// remove without removing them.
#[test]
fn dry_run_checks_the_output_as_a_restore_would() {
    let (staged, _) = small_fixture();
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    fs::write(&output, b"live").expect("plant");
    let exists = restore(&staged, &output, &strings(&["--dry-run"]));
    assert_eq!((exists.code, exists.stdout.as_str()), (3, ""));
    assert!(
        exists.stderr.contains("already exists"),
        "{}",
        exists.stderr
    );
    fs::remove_file(&output).expect("remove");

    let wal = dir.path().join("out.db-wal");
    fs::write(&wal, b"stale").expect("plant");
    let refused = restore(&staged, &output, &strings(&["--dry-run"]));
    assert_eq!((refused.code, refused.stdout.as_str()), (3, ""));
    assert!(
        refused.stderr.contains("--clear-journal-files"),
        "{}",
        refused.stderr
    );
    let listed = restore(
        &staged,
        &output,
        &strings(&["--dry-run", "--clear-journal-files"]),
    );
    assert_eq!(listed.code, 0, "{}", listed.stderr);
    assert!(
        listed
            .stdout
            .contains(&format!("would remove stale {}", path_str(&wal))),
        "{}",
        listed.stdout
    );
    let json = json_of(&restore(
        &staged,
        &output,
        &strings(&["--dry-run", "--clear-journal-files", "--json"]),
    ));
    assert_eq!(
        json["journal_files_to_remove"],
        json!([path_str(&wal)]),
        "{json}"
    );
    assert_eq!(fs::read(&wal).expect("read"), b"stale");
    assert!(!output.exists());
}

/// Go's `-json` result: db_path, replica, txid, duration_ms and integrity_check,
/// for the database the text form describes.
#[test]
fn restore_json_reports_go_s_fields() {
    let (staged, manifest) = small_fixture();
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("out.db");
    let run = restore(
        &staged,
        &output,
        &strings(&["--json", "--integrity-check", "quick"]),
    );
    assert_eq!((run.code, run.stderr.as_str()), (0, ""));
    assert_pages_equal(&output, latest_pages(&manifest), "json restore");
    let json = json_of(&run);
    assert_eq!(json["db_path"], json!(path_str(&output)), "{json}");
    assert_eq!(json["replica"], json!("file"), "{json}");
    assert_eq!(json["integrity_check"], json!("quick"), "{json}");
    assert!(json["duration_ms"].is_u64(), "{json}");
    let chain = latest_chain(&staged);
    assert_eq!(json["txid"], json!(chain.reaches().to_string()), "{json}");
    assert_eq!(json["files"], json!(chain.files().len()), "{json}");
    assert!(json["checksum_verified"].is_boolean(), "{json}");
}

/// `lsr take-over <db>` records a takeover through the replica's newest transaction in the
/// database's meta directory.
#[test]
fn take_over_records_the_replicas_newest_transaction() {
    let db = sqlite_pages();
    let work = tempfile::tempdir().expect("tempdir");
    let replica = work.path().join("replica");
    replica_of(&replica, &db);
    let database = work.path().join("app.db");
    let meta = work.path().join(".app.db-litestream");
    let run = lsr(&[
        "take-over",
        path_str(&database),
        "--replica",
        path_str(&replica),
    ]);
    assert_eq!((run.code, run.stderr.as_str()), (0, ""), "{}", run.stdout);
    assert!(
        run.stdout.starts_with("takeover recorded through "),
        "{}",
        run.stdout
    );
    let record = fs::read(meta.join("lsr-takeover")).expect("takeover record");
    lsr_core::TakeoverRecord::decode(&record).expect("a takeover record");
}

/// `lsr restore` to an output that names no file is refused with exit 3 before anything is
/// written: it has no meta directory for the takeover record.
#[test]
fn restore_to_an_output_with_no_file_name_is_refused_before_writing() {
    let db = sqlite_pages();
    let work = tempfile::tempdir().expect("tempdir");
    let replica = work.path().join("replica");
    replica_of(&replica, &db);
    let dir = tempfile::tempdir().expect("tempdir");
    let output = dir.path().join("sub").join("..");
    let run = restore_checked(&replica, &output, "none");
    assert_eq!(run.code, 3, "{}", run.stderr);
    assert!(run.stderr.contains("no meta directory"), "{}", run.stderr);
    assert_eq!(fs::read_dir(dir.path()).expect("dir").count(), 0);
}
