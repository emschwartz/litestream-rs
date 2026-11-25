//! Each crate's integration tests are one binary (AGENTS.md, "Tests"): a new
//! `tests/*.rs` file or `tests/<dir>/main.rs` would link a binary of its own and undo
//! the merge silently, so this walks every crate and counts its test targets.
//!
//! Clippy reads only the nearest `clippy.toml`, so a crate with its own repeats the root
//! file and adds to it. A copy that is not kept in step holds its crate to an older rule
//! (two copies stayed at a 70-line function limit after the root went to 150), so the
//! second test here reads every copy against the root.

use std::fs;
use std::path::Path;

use lsr_compat::workspace_root;

/// The test targets Cargo discovers under one crate's `tests/`: each top-level `.rs`
/// file, and each directory holding a `main.rs`.
fn test_targets(tests: &Path) -> Vec<String> {
    // A crate without tests/ has no targets; any other read failure must not look like one.
    if !tests.exists() {
        return Vec::new();
    }
    let entries = fs::read_dir(tests).expect("read a tests/ directory");
    let mut targets: Vec<String> = entries
        .map(|entry| entry.expect("read a tests/ entry").path())
        .filter(|path| {
            let is_file = path.is_file() && path.extension().is_some_and(|ext| ext == "rs");
            is_file || path.join("main.rs").is_file()
        })
        .map(|path| path.display().to_string())
        .collect();
    targets.sort();
    targets
}

#[test]
fn every_crate_has_at_most_one_integration_test_binary() {
    let crates = fs::read_dir(workspace_root().join("crates")).expect("read crates/");
    let mut offenders = Vec::new();
    let mut with_tests = 0;
    for entry in crates {
        let tests = entry.expect("read a crates/ entry").path().join("tests");
        let targets = test_targets(&tests);
        with_tests += usize::from(!targets.is_empty());
        if targets.len() > 1 {
            offenders.push(targets);
        }
    }
    // Ten crates have one today; a walk that found none checked nothing.
    assert!(
        with_tests >= 5,
        "only {with_tests} crates have integration tests"
    );
    assert!(
        offenders.is_empty(),
        "crates with more than one integration test binary (add a module to the crate's \
         tests/<name>/main.rs instead): {offenders:#?}"
    );
}

/// The settings of a `clippy.toml`: its lines without comments, blanks and the brackets
/// of a list, so a list's entries compare one by one.
fn settings(text: &str) -> Vec<&str> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter(|line| !line.ends_with('[') && *line != "]")
        .collect()
}

#[test]
fn a_crate_s_own_clippy_settings_hold_every_root_setting() {
    let root = fs::read_to_string(workspace_root().join("clippy.toml")).expect("root clippy.toml");
    let crates = fs::read_dir(workspace_root().join("crates")).expect("read crates/");
    let mut copies = 0;
    let mut missing = Vec::new();
    for entry in crates {
        let path = entry
            .expect("read a crates/ entry")
            .path()
            .join("clippy.toml");
        if !path.exists() {
            continue;
        }
        copies += 1;
        let copy = fs::read_to_string(&path).expect("read a crate's clippy.toml");
        let has = settings(&copy);
        for setting in settings(&root) {
            if !has.contains(&setting) {
                missing.push(format!("{}: {setting}", path.display()));
            }
        }
    }
    // Two crates have a copy today; a walk that found none checked nothing.
    assert!(copies >= 2, "only {copies} crates have a clippy.toml");
    assert!(
        missing.is_empty(),
        "root clippy.toml settings a crate's own clippy.toml lacks: {missing:#?}"
    );
}
