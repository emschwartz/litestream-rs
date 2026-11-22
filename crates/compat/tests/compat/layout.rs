//! Each crate's integration tests are one binary (AGENTS.md, "Tests"): a new
//! `tests/*.rs` file or `tests/<dir>/main.rs` would link a binary of its own and undo
//! the merge silently, so this walks every crate and counts its test targets.

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
