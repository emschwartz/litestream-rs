//! A test never skips by printing and passing: the only skip is `LSR_SKIP` through
//! `lsr_core::needs` (see `needs.rs`). This walks the workspace's test code for the shapes a
//! quiet skip takes and for the retired `LSR_REQUIRE_MINIO` switch.

use crate::walk::workspace_files;

/// Needles that mark a skip-and-pass or the retired opt-in strictness switch.
const QUIET_SKIPS: [&str; 4] = [
    "println!(\"skipped",
    "eprintln!(\"skipped",
    "LSR_REQUIRE_MINIO",
    "MinIO is not running; S3 tests skipped",
];

fn hits(files: &[(String, String)]) -> Vec<String> {
    files
        .iter()
        .filter(|(rel, _)| !rel.ends_with("skip_guard.rs"))
        .filter(|(_, text)| QUIET_SKIPS.iter().any(|n| text.contains(n)))
        .map(|(rel, _)| rel.clone())
        .collect()
}

#[test]
fn no_test_skips_by_passing() {
    let found = hits(&workspace_files());
    assert!(
        found.is_empty(),
        "a quiet skip (use lsr_core::needs, opt out with LSR_SKIP): {found:?}"
    );
}

#[test]
fn the_guard_catches_a_planted_skip() {
    let planted = vec![(
        "crates/x/tests/t.rs".to_owned(),
        "fn t() { println!(\"skipped: no server\"); }".to_owned(),
    )];
    assert_eq!(hits(&planted), ["crates/x/tests/t.rs"]);
}
