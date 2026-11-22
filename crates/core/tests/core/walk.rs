//! The walk of `crates/` the source guards share. It fails, never returns less, when the tree
//! cannot be read, and when it finds far fewer files than the workspace has: a guard over an
//! empty set passes whatever the code does.

use std::fs;
use std::path::{Path, PathBuf};

/// Fewer Rust files than this under `crates/` means the walk lost its root (361 today).
const MIN_RUST_FILES: usize = 200;

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(dir).unwrap_or_else(|e| panic!("the walk cannot read {}: {e}", dir.display()));
    for entry in entries {
        let path = entry.expect("a directory entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n != "target") {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Every Rust file under `crates/` as (`crates/<crate>/...`, text).
pub fn workspace_files() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut paths = Vec::new();
    rust_files(&root, &mut paths);
    let files: Vec<(String, String)> = paths
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("the walk cannot read {}: {e}", path.display()));
            let rel = path
                .strip_prefix(&root)
                .expect("a path under the root")
                .to_string_lossy()
                .replace('\\', "/");
            (format!("crates/{rel}"), text)
        })
        .collect();
    assert_nonempty_walk(files.len());
    files
}

fn assert_nonempty_walk(found: usize) {
    assert!(
        found >= MIN_RUST_FILES,
        "the walk of crates/ found {found} Rust files, expected at least {MIN_RUST_FILES}"
    );
}

#[test]
#[should_panic(expected = "found 0 Rust files")]
fn an_empty_walk_fails() {
    assert_nonempty_walk(0);
}

#[test]
fn the_walk_finds_the_workspace() {
    let files = workspace_files();
    assert!(files.iter().any(|(rel, _)| rel == "crates/core/src/lib.rs"));
}
