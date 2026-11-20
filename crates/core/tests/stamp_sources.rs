//! Source guards for the stamp types (ruling G2): who may make a
//! [`StoredStamp`] and who may look at a bare last-modified time. A walk of
//! `crates/` with std only.

use std::fs;
use std::path::{Path, PathBuf};

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n != "target") {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn workspace_files() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut paths = Vec::new();
    rust_files(&root, &mut paths);
    paths
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).ok()?;
            let rel = path
                .strip_prefix(&root)
                .ok()?
                .to_string_lossy()
                .replace('\\', "/");
            Some((format!("crates/{rel}"), text))
        })
        .collect()
}

/// Test code is exempt: a file under `tests/`, a `tests.rs`, or the part of a
/// source file after its `#[cfg(test)]` line.
fn production_part<'a>(rel: &str, text: &'a str) -> Option<&'a str> {
    if rel.contains("/tests/") || rel.ends_with("/tests.rs") || rel.ends_with("/conformance.rs") {
        return None;
    }
    Some(text.split("#[cfg(test)]").next().unwrap_or(text))
}

fn offenders(needles: &[&str], allowed: &[&str]) -> Vec<String> {
    workspace_files()
        .into_iter()
        .filter(|(rel, _)| !allowed.iter().any(|ok| rel.starts_with(ok) || rel == ok))
        .filter(|(rel, _)| !rel.ends_with("stamp_sources.rs"))
        .filter_map(|(rel, text)| {
            let code = production_part(&rel, &text)?;
            needles
                .iter()
                .any(|needle| code.contains(needle))
                .then_some(rel)
        })
        .collect()
}

#[test]
fn stored_stamp_constructors_only_in_stores() {
    let found = offenders(
        &[
            "StoredStamp::metadata(",
            "StoredStamp::mtime(",
            "StoredStamp::fallback(",
        ],
        &[
            "crates/store-",
            "crates/core/src/mem.rs",
            "crates/compat/",
            "crates/fuzzing/",
        ],
    );
    assert!(
        found.is_empty(),
        "StoredStamp built outside a store: {found:?}"
    );
}

#[test]
fn last_modified_matched_only_in_allowed_sites() {
    let found = offenders(
        &["ListedStamp::LastModified"],
        &[
            "crates/store-s3/",
            "crates/core/src/mem.rs",
            "crates/core/src/stamps.rs",
            "crates/core/src/layout.rs",
            "crates/cli/src/render.rs",
        ],
    );
    assert!(
        found.is_empty(),
        "LastModified read outside its sites: {found:?}"
    );
}
