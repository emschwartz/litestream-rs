//! The source guards: no module of the engine outside `publish` writes to or
//! deletes from a store itself, and none outside `fault` classifies a store
//! error except through its store's role.
//!
//! A raw commit needs a raw upload, which only `begin_upload` makes, so the
//! guard counts calls of `begin_upload` and `delete` in the crate's non-test
//! source. The files it lets through implement a store or pass a store's calls
//! through to another one; each says why.

use std::fs;
use std::path::{Path, PathBuf};

/// Files that may call a store's `begin_upload` or `delete`, and why.
const ALLOWED: &[(&str, &str)] = &[
    ("publish.rs", "the choke point itself"),
    (
        "store.rs",
        "the trait's own definition and its default methods",
    ),
    ("mem.rs", "the in-memory store"),
    (
        "retry.rs",
        "the retry wrapper, a store passing calls through",
    ),
    ("stuck.rs", "a wrapper that passes calls through"),
    (
        "dry_run.rs",
        "the dry-run wrapper, a store passing calls through",
    ),
    (
        "created_at.rs",
        "the created-at wrapper, a store passing calls through",
    ),
    ("conformance.rs", "the store conformance suite"),
    (
        "apart.rs",
        "a local course's view of staging, a store passing calls through",
    ),
    (
        "role.rs",
        "the staging and replica roles, stores passing calls through",
    ),
];

const CALLS: &[&str] = &[".begin_upload(", ".begin_sized_upload(", ".delete("];

/// The two ways to turn a store error into a fault, which only `fault` (and
/// its `role` module) may name: everywhere else an error is classified by its
/// store's role (`fault::role`), so it cannot be taken for the other store's.
const CLASSIFIERS: &[&str] = &["from_staging(", "from_remote("];

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if path.is_dir() {
            if name != "tests" && name != "mem" {
                sources(&path, out);
            }
        } else if name.ends_with(".rs") && name != "tests.rs" && name != "guard.rs" {
            out.push(path);
        }
    }
}

/// The lines of `text` before its test module, if it has one inline.
fn non_test(text: &str) -> impl Iterator<Item = (usize, &str)> {
    let lines: Vec<&str> = text.lines().collect();
    let end = lines
        .windows(2)
        .position(|pair| match pair {
            [attr, item] => attr.trim() == "#[cfg(test)]" && item.trim_start().starts_with("mod "),
            _ => false,
        })
        .unwrap_or(lines.len());
    text.lines().enumerate().take(end)
}

/// Every line of the crate's non-test source outside `allowed` files that
/// names one of `calls`, as `path:line: text`.
fn found(calls: &[&str], allowed: &[&str]) -> Vec<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&root, &mut files);
    assert!(
        files.len() > 20,
        "the walk found {} source files",
        files.len()
    );
    let mut found = Vec::new();
    for path in files {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if allowed.contains(&name) {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        for (n, line) in non_test(&text) {
            let code = line.split("//").next().unwrap_or("");
            if calls.iter().any(|call| code.contains(call)) {
                found.push(format!(
                    "{}:{}: {}",
                    path.display(),
                    n.saturating_add(1),
                    line.trim()
                ));
            }
        }
    }
    found
}

/// Every call of a store's `begin_upload` or `delete` outside the allowed
/// files.
fn raw_calls() -> Vec<String> {
    let allowed: Vec<&str> = ALLOWED.iter().map(|(name, _)| *name).collect();
    found(CALLS, &allowed)
}

#[test]
fn no_engine_module_classifies_a_store_error_outside_its_role() {
    let found = found(CLASSIFIERS, &["fault.rs", "role.rs"]);
    assert!(
        found.is_empty(),
        "store errors classified outside fault::role (use the store's Role::fault):\n{}",
        found.join("\n")
    );
}

#[test]
fn no_engine_module_calls_a_store_write_or_delete_outside_publish() {
    let found = raw_calls();
    assert!(
        found.is_empty(),
        "raw store calls outside publish (route them through crate::publish):\n{}",
        found.join("\n")
    );
}

#[test]
fn the_guard_sees_a_raw_call_and_skips_test_modules() {
    let text =
        "fn a() { store.delete(&keys); }\n#[cfg(test)]\nmod tests {\n    store.delete(&k);\n}\n";
    let seen: Vec<_> = non_test(text)
        .filter(|(_, line)| CALLS.iter().any(|call| line.contains(call)))
        .collect();
    assert_eq!(seen.len(), 1);
}
