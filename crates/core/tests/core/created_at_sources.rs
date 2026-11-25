//! Source guards for the created-at time types (no reader decides on a last-modified time): who may make a
//! [`CreatedAt`] and who may look at a bare last-modified time. A walk of
//! `crates/` with std only (`walk.rs`, which fails on an empty walk).

use crate::walk::workspace_files;

/// Test code is exempt: a file under `tests/`, a `tests.rs`, or the part of a
/// source file after its `#[cfg(test)]` line.
fn production_part<'a>(rel: &str, text: &'a str) -> Option<&'a str> {
    if rel.contains("/tests/") || rel.ends_with("/tests.rs") || rel.ends_with("/conformance.rs") {
        return None;
    }
    Some(text.split("#[cfg(test)]").next().unwrap_or(text))
}

/// The files outside `allowed` whose production code holds a needle. Also asserts that each
/// needle does occur in an allowed file: a guard whose needle matches nothing anywhere (a
/// renamed type, a moved root) would pass for good.
fn offenders(needles: &[&str], allowed: &[&str]) -> Vec<String> {
    let files = workspace_files();
    let is_allowed = |rel: &str| allowed.iter().any(|ok| rel.starts_with(ok) || rel == *ok);
    for needle in needles {
        assert!(
            files
                .iter()
                .any(|(rel, text)| is_allowed(rel) && text.contains(needle)),
            "{needle:?} occurs in no allowed site {allowed:?}: the guard checks nothing"
        );
    }
    files
        .into_iter()
        .filter(|(rel, _)| !is_allowed(rel))
        .filter(|(rel, _)| !rel.ends_with("created_at_sources.rs"))
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
fn stored_created_at_constructors_only_in_stores() {
    let found = offenders(
        &[
            "CreatedAt::metadata(",
            "CreatedAt::mtime(",
            "CreatedAt::fallback(",
        ],
        &[
            "crates/store-",
            "crates/core/src/mem.rs",
            "crates/compat/",
            "crates/fuzzing/",
            // The simulator's view of the S3 replica's bucket: a store over the bucket's own
            // objects, which reads their created-at metadata as the S3 store does.
            "crates/sim/src/remote.rs",
        ],
    );
    assert!(
        found.is_empty(),
        "CreatedAt built outside a store: {found:?}"
    );
}

#[test]
fn last_modified_matched_only_in_allowed_sites() {
    let found = offenders(
        &["ListedTime::LastModified"],
        &[
            "crates/store-s3/",
            "crates/core/src/mem.rs",
            "crates/core/src/created_at.rs",
            "crates/core/src/layout.rs",
            "crates/cli/src/render.rs",
        ],
    );
    assert!(
        found.is_empty(),
        "LastModified read outside its sites: {found:?}"
    );
}
/// The exact planner takes an `AsOf` only over a listing whose created-at times are all
/// exact; production code asks `AsOfPlanner` through `resolve` instead.
#[test]
fn plan_reference_only_in_oracles() {
    let found = offenders(
        &["plan_reference"],
        &[
            "crates/core/src/plan.rs",
            "crates/core/src/lib.rs",
            "crates/compat/",
            "crates/fuzzing/",
        ],
    );
    assert!(
        found.is_empty(),
        "the exact as-of planner called from production code: {found:?}"
    );
}

/// A bare created-at instant (`.at()`) means a commit, a snapshot's write or
/// an upload depending on level and origin; a decision about when a
/// transaction committed reads it through `Stamp`. Each allowed site reads
/// the instant for something else: stores write and report it, the CLI
/// prints it, retention and the snapshot interval age files by it (write
/// time, as Go does with `LastModified`; neither needs it to date a commit),
/// verify measures skew, a local course's file moves up to level 0 under the time it was
/// staged with (`actor/apart.rs`), and the oracles restate the contract on their own.
#[test]
fn bare_created_at_instants_only_in_allowed_sites() {
    let found = offenders(
        &[".at()"],
        &[
            "crates/core/src/layout.rs",
            "crates/store-",
            "crates/cli/src/render.rs",
            "crates/core/src/maintain/retain.rs",
            "crates/core/src/maintain.rs",
            "crates/core/src/verify.rs",
            "crates/core/src/actor/apart.rs",
            "crates/core/src/spec_view.rs",
            "crates/sim/",
            "crates/fuzzing/",
            "crates/compat/",
        ],
    );
    assert!(
        found.is_empty(),
        "a created-at instant read outside its sites: {found:?}"
    );
}
