//! The release table covers upstream: every `v0.5.N` tag in the clone is known (so a new
//! upstream release needs a page-encoding decision here instead of dropping out unnoticed), no known
//! release has vanished, and the releases said to read the wal-index's committed end are
//! exactly the tags that contain the upstream fix. Needs only the clone (`just upstream`), no
//! Go toolchain.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::{Command, Output};

use lsr_compat::{
    GoBuild, GoVersion, KNOWN_RELEASES, QUIRKS, WAL_INDEX_END_COMMIT, workspace_root,
};

fn clone() -> PathBuf {
    workspace_root()
        .ancestors()
        .map(|a| a.join(".upstream").join("litestream"))
        .find(|p| p.join(".git").exists())
        .expect("run 'just upstream' first")
}

fn git(args: &[&str]) -> Output {
    Command::new("git")
        .arg("-C")
        .arg(clone())
        .args(args)
        .output()
        .expect("git runs")
}

/// The release tags `v0.5.N` (no pre-release suffix) among `git tag` run with `args`.
fn release_tags_with(args: &[&str]) -> BTreeSet<String> {
    let out = git(&[&["tag", "--list", "v0.5.*"], args].concat());
    assert!(out.status.success(), "git tag failed");
    String::from_utf8(out.stdout)
        .expect("utf8")
        .lines()
        .filter(|t| {
            t.strip_prefix("v0.5.")
                .is_some_and(|n| !n.is_empty() && n.bytes().all(|c| c.is_ascii_digit()))
        })
        .map(str::to_owned)
        .collect()
}

/// A tag the table does not list fails by name; so does a listed one the clone lacks.
#[test]
#[ignore = "reads the upstream clone; just compat-tags"]
fn every_release_tag_is_known() {
    let tags = release_tags_with(&[]);
    let known: BTreeSet<String> = KNOWN_RELEASES.iter().map(|s| (*s).to_owned()).collect();
    let unlisted: Vec<_> = tags.difference(&known).collect();
    assert!(
        unlisted.is_empty(),
        "upstream release(s) {unlisted:?} are not in KNOWN_RELEASES: add them (check the page encoding; `just compat-live <tag>`)"
    );
    let missing: Vec<_> = known.difference(&tags).collect();
    assert!(
        missing.is_empty(),
        "KNOWN_RELEASES lists {missing:?}, absent upstream"
    );
}

/// `GoBuild::reads_wal_index_end` names exactly the known releases that contain upstream's
/// wal-index fix, and the pinned head contains it, so the alternation tests' exception for
/// the builds without it (#38) never covers a build that has it, and a release that gains it
/// fails here until `FIRST_WAL_INDEX_END_PATCH` names it.
#[test]
#[ignore = "reads the upstream clone; just compat-tags"]
fn wal_index_end_follows_the_clone() {
    let head = GoVersion::Head.name();
    let ancestor = git(&["merge-base", "--is-ancestor", WAL_INDEX_END_COMMIT, head]);
    assert!(
        ancestor.status.success(),
        "the pinned {head} does not contain {WAL_INDEX_END_COMMIT}"
    );
    assert!(GoBuild::from(GoVersion::Head).reads_wal_index_end());
    let fixed = release_tags_with(&["--contains", WAL_INDEX_END_COMMIT]);
    for name in KNOWN_RELEASES {
        assert_eq!(
            GoBuild::new(name).reads_wal_index_end(),
            fixed.contains(name),
            "{name}: contains {WAL_INDEX_END_COMMIT} upstream: {}; set FIRST_WAL_INDEX_END_PATCH",
            fixed.contains(name)
        );
    }
}

/// A quirk row names a release the table knows, so a typo cannot hide a quirk.
#[test]
fn quirks_name_known_releases() {
    for q in QUIRKS {
        assert!(
            KNOWN_RELEASES.contains(&q.name),
            "{} is not a known release",
            q.name
        );
        assert!(!q.reason.is_empty(), "{} has no reason", q.name);
    }
}
