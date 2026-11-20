//! The release table covers upstream: every `v0.5.N` tag in the clone is known (so a new
//! upstream release needs an era decision here instead of dropping out unnoticed), and no
//! known release has vanished. Needs only the clone (`just upstream`), no Go toolchain.

// clippy.toml exempts only #[test] bodies; the helper here is test code too.
#![expect(
    clippy::expect_used,
    reason = "test helpers; clippy.toml exempts only #[test] bodies"
)]

use std::collections::BTreeSet;
use std::process::Command;

use lsr_compat::{KNOWN_RELEASES, QUIRKS, workspace_root};

fn release_tags() -> BTreeSet<String> {
    let root = workspace_root();
    let clone = root
        .ancestors()
        .map(|a| a.join(".upstream").join("litestream"))
        .find(|p| p.join(".git").exists())
        .expect("run 'just upstream' first");
    let out = Command::new("git")
        .arg("-C")
        .arg(&clone)
        .args(["tag", "--list", "v0.5.*"])
        .output()
        .expect("git runs");
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
    let tags = release_tags();
    let known: BTreeSet<String> = KNOWN_RELEASES.iter().map(|s| (*s).to_owned()).collect();
    let unlisted: Vec<_> = tags.difference(&known).collect();
    assert!(
        unlisted.is_empty(),
        "upstream release(s) {unlisted:?} are not in KNOWN_RELEASES: add them (check the era; `just compat-live <tag>`)"
    );
    let missing: Vec<_> = known.difference(&tags).collect();
    assert!(
        missing.is_empty(),
        "KNOWN_RELEASES lists {missing:?}, absent upstream"
    );
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
