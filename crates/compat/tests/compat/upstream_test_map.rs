//! The upstream test map (PLAN section 4): every upstream Go test is
//! mapped, in `docs/upstream-test-map.toml`, to the Rust tests that check the same behaviour, to a
//! reason it does not apply, or to a `missing` note that names the test still to write.
//!
//! The upstream names are a committed file, so the mapping checks run in `just check` with
//! no clone, no Go and no network: `docs/upstream-test-map/upstream-tests.txt`, written by
//! `scripts/upstream-tests.sh` from the pinned clones (`just upstream-test-map`).
//!
//! The Rust side is not committed: a committed list of every Rust test would change on
//! every branch that adds one, and every two such branches would conflict on it. Instead
//! `just upstream-test-map-fresh` (in `just check`) writes `cargo nextest list` to a file under
//! `target/`, names it in `LSR_UPSTREAM_TEST_MAP_RUST_TESTS` and runs the ignored
//! `every_counterpart_exists`, which checks only that each named counterpart is listed.
//!
//! The test map is one TOML table per upstream file, keyed by the test's name as the
//! script lists it (`TestX` or `TestX/sub`), each value an inline table with exactly one
//! of `counterpart` (a Rust test as `<nextest binary id>::<test path>`, or a list of
//! them), `reason` or `missing`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use lsr_compat::workspace_root;
use serde::Deserialize;

/// One test map value. Unknown keys are refused by the parser, so a misspelt key cannot
/// pass as an entry with no key.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    counterpart: Option<Counterparts>,
    reason: Option<String>,
    missing: Option<String>,
}

/// A counterpart is one Rust test or several.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Counterparts {
    One(String),
    Many(Vec<String>),
}

impl Counterparts {
    fn names(&self) -> Vec<&str> {
        match self {
            Self::One(name) => vec![name.as_str()],
            Self::Many(names) => names.iter().map(String::as_str).collect(),
        }
    }
}

/// Upstream file to test name to entry. TOML refuses a key given twice in one table, so
/// a name cannot have two entries.
type TestMap = BTreeMap<String, BTreeMap<String, Entry>>;

fn docs() -> PathBuf {
    workspace_root().join("docs")
}

fn read(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The file as written: the shared `[reasons]` table, then one table per upstream file.
#[derive(Debug, Deserialize)]
struct Doc {
    #[serde(default)]
    reasons: BTreeMap<String, String>,
    #[serde(flatten)]
    files: TestMap,
}

fn doc() -> Doc {
    let path = docs().join("upstream-test-map.toml");
    toml::from_str(&read(&path)).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// The entries with each `reason = "@<id>"` replaced by the shared sentence it names.
fn test_map() -> TestMap {
    let Doc { reasons, mut files } = doc();
    for entry in files.values_mut().flat_map(BTreeMap::values_mut) {
        if let Some(id) = entry.reason.as_deref().and_then(|r| r.strip_prefix('@')) {
            let text = reasons
                .get(id)
                .unwrap_or_else(|| panic!("reason \"@{id}\" is not defined in [reasons]"));
            entry.reason = Some(text.clone());
        }
    }
    files
}

/// The upstream names as `(file, name)`, from the script's tab-separated lines.
fn upstream() -> BTreeSet<(String, String)> {
    let text = read(&docs().join("upstream-test-map").join("upstream-tests.txt"));
    let names: BTreeSet<_> = text
        .lines()
        .map(|line| {
            let (file, name) = line
                .split_once('\t')
                .expect("each line is `<file>\\t<name>`");
            (file.to_owned(), name.to_owned())
        })
        .collect();
    assert!(
        names.len() > 1000,
        "upstream-tests.txt lists {} names; rerun `just upstream` and `just upstream-test-map`",
        names.len()
    );
    names
}

/// The variable naming the file `just upstream-test-map-fresh` writes `cargo nextest list` to.
const RUST_TESTS_VAR: &str = "LSR_UPSTREAM_TEST_MAP_RUST_TESTS";

/// Every Rust test as `<binary id>::<test path>`, from nextest's one-line listing.
fn rust_tests(listing: &str) -> BTreeSet<String> {
    listing
        .lines()
        .filter_map(|line| line.split_once(' '))
        .map(|(binary, test)| format!("{binary}::{test}"))
        .collect()
}

/// The counterparts the test map names that the listing lacks.
fn unknown_counterparts<'a>(test_map: &'a TestMap, tests: &BTreeSet<String>) -> BTreeSet<&'a str> {
    entries(test_map)
        .filter_map(|(_, _, e)| e.counterpart.as_ref())
        .flat_map(Counterparts::names)
        .filter(|name| !tests.contains(*name))
        .collect()
}

fn entries(test_map: &TestMap) -> impl Iterator<Item = (&str, &str, &Entry)> {
    test_map.iter().flat_map(|(file, tests)| {
        tests
            .iter()
            .map(move |(name, entry)| (file.as_str(), name.as_str(), entry))
    })
}

/// A name on the list with no entry fails, naming it.
#[test]
fn every_upstream_test_is_in_the_test_map() {
    let test_map = test_map();
    let unmapped: Vec<_> = upstream()
        .into_iter()
        .filter(|(file, name)| test_map.get(file).is_none_or(|t| !t.contains_key(name)))
        .map(|(file, name)| format!("{file} {name}"))
        .collect();
    assert!(
        unmapped.is_empty(),
        "{} upstream test(s) have no entry in docs/upstream-test-map.toml:\n{}",
        unmapped.len(),
        unmapped.join("\n")
    );
}

/// An entry for a name the list does not have (a typo, or an upstream test since
/// removed) fails, so the test map cannot drift from the pinned commit.
#[test]
fn every_entry_names_an_upstream_test() {
    let names = upstream();
    let test_map = test_map();
    let stray: Vec<_> = entries(&test_map)
        .filter(|(file, name, _)| !names.contains(&((*file).to_owned(), (*name).to_owned())))
        .map(|(file, name, _)| format!("{file} {name}"))
        .collect();
    assert!(
        stray.is_empty(),
        "docs/upstream-test-map.toml has entries for names upstream-tests.txt lacks:\n{}",
        stray.join("\n")
    );
}

/// Every shared reason is used by some entry: an id nothing names is dead text.
#[test]
fn every_shared_reason_is_used() {
    let Doc { reasons, files } = doc();
    let used: BTreeSet<&str> = files
        .values()
        .flat_map(BTreeMap::values)
        .filter_map(|e| e.reason.as_deref()?.strip_prefix('@'))
        .collect();
    let unused: Vec<_> = reasons
        .keys()
        .filter(|id| !used.contains(id.as_str()))
        .collect();
    assert!(unused.is_empty(), "unused shared reasons: {unused:?}");
}

/// Each entry has exactly one of the three keys, none of them empty.
#[test]
fn every_entry_has_exactly_one_key() {
    let test_map = test_map();
    let bad: Vec<_> = entries(&test_map)
        .filter(|(_, _, e)| {
            let keys = [
                e.counterpart.is_some(),
                e.reason.is_some(),
                e.missing.is_some(),
            ];
            let empty = e.reason.as_ref().is_some_and(|r| r.trim().is_empty())
                || e.missing.as_ref().is_some_and(|m| m.trim().is_empty())
                || e.counterpart.as_ref().is_some_and(|c| {
                    c.names().is_empty() || c.names().iter().any(|n| n.trim().is_empty())
                });
            keys.iter().filter(|k| **k).count() != 1 || empty
        })
        .map(|(file, name, _)| format!("{file} {name}"))
        .collect();
    assert!(
        bad.is_empty(),
        "entries need exactly one non-empty key of counterpart, reason or missing:\n{}",
        bad.join("\n")
    );
}

/// Each named counterpart is a Rust test in the listing `just upstream-test-map-fresh` writes. A name
/// it lacks is a typo, or a test renamed or deleted without its entry.
#[test]
#[ignore = "needs the Rust test listing; `just upstream-test-map-fresh` writes it and runs this"]
fn every_counterpart_exists() {
    let path = std::env::var_os(RUST_TESTS_VAR)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{RUST_TESTS_VAR} is unset; run `just upstream-test-map-fresh`"));
    let tests = rust_tests(&read(&path));
    assert!(
        tests.len() > 1000,
        "{} lists {} tests; is it `cargo nextest list --message-format oneline` output?",
        path.display(),
        tests.len()
    );
    let test_map = test_map();
    let unknown = unknown_counterparts(&test_map, &tests);
    assert!(
        unknown.is_empty(),
        "counterpart(s) in docs/upstream-test-map.toml that `cargo nextest list` lacks:\n{}",
        unknown.into_iter().collect::<Vec<_>>().join("\n")
    );
}

/// The check itself: a counterpart missing from a listing is reported, and one present is
/// not, so the ignored test cannot pass on a test map that names a deleted test.
#[test]
fn a_counterpart_missing_from_the_listing_is_reported() {
    let test_map: TestMap = toml::from_str(
        r#"
        ["f_test.go"]
        "TestKept" = { counterpart = "lsr-x::it::a::kept" }
        "TestGone" = { counterpart = ["lsr-x::it::a::kept", "lsr-x::it::a::gone"] }
        "TestSkipped" = { reason = "not in v1" }
        "#,
    )
    .expect("the sample test map parses");
    let tests = rust_tests("lsr-x::it a::kept\nlsr-x::it a::other\n");
    let unknown: Vec<_> = unknown_counterparts(&test_map, &tests)
        .into_iter()
        .collect();
    assert_eq!(unknown, ["lsr-x::it::a::gone"]);
}
