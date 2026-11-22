//! Evidence rows as data (`docs/evidence.toml`): what each row claims a test has shown, checked.
//!
//! A row's mark is derived from its lists, never typed: tested when any of `detected`,
//! `survived` or `recovered` names a test, else the `none` mark it carries. Two layers check it:
//!
//! - always (`the_evidence_rows_are_well_formed`): the file parses with no unknown key, ids are
//!   unique, a tested row has no `none` and an untested row has one from the closed list, every
//!   kill's test is one of its row's tests, its plant is a ledger entry (`docs/planted-bugs.toml`)
//!   whose package is the test's and one of whose test filters matches it, and a tested row has
//!   a kill or says why not (`unkilled`);
//! - with the Rust test listing (`every_cited_test_exists_and_runs_where_it_says`, run by `just
//!   evidence-fresh`): every cited test is listed, a row citing an ignored test names the just
//!   recipe that runs it (`runs_in`) and the recipe exists, and no kill's test is ignored, since
//!   the nightly ledger run that demonstrates kills runs no ignored test.
//!
//! The listing is the one `just upstream-test-map-fresh` writes, for the same reason: a
//! committed list of every Rust test would conflict on every branch that adds one. The ledger
//! run then makes each kill true: it plants the entry and requires its tests to fail.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use lsr_compat::workspace_root;
use serde::Deserialize;

/// The variable naming the listing of every test (`--run-ignored all`).
const ALL_VAR: &str = "LSR_EVIDENCE_ALL_TESTS";
/// The variable naming the listing of the ignored tests alone (`--run-ignored only`).
const IGNORED_VAR: &str = "LSR_EVIDENCE_IGNORED_TESTS";

/// The marks a row with no test evidence may carry.
const NONE_MARKS: [&str; 6] = [
    "code only",
    "assumed",
    "untested",
    "unknown",
    "known wrong",
    "accepted, undetected",
];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Doc {
    row: Vec<Row>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    circumstance: String,
    #[serde(default)]
    detected: Vec<String>,
    #[serde(default)]
    survived: Vec<String>,
    #[serde(default)]
    recovered: Vec<String>,
    none: Option<String>,
    runs_in: Option<String>,
    unkilled: Option<String>,
    #[serde(default)]
    kill: Vec<Kill>,
}

impl Row {
    fn tests(&self) -> BTreeSet<&str> {
        self.detected
            .iter()
            .chain(&self.survived)
            .chain(&self.recovered)
            .map(String::as_str)
            .collect()
    }

    /// The derived mark: the evidence letters, or the row's `none`.
    fn mark(&self) -> String {
        let letters: String = [
            (!self.detected.is_empty(), 'D'),
            (!self.survived.is_empty(), 'S'),
            (!self.recovered.is_empty(), 'R'),
        ]
        .into_iter()
        .filter_map(|(has, letter)| has.then_some(letter))
        .collect();
        if letters.is_empty() {
            self.none.clone().unwrap_or_default()
        } else {
            letters
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Kill {
    test: String,
    plant: String,
}

/// The ledger's fields this check reads; the rest belong to the runner.
#[derive(Debug, Deserialize)]
struct Ledger {
    bug: Vec<Bug>,
}

#[derive(Debug, Deserialize)]
struct Bug {
    name: String,
    package: String,
    tests: Vec<String>,
    test_bin: Option<String>,
}

fn read(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn evidence() -> Doc {
    let path = workspace_root().join("docs").join("evidence.toml");
    toml::from_str(&read(&path)).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn ledger() -> BTreeMap<String, Bug> {
    let path = workspace_root().join("docs").join("planted-bugs.toml");
    let ledger: Ledger =
        toml::from_str(&read(&path)).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
    ledger
        .bug
        .into_iter()
        .map(|b| (b.name.clone(), b))
        .collect()
}

/// A cited test split into its package, its binary (`None` for a library's unit tests) and
/// its path: `lsr-sim::sim::a::b` is package `lsr-sim`, binary `sim`, path `a::b`; a unit test
/// `lsr-sim::a::b` has no binary. A binary id has at most one `::` after the package, and the
/// listing names which ids are binaries, so the split is decided against it.
fn split<'a>(
    test: &'a str,
    binaries: &BTreeSet<String>,
) -> Option<(&'a str, Option<&'a str>, &'a str)> {
    let (package, rest) = test.split_once("::")?;
    match rest.split_once("::") {
        Some((binary, path)) if binaries.contains(&format!("{package}::{binary}")) => {
            Some((package, Some(binary), path))
        }
        Some(_) | None => Some((package, None, rest)),
    }
}

/// The binaries a test's id may name: every `<package>::<binary>` id that is not a package's
/// unit tests. Without a listing, ids whose second part has no `::` after it are not known,
/// so a well-formedness check over the file reads the binaries off the rows' own ids.
fn binaries_of(listing: &BTreeSet<String>) -> BTreeSet<String> {
    listing
        .iter()
        .filter_map(|line| line.split_once(' ').map(|(binary, _)| binary))
        .filter(|binary| binary.contains("::"))
        .map(str::to_owned)
        .collect()
}

/// Everything wrong with `doc` that needs no listing, against `ledger`; `binaries` are the
/// integration-test binary ids known so far.
fn problems(doc: &Doc, ledger: &BTreeMap<String, Bug>, binaries: &BTreeSet<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut ids = BTreeSet::new();
    for row in &doc.row {
        if !ids.insert(row.id.as_str()) {
            out.push(format!("{}: the id is taken", row.id));
        }
        if row.circumstance.trim().is_empty() {
            out.push(format!("{}: no circumstance", row.id));
        }
        let tests = row.tests();
        match (&row.none, tests.is_empty()) {
            (Some(mark), true) if NONE_MARKS.contains(&mark.as_str()) => {}
            (Some(mark), true) => out.push(format!("{}: {mark:?} is not a mark", row.id)),
            (None, true) => out.push(format!("{}: no evidence and no `none` mark", row.id)),
            (Some(_), false) => out.push(format!("{}: tested, so it carries no `none`", row.id)),
            (None, false) => {}
        }
        if !tests.is_empty() && row.kill.is_empty() && row.unkilled.is_none() {
            out.push(format!(
                "{}: tested with no planted bug its test kills, and no `unkilled` reason",
                row.id
            ));
        }
        for kill in &row.kill {
            out.extend(kill_problems(row, kill, ledger, binaries));
        }
    }
    out
}

fn kill_problems(
    row: &Row,
    kill: &Kill,
    ledger: &BTreeMap<String, Bug>,
    binaries: &BTreeSet<String>,
) -> Vec<String> {
    let id = &row.id;
    if !row.tests().contains(kill.test.as_str()) {
        return vec![format!(
            "{id}: the kill names {}, which the row does not cite",
            kill.test
        )];
    }
    let Some(bug) = ledger.get(&kill.plant) else {
        return vec![format!("{id}: no ledger entry {:?}", kill.plant)];
    };
    let Some((package, binary, path)) = split(&kill.test, binaries) else {
        return vec![format!("{id}: {} is not `<package>::<path>`", kill.test)];
    };
    let mut out = Vec::new();
    if bug.package != package {
        out.push(format!(
            "{id}: {} is planted against {}, not {package}",
            kill.plant, bug.package
        ));
    }
    if let Some(bin) = &bug.test_bin
        && binary != Some(bin.as_str())
    {
        out.push(format!(
            "{id}: {} runs binary {bin}, not the one {} is in",
            kill.plant, kill.test
        ));
    }
    if !bug
        .tests
        .iter()
        .any(|filter| path.contains(filter.as_str()))
    {
        out.push(format!(
            "{id}: no test filter of {} matches {path}",
            kill.plant
        ));
    }
    out
}

/// Every cited test, as `<binary id>::<path>`, from a `--message-format oneline` listing.
fn listed(listing: &str) -> BTreeSet<String> {
    listing
        .lines()
        .filter_map(|line| line.split_once(' '))
        .map(|(binary, test)| format!("{binary}::{test}"))
        .collect()
}

/// The just recipes, by name, from the justfile's recipe lines.
fn recipes() -> BTreeSet<String> {
    read(&workspace_root().join("justfile"))
        .lines()
        .filter(|line| !line.starts_with([' ', '\t', '#']))
        .filter_map(|line| line.split_once(':').map(|(head, _)| head))
        .filter_map(|head| head.split_whitespace().next())
        .filter(|name| !name.contains('='))
        .map(str::to_owned)
        .collect()
}

/// What the listings show wrong with `doc`.
fn listing_problems(
    doc: &Doc,
    all: &BTreeSet<String>,
    ignored: &BTreeSet<String>,
    recipes: &BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    for row in &doc.row {
        let tests = row.tests();
        for test in tests.iter().filter(|t| !all.contains(**t)) {
            out.push(format!(
                "{}: {test} is not a test `cargo nextest list` knows",
                row.id
            ));
        }
        let any_ignored = tests.iter().any(|t| ignored.contains(*t));
        match (&row.runs_in, any_ignored) {
            (Some(recipe), true) if recipes.contains(recipe) => {}
            (Some(recipe), true) => out.push(format!("{}: no just recipe {recipe:?}", row.id)),
            (None, true) => out.push(format!(
                "{}: cites an ignored test and no `runs_in`",
                row.id
            )),
            (Some(_), false) => out.push(format!("{}: `runs_in` with no ignored test", row.id)),
            (None, false) => {}
        }
        for kill in row.kill.iter().filter(|k| ignored.contains(&k.test)) {
            out.push(format!(
                "{}: the kill's test {} is ignored, and the ledger run never runs it",
                row.id, kill.test
            ));
        }
    }
    out
}

/// The binaries the rows' own ids name, for the check that has no listing: any id with three or
/// more `::`-parts whose second part is the name of an integration-test binary directory.
fn binaries_from_rows(doc: &Doc) -> BTreeSet<String> {
    let root = workspace_root().join("crates");
    doc.row
        .iter()
        .flat_map(Row::tests)
        .filter_map(|test| {
            let mut parts = test.splitn(3, "::");
            let (package, binary, _) = (parts.next()?, parts.next()?, parts.next()?);
            let dir = package.strip_prefix("lsr-")?;
            root.join(dir)
                .join("tests")
                .join(binary)
                .join("main.rs")
                .is_file()
                .then(|| format!("{package}::{binary}"))
        })
        .collect()
}

#[test]
fn the_evidence_rows_are_well_formed() {
    let doc = evidence();
    assert!(
        doc.row.len() >= 10,
        "docs/evidence.toml lists {} rows",
        doc.row.len()
    );
    let found = problems(&doc, &ledger(), &binaries_from_rows(&doc));
    assert!(
        found.is_empty(),
        "docs/evidence.toml:\n{}",
        found.join("\n")
    );
    let unkilled: Vec<String> = doc
        .row
        .iter()
        .filter(|r| r.kill.is_empty() && !r.tests().is_empty())
        .map(|r| {
            format!(
                "{} [{}]: {}",
                r.id,
                r.mark(),
                r.unkilled.as_deref().unwrap_or_default()
            )
        })
        .collect();
    eprintln!(
        "no kill demonstrated ({}):\n{}",
        unkilled.len(),
        unkilled.join("\n")
    );
}

#[test]
#[ignore = "needs the Rust test listings; `just evidence-fresh` writes them and runs this"]
fn every_cited_test_exists_and_runs_where_it_says() {
    let listing = |var: &str| {
        let path = std::env::var_os(var)
            .map(PathBuf::from)
            .unwrap_or_else(|| panic!("{var} is unset; run `just evidence-fresh`"));
        read(&path)
    };
    let (all_text, ignored_text) = (listing(ALL_VAR), listing(IGNORED_VAR));
    let (all, ignored) = (listed(&all_text), listed(&ignored_text));
    assert!(
        all.len() > 1000 && !ignored.is_empty(),
        "{} tests, {} ignored",
        all.len(),
        ignored.len()
    );
    let doc = evidence();
    let lines: BTreeSet<String> = all_text.lines().map(str::to_owned).collect();
    let mut found = problems(&doc, &ledger(), &binaries_of(&lines));
    found.extend(listing_problems(&doc, &all, &ignored, &recipes()));
    assert!(
        found.is_empty(),
        "docs/evidence.toml:\n{}",
        found.join("\n")
    );
}

/// One ledger entry, `p`, planted against `lsr-x`'s binary `it`, killed by `kills_it`.
fn sample_ledger() -> BTreeMap<String, Bug> {
    [Bug {
        name: "p".into(),
        package: "lsr-x".into(),
        tests: vec!["kills_it".into()],
        test_bin: Some("it".into()),
    }]
    .into_iter()
    .map(|b| (b.name.clone(), b))
    .collect()
}

/// One sound row and one of each kind of wrong one.
const SAMPLE: &str = r#"
        [[row]]
        id = "OK"
        circumstance = "c"
        detected = ["lsr-x::it::m::kills_it"]
        [[row.kill]]
        test = "lsr-x::it::m::kills_it"
        plant = "p"

        [[row]]
        id = "NO-KILL"
        circumstance = "c"
        detected = ["lsr-x::it::m::other"]

        [[row]]
        id = "BAD-MARK"
        circumstance = "c"
        none = "probably fine"

        [[row]]
        id = "WRONG-PLANT"
        circumstance = "c"
        detected = ["lsr-x::m::kills_it"]
        [[row.kill]]
        test = "lsr-x::m::kills_it"
        plant = "p"

        [[row]]
        id = "OK"
        circumstance = "c"
        none = "untested"
        "#;

/// The checks can fail: each kind of wrong row is reported, and a sound one is not.
#[test]
fn a_wrong_row_is_reported() {
    let ledger = sample_ledger();
    let binaries = BTreeSet::from(["lsr-x::it".to_owned()]);
    let doc: Doc = toml::from_str(SAMPLE).unwrap();
    let found = problems(&doc, &ledger, &binaries);
    let rows: BTreeSet<&str> = found.iter().filter_map(|p| p.split(':').next()).collect();
    assert_eq!(
        rows,
        BTreeSet::from(["NO-KILL", "BAD-MARK", "WRONG-PLANT", "OK"]),
        "{found:#?}"
    );
    assert!(
        found.iter().any(|p| p.contains("the id is taken")),
        "{found:#?}"
    );
    let all = BTreeSet::from(["lsr-x::it::m::kills_it".to_owned()]);
    let ignored = all.clone();
    let listing = listing_problems(&doc, &all, &ignored, &BTreeSet::new());
    assert!(
        listing.iter().any(|p| p.contains("no `runs_in`")),
        "{listing:#?}"
    );
    assert!(
        listing
            .iter()
            .any(|p| p.contains("ledger run never runs it")),
        "{listing:#?}"
    );
    assert!(
        listing
            .iter()
            .any(|p| p.contains("lsr-x::it::m::other is not a test")),
        "{listing:#?}"
    );
}
