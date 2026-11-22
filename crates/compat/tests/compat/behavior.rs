//! The behaviour table as data (`docs/behavior.toml`), rendered into `docs/BEHAVIOR.md`.
//!
//! The rows of section 3 live in the TOML, one `[[row]]` each, every column's cell verbatim
//! (the column-to-key map is the document's section 7.2). The rest of the document is
//! `docs/behavior/template.md`, where `<!-- rows: <table> -->` stands for a table. Checked:
//!
//! - always (`the_behavior_doc_is_rendered_from_its_rows`): the rendered document equals
//!   `docs/BEHAVIOR.md` (`just behavior-doc` writes it), ids are unique, every row's table has a
//!   marker and every marker rows, a row with no test in any evidence column carries a no-test
//!   mark from the closed list (optionally after a scope and a colon), and every evidence cell
//!   parses as citations `<kind>::`\``<name>`\``, or a backticked path in the tree with an
//!   optional parenthetical scope, with nothing else in between; every path exists, every `plant::`
//!   names a ledger entry, every `oracle::` a `Violation` variant, every `judge::` an `Anomaly`
//!   variant; and an In review cell starts with a PR number, a branch tag or `None`;
//! - with the Rust test listing (`every_behavior_citation_resolves`, run by `just
//!   evidence-fresh`): every test citation names a test of its crate's package (by its name, or a
//!   path ending in it, as the document cites it) or, for `sim::`, a simulator scenario.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use lsr_compat::workspace_root;
use serde::Deserialize;

const HEADER: &str = "| ID | Circumstance and how it is detected | Response | Signal | Data at risk, and the bound | Recovery | Detected by | Survived by | Recovered by | No-test mark | Notes | In review |";
const RULE: &str = "|---|---|---|---|---|---|---|---|---|---|---|---|";
const MARKS: [&str; 6] = [
    "code only",
    "assumed",
    "untested",
    "unknown",
    "known wrong",
    "accepted, undetected",
];
/// The kinds of citation that are not tests.
const CHECKS: [&str; 3] = ["plant", "oracle", "judge"];

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Doc {
    row: Vec<Row>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    table: String,
    id: String,
    #[serde(default)]
    circumstance: String,
    #[serde(default)]
    response: String,
    #[serde(default)]
    signal: String,
    #[serde(default)]
    at_risk: String,
    #[serde(default)]
    recovery: String,
    #[serde(default)]
    detected_by: String,
    #[serde(default)]
    survived_by: String,
    #[serde(default)]
    recovered_by: String,
    #[serde(default)]
    no_test: String,
    #[serde(default)]
    notes: String,
    #[serde(default)]
    in_review: String,
}

impl Row {
    fn cells(&self) -> [&str; 12] {
        [
            &self.id,
            &self.circumstance,
            &self.response,
            &self.signal,
            &self.at_risk,
            &self.recovery,
            &self.detected_by,
            &self.survived_by,
            &self.recovered_by,
            &self.no_test,
            &self.notes,
            &self.in_review,
        ]
    }

    fn evidence(&self) -> [&str; 3] {
        [&self.detected_by, &self.survived_by, &self.recovered_by]
    }
}

/// One citation: its kind (`sim`, `core`, `plant`, ...) and its name.
type Citation = (String, String);

/// Every citation in a cell, or the text that is not one.
fn citations(cell: &str) -> Result<Vec<Citation>, String> {
    let mut out = Vec::new();
    let mut rest = cell.trim();
    while !rest.is_empty() {
        if let Some((path, tail)) = path_citation(rest) {
            out.push((PATH.to_owned(), path.to_owned()));
            rest = separated(tail)?;
            continue;
        }
        let (kind, after) = rest
            .split_once("::`")
            .ok_or_else(|| format!("{rest:?} is not `kind::`name``"))?;
        let named = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '_';
        if kind.is_empty() || !kind.chars().all(named) {
            return Err(format!("{kind:?} is not a crate or check name"));
        }
        let (name, tail) = after
            .split_once('`')
            .ok_or_else(|| format!("{after:?} has no closing backtick"))?;
        out.push((kind.to_owned(), name.to_owned()));
        rest = separated(tail)?;
    }
    Ok(out)
}

/// The kind of a citation that is a path in the tree (a rig script, say), as the document's
/// section 7.3 allows: `` `scripts/lazyfs.py` (dead-mount restores) ``.
const PATH: &str = "path";

/// A backticked path with an optional parenthetical scope after it, and what follows.
fn path_citation(text: &str) -> Option<(&str, &str)> {
    let (path, tail) = text.strip_prefix('`')?.split_once('`')?;
    let tail = tail.trim_start();
    let tail = match tail.strip_prefix('(') {
        Some(scoped) => scoped.split_once(')').map_or(tail, |(_, after)| after),
        None => tail,
    };
    Some((path, tail))
}

/// What follows a citation, past the comma or semicolon that must separate it from the next.
fn separated(tail: &str) -> Result<&str, String> {
    let rest = tail.trim_start_matches([',', ';', ' ']);
    let between = tail
        .get(..tail.len().saturating_sub(rest.len()))
        .unwrap_or_default();
    if !rest.is_empty() && !between.contains([',', ';']) {
        return Err(format!("{rest:?} follows a citation with no separator"));
    }
    Ok(rest)
}

fn read(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn docs() -> PathBuf {
    workspace_root().join("docs")
}

fn rows() -> Doc {
    let path = docs().join("behavior.toml");
    toml::from_str(&read(&path)).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

/// The document: the template with each marker replaced by its table.
fn render(template: &str, doc: &Doc) -> Result<String, String> {
    let mut by_table: BTreeMap<&str, Vec<&Row>> = BTreeMap::new();
    for row in &doc.row {
        by_table.entry(row.table.as_str()).or_default().push(row);
    }
    let mut used = BTreeSet::new();
    let mut out = Vec::new();
    for line in template.split('\n') {
        let Some(table) = line
            .strip_prefix("<!-- rows: ")
            .and_then(|l| l.strip_suffix(" -->"))
        else {
            out.push(line.to_owned());
            continue;
        };
        let rows = by_table
            .get(table)
            .ok_or_else(|| format!("the marker for {table:?} has no rows"))?;
        used.insert(table);
        out.push(HEADER.to_owned());
        out.push(RULE.to_owned());
        out.extend(
            rows.iter()
                .map(|r| format!("| {} |", r.cells().join(" | "))),
        );
    }
    if let Some(orphan) = by_table.keys().find(|t| !used.contains(*t)) {
        return Err(format!(
            "rows of table {orphan:?} have no marker in the template"
        ));
    }
    Ok(out.join("\n"))
}

/// The names a source file declares as enum variants: lines `    Name {`, `    Name(` or
/// `    Name,` at four spaces.
fn variants(path: &str) -> BTreeSet<String> {
    read(&workspace_root().join(path))
        .lines()
        .filter_map(|l| l.strip_prefix("    "))
        .filter(|l| l.starts_with(|c: char| c.is_ascii_uppercase()))
        .filter_map(|l| l.split([' ', '(', ',', '{']).next())
        .map(str::to_owned)
        .collect()
}

fn ledger() -> BTreeSet<String> {
    #[derive(Deserialize)]
    struct Ledger {
        bug: Vec<Bug>,
    }
    #[derive(Deserialize)]
    struct Bug {
        name: String,
    }
    let ledger: Ledger = toml::from_str(&read(&docs().join("planted-bugs.toml"))).unwrap();
    ledger.bug.into_iter().map(|b| b.name).collect()
}

/// An In review cell names what is in review first: a PR number (`#96`), a branch tag of two to
/// four capitals (`WSF`), or `None`; prose may follow. Notes that slid into it fail here.
fn in_review_form(cell: &str) -> bool {
    let first = cell.split([' ', ':', ',', '.']).next().unwrap_or_default();
    let pr = first
        .strip_prefix('#')
        .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()));
    let tag = (2..=4).contains(&first.len()) && first.chars().all(|c| c.is_ascii_uppercase());
    pr || tag || first == "None"
}

/// What is wrong with the rows that needs no listing.
fn problems(doc: &Doc) -> Vec<String> {
    let (plants, oracle, judge) = (
        ledger(),
        variants("crates/sim/src/oracle.rs"),
        variants("crates/history/src/judge.rs"),
    );
    let mut out = Vec::new();
    let mut ids = BTreeSet::new();
    for row in &doc.row {
        let id = &row.id;
        if !ids.insert(id.as_str()) {
            out.push(format!("{id}: the id is taken"));
        }
        let mut tests = 0_usize;
        for cell in row.evidence() {
            match citations(cell) {
                Err(why) => out.push(format!("{id}: {why}")),
                Ok(cited) => {
                    tests = tests.saturating_add(
                        cited
                            .iter()
                            .filter(|(k, _)| !CHECKS.contains(&k.as_str()))
                            .count(),
                    );
                    for (kind, name) in cited {
                        let head = name.split('(').next().unwrap_or_default().to_owned();
                        let known = match kind.as_str() {
                            "plant" => plants.contains(&name),
                            "oracle" => oracle.contains(&head),
                            "judge" => judge.contains(&head),
                            PATH => workspace_root().join(&name).exists(),
                            _ => true,
                        };
                        if !known {
                            out.push(format!("{id}: {kind}::{name} names nothing that exists"));
                        }
                    }
                }
            }
        }
        let mark = row.no_test.rsplit(':').next().unwrap_or_default().trim();
        if !row.no_test.is_empty() && !MARKS.iter().any(|m| mark.starts_with(m)) {
            out.push(format!("{id}: {:?} is not a no-test mark", row.no_test));
        }
        if !row.in_review.is_empty() && !in_review_form(&row.in_review) {
            out.push(format!(
                "{id}: the In review cell starts with no PR number, branch tag or `None`: {:?}",
                row.in_review
            ));
        }
        if tests == 0 && row.no_test.is_empty() {
            out.push(format!("{id}: no test and no no-test mark"));
        }
    }
    out
}

#[test]
fn the_behavior_doc_is_rendered_from_its_rows() {
    let doc = rows();
    assert!(doc.row.len() > 100, "{} rows", doc.row.len());
    let found = problems(&doc);
    assert!(
        found.is_empty(),
        "docs/behavior.toml:\n{}",
        found.join("\n")
    );
    let template = read(&docs().join("behavior").join("template.md"));
    let rendered = render(&template, &doc).unwrap_or_else(|why| panic!("{why}"));
    let path = docs().join("BEHAVIOR.md");
    if std::env::var_os("LSR_BEHAVIOR_WRITE").is_some() {
        fs::write(&path, &rendered).unwrap();
        return;
    }
    assert!(
        read(&path) == rendered,
        "docs/BEHAVIOR.md is not what docs/behavior.toml and docs/behavior/template.md render; \
         edit those and run `just behavior-doc`"
    );
}

/// The simulator's scenarios: `pub fn <name>() -> Scenario`.
fn scenarios() -> BTreeSet<String> {
    [
        "crates/sim/src/scenario.rs",
        "crates/sim/src/scenario/faults.rs",
    ]
    .iter()
    .flat_map(|p| {
        read(&workspace_root().join(p))
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    })
    .filter(|l| l.ends_with("-> Scenario {"))
    .filter_map(|l| {
        l.trim_start_matches("pub fn ")
            .split('(')
            .next()
            .map(str::to_owned)
    })
    .collect()
}

#[test]
#[ignore = "needs the Rust test listing; `just evidence-fresh` writes it and runs this"]
fn every_behavior_citation_resolves() {
    let path = std::env::var_os("LSR_EVIDENCE_ALL_TESTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("LSR_EVIDENCE_ALL_TESTS is unset; run `just evidence-fresh`"));
    // Package to every test path in it.
    let mut names: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for line in read(&path).lines() {
        let Some((binary, test)) = line.split_once(' ') else {
            continue;
        };
        let package = binary.split("::").next().unwrap_or_default().to_owned();
        names.entry(package).or_default().insert(test.to_owned());
    }
    assert!(names.len() > 5, "{} packages listed", names.len());
    let scenarios = scenarios();
    let mut missing = Vec::new();
    for row in rows().row {
        for (kind, name) in row
            .evidence()
            .iter()
            .filter_map(|c| citations(c).ok())
            .flatten()
        {
            if CHECKS.contains(&kind.as_str()) || kind == PATH {
                continue;
            }
            // The document cites a test by its name, or by a path ending in it.
            let suffix = format!("::{name}");
            let listed = names
                .get(&format!("lsr-{kind}"))
                .is_some_and(|n| n.iter().any(|t| *t == name || t.ends_with(&suffix)));
            if !listed && !(kind == "sim" && scenarios.contains(&name)) {
                missing.push(format!("{}: {kind}::{name}", row.id));
            }
        }
    }
    assert!(
        missing.is_empty(),
        "citations that name no test:\n{}",
        missing.join("\n")
    );
}

#[test]
fn a_citation_cell_parses_or_says_why_not() {
    assert_eq!(
        citations("core::`a`, sim::`b`; plant::`c`"),
        Ok(vec![
            ("core".into(), "a".into()),
            ("sim".into(), "b".into()),
            ("plant".into(), "c".into())
        ])
    );
    assert_eq!(citations(""), Ok(vec![]));
    citations("core `a`").unwrap_err();
    citations("core::`a` sim::`b`").unwrap_err();
    citations("Core::`a`").unwrap_err();
    assert_eq!(
        citations("`scripts/lazyfs.py` (dead-mount restores), sim::`b`"),
        Ok(vec![
            (PATH.into(), "scripts/lazyfs.py".into()),
            ("sim".into(), "b".into())
        ])
    );
}
