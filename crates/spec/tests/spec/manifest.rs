//! The spec's independence is a build fact: it may not depend on the code it
//! judges. A dependency on the core would let an oracle call the planner's
//! helpers and copy their mistakes, so this holds the manifest to none.

use std::fs;
use std::path::PathBuf;

fn manifest() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    fs::read_to_string(&path).unwrap_or_else(|err| panic!("{}: {err}", path.display()))
}

#[test]
fn the_spec_does_not_depend_on_the_core() {
    let text = manifest();
    assert!(
        text.contains("name = \"lsr-spec\""),
        "not the spec's manifest"
    );
    assert!(
        !text.contains("lsr-core"),
        "lsr-spec names lsr-core in its manifest"
    );
}

#[test]
fn the_spec_has_no_dependencies_at_all() {
    let text = manifest();
    let tables: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with('[') && line.contains("dependencies"))
        .collect();
    assert!(
        tables.is_empty(),
        "lsr-spec declares dependency tables {tables:?}: whatever it depends on could reach the core"
    );
}
