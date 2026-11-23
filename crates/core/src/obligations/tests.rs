//! The parser and the closed-world check, on small sources.

use super::*;

const SOURCE: &str = "
/// Doc.
#[derive(Debug)]
pub(super) struct Held {
    /// A field.
    pub(super) one: u8,
    // a comment: with a colon,
    #[expect(dead_code, reason = \"x\")]
    two: Option<u8>,
    pub three: Vec<u8>,
}

struct Other {
    four: u8,
}
";

#[test]
fn the_fields_of_one_struct_are_read_and_nothing_else() {
    assert_eq!(fields_of(SOURCE, "Held"), ["one", "two", "three"]);
    assert_eq!(fields_of(SOURCE, "Other"), ["four"]);
    assert!(fields_of(SOURCE, "Missing").is_empty());
}

#[test]
fn a_ledger_that_is_the_closed_world_has_nothing_wrong() {
    let ledger = [
        ("one", Keep::Derived("d")),
        ("two", Keep::Ephemeral("e")),
        ("three", Keep::Journalled(Record::EngineJournal, "j")),
    ];
    assert!(unclassified(SOURCE, "Held", &ledger).is_empty());
}

/// Every way a ledger can stop being the struct's closed world is named.
#[test]
fn each_way_out_of_the_closed_world_is_named() {
    let ledger = [
        ("one", Keep::Derived("d")),
        ("one", Keep::Derived("d")),
        ("three", Keep::Owed(" ")),
        ("gone", Keep::Ephemeral("e")),
    ];
    assert_eq!(
        unclassified(SOURCE, "Held", &ledger),
        [
            "Held.two: no ledger entry",
            "Held.one: named twice",
            "Held.three: no reason",
            "Held.gone: an entry with no such field",
        ]
    );
    assert_eq!(
        unclassified(SOURCE, "Missing", &[]),
        ["struct Missing: not found, or has no fields"]
    );
}

/// A field line names one identifier and ends with a comma; each part is needed: a line whose
/// type runs on, a name that is not one identifier, and no name at all declare no field.
#[test]
fn a_field_line_is_one_name_and_a_whole_type() {
    assert_eq!(field_name("one: u8,"), Some("one".to_owned()));
    assert_eq!(
        field_name("one: u8"),
        None,
        "the type runs on to the next line"
    );
    assert_eq!(field_name("one two: u8,"), None, "not one identifier");
    assert_eq!(field_name(": u8,"), None, "no name");
}
