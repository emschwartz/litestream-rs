//! The actor's obligation ledger (`crate::obligations`) is the closed world of its state, and
//! every field held in the engine journal is read at boot and written at the end of a tick.

use super::*;
use crate::obligations::{Keep, Record, unclassified};

const PARTS: &str = include_str!("../parts.rs");
const ACTOR: &str = include_str!("../../actor.rs");

#[test]
fn every_field_of_the_actor_s_state_is_classified() {
    let mut wrong = unclassified(PARTS, "Kept", parts::KEPT_LEDGER);
    wrong.extend(unclassified(PARTS, "Engine", parts::ENGINE_LEDGER));
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The body of the method `name` in `source`: from its signature to the closing brace at the
/// method's own indentation.
fn method(source: &str, name: &str) -> String {
    let signature = format!("fn {name}(");
    let mut lines = source.lines().skip_while(|line| !line.contains(&signature));
    let mut body: Vec<&str> = lines.by_ref().take_while(|line| *line != "    }").collect();
    assert!(!body.is_empty(), "no method {name}");
    body.push("    }");
    body.join("\n")
}

/// A field the journal holds is restored by boot and recorded by the end-of-tick reconcile;
/// one that either forgets is an obligation held only in memory.
#[test]
fn every_journalled_field_is_read_at_boot_and_written_at_the_end_of_a_tick() {
    let read = method(ACTOR, "adopt_journal");
    let written = method(ACTOR, "reconcile_journal");
    let journalled: Vec<&str> = parts::KEPT_LEDGER
        .iter()
        .filter(|(_, keep)| matches!(keep, Keep::Journalled(Record::EngineJournal, _)))
        .map(|(field, _)| *field)
        .collect();
    assert!(!journalled.is_empty());
    for field in journalled {
        let named = format!("kept.{field}");
        let in_read = read.contains(&format!("read.{field}")) || read.contains(&named);
        assert!(in_read, "adopt_journal does not restore {field}");
        assert!(
            written.contains(&named),
            "reconcile_journal does not record {field}"
        );
    }
}
