//! The actor's obligation ledger (`crate::obligations`) is the closed world of its state.

use super::*;
use crate::obligations::unclassified;

const PARTS: &str = include_str!("../parts.rs");

#[test]
fn every_field_of_the_actor_s_state_is_classified() {
    let mut wrong = unclassified(PARTS, "Kept", parts::KEPT_LEDGER);
    wrong.extend(unclassified(PARTS, "Engine", parts::ENGINE_LEDGER));
    assert!(wrong.is_empty(), "{wrong:#?}");
}
