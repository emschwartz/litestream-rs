//! Executable contracts: what a component must do, restated from its
//! documentation as a brute-force oracle that the implementation cannot reach.
//!
//! An oracle that shares helpers with the code it judges goes wrong in the same
//! direction as the code (two planner oracles once copied the planner's own
//! flawed rule). This crate depends on nothing, the core least of all, so a
//! test that judges the core by it compares two independent readings of one
//! document. Each module restates one contract over its own minimal types; a
//! thin adapter in the code under test converts its values into these.
//!
//! Every rule cites the clause of the document it implements ([`restore_plan::Clause`]),
//! and every judgement says which clause decided it. Where the document
//! leaves a choice open, the judgement lists every allowed outcome instead of
//! picking one, so the place the contract is silent is written down. An empty
//! set of allowed outcomes means the document decides nothing for that input:
//! a hole in the contract, never a pass.

#![forbid(unsafe_code)]

pub mod restore_plan;
