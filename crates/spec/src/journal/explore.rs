//! The exhaustive checker: a depth-first walk over every state reachable within a
//! configuration's budget, remembering each state so a state reached two ways is expanded once.
//!
//! Every transition is one system call or one crash image, so "every reachable state" includes
//! a crash at every system call of every history the budget allows, followed by a recovery that
//! is itself explored to the end of the budget.

use std::collections::{BTreeSet, HashSet};

use super::world::{Config, Label, Property, State};

/// A history that breaks a property.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Counterexample {
    /// The property broken.
    pub property: Property,
    /// Every transition from the initial state, the breaking one last.
    pub trace: Vec<Label>,
}

/// What a walk found.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Report {
    /// Distinct states expanded.
    pub states: usize,
    /// The first counterexample, if any.
    pub counterexample: Option<Counterexample>,
    /// Every distinct label taken, so a test can assert the walk reached what it claims to.
    pub reached: BTreeSet<String>,
}

/// Walks every state `config` reaches.
#[must_use]
pub fn explore(config: &Config) -> Report {
    let mut walk = Walk {
        config,
        seen: HashSet::new(),
        path: Vec::new(),
        reached: BTreeSet::new(),
    };
    let counterexample = walk.visit(State::initial(config));
    Report {
        states: walk.seen.len(),
        counterexample,
        reached: walk.reached,
    }
}

struct Walk<'a> {
    config: &'a Config,
    seen: HashSet<State>,
    path: Vec<Label>,
    reached: BTreeSet<String>,
}

impl Walk<'_> {
    fn visit(&mut self, state: State) -> Option<Counterexample> {
        if self.seen.contains(&state) {
            return None;
        }
        let steps = state.successors(self.config);
        self.seen.insert(state);
        for (label, next) in steps {
            self.reached.insert(label.kind());
            self.path.push(label);
            let found = match next {
                Err(property) => Some(Counterexample {
                    property,
                    trace: self.path.clone(),
                }),
                Ok(next) => self.visit(next),
            };
            if found.is_some() {
                return found;
            }
            self.path.pop();
        }
        None
    }
}
