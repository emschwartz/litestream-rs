//! A directed walk: one transition at a time, each chosen by what its label says.
//!
//! A step is a pattern over [`Label::kind`]: pieces separated by `*`, each of which must appear
//! in the label's text after the one before, the first at its start. `"Power*s0: Some(New)*len:
//! Short1"` takes the power loss that kept slot 0's new bytes and a length of 4096 to 8191.
//! Scenarios are written this way, and a differential test can replay the same steps against
//! the implementation.

use super::world::{Config, Label, Property, State};

/// Why a step could not be taken.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ScriptError {
    /// No transition matches the step; `offered` is what the state allowed.
    NoSuchStep {
        /// The step.
        step: String,
        /// Every transition's kind from that state.
        offered: Vec<String>,
    },
    /// The step's transition broke a property.
    Broke {
        /// The step.
        step: String,
        /// The property.
        property: Property,
    },
}

/// A walk in progress.
#[derive(Clone, Debug)]
pub struct Script {
    config: Config,
    state: State,
    trace: Vec<Label>,
}

/// Whether `kind` matches `step` (module doc).
fn matches(kind: &str, step: &str) -> bool {
    let mut pieces = step.split('*');
    let Some(first) = pieces.next() else {
        return true;
    };
    let Some(mut rest) = kind.strip_prefix(first) else {
        return false;
    };
    for piece in pieces {
        match rest.find(piece) {
            Some(at) => {
                rest = rest
                    .get(at.saturating_add(piece.len())..)
                    .unwrap_or_default()
            }
            None => return false,
        }
    }
    true
}

impl Script {
    /// A walk from the initial state of `config`.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            state: State::initial(&config),
            config,
            trace: Vec::new(),
        }
    }

    /// Takes each step in turn.
    ///
    /// # Errors
    ///
    /// The first step that matches nothing or breaks a property.
    pub fn run(config: Config, steps: &[&str]) -> Result<Self, ScriptError> {
        let mut script = Self::new(config);
        script.steps(steps)?;
        Ok(script)
    }

    /// Takes each step in turn from here.
    ///
    /// # Errors
    ///
    /// The first step that matches nothing or breaks a property.
    pub fn steps(&mut self, steps: &[&str]) -> Result<&mut Self, ScriptError> {
        for step in steps {
            self.take(step)?;
        }
        Ok(self)
    }

    /// Takes the first transition whose label matches `step`.
    ///
    /// # Errors
    ///
    /// When none matches, or the one taken breaks a property.
    pub fn take(&mut self, step: &str) -> Result<&mut Self, ScriptError> {
        let steps = self.state.successors(&self.config);
        let offered: Vec<String> = steps.iter().map(|(label, _)| label.kind()).collect();
        let Some((label, next)) = steps
            .into_iter()
            .find(|(label, _)| matches(&label.kind(), step))
        else {
            return Err(ScriptError::NoSuchStep {
                step: (*step).to_owned(),
                offered,
            });
        };
        let next = next.map_err(|property| ScriptError::Broke {
            step: (*step).to_owned(),
            property,
        })?;
        self.trace.push(label);
        self.state = next;
        Ok(self)
    }

    /// Where the walk is.
    #[must_use]
    pub const fn state(&self) -> &State {
        &self.state
    }

    /// The transitions taken.
    #[must_use]
    pub fn trace(&self) -> &[Label] {
        &self.trace
    }
}
