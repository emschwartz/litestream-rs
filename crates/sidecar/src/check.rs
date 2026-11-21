//! `lsr config check <file>`: the file loaded as `replicate -config <file>` loads it,
//! then every check the daemon makes before it opens a database, through the daemon's
//! own functions ([`crate::prepare`]), so the check refuses exactly what a start refuses.
//!
//! No `-exec` and no `-once` are given, so only the file is judged. The plan scans a
//! `dir` entry as a start does (a directory walk and each candidate's 16-byte header);
//! nothing is written and no bucket is contacted.

use std::fmt;
use std::path::Path;

use lsr_config::{Expand, Note, Report};

use crate::SidecarError;

/// What `lsr config check` found.
#[derive(Debug)]
pub struct Checked {
    report: Report,
    refusals: Vec<SidecarError>,
    notes: Vec<Note>,
}

impl Checked {
    /// The load's report: every key, warning and load problem.
    #[must_use]
    pub fn report(&self) -> &Report {
        &self.report
    }

    /// What a start would refuse once the file loaded, in the order the daemon checks;
    /// the first is the one the daemon would exit with. Empty when the file does not load
    /// (the report says why) or would start.
    #[must_use]
    pub fn refusals(&self) -> &[SidecarError] {
        &self.refusals
    }

    /// Settings accepted with no effect, which a start logs as warnings.
    #[must_use]
    pub fn notes(&self) -> &[Note] {
        &self.notes
    }

    fn error_lines(&self) -> Vec<String> {
        let mut lines: Vec<String> = self.report.errors.iter().map(ToString::to_string).collect();
        for refusal in &self.refusals {
            if let SidecarError::Map(error) = refusal {
                lines.extend(error.problems().iter().map(ToString::to_string));
            } else {
                lines.push(refusal.to_string());
            }
        }
        lines
    }
}

/// Loads the file as the daemon's `-config` does (`-` is standard input, environment
/// expansion on) and runs the daemon's checks on it.
#[must_use]
pub fn check(path: &Path) -> Checked {
    let loaded = match crate::load_file(path, Expand::Env) {
        Ok(loaded) => loaded,
        Err(SidecarError::Config(error)) => {
            return Checked {
                report: error.report().clone(),
                refusals: Vec::new(),
                notes: Vec::new(),
            };
        }
        Err(other) => {
            return Checked {
                report: Report::default(),
                refusals: vec![other],
                notes: Vec::new(),
            };
        }
    };
    let (refusals, notes) = match crate::prepare(None, false, &loaded.config) {
        Ok(prepared) => (Vec::new(), prepared.plan.notes().to_vec()),
        Err(refused) => {
            let mut all = vec![refused.first];
            all.extend(refused.rest);
            (all, Vec::new())
        }
    };
    Checked {
        report: loaded.report,
        refusals,
        notes,
    }
}

impl fmt::Display for Checked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let errors = self.error_lines();
        for error in &errors {
            writeln!(f, "error: {error}")?;
        }
        for warning in &self.report.warnings {
            writeln!(f, "warning: {warning}")?;
        }
        for note in &self.notes {
            writeln!(f, "warning: {note}")?;
        }
        if errors.is_empty() {
            writeln!(f, "ok: {} keys read", self.report.keys.len())
        } else {
            writeln!(f, "refused: {} errors", errors.len())
        }
    }
}
