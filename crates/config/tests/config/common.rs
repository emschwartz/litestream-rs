//! Shared helpers for the config tests: a fixed environment and short ways to
//! load text and look at what came back.

use std::collections::HashMap;
use std::path::PathBuf;

use lsr_config::{
    ConfigError, Env, Expand, GoConfig, Loaded, Problem, ProblemKind, Warning, WarningKind,
};

/// A fixed environment: home `/home/u`, current directory `/work`, pid 4242.
#[derive(Default)]
pub struct TestEnv {
    pub vars: HashMap<String, String>,
}

impl TestEnv {
    pub fn with(vars: &[(&str, &str)]) -> Self {
        Self {
            vars: vars
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        }
    }
}

impl Env for TestEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.vars.get(name).cloned()
    }
    fn pid(&self) -> u32 {
        4242
    }
    fn home_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/home/u"))
    }
    fn current_dir(&self) -> Option<PathBuf> {
        Some(PathBuf::from("/work"))
    }
}

/// Parses with expansion off, as most of Go's tests do (`ParseConfig(r, false)`).
pub fn parse(text: &str) -> Loaded {
    match GoConfig::parse(text, Expand::Off, &TestEnv::default()) {
        Ok(loaded) => loaded,
        Err(error) => panic!("refused:\n{}", error.report()),
    }
}

pub fn config(text: &str) -> GoConfig {
    parse(text).config
}

/// The problems of a file that must be refused.
pub fn refused(text: &str) -> Vec<Problem> {
    refused_with(text, Expand::Off, &TestEnv::default())
}

pub fn refused_with(text: &str, expand: Expand, env: &dyn Env) -> Vec<Problem> {
    match GoConfig::parse(text, expand, env) {
        Ok(loaded) => panic!("accepted: {:?}", loaded.config),
        Err(error) => error_problems(&error),
    }
}

pub fn error_problems(error: &ConfigError) -> Vec<Problem> {
    error.problems().to_vec()
}

/// The one problem at `path`, or a panic listing them all.
pub fn problem_at(problems: &[Problem], path: &str) -> ProblemKind {
    match problems.iter().find(|p| p.path.to_string() == path) {
        Some(problem) => problem.kind.clone(),
        None => panic!("no problem at {path}: {problems:#?}"),
    }
}

/// Warnings at `path`.
pub fn warnings_at<'a>(warnings: &'a [Warning], path: &str) -> Vec<&'a WarningKind> {
    warnings
        .iter()
        .filter(|w| w.path.to_string() == path)
        .map(|w| &w.kind)
        .collect()
}
