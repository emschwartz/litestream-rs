//! `replicate`'s command line against Go: generated argument vectors given to the pinned
//! `litestream replicate` and to `lsr replicate`, which must both refuse the line as a usage
//! error, both accept it and then fail on the missing config file, or both print help.
//!
//! Every vector starts with `-config <a file with one database and no replica>`, so an
//! accepted line ends when the database is built and nothing replicates; Go makes its `-once`
//! checks after the config load, so the file must load. (Go builds a config from positional
//! arguments and would start; `-config` first makes that a usage error on both sides.) The
//! `MISSING` token names a file that does not exist. The rest
//! is drawn from `replicate`'s flags in every spelling Go's `flag` package reads (`-x`, `--x`,
//! `-x=v`, `-x v`, booleans with and without values), an undefined flag, `--`, positional
//! arguments and help.
//!
//! Both exit 1 on every error of `replicate` (D47), so the outcome is read from standard
//! error on both sides, and the exit codes must also be equal.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use lsr_compat::{Differential, GoVersion, go_binary, lsr_binary};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

/// One argument of the generated line; `PATH` and `MISSING` are replaced with files.
const TOKENS: &[&str] = &[
    "-config",
    "--config",
    "-config=MISSING",
    "-exec",
    "-exec=sleep 1",
    "sleep 1",
    "-once",
    "-once=false",
    "-once=true",
    "-once=maybe",
    "--once",
    "-force-snapshot",
    "-enforce-retention",
    "-restore-if-db-not-exists",
    "-restore-if-db-not-exists=0",
    "-log-level",
    "-log-level=debug",
    "-log-level=DEBUG",
    "-log-level=loud",
    "debug",
    "-no-expand-env",
    "-no-expand-env=1",
    "-bogus",
    "--",
    "db.sqlite",
    "file:///r",
    "-",
    "-h",
    "MISSING",
    "-help",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// The line is refused before the config is read.
    Usage,
    /// The line is accepted and the run fails on the missing config file.
    NoConfig,
    /// The line is accepted and the config is refused (it has no replica).
    NoReplica,
    /// Help was asked for.
    Help,
    /// Anything else, with the text.
    Other,
}

fn outcome(code: Option<i32>, stderr: &str, stdout: &str) -> Outcome {
    const USAGE: &[&str] = &[
        "flag provided but not defined",
        "flag needs an argument",
        "invalid boolean value",
        "invalid value",
        "must be positioned before",
        "cannot specify",
        "must specify at least one replica",
        "without -once",
        "with -exec",
        "help requested",
    ];
    if code == Some(0) {
        return Outcome::Help;
    }
    if stderr.contains("config file not found") {
        return Outcome::NoConfig;
    }
    if USAGE.iter().any(|needle| stderr.contains(needle)) {
        return Outcome::Usage;
    }
    if stderr.contains("config file not found") || stderr.contains("no such file") {
        return Outcome::NoConfig;
    }
    if stderr.contains("must specify replica") || stderr.contains("no replica configured") {
        return Outcome::NoReplica;
    }
    // Help printed with a non-zero exit (`-help=1`, `--h`) is a refusal.
    if stdout.contains("Usage") || stderr.contains("Usage") {
        return Outcome::Usage;
    }
    Outcome::Other
}

fn line(tokens: &[&str], config: &Path, missing: &Path) -> Vec<String> {
    let missing = missing.display().to_string();
    let config = config.display().to_string();
    let mut out = vec!["replicate".to_owned(), "-config".to_owned(), config];
    out.extend(tokens.iter().map(|t| t.replace("MISSING", &missing)));
    out
}

/// The config every line reads: one database, no replica, so a start is refused at once.
const CONFIG: &str = "dbs:\n  - path: /nonexistent/app.db\n";

/// Runs one side. Standard input carries [`CONFIG`] too, so `-config -` ends the same way
/// (an empty one would start both with no databases and run until killed).
/// One side's exit code, standard error and standard output.
type Ran = (Option<i32>, String, String);

fn run(binary: &Path, args: &[String], dir: &Path) -> Ran {
    let mut child = Command::new(binary)
        .args(args)
        .env_clear()
        .env("HOME", dir)
        .env("PATH", "/usr/bin:/bin")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    if let Some(mut stdin) = child.stdin.take() {
        // A child that never reads its input closes the pipe; that is not a failure.
        drop(stdin.write_all(CONFIG.as_bytes()));
    }
    let output = child.wait_with_output().unwrap();
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

/// Both sides' runs of one generated line, in a directory of their own.
fn answers(go: &Path, lsr: &Path, tokens: &[&str]) -> (Vec<String>, Ran, Ran) {
    let dir = tempfile::tempdir().unwrap();
    let missing: PathBuf = dir.path().join("absent.yml");
    let config = dir.path().join("litestream.yml");
    std::fs::write(&config, CONFIG).unwrap();
    let args = line(tokens, &config, &missing);
    let go_run = run(go, &args, dir.path());
    let lsr_run = run(lsr, &args, dir.path());
    (args, go_run, lsr_run)
}

fn compare(go: &Path, lsr: &Path, tokens: &[&str]) -> Result<(), TestCaseError> {
    let (args, (go_code, go_err, go_out), (lsr_code, lsr_err, lsr_out)) = answers(go, lsr, tokens);
    let go_outcome = outcome(go_code, &go_err, &go_out);
    let lsr_outcome = outcome(lsr_code, &lsr_err, &lsr_out);
    // `-once -force-snapshot` and `-once -enforce-retention` are not built yet (D48, #177):
    // refused by name where Go, having accepted the flags, goes on to build the database.
    if lsr_err.contains("is not in this build yet") {
        prop_assert!(
            go_outcome == Outcome::NoReplica && go_code == lsr_code,
            "args: {args:?}\nGo:  {go_outcome:?} ({go_code:?}) {go_err}\nlsr: {lsr_err}"
        );
        return Ok(());
    }
    prop_assert!(
        go_outcome == lsr_outcome && go_outcome != Outcome::Other && go_code == lsr_code,
        "args: {args:?}\nGo:  {go_outcome:?} ({go_code:?}) {go_err}\nlsr: {lsr_outcome:?} ({lsr_code:?}) {lsr_err}"
    );
    Ok(())
}

/// The differential: 512 generated lines per run, shrunk to the shortest that disagrees.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_read_the_same_replicate_command_lines() {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    let lsr = lsr_binary().unwrap();
    let tokens = prop::collection::vec(proptest::sample::select(TOKENS), 0..5);
    let run = Differential::new("replicate command lines");
    let compared = run.free(512, &tokens, |tokens| {
        compare(&go, &lsr, &tokens).map(|()| "compared".to_owned())
    });
    if let Err(error) = compared {
        panic!("{error}");
    }
}
