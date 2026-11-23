//! Go's read-only commands against Go: `databases`, `ltx` and `wal`, generated lines given
//! to the pinned `litestream` and to `lsr` in one directory (neither writes anything), with
//! a config naming two databases on file replicas, one of them the fixture replica.
//!
//! Standard output must be equal byte for byte, the exit codes equal (both exit 1 on every
//! error, D47), and a failure must fall in the same class, read from standard error. `TZ` is
//! `UTC` on both sides: Go prints a file replica's times in the local zone, litestream-rs in
//! UTC.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

use lsr_compat::{GoVersion, Manifest, Staged, all_fixtures, go_binary, lsr_binary, stage};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

const COMMANDS: &[(u32, &str)] = &[(4, "databases"), (4, "ltx"), (1, "wal")];

const FLAGS: &[(u32, &str)] = &[
    (4, "-config @CONFIG@"),
    (6, "-config @DIRCONFIG@"),
    (1, "-config=@MISSING@"),
    (3, "-json"),
    (1, "-json=false"),
    (1, "-no-expand-env"),
    (2, "-level all"),
    (2, "-level 1"),
    (1, "-level=9"),
    (1, "-level 10"),
    (1, "-level x"),
];

const SOURCES: &[(u32, &str)] = &[
    (4, "@DB@"),
    (2, "@URL@"),
    (1, "@EMPTYURL@"),
    (1, "@OTHERDB@"),
    (1, "@NOTINCONFIG@"),
    (2, ""),
    (1, "@DB@ @URL@"),
];

const JUNK: &[&str] = &["-bogus", "-h", "-help", "--", "-level"];

fn weighted(table: &'static [(u32, &'static str)]) -> impl Strategy<Value = &'static str> {
    let choices: Vec<_> = table.iter().map(|(w, t)| (*w, Just(*t).boxed())).collect();
    proptest::strategy::Union::new_weighted(choices)
}

#[derive(Clone, Debug)]
struct Case {
    tokens: Vec<&'static str>,
}

fn case() -> impl Strategy<Value = Case> {
    (
        weighted(COMMANDS),
        prop::collection::vec(weighted(FLAGS), 0..3),
        weighted(SOURCES),
        prop::option::weighted(0.1, (proptest::sample::select(JUNK), 0..5_usize)),
    )
        .prop_map(|(command, flags, source, junk)| {
            let mut tokens: Vec<&'static str> = std::iter::once(&command)
                .chain(flags.iter())
                .chain(std::iter::once(&source))
                .flat_map(|part| part.split_whitespace())
                .collect();
            if let Some((word, at)) = junk {
                tokens.insert(at.min(tokens.len()).max(1), word);
            }
            Case { tokens }
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Usage,
    NoConfig,
    NotInConfig,
    Other,
}

fn failure(stderr: &str, stdout: &str) -> Failure {
    const USAGE: &[&str] = &[
        "flag provided but not defined",
        "flag needs an argument",
        "invalid boolean value",
        "invalid value",
        "required",
        "too many arguments",
        "cannot specify",
        "help requested",
    ];
    if USAGE.iter().any(|needle| stderr.contains(needle)) {
        return Failure::Usage;
    }
    if stderr.contains("config file not found") || stderr.contains("No such file") {
        return Failure::NoConfig;
    }
    if stderr.contains("database not found in config") {
        return Failure::NotInConfig;
    }
    if stdout.contains("Usage") || stderr.contains("Usage") {
        return Failure::Usage;
    }
    Failure::Other
}

struct Rig {
    go: PathBuf,
    lsr: PathBuf,
    staged: Staged,
    dir: tempfile::TempDir,
}

impl Rig {
    fn path(&self, name: &str) -> String {
        self.dir.path().join(name).display().to_string()
    }

    fn line(&self, tokens: &[&str]) -> Vec<String> {
        let replica = self.staged.replica().display().to_string();
        tokens
            .iter()
            .map(|t| {
                t.replace(
                    "@EMPTYURL@",
                    &format!("file://{}", self.path("empty-replica")),
                )
                .replace("@URL@", &format!("file://{replica}"))
                .replace("@DIRCONFIG@", &self.path("with-dir.yml"))
                .replace("@CONFIG@", &self.path("litestream.yml"))
                .replace("@MISSING@", &self.path("absent.yml"))
                .replace("@OTHERDB@", &self.path("other.db"))
                .replace("@NOTINCONFIG@", &self.path("nowhere.db"))
                .replace("@DB@", &self.path("app.db"))
            })
            .collect()
    }
}

/// One side's exit code, standard error and standard output.
type Ran = (Option<i32>, String, String);

fn run(binary: &Path, args: &[String], dir: &Path) -> Ran {
    let output = Command::new(binary)
        .args(args)
        .env_clear()
        .env("HOME", dir)
        .env("PATH", "/usr/bin:/bin")
        .env("TZ", "UTC")
        .env("LITESTREAM_CONFIG", dir.join("litestream.yml"))
        .current_dir(dir)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

type Tally = Mutex<BTreeMap<String, u32>>;

fn answers(rig: &Rig, case: &Case) -> (Vec<String>, Ran, Ran) {
    let args = rig.line(&case.tokens);
    let go = run(&rig.go, &args, rig.dir.path());
    let lsr = run(&rig.lsr, &args, rig.dir.path());
    (args, go, lsr)
}

fn compare(rig: &Rig, case: &Case, tally: &Tally) -> Result<(), TestCaseError> {
    let (args, (go_code, go_err, go_out), (lsr_code, lsr_err, lsr_out)) = answers(rig, case);
    let with_dir = args.iter().any(|a| a.ends_with("with-dir.yml"));
    let ending = if go_code == Some(0) {
        let command = case.tokens.first().copied().unwrap_or_default();
        let config = if with_dir { "-with-dir" } else { "" };
        format!("{command}{config}-ok")
    } else {
        format!("{:?}", failure(&go_err, &go_out))
    };
    let mut counts = tally
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = counts.entry(ending).or_default();
    *count = count.saturating_add(1);
    drop(counts);
    let report = format!(
        "args: {args:?}\nGo  ({go_code:?}): {go_err}{go_out}\nlsr ({lsr_code:?}): {lsr_err}{lsr_out}"
    );
    prop_assert_eq!(go_code, lsr_code, "exit codes\n{}", report);
    if go_code == Some(0) {
        // Help prints each side's own usage text; everything else must match exactly, but for
        // a `dir` entry under `databases`: Go prints the working directory for it, and
        // litestream-rs the entry's directory (docs/decisions.md D162), which also moves the
        // column widths.
        let dir_entry = args.iter().any(|a| a.ends_with("with-dir.yml"))
            && args.first().is_some_and(|c| c == "databases")
            && !go_out.contains("Usage");
        if dir_entry {
            let cwd = rig.dir.path().display().to_string();
            let scan = rig.path("scan");
            let collapse = |text: &str| -> Vec<String> {
                text.lines()
                    .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
                    .collect()
            };
            let mapped: Vec<String> = collapse(&go_out)
                .into_iter()
                .map(|l| {
                    if l.starts_with(&format!("{cwd} ")) || l.contains(&format!("\"{cwd}\"")) {
                        l.replacen(&cwd, &scan, 1)
                    } else {
                        l
                    }
                })
                .collect();
            prop_assert!(
                mapped == collapse(&lsr_out),
                "databases with a dir entry\n{report}"
            );
        } else if !go_out.contains("Usage") {
            prop_assert!(go_out == lsr_out, "standard output differs\n{report}");
        }
    } else {
        let classes = (failure(&go_err, &go_out), failure(&lsr_err, &lsr_out));
        prop_assert!(
            classes.0 == classes.1 && classes.0 != Failure::Other,
            "failure classes {classes:?}\n{report}"
        );
    }
    Ok(())
}

fn rig() -> Rig {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    let fixture = all_fixtures()
        .unwrap()
        .into_iter()
        .find(|f| f.ends_with("small-4096") && f.to_string_lossy().contains("041a8c3"))
        .expect("the 041a8c3 small-4096 fixture");
    let manifest = Manifest::load(&fixture).unwrap();
    let staged = stage(&fixture, &manifest).unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("empty-replica")).unwrap();
    let config = format!(
        "dbs:\n  - path: {}\n    replica:\n      url: file://{}\n  - path: {}\n    replica:\n      url: file://{}\n",
        dir.path().join("app.db").display(),
        staged.replica().display(),
        dir.path().join("other.db").display(),
        dir.path().join("empty-replica").display(),
    );
    std::fs::write(dir.path().join("litestream.yml"), &config).unwrap();
    std::fs::create_dir(dir.path().join("scan")).unwrap();
    let with_dir = format!(
        "{config}  - dir: {}\n    pattern: \"*.db\"\n    replica:\n      url: file://{}\n",
        dir.path().join("scan").display(),
        dir.path().join("scan-replica").display(),
    );
    std::fs::write(dir.path().join("with-dir.yml"), with_dir).unwrap();
    Rig {
        go,
        lsr: lsr_binary().unwrap(),
        staged,
        dir,
    }
}

/// The differential: 512 generated lines per run, shrunk to the shortest that disagrees.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_list_the_same_databases_and_files() {
    let rig = rig();
    let mut runner = TestRunner::new(Config {
        cases: 512,
        failure_persistence: None,
        ..Config::default()
    });
    let tally = Tally::default();
    if let Err(error) = runner.run(&case(), |case| compare(&rig, &case, &tally)) {
        panic!("{error}");
    }
    let tally = tally.into_inner().unwrap();
    eprintln!("Go ended as: {tally:?}");
    for ending in [
        "databases-ok",
        "databases-with-dir-ok",
        "ltx-ok",
        "ltx-with-dir-ok",
        "wal-ok",
        "Usage",
        "NoConfig",
        "NotInConfig",
    ] {
        let seen = tally.get(ending).copied().unwrap_or(0);
        assert!(seen >= 3, "{ending} ended only {seen} cases: {tally:?}");
    }
}
