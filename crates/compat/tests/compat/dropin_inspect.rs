//! Go's read-only commands against Go: `databases`, `ltx` and `wal`, generated lines given
//! to the pinned `litestream` and to `lsr` in one directory (neither writes anything), with
//! a config naming two databases on file replicas, one of them the fixture replica.
//!
//! Standard output must be equal byte for byte, the exit codes equal (both exit 1 on every
//! error, D47), and a failure must fall in the same class, read from standard error. `TZ` is
//! `UTC` on both sides: Go prints a file replica's times in the local zone, litestream-rs in
//! UTC.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use lsr_compat::{
    Differential, GoVersion, Manifest, Staged, all_fixtures, go_binary, lsr_binary, stage,
};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

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

fn answers(rig: &Rig, case: &Case) -> (Vec<String>, Ran, Ran) {
    let args = rig.line(&case.tokens);
    let go = run(&rig.go, &args, rig.dir.path());
    let lsr = run(&rig.lsr, &args, rig.dir.path());
    (args, go, lsr)
}

/// Compares one case and names how Go's run ended.
fn compare(rig: &Rig, case: &Case) -> Result<String, TestCaseError> {
    let (args, (go_code, go_err, go_out), (lsr_code, lsr_err, lsr_out)) = answers(rig, case);
    let with_dir = args.iter().any(|a| a.ends_with("with-dir.yml"));
    let ending = if go_code == Some(0) {
        let command = case.tokens.first().copied().unwrap_or_default();
        let config = if with_dir { "-with-dir" } else { "" };
        format!("{command}{config}-ok")
    } else {
        format!("{:?}", failure(&go_err, &go_out))
    };
    let report = format!(
        "args: {args:?}\nGo  ({go_code:?}): {go_err}{go_out}\nlsr ({lsr_code:?}): {lsr_err}{lsr_out}"
    );
    prop_assert_eq!(go_code, lsr_code, "exit codes\n{}", report);
    if go_code == Some(0) {
        // Help prints each side's own usage text; everything else must match exactly,
        // including the databases a `dir` entry finds (Go 3e0fdc0, docs/decisions/D274.md).
        if !go_out.contains("Usage") {
            prop_assert!(go_out == lsr_out, "standard output differs\n{report}");
        }
    } else {
        let classes = (failure(&go_err, &go_out), failure(&lsr_err, &lsr_out));
        prop_assert!(
            classes.0 == classes.1 && classes.0 != Failure::Other,
            "failure classes {classes:?}\n{report}"
        );
    }
    Ok(ending)
}

/// Endings the run must reach, each with a line built to reach it.
const AIMED: &[(&str, &[&str])] = &[
    ("databases-ok", &["databases", "-config", "@CONFIG@"]),
    (
        "databases-with-dir-ok",
        &["databases", "-config", "@DIRCONFIG@"],
    ),
    ("ltx-ok", &["ltx", "-config", "@CONFIG@", "@DB@"]),
    (
        "ltx-with-dir-ok",
        &["ltx", "-config", "@DIRCONFIG@", "@DB@"],
    ),
    ("wal-ok", &["wal", "@URL@"]),
    ("Usage", &["ltx", "-bogus", "@DB@"]),
    ("NoConfig", &["databases", "-config=@MISSING@"]),
    (
        "NotInConfig",
        &["ltx", "-config", "@CONFIG@", "@NOTINCONFIG@"],
    ),
];

/// Cases per aimed ending.
const AIMED_CASES: u32 = 4;

/// Cases for one aimed line: its tokens, with `-json` or not after the command.
fn aimed(tokens: &'static [&'static str]) -> impl Strategy<Value = Case> {
    any::<bool>().prop_map(move |json| {
        let mut tokens = tokens.to_vec();
        if json {
            tokens.insert(1, "-json");
        }
        Case { tokens }
    })
}

fn rig() -> Rig {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    let fixture = all_fixtures()
        .unwrap()
        .into_iter()
        .find(|f| f.ends_with("small-4096") && f.to_string_lossy().contains("3e0fdc0"))
        .expect("the 3e0fdc0 small-4096 fixture");
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
    // Two databases, one below a subdirectory: Go's walk order and relative replica paths.
    std::fs::create_dir_all(dir.path().join("scan/sub")).unwrap();
    for name in ["scan/b.db", "scan/a.db", "scan/sub/c.db"] {
        std::fs::write(dir.path().join(name), b"SQLite format 3\0").unwrap();
    }
    let with_dir = format!(
        "{config}  - dir: {}\n    pattern: \"*.db\"\n    recursive: true\n    replica:\n      url: file://{}\n",
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

/// The differential: 512 free lines and [`AIMED_CASES`] aimed at each ending in [`AIMED`],
/// shrunk to the shortest that disagrees.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_list_the_same_databases_and_files() {
    let rig = rig();
    let run = Differential::new("read-only commands");
    let mut parts = vec![run.free(512, &case(), |case| compare(&rig, &case))];
    for (ending, tokens) in AIMED {
        let cases = aimed(tokens);
        parts.push(run.aimed(ending, AIMED_CASES, &cases, |case| compare(&rig, &case)));
    }
    if let Some(Err(error)) = parts.into_iter().find(Result::is_err) {
        panic!("{error}");
    }
    let endings: Vec<&str> = AIMED.iter().map(|(ending, _)| *ending).collect();
    match run.require(&endings, 3) {
        Ok(tally) => eprintln!("Go ended as: {tally:?}"),
        Err(error) => panic!("{error}"),
    }
}
