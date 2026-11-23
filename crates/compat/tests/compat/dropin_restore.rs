//! `restore`'s command line against Go: generated argument vectors given to the pinned
//! `litestream restore` and to `lsr restore`, each in a directory of its own laid out the
//! same way, against one fixture replica.
//!
//! The directory holds a config naming one database (`DB`) with the replica as a `file://`
//! URL, and the output path (`OUT`) and the database path each in one of four states:
//! absent, an empty file, a file that is not a database, or absent with a stale `-wal`
//! beside it. Tokens name these paths, another database (`OTHER`), the replica's URL
//! (`URL`), an empty replica's (`EMPTYURL`), a txid the replica holds (`TXID`), and are
//! drawn with `restore`'s flags in Go's spellings, bad values, `--`, help and an undefined
//! flag. `-f` is left out: Go's follow mode runs until killed, and `lsr` refuses it (#177).
//!
//! Three things must agree: the exit code (both exit 1 on every failure, D47); the class of
//! a failure, read from standard error; and the directory afterwards, file by file, a
//! database compared by its restored pages and anything else by its bytes. The last is the
//! oracle that matters: it holds whatever either side says it did.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use lsr_compat::{
    Differential, GoVersion, Manifest, Outcome, Staged, Target, all_fixtures, go_binary,
    hash_pages, lsr_binary, stage,
};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

/// A flag as one or two arguments, valid values weighted over bad ones; the `@WORD@`
/// sentinels are replaced (a bare word could occur inside a temporary path).
const FLAGS: &[(u32, &str)] = &[
    (4, "-o @OUT@"),
    (1, "-o @NESTED@"),
    (2, "-o=@OUT@"),
    (1, "--o @OUT@"),
    (2, "-config @CONFIG@"),
    (1, "-config=@MISSING@"),
    (3, "-txid @TXID@"),
    (1, "-txid 0000000000000000"),
    (1, "-txid ffffffffffffffff"),
    (1, "-txid zz"),
    (2, "-timestamp 2100-01-01T00:00:00Z"),
    (1, "-timestamp 2000-01-01T00:00:00Z"),
    (1, "-timestamp yesterday"),
    (3, "-if-db-not-exists"),
    (3, "-if-replica-exists"),
    (2, "-dry-run"),
    (3, "-force"),
    (1, "-force=false"),
    (1, "-parallelism 4"),
    (1, "-parallelism x"),
    (2, "-integrity-check quick"),
    (1, "-integrity-check bogus"),
    (1, "-no-expand-env"),
];

/// The positional arguments.
const SOURCES: &[(u32, &str)] = &[
    (4, "@DB@"),
    (4, "@URL@"),
    (2, "@EMPTYURL@"),
    (1, "@OTHER@"),
    (1, ""),
    (1, "@DB@ @URL@"),
    (1, "-- @DB@"),
];

/// An argument that breaks the line, inserted now and then.
const JUNK: &[&str] = &["-bogus", "-h", "-help", "--", "-txid"];

fn weighted(table: &'static [(u32, &'static str)]) -> impl Strategy<Value = &'static str> {
    let choices: Vec<_> = table.iter().map(|(w, t)| (*w, Just(*t).boxed())).collect();
    proptest::strategy::Union::new_weighted(choices)
}

/// What sits at a path before the run.
#[derive(Clone, Copy, Debug)]
enum Before {
    Absent,
    Empty,
    NotADatabase,
    StaleWal,
}

fn before() -> impl Strategy<Value = Before> {
    prop_oneof![
        3 => Just(Before::Absent),
        1 => Just(Before::Empty),
        1 => Just(Before::NotADatabase),
        1 => Just(Before::StaleWal),
    ]
}

/// How a run that did not succeed failed, read from standard error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    Usage,
    NoConfig,
    NotInConfig,
    Exists,
    NoBackups,
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
        "invalid -integrity-check",
        "invalid -timestamp",
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
    if ["already exists", "beside the output", "is a directory"]
        .iter()
        .any(|needle| stderr.contains(needle))
    {
        return Failure::Exists;
    }
    if [
        "no matching backup",
        "no exact restore",
        "holds no backup",
        "timestamp does not exist",
        "no backups found",
    ]
    .iter()
    .any(|needle| stderr.contains(needle))
    {
        return Failure::NoBackups;
    }
    if stdout.contains("Usage") || stderr.contains("Usage") {
        return Failure::Usage;
    }
    Failure::Other
}

/// The paths one side's directory holds.
struct Layout {
    dir: PathBuf,
    config: PathBuf,
}

impl Layout {
    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

const NOT_A_DATABASE: &[u8] = b"not a database";

fn put(path: &Path, before: Before) {
    match before {
        Before::Absent => {}
        Before::Empty => std::fs::write(path, b"").unwrap(),
        Before::NotADatabase => std::fs::write(path, NOT_A_DATABASE).unwrap(),
        Before::StaleWal => {
            let mut wal = path.as_os_str().to_owned();
            wal.push("-wal");
            std::fs::write(wal, b"stale").unwrap();
        }
    }
}

fn layout(dir: &Path, replica: &Path, out: Before, db: Before) -> Layout {
    let config = dir.join("litestream.yml");
    // An empty `dir` entry beside the database: Go's lookup of a DB_PATH reads only the
    // `path` entries, so it must not stop a restore.
    std::fs::create_dir_all(dir.join("scan")).unwrap();
    let text = format!(
        "dbs:\n  - path: {}\n    replica:\n      url: file://{}\n  - dir: {}\n    pattern: \"*.db\"\n    replica:\n      url: file://{}\n",
        dir.join("app.db").display(),
        replica.display(),
        dir.join("scan").display(),
        dir.join("scan-replica").display(),
    );
    std::fs::write(&config, text).unwrap();
    std::fs::create_dir_all(dir.join("empty-replica")).unwrap();
    put(&dir.join("out.db"), out);
    put(&dir.join("app.db"), db);
    Layout {
        dir: dir.to_path_buf(),
        config,
    }
}

fn line(tokens: &[&str], at: &Layout, replica: &Path, txid: &str) -> Vec<String> {
    let path = |name: &str| at.path(name).display().to_string();
    let mut out = vec!["restore".to_owned()];
    out.extend(tokens.iter().map(|token| {
        token
            .replace("@EMPTYURL@", &format!("file://{}", path("empty-replica")))
            .replace("@URL@", &format!("file://{}", replica.display()))
            .replace("@OUT@", &path("out.db"))
            .replace("@NESTED@", &path("new/dir/out.db"))
            .replace("@CONFIG@", &at.config.display().to_string())
            .replace("@MISSING@", &path("absent.yml"))
            .replace("@OTHER@", &path("other.db"))
            .replace("@TXID@", txid)
            .replace("@DB@", &path("app.db"))
    }));
    out
}

/// One side's exit code, standard error and standard output.
type Ran = (Option<i32>, String, String);

fn run(binary: &Path, args: &[String], dir: &Path) -> Ran {
    let output = Command::new(binary)
        .args(args)
        .env_clear()
        .env("HOME", dir)
        .env("PATH", "/usr/bin:/bin")
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

/// The directory after a run, by relative path, recursively: a database by its restored
/// pages, a directory as such, anything else by its bytes.
fn after(dir: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        let name = path.strip_prefix(root).unwrap().display().to_string();
        if name == "litestream.yml" || name == "empty-replica" || name == "scan" {
            continue;
        }
        // litestream-rs's restore also writes a takeover record in the restored database's
        // meta directory, so a replicator started on it builds on the replica it came from
        // (docs/decisions.md, the takeover record); Go writes none. It is not the database.
        let last = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if last.starts_with('.') && last.ends_with("-litestream") {
            continue;
        }
        if path.is_dir() {
            out.insert(name, "directory".to_owned());
            walk(root, &path, out);
            continue;
        }
        let bytes = std::fs::read(&path).unwrap_or_default();
        let shown = if bytes.starts_with(b"SQLite format 3\0") {
            hash_pages(&path).map_or_else(|e| format!("unreadable: {e}"), |p| format!("{p:?}"))
        } else {
            String::from_utf8_lossy(&bytes).into_owned()
        };
        out.insert(name, shown);
    }
}

#[derive(Clone, Debug)]
struct Case {
    tokens: Vec<&'static str>,
    out: Before,
    db: Before,
}

fn case() -> impl Strategy<Value = Case> {
    (
        prop::collection::vec(weighted(FLAGS), 0..4),
        weighted(SOURCES),
        prop::option::weighted(0.1, (proptest::sample::select(JUNK), 0..5_usize)),
        before(),
        before(),
    )
        .prop_map(|(flags, source, junk, out, db)| {
            let mut tokens: Vec<&'static str> = flags
                .iter()
                .chain(std::iter::once(&source))
                .flat_map(|part| part.split_whitespace())
                .collect();
            if let Some((word, at)) = junk {
                tokens.insert(at.min(tokens.len()), word);
            }
            Case { tokens, out, db }
        })
}

struct Rig {
    go: PathBuf,
    lsr: PathBuf,
    staged: Staged,
    txid: String,
    /// The replica's newest state, as the manifest records Go restoring it.
    latest: String,
}

/// What one side did: its run and its directory afterwards.
struct Side {
    ran: Ran,
    after: BTreeMap<String, String>,
}

/// The directory a case starts from, as [`after`] shows it.
fn starting(rig: &Rig, case: &Case) -> BTreeMap<String, String> {
    let dir = tempfile::tempdir().unwrap();
    layout(dir.path(), &rig.staged.replica(), case.out, case.db);
    after(dir.path())
}

/// Both sides' runs of one case, each in a directory of its own, and the line run.
fn answers(rig: &Rig, case: &Case) -> (Vec<String>, Side, Side) {
    let replica = rig.staged.replica();
    let side = |binary: &Path| {
        let dir = tempfile::tempdir().unwrap();
        let at = layout(dir.path(), &replica, case.out, case.db);
        let args = line(&case.tokens, &at, &replica, &rig.txid);
        let ran = run(binary, &args, dir.path());
        let after = after(dir.path());
        (args, Side { ran, after })
    };
    let (_, go) = side(&rig.go);
    let (args, lsr) = side(&rig.lsr);
    (args, go, lsr)
}

/// What a run ended as, for the tally.
fn ending(side: &Side) -> String {
    let (code, stderr, stdout) = &side.ran;
    if *code != Some(0) {
        return format!("{:?}", failure(stderr, stdout));
    }
    if side.after.values().any(|v| v.starts_with("PageDigest")) {
        "restored".to_owned()
    } else if stdout.contains("Restore plan") {
        "planned".to_owned()
    } else if stdout.contains("Usage") || stdout.contains("usage") {
        "help".to_owned()
    } else {
        "skipped".to_owned()
    }
}

/// Compares one case and names how Go's run ended.
fn compare(rig: &Rig, case: &Case) -> Result<String, TestCaseError> {
    let (args, go, lsr) = answers(rig, case);
    let ended = ending(&go);
    let ((go_code, go_err, go_out), (lsr_code, lsr_err, lsr_out)) = (&go.ran, &lsr.ran);
    let report = format!(
        "case: {case:?}\nargs: {args:?}\nGo  ({go_code:?}): {go_err}{go_out}\nlsr ({lsr_code:?}): {lsr_err}{lsr_out}"
    );
    // Go refuses a timestamp after the replica's newest file when it restores from a URL
    // (`CalcRestoreTarget`), deciding on listed modification times, and restores the newest
    // state for the same timestamp from a config. litestream-rs restores the newest state
    // both ways (docs/decisions.md D160), or meets the next check Go never reached: accepted
    // when it restored the newest state, or failed and left the directory as it was.
    if go_err.contains("timestamp does not exist") {
        let fine = if *lsr_code == Some(0) {
            lsr.after.values().any(|v| *v == rig.latest) || lsr_out.contains("Restore plan")
        } else {
            lsr.after == starting(rig, case)
        };
        prop_assert!(fine, "a timestamp past the newest file\n{report}");
        return Ok(ended);
    }
    // Go's -if-replica-exists exits 0 for any transaction it cannot find, a txid past the
    // newest file of a replica that has files included, and writes no database; a script
    // then starts its application on an empty one. litestream-rs skips only an empty
    // replica and refuses the rest (docs/decisions.md D160).
    if go_out.contains("no matching backups found") && *go_code == Some(0) && *lsr_code != Some(0) {
        prop_assert!(
            failure(lsr_err, lsr_out) == Failure::NoBackups && lsr.after == starting(rig, case),
            "-if-replica-exists over a replica with files\n{report}"
        );
        return Ok(ended);
    }
    prop_assert_eq!(go_code, lsr_code, "exit codes\n{}", report);
    if *go_code != Some(0) {
        let classes = (failure(go_err, go_out), failure(lsr_err, lsr_out));
        prop_assert!(
            classes.0 == classes.1 && classes.0 != Failure::Other,
            "failure classes {classes:?}\n{report}"
        );
    }
    // Go removes the output (an empty file, or with -force any file and its journal files)
    // before it restores, so a restore that then fails, or skips, has already removed them;
    // litestream-rs removes nothing unless it publishes (docs/decisions.md D160). When
    // litestream-rs left the directory as it was and Go only removed from it, both ended
    // the same way, by the checks above.
    let start = starting(rig, case);
    let go_only_removed = go.after.iter().all(|(name, v)| start.get(name) == Some(v));
    if lsr.after == start && go_only_removed {
        return Ok(ended);
    }
    prop_assert!(
        *go_code == Some(0),
        "after a failure litestream-rs must leave the directory as it was: start {start:?}\nGo:  {:?}\nlsr: {:?}\n{report}",
        go.after,
        lsr.after
    );
    prop_assert!(
        go.after == lsr.after,
        "the directories differ afterwards\nGo:  {:?}\nlsr: {:?}\n{report}",
        go.after,
        lsr.after
    );
    Ok(ended)
}

/// Endings the run must reach, each with a line built to reach it: the tokens, and what sits
/// at the output (`None`: any of the four states) before the run.
const AIMED: &[(&str, &[&str], Option<Before>)] = &[
    ("restored", &["-o", "@OUT@", "@URL@"], Some(Before::Absent)),
    ("planned", &["-dry-run", "-o", "@OUT@", "@URL@"], None),
    ("help", &["-h"], None),
    (
        "skipped",
        &["-if-replica-exists", "-o", "@OUT@", "@EMPTYURL@"],
        Some(Before::Absent),
    ),
    ("Usage", &["-bogus", "@DB@"], None),
    ("NoConfig", &["-config", "@MISSING@", "@DB@"], None),
    ("NotInConfig", &["-config", "@CONFIG@", "@OTHER@"], None),
    (
        "Exists",
        &["-o", "@OUT@", "@URL@"],
        Some(Before::NotADatabase),
    ),
    (
        "NoBackups",
        &["-o", "@OUT@", "@EMPTYURL@"],
        Some(Before::Absent),
    ),
];

/// Cases per aimed ending.
const AIMED_CASES: u32 = 6;

/// Cases for one aimed line: its tokens, the output as given or any, the database any.
fn aimed(tokens: &'static [&'static str], out: Option<Before>) -> impl Strategy<Value = Case> {
    let out = out.map_or_else(|| before().boxed(), |b| Just(b).boxed());
    (out, before()).prop_map(move |(out, db)| Case {
        tokens: tokens.to_vec(),
        out,
        db,
    })
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
    let txid = manifest
        .targets
        .iter()
        .find_map(|e| match (e.target, &e.outcome) {
            (Target::Txid { txid }, Outcome::Restored { .. }) => Some(format!("{txid:016x}")),
            _ => None,
        })
        .expect("a restorable txid target");
    let latest = manifest
        .targets
        .iter()
        .find_map(|e| match (e.target, &e.outcome) {
            (
                Target::Latest,
                Outcome::Restored {
                    page_size,
                    page_count,
                    page_hashes,
                },
            ) => Some(format!(
                "{:?}",
                lsr_compat::PageDigest {
                    page_size: *page_size,
                    page_count: *page_count,
                    page_hashes: page_hashes.clone(),
                }
            )),
            _ => None,
        })
        .expect("a restorable latest target");
    Rig {
        go,
        lsr: lsr_binary().unwrap(),
        staged: stage(&fixture, &manifest).unwrap(),
        txid,
        latest,
    }
}

/// The differential: 512 free cases and [`AIMED_CASES`] aimed at each ending in [`AIMED`],
/// shrunk to the shortest that disagrees.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_restore_the_same_command_lines() {
    let rig = rig();
    let run = Differential::new("restore command lines");
    let mut parts = vec![run.free(512, &case(), |case| compare(&rig, &case))];
    for (ending, tokens, out) in AIMED {
        let cases = aimed(tokens, *out);
        parts.push(run.aimed(ending, AIMED_CASES, &cases, |case| compare(&rig, &case)));
    }
    if let Some(Err(error)) = parts.into_iter().find(Result::is_err) {
        panic!("{error}");
    }
    let endings: Vec<&str> = AIMED.iter().map(|(ending, _, _)| *ending).collect();
    match run.require(&endings, 3) {
        Ok(tally) => eprintln!("Go ended as: {tally:?}"),
        Err(error) => panic!("{error}"),
    }
}
