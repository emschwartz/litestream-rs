//! Config parsing against Go: generated `litestream.yml` files, each read by Go's own
//! `ReadConfig` (yaml.v2, environment expansion, defaults, global settings copied down,
//! validation; through the dumper) and by `GoConfig::load`, then built by Go's
//! `NewDBFromConfig` and by `lsr_config::plan`. At each stage both sides must agree: the
//! same parsed values, or both refusing; and the same databases, or both refusing.
//!
//! The generator writes the keys litestream-rs implements, each value in the spellings
//! yaml.v2 reads differently from YAML 1.2 or from a human: durations bare, fractional,
//! negative, compound and malformed; YAML 1.1 booleans (`yes`, `on`, `y`); octal, hex,
//! underscored and signed integers; byte sizes in SI and IEC units; strings plain, quoted
//! and looking like other types; `~` and empty values; values through `$VAR` and `${VAR}`,
//! set and unset; anchors, aliases and merge keys; flow style; duplicate keys; and a
//! misspelled key now and then. A misspelled key is the one known departure: Go ignores
//! it, litestream-rs refuses the file (CFG-1).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use lsr_compat::{
    Differential, Dump, DumpEnv, Parsed, go_dump, go_dump_binary, go_parse, rust_dump, rust_parse,
};
use lsr_core::needs::{Resource, must};
use proptest::prelude::*;

/// Each kind's spellings: ones Go accepts for that type (drawn most of the time, so most
/// files parse and their values are compared), then odd ones (types yaml.v2 reads
/// differently, malformed or out of range).
const DURATIONS: (&[&str], &[&str]) = (
    &[
        "1s", "500ms", "1h30m", "1.5h", "0s", "2m3s4ms", "\"15s\"", "'1m'", "5m", "24h", "1us",
        "90s",
    ],
    &[
        "-5s", "0", "10", "1d", "300", "-1", "1e3", "abc", "", "~", "1S", "5 s", ".5s", "1µs",
    ],
);
const BOOLS: (&[&str], &[&str]) = (
    &[
        "true", "false", "yes", "no", "on", "off", "y", "n", "True", "FALSE", "Yes", "ON",
    ],
    &["\"true\"", "1", "0", "~", "maybe"],
);
const INTS: (&[&str], &[&str]) = (
    &["10", "0x1F", "010", "3", "64", "1_000", "+5", "2000"],
    &[
        "0",
        "-1",
        "0o10",
        "1e3",
        "\"10\"",
        "3.0",
        "~",
        "99999999999",
        "0b101",
        "ten",
    ],
);
const SIZES: (&[&str], &[&str]) = (
    &[
        "5MiB", "8MiB", "5242880", "10M", "1G", "16MB", "6MB", "1GiB",
    ],
    &[
        "5MB", "5mb", "\"5 MB\"", "0", "-1", "5KB", "abc", "1TiB", "5.5MB", "1EB",
    ],
);
/// Endpoints: a URL, a bare host, odd but parseable text; whitespace, which no URL holds,
/// is left out.
const ENDPOINTS: (&[&str], &[&str]) = (
    &[
        "http://localhost:9000",
        "localhost:9000",
        "https://x.example.com/",
        "http://[::1]:9000",
    ],
    &["plain", "''", "~", "HTTP://Upper:1"],
);
/// Regions, without `:`: with no endpoint the region forms the AWS host, which a `:` breaks
/// on both sides (Go at its first request, litestream-rs at start).
const REGIONS: (&[&str], &[&str]) = (
    &["us-east-1", "auto", "fr_par", "eu.west", "us-west-2"],
    &["a b", "''", "~", "yes", "\"tab\\there\""],
);
const STRINGS: (&[&str], &[&str]) = (
    &[
        "plain",
        "'single'",
        "\"double\"",
        "a b",
        "\":9090\"",
        "localhost:1",
    ],
    &["yes", "0123", "~", "\"\"", "\"tab\\there\"", "on", "12:30"],
);

/// A value spelled one of the ways above, or through a variable.
#[derive(Clone, Debug)]
enum Spelling {
    Literal(&'static str),
    /// `$NAME` with NAME set to the literal.
    Var(&'static str),
    /// `${NAME}` with NAME unset.
    Unset,
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Duration,
    Bool,
    Int,
    Size,
    Str,
    Region,
    Endpoint,
}

fn spelling(kind: Kind) -> impl Strategy<Value = Spelling> {
    let pool = match kind {
        Kind::Duration => DURATIONS,
        Kind::Bool => BOOLS,
        Kind::Int => INTS,
        Kind::Size => SIZES,
        Kind::Str => STRINGS,
        Kind::Region => REGIONS,
        Kind::Endpoint => ENDPOINTS,
    };
    let (valid, odd) = pool;
    prop_oneof![
        60 => proptest::sample::select(valid).prop_map(Spelling::Literal),
        6 => proptest::sample::select(valid).prop_map(Spelling::Var),
        3 => proptest::sample::select(odd).prop_map(Spelling::Literal),
        1 => proptest::sample::select(odd).prop_map(Spelling::Var),
        1 => Just(Spelling::Unset),
    ]
}

const GLOBAL_KEYS: &[(&str, Kind)] = &[
    ("addr", Kind::Str),
    ("l0-retention", Kind::Duration),
    ("l0-retention-check-interval", Kind::Duration),
    ("shutdown-sync-timeout", Kind::Duration),
    ("sync-interval", Kind::Duration),
    ("region", Kind::Region),
    ("endpoint", Kind::Endpoint),
    ("force-path-style", Kind::Bool),
    ("part-size", Kind::Size),
    ("concurrency", Kind::Int),
    ("access-key-id", Kind::Str),
    ("secret-access-key", Kind::Str),
];

const DB_KEYS: &[(&str, Kind)] = &[
    ("monitor-interval", Kind::Duration),
    ("checkpoint-interval", Kind::Duration),
    ("min-checkpoint-page-count", Kind::Int),
    ("truncate-page-n", Kind::Int),
    ("max-sync-wal-bytes", Kind::Int),
    ("restore-if-db-not-exists", Kind::Bool),
    ("meta-path", Kind::Str),
];

const REPLICA_KEYS: &[(&str, Kind)] = &[
    ("sync-interval", Kind::Duration),
    ("max-sync-ltx-files", Kind::Int),
    ("region", Kind::Region),
    ("endpoint", Kind::Endpoint),
    ("force-path-style", Kind::Bool),
    ("part-size", Kind::Size),
    ("concurrency", Kind::Int),
];

/// Snapshot settings core accepts (interval above the top level, retention at least the
/// interval), as interval and retention, either possibly absent.
const SNAPSHOTS: &[(Option<&str>, Option<&str>)] = &[
    (Some("24h"), Some("24h")),
    (Some("48h"), Some("72h")),
    (Some("2h"), None),
    (None, Some("168h")),
    (Some("90m"), Some("90m")),
];

/// A `snapshot` block: mostly one core accepts, sometimes any two durations.
fn snapshot() -> BoxedStrategy<Entries> {
    prop_oneof![
        12 => Just(Vec::new()),
        15 => proptest::sample::select(SNAPSHOTS).prop_map(|(interval, retention)| {
            let mut out = Vec::new();
            if let Some(interval) = interval {
                out.push(("interval", Spelling::Literal(interval)));
            }
            if let Some(retention) = retention {
                out.push(("retention", Spelling::Literal(retention)));
            }
            out
        }),
        1 => entries(&[("interval", Kind::Duration), ("retention", Kind::Duration)], 3),
    ]
    .boxed()
}

/// Level schedules core accepts (increasing, below the default snapshot interval).
const LEVELS: &[&[&str]] = &[
    &["30s", "5m", "1h"],
    &["1m", "10m"],
    &["15s"],
    &["10s", "1m", "5m", "30m"],
];

/// Keys Go ignores silently and litestream-rs refuses as unknown.
const MISSPELLED: &[&str] = &[
    "monitor_interval",
    "sync_interval",
    "retention-enable",
    "levelz",
];

type Entries = Vec<(&'static str, Spelling)>;

/// Up to `most` distinct keys, each with a spelling. (A key set twice is drawn separately,
/// by `Case::duplicate`, since litestream-rs refuses it on purpose.)
fn entries(keys: &'static [(&'static str, Kind)], most: usize) -> BoxedStrategy<Entries> {
    proptest::sample::subsequence(keys, 0..most.min(keys.len()))
        .prop_flat_map(|chosen| {
            chosen
                .into_iter()
                .map(|(key, kind)| spelling(kind).prop_map(move |value| (key, value)))
                .collect::<Vec<_>>()
        })
        .boxed()
}

#[derive(Clone, Debug)]
enum ReplicaShape {
    Url,
    TypeAndBucket,
    FilePath,
    /// The deprecated one-element `replicas` list.
    List,
    /// A replica block defined once with an anchor and used through a merge key.
    Merged,
}

#[derive(Clone, Debug)]
struct Db {
    entries: Entries,
    snapshot: Entries,
    replica: ReplicaShape,
    replica_entries: Entries,
    flow: bool,
}

#[derive(Clone, Debug)]
struct Case {
    global: Entries,
    levels: Vec<Spelling>,
    snapshot: Entries,
    retention: Option<Spelling>,
    dbs: Vec<Db>,
    duplicate: bool,
    misspelled: Option<&'static str>,
    document_marker: bool,
    /// Leave a lone `access-key-id` or `secret-access-key` alone (D42); else the other
    /// is added.
    lone_key: bool,
    /// The Rust-only section, which Go ignores: `app-readers: none`.
    rust_section: bool,
}

fn db() -> impl Strategy<Value = Db> {
    (
        entries(DB_KEYS, 4),
        prop_oneof![9 => Just(Vec::new()), 1 => snapshot()],
        prop_oneof![
            Just(ReplicaShape::Url),
            Just(ReplicaShape::TypeAndBucket),
            Just(ReplicaShape::FilePath),
            Just(ReplicaShape::List),
            Just(ReplicaShape::Merged),
        ],
        entries(REPLICA_KEYS, 4),
        prop::bool::weighted(0.1),
    )
        .prop_map(|(entries, snapshot, replica, replica_entries, flow)| Db {
            entries,
            snapshot,
            replica,
            replica_entries,
            flow,
        })
}

fn case() -> impl Strategy<Value = Case> {
    (
        entries(GLOBAL_KEYS, 5),
        prop_oneof![
            6 => Just(Vec::new()),
            3 => proptest::sample::select(LEVELS).prop_map(|levels| {
                levels.iter().map(|level| Spelling::Literal(level)).collect()
            }),
            1 => prop::collection::vec(spelling(Kind::Duration), 1..3),
        ],
        snapshot(),
        prop::option::weighted(0.2, spelling(Kind::Bool)),
        prop::collection::vec(db(), 1..3),
        prop::bool::weighted(0.03),
        prop::option::weighted(0.03, proptest::sample::select(MISSPELLED)),
        (
            prop::bool::weighted(0.2),
            prop::bool::weighted(0.8),
            prop::bool::weighted(0.1),
        ),
    )
        .prop_map(
            |(global, levels, snapshot, retention, dbs, duplicate, misspelled, flags)| {
                let (document_marker, rust_section, lone_key) = flags;
                Case {
                    global,
                    levels,
                    snapshot,
                    retention,
                    dbs,
                    duplicate,
                    misspelled,
                    document_marker,
                    rust_section,
                    lone_key,
                }
            },
        )
}

/// Renders values, recording the variables they need.
struct Writer {
    text: String,
    vars: BTreeMap<String, String>,
}

impl Writer {
    fn value(&mut self, value: &Spelling) -> String {
        match value {
            Spelling::Literal(text) => (*text).to_owned(),
            Spelling::Var(text) => {
                let name = format!("LSR_TEST_{}", self.vars.len());
                self.vars.insert(name.clone(), (*text).to_owned());
                format!("${name}")
            }
            Spelling::Unset => "${LSR_TEST_UNSET}".to_owned(),
        }
    }

    fn block(&mut self, indent: &str, entries: &Entries) {
        for (key, value) in entries {
            let value = self.value(value);
            writeln!(self.text, "{indent}{key}: {value}").unwrap();
        }
    }
}

/// Writes the missing half of the config's key pair, unless the case keeps it lone.
fn complete_the_key_pair(w: &mut Writer, global: &Entries, lone: bool) {
    let has = |name: &str| global.iter().any(|(key, _)| *key == name);
    let (id, secret) = (has("access-key-id"), has("secret-access-key"));
    if lone || id == secret {
        return;
    }
    let missing = if id {
        "secret-access-key"
    } else {
        "access-key-id"
    };
    writeln!(w.text, "{missing}: paired").unwrap();
}

fn render(case: &Case, base: &Path) -> Writer {
    let mut w = Writer {
        text: String::new(),
        vars: BTreeMap::new(),
    };
    if case.document_marker {
        w.text.push_str("---\n");
    }
    w.block("", &case.global);
    complete_the_key_pair(&mut w, &case.global, case.lone_key);
    if !case.levels.is_empty() {
        w.text.push_str("levels:\n");
        for level in &case.levels {
            let value = w.value(level);
            writeln!(w.text, "  - interval: {value}").unwrap();
        }
    }
    if !case.snapshot.is_empty() {
        w.text.push_str("snapshot:\n");
        w.block("  ", &case.snapshot);
    }
    if let Some(enabled) = &case.retention {
        let value = w.value(enabled);
        writeln!(w.text, "retention:\n  enabled: {value}").unwrap();
    }
    if case.rust_section {
        w.text.push_str("litestream-rs:\n  app-readers: none\n");
    }
    if let Some(key) = case.misspelled {
        writeln!(w.text, "{key}: 1s").unwrap();
    }
    w.text.push_str("dbs:\n");
    let anchored = case
        .dbs
        .iter()
        .any(|db| matches!(db.replica, ReplicaShape::Merged));
    for (index, db) in case.dbs.iter().enumerate() {
        render_db(&mut w, db, index, base, anchored);
    }
    if case.duplicate {
        w.text.push_str("addr: \":9999\"\n");
    }
    w
}

/// One `dbs` entry. With `anchored`, the first entry's replica carries the anchor `shared`
/// and a `Merged` entry merges it, then names its own bucket, so no two share a replica.
fn render_db(w: &mut Writer, db: &Db, index: usize, base: &Path, anchored: bool) {
    let path = base.join(format!("db{index}.sqlite"));
    if db.flow && !(anchored && index == 0) {
        writeln!(
            w.text,
            "  - {{path: {}, replica: {{url: \"file://{}/r{index}\"}}}}",
            path.display(),
            base.display()
        )
        .unwrap();
        return;
    }
    writeln!(w.text, "  - path: {}", path.display()).unwrap();
    w.block("    ", &db.entries);
    if !db.snapshot.is_empty() {
        w.text.push_str("    snapshot:\n");
        w.block("      ", &db.snapshot);
    }
    let indent = match db.replica {
        ReplicaShape::List => {
            w.text.push_str("    replicas:\n      - ");
            "        "
        }
        ReplicaShape::Url
        | ReplicaShape::TypeAndBucket
        | ReplicaShape::FilePath
        | ReplicaShape::Merged => {
            if anchored && index == 0 {
                w.text
                    .push_str("    replica: &shared\n      region: us-east-2\n      ");
            } else {
                w.text.push_str("    replica:\n      ");
            }
            "      "
        }
    };
    match db.replica {
        ReplicaShape::Url | ReplicaShape::List => {
            writeln!(w.text, "url: s3://bucket{index}/prefix").unwrap();
        }
        ReplicaShape::TypeAndBucket => {
            writeln!(
                w.text,
                "type: s3\n{indent}bucket: b{index}\n{indent}path: pre"
            )
            .unwrap();
        }
        ReplicaShape::FilePath => {
            writeln!(
                w.text,
                "type: file\n{indent}path: {}/r{index}",
                base.display()
            )
            .unwrap();
        }
        ReplicaShape::Merged if index == 0 => {
            writeln!(w.text, "url: s3://bucket{index}/prefix").unwrap();
        }
        ReplicaShape::Merged => {
            writeln!(
                w.text,
                "<<: *shared\n{indent}url: s3://bucket{index}/prefix"
            )
            .unwrap();
        }
    }
    w.block(indent, &db.replica_entries);
}

fn answers(go: &Path, case: &Case) -> (Parsed, Parsed, Dump, Dump, String) {
    let base = tempfile::tempdir().unwrap();
    let base = base.path().canonicalize().unwrap();
    let w = render(case, &base);
    let config = base.join("litestream.yml");
    std::fs::write(&config, &w.text).unwrap();
    let env = DumpEnv {
        vars: w.vars.clone(),
        home: base.join("home"),
        cwd: base.clone(),
        credentials: false,
    };
    let go_parsed = go_parse(go, &config, &env).unwrap();
    let rust_parsed = rust_parse(&config, &env);
    let go_built = go_dump(go, &config, &env).unwrap();
    let rust_built = rust_dump(&config, &env);
    let report = format!("vars: {:?}\nconfig:\n{}", w.vars, w.text);
    (go_parsed, rust_parsed, go_built, rust_built, report)
}

/// Go's parse with the values that mean the same as another made equal: a
/// `truncate-page-n` of 0 is Go's default (`effectiveTruncatePageN`, db.go:1604), as unset is.
fn normalized(parsed: Parsed) -> Parsed {
    let Parsed::Config(mut config) = parsed else {
        return parsed;
    };
    // A setting yaml.v2 left nil (`key:` with no value) keeps the store's default, since
    // `replicate` only sets what is non-nil (replicate.go:336-353, store.go:160-171).
    let defaults = [
        (&mut config.snapshot_interval, 86_400_000_000_000),
        (&mut config.snapshot_retention, 86_400_000_000_000),
        (&mut config.l0_retention, 300_000_000_000),
        (&mut config.l0_retention_check_interval, 15_000_000_000),
        (&mut config.shutdown_sync_timeout, 30_000_000_000),
    ];
    for (value, default) in defaults {
        value.get_or_insert(default);
    }
    for db in &mut config.dbs {
        if db.truncate_page_n == Some(0) {
            db.truncate_page_n = None;
        }
        // Go's uploader takes a concurrency or part size only above zero
        // (s3/replica_client.go:503-512), so zero or less is its default, as unset is.
        // (On R2 and Tigris Go's explicit zero gives the generic default, not the
        // provider's: upload parallelism only, noted in D45.)
        if let Some(replica) = &mut db.replica {
            for value in [&mut replica.concurrency, &mut replica.part_size] {
                if value.is_some_and(|n| n <= 0) {
                    *value = None;
                }
            }
        }
    }
    Parsed::Config(config)
}

/// Every leaf where the two values differ, by its path.
fn differing_lines(go: &impl serde::Serialize, rust: &impl serde::Serialize) -> String {
    fn walk(path: &str, go: &serde_json::Value, rust: &serde_json::Value, out: &mut Vec<String>) {
        use serde_json::Value;
        match (go, rust) {
            (Value::Object(a), Value::Object(b)) => {
                for (key, value) in a {
                    let other = b.get(key).unwrap_or(&Value::Null);
                    walk(&format!("{path}.{key}"), value, other, out);
                }
            }
            (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
                for (index, (x, y)) in a.iter().zip(b).enumerate() {
                    walk(&format!("{path}[{index}]"), x, y, out);
                }
            }
            (a, b) if a != b => out.push(format!("  {path}: Go {a}, Rust {b}")),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(
        "",
        &serde_json::to_value(go).unwrap(),
        &serde_json::to_value(rust).unwrap(),
        &mut out,
    );
    out.join("\n")
}

fn refused(dump: &Dump) -> bool {
    matches!(dump, Dump::Refused(_))
}

/// The refusals litestream-rs makes on purpose where Go starts, each a recorded
/// departure: the text a refusal must contain, and the decision.
const DEPARTURES: &[(&str, &str)] = &[
    // Core's schedule: levels increasing, snapshots above the top level, retention at
    // least the snapshot interval (D45).
    ("compaction and snapshot schedule", "D45"),
    // `monitor-interval` zero or less: Go never polls the database and says nothing (D45).
    ("would never poll the database", "D45"),
    // A signing region with whitespace, `/` or a control character (D45).
    ("bad region", "D45"),
    // A key set twice in one mapping: Go keeps the last and warns (D45, CFG-1).
    ("key set twice", "D45"),
    // A duration core cannot hold: below one millisecond, or negative (D45).
    ("is not a usable duration here", "D45"),
    // A part size under S3's 5 MiB minimum: Go's uploader refuses every upload (D45).
    ("is below the 5 MiB minimum", "D45"),
    // Two databases on one meta directory, one replica, or one file: Go starts both and
    // they corrupt each other's state or history (D46).
    ("shares meta directory", "D46"),
    ("replicates to the same place as", "D46"),
    ("is already replicated by", "D46"),
    // One of the two config keys (D42).
    ("must be given together", "D42"),
];

/// How often each departure excused a case, by its index in [`DEPARTURES`].
static EXCUSED: [AtomicUsize; 15] = [const { AtomicUsize::new(0) }; 15];

/// Whether litestream-rs refused this for a recorded departure only.
fn departure(text: &str) -> Option<&'static str> {
    let (index, (_, decision)) = DEPARTURES
        .iter()
        .enumerate()
        .find(|(_, (needle, _))| text.contains(needle))?;
    if let Some(counter) = EXCUSED.get(index) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    Some(decision)
}

/// How the cases were judged: values compared, both refused, a recorded departure, an
/// unknown key, a refusal at one stage matched at the other.
static TALLY: [AtomicUsize; 5] = [const { AtomicUsize::new(0) }; 5];
const COMPARED: usize = 0;
const BOTH_REFUSED: usize = 1;
const DEPARTED: usize = 2;
const UNKNOWN_KEY: usize = 3;
const STAGED: usize = 4;

fn count(outcome: usize) {
    if let Some(counter) = TALLY.get(outcome) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

fn compare(go: &Path, case: &Case) -> Result<(), TestCaseError> {
    let (go_parsed, rust_parsed, go_built, rust_built, report) = answers(go, case);
    let show = || {
        format!(
            "{report}\nGo:   {go_parsed:?}\n      {go_built:?}\nRust: {rust_parsed:?}\n      {rust_built:?}"
        )
    };
    if case.misspelled.is_some() {
        count(UNKNOWN_KEY);
        prop_assert!(
            matches!(rust_parsed, Parsed::Refused(_)),
            "an unknown key must be refused (CFG-1)\n{}",
            show()
        );
        return Ok(());
    }
    match (
        &normalized(go_parsed.clone()),
        &normalized(rust_parsed.clone()),
    ) {
        (Parsed::Config(go_config), Parsed::Config(rust_config)) => {
            prop_assert!(
                go_config == rust_config,
                "parsed values differ:\n{}\n{report}",
                differing_lines(go_config, rust_config)
            );
            if let Dump::Refused(text) = &rust_built
                && !refused(&go_built)
                && departure(text).is_some()
            {
                count(DEPARTED);
                return Ok(());
            }
            count(COMPARED);
            prop_assert_eq!(
                refused(&go_built),
                refused(&rust_built),
                "one side builds, the other refuses\n{}",
                show()
            );
        }
        (Parsed::Refused(_), Parsed::Refused(_)) => count(BOTH_REFUSED),
        // A refusal at load where the other side refuses at build is the same outcome.
        (Parsed::Config(_), Parsed::Refused(text)) => {
            count(if refused(&go_built) { STAGED } else { DEPARTED });
            prop_assert!(
                refused(&go_built) || departure(text).is_some(),
                "Rust refuses what Go starts\n{}",
                show()
            );
        }
        (Parsed::Refused(_), Parsed::Config(_)) => {
            count(STAGED);
            prop_assert!(
                refused(&rust_built),
                "Go refuses what Rust starts\n{}",
                show()
            );
        }
    }
    Ok(())
}

/// The differential: 2048 generated files per run. A disagreement prints the file, its
/// variables and both answers at both stages, shrunk.
#[test]
#[ignore = "runs the pinned Go dumper; just compat"]
fn go_and_rust_parse_the_same_config() {
    let go = must(Resource::GoBuilds, || {
        go_dump_binary().map_err(|e| e.to_string())
    });
    let run = Differential::new("config files");
    let compared = run.free(2048, &case(), |case| {
        compare(&go, &case).map(|()| "compared".to_owned())
    });
    if let Err(error) = compared {
        panic!("{error}");
    }
    let [compared, both, departed, unknown, staged] =
        TALLY.each_ref().map(|n| n.load(Ordering::Relaxed));
    println!(
        "compared {compared}, both refused {both}, departures {departed}, unknown keys {unknown}, refused at different stages {staged}"
    );
    for ((needle, _), excused) in DEPARTURES.iter().zip(&EXCUSED) {
        println!("  {needle}: {}", excused.load(Ordering::Relaxed));
    }
    // A run that mostly exits early has compared nothing: the generator, not Go, decided it.
    assert!(
        compared * 2 >= compared + both + departed + unknown + staged,
        "too few cases compared"
    );
}
