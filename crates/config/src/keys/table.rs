//! The table of yml keys for library users (`docs/config-keys.md`), rendered from the key
//! tables in `keys.rs` and kept fresh by a test: a key whose class changes, a key added or
//! removed, or a missing "what it sets" line fails it. `LSR_CONFIG_KEYS_WRITE=1` rewrites the file.

use super::{
    AGE, DB, Key, LEVEL, LOGGING, OffValue, REPLICA, RESPONSES, RETENTION, RUST, Rule, SETTINGS,
    SNAPSHOT, SOCKET, TOP, VALIDATION,
};

/// One group of keys: the heading, the tag that names it in [`SETS`], and its table.
const GROUPS: &[(&str, &str, &[Key])] = &[
    ("Top level", "top", TOP),
    (
        "Replica settings (at the top level as the default for every replica, or inside a replica)",
        "settings",
        SETTINGS,
    ),
    (
        "`snapshot` (top level, or inside a `dbs` entry)",
        "snapshot",
        SNAPSHOT,
    ),
    ("`retention`", "retention", RETENTION),
    ("`socket`", "socket", SOCKET),
    ("`validation`", "validation", VALIDATION),
    ("`logging`", "logging", LOGGING),
    ("`levels` entries", "levels", LEVEL),
    ("`dbs` entries", "db", DB),
    (
        "`dbs[].replica` (or an entry of `replicas`)",
        "replica",
        REPLICA,
    ),
    ("`age`", "age", AGE),
    (
        "`litestream-rs` (read only by litestream-rs; Go ignores it)",
        "rust",
        RUST,
    ),
    ("`litestream-rs.responses`", "responses", RESPONSES),
];

const DAEMON: &str = "Daemon only; no field in a plan.";
const CONTAINER: &str = "A section; its keys are below.";

/// What each implemented key sets, as `tag.name`. The response keys are generated.
const SETS: &[(&str, &str)] = &[
    (
        "top.addr",
        "Daemon only: the address of `/metrics` and `/healthz`. No field in a plan.",
    ),
    (
        "top.levels",
        "`Config.maintenance`: the compaction levels, level 1 first.",
    ),
    ("top.snapshot", CONTAINER),
    ("top.validation", CONTAINER),
    (
        "top.l0-retention",
        "`Config.maintenance`: how long a covered level 0 file is kept before it is deleted; 0 keeps every one.",
    ),
    (
        "top.l0-retention-check-interval",
        "`Config.maintenance`: how often the level 0 retention pass runs (whole milliseconds, above 0).",
    ),
    ("top.retention", CONTAINER),
    (
        "top.dbs",
        "One entry of `Plan::dbs` per database; a `dir` entry gives one for each database it finds.",
    ),
    (
        "top.exec",
        "Daemon only: the command the daemon runs and supervises. No field in a plan.",
    ),
    ("top.logging", CONTAINER),
    (
        "top.shutdown-sync-timeout",
        "`Config.shutdown_grace` of every database, and `Plan::shutdown_sync_timeout`.",
    ),
    ("top.litestream-rs", CONTAINER),
    (
        "settings.sync-interval",
        "`Config.retry_interval`: the first wait after a failed upload.",
    ),
    (
        "settings.max-sync-ltx-files",
        "`Config.sync_limits.files_per_upload`; 0 or less means every staged file.",
    ),
    (
        "settings.access-key-id",
        "The store: `Credentials::FromConfig`, with `secret-access-key`. Without both, the AWS environment variables and shared files.",
    ),
    (
        "settings.secret-access-key",
        "The store: see `access-key-id`.",
    ),
    ("settings.region", "The store: the `S3Config` region."),
    ("settings.bucket", "The store: the `S3Config` bucket."),
    ("settings.endpoint", "The store: the `S3Config` endpoint."),
    (
        "settings.force-path-style",
        "The store: path-style or virtual-host-style addressing.",
    ),
    (
        "settings.part-size",
        "The store: the `S3Config` multipart part size.",
    ),
    (
        "settings.concurrency",
        "The store: how many multipart parts are in flight.",
    ),
    (
        "snapshot.interval",
        "`Config.maintenance`: how often a snapshot is written. A value inside a `dbs` entry is promoted to the one setting; every entry that sets it must agree.",
    ),
    (
        "snapshot.retention",
        "`Config.maintenance`: how long snapshots are kept. A value inside a `dbs` entry is promoted, as for `interval`.",
    ),
    (
        "retention.enabled",
        "`Config.maintenance`: `true` deletes old files from the replica, `false` keeps every file.",
    ),
    (
        "validation.interval",
        "`Config.verify_interval`: how often the scheduled verify runs; 0 or less never.",
    ),
    ("logging.level", DAEMON),
    ("logging.type", DAEMON),
    ("logging.stderr", DAEMON),
    (
        "levels.interval",
        "`Config.maintenance`: one level's interval.",
    ),
    ("db.path", "`DbPlan::path`: one database."),
    (
        "db.dir",
        "`DbPlan::path` of each database found in the directory.",
    ),
    ("db.pattern", "Which files in `dir` are databases."),
    (
        "db.recursive",
        "Whether `dir` is searched in subdirectories.",
    ),
    (
        "db.snapshot",
        "`Config.maintenance`: promoted to the top-level `snapshot`; see there.",
    ),
    (
        "db.meta-path",
        "`Config.meta_dir` of a `path` entry; the default is `.<name>-litestream` beside the database.",
    ),
    (
        "db.meta-dir",
        "`Config.meta_dir` of each database a `dir` entry finds.",
    ),
    (
        "db.monitor-interval",
        "`Config.poll_interval`: how often the WAL is polled, and how often a missing database file is looked for by the daemon.",
    ),
    (
        "db.checkpoint-interval",
        "`Config.checkpoint.interval`: how often a PASSIVE checkpoint runs on a quiet WAL; 0 never.",
    ),
    (
        "db.min-checkpoint-page-count",
        "`Config.checkpoint.min_pages`: WAL pages that trigger a PASSIVE checkpoint.",
    ),
    (
        "db.truncate-page-n",
        "`Config.checkpoint.truncate_pages`: WAL pages that trigger a TRUNCATE checkpoint. Takes effect only with `litestream-rs.app-readers: none`; 0 or less never truncates.",
    ),
    (
        "db.max-sync-wal-bytes",
        "`Config.sync_limits.wal_bytes_per_step`; 0 or less means no cap.",
    ),
    (
        "db.restore-if-db-not-exists",
        "`DbPlan::restore_if_missing`: only on a `path` entry. The caller restores; see `restore_if_missing` in `lsr-replica`.",
    ),
    (
        "db.replica",
        "`DbPlan::replica`: where the database replicates to.",
    ),
    (
        "db.replicas",
        "`DbPlan::replica`: a list of exactly one replica.",
    ),
    ("replica.type", "`DbPlan::replica`: `s3` or `file`."),
    (
        "replica.path",
        "`DbPlan::replica`: the directory of a `file` replica, or the prefix of an `s3` one.",
    ),
    (
        "replica.url",
        "`DbPlan::replica`: the whole replica as a URL (`s3://` or `file://`).",
    ),
    (
        "rust.app-readers",
        "`Config.readers`: `none` is `ReaderTracking::Tracked`, which lets TRUNCATE run.",
    ),
    ("rust.app-autocheckpoint", "`Config.app_autocheckpoint`."),
    ("rust.responses", CONTAINER),
    (
        "rust.if-replication-stops",
        "`Config.if_replication_stops`.",
    ),
    (
        "rust.max-unreplicated-age",
        "`Config.max_unreplicated_age`.",
    ),
];

fn sets(tag: &str, name: &str) -> Option<String> {
    if tag == "responses" {
        let field = name.replace('-', "_");
        return Some(format!("`Config.responses.{field}`."));
    }
    let id = format!("{tag}.{name}");
    SETS.iter()
        .find(|(known, _)| *known == id)
        .map(|(_, text)| (*text).to_owned())
}

fn off_value(at: OffValue) -> &'static str {
    match at {
        OffValue::False => "`false` or empty",
        OffValue::Empty => "empty",
        OffValue::EmptyList => "an empty list",
        OffValue::Any => "any value, since it matters only with a feature that is refused",
    }
}

fn row(tag: &str, key: &Key) -> Option<String> {
    let treatment = match key.rule {
        Rule::Implemented => sets(tag, key.name)?,
        Rule::OffOnly(at, feature) => format!(
            "Not supported ({feature}). Accepted at {}; any other value is refused.",
            off_value(at)
        ),
        Rule::NoEffect(why) => format!("No effect: {why}."),
    };
    Some(format!("| `{}` | {treatment} |\n", key.name))
}

/// The whole document, or the tag and key that have no "what it sets" line.
fn render() -> Result<String, String> {
    let mut out = String::from(
        "# Config keys for library users\n\n\
         Generated from `crates/config/src/keys.rs` by `crates/config/src/keys/table.rs`; \
         `LSR_CONFIG_KEYS_WRITE=1 cargo nextest run -p lsr-config -E 'test(=keys::table::the_key_table_is_fresh)'` \
         rewrites it, and the test fails when it is stale.\n\n\
         `lsr_config::load_plan` reads a `litestream.yml` and returns a `Plan`: per database a \
         `lsr_replica::Config`, a `ReplicaPlan` (open it with `lsr_config::open_store`), and the \
         restore-if-missing setting. The tables say what each key sets in it. \"Daemon only\" keys \
         are read by `lsr replicate` and are not in a plan.\n\n\
         `busy-timeout` has no effect, and the reason is worth a sentence: Go's `busy-timeout` is how \
         long Go's own SQLite connections wait on a lock, while litestream-rs's connections never wait \
         (a busy timeout of zero) and retry on their own backoff; the TRUNCATE wait it guarded in Go \
         is replaced by the reader-guard rule, which runs TRUNCATE only under \
         `litestream-rs.app-readers: none` (`ReaderTracking::Tracked`).\n",
    );
    for (heading, tag, keys) in GROUPS {
        out.push_str("\n## ");
        out.push_str(heading);
        out.push_str("\n\n| Key | What it does |\n| --- | --- |\n");
        for key in *keys {
            let line = row(tag, key).ok_or_else(|| format!("{tag}.{}", key.name))?;
            out.push_str(&line);
        }
    }
    Ok(out)
}

fn doc_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/config-keys.md")
}

#[test]
fn the_key_table_is_fresh() {
    let rendered = render().unwrap();
    if std::env::var_os("LSR_CONFIG_KEYS_WRITE").is_some() {
        std::fs::write(doc_path(), &rendered).unwrap();
    }
    let committed = std::fs::read_to_string(doc_path()).unwrap();
    assert_eq!(
        committed, rendered,
        "docs/config-keys.md is stale: run with LSR_CONFIG_KEYS_WRITE=1"
    );
}

#[test]
fn every_implemented_key_says_what_it_sets_and_every_line_names_a_key() {
    for (tag, name) in SETS.iter().map(|(id, _)| id.split_once('.').unwrap()) {
        let (_, _, keys) = GROUPS.iter().find(|(_, known, _)| *known == tag).unwrap();
        let key = keys.iter().find(|key| key.name == name);
        assert!(
            matches!(key.map(|k| k.rule), Some(Rule::Implemented)),
            "{tag}.{name} is not an implemented key"
        );
    }
    render().unwrap();
}

#[test]
fn the_groups_cover_every_section() {
    let covered: usize = GROUPS.iter().map(|(_, _, keys)| keys.len()).sum();
    let all: usize = [
        TOP, SETTINGS, SNAPSHOT, RETENTION, SOCKET, VALIDATION, LOGGING, LEVEL, DB, REPLICA, AGE,
        RUST, RESPONSES,
    ]
    .iter()
    .map(|keys| keys.len())
    .sum();
    assert_eq!(covered, all);
}
