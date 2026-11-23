//! `/metrics` against Go: the pinned `litestream replicate` and `lsr replicate`, each on its
//! own database and file replica with `addr:` set and a checkpoint due after every write,
//! scraped once both have synced and checkpointed.
//!
//! Every `litestream_*` family Go serves must be served by `lsr` with the same type, help
//! text and label names, but for the families `lsr` does not build (docs/decisions.md D163);
//! every `litestream_*` family `lsr` serves must be one Go served or one Go serves only for
//! a replica type or an event this run does not produce (Go's file client reports no
//! replica operations, and no checkpoint fails here). Values are not compared: the two
//! daemons sync on their own schedules.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use lsr_compat::{GoVersion, go_binary, lsr_binary, wait_until};
use lsr_core::needs::{Resource, must};

/// Go's families `lsr` does not serve, and why (docs/decisions.md D163).
const NOT_BUILT: &[&str] = &[
    // Go counts per-object failures of an S3 batch delete by the error code in the response.
    "litestream_replica_operation_errors_total",
    // Go's per-level retention, verification and validation passes.
    "litestream_l0_retention_files_total",
    "litestream_compaction_verify_error_count",
    "litestream_validation_checks_total",
    "litestream_last_validation_success",
    "litestream_last_validation_success_timestamp_seconds",
];

/// `lsr`'s families Go serves only for what this run does not produce: Go's file replica
/// client reports no replica operations, and Go creates its checkpoint error counter at the
/// first error, where `lsr` serves zeros from the start.
const GO_SILENT_HERE: &[&str] = &[
    "litestream_replica_operation_total",
    "litestream_replica_operation_bytes",
    "litestream_replica_operation_duration_seconds",
    "litestream_checkpoint_error_count",
];

/// One family: its type, help text, and every label-name set its samples carry.
#[derive(Debug, PartialEq, Eq)]
struct Family {
    kind: String,
    help: String,
    labels: BTreeSet<Vec<String>>,
}

fn families(text: &str) -> BTreeMap<String, Family> {
    let mut map: BTreeMap<String, Family> = BTreeMap::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let (name, help) = rest.split_once(' ').unwrap_or((rest, ""));
            family(&mut map, name).help = help.to_owned();
        } else if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').unwrap_or((rest, ""));
            family(&mut map, name).kind = kind.to_owned();
        } else if !line.starts_with('#') && !line.is_empty() {
            let (head, _value) = line.rsplit_once(' ').unwrap_or((line, ""));
            let (name, labels) = match head.split_once('{') {
                Some((name, rest)) => (name, rest.trim_end_matches('}')),
                None => (head, ""),
            };
            let keys: Vec<String> = labels
                .split(',')
                .filter_map(|pair| pair.split_once('=').map(|(k, _)| k.to_owned()))
                .filter(|key| key != "le" && key != "quantile")
                .collect();
            let base = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|suffix| {
                    name.strip_suffix(suffix)
                        .filter(|base| map.get(*base).is_some_and(|f| f.kind == "histogram"))
                })
                .unwrap_or(name);
            family(&mut map, base).labels.insert(keys);
        }
    }
    map
}

fn family<'a>(map: &'a mut BTreeMap<String, Family>, name: &str) -> &'a mut Family {
    map.entry(name.to_owned()).or_insert_with(|| Family {
        kind: String::new(),
        help: String::new(),
        labels: BTreeSet::new(),
    })
}

fn scrape(port: u16) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: test\r\n\r\n")
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let (head, body) = response.split_once("\r\n\r\n")?;
    head.starts_with("HTTP/1.").then(|| body.to_owned())
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// One daemon on its own database: the child, its port, the application's connection and the
/// file its stdout and stderr go to (the daemon logs to one or the other by configuration).
struct Side {
    child: Child,
    port: u16,
    app: rusqlite::Connection,
    log: PathBuf,
    _dir: tempfile::TempDir,
}

impl Drop for Side {
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

fn start(binary: &Path) -> Side {
    let dir = tempfile::tempdir().unwrap();
    let db: PathBuf = dir.path().join("app.db");
    let app = rusqlite::Connection::open(&db).unwrap();
    app.pragma_update(None, "journal_mode", "wal").unwrap();
    app.execute_batch("CREATE TABLE t (x BLOB);").unwrap();
    let port = free_port();
    let config = format!(
        "addr: 127.0.0.1:{port}\ndbs:\n  - path: {}\n    checkpoint-interval: 1s\n    min-checkpoint-page-count: 1\n    replica:\n      url: file://{}\n",
        db.display(),
        dir.path().join("replica").display(),
    );
    let config_path = dir.path().join("litestream.yml");
    std::fs::write(&config_path, config).unwrap();
    let log = dir.path().join("daemon.log");
    let logfile = std::fs::File::create(&log).unwrap();
    let child = Command::new(binary)
        .args(["replicate", "-config"])
        .arg(&config_path)
        .env("HOME", dir.path())
        .current_dir(dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(logfile.try_clone().unwrap()))
        .stderr(Stdio::from(logfile))
        .spawn()
        .unwrap();
    Side {
        child,
        port,
        app,
        log,
        _dir: dir,
    }
}

/// Whether `text` shows a synced transaction and a finished PASSIVE checkpoint.
fn settled(text: &str) -> bool {
    let positive = |line: &str| {
        line.rsplit_once(' ')
            .and_then(|(_, v)| v.parse::<f64>().ok())
            .is_some_and(|v| v > 0.0)
    };
    let txid = text
        .lines()
        .any(|l| l.starts_with("litestream_txid{") && positive(l));
    let checkpoint = text.lines().any(|l| {
        l.starts_with("litestream_checkpoint_count{") && l.contains("\"PASSIVE\"") && positive(l)
    });
    txid && checkpoint
}

fn scraped_once_settled(side: &Side) -> String {
    let mut last = String::new();
    let mut row = 0_u32;
    wait_until(
        "a synced write and a checkpoint",
        Duration::from_secs(30),
        || {
            row = row.saturating_add(1);
            side.app
                .execute("INSERT INTO t VALUES (randomblob(3000))", [])
                .map_err(|e| lsr_compat::CompatError::Timeout(e.to_string()))?;
            last = scrape(side.port).unwrap_or_default();
            Ok(settled(&last))
        },
    )
    .unwrap_or_else(|e| {
        let log = std::fs::read_to_string(&side.log).unwrap_or_default();
        let from = log.len().saturating_sub(4000);
        let tail = log.get(from..).unwrap_or(&log);
        panic!("{e}\nlast scrape:\n{last}\ndaemon log tail:\n{tail}")
    });
    last
}

/// The differential.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_lsr_serve_the_same_metric_families() {
    let go = must(Resource::GoBuilds, || {
        go_binary(GoVersion::Head).map_err(|e| e.to_string())
    });
    let lsr = lsr_binary().unwrap();
    let go_text = scraped_once_settled(&start(&go));
    let lsr_text = scraped_once_settled(&start(&lsr));
    let ours = |name: &String| name.starts_with("litestream_");
    let go_families: BTreeMap<_, _> = families(&go_text)
        .into_iter()
        .filter(|(n, _)| ours(n))
        .collect();
    let lsr_families: BTreeMap<_, _> = families(&lsr_text)
        .into_iter()
        .filter(|(n, _)| ours(n))
        .collect();
    assert!(
        go_families.contains_key("litestream_checkpoint_count"),
        "Go checkpointed: {go_text}"
    );
    for (name, go_family) in &go_families {
        if NOT_BUILT.contains(&name.as_str()) {
            assert!(
                !lsr_families.contains_key(name),
                "{name} is built now: drop it from NOT_BUILT"
            );
            continue;
        }
        let Some(lsr_family) = lsr_families.get(name) else {
            panic!("lsr does not serve {name}:\n{lsr_text}");
        };
        assert_eq!(
            (&lsr_family.kind, &lsr_family.help),
            (&go_family.kind, &go_family.help),
            "{name}"
        );
        assert!(
            go_family.labels.is_subset(&lsr_family.labels) && lsr_family.labels.len() == 1,
            "{name}: Go's labels {:?}, lsr's {:?}",
            go_family.labels,
            lsr_family.labels
        );
    }
    for name in lsr_families.keys() {
        assert!(
            go_families.contains_key(name) || GO_SILENT_HERE.contains(&name.as_str()),
            "lsr serves {name}, which Go did not:\n{go_text}"
        );
    }
}
