//! Go's sidecar to Rust's sidecar and back: the same `litestream.yml`, the same meta
//! directories and the same replicas, each daemon started the way an operator starts it
//! (`replicate -config <file>`) and stopped with SIGINT, while the application keeps its own
//! connection open in this process.
//!
//! - `go_config_and_meta_start_the_rust_sidecar_with_no_snapshot`: Go runs three databases from
//!   one file, stops; `lsr replicate -config` starts on the file unchanged and each database's
//!   next file is Go's position plus one, incremental, with no snapshot cause logged; then Go starts
//!   again on what `lsr` left, the same way. The file carries Go keys litestream-rs accepts only at their
//!   off value, Go tuning knobs litestream-rs warns about, and the `litestream-rs` section Go does not
//!   define, so it shows both programs start on such a file.
//! - `go_and_rust_sidecars_alternate_on_one_config_file`: the alternation run of
//!   `alternation.rs` with Rust as the daemon on Go's config file, so both sides checkpoint
//!   on the file's schedule (every 5 s, PASSIVE at 1,000 pages, TRUNCATE at 1,500) and the run crosses checkpoints, WAL restarts and TRUNCATEs
//!   on both.
//!
//! Metrics are compared in `dropin_metrics`.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "test code: an overflow panic is a test failure, which is what a test wants"
)]

use crate::alternate;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use alternate::{Launch, alternate, seed};
use lsr_compat::{
    AppFixture, BEYOND_A_RUN, GoBuild, Kind, PageEncoding, Replicator, Shape, Summary, Target,
    builds_in, builds_under_test, go_restore, lsr_binary, position, rust_restore, snapshots,
    wait_boot_file, wait_staged_past, wait_until, wait_uploaded,
};
use lsr_core::RestoreTarget;
use lsr_ltx::Txid;

const DATABASES: usize = 3;
const COMMITS: u32 = 6;
const UP: Duration = Duration::from_secs(30);

/// The daemon config both programs read. Beside what replicates, it holds keys Go knows and
/// litestream-rs accepts only at their off value (`socket.enabled`, `mcp-addr`, `heartbeat-url`,
/// `auto-recover`), Go tuning knobs litestream-rs accepts with a warning (`shutdown-sync-interval`,
/// `busy-timeout`), and the Rust-only section, which Go ignores. The application keeps
/// `wal_autocheckpoint = 0` (`AppFixture`), which the section says.
fn write_sidecar_config(path: &Path, fixtures: &[AppFixture]) {
    // The snapshot window is one no run crosses: a day-long one starts at midnight UTC, where
    // a run found a scheduled snapshot and failed the no-new-snapshot check.
    let mut text = format!(
        "levels:
  - interval: 1s
  - interval: 2s
  - interval: 4s
snapshot:
  interval: {window}
  retention: {window}
socket:
  enabled: false
mcp-addr: \"\"
heartbeat-url: \"\"
shutdown-sync-interval: 500ms
litestream-rs:
  app-autocheckpoint: off
  responses:
    wal-unlinked: stop
dbs:
",
        window = BEYOND_A_RUN,
    );
    for fixture in fixtures {
        text.push_str(&format!(
            "  - path: {db}
    monitor-interval: 100ms
    checkpoint-interval: 1h
    busy-timeout: 1s
    replica:
      type: file
      path: {replica}
      sync-interval: 100ms
      auto-recover: false
",
            db = fixture.db.display(),
            replica = fixture.replica.display(),
        ));
    }
    std::fs::write(path, text).expect("write the config");
}

/// `lsr config check` accepts the file and warns about the tuning knobs it ignores.
fn check_config(lsr: &Path, config: &Path) {
    let out = Command::new(lsr)
        .arg("config")
        .arg("check")
        .arg(config)
        .output()
        .expect("lsr config check runs");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "lsr refuses the config: {text}");
    for knob in ["shutdown-sync-interval", "busy-timeout"] {
        assert!(text.contains(knob), "no warning for {knob}: {text}");
    }
}

/// One daemon's turn over every fixture: it comes up with every database, each database commits
/// [`COMMITS`] transactions, each staged before the next, and the replicas end holding them
/// all, and a snapshot when `snapshot` says the daemon writes one this soon. Returns each
/// database's boot file when it had a previous position.
fn turn(
    process: &mut Replicator,
    fixtures: &mut [AppFixture],
    previous: &[Option<Txid>],
    snapshot: bool,
) -> Vec<Option<Summary>> {
    process
        .wait_up_all(fixtures.len(), UP)
        .expect("the daemon comes up with every database");
    let mut at = previous.to_vec();
    let mut boots: Vec<Option<Summary>> = fixtures.iter().map(|_| None).collect();
    for i in 0..COMMITS {
        for (k, fixture) in fixtures.iter_mut().enumerate() {
            fixture
                .commit(Shape::Insert { bytes: 300 })
                .expect("commit");
            if let (0, Some(prev)) = (i, previous[k]) {
                boots[k] = Some(wait_boot_file(process, fixture, prev).expect("the boot's file"));
            }
            at[k] = Some(wait_staged_past(process, fixture, at[k]).expect("staged"));
        }
    }
    for fixture in fixtures.iter() {
        wait_uploaded(process, fixture).expect("uploaded");
        if !snapshot {
            continue;
        }
        wait_until("a snapshot on the replica", UP, || {
            process.ensure_running()?;
            Ok(!snapshots(&fixture.replica)?.is_empty())
        })
        .map_err(|err| process.explain(err))
        .expect("a snapshot exists");
    }
    boots
}

/// Every database's switch to `kind` continued: its first file is the next transaction and
/// holds only what the commits wrote.
fn assert_continued(name: &str, kind: Kind, boots: &[Option<Summary>], prev: &[Txid]) {
    for (k, (boot, prev)) in boots.iter().zip(prev).enumerate() {
        let boot = boot
            .as_ref()
            .expect("a boot file after a previous position");
        let fields = boot.header.fields();
        assert_eq!(
            fields.txids.min().get(),
            prev.get() + 1,
            "{name}: database {k}: {kind:?}'s first file"
        );
        assert!(
            !boot.header.is_snapshot() && !boot.holds_every_page(),
            "{name}: database {k}: {kind:?} snapshotted at boot: its first file holds {} of {} pages",
            boot.pages,
            fields.commit
        );
    }
}

/// What a stopped daemon left: each database's position, pages and level-9 files.
struct Left {
    positions: Vec<Txid>,
    pages: Vec<Vec<String>>,
    l9: Vec<Snapshots>,
}

/// A replica's level-9 files by key, with their bytes, so a file rewritten under the same
/// key differs as surely as a new key.
type Snapshots = BTreeMap<String, Vec<u8>>;

fn snapshot_bytes(fixture: &AppFixture) -> Snapshots {
    snapshots(&fixture.replica)
        .expect("l9")
        .into_iter()
        .map(|rel| {
            let bytes = fs::read(fixture.replica.join(&rel)).expect("read a snapshot");
            (rel, bytes)
        })
        .collect()
}

fn left(fixtures: &[AppFixture]) -> Left {
    Left {
        positions: fixtures
            .iter()
            .map(|s| position(s).expect("position").expect("staged"))
            .collect(),
        pages: fixtures
            .iter()
            .map(|s| s.hash_now().expect("hash"))
            .collect(),
        l9: fixtures.iter().map(snapshot_bytes).collect(),
    }
}

fn assert_no_new_snapshot(name: &str, fixtures: &[AppFixture], before: &Left) {
    for (k, fixture) in fixtures.iter().enumerate() {
        let now = snapshot_bytes(fixture);
        assert!(
            now == before.l9[k],
            "{name}: database {k}: snapshots {:?} after {:?}, or one rewritten",
            now.keys(),
            before.l9[k].keys()
        );
    }
}

/// A Go build before v0.5.4 may or may not have written its first snapshot in a run this
/// short: whether its try at start came after the database's first sync is Go's own timing.
/// Where Go left none, lsr may write its scheduled first one, as Go from v0.5.4 does at
/// start; that is not a snapshot forced by a lost position, which `assert_continued` and the
/// absence of a logged cause already rule out. Where Go left one, lsr writes none. Either
/// way every snapshot Go left stays as Go wrote it.
fn assert_at_most_a_first_snapshot(name: &str, fixtures: &[AppFixture], before: &Left) {
    for (k, fixture) in fixtures.iter().enumerate() {
        let now = snapshot_bytes(fixture);
        let kept = before.l9[k]
            .iter()
            .all(|(key, bytes)| now.get(key) == Some(bytes));
        let new = now
            .keys()
            .filter(|key| !before.l9[k].contains_key(*key))
            .count();
        let allowed = usize::from(before.l9[k].is_empty());
        assert!(
            kept && new <= allowed,
            "{name}: database {k}: snapshots {:?} after Go's {:?}: {new} new, at most {allowed}, \
             Go's kept as written: {kept}",
            now.keys(),
            before.l9[k].keys()
        );
    }
}

/// What Go said about the file's keys it does not define, if anything, for the report.
fn go_key_warnings(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|l| l.contains("unrecognized or duplicate key"))
        .collect()
}

fn fixture_log(fixtures: &[AppFixture], name: &str) -> PathBuf {
    fixtures[0].dir().join(name)
}

/// Go, then `lsr`, then Go again, on one config file over three databases.
fn sidecar_handover(build: &GoBuild, lsr: &Path) {
    let name = build.name();
    let bin = build.binary().expect("the build under test is present");
    let mut fixtures: Vec<AppFixture> = (0..DATABASES)
        .map(|k| {
            let fixture =
                AppFixture::create(4096, 71 + u64::try_from(k).expect("small")).expect("fixture");
            fixture.prepare_for_processes(200).expect("prepare");
            fixture
        })
        .collect();
    let config = fixture_log(&fixtures, "litestream.yml");
    write_sidecar_config(&config, &fixtures);
    check_config(lsr, &config);

    let mut go = Replicator::start(&bin, &config, &fixture_log(&fixtures, "go-1.log")).expect("Go");
    // Before v0.5.4 Go may write no snapshot in a run this short, so none is waited for, on
    // Go's turn or lsr's; lsr is then checked against whatever snapshots Go left, possibly
    // none, so a snapshot lsr wrote still fails the run.
    let snapshot = build.snapshots_soon_after_start();
    turn(&mut go, &mut fixtures, &[None; DATABASES], snapshot);
    let go_log = go.log_text();
    go.stop().expect("Go stops");
    for warning in go_key_warnings(&go_log) {
        println!("{name}: Go warned: {warning}");
    }
    let at_go = left(&fixtures);

    let log = fixture_log(&fixtures, "lsr.log");
    let mut rs = Replicator::start_lsr_config(lsr, &config, &log).expect("lsr");
    let previous: Vec<Option<Txid>> = at_go.positions.iter().copied().map(Some).collect();
    let boots = turn(&mut rs, &mut fixtures, &previous, snapshot);
    let rs_log = rs.log_text();
    rs.stop().expect("lsr drains and exits 0");
    assert_continued(name, Kind::Rust, &boots, &at_go.positions);
    if snapshot {
        assert_no_new_snapshot(name, &fixtures, &at_go);
    } else {
        assert_at_most_a_first_snapshot(name, &fixtures, &at_go);
    }
    assert_eq!(
        rs_log.matches("replica position recovered").count(),
        DATABASES,
        "{name}: every database recovered its position: {rs_log}"
    );
    assert!(
        !rs_log.contains("snapshot required"),
        "{name}: lsr logged a snapshot cause: {rs_log}"
    );
    let at_rust = left(&fixtures);
    for (k, fixture) in fixtures.iter().enumerate() {
        let latest = rust_restore(fixture, RestoreTarget::Latest, "rs-1.db");
        assert_eq!(latest, at_rust.pages[k], "{name}: database {k}");
        let go_newest = rust_restore(
            fixture,
            RestoreTarget::Txid(at_go.positions[k]),
            "rs-go-position.db",
        );
        assert_eq!(
            go_newest, at_go.pages[k],
            "{name}: database {k} at Go's position"
        );
    }
    println!("{name}: lsr continued every database at Go's position plus one");
    if build.page_encoding() == PageEncoding::Frame {
        println!("{name}: reads only frame-encoded files, so cannot resume from lsr's");
        return;
    }
    go_resumes(name, &bin, &config, &mut fixtures, &at_rust);
}

/// Go starts again on what `lsr` left.
fn go_resumes(name: &str, bin: &Path, config: &Path, fixtures: &mut [AppFixture], at_rust: &Left) {
    let mut go = Replicator::start(bin, config, &fixture_log(fixtures, "go-2.log")).expect("Go");
    let previous: Vec<Option<Txid>> = at_rust.positions.iter().copied().map(Some).collect();
    let boots = turn(&mut go, fixtures, &previous, true);
    go.stop().expect("Go stops");
    assert_continued(name, Kind::Go, &boots, &at_rust.positions);
    assert_no_new_snapshot(name, fixtures, at_rust);
    for (k, fixture) in fixtures.iter().enumerate() {
        let now = fixture.hash_now().expect("hash");
        let latest = go_restore(bin, config, fixture, Target::Latest, "go-2.db");
        assert_eq!(latest, now, "{name}: database {k}: Go at Latest");
        let rust = rust_restore(fixture, RestoreTarget::Latest, "rs-2.db");
        assert_eq!(rust, now, "{name}: database {k}: Rust at Latest");
        let txid = at_rust.positions[k].get();
        let mid = go_restore(
            bin,
            config,
            fixture,
            Target::Txid { txid },
            "go-rs-position.db",
        );
        assert_eq!(
            mid, at_rust.pages[k],
            "{name}: database {k} at lsr's position"
        );
    }
    println!("{name}: Go continued every database at lsr's position plus one");
}

/// The tests that run the Go builds under test (`LSR_COMPAT_VERSIONS`).
/// `just compat-live` selects the tests by this module (`test(/(^|::)live::/)`).
mod live {
    use super::*;

    /// Go's `litestream replicate -config` runs three databases and stops; `lsr replicate
    /// -config` starts on the same file and meta directories and each database continues at Go's
    /// position plus one with no snapshot; then Go continues from what `lsr` left. Restores by both
    /// match the databases at every handover point.
    #[test]
    #[ignore = "runs the pinned Go binaries; just compat"]
    fn go_config_and_meta_start_the_rust_sidecar_with_no_snapshot() {
        let lsr = lsr_binary().expect("lsr builds");
        for build in builds_under_test() {
            sidecar_handover(&build, &lsr);
        }
    }

    /// The alternation run with Rust as the daemon on Go's own config file: Go and `lsr
    /// replicate -config` take turns on one prefix, each turn ended by SIGKILL at a seeded point,
    /// every switch a plain restart with no snapshot, every acknowledged transaction restoring to
    /// a state the database had. A Go build without upstream bcd69ae may take a WAL tail SQLite
    /// never committed at its boot; that one outcome is accepted, asserted exactly and counted
    /// (#38, `alternate/uncommitted.rs`).
    #[test]
    #[ignore = "runs the pinned Go binaries; just compat"]
    fn go_and_rust_sidecars_alternate_on_one_config_file() {
        let builds = builds_in(PageEncoding::Block);
        if builds.is_empty() {
            println!(
                "no block-encoding build under test: a frame-encoding Go cannot read Rust's files"
            );
            return;
        }
        let lsr = lsr_binary().expect("lsr builds");
        let seed = seed();
        for build in &builds {
            alternate(build, &lsr, seed, Launch::ConfigFile);
        }
    }
}
