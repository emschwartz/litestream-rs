//! Go's sidecar to Rust's sidecar and back: the same `litestream.yml`, the same meta
//! directories and the same replicas, each daemon started the way an operator starts it
//! (`replicate -config <file>`) and stopped with SIGINT, while the application keeps its own
//! connection open in this process (docs/plans/phase-4-sidecar.md, "Go sidecar to Rust
//! sidecar").
//!
//! - `go_config_and_meta_start_the_rust_sidecar_with_no_snapshot`: Go runs three databases from
//!   one file, stops; `lsr replicate -config` starts on the file unchanged and each database's
//!   next file is Go's tip plus one, incremental, with no snapshot cause logged; then Go starts
//!   again on what `lsr` left, the same way. The file carries Go keys v1 accepts only at their
//!   off value, Go tuning knobs v1 warns about, and the `litestream-rs` section Go does not
//!   define, so it shows both programs start on such a file.
//! - `go_and_rust_sidecars_alternate_on_one_config_file`: the alternation run of
//!   `alternation.rs` with Rust as the daemon on Go's config file.
//!
//! Metrics are not compared: the daemon does not serve `addr` yet (it warns that the listener
//! is not served), so there is no `/metrics` to scrape.

#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "test helpers; clippy.toml exempts only #[test] bodies"
)]

#[expect(
    dead_code,
    reason = "each test binary starts Rust one way, so the other way's variant is unused in it"
)]
mod alternate;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use alternate::{Launch, alternate, seed};
use lsr_compat::{
    Era, GoBuild, GoRestore, Kind, Replicator, Shape, Site, Summary, Target, builds_in,
    builds_under_test, hash_pages, integrity_check, lsr_binary, restore_replica, restore_with_go,
    snapshots, tip, wait_boot_file, wait_staged_past, wait_until, wait_uploaded,
};
use lsr_core::RestoreTarget;
use lsr_ltx::Txid;

const DATABASES: usize = 3;
const COMMITS: u32 = 6;
const UP: Duration = Duration::from_secs(30);

/// The daemon config both programs read. Beside what replicates, it holds keys Go knows and
/// v1 accepts only at their off value (`socket.enabled`, `mcp-addr`, `heartbeat-url`,
/// `auto-recover`), Go tuning knobs v1 accepts with a warning (`shutdown-sync-interval`,
/// `busy-timeout`), and the Rust-only section, which Go ignores. The application keeps
/// `wal_autocheckpoint = 0` (`Site`), which the section says.
fn write_sidecar_config(path: &Path, sites: &[Site]) {
    let mut text = String::from(
        "levels:
  - interval: 1s
  - interval: 2s
  - interval: 4s
snapshot:
  interval: 24h
  retention: 24h
socket:
  enabled: false
mcp-addr: \"\"
heartbeat-url: \"\"
shutdown-sync-interval: 500ms
litestream-rs:
  host-autocheckpoint: off
  responses:
    wal-unlinked: stop
dbs:
",
    );
    for site in sites {
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
            db = site.db.display(),
            replica = site.replica.display(),
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

fn pages_of(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

fn fresh(site: &Site, name: &str) -> PathBuf {
    let out = site.dir().join(name);
    for suffix in ["", "-wal", "-shm"] {
        drop(std::fs::remove_file(format!("{}{suffix}", out.display())));
    }
    out
}

fn rust_restore(site: &Site, target: RestoreTarget, name: &str) -> Vec<String> {
    let out = fresh(site, name);
    let restored = restore_replica(&site.replica, target, &out).expect("sink");
    assert!(
        restored.is_ok(),
        "{name}: Rust cannot restore: {restored:?}"
    );
    pages_of(&out)
}

fn go_restore(bin: &Path, config: &Path, site: &Site, target: Target, name: &str) -> Vec<String> {
    let out = fresh(site, name);
    let result = restore_with_go(bin, config, &site.db, &out, target).expect("go runs");
    assert_eq!(result, GoRestore::Restored, "{name}: Go restore");
    pages_of(&out)
}

/// One daemon's turn over every site: it comes up with every database, each database commits
/// [`COMMITS`] transactions, each staged before the next, and the replicas end holding them
/// all and a snapshot. Returns each database's boot file when it had a previous tip.
fn turn(
    process: &mut Replicator,
    sites: &mut [Site],
    previous: &[Option<Txid>],
) -> Vec<Option<Summary>> {
    process
        .wait_up_all(sites.len(), UP)
        .expect("the daemon comes up with every database");
    let mut at = previous.to_vec();
    let mut boots: Vec<Option<Summary>> = sites.iter().map(|_| None).collect();
    for i in 0..COMMITS {
        for (k, site) in sites.iter_mut().enumerate() {
            site.commit(Shape::Insert { bytes: 300 }).expect("commit");
            if let (0, Some(prev)) = (i, previous[k]) {
                boots[k] = Some(wait_boot_file(process, site, prev).expect("the boot's file"));
            }
            at[k] = Some(wait_staged_past(process, site, at[k]).expect("staged"));
        }
    }
    for site in sites.iter() {
        wait_uploaded(process, site).expect("uploaded");
        wait_until("a snapshot on the replica", UP, || {
            process.ensure_running()?;
            Ok(!snapshots(&site.replica)?.is_empty())
        })
        .expect("a snapshot exists");
    }
    boots
}

/// Every database's switch to `kind` continued: its first file is the next transaction and
/// holds only what the commits wrote.
fn assert_continued(name: &str, kind: Kind, boots: &[Option<Summary>], prev: &[Txid]) {
    for (k, (boot, prev)) in boots.iter().zip(prev).enumerate() {
        let boot = boot.as_ref().expect("a boot file after a previous tip");
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

/// What a stopped daemon left: each database's tip, pages and level-9 files.
struct Left {
    tips: Vec<Txid>,
    pages: Vec<Vec<String>>,
    l9: Vec<BTreeSet<String>>,
}

fn left(sites: &[Site]) -> Left {
    Left {
        tips: sites
            .iter()
            .map(|s| tip(s).expect("tip").expect("staged"))
            .collect(),
        pages: sites.iter().map(|s| s.hash_now().expect("hash")).collect(),
        l9: sites
            .iter()
            .map(|s| snapshots(&s.replica).expect("l9"))
            .collect(),
    }
}

fn assert_no_new_snapshot(name: &str, sites: &[Site], before: &Left) {
    for (k, site) in sites.iter().enumerate() {
        assert_eq!(
            snapshots(&site.replica).expect("l9"),
            before.l9[k],
            "{name}: database {k}: a new snapshot"
        );
    }
}

/// What Go said about the file's keys it does not define, if anything, for the report.
fn go_key_warnings(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|l| l.contains("unrecognized or duplicate key"))
        .collect()
}

fn site_log(sites: &[Site], name: &str) -> PathBuf {
    sites[0].dir().join(name)
}

/// Go, then `lsr`, then Go again, on one config file over three databases.
fn sidecar_handover(build: &GoBuild, lsr: &Path) {
    let name = build.name();
    let bin = build.binary().expect("the build under test is present");
    let mut sites: Vec<Site> = (0..DATABASES)
        .map(|k| {
            let site = Site::create(4096, 71 + u64::try_from(k).expect("small")).expect("site");
            site.prepare_for_processes(200).expect("prepare");
            site
        })
        .collect();
    let config = site_log(&sites, "litestream.yml");
    write_sidecar_config(&config, &sites);
    check_config(lsr, &config);

    let mut go = Replicator::start(&bin, &config, &site_log(&sites, "go-1.log")).expect("Go");
    turn(&mut go, &mut sites, &[None; DATABASES]);
    let go_log = go.log_text();
    go.stop().expect("Go stops");
    for warning in go_key_warnings(&go_log) {
        println!("{name}: Go warned: {warning}");
    }
    let at_go = left(&sites);

    let log = site_log(&sites, "lsr.log");
    let mut rs = Replicator::start_lsr_config(lsr, &config, &log).expect("lsr");
    let previous: Vec<Option<Txid>> = at_go.tips.iter().copied().map(Some).collect();
    let boots = turn(&mut rs, &mut sites, &previous);
    let rs_log = rs.log_text();
    rs.stop().expect("lsr drains and exits 0");
    assert_continued(name, Kind::Rust, &boots, &at_go.tips);
    assert_no_new_snapshot(name, &sites, &at_go);
    assert_eq!(
        rs_log.matches("replica position recovered").count(),
        DATABASES,
        "{name}: every database recovered its position: {rs_log}"
    );
    assert!(
        !rs_log.contains("snapshot required"),
        "{name}: lsr logged a snapshot cause: {rs_log}"
    );
    let at_rust = left(&sites);
    for (k, site) in sites.iter().enumerate() {
        let latest = rust_restore(site, RestoreTarget::Latest, "rs-1.db");
        assert_eq!(latest, at_rust.pages[k], "{name}: database {k}");
        let go_tip = rust_restore(site, RestoreTarget::Txid(at_go.tips[k]), "rs-go-tip.db");
        assert_eq!(go_tip, at_go.pages[k], "{name}: database {k} at Go's tip");
    }
    println!("{name}: lsr continued every database at Go's tip plus one");
    if build.era() == Era::Frames {
        println!("{name}: reads only frame-encoded files, so cannot resume from lsr's");
        return;
    }
    go_resumes(name, &bin, &config, &mut sites, &at_rust);
}

/// Go starts again on what `lsr` left.
fn go_resumes(name: &str, bin: &Path, config: &Path, sites: &mut [Site], at_rust: &Left) {
    let mut go = Replicator::start(bin, config, &site_log(sites, "go-2.log")).expect("Go");
    let previous: Vec<Option<Txid>> = at_rust.tips.iter().copied().map(Some).collect();
    let boots = turn(&mut go, sites, &previous);
    go.stop().expect("Go stops");
    assert_continued(name, Kind::Go, &boots, &at_rust.tips);
    assert_no_new_snapshot(name, sites, at_rust);
    for (k, site) in sites.iter().enumerate() {
        let now = site.hash_now().expect("hash");
        let latest = go_restore(bin, config, site, Target::Latest, "go-2.db");
        assert_eq!(latest, now, "{name}: database {k}: Go at Latest");
        let rust = rust_restore(site, RestoreTarget::Latest, "rs-2.db");
        assert_eq!(rust, now, "{name}: database {k}: Rust at Latest");
        let txid = at_rust.tips[k].get();
        let mid = go_restore(bin, config, site, Target::Txid { txid }, "go-rs-tip.db");
        assert_eq!(mid, at_rust.pages[k], "{name}: database {k} at lsr's tip");
    }
    println!("{name}: Go continued every database at lsr's tip plus one");
}

/// Go's `litestream replicate -config` runs three databases and stops; `lsr replicate
/// -config` starts on the same file and meta directories and each database continues at Go's
/// tip plus one with no snapshot; then Go continues from what `lsr` left. Restores by both
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
/// a state the database had.
#[test]
#[ignore = "runs the pinned Go binaries; just compat"]
fn go_and_rust_sidecars_alternate_on_one_config_file() {
    let builds = builds_in(Era::Blocks);
    if builds.is_empty() {
        println!("no block-era build under test: a frame-era Go cannot read Rust's files");
        return;
    }
    let lsr = lsr_binary().expect("lsr builds");
    let seed = seed();
    for build in &builds {
        alternate(build, &lsr, seed, Launch::ConfigFile);
    }
}
