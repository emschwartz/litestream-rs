//! The Go half of the writing claim: a replica the Rust actor wrote from a live SQLite
//! database restores, with every Go build under test that can read Rust's block-encoded files,
//! to the pages the database had at that point, and Go's own compaction and retention can run
//! over it. Equality is on restored pages ([`hash_pages`]), never on LTX bytes.
//!
//! The Go tests run every build under test (`just compat`: the pinned five; `just compat-live`:
//! the names given, see [`builds_under_test`]). The smoke test at the top needs no Go.
//! Not covered: a database that crosses the lock page (needs a gigabyte of data per run).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lsr_compat::{
    AppFixture, Chain, GoBuild, GoRestore, PageEncoding, Replicator, Target, UP_LINE, Written,
    build_verify_helper, builds_in, builds_under_test, clock_ending_now, go_verify, hash_pages,
    integrity_check, list, newest_l0, restore_replica, restore_with_go, wait_until, write_config,
};
use lsr_core::{Event, Level, MaintainedEvent, PlanOutcome, RestoreError, RestoreTarget};
use lsr_ltx::{Timestamp, Txid};

const COMMITS: u32 = 300;
/// Longer than the longest monitor interval in the Go config (4s).
const MONITORS_RAN: Duration = Duration::from_secs(6);

/// Removes a database and the sidecars SQLite left beside it, so the path can be restored to again.
fn clear(path: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let file = PathBuf::from(format!("{}{suffix}", path.display()));
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => panic!("remove {}: {err}", file.display()),
        }
    }
}

/// Runs the chain writer for `commits` commits at `page_size` and drains it.
fn write_chain(commits: u32, page_size: u32, seed: u64) -> Written {
    let fixture = AppFixture::create(page_size, seed).expect("fixture");
    let mut chain =
        Chain::start(fixture, clock_ending_now(commits).expect("clock")).expect("start");
    chain.run(commits).expect("run");
    chain.finish().expect("finish")
}

fn restored_pages(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

/// Restores with Rust; `None` when the plan has no chain for the target.
fn rust_restore(
    replica: &Path,
    target: RestoreTarget,
    out: &Path,
) -> Option<Result<(), RestoreError>> {
    match restore_replica(replica, target, out).expect("sink") {
        Ok(_) => Some(Ok(())),
        Err(err @ RestoreError::NoPlan(_)) => Some(Err(err)),
        Err(other) => panic!("restore failed: {other:?}"),
    }
}

/// A chain's replica restores at every transaction the run recorded; without Go.
#[test]
fn rust_restores_its_own_chain_at_every_point() {
    let written = write_chain(25, 4096, 7);
    let out = written.fixture.dir().join("rs.db");
    let latest = written.points.last().expect("points");
    restore_replica(&written.fixture.replica, RestoreTarget::Latest, &out)
        .expect("sink")
        .expect("latest restores");
    assert_eq!(restored_pages(&out), latest.page_hashes);
    let mut checked = 0;
    for point in &written.points {
        clear(&out);
        if let Some(Ok(())) = rust_restore(
            &written.fixture.replica,
            RestoreTarget::Txid(point.txid),
            &out,
        ) {
            assert_eq!(
                restored_pages(&out),
                point.page_hashes,
                "txid {}",
                point.txid
            );
            checked += 1;
        }
    }
    assert!(checked > 0, "no recorded point could be restored");
}

fn go_config(written: &Written) -> PathBuf {
    let config = written.fixture.dir().join("go.yml");
    write_config(&config, &written.fixture.db, &written.fixture.replica).expect("config");
    config
}

fn go_restore(
    build: &GoBuild,
    written: &Written,
    config: &Path,
    name: &str,
    target: Target,
) -> (GoRestore, PathBuf) {
    let bin = build.binary().expect("the build under test is present");
    let out = written.fixture.dir().join(name);
    clear(&out);
    let result = restore_with_go(&bin, config, &written.fixture.db, &out, target).expect("go runs");
    (result, out)
}

/// Go's own decoder accepts every file the Rust run ever wrote, including the files that
/// retention later deleted (the run keeps a copy of each).
#[test]
#[ignore = "runs the pinned Go binary; just compat"]
fn go_verifies_every_rust_file() {
    let written = write_chain(COMMITS, 4096, 11);
    let tmp = tempfile::tempdir().expect("tmp");
    // A missing Go toolchain or a failed helper build fails the test: this one only runs under
    // `just compat`, which needs Go, and a skip there would pass with nothing verified.
    let helper = build_verify_helper(tmp.path()).expect("build the Go verify helper (needs Go)");
    assert!(
        written.archive.len() > 10,
        "only {} files",
        written.archive.len()
    );
    go_verify(&helper, &written.archive).expect("Go verifies every file");
}

/// The run reaches every shape the plan names: L1, L2, a snapshot, a Rust checkpoint of each
/// kind it chooses to run, and a shrunken database.
#[test]
#[ignore = "runs 300 commits; just compat"]
fn rust_chain_covers_every_shape() {
    let written = write_chain(COMMITS, 4096, 11);
    let files = list(&written.fixture.replica).expect("list").files;
    let levels: std::collections::BTreeSet<u8> =
        written.archive.iter().filter_map(|p| level_of(p)).collect();
    for level in [0, 1, 2, 9] {
        assert!(levels.contains(&level), "no level {level} file: {levels:?}");
    }
    assert!(!files.is_empty());
    let checkpoints = written
        .events
        .iter()
        .filter(|e| matches!(e, Event::Checkpointed { .. }))
        .count();
    assert!(checkpoints > 0, "the actor never checkpointed");
    let counts: Vec<usize> = written
        .points
        .windows(2)
        .map(|w| {
            w[1].page_hashes
                .len()
                .saturating_sub(w[0].page_hashes.len())
        })
        .collect();
    let shrank = written
        .points
        .windows(2)
        .any(|w| w[1].page_hashes.len() < w[0].page_hashes.len());
    assert!(shrank, "the database never shrank: {counts:?}");
    let compacted = written.events.iter().any(|e| {
        matches!(
            e,
            Event::Maintained {
                event: MaintainedEvent::Compacted { .. },
                ..
            }
        )
    });
    assert!(compacted, "no compaction event");
}

fn level_of(path: &Path) -> Option<u8> {
    path.file_name()?.to_str()?.split('-').next()?.parse().ok()
}

#[expect(
    clippy::disallowed_methods,
    reason = "measures a real Go process's uptime against its own intervals"
)]
fn compacts_and_prunes(build: &GoBuild) {
    let written = write_chain(COMMITS, 4096, 11);
    let bin = build.binary().expect("the build under test is present");
    let config = go_config(&written);
    let before = list(&written.fixture.replica).expect("list").files.len();
    let newest_l9 = |dir: &Path| -> Result<Option<u64>, lsr_compat::CompatError> {
        Ok(list(dir)?
            .files
            .iter()
            .filter(|f| f.key.level() == Level::SNAPSHOT)
            .map(|f| f.key.range().max().get())
            .max())
    };
    let rust_l9 = newest_l9(&written.fixture.replica).expect("l9");
    let position = newest_l0(&written.fixture.meta)
        .expect("l0")
        .map(|h| h.fields().txids.max());
    // Go's store snapshots only when its position (the newest L0, which it reads from the
    // meta dir) is past the newest snapshot. Whether the Rust run's last snapshot reached the
    // position depends on the run's timing, so the test decides what to wait for from what the
    // replica holds before Go starts, never from how long Go takes.
    let go_snapshots = position.is_some_and(|t| rust_l9.is_none_or(|l9| t.get() > l9));
    println!("Rust's newest L9 {rust_l9:?}, position {position:?}: Go snapshots {go_snapshots}");
    let go_log = written.fixture.dir().join("go.log");
    let mut go = Replicator::start(&bin, &config, &go_log).expect("start");
    // Either Go takes its own snapshot of the unchanged database, or the Rust snapshot is
    // already at the position and Go declines. A declining Go logs nothing at its default level
    // and may write nothing, so that branch waits for Go to say it is up and then keeps it
    // running past its longest monitor interval (4s in the config), so every monitor has run.
    let mut up_since: Option<Instant> = None;
    wait_until(
        if go_snapshots { "a Go snapshot over the Rust one" } else { "Go's monitors to run once" },
        Duration::from_secs(40),
        || {
            go.ensure_running()?;
            if go_snapshots {
                return Ok(newest_l9(&written.fixture.replica)? > rust_l9);
            }
            let up = std::fs::read_to_string(&go_log).is_ok_and(|log| log.contains(UP_LINE));
            if up && up_since.is_none() {
                up_since = Some(Instant::now());
            }
            Ok(up_since.is_some_and(|since| since.elapsed() >= MONITORS_RAN))
        },
    )
    .map_err(|e| {
        let position = newest_l0(&written.fixture.meta).ok().flatten().map(|h| h.fields().txids.max());
        let log = std::fs::read_to_string(written.fixture.dir().join("go.log")).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(25).collect();
        format!("{e:?}: Rust's newest L9 ends at {rust_l9:?}, Go's meta position {position:?}; Go log tail {tail:#?}")
    })
    .expect("Go replicates");
    go.stop().expect("stop");
    println!(
        "files before {before}, after {}",
        list(&written.fixture.replica).expect("list").files.len()
    );
    let want = &written.points.last().expect("points").page_hashes;
    let (result, out) = go_restore(build, &written, &config, "after.db", Target::Latest);
    assert_eq!(result, GoRestore::Restored);
    assert_eq!(&restored_pages(&out), want);
    let rs = written.fixture.dir().join("rs-after.db");
    restore_replica(&written.fixture.replica, RestoreTarget::Latest, &rs)
        .expect("sink")
        .expect("Rust restores after Go");
    assert_eq!(&restored_pages(&rs), want);
}

/// The tests that run the Go builds under test (`LSR_COMPAT_VERSIONS`).
/// `just compat-live` selects the tests by this module (`test(/(^|::)live::/)`).
mod live {
    use super::*;

    /// Every build in the block page encoding restores the latest state; the frame page encoding reads only frames
    /// and refuses, and the refusal is asserted rather than skipped.
    #[test]
    #[ignore = "runs the Go binaries; just compat"]
    fn go_restores_rust_chain_latest() {
        let written = write_chain(COMMITS, 4096, 11);
        let config = go_config(&written);
        let want = &written.points.last().expect("points").page_hashes;
        for build in builds_under_test() {
            let name = build.name();
            let (result, out) = go_restore(&build, &written, &config, "latest.db", Target::Latest);
            match build.page_encoding() {
                PageEncoding::Block => {
                    assert_eq!(result, GoRestore::Restored, "{name}");
                    assert_eq!(&restored_pages(&out), want, "{name}");
                }
                PageEncoding::Frame => {
                    println!("{name} on a Rust chain: {result:?}");
                    assert!(
                        matches!(result, GoRestore::Refused { .. }),
                        "{name} read a block file"
                    );
                }
            }
        }
    }

    /// Every transaction Rust can restore, Go restores to the same pages: the recorded points
    /// (their hashes are the source's) and the last transaction of every file the replica holds
    /// at the end (retention has removed most older points, so these are what is left to restore).
    #[test]
    #[ignore = "runs the pinned Go binary many times; just compat"]
    fn go_restores_rust_chain_each_txid() {
        let written = write_chain(COMMITS, 4096, 11);
        let config = go_config(&written);
        let mut targets: Vec<Txid> = written.points.iter().map(|p| p.txid).collect();
        targets.extend(
            list(&written.fixture.replica)
                .expect("list")
                .files
                .iter()
                .map(|f| f.key.range().max()),
        );
        targets.sort();
        targets.dedup();
        for build in builds_in(PageEncoding::Block) {
            let version = build.name();
            let mut checked = 0;
            for txid in &targets {
                let rs = written.fixture.dir().join("rs.db");
                clear(&rs);
                let Some(rust) =
                    rust_restore(&written.fixture.replica, RestoreTarget::Txid(*txid), &rs)
                else {
                    continue;
                };
                let (go, out) = go_restore(
                    &build,
                    &written,
                    &config,
                    "go.db",
                    Target::Txid { txid: txid.get() },
                );
                match rust {
                    Ok(()) => {
                        assert_eq!(go, GoRestore::Restored, "{version} txid {txid}");
                        let want = written
                            .points
                            .iter()
                            .find(|p| p.txid == *txid)
                            .map_or_else(|| restored_pages(&rs), |p| p.page_hashes.clone());
                        assert_eq!(restored_pages(&out), want, "{version} txid {txid}");
                        checked += 1;
                    }
                    Err(_) => assert!(
                        matches!(go, GoRestore::Refused { .. }),
                        "{version} restored txid {txid} that Rust has no chain for"
                    ),
                }
            }
            assert!(
                checked > 3,
                "{version} checked {checked} of {}",
                targets.len()
            );
        }
    }

    /// Each instant between commits: Rust and Go name the same state, or both refuse.
    #[test]
    #[ignore = "runs the pinned Go binary many times; just compat"]
    fn go_restores_rust_chain_as_of() {
        let written = write_chain(COMMITS, 4096, 11);
        let config = go_config(&written);
        let stride = (written.points.len() / 30).max(1);
        for build in builds_in(PageEncoding::Block) {
            for point in written.points.iter().step_by(stride) {
                let at = point.at_ms + 40;
                let rs = written.fixture.dir().join("rs.db");
                clear(&rs);
                let rust = restore_replica(
                    &written.fixture.replica,
                    RestoreTarget::AsOf(Timestamp::from_unix_millis(at)),
                    &rs,
                )
                .expect("sink");
                let (go, go_out) = go_restore(
                    &build,
                    &written,
                    &config,
                    "go.db",
                    Target::AsOf { unix_ms: at },
                );
                let label = format!("{} as of {at}", build.name());
                match (&rust, &go) {
                    (Ok(_), GoRestore::Restored) => {
                        assert_eq!(restored_pages(&go_out), restored_pages(&rs), "{label}");
                    }
                    (Err(RestoreError::NoPlan(no)), GoRestore::Restored)
                        if let PlanOutcome::AsOfInsideFile { chain, .. } = no.outcome() =>
                    {
                        let end = written.fixture.dir().join("end.db");
                        clear(&end);
                        restore_replica(
                            &written.fixture.replica,
                            RestoreTarget::Txid(chain.reaches()),
                            &end,
                        )
                        .expect("sink")
                        .expect("the chain's own end restores");
                        assert_eq!(restored_pages(&go_out), restored_pages(&end), "{label}");
                    }
                    (Err(RestoreError::NoPlan(no)), GoRestore::Refused { .. })
                        if matches!(no.outcome(), PlanOutcome::NoSnapshot { .. }) => {}
                    other => panic!("{label}: Rust and Go disagree: {other:?}"),
                }
            }
        }
    }

    /// Go's compaction and retention run over a Rust replica; it still restores to the source.
    #[test]
    #[ignore = "runs the pinned Go binary for several seconds; just compat"]
    fn go_compacts_and_prunes_a_rust_replica() {
        for build in builds_in(PageEncoding::Block) {
            compacts_and_prunes(&build);
        }
    }
}
