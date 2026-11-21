//! Go and Rust on one S3 bucket: a local MinIO, with Go's AWS SDK on one side and Rust's
//! S3 store (the rusty-s3 signer over ureq) on the other. Every other compat check uses a
//! directory replica; these run the same harness over S3 ([`AppFixture::create_on_s3`]), so they
//! also cover request signing, key encoding (the run prefix holds a space, `+` and `=`),
//! listing across pages (the harness lists two files a page), multipart uploads (a database
//! of about 8 MB makes every writer's boot snapshot larger than the 5 MiB part size) and the
//! created-at times a point-in-time restore reads (Rust's and Go's `litestream-timestamp` metadata, or
//! the object's `LastModified` for Go before v0.5.16).
//!
//! `just compat-s3` starts MinIO, sets `LSR_COMPAT_S3_*` and runs these; unset, they skip.
//! They never reach a real store: the endpoint must be loopback `http`, credentials come from
//! those variables only, and every Go and `lsr` process runs without the machine's `AWS_*` and
//! `LITESTREAM_*` variables (`crates/compat/src/s3.rs`).
//!
//! Not here: the alternation run (`alternation.rs`). Its kill points watch the directory
//! replica's scratch files to land a kill mid-upload or mid-compaction, which S3 does not show
//! (an upload in flight is invisible until it completes), so running it over S3 would need
//! new kill points, not a new fixture.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_compat::{
    AppFixture, ENDPOINT_VAR, Era, GoBuild, GoRestore, Kind, MinioEnv, Remote, Replicator,
    S3Replica, Schedule, Shape, Summary, Target, builds_in, builds_under_test, hash_pages,
    integrity_check, lsr_binary, real_now_ms, restore_with_go, tip, uploaded, wait_boot_file,
    wait_staged_past, wait_until, wait_uploaded, write_config_for,
};
use lsr_core::RestoreTarget;
use lsr_ltx::{Timestamp, Txid};

const UP: Duration = Duration::from_secs(30);
/// Rows of 3,000 bytes the restore runs prepare: a database of about 8 MB.
const BIG_ROWS: u32 = 2000;
/// S3's minimum part and both writers' default: a larger object went up in parts.
const PART: u64 = 5 * 1024 * 1024;
/// Commits per handover turn, each staged before the next.
const COMMITS: u32 = 8;

fn minio() -> Option<MinioEnv> {
    match MinioEnv::from_env() {
        Ok(Some(env)) => Some(env),
        Ok(None) => {
            println!("skipped: {ENDPOINT_VAR} is unset (just compat-s3)");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

/// A prefix unique to the run, holding characters a key must encode.
fn fixture(env: &MinioEnv, test: &str, seed: u64) -> AppFixture {
    let now = real_now_ms().expect("clock");
    let prefix = format!("compat s3+{test}={now}-{}", std::process::id());
    let s3 = S3Replica::new(env, &prefix).expect("an S3 replica");
    AppFixture::create_on_s3(4096, seed, s3).expect("fixture")
}

fn s3_of(fixture: &AppFixture) -> &S3Replica {
    match fixture.remote() {
        Remote::S3(s3) => s3,
        Remote::Dir(dir) => panic!("{} is not an S3 fixture", dir.display()),
    }
}

fn snapshots(fixture: &AppFixture) -> BTreeSet<String> {
    fixture.remote().snapshots().expect("list the snapshots")
}

fn pages_of(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

fn fresh(fixture: &AppFixture, name: &str) -> PathBuf {
    let out = fixture.dir().join(name);
    for suffix in ["", "-wal", "-shm"] {
        drop(std::fs::remove_file(format!("{}{suffix}", out.display())));
    }
    out
}

fn config(fixture: &AppFixture, name: &str, schedule: Schedule<'_>) -> PathBuf {
    let path = fixture.dir().join(name);
    write_config_for(&path, &fixture.db, fixture.remote(), schedule).expect("config");
    path
}

/// Starts Go's `litestream replicate -config`, or Rust's `lsr replicate -config` on the
/// same file (both read the S3 section, credentials included, from it).
fn start(kind: Kind, bin: &Path, config: &Path, log: &Path) -> Replicator {
    match kind {
        Kind::Go => Replicator::start(bin, config, log),
        Kind::Rust => Replicator::start_lsr_config(bin, config, log),
    }
    .expect("the process starts")
}

/// A state the replica must restore to: the newest uploaded transaction, the instant just
/// after it was uploaded, and the database's pages then.
struct Mark {
    txid: Txid,
    at_ms: i64,
    pages: Vec<String>,
}

/// The writer comes up and uploads the database; the application commits three rounds of three
/// transactions, each staged before the next, and each round is uploaded before its mark
/// is taken. The next round starts only once the clock has passed the mark, so every file
/// of a later round is recorded as created after it. Stops the writer with SIGINT.
fn write_rounds(mut process: Replicator, fixture: &mut AppFixture) -> Vec<Mark> {
    let kind = process.kind();
    process.wait_up(UP).expect("the writer comes up");
    // The boot's first file holds every page (a level-0 file from transaction 1, or a
    // snapshot), whichever a version writes first.
    wait_until("the writer's first upload", UP, || {
        process.ensure_running()?;
        Ok(uploaded(fixture)?.is_some())
    })
    .expect("a first upload");
    let mut at = tip(fixture).expect("tip");
    let mut marks = Vec::new();
    for _ in 0..3 {
        for _ in 0..3 {
            fixture
                .commit(Shape::Insert { bytes: 900 })
                .expect("commit");
            at = Some(wait_staged_past(&mut process, fixture, at).expect("staged"));
        }
        let txid = wait_uploaded(&mut process, fixture).expect("uploaded");
        let pages = fixture.hash_now().expect("hash");
        let at_ms = real_now_ms().expect("clock");
        wait_until("the clock to pass the mark", UP, || {
            Ok(real_now_ms()? > at_ms)
        })
        .expect("the clock moves");
        marks.push(Mark { txid, at_ms, pages });
    }
    let stopped = process.stop();
    assert!(stopped.is_ok(), "{kind:?} does not stop: {stopped:?}");
    let largest = s3_of(fixture).largest().expect("list");
    assert!(
        largest > PART,
        "{kind:?}: no object went past the {PART}-byte part size (largest {largest})"
    );
    marks
}

/// The three targets a reader restores, with the pages each must give.
fn targets(marks: &[Mark]) -> [(&'static str, RestoreTarget, Target, &[String]); 3] {
    let [first, middle, last] = marks else {
        panic!("three rounds, three marks: {}", marks.len());
    };
    [
        ("latest", RestoreTarget::Latest, Target::Latest, &last.pages),
        (
            "txid",
            RestoreTarget::Txid(first.txid),
            Target::Txid {
                txid: first.txid.get(),
            },
            &first.pages,
        ),
        (
            "timestamp",
            RestoreTarget::AsOf(Timestamp::from_unix_millis(middle.at_ms)),
            Target::AsOf {
                unix_ms: middle.at_ms,
            },
            &middle.pages,
        ),
    ]
}

fn rust_reads(fixture: &AppFixture, marks: &[Mark], label: &str) {
    for (name, target, _, want) in targets(marks) {
        let out = fresh(fixture, &format!("rs-{name}.db"));
        let restored = fixture.remote().restore(target, &out).expect("sink");
        assert!(
            restored.is_ok(),
            "{label}: Rust cannot restore {name}: {restored:?}"
        );
        assert_eq!(pages_of(&out), want, "{label}: Rust at {name}");
    }
}

fn go_reads(build: &GoBuild, config: &Path, fixture: &AppFixture, marks: &[Mark], label: &str) {
    let bin = build.binary().expect("the build under test is present");
    for (name, _, target, want) in targets(marks) {
        let out = fresh(fixture, &format!("go-{}-{name}.db", build.name()));
        let result = restore_with_go(&bin, config, &fixture.db, &out, target).expect("go runs");
        assert_eq!(
            result,
            GoRestore::Restored,
            "{label}: {} at {name}",
            build.name()
        );
        assert_eq!(pages_of(&out), want, "{label}: {} at {name}", build.name());
    }
}

/// Every build under test writes a replica to MinIO from a live database (its boot
/// snapshot a multipart upload); Rust restores it at Latest, at a txid and at an instant
/// between two uploads, to the pages the database had, and so does that Go build.
#[test]
#[ignore = "needs MinIO and the pinned Go binaries; just compat-s3"]
fn over_s3_rust_restores_go_replica() {
    let Some(env) = minio() else { return };
    for build in builds_under_test() {
        let label = format!("{} wrote", build.name());
        let bin = build.binary().expect("the build under test is present");
        let mut fixture = fixture(&env, "go-writes", 61);
        fixture.prepare_for_processes(BIG_ROWS).expect("prepare");
        let config = config(&fixture, "go.yml", Schedule::LEVEL_ZERO_ONLY);
        let go = start(Kind::Go, &bin, &config, &fixture.dir().join("go.log"));
        let marks = write_rounds(go, &mut fixture);
        rust_reads(&fixture, &marks, &label);
        go_reads(&build, &config, &fixture, &marks, &label);
        s3_of(&fixture).delete_all().expect("clean up");
    }
}

/// `lsr replicate` writes a replica to MinIO from a live database (its boot snapshot a
/// multipart upload); every block-era build under test restores it at Latest, at a txid
/// and at an instant between two uploads, as Rust does. A frame-era build refuses it.
#[test]
#[ignore = "needs MinIO and the pinned Go binaries; just compat-s3"]
fn over_s3_go_restores_rust_replica() {
    let Some(env) = minio() else { return };
    let lsr = lsr_binary().expect("lsr builds");
    let mut fixture = fixture(&env, "rust-writes", 62);
    fixture.prepare_for_processes(BIG_ROWS).expect("prepare");
    let config = config(&fixture, "lsr.yml", Schedule::LEVEL_ZERO_ONLY);
    let rust = start(Kind::Rust, &lsr, &config, &fixture.dir().join("lsr.log"));
    let marks = write_rounds(rust, &mut fixture);
    rust_reads(&fixture, &marks, "lsr wrote");
    for build in builds_in(Era::Blocks) {
        go_reads(&build, &config, &fixture, &marks, "lsr wrote");
    }
    for build in builds_in(Era::Frames) {
        let bin = build.binary().expect("the build under test is present");
        let out = fresh(&fixture, &format!("go-{}-refused.db", build.name()));
        let result =
            restore_with_go(&bin, &config, &fixture.db, &out, Target::Latest).expect("go runs");
        assert!(
            matches!(result, GoRestore::Refused { .. }),
            "{} read a block-encoded replica: {result:?}",
            build.name()
        );
    }
    s3_of(&fixture).delete_all().expect("clean up");
}

/// One process's turn on an S3 fixture: it comes up on what the previous one left, the
/// application commits [`COMMITS`] transactions, each staged before the next, and the
/// process stops with SIGINT once MinIO holds them all and a snapshot. Returns the boot's
/// first file when there was a previous tip.
fn turn(
    mut process: Replicator,
    fixture: &mut AppFixture,
    previous: Option<Txid>,
) -> Option<Summary> {
    let kind = process.kind();
    process.wait_up(UP).expect("the process comes up");
    let mut at = previous;
    let mut boot = None;
    for i in 0..COMMITS {
        fixture
            .commit(Shape::Insert { bytes: 300 })
            .expect("commit");
        if let (0, Some(prev)) = (i, previous) {
            boot = Some(wait_boot_file(&mut process, fixture, prev).expect("the boot's file"));
        }
        at = Some(wait_staged_past(&mut process, fixture, at).expect("staged"));
    }
    wait_uploaded(&mut process, fixture).expect("uploaded");
    wait_until("a snapshot on S3", UP, || {
        process.ensure_running()?;
        Ok(!fixture.remote().snapshots()?.is_empty())
    })
    .expect("a snapshot exists");
    let stopped = process.stop();
    assert!(stopped.is_ok(), "{kind:?} does not stop: {stopped:?}");
    boot
}

/// The switch to `kind` continued: its first file is the next transaction and holds only
/// what the commits wrote.
fn assert_continued(name: &str, kind: Kind, boot: Option<&Summary>, prev: Txid) {
    let boot = boot.expect("a boot file after a previous tip");
    let fields = boot.header.fields();
    assert_eq!(
        fields.txids.min().get(),
        prev.get().checked_add(1).expect("a next txid"),
        "{name}: {kind:?}'s first file"
    );
    assert!(
        !boot.header.is_snapshot() && !boot.holds_every_page(),
        "{name}: {kind:?} snapshotted at boot: its first file holds {} of {} pages",
        boot.pages,
        fields.commit
    );
}

fn rust_restore(fixture: &AppFixture, target: RestoreTarget, name: &str) -> Vec<String> {
    let out = fresh(fixture, name);
    let restored = fixture.remote().restore(target, &out).expect("sink");
    assert!(
        restored.is_ok(),
        "{name}: Rust cannot restore: {restored:?}"
    );
    pages_of(&out)
}

fn go_restore(
    bin: &Path,
    config: &Path,
    fixture: &AppFixture,
    target: Target,
    name: &str,
) -> Vec<String> {
    let out = fresh(fixture, name);
    let result = restore_with_go(bin, config, &fixture.db, &out, target).expect("go runs");
    assert_eq!(result, GoRestore::Restored, "{name}: Go restore");
    pages_of(&out)
}

/// Go replicates to a MinIO prefix and stops; `lsr replicate -config` starts on the same
/// database, meta directory and prefix, and its first file is the next transaction,
/// incremental, with no new snapshot. Then, for a block-era build, the reverse: Go
/// resumes from what `lsr` left, with no snapshot either. Both restore the combined chain
/// at each handover point.
#[test]
#[ignore = "needs MinIO and the pinned Go binaries; just compat-s3"]
fn over_s3_go_and_rust_hand_over() {
    let Some(env) = minio() else { return };
    let lsr = lsr_binary().expect("lsr builds");
    for build in builds_under_test() {
        hand_over(&env, &build, &lsr);
    }
}

fn hand_over(env: &MinioEnv, build: &GoBuild, lsr: &Path) {
    let name = build.name();
    let bin = build.binary().expect("the build under test is present");
    let mut fixture = fixture(env, "handover", 63);
    fixture.prepare_for_processes(200).expect("prepare");
    // Go snapshots within seconds on the first config; the daily window of the second
    // keeps the later turns from taking a scheduled snapshot once one exists.
    let first = config(&fixture, "go-first.yml", Schedule::FAST);
    let daily = config(&fixture, "daily.yml", Schedule::DAILY_SNAPSHOTS);
    let go = start(Kind::Go, &bin, &first, &fixture.dir().join("go-1.log"));
    assert!(turn(go, &mut fixture, None).is_none());
    let go_tip = tip(&fixture).expect("tip").expect("Go staged");
    let at_go_stop = fixture.hash_now().expect("hash");
    let l9 = snapshots(&fixture);

    let rs = start(Kind::Rust, lsr, &daily, &fixture.dir().join("lsr.log"));
    let boot = turn(rs, &mut fixture, Some(go_tip));
    assert_continued(name, Kind::Rust, boot.as_ref(), go_tip);
    assert_eq!(snapshots(&fixture), l9, "{name}: Rust took a snapshot");
    let rust_tip = tip(&fixture).expect("tip").expect("Rust staged");
    let at_rust_stop = fixture.hash_now().expect("hash");
    assert_eq!(
        rust_restore(&fixture, RestoreTarget::Latest, "rs-1.db"),
        at_rust_stop,
        "{name}"
    );
    assert_eq!(
        rust_restore(&fixture, RestoreTarget::Txid(go_tip), "rs-go-tip.db"),
        at_go_stop,
        "{name}: Rust at the txid where Go stopped"
    );
    if build.era() == Era::Frames {
        println!("{name}: reads only frame-encoded files, so cannot resume from lsr's");
        s3_of(&fixture).delete_all().expect("clean up");
        return;
    }
    let latest = go_restore(&bin, &daily, &fixture, Target::Latest, "go-1.db");
    assert_eq!(latest, at_rust_stop, "{name}: Go on the Go-then-Rust chain");

    let go = start(Kind::Go, &bin, &daily, &fixture.dir().join("go-2.log"));
    let boot = turn(go, &mut fixture, Some(rust_tip));
    assert_continued(name, Kind::Go, boot.as_ref(), rust_tip);
    assert_eq!(snapshots(&fixture), l9, "{name}: Go took a snapshot");
    let at_end = fixture.hash_now().expect("hash");
    assert_eq!(
        go_restore(&bin, &daily, &fixture, Target::Latest, "go-2.db"),
        at_end,
        "{name}"
    );
    assert_eq!(
        rust_restore(&fixture, RestoreTarget::Latest, "rs-2.db"),
        at_end,
        "{name}"
    );
    let at_rust = Target::Txid {
        txid: rust_tip.get(),
    };
    assert_eq!(
        go_restore(&bin, &daily, &fixture, at_rust, "go-rs-tip.db"),
        at_rust_stop,
        "{name}: Go at the txid where lsr stopped"
    );
    println!("{name}: Go -> lsr at {go_tip}, lsr -> Go at {rust_tip}, over S3");
    s3_of(&fixture).delete_all().expect("clean up");
}
