//! The Go half of the merged first snapshot (`docs/decisions/D205.md`): a first start that meets
//! writes leaves the replica's level-9 snapshot built by merging level-0 files, an object Go
//! never writes (Go snapshots by reading the database again). The contract is that Go restores
//! what lsr wrote, so the pinned Go builds restore from that prefix with the level-0 files still
//! there and with every level but 9 gone (the snapshot standing alone), read its listing with
//! `ltx`, and find the same header fields on it that Go's own snapshot of the same database has,
//! the differences explained at the comparison. Equality is on restored pages, never on bytes.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use lsr_compat::{
    AppFixture, Chain, GoBuild, GoRestore, PageEncoding, Replicator, Shape, Target, Written,
    builds_in, clock_ending_now, hash_pages, integrity_check, list, scrub_env, wait_until,
    write_config, write_config_no_scheduled_snapshot,
};
use lsr_core::Level;
use lsr_ltx::{Decoder, Header};

/// Commits the first start meets between staging and uploading its first level-0 files.
const WRITES_DURING_FIRST_START: u32 = 4;
/// Virtual time between an upload and whatever runs next: a snapshot read from the database
/// would be stamped this much after the newest level-0 file, a merged one with that file's stamp.
const GAP_MS: i64 = 500;

fn first_start() -> Written {
    let fixture = AppFixture::create(4096, 21).expect("fixture");
    let mut chain = Chain::start(fixture, clock_ending_now(40).expect("clock")).expect("start");
    chain
        .first_start_while_writing(WRITES_DURING_FIRST_START, GAP_MS)
        .expect("first start");
    chain.finish().expect("finish")
}

/// What one LTX file says, decoded and verified.
struct Seen {
    header: Header,
    pages: u64,
    post_apply: Option<u64>,
}

fn seen(path: &Path) -> Seen {
    let bytes = fs::read(path).expect("read");
    let mut decoder = Decoder::new();
    let mut rest = bytes.as_slice();
    while !rest.is_empty() {
        let (used, _event) = decoder.feed(rest).expect("decodes");
        rest = rest.get(used..).expect("consumed");
    }
    let verified = decoder.finish().expect("verifies");
    Seen {
        header: *verified.header(),
        pages: verified.page_count(),
        post_apply: verified
            .trailer()
            .post_apply()
            .map(lsr_ltx::DbChecksum::get),
    }
}

fn snapshot_of(replica: &Path) -> PathBuf {
    let listing = list(replica).expect("list");
    let mut snapshots = listing
        .files
        .iter()
        .filter(|f| f.key.level() == Level::SNAPSHOT);
    let only = snapshots.next().expect("a level-9 file");
    assert!(snapshots.next().is_none(), "more than one level-9 file");
    replica.join(&only.rel)
}

fn restored_pages(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

/// A first start that meets writes snapshots by merging its level-0 files: the level-9 file
/// spans more than one level-0 file from transaction 1, and carries the newest of them as its
/// stamp, where a snapshot read from the database would carry the later time of its write.
/// Needs no Go.
#[test]
fn the_first_snapshot_of_a_first_start_is_merged_from_level_zero_files() {
    let written = first_start();
    let snapshot = seen(&snapshot_of(&written.fixture.replica));
    let range = snapshot.header.fields().txids;
    assert_eq!(range.min().get(), 1);
    assert!(
        range.max().get() > 1,
        "the writes during the first start made no second level-0 file: {range:?}"
    );
    let newest_input = written
        .archive
        .iter()
        .filter(|path| {
            path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                n.starts_with("0-") && n.ends_with(&format!("-{:016x}.ltx", range.max().get()))
            })
        })
        .map(|path| seen(path).header.fields().timestamp)
        .next()
        .expect("the level-0 file the snapshot ends at");
    assert_eq!(
        snapshot.header.fields().timestamp,
        newest_input,
        "the snapshot was written from the database, not merged from the level-0 files"
    );
}

/// A run long enough to compact: the first and last transaction of each level's oldest file, as
/// the run's archive (every file it ever wrote) shows, and the first snapshot's last transaction.
fn level_starts(written: &Written) -> (u64, Vec<(u8, u64)>) {
    let mut first_snapshot_max: Option<u64> = None;
    let mut starts: Vec<(u8, u64)> = Vec::new();
    for path in &written.archive {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let Some((level, rest)) = name.split_once('-') else {
            continue;
        };
        let (Ok(level), Some((min, max))) = (
            level.parse::<u8>(),
            rest.trim_end_matches(".ltx").split_once('-'),
        ) else {
            continue;
        };
        let (Ok(min), Ok(max)) = (u64::from_str_radix(min, 16), u64::from_str_radix(max, 16))
        else {
            continue;
        };
        if level == 9 {
            first_snapshot_max = Some(first_snapshot_max.map_or(max, |m| m.min(max)));
        } else if level > 0 {
            match starts.iter_mut().find(|(l, _)| *l == level) {
                Some((_, m)) => *m = (*m).min(min),
                None => starts.push((level, min)),
            }
        }
    }
    (first_snapshot_max.expect("a snapshot"), starts)
}

/// Levels 1 to 3 start above the first snapshot (`docs/decisions/D224.md`): a first start that
/// runs on through several windows never carries the database into them. Needs no Go.
#[test]
fn levels_start_above_the_first_snapshot() {
    let fixture = AppFixture::create(4096, 31).expect("fixture");
    let mut chain = Chain::start(fixture, clock_ending_now(120).expect("clock")).expect("start");
    chain.run(120).expect("run");
    let written = chain.finish().expect("finish");
    let (snapshot, starts) = level_starts(&written);
    assert!(!starts.is_empty(), "the run compacted nothing");
    for (level, min) in starts {
        assert!(
            min > snapshot,
            "level {level} starts at {min}, inside the first snapshot (through {snapshot})"
        );
    }
}

/// Go's own snapshot of the database the Rust one holds: Go replicates a copy of it to a fresh
/// replica and takes its first snapshot there.
fn go_snapshot_of(build: &GoBuild, db_copy: &Path, dir: &Path) -> PathBuf {
    let bin = build.binary().expect("the build under test is present");
    let replica = dir.join("go-replica");
    let config = dir.join("go-own.yml");
    write_config_no_scheduled_snapshot(&config, db_copy, &replica).expect("config");
    let log = dir.join("go-own.log");
    let mut go = Replicator::start(&bin, &config, &log).expect("start");
    wait_until("Go's first snapshot", Duration::from_secs(40), || {
        go.ensure_running()?;
        Ok(list(&replica)?
            .files
            .iter()
            .any(|f| f.key.level() == Level::SNAPSHOT))
    })
    .expect("Go snapshots");
    go.stop().expect("stop");
    snapshot_of(&replica)
}

fn go_ltx_listing(build: &GoBuild, config: &Path, db: &Path, home: &Path) -> String {
    let bin = build.binary().expect("the build under test is present");
    let mut command = Command::new(&bin);
    command
        .args(["ltx", "-level", "all", "-config"])
        .arg(config)
        .arg(db);
    scrub_env(&mut command, home).expect("scrub");
    let output = command.output().expect("go runs");
    assert!(
        output.status.success(),
        "ltx: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The tests that run the Go builds under test (`LSR_COMPAT_VERSIONS`).
/// `just compat-live` selects the tests by this module.
mod live {
    use super::*;

    /// Go takes over a replica whose levels start above the snapshot and runs its own compaction
    /// over it, with no error (it refuses inputs that do not follow each other), and restores
    /// the database, as Rust does on the combined chain.
    #[test]
    #[ignore = "runs the pinned Go binary; just compat"]
    fn go_compacts_over_levels_that_start_above_the_snapshot() {
        let fixture = AppFixture::create(4096, 32).expect("fixture");
        let mut chain = Chain::start(fixture, clock_ending_now(60).expect("clock")).expect("start");
        chain.run(60).expect("run");
        let mut written = chain.finish().expect("finish");
        let (snapshot, starts) = level_starts(&written);
        assert!(
            starts.iter().all(|(_, min)| *min > snapshot),
            "levels do not start above the snapshot: {starts:?} / {snapshot}"
        );
        let counts = |replica: &Path, level: u8| {
            list(replica)
                .expect("list")
                .files
                .iter()
                .filter(|f| f.key.level().get() == level)
                .count()
        };
        let config = written.fixture.dir().join("go-takeover.yml");
        write_config_no_scheduled_snapshot(&config, &written.fixture.db, &written.fixture.replica)
            .expect("config");
        for build in builds_in(PageEncoding::Block) {
            let name = build.name().to_owned();
            let bin = build.binary().expect("the build under test is present");
            let log = written
                .fixture
                .dir()
                .join(format!("go-takeover-{name}.log"));
            let before = [1, 2, 3].map(|l| counts(&written.fixture.replica, l));
            let mut go = Replicator::start(&bin, &config, &log).expect("start");
            let replica = written.fixture.replica.clone();
            let mut commits = 0_u32;
            let fixture = &mut written.fixture;
            wait_until(
                "Go's compaction over the replica",
                Duration::from_secs(60),
                || {
                    go.ensure_running()?;
                    if commits < 40 {
                        fixture.commit(Shape::Insert { bytes: 300 })?;
                        commits += 1;
                    }
                    let now = [1, 2, 3].map(|l| counts(&replica, l));
                    Ok(now.iter().sum::<usize>() > before.iter().sum::<usize>() + 2)
                },
            )
            .map_err(|err| go.explain(err))
            .expect("Go compacts");
            go.stop().expect("stop");
            let text = fs::read_to_string(&log).expect("log");
            let errors: Vec<&str> = text
                .lines()
                .filter(|line| line.contains("level=ERROR") || line.contains("non-contiguous"))
                .collect();
            assert!(errors.is_empty(), "{name}: Go reported errors: {errors:#?}");
            let want = written.fixture.hash_now().expect("hash");
            let out = written.fixture.dir().join(format!("takeover-{name}.db"));
            let result = lsr_compat::restore_with_go(
                &bin,
                &config,
                &written.fixture.db,
                &out,
                Target::Latest,
            )
            .expect("go runs");
            assert_eq!(result, GoRestore::Restored, "{name}");
            assert_eq!(restored_pages(&out), want, "{name}");
        }
    }

    /// Go restores the merged snapshot's prefix to the database's pages, with the level-0 files
    /// still there and with only the snapshot left, lists it with `ltx`, and finds on it the
    /// header fields Go's own snapshot of the same database has.
    #[test]
    #[ignore = "runs the pinned Go binary; just compat"]
    fn go_restores_and_reads_the_merged_first_snapshot() {
        let written = first_start();
        let want = written.fixture.hash_now().expect("hash");
        let l9 = snapshot_of(&written.fixture.replica);
        let l9_name = l9
            .file_name()
            .and_then(|n| n.to_str())
            .expect("name")
            .to_owned();
        let max = seen(&l9).header.fields().txids.max();
        let dir = written.fixture.dir().to_path_buf();
        let alone = dir.join("alone");
        fs::create_dir_all(alone.join("ltx").join("9")).expect("dir");
        fs::copy(&l9, alone.join("ltx").join("9").join(&l9_name)).expect("copy");
        let level_zero = list(&written.fixture.replica)
            .expect("list")
            .files
            .iter()
            .filter(|f| f.key.level() == Level::ZERO)
            .count();
        assert!(level_zero > 0, "no level-0 file is left for the first case");
        for build in builds_in(PageEncoding::Block) {
            let name = build.name().to_owned();
            let bin = build.binary().expect("the build under test is present");
            for (case, replica) in [
                ("level 0 present", &written.fixture.replica),
                ("alone", &alone),
            ] {
                let config = dir.join(format!("go-{case}.yml").replace(' ', "-"));
                write_config(&config, &written.fixture.db, replica).expect("config");
                let out = dir.join(format!("restored-{name}-{}.db", case.replace(" ", "-")));
                let result = lsr_compat::restore_with_go(
                    &bin,
                    &config,
                    &written.fixture.db,
                    &out,
                    Target::Latest,
                )
                .expect("go runs");
                assert_eq!(result, GoRestore::Restored, "{name}, {case}");
                assert_eq!(restored_pages(&out), want, "{name}, {case}");
                let listing = go_ltx_listing(&build, &config, &written.fixture.db, &dir);
                let wanted = format!("{:016x}", max.get());
                assert!(
                    listing
                        .lines()
                        .any(|l| l.contains(&wanted) && l.trim_start().starts_with('9')),
                    "{name}, {case}: Go's ltx listing does not show the level-9 file {l9_name}:\n{listing}"
                );
            }
            let copy = dir.join("go-origin").join("app.db");
            fs::create_dir_all(copy.parent().expect("parent")).expect("dir");
            let restored = dir.join(format!("restored-{name}-alone.db"));
            fs::copy(&restored, &copy).expect("copy");
            let go_dir = dir.join(format!("own-{name}"));
            fs::create_dir_all(&go_dir).expect("dir");
            let go = seen(&go_snapshot_of(&build, &copy, &go_dir));
            let rust = seen(&l9);
            let (g, r) = (go.header.fields(), rust.header.fields());
            println!(
                "{name}: Go    {g:?} pages {} post {:?}",
                go.pages, go.post_apply
            );
            println!(
                "{name}: Rust  {r:?} pages {} post {:?}",
                rust.pages, rust.post_apply
            );
            assert_eq!(g.tracked, r.tracked, "{name}: flags");
            assert_eq!(g.page_size, r.page_size, "{name}: page size");
            assert_eq!(g.commit, r.commit, "{name}: page count of the database");
            assert_eq!(go.pages, rust.pages, "{name}: pages the file holds");
            assert_eq!(g.txids.min(), r.txids.min(), "{name}: first transaction");
            assert_eq!(g.pre_apply, r.pre_apply, "{name}: pre-apply checksum");
            assert_eq!(
                go.post_apply, rust.post_apply,
                "{name}: post-apply checksum"
            );
            // Differences, and why a Go reader does not mind: the transactions (Go's snapshot of
            // a fresh replica counts its own; ours is the level-0 run it merged, so it ends where
            // the files do), the timestamp (a merge keeps its newest input's stamp, Go stamps the
            // write) and the WAL origin (Go's snapshot carries the offset, size and salts of the
            // sync it came from; a merge has no one WAL to point at). Go reads a snapshot for its
            // pages, page size, commit, checksum flag and chain position, and takes no decision
            // from the WAL origin of a replica file. None of those differ.
            assert_eq!(g.node_id, r.node_id, "{name}: node id");
            assert!(r.wal.is_none(), "{name}: a merged file has no WAL origin");
        }
    }
}
