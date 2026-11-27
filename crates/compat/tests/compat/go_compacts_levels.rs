//! Go compacts the levels the Rust engine wrote under a writer.
//!
//! Go's compaction reads every file of the level below that starts above its destination's
//! newest transaction and refuses the set when one does not start where the one before ended
//! (`non-contiguous transaction ids in input files`, ltx `compactor.go:125`), at every tick
//! for as long as the hole is there. An engine whose level 2 skipped a level-1 file left
//! exactly that, and a rollback to Go then logged `compaction failed` at level 3 until
//! retention took the files (`docs/decisions/D228.md`).
//!
//! The writer here commits while the engine compacts: once more right after each window's
//! first level-1 file, which is how a window once gained a second one and where the skip
//! happened; a level is now compacted once per window, so the commit lands in the next one.
//! The run stops with three level-2 files above level 3's newest, so Go's first compaction
//! into level 3 has to read across the places a hole would be.
//!
//! The second test shows the refusal itself and the repair: with one of those level-2 files
//! taken away Go refuses, at once and by name; the Rust engine started on that replica
//! writes the file again from level 1 at its first listing; and Go then compacts.

use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_compat::{
    AppFixture, BEYOND_A_RUN, Chain, GoBuild, GoRestore, PageEncoding, Remote, ReplicaFile,
    Replicator, STEP_MS, Schedule, Shape, Target, Written, builds_in, hash_pages, integrity_check,
    list, real_now_ms, restore_replica, restore_with_go, wait_until, write_config_for,
};
use lsr_core::{Event, FileKey, L0Retention, MaintainedEvent, Maintenance, RestoreTarget};

/// The longest level, in milliseconds: level 3 compacts every 8 seconds, level 2 every 2.
const LEVEL_THREE_MS: i64 = 8000;
/// 23 seconds of commits: the run crosses two of level 3's windows and ends late in a third.
const COMMITS: u32 = 230;

/// Levels of 1 and 2 seconds under a level 3 of `level_three` seconds.
fn schedule(level_three: u64) -> Maintenance {
    let secs = Duration::from_secs;
    Maintenance::new(
        vec![secs(1), secs(2), secs(level_three)],
        secs(7200),
        secs(7200),
        L0Retention::After(secs(2)),
    )
    .expect("schedule")
}

/// The same levels for Go, and no snapshot of its own: it works with the levels as they are.
const GO_SCHEDULE: Schedule<'static> = Schedule {
    levels: &["1s", "2s", "8s"],
    snapshot_interval: BEYOND_A_RUN,
    snapshot_retention: BEYOND_A_RUN,
};

/// A start such that the run ends half a second before one of level 3's windows opens, at
/// least a second ago: levels 2 and 1 have compacted three times since level 3 last did.
fn start_ms() -> i64 {
    let latest = real_now_ms().expect("clock").saturating_sub(1000);
    let window = latest
        .div_euclid(LEVEL_THREE_MS)
        .saturating_mul(LEVEL_THREE_MS);
    let end = window.saturating_sub(500);
    end.saturating_sub(STEP_MS.saturating_mul(i64::from(COMMITS)))
}

fn level_one_output(event: &Event) -> Option<FileKey> {
    if let Event::Maintained {
        event: MaintainedEvent::Compacted(info),
        ..
    } = event
        && info.key.level().get() == 1
    {
        return Some(info.key);
    }
    None
}

/// A writer that commits every tenth of a second, and once more right after the first
/// level-1 file each of those commits brings.
fn write_levels() -> Written {
    let fixture = AppFixture::create(4096, 3).expect("fixture");
    let mut chain = Chain::start_scheduled(fixture, start_ms(), schedule(8)).expect("start");
    for _ in 0..COMMITS {
        chain
            .fixture
            .commit(Shape::Insert { bytes: 64 })
            .expect("commit");
        let mut landed = false;
        chain
            .catch_up_with(|chain, event| {
                if !landed && level_one_output(event).is_some() {
                    landed = true;
                    chain.fixture.commit(Shape::Tiny)?;
                }
                Ok(())
            })
            .expect("catch up");
    }
    chain.finish().expect("finish")
}

fn ranges(replica: &Path, level: u8) -> Vec<(u64, u64)> {
    list(replica)
        .expect("list")
        .files
        .iter()
        .filter(|file| file.key.level().get() == level)
        .map(|file| (file.key.range().min().get(), file.key.range().max().get()))
        .collect()
}

/// The first two neighbours where the later does not start right after the earlier ends.
fn first_hole(ranges: &[(u64, u64)]) -> Option<((u64, u64), (u64, u64))> {
    let pairs = ranges.iter().zip(ranges.iter().skip(1));
    pairs
        .into_iter()
        .find_map(|(a, b)| (b.0 != a.1.saturating_add(1)).then_some((*a, *b)))
}

fn pages(path: &Path) -> Vec<String> {
    integrity_check(path).expect("integrity_check");
    hash_pages(path).expect("hash").page_hashes
}

/// What the run must have reached for the Go half to mean anything: several level-2 files
/// for Go's level 3 to read. A level is compacted once per window, so level 1 no longer gains
/// two files in one window here (it takes a backlog of more than 32 level-0 files, which the
/// simulator's `level_holes` scenario builds); the writer's extra commit after each level-1
/// file stays, since the engine's next windows still have to take it
/// (`docs/decisions/D242.md`).
fn assert_shape(written: &Written) {
    let replica = &written.fixture.replica;
    let three = ranges(replica, 3).last().expect("level 3").1;
    let above = ranges(replica, 2).iter().filter(|r| r.0 > three).count();
    assert!(above >= 3, "{above} level-2 files above level 3's newest");
}

/// Needs no Go: every level the engine wrote under the writer is whole.
#[test]
#[ignore = "runs 230 commits; just compat"]
fn levels_written_under_a_writer_have_no_hole() {
    let written = write_levels();
    assert_shape(&written);
    for level in 1..=3 {
        let ranges = ranges(&written.fixture.replica, level);
        assert!(ranges.len() >= 3, "level {level} holds {ranges:?}");
        assert_eq!(first_hole(&ranges), None, "level {level}: {ranges:?}");
    }
}

fn go_config(fixture: &AppFixture) -> PathBuf {
    let config = fixture.dir().join("go.yml");
    let remote = Remote::Dir(fixture.replica.clone());
    write_config_for(&config, &fixture.db, &remote, GO_SCHEDULE).expect("config");
    config
}

/// Runs `build` on the fixture until its level 3 holds everything level 2 held when it
/// started, or it logs that a compaction failed. Returns those log lines: none when it
/// compacted. Go compacts each level when it starts: level 3's newest file is older than its
/// window.
fn go_compacts(build: &GoBuild, fixture: &AppFixture, log: &str) -> Vec<String> {
    let bin = build.binary().expect("the build under test is present");
    let replica = &fixture.replica;
    let two = ranges(replica, 2).last().expect("level 2").1;
    let mut go =
        Replicator::start(&bin, &go_config(fixture), &fixture.dir().join(log)).expect("start");
    let refused = |go: &Replicator| -> Vec<String> {
        let log = go.log_text();
        let failed = log
            .lines()
            .filter(|line| line.contains("compaction failed"));
        failed.map(str::to_owned).collect()
    };
    wait_until(
        "Go's compaction into level 3",
        Duration::from_secs(60),
        || {
            go.ensure_running()?;
            let reached = ranges(replica, 3).last().is_some_and(|r| r.1 >= two);
            Ok(reached || !refused(&go).is_empty())
        },
    )
    .map_err(|err| go.explain(err))
    .expect("Go compacts level 2 into level 3, or says why not");
    let said = refused(&go);
    go.stop().expect("stop");
    said
}

/// Go and Rust both restore the newest state from the fixture's replica to `want`.
fn both_restore(build: &GoBuild, fixture: &AppFixture, want: &[String], tag: &str) {
    let name = build.name();
    let bin = build.binary().expect("the build under test is present");
    let out = fixture.dir().join(format!("go-{tag}.db"));
    let restored = restore_with_go(&bin, &go_config(fixture), &fixture.db, &out, Target::Latest);
    assert_eq!(
        restored.expect("Go runs"),
        GoRestore::Restored,
        "{name} {tag}"
    );
    assert_eq!(pages(&out), want, "{name} {tag}");
    let rs = fixture.dir().join(format!("rs-{tag}.db"));
    restore_replica(&fixture.replica, RestoreTarget::Latest, &rs)
        .expect("sink")
        .expect("Rust restores");
    assert_eq!(pages(&rs), want, "{name} {tag}");
}

fn go_compacts_and_restores(build: &GoBuild) {
    let name = build.name();
    let written = write_levels();
    assert_shape(&written);
    let refused = go_compacts(build, &written.fixture, "go.log");
    assert_eq!(refused, Vec::<String>::new(), "{name}");
    for level in 1..=3 {
        let ranges = ranges(&written.fixture.replica, level);
        assert_eq!(
            first_hole(&ranges),
            None,
            "{name} level {level}: {ranges:?}"
        );
    }
    let want = &written.points.last().expect("points").page_hashes;
    both_restore(build, &written.fixture, want, "after");
}

/// The replica an engine that skipped a level-1 file left: one of the level-2 files above
/// level 3's newest is gone, and level 1 still holds its transactions.
fn go_refuses_a_hole_and_rust_closes_it(build: &GoBuild) {
    let name = build.name();
    let written = write_levels();
    assert_shape(&written);
    let want = written.points.last().expect("points").page_hashes.clone();
    let fixture = written.fixture;
    let three = ranges(&fixture.replica, 3).last().expect("level 3").1;
    let above: Vec<ReplicaFile> = list(&fixture.replica)
        .expect("list")
        .files
        .into_iter()
        .filter(|file| file.key.level().get() == 2 && file.key.range().min().get() > three)
        .collect();
    let gone = &above[1];
    let path = fixture.replica.join(&gone.rel);
    std::fs::remove_file(&path).expect("remove a level-2 file");
    // The checker can fail: this is the line the first test asserts Go never logs.
    let said = go_compacts(build, &fixture, "go-hole.log");
    assert!(
        said.iter()
            .any(|line| line.contains("non-contiguous transaction ids in input files")),
        "{name} read across a hole, or refused for another reason: {said:?}"
    );
    // The hole costs Go's restore nothing: its plan takes level 1 there.
    both_restore(build, &fixture, &want, "hole");
    // The Rust engine on that replica, with a level 3 it will not compact during the run, so
    // closing the hole is all it does to the levels.
    let now = real_now_ms().expect("clock");
    let mut chain = Chain::start_scheduled(fixture, now, schedule(3600)).expect("start");
    chain
        .settle()
        .expect("the engine boots and closes the hole");
    let fixture = chain.finish().expect("finish").fixture;
    assert!(path.is_file(), "{name}: {} was not written again", gone.rel);
    let two = ranges(&fixture.replica, 2);
    assert_eq!(first_hole(&two), None, "{name}: {two:?}");
    let refused = go_compacts(build, &fixture, "go-closed.log");
    assert_eq!(refused, Vec::<String>::new(), "{name}");
    let three = ranges(&fixture.replica, 3);
    assert_eq!(first_hole(&three), None, "{name}: {three:?}");
    both_restore(build, &fixture, &want, "closed");
}

/// The tests that run the Go builds under test (`LSR_COMPAT_VERSIONS`).
/// `just compat-live` selects the tests by this module (`test(/(^|::)live::/)`).
mod live {
    use super::*;

    /// Every build that reads Rust's files runs its own compaction over levels 1 to 3 as the
    /// Rust engine left them under a writer, logs no refusal, and restores the database.
    #[test]
    #[ignore = "runs the Go binaries; just compat"]
    fn go_compacts_and_restores_levels_rust_wrote_under_a_writer() {
        let builds = builds_in(PageEncoding::Block);
        if builds.is_empty() {
            println!(
                "no block-encoding build under test: a frame-encoding Go cannot read Rust's files"
            );
            return;
        }
        for build in &builds {
            go_compacts_and_restores(build);
        }
    }

    /// With a hole in level 2 above level 3's newest file, every such build refuses the
    /// compaction by name and still restores; the Rust engine started on that replica closes
    /// the hole from level 1, and the same build then compacts and restores.
    #[test]
    #[ignore = "runs the Go binaries; just compat"]
    fn go_refuses_a_hole_in_a_level_and_compacts_once_rust_closed_it() {
        let builds = builds_in(PageEncoding::Block);
        if builds.is_empty() {
            println!(
                "no block-encoding build under test: a frame-encoding Go cannot read Rust's files"
            );
            return;
        }
        for build in &builds {
            go_refuses_a_hole_and_rust_closes_it(build);
        }
    }
}
