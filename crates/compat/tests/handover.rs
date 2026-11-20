//! Handover between Go and Rust: each reads the other's staged files.
//!
//! The fixtures are replicas, with no local meta dir or WAL beside them, so the
//! Go-to-Rust half builds the WAL a Go process would have left: the newest L0
//! file's own WAL origin (salts, offset, size) fixes its salts and frame count, and
//! its last page is the last frame. The Rust-to-Go half runs a transcription of
//! Go's `verify` (`db.go:1782-1925`) over files Rust wrote.

#![expect(
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    reason = "clippy.toml exempts only #[test] bodies; these are the test file's own helpers, where a failure is the test failing"
)]

use std::fs;
use std::io::Cursor;

use lsr_compat::{Manifest, all_fixtures, parse_rel};
use lsr_core::conformance::block_on;
use lsr_core::{
    CommittedRead, FakeDb, FakeWal, FileKey, Level, MemStore, ObjectStore, Recovered,
    SnapshotCause, Step, StepEnd, SyncConfig, Synced, Tip, TipView, Upload, WalFileLen, WalView,
    recover, sync_step,
};
use lsr_core::{PageLimit, ReadLen};
use lsr_ltx::{Event, Header, PageNo, PageSize, ReadDecoder, Timestamp};
use lsr_wal::{Salts, WalReader};

const WAL_HEADER: u64 = 32;
const FRAME_HEADER: u64 = 24;

fn decode(bytes: &[u8]) -> (Header, Vec<(PageNo, Vec<u8>)>) {
    let mut decoder = ReadDecoder::new(bytes);
    let mut header = None;
    let mut pages = Vec::new();
    while let Some(event) = decoder.next_event().expect("decode") {
        match event {
            Event::Header(h) => header = Some(h),
            Event::Page { pgno, data } => pages.push((pgno, data.to_vec())),
        }
    }
    decoder.finish().expect("finish");
    (header.expect("header"), pages)
}

fn frame_len(ps: PageSize) -> u64 {
    FRAME_HEADER.saturating_add(u64::try_from(ps.bytes()).expect("page size"))
}

/// Go's header rules for the file that follows `prev` in the same WAL generation.
fn go_next_header_rules(prev: &Header, next: &Header) -> Result<(), String> {
    let (p, n) = (prev.fields(), next.fields());
    let (po, no) = (
        p.wal.ok_or("prev has no origin")?,
        n.wal.ok_or("no origin")?,
    );
    let checks = [
        (n.txids.min() == n.txids.max(), "one txid per L0 file"),
        (Some(n.txids.min()) == p.txids.max().next(), "txid follows"),
        (
            no.offset() == po.offset().saturating_add(po.size()),
            "offset continues",
        ),
        (no.salts() == po.salts(), "same salts"),
        (n.page_size == p.page_size, "same page size"),
        (!n.tracked, "untracked"),
        (n.pre_apply.is_none(), "no pre-apply checksum"),
        (n.node_id == 0, "node id 0"),
        (n.commit > 0, "commit"),
        (n.timestamp >= p.timestamp, "time does not go back"),
    ];
    match checks.iter().find(|(ok, _)| !ok) {
        Some((_, rule)) => Err((*rule).to_string()),
        None => Ok(()),
    }
}

fn be32(bytes: &[u8], at: usize) -> Option<u32> {
    let raw: [u8; 4] = bytes.get(at..at.checked_add(4)?)?.try_into().ok()?;
    Some(u32::from_be_bytes(raw))
}

/// Go's `verifyWithExecutor` as a yes or no: would it continue incrementally?
///
/// A salt reset after a matching last page runs Go's `detectFullCheckpoint`,
/// which is not transcribed; that case answers `false`.
fn go_would_continue(tip: &Header, tip_pages: &[(PageNo, Vec<u8>)], wal: &[u8]) -> bool {
    let fields = tip.fields();
    let Some(origin) = fields.wal else {
        return false;
    };
    let frame = frame_len(fields.page_size);
    let offset = origin.offset().saturating_add(origin.size());
    if offset > u64::try_from(wal.len()).unwrap_or(0) {
        return false;
    }
    let (Some(salt1), Some(salt2)) = (be32(wal, 16), be32(wal, 20)) else {
        return false;
    };
    let salt_match = (salt1, salt2) == origin.salts();
    if offset == WAL_HEADER {
        return salt_match;
    }
    let Some(prev) = offset.checked_sub(frame) else {
        return false;
    };
    if prev == WAL_HEADER {
        return salt_match;
    }
    if prev < WAL_HEADER {
        return false;
    }
    let Ok(at) = usize::try_from(prev) else {
        return false;
    };
    let Some(bytes) = usize::try_from(frame)
        .ok()
        .and_then(|len| wal.get(at..at.checked_add(len)?))
    else {
        return false;
    };
    let (Some(pgno), Some(f1), Some(f2)) = (be32(bytes, 0), be32(bytes, 8), be32(bytes, 12)) else {
        return false;
    };
    if (f1, f2) != origin.salts() {
        return false;
    }
    let data = bytes
        .get(usize::try_from(FRAME_HEADER).unwrap_or(0)..)
        .unwrap_or_default();
    let last_page_match = tip_pages
        .iter()
        .any(|(p, page)| p.get() == pgno && page.as_slice() == data);
    last_page_match && salt_match
}

struct GoTip {
    fixture: String,
    key: FileKey,
    bytes: Vec<u8>,
    header: Header,
    pages: Vec<(PageNo, Vec<u8>)>,
}

/// The newest L0 file of each fixture that records a WAL origin with frames.
fn go_tips() -> Vec<GoTip> {
    let mut tips = Vec::new();
    for dir in all_fixtures().expect("list") {
        let manifest = Manifest::load(&dir).expect("load");
        let mut best: Option<GoTip> = None;
        for file in &manifest.files {
            let Some(key) = parse_rel(&file.key) else {
                continue;
            };
            if key.level() != Level::ZERO {
                continue;
            }
            let bytes = fs::read(Manifest::replica_dir(&dir).join(&file.key)).expect("read");
            let (header, pages) = decode(&bytes);
            if header.fields().wal.is_none_or(|o| o.size() == 0) {
                continue;
            }
            if best
                .as_ref()
                .is_none_or(|b| key.range().max() > b.key.range().max())
            {
                best = Some(GoTip {
                    fixture: format!("{}/{}", manifest.go_version, manifest.scenario),
                    key,
                    bytes,
                    header,
                    pages,
                });
            }
        }
        tips.extend(best);
    }
    tips
}

fn page_one(ps: PageSize) -> PageNo {
    PageNo::new(1, ps).expect("page 1")
}

/// The WAL a Go process left beside `tip`: its salts, its frame count, its last page last.
fn wal_beside(tip: &GoTip) -> FakeWal {
    let fields = tip.header.fields();
    let origin = fields.wal.expect("origin");
    let end = origin.offset().saturating_add(origin.size());
    let span = end.saturating_sub(WAL_HEADER);
    let frame = frame_len(fields.page_size);
    assert_eq!(span % frame, 0, "{}: a whole number of frames", tip.fixture);
    let (salt1, salt2) = origin.salts();
    let wal = FakeWal::new(fields.page_size, Salts { salt1, salt2 });
    let commit = std::num::NonZeroU32::new(fields.commit).expect("commit");
    let filler = vec![0x11; fields.page_size.bytes()];
    for _ in 1..(span / frame) {
        wal.append_tx(&[(page_one(fields.page_size), filler.clone())], commit);
    }
    let (pgno, data) = tip.pages.last().expect("a page");
    wal.append_tx(&[(*pgno, data.clone())], commit);
    wal
}

fn view_of(wal: &FakeWal, reader: &WalReader<Cursor<Vec<u8>>>) -> WalView {
    WalView {
        header: Some(*reader.header()),
        len: Some(WalFileLen::new(
            u64::try_from(wal.file_bytes().len()).expect("len"),
        )),
        committed: CommittedRead::new(wal.committed_end(), wal.backfilled()),
        epoch: wal.epoch(),
    }
}

fn put(store: &MemStore, key: &FileKey, bytes: &[u8]) {
    block_on(async {
        let mut upload = store
            .begin_upload(key, Timestamp::from_unix_millis(1))
            .await
            .expect("begin");
        upload.write(bytes).await.expect("write");
        upload.commit().await.expect("commit");
    });
}

fn staged(store: &MemStore) -> Vec<Vec<u8>> {
    block_on(async {
        let listing = store
            .list(Level::ZERO, None, PageLimit::MAX)
            .await
            .expect("list");
        let mut out = Vec::new();
        for info in listing.files {
            let len = ReadLen::new(ReadLen::MAX_RAW).expect("len");
            out.push(store.read(&info.key, 0, len).await.expect("read"));
        }
        out
    })
}

#[test]
fn rust_continues_from_every_go_written_tip_and_its_next_file_passes_go_rules() {
    let tips = go_tips();
    assert!(
        tips.len() >= 15,
        "only {} fixtures record a WAL origin (20 measured)",
        tips.len()
    );
    for tip in tips {
        let wal = wal_beside(&tip);
        let store = MemStore::new();
        put(&store, &tip.key, &tip.bytes);
        let mut reader = WalReader::open(Cursor::new(wal.file_bytes())).expect("wal");
        let loaded = block_on(TipView::load(&store, Some(&mut reader)))
            .expect("load")
            .expect("a tip");
        let recovered = recover(Some(Tip::Boot(&loaded)), &view_of(&wal, &reader), None);
        let Recovered::Continue(position) = recovered else {
            panic!("{}: {recovered:?}", tip.fixture);
        };
        let ps = tip.header.fields().page_size;
        assert_eq!(position.txid(), tip.header.fields().txids.max());

        let commit = std::num::NonZeroU32::new(tip.header.fields().commit).expect("commit");
        wal.append_tx(&[(page_one(ps), vec![7; ps.bytes()])], commit);
        let mut next_reader = WalReader::open(Cursor::new(wal.file_bytes())).expect("wal");
        let committed = CommittedRead::new(wal.committed_end(), wal.backfilled()).expect("read");
        let now = position.timestamp().unix_millis().saturating_add(1000);
        let synced = block_on(sync_step(
            Step::Incremental(position),
            &mut next_reader,
            committed,
            wal.epoch(),
            &mut FakeDb::new(ps),
            &store,
            &SyncConfig::default(),
            Timestamp::from_unix_millis(now),
        ))
        .unwrap_or_else(|failed| panic!("{}: {failed:?}", tip.fixture));
        assert!(matches!(synced, Synced::Wrote { .. }), "{}", tip.fixture);
        let files = staged(&store);
        let (next, _) = decode(files.last().expect("the new file"));
        go_next_header_rules(&tip.header, &next)
            .unwrap_or_else(|rule| panic!("{}: breaks Go's rule: {rule}", tip.fixture));
    }
}

#[test]
fn a_go_tip_whose_wal_was_truncated_is_a_snapshot_not_a_continue() {
    for tip in go_tips() {
        let wal = wal_beside(&tip);
        let store = MemStore::new();
        put(&store, &tip.key, &tip.bytes);
        let mut reader = WalReader::open(Cursor::new(wal.file_bytes())).expect("wal");
        let loaded = block_on(TipView::load(&store, Some(&mut reader)))
            .expect("load")
            .expect("a tip");
        wal.truncate_file(usize::try_from(WAL_HEADER).expect("small"));
        let recovered = recover(Some(Tip::Boot(&loaded)), &view_of(&wal, &reader), None);
        assert!(
            matches!(
                recovered,
                Recovered::Snapshot {
                    cause: SnapshotCause::LocalRewound { .. },
                    ..
                }
            ),
            "{}: {recovered:?}",
            tip.fixture
        );
    }
}

fn resume_from(synced: Synced) -> lsr_core::Position {
    match synced {
        Synced::Idle(t)
        | Synced::Wrote {
            end: StepEnd::Through(t),
            ..
        } => t.into_position(),
        Synced::Wrote {
            end: StepEnd::Limited(p),
            ..
        } => p,
    }
}

/// A chain Rust wrote over `ps`-byte pages: a snapshot, then one step per `cap`.
fn rust_chain(ps: u32, cap: Option<u64>) -> (Vec<Vec<u8>>, Vec<u8>) {
    let ps = PageSize::new(ps).expect("page size");
    let wal = FakeWal::new(
        ps,
        Salts {
            salt1: 77,
            salt2: 88,
        },
    );
    let page = |n: u32, fill: u8| (PageNo::new(n, ps).expect("page"), vec![fill; ps.bytes()]);
    let commit = |n: u32| std::num::NonZeroU32::new(n).expect("commit");
    let store = MemStore::new();
    let mut db = FakeDb::new(ps);
    for n in 1..=5 {
        db.write_page(PageNo::new(n, ps).expect("page"), &vec![0xD0; ps.bytes()]);
    }
    let cfg = SyncConfig {
        cap: cap.and_then(std::num::NonZeroU64::new),
        node_id: 0,
    };
    let mut run = |step: Step, wal: &FakeWal, now: i64| {
        let mut reader = WalReader::open(Cursor::new(wal.file_bytes())).expect("wal");
        let committed = CommittedRead::new(wal.committed_end(), wal.backfilled()).expect("read");
        block_on(sync_step(
            step,
            &mut reader,
            committed,
            wal.epoch(),
            &mut db,
            &store,
            &cfg,
            Timestamp::from_unix_millis(now),
        ))
        .expect("step")
    };
    wal.append_tx(&[page(1, 1), page(2, 2)], commit(2));
    let step = Step::Snapshot {
        after: None,
        prev_timestamp: None,
        cause: SnapshotCause::FirstSync,
    };
    let mut synced = run(step, &wal, 1000);
    for (i, pages) in [vec![3], vec![1, 2, 3, 4], vec![2], vec![5, 1]]
        .iter()
        .enumerate()
    {
        let list: Vec<_> = pages.iter().map(|n| page(*n, 9)).collect();
        wal.append_tx(&list, commit(*pages.iter().max().expect("page")));
        let position = resume_from(synced);
        let now = 2000 + i64::try_from(i).expect("small") * 1000;
        synced = run(Step::Incremental(position), &wal, now);
    }
    while matches!(
        synced,
        Synced::Wrote {
            end: StepEnd::Limited(_),
            ..
        }
    ) {
        synced = run(Step::Incremental(resume_from(synced)), &wal, 9000);
    }
    (staged(&store), wal.file_bytes())
}

#[test]
fn go_would_continue_at_every_tip_of_a_chain_rust_wrote() {
    for ps in [512, 4096] {
        for cap in [None, Some(1), Some(3000)] {
            let (files, wal) = rust_chain(ps, cap);
            assert!(files.len() >= 2);
            for bytes in &files {
                let (header, pages) = decode(bytes);
                assert!(
                    go_would_continue(&header, &pages, &wal),
                    "page size {ps}, cap {cap:?}: Go would snapshot at {:?}",
                    header.fields().txids
                );
            }
        }
    }
}

#[test]
fn the_go_transcription_says_no_when_the_wal_does_not_match() {
    let (files, wal) = rust_chain(512, Some(1));
    let (header, pages) = decode(files.last().expect("a file"));
    assert!(go_would_continue(&header, &pages, &wal));
    assert!(!go_would_continue(&header, &pages, &wal[..64]), "truncated");
    let mut other_salts = wal.clone();
    other_salts[16] ^= 1;
    assert!(
        !go_would_continue(&header, &pages, &other_salts),
        "salt reset"
    );
    let origin = header.fields().wal.expect("origin");
    let end = usize::try_from(origin.offset() + origin.size()).expect("small");
    let mut flipped = wal.clone();
    flipped[end - 10] ^= 1;
    assert!(
        !go_would_continue(&header, &pages, &flipped),
        "last page differs"
    );
}

/// A live Go process taking over a chain the Rust actor wrote, and the reverse.
mod live_go {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use lsr_compat::{
        Chain, Era, GoBuild, GoRestore, Replicator, Shape, Site, Target, Written, builds_in,
        builds_under_test, clock_ending_now, hash_pages, integrity_check, list, newest_l0,
        real_now_ms, restore_replica, restore_with_go, wait_until, write_config,
        write_config_snapshotting,
    };
    use lsr_core::{Event, Level, RestoreTarget};
    use lsr_ltx::{Header, Txid};

    use super::{decode, fs};

    fn pages_of(path: &Path) -> Vec<String> {
        integrity_check(path).expect("integrity_check");
        hash_pages(path).expect("hash").page_hashes
    }

    /// Runs `each` over the block-era builds under test, and says so when the run names none:
    /// the Rust-to-Go direction has nothing to run then, because frame-era builds refuse
    /// Rust's files (Go-to-Rust handover runs over every era).
    fn for_blocks(mut each: impl FnMut(&GoBuild)) {
        let builds = builds_in(Era::Blocks);
        if builds.is_empty() {
            println!("no block-era build under test: nothing to hand over");
        }
        for build in &builds {
            each(build);
        }
    }

    fn l9(replica: &Path) -> BTreeSet<String> {
        list(replica)
            .expect("list")
            .files
            .into_iter()
            .filter(|f| f.key.level() == Level::SNAPSHOT)
            .map(|f| f.rel)
            .collect()
    }

    /// The headers of the L0 files in `meta` newer than `after`, oldest first.
    fn l0_after(meta: &Path, after: u64) -> Vec<Header> {
        let mut files: Vec<_> = list(meta)
            .expect("list")
            .files
            .into_iter()
            .filter(|f| f.key.level() == Level::ZERO && f.key.range().max().get() > after)
            .collect();
        files.sort_by_key(|f| f.key);
        files
            .iter()
            .map(|f| decode(&fs::read(meta.join(&f.rel)).expect("read")).0)
            .collect()
    }

    /// Waits until Go has staged a file past `above` and then nothing newer for a while. The
    /// WAL file's length cannot say where the log ends: SQLite reuses the file after a restart.
    #[expect(
        clippy::disallowed_methods,
        reason = "waits on a real Go process, which keeps real time"
    )]
    fn wait_quiet(
        go: &mut Replicator,
        meta: &Path,
        above: u64,
    ) -> Result<(), lsr_compat::CompatError> {
        let mut last = None;
        let mut since = Instant::now();
        wait_until(
            "Go to stage the commits and go quiet",
            Duration::from_secs(30),
            || {
                go.ensure_running()?;
                let tip = newest_l0(meta)?.map(|h| h.fields().txids.max().get());
                if tip != last {
                    last = tip;
                    since = Instant::now();
                }
                Ok(tip.is_some_and(|t| t > above) && since.elapsed() >= Duration::from_millis(700))
            },
        )
    }

    fn restore_go(
        binary: &Path,
        config: &Path,
        site: &Site,
        name: &str,
        target: Target,
    ) -> PathBuf {
        let out = site.dir().join(name);
        for suffix in ["", "-wal", "-shm"] {
            drop(fs::remove_file(format!("{}{suffix}", out.display())));
        }
        let result = restore_with_go(binary, config, &site.db, &out, target).expect("go runs");
        assert_eq!(result, GoRestore::Restored, "go restore {name}");
        out
    }

    /// A Rust run of `commits` commits, optionally ended by a TRUNCATE checkpoint.
    fn rust_run(commits: u32, truncate: bool) -> Written {
        let site = Site::create(4096, 21).expect("site");
        let mut chain =
            Chain::start(site, clock_ending_now(commits + 20).expect("clock")).expect("start");
        chain.run(commits).expect("run");
        if truncate {
            chain.grow_until_truncate().expect("truncate");
        }
        chain.finish().expect("finish")
    }

    /// Go replicates over the Rust run's meta and replica; the app commits three more
    /// transactions; Go's first new file is the next transaction, read from the same WAL
    /// generation past its first frame, and no snapshot is taken.
    fn resumes(build: &GoBuild, truncate: bool) {
        let version = build.name();
        let bin = build.binary().expect("the build under test is present");
        let mut written = rust_run(60, truncate);
        let tip = written.points.last().expect("points").txid;
        let tip_head = newest_l0(&written.site.meta)
            .expect("l0")
            .expect("a staged file");
        let tip_salts = tip_head.fields().wal.expect("origin").salts();
        let snapshots = l9(&written.site.replica);
        let config = written.site.dir().join("go.yml");
        write_config_snapshotting(&config, &written.site.db, &written.site.replica, "1h")
            .expect("config");
        let mut go =
            Replicator::start(&bin, &config, &written.site.dir().join("go.log")).expect("start");
        for _ in 0..3 {
            written
                .site
                .commit(Shape::Insert { bytes: 300 })
                .expect("commit");
        }
        wait_quiet(&mut go, &written.site.meta, tip.get()).expect("Go resumes");
        go.stop().expect("stop");
        let new = l0_after(&written.site.meta, tip.get());
        let first = new.first().expect("a new file").fields();
        assert_eq!(
            first.txids.min().get(),
            tip.get() + 1,
            "{version}: Go's next file"
        );
        let origin = first.wal.expect("origin");
        assert!(
            origin.offset() > 32,
            "{version}: Go restarted the WAL read at {}",
            origin.offset()
        );
        assert_eq!(
            origin.salts(),
            tip_salts,
            "{version}: Go read another WAL generation"
        );
        assert!(
            !first.txids.min().get().eq(&1),
            "{version}: Go wrote a snapshot"
        );
        assert_eq!(
            l9(&written.site.replica),
            snapshots,
            "{version}: a new snapshot appeared"
        );
        let want = written.site.hash_now().expect("hash");
        let out = restore_go(&bin, &config, &written.site, "after-go.db", Target::Latest);
        assert_eq!(pages_of(&out), want, "{version}");
        let rs = written.site.dir().join("rs-after-go.db");
        restore_replica(&written.site.replica, RestoreTarget::Latest, &rs)
            .expect("sink")
            .expect("Rust restores the handed-over chain");
        assert_eq!(pages_of(&rs), want, "{version}: Rust on the combined chain");
    }

    #[test]
    #[ignore = "runs the pinned Go binary; just compat"]
    fn go_resumes_from_rust_meta() {
        for_blocks(|build| resumes(build, false));
    }

    #[test]
    #[ignore = "runs the pinned Go binary; just compat"]
    fn go_resumes_after_rust_checkpoint() {
        for_blocks(|build| resumes(build, true));
    }

    /// Every build under test, frame era included, writes a replica from a live database that
    /// Rust restores to the pages the database held, the same pages Go's own restore returns.
    #[test]
    #[ignore = "runs the Go binaries; just compat"]
    fn rust_restores_what_go_wrote() {
        for build in builds_under_test() {
            let name = build.name();
            let bin = build.binary().expect("the build under test is present");
            let mut site = Site::create(4096, 41).expect("site");
            let config = site.dir().join("go.yml");
            write_config(&config, &site.db, &site.replica).expect("config");
            let mut go =
                Replicator::start(&bin, &config, &site.dir().join("go.log")).expect("start");
            for i in 0..40 {
                site.commit(if i % 7 == 6 {
                    Shape::Tiny
                } else {
                    Shape::Insert { bytes: 900 }
                })
                .expect("commit");
                go.ensure_running().expect("Go runs");
            }
            wait_quiet(&mut go, &site.meta, 0).expect("Go catches up");
            go.stop().expect("stop");
            let want = site.hash_now().expect("hash");
            let rs = site.dir().join("rs.db");
            restore_replica(&site.replica, RestoreTarget::Latest, &rs)
                .expect("sink")
                .unwrap_or_else(|e| panic!("{name}: Rust cannot restore Go's replica: {e:?}"));
            assert_eq!(pages_of(&rs), want, "{name}: Rust on Go's replica");
            let go_out = restore_go(&bin, &config, &site, "go.db", Target::Latest);
            assert_eq!(pages_of(&go_out), want, "{name}: Go on its own replica");
        }
    }

    /// Go writes, stops; the Rust actor boots from Go's meta and replica and goes on. One
    /// chain: Go restores the end and the point where Go stopped.
    #[test]
    #[ignore = "runs the pinned Go binary; just compat"]
    fn rust_resumes_from_go_meta() {
        for build in builds_under_test() {
            rust_resumes(&build);
        }
    }

    /// Go writes 40 commits and stops. Returns the site, the txid Go stopped at and the pages
    /// the database held then.
    #[expect(
        clippy::disallowed_methods,
        reason = "paces the app's commits for a real Go process polling the WAL; a known exception to the no-sleeps rule (AGENTS.md), to be replaced by a condition"
    )]
    fn go_writes_and_stops(build: &GoBuild, config: &Path) -> (Site, u64, Vec<String>) {
        let bin = build.binary().expect("the build under test is present");
        let mut site = Site::create(4096, 33).expect("site");
        write_config(config, &site.db, &site.replica).expect("config");
        let mut go = Replicator::start(&bin, config, &site.dir().join("go.log")).expect("start");
        for i in 0..40 {
            site.commit(if i % 7 == 6 {
                Shape::Tiny
            } else {
                Shape::Insert { bytes: 900 }
            })
            .expect("commit");
            std::thread::sleep(Duration::from_millis(30));
            go.ensure_running().expect("Go runs");
        }
        wait_quiet(&mut go, &site.meta, 0).expect("Go catches up");
        go.stop().expect("stop");
        let go_tip = newest_l0(&site.meta)
            .expect("l0")
            .expect("file")
            .fields()
            .txids
            .max()
            .get();
        let at_stop = site.hash_now().expect("hash");
        (site, go_tip, at_stop)
    }

    /// Rust restores `target` from the combined chain and the pages equal `want`.
    fn rust_restores_combined(
        written: &Written,
        target: RestoreTarget,
        want: &[String],
        label: &str,
    ) {
        let out = written.site.dir().join(format!("rs-{label}.db"));
        restore_replica(&written.site.replica, target, &out)
            .expect("sink")
            .map_err(|e| format!("{label}: Rust cannot restore the combined chain: {e:?}"))
            .expect("Rust restores a Go-then-Rust chain");
        assert_eq!(pages_of(&out), want, "{label}: Rust on the combined chain");
    }

    /// Go writes, stops; the Rust actor boots from Go's meta and replica and goes on. Rust
    /// restores the combined chain at the end and at the txid where Go stopped. A block-era Go
    /// restores it too; a frame-era Go refuses it (it reads only frames), and the newest
    /// block-era build under test, when there is one, reads the frames-then-blocks chain.
    fn rust_resumes(build: &GoBuild) {
        let name = build.name();
        let bin = build.binary().expect("the build under test is present");
        let config = std::env::temp_dir().join(format!("lsr-go-{name}-{}.yml", std::process::id()));
        let (site, go_tip, at_stop) = go_writes_and_stops(build, &config);
        let site_config = site.dir().join("go-restore.yml");
        write_config(&site_config, &site.db, &site.replica).expect("config");
        drop(fs::remove_file(&config));
        let mut chain = Chain::start(site, real_now_ms().expect("clock")).expect("start");
        for _ in 0..5 {
            chain.commit(Shape::Insert { bytes: 700 }).expect("commit");
        }
        let written = chain.finish().expect("finish");
        let boot = written
            .events
            .iter()
            .find(|e| matches!(e, Event::Booted { .. }))
            .ok_or_else(|| format!("{name}: Rust never booted: {:?}", written.events))
            .expect("a Booted event");
        println!("{name} ({:?}): Rust boot {boot:?}", build.era());
        let want = &written.points.last().expect("points").page_hashes;
        rust_restores_combined(&written, RestoreTarget::Latest, want, "latest");
        rust_restores_combined(
            &written,
            RestoreTarget::Txid(Txid::new(go_tip).expect("txid")),
            &at_stop,
            "mid",
        );
        match build.era() {
            Era::Blocks => go_reads_combined(&bin, &site_config, &written, go_tip, &at_stop, "Go"),
            Era::Frames => {
                let out = written.site.dir().join("refused.db");
                let result =
                    restore_with_go(&bin, &site_config, &written.site.db, &out, Target::Latest)
                        .expect("go runs");
                assert!(
                    matches!(result, GoRestore::Refused { .. }),
                    "{name} read the combined chain: {result:?}"
                );
                match builds_in(Era::Blocks).pop() {
                    Some(newest) => {
                        let newest_bin = newest.binary().expect("the build under test is present");
                        go_reads_combined(
                            &newest_bin,
                            &site_config,
                            &written,
                            go_tip,
                            &at_stop,
                            newest.name(),
                        );
                    }
                    None => println!(
                        "{name}: no block-era build under test; Rust's restore is the assertion"
                    ),
                }
            }
        }
    }

    /// `bin` restores the combined chain at Latest and at the txid where Go stopped.
    fn go_reads_combined(
        bin: &Path,
        config: &Path,
        written: &Written,
        go_tip: u64,
        at_stop: &[String],
        who: &str,
    ) {
        let want = &written.points.last().expect("points").page_hashes;
        let latest = restore_go(bin, config, &written.site, "latest.db", Target::Latest);
        assert_eq!(&pages_of(&latest), want, "{who} on the combined chain");
        let mid = restore_go(
            bin,
            config,
            &written.site,
            "mid.db",
            Target::Txid { txid: go_tip },
        );
        assert_eq!(
            pages_of(&mid),
            at_stop,
            "{who} at the txid where Go stopped"
        );
    }
}
