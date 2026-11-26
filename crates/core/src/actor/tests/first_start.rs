//! The first start on an empty replica reads the database once: the level-9 snapshot is built
//! from the level-0 file the start just wrote, not from a second pass over the database file
//! (a database of tens of gigabytes beside a live application would be read twice).

use super::*;
use crate::layout::CreatedAt;

/// A database of this many pages, all in the file but page 1, which one commit rewrote in
/// the WAL: the engine reads every other page from the file.
const PAGES: u32 = 40;

fn page_bytes(page: u32, commit: u8) -> Vec<u8> {
    let fill = if page == 1 {
        commit
    } else {
        u8::try_from(page).unwrap()
    };
    vec![fill; PS.bytes()]
}

/// A quiet database of [`PAGES`] pages with one committed transaction in its WAL.
fn quiet() -> Rig {
    quiet_of(PAGES, page_bytes)
}

/// A quiet database of `pages` pages, page `n` holding `fill(n, commit)`, with one committed
/// transaction in its WAL that rewrites page 1.
fn quiet_of(pages: u32, fill: impl Fn(u32, u8) -> Vec<u8>) -> Rig {
    let wal = FakeWal::new(
        PS,
        Salts {
            salt1: 10,
            salt2: 20,
        },
    );
    let mut db = FakeDb::new(PS);
    db.set_page_count(pages);
    for n in 1..=pages {
        db.write_page(pg(n), &fill(n, 0));
    }
    db.link(&wal);
    let ck = FakeCheckpointer::new(wal.clone());
    let rig = Rig {
        wal,
        db,
        ck,
        staging: Counting::default(),
        remote: Counting::default(),
        takeover: None,
        announced: Vec::new(),
        short: Vec::new(),
    };
    rig.wal.append_tx(&[(pg(1), fill(1, 1))], nz(pages));
    rig
}

fn bytes_at(rig: &Rig, level: Level) -> u64 {
    Rig::files(&rig.remote, level).iter().map(|f| f.size).sum()
}

/// What a restore to `target` leaves, every page.
fn restored_pages(store: &MemStore, target: RestoreTarget) -> Vec<Vec<u8>> {
    let sink = MemSink::new();
    block_on(restore(store, target, sink.clone())).unwrap();
    let database = sink.published().unwrap();
    (1..=PAGES).map(|n| database.page(n).unwrap()).collect()
}

fn expected(commit: u8) -> Vec<Vec<u8>> {
    (1..=PAGES).map(|n| page_bytes(n, commit)).collect()
}

/// Every page the database file holds is read once for the first start, and the replica holds
/// the database twice at most: once at level 0, once at level 9.
#[test]
fn the_first_start_reads_each_page_once() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    let level_9 = Rig::files(&rig.remote, Level::SNAPSHOT);
    let [snapshot] = level_9.as_slice() else {
        panic!("one snapshot expected: {level_9:?}");
    };
    assert_eq!(snapshot.key.range().min().get(), 1);
    assert_eq!(snapshot.key.range().max().get(), 1);
    assert_eq!(page_count(&rig, snapshot), PAGES);
    assert_eq!(
        rig.db.reads(),
        u64::from(PAGES - 1),
        "pages the file supplied (page 1 is in the WAL): one read each"
    );
    // The level-0 file and the snapshot are the only uploads: the database's pages twice.
    let uploaded = bytes_at(&rig, Level::ZERO) + bytes_at(&rig, Level::SNAPSHOT);
    let held: u64 = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .chain(&level_9)
        .map(|f| f.size)
        .sum();
    assert_eq!(uploaded, held);
    assert_eq!(Rig::files(&rig.remote, Level::ZERO).len(), 1);
}

/// A first start reads nothing of the snapshot's key before it writes it, as Go reads nothing:
/// the listing it planned from shows every file this engine wrote, so the key holds nothing.
/// Before, a one-byte read of the key (a not-found GET on S3) went out ahead of every merge.
#[test]
fn the_first_start_sends_no_read_ahead_of_the_snapshot() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    let snapshot = FileKey::snapshot(Txid::new(1).unwrap());
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT).len(), 1);
    assert_eq!(rig.remote.reads_of(&snapshot), 0);
}

/// A first start moves the database twice, to level 0 and to level 9: the levels between hold
/// nothing (they start above the snapshot), and the only download is the snapshot's own check.
/// Before, the level-0 file was carried into levels 1, 2 and 3 (five uploads, two downloads).
#[test]
fn the_first_start_uploads_the_database_twice_and_downloads_none() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    for level in [1, 2, 3] {
        let files = Rig::files(&rig.remote, Level::new(level).unwrap());
        assert!(files.is_empty(), "level {level} holds {files:?}");
    }
    let held: u64 = (0..=9)
        .filter_map(Level::new)
        .flat_map(|level| Rig::files(&rig.remote, level))
        .map(|f| f.size)
        .sum();
    assert_eq!(
        held,
        bytes_at(&rig, Level::ZERO) + bytes_at(&rig, Level::SNAPSHOT),
        "every uploaded byte is at level 0 or 9"
    );
    // The one read is the snapshot's check after it was written.
    let reads = rig.remote.reads.lock().unwrap().clone();
    assert!(
        reads.iter().all(|key| key.level().is_snapshot()),
        "downloaded {reads:?}"
    );
}

/// A database whose first file takes several slices to send: pages no codec shrinks.
const BIG_PAGES: u32 = 24_000;

/// What a slice takes to send, as the clock sees it: a real replica is slower than a
/// memory store by this much, and the levels' windows open meanwhile.
const SLICE_MILLIS: i64 = 4_000;

fn noisy(page: u32) -> Vec<u8> {
    let mut state = u64::from(page).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..PS.bytes())
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// A database too big for one slice, with one committed transaction in its WAL.
fn big() -> Rig {
    quiet_of(BIG_PAGES, |n, _| noisy(n))
}

/// While the first snapshot is planned, pending or in flight, no level gains a file: the
/// database is carried to level 9 once, not through levels 1 to 3 on the way. A first file
/// that takes several slices to send keeps the snapshot waiting for the uploads, and the
/// levels wait with it.
#[test]
fn a_first_file_sent_in_slices_does_not_let_the_levels_run() {
    let mut rig = big();
    let mut actor = Actor::new(ActorConfig {
        upload: UploadConfig {
            bytes_per_tick: std::num::NonZeroU64::new(256 * 1024).unwrap(),
            ..UploadConfig::default()
        },
        ..cfg()
    });
    let mut now = T0;
    for _ in 0..400 {
        let tick = rig.tick(&mut actor, now);
        for level in [1, 2, 3] {
            let files = Rig::files(&rig.remote, Level::new(level).unwrap());
            assert!(
                files.is_empty(),
                "level {level} holds {files:?} after {tick:?}"
            );
        }
        match tick {
            Tick::Ran {
                next: Next::Now, ..
            } => now += SLICE_MILLIS,
            Tick::Ran {
                next: Next::At(at), ..
            } => now = (now + SLICE_MILLIS).max(at.unix_millis()),
            Tick::Ran {
                next: Next::OnWalChange,
                ..
            }
            | Tick::Stopped(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_) => break,
        }
    }
    assert!(!Rig::files(&rig.remote, Level::SNAPSHOT).is_empty());
}

/// The first snapshot starts when the first level-0 file is confirmed, not at the next
/// window of some level: a send that took many ticks already let a listing see the empty
/// replica, and the clock stands still from the confirmation on.
#[test]
fn the_first_snapshot_starts_as_the_first_level_zero_file_is_confirmed() {
    let mut rig = big();
    let mut actor = Actor::new(ActorConfig {
        upload: UploadConfig {
            bytes_per_tick: std::num::NonZeroU64::new(256 * 1024).unwrap(),
            ..UploadConfig::default()
        },
        ..cfg()
    });
    let mut now = T0;
    for _ in 0..400 {
        rig.tick(&mut actor, now);
        if !Rig::files(&rig.remote, Level::ZERO).is_empty() {
            break;
        }
        now += SLICE_MILLIS;
    }
    assert!(!Rig::files(&rig.remote, Level::ZERO).is_empty());
    rig.settle(&mut actor, now);
    assert!(
        !Rig::files(&rig.remote, Level::SNAPSHOT).is_empty(),
        "no snapshot at the instant level 0 was confirmed"
    );
}

/// A config that sends a quarter of a megabyte a tick.
fn slow() -> ActorConfig {
    ActorConfig {
        upload: UploadConfig {
            bytes_per_tick: std::num::NonZeroU64::new(256 * 1024).unwrap(),
            ..UploadConfig::default()
        },
        ..cfg()
    }
}

/// Ticks until the actor waits on the WAL or the clock reaches `until`, the clock moving [`SLICE_MILLIS`] a tick, and
/// returns the levels of the objects the replica gained, in the order each first appeared.
fn written(
    rig: &mut Rig,
    actor: &mut Actor,
    now: &mut i64,
    until: i64,
    after_tick: &mut dyn FnMut(&Rig),
) -> Vec<u8> {
    let mut seen: Vec<FileKey> = (0..=9)
        .filter_map(Level::new)
        .flat_map(|level| Rig::files(&rig.remote, level))
        .map(|file| file.key)
        .collect();
    let mut order = Vec::new();
    for _ in 0..600 {
        if *now >= until {
            break;
        }
        let tick = rig.tick(actor, *now);
        after_tick(rig);
        for level in (0..=9).filter_map(Level::new) {
            for file in Rig::files(&rig.remote, level) {
                if !seen.contains(&file.key) {
                    seen.push(file.key);
                    order.push(level.get());
                }
            }
        }
        match tick {
            Tick::Ran {
                next: Next::Now, ..
            } => *now = now.saturating_add(SLICE_MILLIS),
            Tick::Ran {
                next: Next::At(at), ..
            } => *now = now.saturating_add(SLICE_MILLIS).max(at.unix_millis()),
            Tick::Ran {
                next: Next::OnWalChange,
                ..
            }
            | Tick::Stopped(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_) => break,
        }
    }
    order
}

/// A restart on a replica that already holds a snapshot and files (a database restored from
/// it, with no staging) and a large new transaction sent in slices: the rule that holds the
/// levels back applies only while there is no snapshot, so the new file goes through levels 1
/// to 3 as in steady state (above the snapshot's end, as an empty level starts), and no second
/// snapshot is written before the schedule's next one.
#[test]
fn a_first_start_after_a_restore_does_not_hold_the_levels_or_write_another_snapshot() {
    let mut rig = quiet();
    let mut actor = Actor::new(slow());
    let mut now = T0;
    let first = written(&mut rig, &mut actor, &mut now, i64::MAX, &mut |_| {});
    assert_eq!(first, vec![0, 9], "the replica the restore starts from");
    // The restart: no staging, a new engine, and a transaction too big for one slice.
    rig.staging = Counting::default();
    let pages: Vec<_> = (2..=4000).map(|n| (pg(n), noisy(n))).collect();
    rig.wal.append_tx(&pages, nz(4000));
    let mut actor = Actor::new(slow());
    now += 60_000;
    let hour = now + 3_600_000;
    let after = written(&mut rig, &mut actor, &mut now, hour, &mut |_| {});
    assert_eq!(after, vec![0, 1, 2, 3]);
    for level in [1, 2, 3] {
        let files = Rig::files(&rig.remote, Level::new(level).unwrap());
        let ranges: Vec<_> = files
            .iter()
            .map(|f| (f.key.range().min().get(), f.key.range().max().get()))
            .collect();
        assert_eq!(
            ranges,
            vec![(2, 2)],
            "level {level} starts above the snapshot"
        );
    }
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT).len(), 1);
}

/// A first snapshot the store refuses once: it backs off, the levels run meanwhile (a failing
/// snapshot never stops compaction), and the snapshot is written when its backoff ends, after
/// the levels.
#[test]
fn a_refused_first_snapshot_lets_the_levels_run_and_is_retried() {
    let mut rig = big();
    let mut actor = Actor::new(slow());
    let mut now = T0;
    let mut refused = false;
    let order = written(&mut rig, &mut actor, &mut now, i64::MAX, &mut |rig| {
        if !refused && !Rig::files(&rig.remote, Level::ZERO).is_empty() {
            rig.remote
                .inner
                .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
            refused = true;
        }
    });
    assert_eq!(order, vec![0, 1, 2, 3, 9]);
    let snapshots = Rig::files(&rig.remote, Level::SNAPSHOT);
    let [snapshot] = snapshots.as_slice() else {
        panic!("one snapshot expected: {snapshots:?}");
    };
    assert_eq!(snapshot.key.range().max().get(), 1);
}

/// The pages the file holds, by its own trailer.
fn page_count(rig: &Rig, info: &FileInfo) -> u32 {
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bytes = block_on(rig.remote.inner.read(&info.key, 0, all)).unwrap();
    let mut decoder = ReadDecoder::new(&bytes[..]);
    while decoder.next_event().unwrap().is_some() {}
    u32::try_from(decoder.finish().unwrap().page_count()).unwrap()
}

/// A snapshot built from level 0 that cannot be written leaves the first snapshot to be read
/// from the database: the replica still gets one.
#[test]
fn a_failed_first_snapshot_from_level_zero_is_read_from_the_database_next() {
    let (mut rig, mut actor) = super::rejected_snapshot::before_first_snapshot();
    let reads = rig.db.reads();
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let seen = rig.follow(&mut actor, T0, 200);
    let level_9 = Rig::files(&rig.remote, Level::SNAPSHOT);
    assert!(!level_9.is_empty(), "no snapshot followed: {seen:?}");
    assert_eq!(
        rig.db.reads().saturating_sub(reads),
        2,
        "the second try read pages 2 and 3, the two the file supplies"
    );
}

/// The snapshot built from the level-0 file restores to the database: at the latest state, at
/// its transaction, at an instant, and alone (the level-0 file gone).
#[test]
fn the_snapshot_of_a_first_start_restores_the_database() {
    let mut rig = quiet();
    let mut actor = Actor::new(cfg());
    rig.settle(&mut actor, T0);
    rig.wal.append_tx(&[(pg(1), page_bytes(1, 2))], nz(PAGES));
    rig.follow(&mut actor, T0 + 1000, 20);
    let first = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .find(|f| f.key.range().max().get() == 1)
        .unwrap();
    let second = Rig::files(&rig.remote, Level::ZERO)
        .into_iter()
        .find(|f| f.key.range().max().get() == 2)
        .unwrap();
    let store = &rig.remote.inner;
    assert!(restored_pages(store, RestoreTarget::Latest) == expected(2));
    assert!(restored_pages(store, RestoreTarget::Txid(first.key.range().max())) == expected(1));
    let instant: CreatedAt = second.created_at().unwrap();
    assert!(restored_pages(store, RestoreTarget::AsOf(instant.at())) == expected(1));
    // The snapshot alone: a replica holding nothing else restores the first state.
    let others: Vec<FileKey> = Level::ALL
        .iter()
        .flat_map(|level| Rig::files(&rig.remote, *level))
        .filter(|f| !f.key.level().is_snapshot() || f.key.range().max().get() > 1)
        .map(|f| f.key)
        .collect();
    block_on(store.delete(&others)).unwrap();
    assert!(restored_pages(store, RestoreTarget::Latest) == expected(1));
}

/// The other full copies of the database file, on the three-page database of the rig: pages 2
/// and 3 come from the file when page 1 is in the WAL.
mod other_copies {
    use super::super::vouch::redeployed_with_a_new_meta_directory;
    use super::*;

    fn reads_after(rig: &mut Rig, run: impl FnOnce(&mut Rig)) -> u64 {
        let before = rig.db.reads();
        run(rig);
        rig.db.reads().saturating_sub(before)
    }

    /// A WAL that restarted under a running engine costs a snapshot (the next level-0 file
    /// holds every page): one read of the file.
    #[test]
    fn a_resnapshot_reads_each_page_once() {
        let (mut rig, mut actor) = caught_up();
        let reads = reads_after(&mut rig, |rig| {
            rig.commit(2);
            rig.wal.force_restart();
            let seen = rig.settle(&mut actor, T0 + 1);
            assert!(
                seen.iter()
                    .any(|tick| matches!(event(tick), Some(Event::Resnapshot(_)))),
                "{seen:?}"
            );
        });
        assert_eq!(reads, 3);
    }

    /// A scheduled verify reads the live pages once.
    #[test]
    fn a_scheduled_verify_reads_each_page_once() {
        let (mut rig, mut actor) = caught_up();
        let reads = reads_after(&mut rig, |rig| {
            actor.read_live_pages();
            rig.settle(&mut actor, T0 + 1);
        });
        assert!(actor.take_live_pages().is_some());
        assert_eq!(reads, 2);
    }

    /// A start with an empty working folder, after the WAL restarted, checks the database
    /// against the replica and then starts a chain above it: the check reads every page of the
    /// file (page 1 included: the WAL has none), and the snapshot that follows reads them
    /// again. The check also looks for the replicator's counter before it compares: one more
    /// read of page 1, the schema's root, which here holds no b-tree, so the walk ends there. A
    /// schema with more pages costs one read of each, never of the data pages. Only a start
    /// that runs the check (a fresh working folder over a replica it cannot account for) pays
    /// it; a start that continues from its own staging reads nothing extra.
    #[test]
    fn a_start_with_an_empty_working_folder_reads_each_page_twice_and_the_schema_once() {
        let mut rig = redeployed_with_a_new_meta_directory();
        let mut actor = Actor::new(go_like());
        let reads = reads_after(&mut rig, |rig| {
            rig.settle(&mut actor, T0 + 2);
        });
        let (pages, schema_reads) = (3, 1);
        assert_eq!(reads, 2 * pages + schema_reads);
    }
}
