//! A level-9 snapshot larger than a tick's bytes goes to the replica a slice a tick, and the
//! engine syncs and uploads the commits made meanwhile.

use std::num::NonZeroU64;

use super::rejected_snapshot::before_first_snapshot_with;
use super::*;

/// A tick's bytes: the snapshot of the database below takes some forty ticks, and the
/// encoder's buffer (256 KiB) holds a part of it only.
const SLICE: u64 = 16 * 1024;

/// The database's size in pages; the last two are read from the database file.
const PAGES: u32 = 1_202;

/// Bytes that do not compress.
fn noise(seed: u64) -> Vec<u8> {
    let mut x = 0x9E37_79B9_7F4A_7C15_u64 ^ seed;
    (0..PS.bytes())
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_be_bytes()[0]
        })
        .collect()
}

fn newest(rig: &Rig, level: Level) -> Option<u64> {
    Rig::files(&rig.remote, level)
        .iter()
        .map(|f| f.key.range().max().get())
        .max()
}

/// Page `n` of the latest state the replica restores to.
fn restored_page(rig: &Rig, n: u32) -> Vec<u8> {
    let sink = MemSink::new();
    block_on(restore(
        &rig.remote.inner,
        RestoreTarget::Latest,
        sink.clone(),
    ))
    .unwrap();
    sink.published().unwrap().page(n).unwrap().to_vec()
}

/// An engine whose next listing plans a snapshot read from the database, of a database of
/// [`PAGES`] pages: all but the last two from WAL frames that do not compress, those from the
/// database file. Every staged file is on the replica.
fn before_a_long_snapshot() -> (Rig, Actor) {
    let mut config = cfg();
    config.upload.bytes_per_tick = NonZeroU64::new(SLICE).unwrap();
    let (mut rig, mut actor) = before_first_snapshot_with(config);
    actor.engine.maintain.state.refuse_seed();
    rig.db.set_page_count(PAGES);
    for n in [PAGES - 1, PAGES] {
        rig.db.write_page(pg(n), &vec![0xD0; PS.bytes()]);
    }
    let pages: Vec<_> = (1..PAGES - 1)
        .map(|n| (pg(n), noise(u64::from(n))))
        .collect();
    rig.wal.append_tx(&pages, nz(PAGES));
    let staged = actor.staged().map(Txid::get);
    for _ in 0..200 {
        let newer = staged.is_some() && actor.staged().map(Txid::get) > staged;
        if newer && newest(&rig, Level::ZERO) == actor.staged().map(Txid::get) {
            return (rig, actor);
        }
        rig.tick(&mut actor, T0);
    }
    panic!("the database's pages never reached the replica");
}

/// The snapshot is part-way when a tick ends sending with no staged file in flight: a commit
/// made then is synced and uploaded between two of the snapshot's slices,
/// and is on the replica before the snapshot is. The last page of that commit is one the snapshot
/// reads from the database file: the sync records its new checksum, and the snapshot still
/// checks the file's older page against the one the table held when it began.
#[test]
fn a_commit_made_during_a_long_snapshot_reaches_the_replica_before_it_ends() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    assert_eq!(newest(&rig, Level::SNAPSHOT), None);
    let mut committed = None;
    let mut first = false;
    for _ in 0..1_000 {
        let tick = rig.tick(&mut actor, T0);
        assert!(
            !matches!(
                tick,
                Tick::Ran {
                    event: Event::Retrying { .. } | Event::Degraded { .. },
                    ..
                }
            ),
            "{tick:?}"
        );
        if newest(&rig, Level::SNAPSHOT).is_some() {
            break;
        }
        let sending = matches!(event(&tick), Some(Event::Sending { .. }));
        if committed.is_none() && sending && actor.engine.in_flight.sent().is_none() {
            rig.wal
                .append_tx(&[(pg(PAGES), vec![0x42; PS.bytes()])], nz(PAGES));
            committed = actor.staged().map(|txid| txid.get() + 1);
        }
        if committed.is_some() && newest(&rig, Level::ZERO) == committed {
            first = true;
        }
    }
    let committed = committed.expect("no tick ended with the snapshot part-way");
    assert!(
        first,
        "the commit reached the replica only after the snapshot"
    );
    let snapshot = newest(&rig, Level::SNAPSHOT).expect("the snapshot was never written");
    assert!(
        snapshot < committed,
        "the snapshot is of the database before the commit"
    );
    assert_eq!(restored_page(&rig, PAGES), vec![0x42; PS.bytes()]);
    assert_eq!(restored_page(&rig, 3), noise(3));
}
/// Ticks until the snapshot is part-way: a tick that ends sending with no staged file in
/// flight.
fn part_way(rig: &mut Rig, actor: &mut Actor) {
    for _ in 0..50 {
        let tick = rig.tick(actor, T0);
        if matches!(event(&tick), Some(Event::Sending { .. })) && actor.engine.sliced.is_some() {
            return;
        }
    }
    panic!("the snapshot never began");
}

/// No checkpoint of the engine's runs under a snapshot part-way, however much it is owed: it
/// would end the read transaction the snapshot reads under. It runs once the snapshot is
/// published.
#[test]
fn no_checkpoint_runs_while_a_snapshot_is_part_way() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    actor.kept.cfg.checkpoint.min_pages = NonZeroU32::new(1);
    part_way(&mut rig, &mut actor);
    let calls = rig.ck.calls().len();
    rig.wal
        .append_tx(&[(pg(PAGES), vec![0x42; PS.bytes()])], nz(PAGES));
    for _ in 0..200 {
        if newest(&rig, Level::SNAPSHOT).is_some() {
            break;
        }
        rig.tick(&mut actor, T0);
        assert!(
            newest(&rig, Level::SNAPSHOT).is_some() || rig.ck.calls().len() == calls,
            "{:?}",
            rig.ck.calls()
        );
    }
    rig.settle(&mut actor, T0);
    assert!(
        rig.ck.calls().len() > calls,
        "the checkpoint owed runs after it"
    );
}

/// A drain runs no maintenance: the snapshot part-way ends and its upload is aborted.
#[test]
fn a_drain_ends_the_snapshot_part_way() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    actor.begin_shutdown(ts(T0 + 10_000));
    let ticks = rig.settle(&mut actor, T0);
    assert!(
        matches!(ticks.last(), Some(Tick::Drained { .. })),
        "{ticks:?}"
    );
    assert!(actor.engine.sliced.is_none());
    assert!(!actor.engine.pages.pinned());
    assert_eq!(newest(&rig, Level::SNAPSHOT), None);
}

/// A failed slice ends the snapshot and lets the pin go; the job is retried from its first
/// page on its own schedule.
#[test]
fn a_failed_slice_ends_the_snapshot_and_lets_the_pin_go() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let mut failed = false;
    for _ in 0..50 {
        let tick = rig.tick(&mut actor, T0);
        if matches!(event(&tick), Some(Event::Retrying { .. })) {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert!(actor.engine.sliced.is_none());
    assert!(!actor.engine.pages.pinned());
    assert_eq!(newest(&rig, Level::SNAPSHOT), None);
}

/// A snapshot whose upload waits for its parts hands nothing over and is no work to tick for;
/// the engine says it waits for parts. Once they land the next tick reads on, and the snapshot
/// is written.
#[test]
fn a_snapshot_whose_upload_waits_for_its_parts_is_no_slice() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    let sent = actor.engine.sliced.as_ref().map(|(_, s)| s.sent());
    rig.remote.flights.lock().unwrap().extend([Flight::Full; 3]);
    let tick = rig.tick(&mut actor, T0);
    assert!(actor.awaits_parts(), "{tick:?}");
    assert!(
        !matches!(event(&tick), Some(Event::Sending { .. })),
        "{tick:?}"
    );
    assert_ne!(next_of(&tick), Some(Next::Now), "{tick:?}");
    assert_eq!(actor.engine.sliced.as_ref().map(|(_, s)| s.sent()), sent);
    for _ in 0..200 {
        if newest(&rig, Level::SNAPSHOT).is_some() {
            break;
        }
        rig.tick(&mut actor, T0);
    }
    assert!(
        newest(&rig, Level::SNAPSHOT).is_some(),
        "a snapshot that waited for its parts was never written"
    );
}

/// With a commit waiting at every tick the snapshot's slices go first for exactly their share,
/// then the commit is synced, and so on until the snapshot is whole.
#[test]
fn a_snapshot_goes_first_for_exactly_its_share_while_commits_wait() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    let (mut run, mut longest, mut syncs) = (0_u32, 0_u32, 0_u32);
    for n in 0..30_u8 {
        rig.wal
            .append_tx(&[(pg(PAGES), vec![n; PS.bytes()])], nz(PAGES));
        match event(&rig.tick(&mut actor, T0)) {
            Some(Event::Sending { .. }) => run += 1,
            other => {
                syncs += u32::from(matches!(other, Some(Event::Synced { .. })));
                (longest, run) = (longest.max(run), 0);
            }
        }
    }
    assert!(
        actor.engine.sliced.is_some(),
        "the snapshot ended inside the window"
    );
    assert!(syncs >= 3, "{syncs}");
    assert_eq!(longest, super::super::state::FAIR_SHARE);
}

/// The snapshot's slices go first only for their share: after [`state::FAIR_SHARE`] of them a
/// commit made meanwhile is synced.
#[test]
fn a_snapshot_leaves_the_wal_step_a_tick_after_each_share_of_slices() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    rig.wal
        .append_tx(&[(pg(PAGES), vec![0x42; PS.bytes()])], nz(PAGES));
    let mut slices = 0;
    loop {
        assert!(
            slices <= super::super::state::FAIR_SHARE,
            "no sync came within {slices} slices of the snapshot"
        );
        match event(&rig.tick(&mut actor, T0)) {
            Some(Event::Sending { .. }) => slices += 1,
            Some(Event::Synced { .. }) => break,
            other => panic!("{other:?}"),
        }
    }
    assert!(slices <= super::super::state::FAIR_SHARE, "{slices}");
    actor.engine.sliced = None;
    rig.tick(&mut actor, T0);
}

/// After a boot the read-back of the newest file of each level reads the snapshot a slice a
/// tick, never all of it in one: each tick reads at most a tick's bytes and one read past them.
/// A commit made while it is part-way reaches the replica before it ends.
#[test]
fn a_boot_reads_back_a_long_snapshot_a_slice_a_tick() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    for _ in 0..1_000 {
        if newest(&rig, Level::SNAPSHOT).is_some() {
            break;
        }
        rig.tick(&mut actor, T0);
    }
    let snapshot = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    assert!(snapshot.size > 20 * SLICE, "{snapshot:?}");
    let mut config = cfg();
    config.upload.bytes_per_tick = NonZeroU64::new(SLICE).unwrap();
    let mut actor = Actor::new(config);
    let reads = rig.remote.reads_of(&snapshot.key);
    let (mut slices, mut committed, mut first) = (0, None, false);
    for _ in 0..2_000 {
        let before = rig.remote.read_bytes();
        let tick = rig.tick(&mut actor, T0 + 1);
        let read = rig.remote.read_bytes() - before;
        // The boot's own tick compares the newest staged file with the replica's copy, whole.
        if !actor.booted() || matches!(event(&tick), Some(Event::Booted { .. })) {
            continue;
        }
        assert!(
            read <= SLICE + 256 * 1024,
            "{read} bytes in one tick: {tick:?}"
        );
        if !actor.engine.maintain.read_back_owed {
            break;
        }
        if matches!(
            event(&tick),
            Some(Event::Maintained {
                event: MaintainedEvent::ReadBack,
                ..
            })
        ) {
            slices += 1;
        }
        if committed.is_none() && slices == 1 {
            rig.wal
                .append_tx(&[(pg(PAGES), vec![0x42; PS.bytes()])], nz(PAGES));
            committed = actor.staged().map(|txid| txid.get() + 1);
        }
        if committed.is_some() && newest(&rig, Level::ZERO) == committed {
            first = true;
        }
    }
    assert!(
        !actor.engine.maintain.read_back_owed,
        "the read-back never ended"
    );
    assert_eq!(
        rig.remote.reads_of(&snapshot.key),
        reads + 1,
        "one stream of the snapshot"
    );
    assert!(slices >= 2, "{slices} slices");
    assert!(
        committed.is_some() && first,
        "the commit waited for the read-back"
    );
}

/// A file of many slices put in flight while the snapshot is part-way: the WAL step has staged
/// it, and the next tick sends its first slice.
fn file_in_flight_beside_the_snapshot(rig: &mut Rig, actor: &mut Actor) {
    let pages: Vec<_> = (3..403)
        .map(|n| (pg(n), noise(u64::from(n) << 20)))
        .collect();
    rig.wal.append_tx(&pages, nz(PAGES));
    for _ in 0..50 {
        rig.tick(actor, T0);
        if actor.engine.in_flight.sent().is_some() && actor.engine.sliced.is_some() {
            return;
        }
    }
    panic!("the file never went in flight beside the snapshot");
}

/// A file in flight and a snapshot part-way share one count of slices: with a commit waiting at
/// every tick, at most [`state::FAIR_SHARE`] ticks in a row end in a slice of either before the
/// WAL step syncs. Before, the snapshot's share ended in the file's slice, which was counted
/// apart, and a commit waited behind both.
#[test]
fn a_commit_waits_at_most_a_share_of_slices_of_a_file_and_a_snapshot_together() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    file_in_flight_beside_the_snapshot(&mut rig, &mut actor);
    let (mut run, mut syncs) = (0_u32, 0_u32);
    for n in 0..60_u8 {
        if actor.engine.in_flight.sent().is_none() || actor.engine.sliced.is_none() {
            break;
        }
        rig.wal
            .append_tx(&[(pg(PAGES), vec![n; PS.bytes()])], nz(PAGES));
        let tick = rig.tick(&mut actor, T0);
        match event(&tick) {
            Some(Event::Sending { .. }) => run += 1,
            Some(Event::Synced { .. }) => (run, syncs) = (0, syncs + 1),
            Some(Event::Uploaded(_)) => run = 0,
            other => panic!("{other:?}"),
        }
        assert!(
            run <= super::super::state::FAIR_SHARE,
            "{run} slices in a row ahead of a waiting commit"
        );
    }
    assert!(syncs >= 2, "{syncs}");
}

/// Inside a share the file in flight and the snapshot take turns: neither waits for the other
/// to end, and the file's slices still go up while the snapshot is part-way.
#[test]
fn a_file_in_flight_and_a_snapshot_part_way_take_turns() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    file_in_flight_beside_the_snapshot(&mut rig, &mut actor);
    let job_sent = |actor: &Actor| actor.engine.sliced.as_ref().map(|(_, s)| s.sent());
    let mut turns = Vec::new();
    for _ in 0..12 {
        let (file, job) = (actor.engine.in_flight.sent(), job_sent(&actor));
        let tick = rig.tick(&mut actor, T0);
        assert!(
            matches!(event(&tick), Some(Event::Sending { .. })),
            "{tick:?}"
        );
        turns.push((
            actor.engine.in_flight.sent() != file,
            job_sent(&actor) != job,
        ));
    }
    // The file sent its first slice last; the snapshot has the next, and so on.
    let expected: Vec<_> = (0..12).map(|n| (n % 2 == 1, n % 2 == 0)).collect();
    assert_eq!(turns, expected);
}
