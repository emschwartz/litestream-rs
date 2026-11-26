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
        if matches!(event(&tick), Some(Event::Sending { .. })) && actor.engine.snapshot.is_some() {
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
    assert!(actor.engine.snapshot.is_none());
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
    assert!(actor.engine.snapshot.is_none());
    assert!(!actor.engine.pages.pinned());
    assert_eq!(newest(&rig, Level::SNAPSHOT), None);
}

/// A snapshot whose upload waits for its parts hands nothing over and is no work to tick for;
/// the engine says it waits for parts.
#[test]
fn a_snapshot_whose_upload_waits_for_its_parts_is_no_slice() {
    let (mut rig, mut actor) = before_a_long_snapshot();
    part_way(&mut rig, &mut actor);
    let sent = actor.engine.snapshot.as_ref().map(|(_, s)| s.sent);
    rig.remote.flights.lock().unwrap().extend([Flight::Full; 3]);
    let tick = rig.tick(&mut actor, T0);
    assert!(actor.awaits_parts(), "{tick:?}");
    assert_ne!(next_of(&tick), Some(Next::Now), "{tick:?}");
    assert_eq!(actor.engine.snapshot.as_ref().map(|(_, s)| s.sent), sent);
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
        match event(&rig.tick(&mut actor, T0)) {
            Some(Event::Sending { .. }) => slices += 1,
            Some(Event::Synced { .. }) => break,
            other => panic!("{other:?}"),
        }
    }
    assert!(slices <= super::super::state::FAIR_SHARE, "{slices}");
    actor.engine.snapshot = None;
    rig.tick(&mut actor, T0);
}
