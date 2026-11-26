//! A compaction larger than a tick's bytes goes to the replica a slice a tick, and the engine
//! syncs and uploads the commits made meanwhile.

use std::num::NonZeroU64;

use super::*;

/// A tick's bytes: the level-1 compaction below takes many ticks.
const SLICE: u64 = 16 * 1024;

/// The database's size in pages, every one of them in one level-0 file: more than one read of
/// the merge's input stream holds.
const PAGES: u32 = 3_000;

/// Bytes that do not compress.
fn noise(seed: u32) -> Vec<u8> {
    let mut x = 0x9E37_79B9_7F4A_7C15_u64 ^ u64::from(seed);
    (0..PS.bytes())
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x.to_le_bytes()[0]
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

/// An engine whose replica holds its first snapshot and a level-0 file of [`PAGES`] pages that
/// do not compress, which level 1 compacts at the next window.
fn before_a_long_compaction() -> (Rig, Actor) {
    let mut config = cfg();
    config.upload.bytes_per_tick = NonZeroU64::new(SLICE).unwrap();
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    rig.follow(&mut actor, T0, 20);
    assert!(newest(&rig, Level::SNAPSHOT).is_some());
    let pages: Vec<_> = (1..=PAGES).map(|n| (pg(n), noise(n))).collect();
    rig.wal.append_tx(&pages, nz(PAGES));
    rig.settle(&mut actor, T0 + 1);
    assert_eq!(newest(&rig, Level::ZERO), actor.staged().map(Txid::get));
    assert_eq!(newest(&rig, Level::ONE), None);
    (rig, actor)
}

fn compacting(actor: &Actor) -> bool {
    matches!(&actor.engine.sliced, Some((Job::Compact(_), _)))
}

/// The compaction is part-way when a tick ends with its upload open: a commit made then is
/// synced and uploaded between two of its slices, and is on the replica before the compaction's
/// output is. The output covers only the files listed before the commit.
#[test]
fn a_commit_made_during_a_long_compaction_reaches_the_replica_before_it_ends() {
    let (mut rig, mut actor) = before_a_long_compaction();
    let now = T0 + 60_000;
    let mut committed = None;
    let mut first = false;
    let mut slices = 0;
    for _ in 0..2_000 {
        let tick = rig.tick(&mut actor, now);
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
        if newest(&rig, Level::ONE).is_some() {
            break;
        }
        slices += usize::from(compacting(&actor));
        if committed.is_none() && compacting(&actor) && actor.engine.in_flight.sent().is_none() {
            rig.wal
                .append_tx(&[(pg(PAGES), vec![0x42; PS.bytes()])], nz(PAGES));
            committed = actor.staged().map(|txid| txid.get() + 1);
        }
        if committed.is_some() && newest(&rig, Level::ZERO) == committed {
            first = true;
        }
    }
    let committed = committed.expect("no tick ended with the compaction part-way");
    assert!(slices > 8, "{slices} ticks with the compaction part-way");
    assert!(
        first,
        "the commit reached the replica only after the compaction"
    );
    let output = newest(&rig, Level::ONE).expect("the compaction never ended");
    assert!(
        output < committed,
        "the output takes in only what was listed"
    );
    assert_eq!(rig.remote.inner.scratch(), 0, "the upload was committed");
    assert_eq!(restored_page(&rig, PAGES), vec![0x42; PS.bytes()]);
    assert_eq!(restored_page(&rig, 3), noise(3));
}
/// Ticks at `now` until the compaction is part-way.
fn part_way(rig: &mut Rig, actor: &mut Actor, now: i64) {
    for _ in 0..100 {
        rig.tick(actor, now);
        if compacting(actor) {
            return;
        }
    }
    panic!("the compaction never began");
}

/// Ticks at `now` until level 1 holds a file, at most `ticks` times.
fn until_compacted(rig: &mut Rig, actor: &mut Actor, now: i64, ticks: usize) -> Option<u64> {
    for _ in 0..ticks {
        if let Some(output) = newest(rig, Level::ONE) {
            return Some(output);
        }
        rig.tick(actor, now);
    }
    newest(rig, Level::ONE)
}

/// A drain runs no maintenance: it ends the compaction part-way and aborts its upload, and
/// nothing of it is on the replica.
#[test]
fn a_drain_ends_the_compaction_part_way() {
    let (mut rig, mut actor) = before_a_long_compaction();
    part_way(&mut rig, &mut actor, T0 + 60_000);
    actor.begin_shutdown(ts(T0 + 70_000));
    let ticks = rig.settle(&mut actor, T0 + 60_000);
    assert!(
        matches!(ticks.last(), Some(Tick::Drained { .. })),
        "{ticks:?}"
    );
    assert!(actor.engine.sliced.is_none());
    assert_eq!(newest(&rig, Level::ONE), None);
    assert_eq!(rig.remote.inner.scratch(), 0, "the upload was aborted");
}

/// A slice that fails ends the compaction, its upload aborted; the job runs again from its
/// first page once its level's wait is over.
#[test]
fn a_failed_slice_ends_the_compaction_and_its_retry_starts_again() {
    let (mut rig, mut actor) = before_a_long_compaction();
    let now = T0 + 60_000;
    part_way(&mut rig, &mut actor, now);
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let mut failed = false;
    for _ in 0..50 {
        let tick = rig.tick(&mut actor, now);
        if matches!(event(&tick), Some(Event::Retrying { .. })) {
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert!(actor.engine.sliced.is_none());
    assert_eq!(rig.remote.inner.scratch(), 0, "the upload was aborted");
    assert_eq!(newest(&rig, Level::ONE), None);
    assert!(
        until_compacted(&mut rig, &mut actor, now + 120_000, 2_000).is_some(),
        "the compaction was never retried"
    );
}

/// A compaction whose upload waits for its parts hands nothing over and is no work to tick
/// for; once the parts land the next tick reads on, and the output is written.
#[test]
fn a_compaction_whose_upload_waits_for_its_parts_is_no_slice() {
    let (mut rig, mut actor) = before_a_long_compaction();
    let now = T0 + 60_000;
    part_way(&mut rig, &mut actor, now);
    let sent = actor.engine.sliced.as_ref().map(|(_, s)| s.sent());
    rig.remote.flights.lock().unwrap().extend([Flight::Full; 3]);
    let tick = rig.tick(&mut actor, now);
    assert!(actor.awaits_parts(), "{tick:?}");
    assert_ne!(next_of(&tick), Some(Next::Now), "{tick:?}");
    assert_eq!(actor.engine.sliced.as_ref().map(|(_, s)| s.sent()), sent);
    assert!(
        until_compacted(&mut rig, &mut actor, now, 2_000).is_some(),
        "a compaction that waited for its parts was never written"
    );
}

/// A compaction holds no read of the database, so the engine's checkpoints run while it is
/// part-way, as Go's do.
#[test]
fn checkpoints_run_while_a_compaction_is_part_way() {
    let (mut rig, mut actor) = before_a_long_compaction();
    actor.kept.cfg.checkpoint.min_pages = Some(nz(1));
    let now = T0 + 60_000;
    part_way(&mut rig, &mut actor, now);
    let before = rig.ck.calls().len();
    rig.wal
        .append_tx(&[(pg(PAGES), vec![0x42; PS.bytes()])], nz(PAGES));
    let mut checkpointed = false;
    for _ in 0..50 {
        let tick = rig.tick(&mut actor, now);
        checkpointed |= matches!(event(&tick), Some(Event::Checkpointed { .. }));
        if checkpointed || !compacting(&actor) {
            break;
        }
    }
    assert!(
        checkpointed,
        "no checkpoint while the compaction was part-way"
    );
    assert!(compacting(&actor), "the compaction ended first");
    assert!(rig.ck.calls().len() > before);
}

/// The last slice hands over the file's tail and then finds parts still in flight: it waits
/// to publish, but it handed bytes over, so it is the tick's unit and nothing else runs in it.
#[test]
fn a_slice_that_hands_bytes_over_before_it_waits_is_the_ticks_unit() {
    let (mut rig, mut actor) = before_a_long_compaction();
    let now = T0 + 60_000;
    part_way(&mut rig, &mut actor, now);
    let mut seen = false;
    for _ in 0..2_000 {
        if newest(&rig, Level::ONE).is_some() {
            break;
        }
        let before = actor.engine.sliced.as_ref().map(|(_, s)| s.sent());
        // The slice's first question about its parts finds room; any after it, a part.
        rig.remote
            .flights
            .lock()
            .unwrap()
            .extend([Flight::Idle, Flight::Full]);
        let tick = rig.tick(&mut actor, now);
        rig.remote.flights.lock().unwrap().clear();
        let Some((_, sliced)) = actor.engine.sliced.as_ref() else {
            continue;
        };
        if sliced.waits() && Some(sliced.sent()) != before {
            assert_eq!(
                event(&tick),
                Some(&Event::Sending {
                    bytes: sliced.sent()
                }),
                "{tick:?}"
            );
            seen = true;
        }
    }
    assert!(seen, "no slice waited after handing bytes over");
    assert!(
        newest(&rig, Level::ONE).is_some(),
        "the compaction never ended"
    );
}

/// A merge that reads staged copies keeps them: the staging prune waits until it ends.
#[test]
fn the_staging_prune_waits_for_a_merge_reading_staged_copies() {
    let (mut rig, mut actor) = before_a_long_compaction();
    let now = T0 + 60_000;
    part_way(&mut rig, &mut actor, now);
    let reads_staging = |actor: &Actor| matches!(&actor.engine.sliced, Some((_, s)) if matches!(&**s, Sliced::Merge(m) if m.reads_staging()));
    assert!(reads_staging(&actor));
    // As if level 1 already held the newest staged file and no prune had run since: the files
    // below it may go, but for the merge reading them.
    actor.engine.maintain.level_one = actor.staged();
    actor.engine.pruned = None;
    assert_eq!(actor.prune_ready(ts(now)), None);
    block_on(actor.end_sliced());
    assert!(actor.prune_ready(ts(now)).is_some());
}

/// A staged copy that goes missing part-way starts the merge again from the replica's copies,
/// its first upload aborted; the output is written all the same.
#[test]
fn a_staged_copy_lost_part_way_starts_the_merge_again_from_the_replica() {
    let (mut rig, mut actor) = before_a_long_compaction();
    let now = T0 + 60_000;
    part_way(&mut rig, &mut actor, now);
    rig.tick(&mut actor, now);
    let reads_staging = |actor: &Actor| matches!(&actor.engine.sliced, Some((_, s)) if matches!(&**s, Sliced::Merge(m) if m.reads_staging()));
    assert!(reads_staging(&actor));
    let sent = actor.engine.sliced.as_ref().map(|(_, s)| s.sent());
    assert!(sent > Some(0));
    let key = Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key)
        .max()
        .unwrap();
    rig.staging
        .inner
        .inject_failure(StoreOp::Read, StoreError::NotFound { key });
    // The slices hand over what the merge holds before the next read meets the loss.
    for _ in 0..100 {
        rig.tick(&mut actor, now);
        if !reads_staging(&actor) {
            break;
        }
    }
    assert!(compacting(&actor), "the merge ended");
    assert!(!reads_staging(&actor));
    assert!(actor.engine.sliced.as_ref().map(|(_, s)| s.sent()) < sent);
    assert!(until_compacted(&mut rig, &mut actor, now, 2_000).is_some());
    assert_eq!(
        rig.remote.inner.scratch(),
        0,
        "the first upload was aborted"
    );
    assert_eq!(restored_page(&rig, 3), noise(3));
}
