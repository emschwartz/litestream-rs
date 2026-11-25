//! A staged file larger than a tick's bytes goes up a slice a tick: what the ticks in between
//! do, and what a drain, a stop, a rebuild and a bad staged file make of the file in flight.

use std::num::NonZeroU64;

use super::*;
use crate::fault::BugSite;

/// A tick's bytes: a file's header alone takes two ticks, so the upload is begun by the second.
const SLICE: u64 = 64;

/// Commits page 1 filled with `n` and two pages that do not compress: a file of some twenty
/// slices.
fn commit_big(rig: &Rig, n: u8) {
    let mut x = 0x9E37_79B9_7F4A_7C15_u64 ^ u64::from(n);
    let mut noise = || -> Vec<u8> {
        (0..PS.bytes())
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_be_bytes()[0]
            })
            .collect()
    };
    let pages = [
        (pg(1), vec![n; PS.bytes()]),
        (pg(2), noise()),
        (pg(3), noise()),
    ];
    rig.wal.append_tx(&pages, nz(3));
}

fn slicing(bytes: u64) -> ActorConfig {
    let mut config = cfg();
    config.upload.bytes_per_tick = NonZeroU64::new(bytes).unwrap();
    config
}

fn caught_up_with(config: ActorConfig) -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    (rig, actor)
}

/// The staged size of transaction `n`.
fn staged_size(rig: &Rig, n: u64) -> u64 {
    Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .find(|f| f.key.range().max().get() == n)
        .unwrap()
        .size
}

/// Commits `n`, stages it, and sends its first two slices: an upload is open for it.
fn in_flight(rig: &mut Rig, actor: &mut Actor, n: u8, now: i64) {
    commit_big(rig, n);
    assert_eq!(
        event(&rig.tick(actor, now)),
        Some(&Event::Synced { files: 1 })
    );
    assert!(staged_size(rig, u64::from(n)) > 10 * SLICE);
    for bytes in [SLICE, SLICE.saturating_mul(2)] {
        assert_eq!(
            event(&rig.tick(actor, now)),
            Some(&Event::Sending { bytes })
        );
    }
    assert_eq!(open_uploads(rig), 0, "an upload in hand is not scratch");
}

fn newest_replica_file(rig: &Rig) -> Option<u64> {
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .max()
}

fn sent(actor: &Actor) -> Option<u64> {
    actor.engine.in_flight.sent()
}

/// Uploads the replica holds begun and neither committed nor aborted.
fn open_uploads(rig: &Rig) -> u64 {
    rig.remote.inner.scratch()
}

#[test]
fn a_file_larger_than_the_ticks_bytes_goes_up_a_slice_a_tick() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    commit_big(&rig, 2);
    rig.tick(&mut actor, T0 + 1);
    let size = staged_size(&rig, 2);
    assert!(size > 10 * SLICE);
    assert_eq!(newest_replica_file(&rig), Some(1));
    let mut slices = 0;
    let put = loop {
        let tick = rig.tick(&mut actor, T0 + 1);
        match event(&tick) {
            Some(Event::Sending { bytes }) => {
                slices += 1;
                assert_eq!(*bytes, slices * SLICE);
                assert_eq!(
                    next_of(&tick),
                    Some(Next::Now),
                    "the next slice is due at once"
                );
                assert_eq!(
                    newest_replica_file(&rig),
                    Some(1),
                    "nothing is published yet"
                );
                assert_eq!(actor.replicated(), Txid::new(1));
            }
            Some(Event::Uploaded(Did::Put(info))) => break *info,
            // Maintenance, on its turn.
            Some(Event::Maintained { .. }) => assert_eq!(sent(&actor), Some(slices * SLICE)),
            _ => panic!("{tick:?}"),
        }
    };
    assert_eq!(slices, size / SLICE);
    assert_eq!(put.size, size);
    assert_eq!(actor.replicated(), Txid::new(2));
    assert_eq!(sent(&actor), None);
    assert_eq!(open_uploads(&rig), 0);
    assert_eq!(restored(&rig), 2);
}

/// A file in flight goes first, and the WAL step has the tick after each `FAIR_SHARE` slices:
/// an application that commits before every tick has its commits staged within that many
/// slices, and the file takes the slices it takes with no commits at all.
#[test]
fn a_file_in_flight_leaves_the_wal_step_a_tick_after_each_share_of_slices() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    let size = staged_size(&rig, 2);
    // The helper sent two slices with no commit between them.
    let mut seen = vec![Tick::Stopped(StopCause::Shutdown); 0];
    let before = 2_u64;
    for n in 3..100_u8 {
        rig.commit(n);
        let tick = rig.tick(&mut actor, T0 + i64::from(n));
        let done = matches!(event(&tick), Some(Event::Uploaded(_)));
        seen.push(tick);
        if done {
            break;
        }
    }
    let synced = |t: &Tick| matches!(event(t), Some(Event::Synced { files: 1 }));
    let sliced = |t: &Tick| matches!(event(t), Some(Event::Sending { .. } | Event::Uploaded(_)));
    // Every tick is a slice but the one after each share of them, which stages the commits
    // made since the last in one file.
    let (mut in_a_row, mut slices, mut syncs) = (before, before, 0_usize);
    for tick in &seen {
        if in_a_row == u64::from(state::FAIR_SHARE) {
            assert!(synced(tick), "{seen:?}");
            (in_a_row, syncs) = (0, syncs.saturating_add(1));
        } else {
            assert!(sliced(tick), "{seen:?}");
            (in_a_row, slices) = (in_a_row.saturating_add(1), slices.saturating_add(1));
        }
    }
    assert!(syncs >= 2, "{seen:?}");
    assert_eq!(slices, size / SLICE + 1, "{seen:?}");
    assert_eq!(newest_replica_file(&rig), Some(2));
    assert_eq!(staged(&rig).len(), syncs.saturating_add(2));
}

/// Checkpoints are the WAL step's, so they run in its turns between the slices of a file: the
/// WAL does not grow for as long as the file takes.
#[test]
fn checkpoints_run_while_a_file_is_in_flight() {
    let mut config = slicing(SLICE);
    config.checkpoint.min_pages = Some(nz(1));
    let (mut rig, mut actor) = caught_up_with(config);
    commit_big(&rig, 2);
    rig.tick(&mut actor, T0 + 1);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 1)),
        Some(&Event::Sending { bytes: SLICE })
    );
    let before = rig.ck.calls().len();
    rig.commit(3);
    let share = u64::from(state::FAIR_SHARE);
    for slice in 2..=share {
        assert_eq!(
            event(&rig.tick(&mut actor, T0 + 2)),
            Some(&Event::Sending {
                bytes: slice.saturating_mul(SLICE)
            })
        );
    }
    let tick = rig.tick(&mut actor, T0 + 2);
    assert!(
        matches!(event(&tick), Some(Event::Checkpointed { busy: false, .. })),
        "{tick:?}"
    );
    assert!(rig.ck.calls().len() > before, "{:?}", rig.ck.calls());
    assert_eq!(
        sent(&actor),
        Some(share.saturating_mul(SLICE)),
        "the file is still in flight"
    );
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 2)),
        Some(&Event::Sending {
            bytes: share.saturating_add(1).saturating_mul(SLICE)
        })
    );
}

/// The bytes are the tick's, not each file's: a tick that finishes one file starts the next
/// with what is left, and reports the file it published.
#[test]
fn a_ticks_bytes_are_shared_by_the_files_it_sends() {
    let size = {
        let (mut rig, mut actor) = caught_up_with(cfg());
        rig.commit(2);
        rig.tick(&mut actor, T0 + 1);
        staged_size(&rig, 2)
    };
    let (mut rig, mut actor) = caught_up_with(slicing(size + 10));
    for n in 2..=3 {
        rig.commit(n);
        rig.tick(&mut actor, T0 + 1);
    }
    assert_eq!(staged(&rig), [1, 2, 3]);
    assert_eq!(staged_size(&rig, 3), size);
    let first = rig.tick(&mut actor, T0 + 1);
    assert!(
        matches!(event(&first), Some(Event::Uploaded(Did::Put(info))) if info.key.range().max().get() == 2),
        "{first:?}"
    );
    assert_eq!(sent(&actor), Some(10));
    assert_eq!(actor.degraded(Task::Upload), None);
    let second = rig.tick(&mut actor, T0 + 1);
    assert!(
        matches!(event(&second), Some(Event::Uploaded(Did::Put(info))) if info.key.range().max().get() == 3),
        "{second:?}"
    );
}

/// A drain keeps sending the file in flight, a slice a tick, and ends when it is whole.
#[test]
fn a_drain_finishes_the_file_in_flight() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 40);
    assert_eq!(
        seen.last(),
        Some(&Tick::Drained { txid: Txid::new(2) }),
        "{seen:?}"
    );
    let slices = seen
        .iter()
        .filter(|t| matches!(event(t), Some(Event::Sending { .. })));
    assert!(slices.count() >= 1, "{seen:?}");
    assert_eq!(newest_replica_file(&rig), Some(2));
    assert_eq!(open_uploads(&rig), 0);
}

/// A drain whose deadline passes with a file in flight abandons it: the upload is aborted,
/// nothing of it is published, and the file counts as not uploaded.
#[test]
fn a_drain_past_its_deadline_aborts_the_file_in_flight() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    actor.begin_shutdown(ts(T0 + 10));
    assert_eq!(
        rig.tick(&mut actor, T0 + 10),
        Tick::DeadlineReached(Unfinished {
            unsynced_wal_bytes: 0,
            staged_not_uploaded: 1,
        })
    );
    assert_eq!(sent(&actor), None);
    assert_eq!(open_uploads(&rig), 0, "aborted, not left open");
    assert_eq!(newest_replica_file(&rig), Some(1));
}

/// A stop runs no tick after it, so the file in flight is dropped where it is: what it wrote
/// is scratch, which the next start removes before it sends the file from its first byte.
#[test]
fn a_stop_drops_the_file_in_flight_and_the_next_start_clears_what_it_left() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    let stopped = actor.fault(Task::Sync, Fault::RemoteAheadOfLocal, ts(T0 + 2));
    assert!(matches!(stopped, Tick::Stopped(_)), "{stopped:?}");
    assert_eq!(sent(&actor), None);
    assert_eq!(open_uploads(&rig), 1, "left as scratch");
    let mut next = Actor::new(slicing(SLICE));
    let boot = rig.tick(&mut next, T0 + 3);
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    assert_eq!(open_uploads(&rig), 0);
    assert_eq!(
        event(&rig.tick(&mut next, T0 + 3)),
        Some(&Event::Sending { bytes: SLICE })
    );
    rig.settle(&mut next, T0 + 3);
    assert_eq!(newest_replica_file(&rig), Some(2));
    assert_eq!(restored(&rig), 2);
}

/// A rebuilt engine keeps nothing of the one it replaces, the file in flight included: its
/// boot removes the scratch and the file goes up again from its first byte.
#[test]
fn a_rebuild_drops_the_file_in_flight() {
    let mut config = slicing(SLICE);
    config.responses = Responses::GO_LIKE;
    let (mut rig, mut actor) = caught_up_with(config);
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    let failed = actor.fault(Task::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 2));
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    let until = until.unix_millis();
    assert_eq!(actor.phase(), Phase::Boot);
    assert_eq!(sent(&actor), None);
    assert_eq!(open_uploads(&rig), 1);
    let boot = rig.tick(&mut actor, until);
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    assert_eq!(open_uploads(&rig), 0);
    assert_eq!(
        event(&rig.tick(&mut actor, until)),
        Some(&Event::Sending { bytes: SLICE })
    );
    rig.settle(&mut actor, until);
    assert_eq!(newest_replica_file(&rig), Some(2));
}

/// A staged file found bad part-way through its send: the upload is aborted on the slice that
/// read it wrong, a second bad read drops the file, and the snapshot that takes its place is
/// sent from its own first byte. Nothing of the bad file is ever published.
#[test]
fn a_staged_file_that_goes_bad_under_its_send_is_aborted_then_replaced() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    tear(&rig, 2, flipped);
    let mut now = T0 + 1;
    let mut seen = Vec::new();
    let resnapshot = loop {
        let tick = rig.tick(&mut actor, now);
        match event(&tick) {
            Some(Event::Retrying { until, .. }) => {
                assert_eq!(sent(&actor), None, "{seen:?}");
                assert_eq!(open_uploads(&rig), 0, "{seen:?}");
                now = until.unix_millis();
            }
            Some(Event::Resnapshot(cause)) => break *cause,
            // Slices of the retry, and maintenance on its turns.
            _ => {}
        }
        seen.push(tick);
        assert!(seen.len() < 200, "{seen:?}");
    };
    assert_eq!(resnapshot, SnapshotCause::NewestStagedFileUnreadable);
    assert_eq!(newest_replica_file(&rig), Some(1));
    rig.settle(&mut actor, now);
    assert_eq!(newest_replica_file(&rig), Some(2));
    assert_eq!(open_uploads(&rig), 0);
    assert_eq!(restored(&rig), 2);
}

/// A failed slice is the upload task's failure like any other: the file waits out the backoff
/// and then goes up from its first byte.
#[test]
fn a_store_fault_during_a_send_retries_the_file_from_its_first_byte() {
    let (mut rig, mut actor) = caught_up_with(slicing(SLICE));
    in_flight(&mut rig, &mut actor, 2, T0 + 1);
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    let failed = rig.tick(&mut actor, T0 + 1);
    let Some(Event::Retrying { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    assert_eq!(sent(&actor), None);
    assert_eq!(open_uploads(&rig), 0);
    assert_eq!(
        event(&rig.tick(&mut actor, until.unix_millis())),
        Some(&Event::Sending { bytes: SLICE })
    );
    rig.settle(&mut actor, until.unix_millis());
    assert_eq!(restored(&rig), 2);
    assert_eq!(
        rig.remote.reads_of(&FileKey::new(
            Level::ZERO,
            TxidRange::single(Txid::new(2).unwrap())
        )),
        0
    );
}

/// Maintenance keeps its share of the ticks while a file is in flight, and the send goes on
/// from where it was afterwards.
#[test]
fn maintenance_takes_its_turn_between_the_slices_of_a_file() {
    let (mut rig, mut actor) = caught_up_with(slicing(8));
    // The first snapshot is written before any level, and a snapshot waits for idle uploads:
    // with one in flight it holds the levels back (D231), so the replica has its snapshot first.
    rig.follow(&mut actor, T0, 20);
    assert!(!Rig::files(&rig.remote, Level::SNAPSHOT).is_empty());
    for n in 2..=5 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    // A new level-1 window, with level-0 files to compact.
    let now = T0 + 60_000;
    rig.commit(6);
    rig.tick(&mut actor, now);
    let mut seen = Vec::new();
    let mut last_sent = 0;
    for _ in 0..80 {
        let tick = rig.tick(&mut actor, now);
        match event(&tick) {
            Some(Event::Sending { bytes }) => {
                assert!(*bytes > last_sent, "the send started again: {seen:?}");
                last_sent = *bytes;
            }
            Some(Event::Uploaded(_)) => break,
            _ => {}
        }
        seen.push(tick);
    }
    let maintained: Vec<usize> = (0..seen.len())
        .filter(|at| matches!(event(&seen[*at]), Some(Event::Maintained { .. })))
        .collect();
    assert!(maintained.len() >= 2, "{seen:?}");
    // Its share is exact: one tick after every `FAIR_SHARE` slices.
    let share = usize::try_from(state::FAIR_SHARE).unwrap();
    assert!(
        maintained
            .windows(2)
            .all(|pair| pair[1] - pair[0] == share + 1),
        "{maintained:?} in {seen:?}"
    );
    assert_eq!(newest_replica_file(&rig), Some(6));
}

/// Another writer's level-0 file at the id of a file in flight is not this engine's, though it
/// is not past what the engine staged. Maintenance has its turn between the slices and lists
/// the replica with that file on it, the whole replica when `whole` and its tails otherwise:
/// it plans nothing from the listing, so no compaction takes the other writer's pages into a
/// level above. The engine's own put of the id is refused and it stops; once the file is
/// removed, a restart sends its own file and the replica restores the commit. Before, the
/// compaction ran, the restart took level 1 as holding the commit and dropped its staged
/// file, and the replica restored a database no commit made.
fn another_writers_file_at_the_id_in_flight_is_not_compacted(whole: bool) {
    let (mut rig, mut actor) = caught_up_with(slicing(8));
    for n in 2..=5 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    // The first snapshot goes before any level (`crate::maintain`), so it is written first.
    // The commit after it is level 1's to compact, in a listing its upload makes due.
    rig.settle(&mut actor, T0 + 60_000);
    assert_eq!(Rig::files(&rig.remote, Level::SNAPSHOT).len(), 1);
    commit_and_settle(&mut rig, &mut actor, 6, T0 + 60_001);
    let now = T0 + 120_000;
    rig.commit(7);
    assert_eq!(
        event(&rig.tick(&mut actor, now)),
        Some(&Event::Synced { files: 1 })
    );
    let theirs = super::second_writer::put_foreign(&rig, 7);
    if whole {
        actor.engine.maintain.remembered.owe_full();
    }
    let above_level_zero = |rig: &Rig| -> Vec<FileKey> {
        Level::ALL
            .into_iter()
            .filter(|level| *level != Level::ZERO)
            .flat_map(|level| Rig::files(&rig.remote, level))
            .map(|info| info.key)
            .filter(|key| key.range().max().get() >= 7)
            .collect()
    };
    let all = u32::try_from(Level::ALL.len()).unwrap();
    let mut seen = Vec::new();
    let mut waits = Vec::new();
    for _ in 0..200 {
        let lists = rig.remote.lists();
        let tick = rig.tick(&mut actor, now);
        assert_eq!(above_level_zero(&rig), [], "after {tick:?}: {seen:?}");
        if let Some(Event::Maintained { event, .. }) = event(&tick) {
            // The prune of staged files reports as maintenance and lists nothing. A pass that
            // lists the replica plans nothing from what it shows.
            let listed = rig.remote.lists().saturating_sub(lists);
            if listed == 0 {
                assert!(matches!(event, MaintainedEvent::Deleted(_)), "{seen:?}");
            } else {
                assert_eq!(*event, MaintainedEvent::Waiting { held: None }, "{seen:?}");
                waits.push(listed);
            }
        }
        let stopped = matches!(tick, Tick::Stopped(_));
        seen.push(tick);
        if stopped {
            break;
        }
    }
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    // The listing is not asked for again at every turn: once, then after the retry interval.
    assert_eq!(waits.len(), 1, "{waits:?} in {seen:?}");
    assert_eq!(waits[0] >= all, whole, "{waits:?}");
    // The operator removes the other writer's file and restarts.
    block_on(rig.remote.inner.delete(&[theirs])).unwrap();
    let mut next = Actor::new(slicing(8));
    let seen = rig.follow(&mut next, now + 1000, 400);
    no_stop(&seen);
    assert_eq!(newest_replica_file(&rig), Some(7));
    assert_eq!(restored(&rig), 7);
}

#[test]
fn a_whole_listing_between_slices_plans_nothing_over_another_writers_file_at_the_id_in_flight() {
    another_writers_file_at_the_id_in_flight_is_not_compacted(true);
}

#[test]
fn a_short_listing_between_slices_plans_nothing_over_another_writers_file_at_the_id_in_flight() {
    another_writers_file_at_the_id_in_flight_is_not_compacted(false);
}
