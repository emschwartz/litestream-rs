//! The local course (`crate::actor` `local`): a process that starts while the replica
//! cannot be listed stages and checkpoints from local state once staging and the WAL agree,
//! uploads nothing, and is replaced whole by the boot that lists the replica.

use super::second_writer::put_foreign;
use super::*;
use crate::actor::lag::BOOT_UNDECIDED_BOUND;
use crate::actor::local::LocalTry;
use crate::actor::parts::Course;
use crate::source::{CheckpointEffect, CheckpointScript};

/// The bound after which a failing boot is judged, in milliseconds.
fn bound() -> i64 {
    i64::try_from(BOOT_UNDECIDED_BOUND.as_millis()).unwrap()
}

/// The replica answers nothing: the boot's first call to it fails.
fn down(rig: &Rig) {
    *rig.remote.scratch_error.lock().unwrap() = Some(transient(StoreOp::List));
}

/// The replica answers again.
fn up(rig: &Rig) {
    *rig.remote.scratch_error.lock().unwrap() = None;
}

/// [`go_like`] with a checkpoint as soon as the WAL holds two frames.
fn checkpointing() -> ActorConfig {
    ActorConfig {
        checkpoint: CheckpointConfig {
            interval: None,
            min_pages: NonZeroU32::new(2),
            truncate_pages: None,
        },
        ..go_like()
    }
}

/// Ticks from `from`, following the actor's own wakes, until `until`.
fn run(rig: &mut Rig, actor: &mut Actor, from: i64, until: i64) -> Vec<Tick> {
    let mut seen = Vec::new();
    let mut now = from;
    for _ in 0..10_000 {
        if now > until {
            return seen;
        }
        let tick = rig.tick(actor, now);
        now = match next_of(&tick) {
            Some(Next::Now) => now,
            Some(Next::At(at)) => now.max(at.unix_millis()),
            Some(Next::OnWalChange) | None => until.saturating_add(1),
        };
        seen.push(tick);
    }
    panic!("never reached {until}: {:?}", seen.last());
}

/// Page 1 of every state the replica restores to, oldest first, without the engine's own
/// writes (the fake's sequence bump leaves page 1 zeroed) and without repeats: the commits
/// the replica holds as restore points.
fn commits_restorable(rig: &Rig) -> Vec<u8> {
    let newest = ranges(&rig.remote).iter().map(|(_, max)| *max).max();
    let mut seen = Vec::new();
    for n in 1..=newest.unwrap() {
        let sink = MemSink::new();
        let at = RestoreTarget::Txid(Txid::new(n).unwrap());
        block_on(restore(&rig.remote.inner, at, sink.clone())).unwrap();
        let fill = sink.published().unwrap().page(1).unwrap()[0];
        if fill != 0 && seen.last() != Some(&fill) {
            seen.push(fill);
        }
    }
    seen
}

/// The boot's settling of the course's slot, with no send in flight.
fn settled(rig: &Rig, actor: &mut Actor) -> Result<(), Fault> {
    use crate::fault::role::{Replica, Staging};
    block_on(async {
        let mut none = crate::upload::InFlight::default();
        let unsent = none.stop().await;
        let held = crate::inventory::Inventory::load(&Replica(&rig.remote))
            .await
            .unwrap()
            .newest();
        actor
            .settle_course(&Staging(&rig.staging), held, &unsent)
            .await
    })
}

fn is_local(actor: &Actor) -> bool {
    matches!(actor.engine.course, Course::Local(_))
}

/// A process that stopped with transactions 2 and 3 staged and the replica at 1, restarted
/// under `cfg` with the store down, ticked through the tick that began the course, of which
/// no step has run: the rig, the restarted actor and the time.
fn just_begun(cfg: ActorConfig) -> (Rig, Actor, i64) {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    assert_eq!(
        (all_staged(&rig), ranges(&rig.remote)),
        (vec![2, 3], vec![(1, 1)])
    );
    down(&rig);
    let mut actor = Actor::new(cfg);
    let at = (T0 + 1_000).saturating_add(bound());
    run(&mut rig, &mut actor, T0 + 1_000, at.saturating_sub(1));
    assert!(!is_local(&actor));
    rig.tick(&mut actor, at);
    assert!(is_local(&actor));
    (rig, actor, at)
}

/// [`just_begun`], ticked until the course has nothing more to do at that time.
fn restarted_down(cfg: ActorConfig) -> (Rig, Actor, i64) {
    let (mut rig, mut actor, at) = just_begun(cfg);
    run(&mut rig, &mut actor, at, at);
    (rig, actor, at)
}

/// The store is down across a restart and the application commits throughout. Before the
/// bound nothing is staged; after it every commit is staged within its tick, checkpoints run
/// and the WAL stays short, and the replica is not touched. When the store answers, the boot
/// continues from the newest staged file with no snapshot, and the replica gets every commit.
#[test]
fn a_restart_during_an_outage_stages_and_checkpoints_from_local_state() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    down(&rig);
    let mut actor = Actor::new(checkpointing());
    let start = T0 + 1_000;
    rig.commit(4);
    let before = run(&mut rig, &mut actor, start, start + bound() - 1);
    assert!(!is_local(&actor), "{before:?}");
    assert_eq!(
        all_staged(&rig),
        vec![2, 3],
        "nothing is staged before the bound"
    );
    let calls = rig.remote.calls();
    let mut seen = run(&mut rig, &mut actor, start + bound(), start + bound());
    assert!(is_local(&actor), "{seen:?}");
    assert!(!actor.booted());
    assert!(actor.boot_judged());
    let mut frames = Vec::new();
    for n in 5..25_u8 {
        rig.commit(n);
        let now = start + bound() + i64::from(n);
        seen.extend(rig.settle(&mut actor, now));
        assert_eq!(
            actor.staged().map(Txid::get),
            all_staged(&rig).last().copied()
        );
        frames.push(rig.wal.committed_end().offset().frame_index());
    }
    no_stop(&seen);
    // One file a commit, and the engine's own write after each checkpoint.
    assert!(all_staged(&rig).len() >= 23, "{:?}", all_staged(&rig));
    let checkpoints = seen
        .iter()
        .filter(|tick| matches!(event(tick), Some(Event::Checkpointed { .. })))
        .count();
    assert!(checkpoints > 0, "{seen:?}");
    assert!(rig.wal.restarts() > 0);
    assert!(
        frames.iter().all(|frames| *frames <= 4),
        "the WAL grew: {frames:?}"
    );
    assert_eq!(rig.remote.calls(), calls, "the replica was asked");
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    up(&rig);
    let after = run(&mut rig, &mut actor, start + 600_000, start + 600_000);
    no_stop(&after);
    let booted: Vec<&Event> = after
        .iter()
        .filter_map(event)
        .filter(|event| matches!(event, Event::Booted { .. }))
        .collect();
    assert_eq!(booted, vec![&Event::Booted { cause: None }], "{after:?}");
    assert!(actor.booted() && actor.listed());
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(commits_restorable(&rig), (1..25).collect::<Vec<u8>>());
    assert_eq!(snapshots(&rig), vec![1], "{after:?}");
}

/// A TRUNCATE checkpoint in a local course takes no snapshot, as one in a booted engine does
/// not: the course goes on in the new WAL, and the boot that lists the replica moves every
/// file up, so the replica restores every commit.
#[test]
fn a_courses_truncate_takes_no_snapshot() {
    let cfg = ActorConfig {
        checkpoint: CheckpointConfig {
            interval: None,
            min_pages: None,
            truncate_pages: NonZeroU32::new(2),
        },
        ..go_like()
    };
    let (mut rig, mut actor, at) = restarted_down(cfg);
    let counted = actor.snapshots().total();
    // A reader holds one PASSIVE back, so the TRUNCATE comes due.
    rig.ck.script(CheckpointScript {
        busy: false,
        effect: CheckpointEffect::Backfill(1),
    });
    let mut seen = Vec::new();
    for n in 4..10_u8 {
        rig.commit(n);
        seen.extend(rig.settle(&mut actor, at + i64::from(n)));
    }
    no_stop(&seen);
    assert!(is_local(&actor), "{seen:?}");
    let truncates = seen
        .iter()
        .filter(|tick| {
            matches!(
                event(tick),
                Some(Event::Checkpointed {
                    mode: CheckpointMode::Truncate,
                    ..
                })
            )
        })
        .count();
    assert!(truncates > 0, "{seen:?}");
    assert_eq!(actor.snapshots().total(), counted, "{seen:?}");
    assert_eq!(
        staged(&rig),
        vec![2, 3],
        "level 0 is as the course found it"
    );
    // A file a commit, and one a checkpoint's new generation.
    assert!(apart(&rig).len() >= 6, "{:?}", apart(&rig));
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    up(&rig);
    let after = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&after);
    assert!(actor.booted() && actor.listed(), "{after:?}");
    assert_eq!(apart(&rig), Vec::<u64>::new());
    assert_eq!(commits_restorable(&rig), (1..10).collect::<Vec<u8>>());
}

/// The local course begins only once boot attempts have failed for the bound: before it the
/// engine has no position and stages nothing, as it did before the course existed.
#[test]
fn the_course_begins_at_the_bound_and_not_before() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let start = T0 + 1_000;
    run(&mut rig, &mut actor, start, start + bound() - 1);
    assert!(!is_local(&actor));
    assert_eq!(actor.kept.local, LocalTry::Open);
    assert_eq!(actor.staged(), None);
    let tick = rig.tick(&mut actor, start + bound());
    assert!(is_local(&actor), "{tick:?}");
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(actor.staged(), Txid::new(3));
    // The tick is the failed boot attempt's, and the next one is due at once.
    assert!(
        matches!(
            tick,
            Tick::Ran {
                event: Event::Retrying {
                    fault: Fault::StoreTransient,
                    ..
                },
                next: Next::Now,
            }
        ),
        "{tick:?}"
    );
    assert_eq!(actor.phase(), Phase::Boot);
}

/// Boot attempts move to the upload task with their run of failures, so the store's fault
/// is reported there and the next attempt waits its doubled backoff; the sync task is clean
/// and is the local steps'.
#[test]
fn boot_attempts_wait_on_the_upload_task_while_the_course_runs() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    assert!(is_local(&actor));
    let (sync, upload) = (
        *actor.kept.tasks.get(Task::Sync),
        *actor.kept.tasks.get(Task::Upload),
    );
    assert_eq!(sync.failures(), 0);
    assert!(sync.ready(ts(at)));
    assert!(upload.failures() >= 4, "{upload:?}");
    assert!(!upload.ready(ts(at)));
    // A commit is staged at once, though the boot's next attempt is not due.
    rig.commit(4);
    let tick = rig.tick(&mut actor, at + 1);
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }));
    assert_eq!(all_staged(&rig), vec![2, 3, 4]);
    // The next attempt runs when the upload task's wait ends, fails there and waits longer.
    let due = upload.until().unwrap().unix_millis();
    let scratch = rig.remote.scratch_calls.load(Ordering::SeqCst);
    run(&mut rig, &mut actor, at + 2, due - 1);
    assert_eq!(rig.remote.scratch_calls.load(Ordering::SeqCst), scratch);
    let tick = rig.tick(&mut actor, due);
    assert_eq!(rig.remote.scratch_calls.load(Ordering::SeqCst), scratch + 1);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Retrying { .. } | Event::Degraded { .. })
        ),
        "{tick:?}"
    );
    let again = *actor.kept.tasks.get(Task::Upload);
    assert_eq!(again.failures(), upload.failures() + 1);
    assert_eq!(actor.kept.tasks.get(Task::Sync).failures(), 0);
    assert!(is_local(&actor));
}

/// Nothing staged reaches the replica before a boot has listed it: with the boot's first
/// call still failing and every other call to the replica working, a local course makes no
/// listing, read or upload.
#[test]
fn nothing_is_uploaded_before_a_boot_lists_the_replica() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    assert!(is_local(&actor));
    let calls = rig.remote.calls();
    let mut seen = Vec::new();
    for n in 4..10_u8 {
        rig.commit(n);
        seen.extend(run(
            &mut rig,
            &mut actor,
            at + i64::from(n),
            at + i64::from(n),
        ));
    }
    seen.extend(run(&mut rig, &mut actor, at + 100, at + 120_000));
    assert_eq!(all_staged(&rig), vec![2, 3, 4, 5, 6, 7, 8, 9]);
    assert_eq!(rig.remote.calls(), calls, "{seen:?}");
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    assert!(
        !seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Uploaded(_) | Event::Maintained { .. })
        )),
        "{seen:?}"
    );
    assert!(!actor.booted() && !actor.listed());
}

/// A staged file that does not read whole: staging does not agree with itself, so the
/// engine stays as it was, with no position, and the try is not made again.
#[test]
fn staging_that_does_not_read_whole_begins_no_course() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3, 4], T0 + 1);
    tear(&rig, 3, truncated);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let start = T0 + 1_000;
    rig.commit(5);
    run(&mut rig, &mut actor, start, start + bound());
    assert!(!is_local(&actor));
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(actor.staged(), None);
    // Later attempts read staging no more.
    let reads = rig.staging.calls();
    run(
        &mut rig,
        &mut actor,
        start + bound() + 1,
        start + 10 * bound(),
    );
    assert_eq!(rig.staging.calls(), reads);
    assert_eq!(
        all_staged(&rig),
        vec![2, 3, 4],
        "nothing was staged or deleted"
    );
}

/// A staged file that does not start where the one before it ended is no agreement either.
#[test]
fn staged_files_that_do_not_chain_begin_no_course() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(4).unwrap()));
    let two = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(2).unwrap()));
    let two = block_on(rig.staging.inner.read(&two, 0, all)).unwrap();
    super::second_writer::put_whole(
        &rig.staging.inner,
        &key,
        &super::witness::relabelled(&two, 4),
    );
    down(&rig);
    let mut actor = Actor::new(go_like());
    run(&mut rig, &mut actor, T0 + 1_000, T0 + 1_000 + bound());
    assert!(!is_local(&actor));
    assert_eq!(actor.kept.local, LocalTry::Spent);
}

/// A WAL that does not continue from the newest staged file (it was restarted while the
/// process was down): only a snapshot can follow, and only a boot numbers one.
#[test]
fn a_wal_that_does_not_continue_staging_begins_no_course() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    rig.wal.force_restart();
    rig.commit(4);
    down(&rig);
    let mut actor = Actor::new(go_like());
    run(&mut rig, &mut actor, T0 + 1_000, T0 + 1_000 + bound());
    assert!(!is_local(&actor));
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(all_staged(&rig), vec![2, 3]);
}

/// With nothing staged there is nothing to agree with: a first start during an outage waits
/// for the store.
#[test]
fn empty_staging_begins_no_course() {
    let mut rig = Rig::new();
    down(&rig);
    let mut actor = Actor::new(go_like());
    run(&mut rig, &mut actor, T0, T0 + bound());
    assert!(!is_local(&actor));
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert!(all_staged(&rig).is_empty());
}

/// A read of staging that fails during the try judges nothing: the next failed boot attempt
/// tries again.
#[test]
fn a_failed_read_during_the_try_leaves_it_open() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let start = T0 + 1_000;
    run(&mut rig, &mut actor, start, start + bound() - 1);
    rig.staging
        .inner
        .inject_failure(StoreOp::List, transient(StoreOp::List));
    let tick = rig.tick(&mut actor, start + bound());
    assert!(!is_local(&actor), "{tick:?}");
    assert_eq!(actor.kept.local, LocalTry::Open);
    run(
        &mut rig,
        &mut actor,
        start + bound() + 1,
        start + 5 * bound(),
    );
    assert!(is_local(&actor));
}

/// A draining engine begins no course: the drain ends at its deadline with the boot undone.
#[test]
fn a_drain_begins_no_course() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let start = T0 + 1_000;
    actor.begin_shutdown(ts(start + 3 * bound()));
    let seen = run(&mut rig, &mut actor, start, start + 2 * bound());
    assert!(!is_local(&actor), "{seen:?}");
    assert_eq!(actor.kept.local, LocalTry::Open);
}

/// The store returns with another writer's file where this engine staged its own: the boot
/// refuses it exactly as a boot from nothing does, and none of the files staged meanwhile
/// is uploaded.
#[test]
fn the_boot_after_the_course_stops_for_another_writer() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    for n in 4..7_u8 {
        rig.commit(n);
        rig.settle(&mut actor, at + i64::from(n));
    }
    assert_eq!(all_staged(&rig), vec![2, 3, 4, 5, 6]);
    let foreign = put_foreign(&rig, 2);
    let theirs = super::second_writer::read_whole(&rig.remote.inner, &foreign);
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
    assert_eq!(
        super::second_writer::read_whole(&rig.remote.inner, &foreign),
        theirs
    );
    assert_eq!(
        all_staged(&rig),
        vec![2, 3, 4, 5, 6],
        "staging is left for the operator"
    );
}

/// Deletes every file of the replica that holds a transaction from `from` on: the bucket
/// as it was before those were written.
fn roll_back(rig: &Rig, from: u64) {
    let gone: Vec<FileKey> = block_on(Inventory::load(&rig.remote.inner))
        .unwrap()
        .files()
        .iter()
        .map(|info| info.key)
        .filter(|key| key.range().max().get() >= from)
        .collect();
    block_on(rig.remote.inner.delete(&gone)).unwrap();
}

/// A process that stopped with the replica at 3 and transactions 4 and 5 staged, restarted
/// with the store down and on the local course, with commits 6 to 8 staged there.
fn local_past_three() -> (Rig, Actor, i64) {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    stage_held(&mut rig, &mut actor, &[4, 5], T0 + 2);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    assert_eq!(all_staged(&rig), vec![2, 3, 4, 5]);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let at = (T0 + 1_000).saturating_add(bound());
    run(&mut rig, &mut actor, T0 + 1_000, at);
    assert!(is_local(&actor));
    for n in 6..9_u8 {
        rig.commit(n);
        rig.settle(&mut actor, at.saturating_add(i64::from(n)));
    }
    assert_eq!(all_staged(&rig), vec![2, 3, 4, 5, 6, 7, 8]);
    (rig, actor, at.saturating_add(10))
}

/// The store returns rolled back to a transaction staging still holds the files after: the
/// boot finds the staged files follow the replica's newest and sends them again. No
/// snapshot, and every commit is a restore point.
#[test]
fn the_boot_after_the_course_refills_a_replica_rolled_back_within_staging() {
    let (mut rig, mut actor, at) = local_past_three();
    roll_back(&rig, 2);
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Booted { cause: None })),
        "{seen:?}"
    );
    assert_eq!(commits_restorable(&rig), (1..9).collect::<Vec<u8>>());
}

/// The store returns rolled back past what staging holds (it is empty, and staging begins
/// at 2): the staged files follow nothing the replica has, so the boot drops them and
/// snapshots, as a boot from nothing does. The snapshot holds every commit made during the
/// outage; their restore points are gone with the files.
#[test]
fn the_boot_after_the_course_snapshots_over_a_replica_rolled_back_past_staging() {
    let (mut rig, mut actor, at) = local_past_three();
    roll_back(&rig, 1);
    assert!(ranges(&rig.remote).is_empty());
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    let booted = seen.iter().find_map(|tick| match event(tick) {
        Some(Event::Booted { cause }) => Some(*cause),
        Some(_) | None => None,
    });
    assert_eq!(
        booted,
        Some(Some(SnapshotCause::StagedUnverified {
            from: Txid::new(2).unwrap()
        })),
        "{seen:?}"
    );
    assert_eq!(restored(&rig), 8, "{seen:?}");
    assert_eq!(commits_restorable(&rig), vec![8]);
}

/// The store returns and refuses the credentials: the refusal is recorded as any refusal
/// at boot is (the start is held again until it heals), reported on the upload task, and the
/// course goes on staging. When the refusal heals, the boot continues with no snapshot.
#[test]
fn a_refusal_during_the_course_holds_the_start_and_keeps_the_course() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    assert!(actor.boot_judged());
    *rig.remote.scratch_error.lock().unwrap() = Some(StoreError::Denied {
        op: StoreOp::List,
        why: Denied::Credentials,
    });
    let seen = run(&mut rig, &mut actor, at + 1, at + 60_000);
    assert!(is_local(&actor), "{seen:?}");
    assert_eq!(
        actor.trip(),
        Some(crate::failstop::Trip::RefusedAtStart(Denied::Credentials))
    );
    assert!(!actor.boot_judged(), "a refusal holds the start");
    let degraded = actor.degraded(Task::Upload).map(|was| was.cause);
    assert_eq!(
        degraded,
        Some(DegradedCause::StoreDenied(Denied::Credentials)),
        "{seen:?}"
    );
    assert_eq!(actor.degraded(Task::Sync), None);
    rig.commit(4);
    let tick = rig.tick(&mut actor, at + 60_001);
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }), "{tick:?}");
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Booted { cause: None })),
        "{seen:?}"
    );
    assert_eq!(actor.degraded(Task::Upload), None);
    assert_eq!(actor.trip(), None);
    assert_eq!(restored(&rig), 4);
}

/// A step that finds the WAL restarted under the position ends the course: the engine has
/// no position again, boot attempts are the sync task's with their run of failures, and the
/// boot that lists the replica takes the snapshot.
#[test]
fn a_step_that_needs_a_snapshot_ends_the_course() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    let failures = actor.kept.tasks.get(Task::Upload).failures();
    // Two restarts: one is what the course's own checkpoint leaves, and is followed.
    rig.wal.force_restart();
    rig.wal.force_restart();
    rig.commit(4);
    let tick = rig.tick(&mut actor, at + 1);
    assert_eq!(
        event(&tick),
        Some(&Event::Resnapshot(SnapshotCause::GenerationChanged)),
        "{tick:?}"
    );
    assert!(matches!(actor.engine.course, Course::Unbooted));
    assert_eq!(actor.kept.tasks.get(Task::Sync).failures(), failures);
    assert_eq!(actor.kept.tasks.get(Task::Upload).failures(), 0);
    assert_eq!(all_staged(&rig), vec![2, 3], "no snapshot was staged");
    // The course is not tried again.
    run(&mut rig, &mut actor, at + 2, at + 10 * bound());
    assert!(matches!(actor.engine.course, Course::Unbooted));
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert!(
        seen.iter().any(|tick| event(tick)
            == Some(&Event::Booted {
                cause: Some(SnapshotCause::GenerationChanged)
            })),
        "{seen:?}"
    );
    assert_eq!(restored(&rig), 4);
}

/// A boot attempt that deleted staged files before it failed leaves the course's position
/// naming a file that is gone: the course ends rather than stage across the hole.
#[test]
fn a_failed_attempt_that_left_the_newest_staged_file_gone_ends_the_course() {
    let (mut rig, mut actor, _) = restarted_down(go_like());
    let upload = *actor.kept.tasks.get(Task::Upload);
    let due = upload.until().unwrap().unix_millis();
    // An attempt that fails with staging untouched keeps the course.
    let tick = rig.tick(&mut actor, due);
    assert!(is_local(&actor), "{tick:?}");
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(3).unwrap()));
    block_on(rig.staging.inner.delete(&[key])).unwrap();
    let due = actor
        .kept
        .tasks
        .get(Task::Upload)
        .until()
        .unwrap()
        .unix_millis();
    let tick = rig.tick(&mut actor, due);
    assert!(matches!(actor.engine.course, Course::Unbooted), "{tick:?}");
    assert_eq!(
        actor.kept.tasks.get(Task::Sync).failures(),
        upload.failures() + 2
    );
    assert_eq!(actor.kept.tasks.get(Task::Upload).failures(), 0);
    // The wake is the sync task's, where the boot now waits.
    assert_eq!(
        next_of(&tick),
        actor.kept.tasks.get(Task::Sync).until().map(Next::At)
    );
}

/// A rebuild ends the course like any engine state, and hands the boot's failures back to
/// the sync task.
#[test]
fn a_rebuild_ends_the_course() {
    let (_rig, mut actor, _) = restarted_down(go_like());
    let failures = actor.kept.tasks.get(Task::Upload).failures();
    actor.rebuild();
    assert!(matches!(actor.engine.course, Course::Unbooted));
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(actor.kept.tasks.get(Task::Sync).failures(), failures);
    assert_eq!(actor.kept.tasks.get(Task::Upload).failures(), 0);
}

/// A failure of the course's own sync step is the sync task's and says nothing of the boot:
/// the run of failed boot attempts is not restarted or extended by it.
#[test]
fn a_failed_local_step_is_no_boot_failure() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    let since = actor.kept.boot_failing_since;
    let upload = *actor.kept.tasks.get(Task::Upload);
    actor.kept.boot_failing_since = None;
    rig.commit(4);
    rig.wal.inject(
        crate::source::SourceOp::Committed,
        crate::source::SourceError::Io(std::io::ErrorKind::Other),
    );
    let tick = rig.tick(&mut actor, at + 1);
    assert!(
        matches!(event(&tick), Some(Event::Retrying { .. })),
        "{tick:?}"
    );
    assert_eq!(actor.kept.tasks.get(Task::Sync).failures(), 1);
    assert_eq!(*actor.kept.tasks.get(Task::Upload), upload);
    assert_eq!(actor.kept.boot_failing_since, None);
    assert!(since.is_some());
    assert!(is_local(&actor));
}

/// An operator's request for a snapshot waits for the boot: the course takes none.
#[test]
fn a_requested_snapshot_waits_for_the_boot() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    actor.request_snapshot();
    assert_eq!(actor.pending_snapshot(), None);
    rig.commit(4);
    let tick = rig.tick(&mut actor, at + 1);
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }));
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert!(snapshots(&rig).contains(&5), "{seen:?}");
}

/// A draining course syncs and does not checkpoint, and the drain's deadline ends it.
#[test]
fn a_draining_course_syncs_without_checkpointing() {
    let (mut rig, mut actor, at) = restarted_down(checkpointing());
    assert!(actor.checkpoints());
    actor.begin_shutdown(ts(at + 5_000));
    assert!(!actor.checkpoints());
    let before = all_staged(&rig).len();
    let mut seen = Vec::new();
    for n in 4..10_u8 {
        rig.commit(n);
        seen.extend(rig.settle(&mut actor, at + i64::from(n)));
    }
    assert_eq!(all_staged(&rig).len(), before + 6, "a file a commit");
    assert!(
        !seen
            .iter()
            .any(|tick| matches!(event(tick), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
    let tick = rig.tick(&mut actor, at + 5_000);
    assert!(matches!(tick, Tick::DeadlineReached(_)), "{tick:?}");
}

/// The staged files the course began over, and each it stages, count toward the cap on
/// staged files, as a booted engine's pending files do.
#[test]
fn the_courses_files_count_toward_the_cap_on_staged_files() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    assert_eq!(actor.engine.witnesses.len(), 1, "file 3, after the oldest");
    rig.commit(4);
    rig.settle(&mut actor, at.saturating_add(1));
    assert_eq!(actor.engine.witnesses.len(), 2);
}

/// A WAL that returns at another page size is reported before anything is staged, as a
/// booted engine reports it, from the course's first step on: the course knows the size its
/// files have.
#[test]
fn a_wal_at_another_page_size_is_reported_by_the_course() {
    let (mut rig, mut actor, at) = just_begun(go_like());
    let before = (all_staged(&rig), apart(&rig));
    resize(&mut rig, big());
    let tick = rig.tick(&mut actor, at);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Degraded {
                task: Task::Sync,
                cause: DegradedCause::PageSizeChanged { .. },
                ..
            })
        ),
        "{tick:?}"
    );
    assert_eq!((all_staged(&rig), apart(&rig)), before);
}

/// The course wakes for its own checkpoint schedule, not only for the next boot attempt.
#[test]
fn the_course_wakes_for_its_checkpoint_schedule() {
    let cfg = ActorConfig {
        checkpoint: CheckpointConfig {
            interval: Some(Duration::from_secs(2)),
            min_pages: None,
            truncate_pages: None,
        },
        ..go_like()
    };
    let (mut rig, mut actor, at) = just_begun(cfg);
    rig.commit(4);
    let seen = run(&mut rig, &mut actor, at, at);
    // The interval runs from when the course began, as a booted engine's does from its boot.
    assert!(
        !seen
            .iter()
            .any(|tick| matches!(event(tick), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
    let boot = actor.kept.tasks.get(Task::Upload).until().unwrap();
    let wake = match seen.last().and_then(next_of) {
        Some(Next::At(wake)) => wake,
        other => panic!("{other:?}: {seen:?}"),
    };
    assert_eq!(wake, ts(at + 2_000));
    assert!(wake < boot, "{wake:?} {boot:?}");
    // Its own work is work at once, whether or not a boot attempt is due with it.
    assert!(matches!(actor.next(wake), Next::Now));
    let seen = run(&mut rig, &mut actor, wake.unix_millis(), wake.unix_millis());
    assert!(
        seen.iter()
            .any(|tick| matches!(event(tick), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
}

/// The course's own step comes before a boot attempt that is due, as the WAL step comes
/// before an upload; after eight ticks in a row that ended in the course's step, the attempt
/// goes first, so an application that never stops writing does not hide the store's return.
#[test]
fn a_due_boot_attempt_waits_its_turn_behind_the_courses_steps() {
    let (mut rig, mut actor, _) = restarted_down(go_like());
    let due = actor
        .kept
        .tasks
        .get(Task::Upload)
        .until()
        .unwrap()
        .unix_millis();
    let scratch = rig.remote.scratch_calls.load(Ordering::SeqCst);
    for n in 4..12_u8 {
        rig.commit(n);
        let tick = rig.tick(&mut actor, due);
        assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }), "{n}");
        assert_eq!(rig.remote.scratch_calls.load(Ordering::SeqCst), scratch);
    }
    // The ninth tick is the attempt's, with a commit waiting.
    rig.commit(12);
    let tick = rig.tick(&mut actor, due);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Retrying { .. } | Event::Degraded { .. })
        ),
        "{tick:?}"
    );
    assert_eq!(rig.remote.scratch_calls.load(Ordering::SeqCst), scratch + 1);
    let tick = rig.tick(&mut actor, due);
    assert_eq!(event(&tick), Some(&Event::Synced { files: 1 }));
    assert_eq!(all_staged(&rig).len(), 11);
    // The count began again: the next attempt, due now, waits eight more steps.
    let due = actor
        .kept
        .tasks
        .get(Task::Upload)
        .until()
        .unwrap()
        .unix_millis();
    let scratch = rig.remote.scratch_calls.load(Ordering::SeqCst);
    for n in 13..20_u8 {
        rig.commit(n);
        rig.tick(&mut actor, due);
    }
    assert_eq!(rig.remote.scratch_calls.load(Ordering::SeqCst), scratch);
    rig.commit(20);
    rig.tick(&mut actor, due);
    assert_eq!(rig.remote.scratch_calls.load(Ordering::SeqCst), scratch + 1);
}

/// The transactions of the files in the course's slot.
fn apart(rig: &Rig) -> Vec<u64> {
    Rig::files(&rig.staging, Level::LOCAL_COURSE)
        .iter()
        .map(|f| f.key.range().max().get())
        .collect()
}

/// Every staged file's transaction: level 0, then the course's slot.
fn all_staged(rig: &Rig) -> Vec<u64> {
    let mut all = staged(rig);
    all.extend(apart(rig));
    all
}

fn l0(n: u64) -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()))
}

/// The key of transaction `n`'s file in the course's slot.
fn slot(n: u64) -> FileKey {
    FileKey::new(
        Level::LOCAL_COURSE,
        TxidRange::single(Txid::new(n).unwrap()),
    )
}

/// The replica holds transactions 1 to 3 and staging only 2: its newest file was deleted
/// and an older one put back, as an operator restoring an old copy of the directory leaves
/// it. The WAL still holds everything from the end of 2.
fn staging_behind() -> Rig {
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    let two = block_on(rig.staging.inner.read(&l0(2), 0, all)).unwrap();
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    let held: Vec<FileKey> = staged(&rig).into_iter().map(l0).collect();
    block_on(rig.staging.inner.delete(&held)).unwrap();
    super::second_writer::put_whole(&rig.staging.inner, &l0(2), &two);
    assert_eq!(all_staged(&rig), vec![2]);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    rig
}

/// The transaction each batch of uploads in `seen` ended at.
fn uploaded(seen: &[Tick]) -> Vec<u64> {
    seen.iter()
        .filter_map(|tick| match event(tick) {
            Some(Event::Uploaded(crate::upload::Did::Put(info))) => {
                Some(info.key.range().max().get())
            }
            Some(_) | None => None,
        })
        .collect()
}

/// [`staging_behind`], restarted with the store down and a commit made, on a local course
/// that began at transaction 2 and staged its own transaction 3 and 4.
fn course_begun_behind() -> (Rig, Actor, i64) {
    let mut rig = staging_behind();
    down(&rig);
    let mut actor = Actor::new(go_like());
    let at = (T0 + 1_000).saturating_add(bound());
    rig.commit(4);
    run(&mut rig, &mut actor, T0 + 1_000, at);
    assert!(is_local(&actor));
    rig.commit(5);
    rig.settle(&mut actor, at.saturating_add(1));
    assert_eq!((staged(&rig), apart(&rig)), (vec![2], vec![3, 4]));
    (rig, actor, at)
}

/// A course that began with staging behind the replica staged its files at transactions
/// the replica already holds. The boot that lists the replica deletes them before it judges
/// anything, and decides as if the course had not run: the WAL still vouches for the
/// replica's newest file, so it continues from there, and the replica's file is untouched.
#[test]
fn a_course_begun_behind_the_replica_is_dropped_when_the_replica_is_listed() {
    let (mut rig, mut actor, at) = course_begun_behind();
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert!(
        seen.iter()
            .any(|tick| event(tick) == Some(&Event::Booted { cause: None })),
        "{seen:?}"
    );
    assert_eq!(actor.kept.local, LocalTry::Spent);
    // Commits 4 and 5 go up as the one file the boot's engine staged for them, above the
    // replica's own transaction 3.
    assert_eq!(uploaded(&seen), vec![4]);
    assert_eq!(restored(&rig), 5);
}

/// The same with the replica's newest file unreadable when the store returns, to the process
/// that ran the course and to one started after it. An upload that found a staged file under
/// an unreadable object's key would take the object for its own, cut short, and write the
/// staged file over it: here that file would be the course's, with other content than the
/// replica's. No boot or upload reads the course's slot, so nothing stands at that key: the
/// unreadable file is reported and left alone, and once it reads again the course's files are
/// deleted for numbering transactions the replica holds.
#[test]
fn a_courses_file_is_never_written_over_an_unreadable_replica_file() {
    for later_process in [false, true] {
        let (mut rig, mut actor, at) = course_begun_behind();
        if later_process {
            actor = Actor::new(go_like());
        }
        let three = super::second_writer::read_whole(&rig.remote.inner, &l0(3));
        let rotten = b"not an ltx file".to_vec();
        super::second_writer::put_whole(&rig.remote.inner, &l0(3), &rotten);
        up(&rig);
        let seen = run(&mut rig, &mut actor, at + 600_000, at + 660_000);
        assert_eq!(
            super::second_writer::read_whole(&rig.remote.inner, &l0(3)),
            rotten,
            "{seen:?}"
        );
        assert_eq!((staged(&rig), apart(&rig)), (vec![2], vec![3, 4]));
        assert!(!actor.booted());
        assert!(
            seen.iter().any(|tick| matches!(
                event(tick),
                Some(
                    Event::Retrying {
                        fault: Fault::RemoteCorrupt,
                        ..
                    } | Event::Degraded {
                        cause: DegradedCause::RemoteCorrupt,
                        ..
                    }
                )
            )),
            "{seen:?}"
        );
        super::second_writer::put_whole(&rig.remote.inner, &l0(3), &three);
        let seen = run(&mut rig, &mut actor, at + 1_200_000, at + 1_200_000);
        no_stop(&seen);
        assert!(actor.booted(), "{seen:?}");
        assert!(apart(&rig).is_empty());
        assert_eq!(uploaded(&seen), vec![4]);
        assert_eq!(restored(&rig), 5);
    }
}

/// A course that ended early (here by a rebuild) leaves its files in staging, and the
/// process still knows no boot judged them: the boot that lists the replica deletes them.
#[test]
fn the_files_of_a_course_that_ended_early_are_still_judged() {
    let (mut rig, mut actor, at) = course_begun_behind();
    actor.rebuild();
    assert_eq!((staged(&rig), apart(&rig)), (vec![2], vec![3, 4]));
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert!(actor.booted(), "{seen:?}");
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(uploaded(&seen), vec![4]);
    assert_eq!(restored(&rig), 5);
}

/// A delete of the course's files that fails leaves them unjudged: the boot attempt fails
/// with the fault, and the next one deletes what is left before it judges.
#[test]
fn a_failed_delete_of_the_courses_files_is_tried_again() {
    let (mut rig, mut actor, at) = course_begun_behind();
    up(&rig);
    rig.staging
        .inner
        .inject_failure(StoreOp::Delete, transient(StoreOp::Delete));
    let due = actor
        .kept
        .tasks
        .get(Task::Upload)
        .until()
        .unwrap()
        .unix_millis()
        .max(at + 2);
    let tick = rig.tick(&mut actor, due);
    assert!(
        matches!(
            event(&tick),
            Some(Event::Retrying { .. } | Event::Degraded { .. })
        ),
        "{tick:?}"
    );
    assert_eq!(apart(&rig), vec![3, 4], "the delete failed");
    assert!(!actor.booted());
    let seen = run(&mut rig, &mut actor, due + 600_000, due + 600_000);
    no_stop(&seen);
    assert!(actor.booted(), "{seen:?}");
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(uploaded(&seen), vec![4]);
    assert_eq!(restored(&rig), 5);
}

/// The replica's newest file is the one the course began at: the course's files are all
/// above it, and the boot sends them as staged, one restore point a commit.
#[test]
fn a_course_begun_at_the_replicas_newest_file_keeps_its_files() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let at = (T0 + 1_000).saturating_add(bound());
    rig.commit(4);
    run(&mut rig, &mut actor, T0 + 1_000, at);
    rig.commit(5);
    rig.settle(&mut actor, at.saturating_add(1));
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert_eq!(actor.kept.local, LocalTry::Spent);
    // Two files, one a commit, in one batch that ends at the second.
    assert_eq!(uploaded(&seen), vec![5]);
    assert_eq!(restored(&rig), 5);
}

/// The boot that settles a course's files against the replica reads the listing it made for
/// the boot, and lists nothing more for them: a boot with no course lists the same.
#[test]
fn settling_a_course_lists_the_replica_no_more_than_a_boot_without_one() {
    let boot_lists = |with_course: bool| {
        let (mut rig, mut actor) = caught_up();
        commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
        commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
        let mut at = T0 + 1_000;
        actor = Actor::new(go_like());
        if with_course {
            down(&rig);
            at = at.saturating_add(bound());
            rig.commit(4);
            run(&mut rig, &mut actor, T0 + 1_000, at);
            assert!(is_local(&actor));
            up(&rig);
            at += 600_000;
        }
        let before = rig.remote.lists();
        let seen = run(&mut rig, &mut actor, at, at);
        assert!(
            seen.iter()
                .any(|tick| event(tick) == Some(&Event::Booted { cause: None })),
            "{seen:?}"
        );
        rig.remote.lists() - before
    };
    assert_eq!(boot_lists(true), boot_lists(false));
}

/// A replica that holds nothing has no file above the course either: the snapshot that
/// never went up and the course's files after it are sent as staged.
#[test]
fn a_course_over_an_empty_replica_keeps_its_files() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    rig.remote
        .inner
        .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    rig.commit(1);
    rig.settle(&mut actor, T0);
    assert_eq!(all_staged(&rig), vec![1]);
    assert!(ranges(&rig.remote).is_empty());
    down(&rig);
    let mut actor = Actor::new(go_like());
    let at = (T0 + 1_000).saturating_add(bound());
    rig.commit(2);
    run(&mut rig, &mut actor, T0 + 1_000, at);
    rig.commit(3);
    rig.settle(&mut actor, at.saturating_add(1));
    up(&rig);
    let seen = run(&mut rig, &mut actor, at + 600_000, at + 600_000);
    no_stop(&seen);
    assert_eq!(actor.kept.local, LocalTry::Spent);
    assert_eq!(uploaded(&seen), vec![3]);
    assert_eq!(restored(&rig), 3);
}

/// A second restart while the store is still down: the new process finds the first one's
/// files in the slot, sees that they continue level 0 and that the WAL continues them, and
/// goes on from the newest. When the store answers, every commit of both is a restore point.
#[test]
fn a_later_process_goes_on_from_the_course_of_the_first() {
    let (mut rig, mut first, at) = restarted_down(go_like());
    rig.commit(4);
    rig.settle(&mut first, at.saturating_add(1));
    assert_eq!((staged(&rig), apart(&rig)), (vec![2, 3], vec![4]));
    let mut second = Actor::new(go_like());
    let start = at.saturating_add(1_000);
    let begun = start.saturating_add(bound());
    rig.commit(5);
    run(&mut rig, &mut second, start, begun);
    assert!(is_local(&second));
    rig.commit(6);
    rig.settle(&mut second, begun.saturating_add(1));
    assert_eq!((staged(&rig), apart(&rig)), (vec![2, 3], vec![4, 5, 6]));
    assert_eq!(second.staged(), Txid::new(6));
    up(&rig);
    let seen = run(&mut rig, &mut second, begun + 600_000, begun + 600_000);
    no_stop(&seen);
    assert!(second.booted(), "{seen:?}");
    assert!(apart(&rig).is_empty());
    assert_eq!(commits_restorable(&rig), (1..=6).collect::<Vec<u8>>());
}

/// A boot whose listing answered moved the course's files up to level 0 and then failed
/// further on: the course goes on, over the level 0 it has now, and its next file is the
/// slot's again.
#[test]
fn a_course_goes_on_over_the_files_a_boot_moved_up() {
    let (mut rig, mut actor, at) = restarted_down(go_like());
    rig.commit(4);
    rig.settle(&mut actor, at.saturating_add(1));
    assert_eq!((staged(&rig), apart(&rig)), (vec![2, 3], vec![4]));
    // The replica holds transaction 1, below level 0's newest.
    assert_eq!(settled(&rig, &mut actor), Ok(()));
    assert_eq!((staged(&rig), apart(&rig)), (vec![2, 3, 4], vec![]));
    assert!(is_local(&actor));
    // The next attempt fails, and finds the course's newest file where it now is.
    let due = actor
        .kept
        .tasks
        .get(Task::Upload)
        .until()
        .unwrap()
        .unix_millis();
    rig.tick(&mut actor, due);
    assert!(is_local(&actor));
    rig.commit(5);
    rig.settle(&mut actor, due.saturating_add(1));
    assert_eq!((staged(&rig), apart(&rig)), (vec![2, 3, 4], vec![5]));
    assert_eq!(actor.staged(), Txid::new(5));
    up(&rig);
    let seen = run(&mut rig, &mut actor, due + 600_000, due + 600_000);
    no_stop(&seen);
    assert_eq!(commits_restorable(&rig), (1..=5).collect::<Vec<u8>>());
}

/// Another build ran between the course and this boot (an older `lsr`, or Go, neither of
/// which reads the slot) and staged its own files at level 0 from where the course began.
/// Level 0 now ends past the slot's oldest file, which no step of a course or of a boot
/// leaves: the slot is deleted whole, and the other build's files stand as it wrote them.
#[test]
fn a_slot_that_level_zero_has_passed_is_deleted_and_level_zero_kept() {
    let rig = Rig::new();
    let mut actor = Actor::new(cfg());
    for (n, theirs) in [(1, &b"one"[..]), (2, b"their two"), (3, b"their three")] {
        super::second_writer::put_whole(&rig.staging.inner, &l0(n), theirs);
    }
    for (n, ours) in [(2, &b"our two"[..]), (3, b"our three"), (4, b"our four")] {
        super::second_writer::put_whole(&rig.staging.inner, &slot(n), ours);
    }
    assert_eq!(settled(&rig, &mut actor), Ok(()));
    assert_eq!((staged(&rig), apart(&rig)), (vec![1, 2, 3], vec![]));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    assert_eq!(
        block_on(rig.staging.inner.read(&l0(2), 0, all)).unwrap(),
        b"their two"
    );
}

/// A boot died after it copied the slot's oldest file up and before it deleted it from the
/// slot: level 0 ends at that file, not past it, and the next boot finishes the move.
#[test]
fn a_move_that_stopped_between_copy_and_delete_is_finished() {
    let rig = Rig::new();
    let mut actor = Actor::new(cfg());
    super::second_writer::put_whole(&rig.staging.inner, &l0(1), b"one");
    super::second_writer::put_whole(&rig.staging.inner, &l0(2), b"two");
    super::second_writer::put_whole(&rig.staging.inner, &slot(2), b"two");
    super::second_writer::put_whole(&rig.staging.inner, &slot(3), b"three");
    assert_eq!(settled(&rig, &mut actor), Ok(()));
    assert_eq!((staged(&rig), apart(&rig)), (vec![1, 2, 3], vec![]));
}

/// Level 0 was emptied under a course: its files are above no level-0 file. A replica that
/// holds anything is past them, and they are deleted; over an empty replica they move up.
#[test]
fn slot_files_over_an_empty_level_zero_are_settled_against_the_replica() {
    for replica_holds in [false, true] {
        let rig = Rig::new();
        let mut actor = Actor::new(cfg());
        super::second_writer::put_whole(&rig.staging.inner, &slot(1), b"one");
        if replica_holds {
            super::second_writer::put_whole(&rig.remote.inner, &l0(1), b"its own");
        }
        assert_eq!(settled(&rig, &mut actor), Ok(()));
        let want = if replica_holds { vec![] } else { vec![1] };
        assert_eq!((staged(&rig), apart(&rig)), (want, vec![]));
    }
}
