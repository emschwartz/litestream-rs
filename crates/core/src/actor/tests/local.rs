//! The local course (`crate::actor` `local`): a process that starts while the replica
//! cannot be listed stages and checkpoints from local state once staging and the WAL agree,
//! uploads nothing, and is replaced whole by the boot that lists the replica.

use super::second_writer::put_foreign;
use super::*;
use crate::actor::lag::BOOT_UNDECIDED_BOUND;
use crate::actor::local::LocalTry;
use crate::actor::parts::Course;

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

fn is_local(actor: &Actor) -> bool {
    matches!(actor.engine.course, Course::Local(_))
}

/// A process that stopped with transactions 2 and 3 staged and the replica at 1, restarted
/// under `cfg` with the store down, ticked until the boot has failed for the bound: the
/// rig, the restarted actor and the time.
fn restarted_down(cfg: ActorConfig) -> (Rig, Actor, i64) {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    assert_eq!(
        (staged(&rig), ranges(&rig.remote)),
        (vec![2, 3], vec![(1, 1)])
    );
    down(&rig);
    let mut actor = Actor::new(cfg);
    let at = (T0 + 1_000).saturating_add(bound());
    run(&mut rig, &mut actor, T0 + 1_000, at);
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
        staged(&rig),
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
        assert_eq!(actor.staged().map(Txid::get), staged(&rig).last().copied());
        frames.push(rig.wal.committed_end().offset().frame_index());
    }
    no_stop(&seen);
    // One file a commit, and the engine's own write after each checkpoint.
    assert!(staged(&rig).len() >= 23, "{:?}", staged(&rig));
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
    assert_eq!(commits_restorable(&rig), (1..25).collect::<Vec<u8>>());
    assert_eq!(snapshots(&rig), vec![1], "{after:?}");
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
    assert_eq!(staged(&rig), vec![2, 3, 4]);
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
    assert_eq!(staged(&rig), vec![2, 3, 4, 5, 6, 7, 8, 9]);
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
    assert_eq!(staged(&rig), vec![2, 3, 4], "nothing was staged or deleted");
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
    assert_eq!(staged(&rig), vec![2, 3]);
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
    assert!(staged(&rig).is_empty());
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
    assert_eq!(staged(&rig), vec![2, 3, 4, 5, 6]);
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
        staged(&rig),
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
    assert_eq!(staged(&rig), vec![2, 3, 4, 5]);
    down(&rig);
    let mut actor = Actor::new(go_like());
    let at = (T0 + 1_000).saturating_add(bound());
    run(&mut rig, &mut actor, T0 + 1_000, at);
    assert!(is_local(&actor));
    for n in 6..9_u8 {
        rig.commit(n);
        rig.settle(&mut actor, at.saturating_add(i64::from(n)));
    }
    assert_eq!(staged(&rig), vec![2, 3, 4, 5, 6, 7, 8]);
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
        Some(PersistentCause::StoreDenied(Denied::Credentials)),
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
    assert_eq!(staged(&rig), vec![2, 3], "no snapshot was staged");
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
    let before = staged(&rig).len();
    let mut seen = Vec::new();
    for n in 4..10_u8 {
        rig.commit(n);
        seen.extend(rig.settle(&mut actor, at + i64::from(n)));
    }
    assert_eq!(staged(&rig).len(), before + 6, "a file a commit");
    assert!(
        !seen
            .iter()
            .any(|tick| matches!(event(tick), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
    let tick = rig.tick(&mut actor, at + 5_000);
    assert!(matches!(tick, Tick::DeadlineReached(_)), "{tick:?}");
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
    let (mut rig, mut actor, at) = restarted_down(cfg);
    rig.commit(4);
    let seen = rig.settle(&mut actor, at + 1);
    let boot = actor.kept.tasks.get(Task::Upload).until().unwrap();
    let wake = match seen.last().and_then(next_of) {
        Some(Next::At(wake)) => wake,
        other => panic!("{other:?}: {seen:?}"),
    };
    assert!(wake < boot, "{wake:?} {boot:?}");
    let seen = run(&mut rig, &mut actor, wake.unix_millis(), wake.unix_millis());
    assert!(
        seen.iter()
            .any(|tick| matches!(event(tick), Some(Event::Checkpointed { .. }))),
        "{seen:?}"
    );
}
