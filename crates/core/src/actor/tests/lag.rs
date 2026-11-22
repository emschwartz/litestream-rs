//! The fail-stop clock and its trips, as the actor feeds them (`crate::failstop`,
//! `actor::lag`): the clock starts at the oldest pending staged file and survives a restart,
//! the probe at the bound, clearing at zero age, and the refusal at boot.

use super::*;
use crate::failstop::{MaxUnreplicatedAge, Trip};
use crate::store::Denied;

/// The default bound, in milliseconds.
const BOUND: i64 = 600_000;

/// Upload failures enough to outlast any test here.
fn remote_down(rig: &Rig, failures: usize) {
    for _ in 0..failures {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, transient(StoreOp::Upload));
    }
}

/// Ticks from `from`, following the actor's own wakes, until `until`; each tick's time and the
/// trip after it.
fn drive(rig: &mut Rig, actor: &mut Actor, from: i64, until: i64) -> Vec<(i64, Option<Trip>)> {
    let mut seen = Vec::new();
    let mut now = from;
    for _ in 0..10_000 {
        if now > until {
            return seen;
        }
        let tick = rig.tick(actor, now);
        seen.push((now, actor.trip()));
        now = match next_of(&tick) {
            Some(Next::Now) => now,
            Some(Next::At(at)) => now.max(at.unix_millis()),
            Some(Next::OnWalChange) | None => until.saturating_add(1),
        };
    }
    panic!("never reached {until}: {:?}", seen.last());
}

fn first_trip(seen: &[(i64, Option<Trip>)]) -> Option<i64> {
    seen.iter()
        .find(|(_, trip)| trip.is_some())
        .map(|(at, _)| *at)
}

/// A caught-up actor under the default responses, one commit staged at `T0 + 10` that the
/// replica refuses `failures` times.
fn lagging(failures: usize) -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    remote_down(&rig, failures);
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    (rig, actor)
}

/// Nothing off the replica: no clock, no trip.
#[test]
fn a_caught_up_engine_has_no_clock_and_no_trip() {
    let (_rig, actor) = caught_up();
    assert_eq!(actor.unreplicated_since(), None);
    assert_eq!(actor.trip(), None);
}

/// A replica that keeps refusing: the probe at the bound fails and trips at the bound itself,
/// not at the retry after it (511 s, then 811 s).
#[test]
fn a_store_down_past_the_bound_trips_at_the_probe() {
    let (mut rig, mut actor) = lagging(1_000);
    let since = actor.unreplicated_since().expect("the clock runs");
    assert!(since.unix_millis() <= T0 + 10, "{since:?}");
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + 10 + BOUND + 60_000);
    let tripped = first_trip(&seen).expect("trips");
    assert_eq!(
        tripped,
        since.unix_millis() + BOUND,
        "the probe runs at the bound: {seen:?}"
    );
    assert_eq!(actor.trip(), Some(Trip::UnreplicatedTooLong { since }));
}

/// The store heals while the engine waits out a long retry (the last failure at 511 s, the
/// next retry at 811 s): the probe at 600 s succeeds and nothing trips.
#[test]
fn a_heal_inside_the_backoff_is_found_by_the_probe_and_never_trips() {
    // The first failure, then retries at 1, 3, 7, 15, 31, 63, 127, 255 and 511 s.
    let (mut rig, mut actor) = lagging(10);
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + 10 + BOUND + 400_000);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
    assert_eq!(actor.unreplicated_since(), None, "caught up at the probe");
}

/// One failed upload is not a trip, however long the clock has run when it fails.
#[test]
fn a_failure_before_the_bound_never_trips() {
    let (mut rig, mut actor) = lagging(1);
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + 10 + 2 * BOUND);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
}

/// A trip for age clears when the age is zero again, and only then: ten failures up to 511 s,
/// the probe at 600 s fails and trips, the retry after it lands.
#[test]
fn a_trip_for_age_clears_once_caught_up() {
    let (mut rig, mut actor) = lagging(11);
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + 10 + BOUND + 1_000);
    assert!(actor.trip().is_some(), "{seen:?}");
    let seen = drive(&mut rig, &mut actor, T0 + BOUND + 2_000, T0 + 2 * BOUND);
    assert_eq!(seen.last().map(|(_, trip)| *trip), Some(None), "{seen:?}");
    assert_eq!(actor.unreplicated_since(), None);
}

/// The clock starts at the oldest pending staged file's stamp, which a restart does not
/// reset: a crash loop with a lifetime shorter than the bound still trips.
#[test]
fn a_crash_loop_does_not_reset_the_clock() {
    let (mut rig, mut actor) = lagging(10_000);
    let since = actor.unreplicated_since().expect("the clock runs");
    let life = 240_000;
    let mut now = T0 + 10;
    let mut tripped = None;
    for _ in 0..5 {
        let seen = drive(&mut rig, &mut actor, now, now + life);
        if let Some(at) = first_trip(&seen) {
            tripped = Some(at);
            break;
        }
        now += life;
        // A crash: a new process, a new actor, the staged files left behind.
        actor = Actor::new(go_like());
        rig.tick(&mut actor, now);
        assert_eq!(
            actor.unreplicated_since(),
            Some(since),
            "the restart keeps the clock"
        );
    }
    let at = tripped.expect("the crash loop trips");
    assert!(at <= since.unix_millis() + BOUND + life, "{at}");
}

/// A snapshot the engine owes after it wrote unstaged frames into the database file keeps the
/// lag that was there: those commits are in no staged file and in no WAL frame. Here the WAL
/// sync fails, the TRUNCATE empties the WAL, and staging refuses the snapshot.
#[test]
fn a_snapshot_owed_after_a_truncate_keeps_the_lag() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(2);
    let failed = rig.tick(&mut actor, T0 + 1);
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("{failed:?}");
    };
    let since = actor
        .unreplicated_since()
        .expect("the commit is off the replica");
    for _ in 0..1_000 {
        rig.staging.inner.inject_failure(
            StoreOp::Upload,
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: ErrorKind::StorageFull,
            },
        );
    }
    let seen = drive(&mut rig, &mut actor, until.unix_millis(), T0 + 30_000);
    assert!(
        rig.ck
            .calls()
            .contains(&CheckpointCall::Checkpoint(CheckpointMode::Truncate)),
        "the TRUNCATE ran: {seen:?}"
    );
    assert!(actor.pending_snapshot().is_some());
    assert_eq!(actor.unreplicated_since(), Some(since), "{seen:?}");
}

/// A refusal before the first listing trips; a timeout does not; the first listing ends it.
#[test]
fn a_refusal_at_boot_trips_until_the_first_listing() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.remote.inner.inject_failure(
        StoreOp::List,
        StoreError::Denied {
            op: StoreOp::List,
            why: Denied::ClockSkew,
        },
    );
    assert!(
        !actor.boot_judged(),
        "nothing is judged before the first boot attempt"
    );
    rig.tick(&mut actor, T0);
    assert_eq!(actor.trip(), Some(Trip::RefusedAtStart(Denied::ClockSkew)));
    assert!(!actor.boot_judged(), "a refusal is no judgment");
    rig.settle(&mut actor, T0 + 600_000);
    assert!(actor.boot_judged());
    assert!(actor.listed());
    assert_eq!(actor.trip(), None, "the first listing releases it");
}

#[test]
fn a_timeout_at_boot_is_no_refusal() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.remote
        .inner
        .inject_failure(StoreOp::List, transient(StoreOp::List));
    rig.tick(&mut actor, T0);
    assert!(!actor.listed());
    assert_eq!(actor.trip(), None);
    assert!(
        actor.boot_judged(),
        "a store that times out does not hold the start"
    );
}

/// After the first listing a refusal is an ordinary degraded state: no refusal trip, only the
/// age, at the bound.
#[test]
fn a_refusal_after_the_first_listing_is_only_age() {
    let (mut rig, mut actor) = {
        let mut rig = Rig::new();
        let mut actor = Actor::new(go_like());
        rig.settle(&mut actor, T0);
        (rig, actor)
    };
    for _ in 0..1_000 {
        rig.remote.inner.inject_failure(
            StoreOp::Upload,
            StoreError::Denied {
                op: StoreOp::Upload,
                why: Denied::Credentials,
            },
        );
    }
    rig.commit(2);
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + BOUND - 1_000);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
    let seen = drive(
        &mut rig,
        &mut actor,
        T0 + BOUND - 1_000,
        T0 + BOUND + 60_000,
    );
    assert!(
        matches!(
            seen.last().and_then(|(_, trip)| *trip),
            Some(Trip::UnreplicatedTooLong { .. })
        ),
        "{seen:?}"
    );
}

/// A stop trips at once; the end of a drain does not.
#[test]
fn an_unsafe_stop_trips_and_a_drain_does_not() {
    let (mut rig, mut actor) = caught_up();
    rig.remote.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Denied {
            op: StoreOp::Upload,
            why: Denied::Credentials,
        },
    );
    rig.commit(2);
    rig.settle(&mut actor, T0 + 10);
    assert_eq!(
        actor.trip(),
        Some(Trip::Stopped(StopCause::StoreDenied(Denied::Credentials)))
    );

    let (mut rig, mut actor) = caught_up();
    remote_down(&rig, 1_000);
    rig.commit(2);
    actor.begin_shutdown(ts(T0 + 10));
    rig.follow(&mut actor, T0 + 10, 20);
    assert_eq!(actor.phase(), Phase::Stopped(StopCause::Shutdown));
    assert!(
        !matches!(actor.trip(), Some(Trip::Stopped(_))),
        "{:?}",
        actor.trip()
    );
}

/// Maintenance failing for longer than the bound with uploads landing never trips.
#[test]
fn a_maintenance_fault_never_trips() {
    let (mut rig, mut actor) = {
        let mut rig = Rig::new();
        let mut actor = Actor::new(go_like());
        rig.settle(&mut actor, T0);
        (rig, actor)
    };
    for _ in 0..1_000 {
        rig.remote
            .inner
            .inject_failure(StoreOp::List, transient(StoreOp::List));
    }
    let mut now = T0;
    let mut degraded = false;
    for n in 2..40 {
        now += 60_000;
        rig.commit(n);
        drive(&mut rig, &mut actor, now, now + 50_000);
        degraded |= actor.degraded(Task::Maintenance).is_some();
        assert_eq!(actor.degraded(Task::Upload), None, "uploads land");
        assert_eq!(actor.trip(), None, "at {now}");
    }
    assert!(degraded, "maintenance kept failing");
}

/// The bound below five minutes is refused.
#[test]
fn the_bound_is_at_least_the_backoff_cap() {
    assert_eq!(MaxUnreplicatedAge::new(Duration::from_secs(299)), None);
    assert!(MaxUnreplicatedAge::new(Duration::from_secs(300)).is_some());
}

/// A caught-up actor whose replica refuses every upload from now on, and the same page 1 the
/// last commit wrote committed again: a transaction that changes nothing, as the driver's start
/// and the frame after a checkpoint are.
fn a_no_op_pending() -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    remote_down(&rig, 10_000);
    rig.commit(1);
    rig.tick(&mut actor, T0 + 10);
    (rig, actor)
}

/// A staged file that changes no page holds no commit of the application's: no clock, and
/// no trip however long the replica refuses it.
#[test]
fn a_file_that_changes_nothing_is_not_lag() {
    let (mut rig, mut actor) = a_no_op_pending();
    assert!(
        actor.staged() > actor.replicated(),
        "the no-op file is staged, not uploaded"
    );
    assert_eq!(actor.unreplicated_since(), None);
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + 2 * BOUND);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
}

/// The same file still pending when the process restarts: the boot finds it changes nothing
/// against the files before it, so a crash loop with the store down and the application idle
/// never trips.
#[test]
fn a_file_that_changes_nothing_is_not_lag_after_a_restart() {
    let (mut rig, _) = a_no_op_pending();
    let mut actor = Actor::new(go_like());
    let seen = drive(&mut rig, &mut actor, T0 + 60_000, T0 + 2 * BOUND);
    assert!(actor.booted());
    assert!(actor.staged() > actor.replicated());
    assert_eq!(actor.unreplicated_since(), None);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
}

/// A commit after the no-op file is lag again, dated from that commit's file.
#[test]
fn a_commit_after_a_file_that_changes_nothing_is_lag() {
    let (mut rig, mut actor) = a_no_op_pending();
    rig.commit(2);
    rig.settle(&mut actor, T0 + 1_000);
    let since = actor
        .unreplicated_since()
        .expect("the commit is off the replica");
    assert!(since.unix_millis() >= T0 + 10, "{since:?}");
}

/// An operator's snapshot taken while nothing is off the replica holds nothing the replica
/// lacks: no clock while the replica refuses it.
#[test]
fn an_operator_snapshot_while_caught_up_is_not_lag() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    remote_down(&rig, 10_000);
    actor.request_snapshot();
    rig.tick(&mut actor, T0 + 10);
    assert!(
        actor.staged() > actor.replicated(),
        "the snapshot is staged, not uploaded"
    );
    assert_eq!(actor.unreplicated_since(), None);
    let seen = drive(&mut rig, &mut actor, T0 + 10, T0 + 2 * BOUND);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
}

/// A boot that asks for a snapshot is judged once that snapshot is staged or its step fails:
/// a stop the snapshot finds is a stop at boot, and holds an application not yet started.
#[test]
fn a_boot_is_judged_once_its_snapshot_is_tried() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    let boot = rig.tick(&mut actor, T0);
    assert!(
        matches!(
            event(&boot),
            Some(Event::Booted {
                cause: Some(SnapshotCause::FirstSync)
            })
        ),
        "{boot:?}"
    );
    assert!(actor.listed());
    assert!(!actor.boot_judged(), "the boot's snapshot is still owed");
    rig.settle(&mut actor, T0);
    assert!(actor.boot_judged());
}

/// A boot that timed out once, then listed, still waits for its snapshot; a snapshot step
/// that fails is a try, and judges the boot.
#[test]
fn a_failed_boot_snapshot_judges_the_boot_and_an_earlier_timeout_does_not() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.remote
        .inner
        .inject_failure(StoreOp::List, transient(StoreOp::List));
    rig.tick(&mut actor, T0);
    assert!(actor.boot_judged(), "a timeout at boot judges it");
    rig.staging.inner.inject_failure(
        StoreOp::Upload,
        StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::StorageFull,
        },
    );
    let boot = rig.tick(&mut actor, T0 + 1_000);
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    let failed = rig.tick(&mut actor, T0 + 1_000);
    assert!(actor.pending_snapshot().is_some(), "{failed:?}");
    assert!(actor.boot_judged(), "the snapshot was tried");
}

/// Only the tasks a commit passes through on its way to the replica count as failures for
/// the clock: a blocked checkpoint, a prune or maintenance never trips (the design's table).
#[test]
fn only_sync_and_upload_failures_count_for_the_clock() {
    let counts = Task::ALL.map(super::super::lag::carries_commits);
    assert_eq!(counts, [true, false, true, false, false]);
}

/// WAL frames the engine could not stage are lag, in the generation it synced and in a new
/// one after a restart.
#[test]
fn unstaged_wal_frames_are_lag_in_either_generation() {
    for restart in [false, true] {
        let mut rig = Rig::new();
        let mut actor = Actor::new(go_like());
        rig.settle(&mut actor, T0);
        assert_eq!(actor.unreplicated_since(), None);
        if restart {
            rig.wal.force_restart();
        }
        rig.commit(2);
        for _ in 0..100 {
            rig.staging.inner.inject_failure(
                StoreOp::Upload,
                StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: ErrorKind::StorageFull,
                },
            );
        }
        rig.tick(&mut actor, T0 + 10);
        assert_eq!(actor.staged(), actor.replicated(), "nothing staged");
        assert!(
            actor.unreplicated_since().is_some(),
            "restart={restart}: the commit is off the replica"
        );
    }
}

/// An operator's snapshot that takes in a commit made since the last tick is lag: the commit
/// is in no other file.
#[test]
fn an_operator_snapshot_that_takes_a_new_commit_is_lag() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    remote_down(&rig, 10_000);
    rig.commit(2);
    actor.request_snapshot();
    rig.tick(&mut actor, T0 + 10);
    assert!(
        actor.staged() > actor.replicated(),
        "the snapshot is staged"
    );
    assert!(actor.unreplicated_since().is_some());
}

/// A writer that commits while every tick runs keeps something off the replica at the end
/// of each one, yet every commit lands a tick or two later. The clock is the oldest commit off
/// the replica now, not the start of that unbroken run: an upload that fails after twenty
/// minutes of it must not trip.
#[test]
fn a_steady_writer_whose_uploads_trail_by_a_tick_never_trips() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    let mut now = T0;
    for n in 0..120_u8 {
        now += 10_000;
        // Lands after the tick's own reads of the WAL end, before the clock's.
        let reads = if n % 2 == 0 { 2 } else { 1 };
        rig.wal.append_tx_before_read(
            reads,
            &[(pg(1), vec![n.wrapping_add(10); PS.bytes()])],
            nz(3),
        );
        rig.tick(&mut actor, now);
        assert!(
            actor.unreplicated_since().is_some(),
            "the run of lag is unbroken at {now}"
        );
    }
    assert!(
        actor.replicated().is_some_and(|txid| txid.get() > 50),
        "commits land: {:?}",
        actor.replicated()
    );
    let since = actor.unreplicated_since().expect("the clock runs");
    assert!(
        now - since.unix_millis() <= 60_000,
        "the clock dates from {since:?}, {} ms before the newest tick, though every older commit landed",
        now - since.unix_millis()
    );
    remote_down(&rig, 1_000);
    rig.commit(200);
    let seen = drive(&mut rig, &mut actor, now + 1, now + 60_000);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
}

/// A boot that finds the newest staged file unreadable owes a snapshot for that doubt, with no
/// synced position to say which WAL frames are staged. With every staged file on the replica,
/// the frames the WAL held then are not lag, nor is the snapshot of them, however long the
/// replica refuses it (`docs/decisions.md` D81).
#[test]
fn a_doubt_snapshot_at_boot_over_a_replicated_wal_is_not_lag() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    commit_and_settle(&mut rig, &mut actor, 3, T0 + 1);
    assert_eq!(actor.replicated(), actor.staged(), "caught up");
    tear(&rig, 3, flipped);
    let mut actor = Actor::new(go_like());
    let again = first_bad_read(&mut rig, &mut actor, T0 + 2);
    remote_down(&rig, 10_000);
    let boot = rig.tick(&mut actor, again);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::NewestStagedFileUnreadable)
        })
    );
    let seen = drive(&mut rig, &mut actor, again, again + 2 * BOUND);
    assert!(
        actor.staged() > actor.replicated(),
        "the snapshot is staged, not uploaded"
    );
    assert_eq!(actor.unreplicated_since(), None);
    assert_eq!(first_trip(&seen), None, "{seen:?}");
}

/// The same doubt with a commit after the boot: that commit is lag, dated from when it was
/// first seen, and trips once the replica has refused it past the bound.
#[test]
fn a_commit_after_a_doubt_at_boot_is_lag() {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    tear(&rig, 2, flipped);
    let mut actor = Actor::new(go_like());
    let again = first_bad_read(&mut rig, &mut actor, T0 + 2);
    remote_down(&rig, 10_000);
    rig.tick(&mut actor, again);
    rig.commit(9);
    let seen = drive(&mut rig, &mut actor, again + 1, again + 2 * BOUND);
    assert!(actor.unreplicated_since().is_some());
    assert!(first_trip(&seen).is_some(), "{seen:?}");
}

/// A wal-index that cannot be read says nothing new: frames the clock dated stay dated, not
/// forgotten, and not dated again from the next sighting.
#[test]
fn an_unreadable_wal_index_keeps_the_date_of_unstaged_frames() {
    let (mut rig, mut actor) = caught_up();
    rig.commit(9);
    // The sync's read fails; the clock's own read after it sees the frames.
    rig.wal.inject(SourceOp::Committed, SourceError::Busy);
    rig.tick(&mut actor, T0 + 10);
    let since = actor.unreplicated_since().expect("the frames are dated");
    // Before the sync's retry: only the clock reads, and that read fails.
    rig.wal.inject(SourceOp::Committed, SourceError::Busy);
    rig.tick(&mut actor, T0 + 20);
    assert_eq!(actor.unreplicated_since(), Some(since));
}
