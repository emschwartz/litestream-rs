//! Persistent faults under each configured response: a degraded lane keeps
//! trying with the capped backoff and the others run, or the actor stops.

use super::*;

fn degraded(tick: &Tick) -> Option<(Lane, PersistentCause, Timestamp)> {
    match event(tick) {
        Some(Event::Degraded {
            lane, cause, until, ..
        }) => Some((*lane, *cause, *until)),
        Some(_) | None => None,
    }
}

fn auth() -> StoreError {
    StoreError::Auth {
        op: StoreOp::Upload,
    }
}

fn refused(op: StoreOp) -> StoreError {
    StoreError::Refused { op, status: 403 }
}

fn caught_up_go_like() -> (Rig, Actor) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.settle(&mut actor, T0);
    (rig, actor)
}

fn replica_tip(rig: &Rig) -> Option<u64> {
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .max()
}

/// The counterpart of `halt_is_terminal`: under the default responses the
/// upload lane is degraded, sync goes on, and the next upload that succeeds
/// clears it.
#[test]
fn revoked_credentials_degrade_the_upload_lane_until_an_upload_succeeds() {
    let (mut rig, mut actor) = caught_up_go_like();
    rig.remote.inner.inject_failure(StoreOp::Upload, auth());
    rig.commit(2);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 10)),
        Some(&Event::Synced { files: 1 })
    );
    let failed = rig.tick(&mut actor, T0 + 10);
    let Some((lane, cause, until)) = degraded(&failed) else {
        panic!("{failed:?}");
    };
    assert_eq!((lane, cause), (Lane::Upload, PersistentCause::Credentials));
    assert_eq!(until, ts(T0 + 1_010));
    assert_eq!(actor.phase(), Phase::Running);
    let state = actor.degraded(Lane::Upload).unwrap();
    assert_eq!((state.since, state.retries), (ts(T0 + 10), 1));
    assert_eq!(actor.degraded(Lane::Sync), None);
    // The sync lane does not wait on the degraded upload lane.
    rig.commit(3);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 20)),
        Some(&Event::Synced { files: 1 })
    );
    let seen = rig.follow(&mut actor, until.unix_millis(), 50);
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Uploaded(_)))),
        "{seen:?}"
    );
    assert_eq!(actor.degraded(Lane::Upload), None, "{seen:?}");
    assert_eq!(replica_tip(&rig), Some(3));
}

/// A persistent failure retried for an hour: the waits double from the
/// retry interval to five minutes and no further, and every one is reported.
#[test]
fn the_degraded_backoff_doubles_to_the_cap_and_no_further() {
    let (mut rig, mut actor) = caught_up_go_like();
    for _ in 0..16 {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, refused(StoreOp::Upload));
    }
    rig.commit(2);
    let mut now = T0 + 10;
    let mut waits = Vec::new();
    for _ in 0..200 {
        let tick = rig.tick(&mut actor, now);
        if let Some((lane, cause, until)) = degraded(&tick) {
            assert_eq!((lane, cause), (Lane::Upload, PersistentCause::StoreRefused));
            waits.push(until.unix_millis() - now);
        }
        match next_of(&tick) {
            Some(Next::Now) => {}
            Some(Next::At(at)) => now = now.max(at.unix_millis()),
            Some(Next::OnWalChange) | None => break,
        }
    }
    let cap = i64::try_from(crate::MAX_BACKOFF.as_millis()).unwrap();
    let want: Vec<i64> = (0..16).map(|n| (1_000_i64 << n).min(cap)).collect();
    assert_eq!(waits, want);
    // The seventeenth attempt succeeded and cleared the lane.
    assert_eq!(actor.degraded(Lane::Upload), None);
    assert_eq!(replica_tip(&rig), Some(2));
}

/// A retention delete the store refuses degrades maintenance only: commits
/// keep syncing and uploading meanwhile.
#[test]
fn a_refused_retention_delete_leaves_sync_and_upload_running() {
    let (mut rig, mut actor) = caught_up_go_like();
    let errors = [refused(StoreOp::Delete), refused(StoreOp::Delete)];
    for n in 2..=4 {
        rig.commit(n);
        rig.settle(&mut actor, T0 + 1);
    }
    for err in errors {
        rig.remote.inner.inject_failure(StoreOp::Delete, err);
    }
    let mut now = T0 + 60_000;
    let failed_at = loop {
        let tick = rig.tick(&mut actor, now);
        if let Some((lane, cause, _)) = degraded(&tick) {
            assert_eq!(
                (lane, cause),
                (Lane::Maintenance, PersistentCause::StoreRefused)
            );
            break now;
        }
        assert!(!matches!(tick, Tick::Halted(_)), "{tick:?}");
        match next_of(&tick) {
            Some(Next::Now) => {}
            Some(Next::At(at)) => now = at.unix_millis(),
            next => panic!("no retention delete ran: {tick:?} {next:?}"),
        }
    };
    assert!(actor.degraded(Lane::Maintenance).is_some());
    rig.commit(5);
    let seen = rig.settle(&mut actor, failed_at);
    assert!(
        seen.iter()
            .any(|t| event(t) == Some(&Event::Synced { files: 1 })),
        "{seen:?}"
    );
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Uploaded(Did::Put(_))))),
        "{seen:?}"
    );
    assert_eq!(replica_tip(&rig), Some(5));
    assert_eq!(actor.degraded(Lane::Sync), None);
    assert_eq!(actor.degraded(Lane::Upload), None);
    assert!(actor.degraded(Lane::Maintenance).is_some());
}

/// While a lane is degraded, retention pins the newest transaction the
/// replica held when it began; the pin goes once every lane has recovered.
#[test]
fn a_degraded_lane_pins_the_replicas_newest_transaction_until_it_recovers() {
    let (mut rig, mut actor) = caught_up_go_like();
    let held = replica_tip(&rig).map(|n| lsr_ltx::Txid::new(n).unwrap());
    rig.remote.inner.inject_failure(StoreOp::Upload, auth());
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    let failed = rig.tick(&mut actor, T0 + 10);
    assert!(degraded(&failed).is_some(), "{failed:?}");
    assert_eq!(actor.retention_pin(), held);
    // A second lane degrading later keeps the first pin.
    actor.fault(Lane::Checkpoint, Fault::WalUnlinked, ts(T0 + 20));
    assert_eq!(actor.retention_pin(), held);
    rig.follow(&mut actor, T0 + 1_010, 50);
    assert_eq!(actor.degraded(Lane::Upload), None);
    assert_eq!(
        actor.retention_pin(),
        held,
        "the checkpoint lane is still degraded"
    );
    actor.kept.lanes.get_mut(Lane::Checkpoint).succeed();
    let tick = rig.tick(&mut actor, T0 + 100_000_000);
    let lanes: Vec<_> = Lane::ALL.iter().map(|l| actor.degraded(*l)).collect();
    assert_eq!(actor.retention_pin(), None, "{tick:?} {lanes:?}");
}

/// `Bug` never continues from the state it left: the actor is rebuilt and
/// boots again after the wait, and replication goes on.
#[test]
fn a_bug_rebuilds_the_actor_and_boots_again_after_the_wait() {
    let (mut rig, mut actor) = caught_up_go_like();
    let failed = actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    let Some((lane, cause, until)) = degraded(&failed) else {
        panic!("{failed:?}");
    };
    assert_eq!((lane, cause), (Lane::Sync, PersistentCause::Bug));
    assert_eq!(actor.phase(), Phase::Boot);
    assert_eq!(actor.position(), None, "nothing of the old state is kept");
    let io = rig.io();
    let early = rig.tick(&mut actor, T0 + 500);
    assert_eq!(event(&early), Some(&Event::Idle));
    assert_eq!(next_of(&early), Some(Next::At(until)));
    assert_eq!(rig.io(), io, "the boot waits for its retry");
    let boot = rig.tick(&mut actor, until.unix_millis());
    assert!(
        matches!(event(&boot), Some(Event::Booted { .. })),
        "{boot:?}"
    );
    assert_eq!(actor.degraded(Lane::Sync), None);
    assert_eq!(actor.phase(), Phase::Running);
    rig.commit(2);
    rig.settle(&mut actor, until.unix_millis() + 1);
    assert_eq!(replica_tip(&rig), Some(2));
}

#[test]
fn a_bug_stops_under_strict() {
    let (_rig, mut actor) = caught_up();
    let tick = actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    assert_eq!(tick, Tick::Halted(HaltCause::Bug));
}

/// Another writer stops uploads and maintenance under the default
/// responses, and the cause stays.
#[test]
fn a_second_writer_stops_the_engine_by_default() {
    let (mut rig, mut actor) = caught_up_go_like();
    let tick = actor.fault(Lane::Upload, Fault::RemoteAheadOfLocal, ts(T0 + 10));
    assert_eq!(tick, Tick::Halted(HaltCause::OtherWriter));
    let io = rig.io();
    rig.commit(2);
    for at in [T0 + 10, T0 + 600_000] {
        assert_eq!(
            rig.tick(&mut actor, at),
            Tick::Halted(HaltCause::OtherWriter)
        );
    }
    assert_eq!(rig.io(), io, "no upload, no maintenance, no sync");
}

/// Continuing past another writer is a rebuild: the next boot takes the
/// replica as it is.
#[test]
fn continuing_past_a_second_writer_boots_again() {
    let mut rig = Rig::new();
    let mut config = go_like();
    config.responses.other_writer = crate::OtherWriter::ContinueAndInterleaveHistories;
    let mut actor = Actor::new(config);
    rig.settle(&mut actor, T0);
    let tick = actor.fault(Lane::Upload, Fault::RemoteAheadOfLocal, ts(T0 + 10));
    assert_eq!(
        degraded(&tick).map(|(lane, cause, _)| (lane, cause)),
        Some((Lane::Sync, PersistentCause::OtherWriter))
    );
    let seen = rig.follow(&mut actor, T0 + 10, 20);
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Booted { .. }))),
        "{seen:?}"
    );
    assert_eq!(actor.degraded(Lane::Sync), None);
}

/// A replica that went backwards is listed again and taken as it now is:
/// the uploads go on from what it holds.
#[test]
fn a_regressed_replica_is_listed_again_and_uploads_go_on() {
    let (mut rig, mut actor) = caught_up_go_like();
    rig.commit(2);
    rig.settle(&mut actor, T0 + 10);
    let tip = replica_tip(&rig).unwrap();
    let tick = actor.fault(Lane::Upload, Fault::RemoteRegressed, ts(T0 + 20));
    assert_eq!(
        degraded(&tick).map(|(lane, cause, _)| (lane, cause)),
        Some((Lane::Upload, PersistentCause::RemoteRegressed))
    );
    assert_eq!(
        actor.engine.upload,
        UploadState::ListFirst { confirmed: None }
    );
    rig.commit(3);
    let lists = rig.remote.calls();
    rig.follow(&mut actor, T0 + 20, 50);
    assert!(rig.remote.calls() > lists);
    assert_eq!(replica_tip(&rig), Some(tip + 1));
    assert_eq!(actor.degraded(Lane::Upload), None);
}

/// Under the default responses a page-size change keeps the boot degraded:
/// it retries, and never stages or publishes a file of the new size.
#[test]
fn a_page_size_change_keeps_the_boot_degraded_and_stages_nothing() {
    let (mut rig, _) = caught_up();
    let before = everything(&rig);
    resize(&mut rig, big());
    let mut actor = Actor::new(go_like());
    let seen = rig.follow(&mut actor, T0 + 10, 30);
    let changed = PersistentCause::PageSizeChanged {
        was: PS,
        now: big(),
    };
    let failures = seen.iter().filter_map(degraded).count();
    assert!(failures > 5, "{seen:?}");
    assert!(
        seen.iter()
            .filter_map(degraded)
            .all(|(lane, cause, _)| lane == Lane::Sync && cause == changed),
        "{seen:?}"
    );
    assert!(seen.iter().all(|t| !matches!(t, Tick::Halted(_))));
    assert_eq!(everything(&rig), before);
    assert_eq!(actor.degraded(Lane::Sync).map(|d| d.cause), Some(changed));
}

/// The offset of a byte in the page of the WAL's last frame.
fn last_page_byte(rig: &Rig) -> usize {
    rig.wal.file_bytes().len().saturating_sub(10)
}

/// A committed frame that fails its checksum is never shipped, and an
/// operator's snapshot waits: a snapshot reads the whole WAL first. Once the
/// frame verifies again the lane recovers and the snapshot runs.
#[test]
fn a_corrupt_committed_frame_is_never_staged_nor_snapshotted_past() {
    let (mut rig, mut actor) = caught_up_go_like();
    let before = staged(&rig);
    rig.commit(2);
    let at = last_page_byte(&rig);
    rig.wal.corrupt_byte(at);
    actor.request_snapshot();
    let seen = rig.follow(&mut actor, T0 + 10, 12);
    assert!(
        seen.iter()
            .filter_map(degraded)
            .all(|(lane, cause, _)| lane == Lane::Sync && cause == PersistentCause::WalCorrupt),
        "{seen:?}"
    );
    assert!(seen.iter().filter_map(degraded).count() > 3, "{seen:?}");
    assert_eq!(staged(&rig), before, "nothing was staged past the frame");
    let now = match seen.last().and_then(next_of) {
        Some(Next::At(at)) => at.unix_millis(),
        other => panic!("{other:?}"),
    };
    rig.wal.corrupt_byte(at);
    let healed = rig.follow(&mut actor, now, 20);
    assert_eq!(actor.degraded(Lane::Sync), None, "{healed:?}");
    assert_eq!(replica_tip(&rig), Some(2), "{healed:?}");
    assert!(snapshots(&rig).contains(&2), "the operator's snapshot ran");
}

/// A drain whose boot keeps failing ends at its deadline, as one whose
/// upload keeps failing does: a degraded boot cannot hold a shutdown open.
#[test]
fn a_drain_whose_boot_stays_degraded_ends_at_its_deadline() {
    let (mut rig, _) = caught_up();
    resize(&mut rig, big());
    let mut actor = Actor::new(go_like());
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert!(seen.iter().any(|t| degraded(t).is_some()), "{seen:?}");
    assert!(
        matches!(seen.last(), Some(Tick::DeadlineReached(_))),
        "{seen:?}"
    );
}

/// Ticks, following the actor's wake times, until it has booted again.
fn boot_again(rig: &mut Rig, actor: &mut Actor, now: i64) -> i64 {
    let mut now = now;
    for _ in 0..50 {
        let tick = rig.tick(actor, now);
        if matches!(event(&tick), Some(Event::Booted { .. })) {
            return now;
        }
        match next_of(&tick) {
            Some(Next::At(at)) => now = now.max(at.unix_millis()),
            Some(Next::Now | Next::OnWalChange) | None => {}
        }
    }
    panic!("never booted again");
}

/// Live pages read for a scheduled verify and not yet taken are lost with
/// the rest of the engine's state by a rebuild: the request stands, and the
/// rebuilt engine reads them again.
#[test]
fn a_rebuild_reads_again_the_live_pages_nobody_took() {
    let (mut rig, mut actor) = caught_up_go_like();
    actor.read_live_pages();
    rig.settle(&mut actor, T0 + 1);
    // Read, and waiting to be taken, as the rebuild comes.
    actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    let now = boot_again(&mut rig, &mut actor, T0 + 10);
    rig.settle(&mut actor, now + 1);
    let live = actor.take_live_pages();
    assert!(live.is_some(), "the verify would wait forever");
    assert_eq!(live.map(|live| live.txid().get()), replica_tip(&rig));
}

/// A request not yet served survives a rebuild too.
#[test]
fn a_rebuild_keeps_a_request_for_live_pages() {
    let (mut rig, mut actor) = caught_up_go_like();
    actor.read_live_pages();
    actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    let now = boot_again(&mut rig, &mut actor, T0 + 10);
    rig.settle(&mut actor, now + 1);
    assert!(actor.take_live_pages().is_some());
}

/// An operator's snapshot request is not engine state: a rebuild before the
/// snapshot was staged still owes it.
#[test]
fn a_rebuild_keeps_an_operators_snapshot_request() {
    let (mut rig, mut actor) = caught_up_go_like();
    actor.request_snapshot();
    actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    assert_eq!(
        actor.pending_snapshot(),
        Some(SnapshotCause::OperatorRequested)
    );
    let now = boot_again(&mut rig, &mut actor, T0 + 10);
    let staged = actor.staged();
    rig.settle(&mut actor, now + 1);
    assert_eq!(actor.pending_snapshot(), None, "the snapshot was staged");
    assert!(actor.staged() > staged);
}

/// The request also survives when another cause was pending before it and
/// took its place in the engine.
#[test]
fn a_rebuild_keeps_a_snapshot_request_made_behind_another_cause() {
    let (mut rig, mut actor) = caught_up_go_like();
    rig.commit(2);
    rig.tick(&mut actor, T0 + 1);
    tear(&rig, 2, truncated);
    let lost = rig.tick(&mut actor, T0 + 1);
    let was = actor.pending_snapshot();
    assert_eq!(was, Some(SnapshotCause::LocalTipUnreadable), "{lost:?}");
    actor.request_snapshot();
    assert_eq!(actor.pending_snapshot(), was);
    actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    assert_eq!(
        actor.pending_snapshot(),
        Some(SnapshotCause::OperatorRequested)
    );
}

/// A maintenance job that keeps failing persistently waits on its level's
/// backoff, not the lane's, and its degraded state still counts every
/// failure.
#[test]
fn a_degraded_maintenance_job_counts_its_retries() {
    let (mut rig, mut actor) = caught_up_go_like();
    let mut now = T0 + 60_000;
    rig.commit(2);
    rig.settle(&mut actor, T0 + 1);
    for _ in 0..3 {
        rig.remote
            .inner
            .inject_failure(StoreOp::Upload, refused(StoreOp::Upload));
    }
    let mut counted = Vec::new();
    for _ in 0..200 {
        let tick = rig.tick(&mut actor, now);
        if let Some((lane, _, _)) = degraded(&tick) {
            assert_eq!(lane, Lane::Maintenance, "{tick:?}");
            counted.push(actor.degraded(Lane::Maintenance).map(|d| d.retries));
        }
        match next_of(&tick) {
            Some(Next::Now) => {}
            Some(Next::At(at)) => now = now.max(at.unix_millis()),
            Some(Next::OnWalChange) | None => break,
        }
        if counted.len() == 3 {
            break;
        }
    }
    assert_eq!(counted, [Some(1), Some(2), Some(3)]);
}

/// The actor built to replace one whose engine is distrusted knows nothing
/// of the WAL or the replica and keeps everything asked of the old one and
/// everything it reported.
#[test]
fn a_rebuilt_actor_keeps_what_was_asked_and_reported_and_boots_again() {
    let (mut rig, mut actor) = caught_up_go_like();
    let held = replica_tip(&rig).map(|n| lsr_ltx::Txid::new(n).unwrap());
    rig.remote.inner.inject_failure(StoreOp::Upload, auth());
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    let failed = rig.tick(&mut actor, T0 + 10);
    assert!(degraded(&failed).is_some(), "{failed:?}");
    actor.request_snapshot();
    actor.read_live_pages();
    actor.begin_shutdown(ts(T0 + 60_000));
    assert!(actor.booted());

    let mut fresh = actor.rebuilt();
    assert!(!fresh.booted());
    assert_eq!(fresh.staged(), None);
    assert_eq!(fresh.replicated(), None);
    assert_eq!(
        fresh.phase(),
        Phase::Draining {
            deadline: ts(T0 + 60_000)
        }
    );
    assert_eq!(fresh.degraded(Lane::Upload), actor.degraded(Lane::Upload));
    assert_eq!(fresh.retention_pin(), held);
    assert_eq!(
        fresh.pending_snapshot(),
        Some(SnapshotCause::OperatorRequested)
    );
    assert_eq!(fresh.app_checkpoints_ran(), actor.app_checkpoints_ran());

    let seen = rig.follow(&mut fresh, T0 + 20, 50);
    assert!(fresh.booted(), "{seen:?}");
    assert!(
        matches!(seen.last(), Some(Tick::Drained { .. })),
        "{seen:?}"
    );
    assert_eq!(fresh.degraded(Lane::Upload), None);
    assert_eq!(fresh.pending_snapshot(), None);
}

/// The engine's own rebuild shows in `booted`, so a driver need not track it.
#[test]
fn booted_follows_the_engines_own_rebuild() {
    let (mut rig, mut actor) = caught_up_go_like();
    assert!(actor.booted());
    actor.fault(Lane::Upload, Fault::Bug(BugSite::PageMap), ts(T0 + 10));
    assert!(!actor.booted());
    boot_again(&mut rig, &mut actor, T0 + 10);
    assert!(actor.booted());
}
