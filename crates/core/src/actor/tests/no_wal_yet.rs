//! A database a restore just published: the replica holds its newest staged file, the WAL
//! file SQLite created has no header yet, nothing is staged, and the restore left a takeover
//! record through what it restored (`crate::takeover`). Boot makes
//! the first sync a `WalMissing` snapshot, which writes the header itself (one
//! `_litestream_seq` bump, Go's `ensureWALExists`) rather than wait for the
//! application to write. Ticks stay bounded by the poll and the backoff.

use super::*;

/// How often the replicator thread polls with nothing to do.
const POLL: i64 = 1_000;

/// A simulated minute.
const MINUTE: i64 = 60_000;

/// More than a minute of polls and every backoff in it: an actor that asks for
/// a tick at once while doing nothing passes this at one instant.
const BOUND: usize = 200;

/// The replica caught up through transaction 1; then a restore's database: no
/// staging, and a WAL file with no header. With `snapshot_due`, the replica
/// holds no level-9 snapshot, so one is due at once.
fn restored_with_no_wal(snapshot_due: bool) -> (Rig, Actor) {
    let (mut rig, _) = caught_up();
    rig.staging = Counting::default();
    // The restore wrote transaction 1 into the database file; SQLite has not
    // written the WAL since it was emptied.
    rig.db.write_page(pg(1), &vec![1; PS.bytes()]);
    rig.wal.force_restart();
    rig.wal.truncate_file(0);
    if snapshot_due {
        let keys: Vec<FileKey> = Rig::files(&rig.remote, Level::SNAPSHOT)
            .iter()
            .map(|info| info.key)
            .collect();
        block_on(rig.remote.inner.delete(&keys)).unwrap();
    }
    let mut actor = Actor::new(go_like());
    actor.remember_takeover(crate::TakeoverRecord::through(Txid::MIN));
    (rig, actor)
}

/// Ticks for `span` milliseconds from `start` as the replicator thread does:
/// again at once after work that asks for it, else at the engine's wake or the
/// next poll, whichever is first. Stops at `BOUND * 10` ticks, so an actor that
/// spins at one instant ends the run instead of hanging it.
fn run_like_the_thread(rig: &mut Rig, actor: &mut Actor, start: i64, span: i64) -> Vec<Tick> {
    let mut seen = Vec::new();
    let mut now = start;
    while now < start.saturating_add(span) && seen.len() < BOUND * 10 {
        let tick = rig.tick(actor, now);
        let poll = now.saturating_add(POLL);
        now = match &tick {
            Tick::Ran {
                event,
                next: Next::Now,
            } if *event != Event::Idle => now,
            Tick::Ran {
                next: Next::At(at), ..
            } => at.unix_millis().min(poll).max(now),
            Tick::Ran { .. } => poll,
            Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_) => {
                seen.push(tick);
                break;
            }
        };
        seen.push(tick);
    }
    seen
}

fn retries(seen: &[Tick]) -> usize {
    seen.iter()
        .filter(|tick| matches!(event(tick), Some(Event::Retrying { .. })))
        .count()
}

fn bumps(rig: &Rig) -> usize {
    rig.ck
        .calls()
        .iter()
        .filter(|call| **call == CheckpointCall::BumpSeq)
        .count()
}

#[test]
fn a_restored_database_with_no_wal_header_ticks_by_the_poll_not_the_cpu() {
    for snapshot_due in [false, true] {
        let (mut rig, mut actor) = restored_with_no_wal(snapshot_due);
        let seen = run_like_the_thread(&mut rig, &mut actor, T0 + 10, MINUTE);
        assert!(seen.len() <= BOUND, "{snapshot_due}: {} ticks", seen.len());
        no_stop(&seen);
        assert_eq!(retries(&seen), 0, "{snapshot_due}: {seen:?}");
        // The pending snapshot ran on the header the bump wrote, and reached
        // the replica after the restored newest staged file.
        assert_eq!(bumps(&rig), 1, "{snapshot_due}: {:?}", rig.ck.calls());
        assert_eq!(actor.pending_snapshot(), None, "{snapshot_due}");
        assert_eq!(snapshots(&rig), vec![1, 2], "{snapshot_due}");
        assert_eq!(actor.replicated(), Some(Txid::new(2).unwrap()));
    }
}

/// The bump that would write the header keeps failing: the sync task waits
/// its doubling backoff, and a level-9 snapshot that is due waits with it
/// rather than stepping the WAL on every tick.
#[test]
fn a_failing_header_write_waits_out_the_sync_tasks_backoff() {
    for snapshot_due in [false, true] {
        let (mut rig, mut actor) = restored_with_no_wal(snapshot_due);
        for _ in 0..1_000 {
            rig.ck.inject(SourceOp::BumpSeq, SourceError::Busy);
        }
        let seen = run_like_the_thread(&mut rig, &mut actor, T0 + 10, MINUTE);
        assert!(seen.len() <= BOUND, "{snapshot_due}: {} ticks", seen.len());
        // 1, 2, 4, 8, 16 and 32 seconds: six tries in a minute, then the wait
        // runs past it.
        assert!(retries(&seen) <= 7, "{snapshot_due}: {seen:?}");
        assert_eq!(bumps(&rig), retries(&seen), "{snapshot_due}");
        assert_eq!(
            actor.pending_snapshot(),
            Some(SnapshotCause::WalMissing),
            "{snapshot_due}"
        );
    }
}

/// Once the header can be written, the snapshot still pending is staged.
#[test]
fn a_header_write_that_failed_once_snapshots_on_the_retry() {
    let (mut rig, mut actor) = restored_with_no_wal(true);
    rig.ck.inject(SourceOp::BumpSeq, SourceError::Busy);
    let seen = run_like_the_thread(&mut rig, &mut actor, T0 + 10, MINUTE);
    assert_eq!(retries(&seen), 1, "{seen:?}");
    assert_eq!(actor.pending_snapshot(), None);
    assert_eq!(snapshots(&rig), vec![1, 2]);
}

#[test]
fn a_drain_of_a_restored_database_with_no_wal_header_finishes() {
    let (mut rig, mut actor) = restored_with_no_wal(false);
    actor.begin_shutdown(actor.shutdown_deadline(ts(T0 + 10)));
    let seen = run_like_the_thread(&mut rig, &mut actor, T0 + 10, MINUTE);
    assert_eq!(
        seen.last(),
        Some(&Tick::Drained {
            txid: Some(Txid::new(2).unwrap())
        }),
        "{seen:?}"
    );
    assert!(seen.len() <= BOUND, "{} ticks", seen.len());
}
