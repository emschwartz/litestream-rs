//! Another writer on the replica: the stop it causes holds across a restart
//! because the next boot refuses its files again (`crate::takeover`), and a
//! regression that only maintenance's listing can see is caught there.

use lsr_ltx::{Encoder, Header, HeaderFields};

use super::*;

fn go_like_actor() -> Actor {
    Actor::new(go_like())
}

/// A valid level-0 file for transaction `n` that this engine never staged.
pub(super) fn put_foreign(rig: &Rig, n: u64) -> FileKey {
    let txid = Txid::new(n).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: PS,
        commit: 3,
        txids: TxidRange::single(txid),
        timestamp: ts(T0),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    encoder.write_page(pg(1), &vec![0xee; PS.bytes()]).unwrap();
    let bytes = encoder.finish(None).unwrap().0;
    let key = FileKey::new(Level::ZERO, TxidRange::single(txid));
    block_on(async {
        let mut up = rig
            .remote
            .inner
            .begin_upload(&key, ts(T0), WriteMode::Replace)
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap()
    });
    key
}

pub(super) fn read_whole(store: &MemStore, key: &FileKey) -> Vec<u8> {
    block_on(store.read(key, 0, ReadLen::CHUNK)).unwrap()
}

pub(super) fn put_whole(store: &MemStore, key: &FileKey, bytes: &[u8]) {
    block_on(async {
        let mut up = store
            .begin_upload(key, ts(T0), WriteMode::Replace)
            .await
            .unwrap();
        up.write(bytes).await.unwrap();
        up.commit().await.unwrap()
    });
}

fn newest_l0(rig: &Rig) -> Option<u64> {
    Rig::files(&rig.remote, Level::ZERO)
        .iter()
        .map(|f| f.key.range().max().get())
        .max()
}

/// Stops the caught-up engine for another writer whose file sits above
/// everything it has.
fn stopped_by_other_writer(rig: &mut Rig) {
    let mut actor = go_like_actor();
    rig.settle(&mut actor, T0);
    let ours = actor.staged().max(actor.engine.upload.confirmed());
    put_foreign(rig, ours.unwrap().next().unwrap().get());
    let tick = actor.fault(Task::Upload, Fault::RemoteAheadOfLocal, ts(T0 + 10));
    assert_eq!(tick, Tick::Stopped(StopCause::OtherWriter));
}

/// The defect: a restart took the other writer's file as `BehindRemote` and
/// snapshotted above it. The boot now refuses replica files nothing vouches
/// for, so it stops again and writes nothing while the file stands.
#[test]
fn a_restart_keeps_the_stop_for_another_writer() {
    let mut rig = Rig::new();
    stopped_by_other_writer(&mut rig);
    rig.commit(2);
    let before = newest_l0(&rig);
    let mut next = go_like_actor();
    let seen = rig.follow(&mut next, T0 + 20, 20);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|t| matches!(event(t), Some(Event::Booted { .. }))),
        "{seen:?}"
    );
    assert_eq!(newest_l0(&rig), before, "nothing was uploaded above it");
    assert_eq!(next.refused_through().map(Txid::get), before);
}

/// Once the operator removes the other writer's files the boot replicates
/// as before.
#[test]
fn a_restart_replicates_once_the_other_writers_files_are_gone() {
    let mut rig = Rig::new();
    stopped_by_other_writer(&mut rig);
    let foreign = newest_l0(&rig).unwrap();
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(foreign).unwrap()));
    block_on(rig.remote.inner.delete(&[key])).unwrap();
    rig.commit(2);
    let mut next = go_like_actor();
    let seen = rig.follow(&mut next, T0 + 20, 50);
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Booted { .. }))),
        "{seen:?}"
    );
    assert_eq!(
        newest_l0(&rig),
        Some(foreign),
        "the next upload took the freed id"
    );
}

/// An operator who chose to continue past another writer gets that: the
/// boot builds above its files, as Go does.
#[test]
fn continuing_past_another_writer_boots_above_its_files() {
    let mut rig = Rig::new();
    stopped_by_other_writer(&mut rig);
    let mut config = go_like();
    config.responses.other_writer = crate::OtherWriter::ContinueAndInterleaveHistories;
    let mut next = Actor::new(config);
    let seen = rig.follow(&mut next, T0 + 20, 50);
    assert!(
        seen.iter()
            .any(|t| matches!(event(t), Some(Event::Booted { .. }))),
        "{seen:?}"
    );
}

/// Deletes every replica file whose range ends at the newest transaction, as
/// a bucket put back to an older version does; returns the newest it removed.
fn roll_back(rig: &Rig) -> u64 {
    let all = block_on(Inventory::load(&rig.remote.inner)).unwrap();
    let newest = all.newest().unwrap();
    let gone: Vec<FileKey> = all
        .files()
        .iter()
        .map(|info| info.key)
        .filter(|key| key.range().max() == newest)
        .collect();
    block_on(rig.remote.inner.delete(&gone)).unwrap();
    newest.get()
}

fn regressed(seen: &[Tick]) -> bool {
    seen.iter().any(|t| {
        matches!(
            event(t),
            Some(Event::Degraded {
                cause: DegradedCause::RemoteRegressed,
                ..
            })
        )
    })
}

/// A replica that lost its newest file while the engine ran: the next maintenance listing ends
/// below what the engine confirmed, the regression is reported, and replication goes on. The
/// staged copy of the lost file is still there, so the listing's relist sends it again, and the
/// replica holds it once more. A hole below the newest file is not watched here (restore reports
/// it, `docs/decisions/D173.md`).
#[test]
fn a_rollback_while_running_is_seen_by_the_next_maintenance_listing() {
    let mut rig = Rig::new();
    let mut actor = go_like_actor();
    rig.settle(&mut actor, T0);
    rig.commit(2);
    // Settled when the upload is in and the next listing is scheduled.
    rig.settle(&mut actor, T0 + 10);
    let lost = roll_back(&rig);
    let seen = rig.follow(&mut actor, super::listing_due(T0 + 10), 100);
    assert!(regressed(&seen), "{seen:?}");
    assert!(
        !seen.iter().any(|tick| matches!(tick, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(
        newest_l0(&rig),
        Some(lost),
        "the staged copy is sent again: {seen:?}"
    );
}

/// A whole listing, which the short pass's own check does not stand in for, reports a replica
/// below what the engine confirmed too.
#[test]
fn a_rollback_is_seen_by_a_whole_listing_too() {
    let mut rig = Rig::new();
    let mut actor = go_like_actor();
    rig.settle(&mut actor, T0);
    rig.commit(2);
    rig.settle(&mut actor, T0 + 10);
    roll_back(&rig);
    actor.engine.maintain.remembered.owe_full();
    let listing = rig.tick(&mut actor, super::listing_due(T0 + 10));
    assert!(regressed(&[listing]));
}

/// The listing that finds the replica below what the engine confirmed reports it on that
/// tick, not at some later one that happens upon the same loss (an upload's relist).
#[test]
fn a_rollback_is_reported_by_the_listing_that_sees_it() {
    let mut rig = Rig::new();
    let mut actor = go_like_actor();
    rig.settle(&mut actor, T0);
    rig.commit(2);
    rig.settle(&mut actor, T0 + 10);
    roll_back(&rig);
    let listing = rig.tick(&mut actor, super::listing_due(T0 + 10));
    assert!(regressed(&[listing]));
}

/// Failover with the old host still running: this engine's upload of the
/// next id lost its answer, and before its relist the new host's file took
/// the same id with other bytes. The relist finds the replica's object
/// verifying and other than the staged file, which is still the bytes this
/// process witnessed: another writer. The defect: the divergence dropped the
/// staged file and snapshotted above the other writer's file, interleaving
/// the two histories. Now it stops, and its record leaves the contested id
/// out, so a restart stops again.
#[test]
fn a_running_engine_stops_when_another_writer_took_its_next_id() {
    let mut rig = Rig::new();
    let mut actor = go_like_actor();
    rig.settle(&mut actor, T0);
    rig.commit(2);
    assert_eq!(
        event(&rig.tick(&mut actor, T0 + 10)),
        Some(&Event::Synced { files: 1 })
    );
    let next = actor.staged().unwrap();
    rig.remote
        .inner
        .inject_failure_after_publish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::TimedOut,
        });
    let failed = rig.tick(&mut actor, T0 + 10);
    assert!(
        matches!(event(&failed), Some(Event::Retrying { .. })),
        "{failed:?}"
    );
    put_foreign(&rig, next.get());
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|t| matches!(event(t), Some(Event::Resnapshot(_)))),
        "{seen:?}"
    );
    assert_eq!(newest_l0(&rig), Some(next.get()));
    // The restart: staging holds this engine's own copy of `next`, the
    // replica the other writer's. The boot sees the divergence under one key
    // and refuses it, with nothing kept from the stop.
    let mut restarted = go_like_actor();
    let again = rig.follow(&mut restarted, T0 + 20, 20);
    assert_eq!(
        again.last(),
        Some(&Tick::Stopped(StopCause::OtherWriter)),
        "{again:?}"
    );
    assert_eq!(restarted.refused_through(), Some(next));
    assert_eq!(newest_l0(&rig), Some(next.get()), "nothing went over it");
}

#[test]
fn a_divergence_from_a_replaced_staged_file_is_not_another_writer() {
    let mut rig = Rig::new();
    let mut actor = go_like_actor();
    rig.settle(&mut actor, T0);
    rig.commit(2);
    rig.tick(&mut actor, T0 + 10);
    let next = actor.staged().unwrap();
    rig.remote
        .inner
        .inject_failure_after_publish(StoreError::Transient {
            op: StoreOp::Upload,
            kind: ErrorKind::TimedOut,
        });
    rig.tick(&mut actor, T0 + 10);
    // The staged copy is replaced by another file of the same id that
    // verifies; the replica keeps this engine's own object.
    let key = FileKey::new(Level::ZERO, TxidRange::single(next));
    let own = read_whole(&rig.remote.inner, &key);
    put_foreign(&rig, next.get());
    let other = read_whole(&rig.remote.inner, &key);
    put_whole(&rig.staging.inner, &key, &other);
    put_whole(&rig.remote.inner, &key, &own);
    let seen = rig.follow(&mut actor, T0 + 10, 50);
    assert!(
        !seen.contains(&Tick::Stopped(StopCause::OtherWriter)),
        "{seen:?}"
    );
}

fn puts(seen: &[Tick]) -> Vec<FileKey> {
    seen.iter()
        .filter_map(|tick| match event(tick) {
            Some(Event::Uploaded(Did::Put(info))) => Some(info.key),
            Some(_) | None => None,
        })
        .collect()
}

/// Failover with the old host still alive: host B starts on the replica host
/// A writes to (a database of its own, its own WAL and staging) while A goes
/// on committing. The bucket must refuse a level-0 key that is taken, so
/// whichever host comes second to an id is refused rather than replacing the
/// other's file, and the stop rules then stop it rather than build on the
/// other's file. No id is published by both hosts, and one host stops for
/// another writer.
///
/// The bucket's refusal (`If-None-Match` on level-0 keys) is modelled by the
/// fake remote; a host that published an id the other already holds is the
/// interleaving this test exists to forbid.
#[test]
fn failover_with_the_old_host_alive_never_publishes_one_id_twice() {
    let mut a = Rig::new();
    let mut host_a = go_like_actor();
    a.settle(&mut host_a, T0);
    a.commit(2);
    let mut seen_a = a.follow(&mut host_a, T0 + 10, 50);
    let mut b = Rig::new();
    b.remote = a.remote.clone();
    b.wal = FakeWal::new(
        PS,
        Salts {
            salt1: 77,
            salt2: 88,
        },
    );
    b.ck = FakeCheckpointer::new(b.wal.clone());
    b.commit(5);
    let mut host_b = go_like_actor();
    let mut seen_b = Vec::new();
    let mut now = T0 + 20;
    for round in 0..20_u8 {
        b.commit(round.wrapping_add(100));
        seen_b.extend(b.follow(&mut host_b, now, 20));
        a.commit(round.wrapping_add(50));
        seen_a.extend(a.follow(&mut host_a, now, 20));
        now += 60_000;
    }
    let (puts_a, puts_b) = (puts(&seen_a), puts(&seen_b));
    let both: Vec<&FileKey> = puts_a.iter().filter(|key| puts_b.contains(key)).collect();
    assert!(both.is_empty(), "published by both hosts: {both:?}");
    let stopped = |actor: &Actor| actor.phase() == Phase::Stopped(StopCause::OtherWriter);
    assert!(
        stopped(&host_a) || stopped(&host_b),
        "{seen_a:?}\n{seen_b:?}"
    );
}
