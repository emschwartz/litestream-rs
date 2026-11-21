//! A boot whose staging directory holds no level-0 file continues from the
//! replica's newest level-0 file when the WAL still matches it.

use super::*;

/// Caught up through transaction 2, then the staging directory is gone and a
/// new actor starts.
fn wiped() -> (Rig, Actor) {
    let (mut rig, mut actor) = caught_up();
    commit_and_settle(&mut rig, &mut actor, 2, T0 + 1);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
    rig.staging = Counting::default();
    (rig, Actor::new(cfg()))
}

fn newest_staged_file_key() -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::single(Txid::new(2).unwrap()))
}

#[test]
fn with_staging_gone_boot_continues_from_the_replicas_newest_staged_file() {
    let (mut rig, mut actor) = wiped();
    // Under one read's length, so the newest staged file's one read is the whole object.
    let size = Rig::files(&rig.remote, Level::ZERO)[1].size;
    assert!(size < 1 << 20, "{size}");
    let before = rig.remote.calls();
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    // The upload task's listing (one page per level) and its read-back of the
    // newest file, then one read of that file as the newest staged file, from byte 0.
    let levels = u32::try_from(Level::ALL.len()).unwrap();
    assert_eq!(rig.remote.calls() - before, levels + 2);
    assert_eq!(rig.remote.reads_of(&newest_staged_file_key()), 2);
    rig.commit(3);
    let seen = rig.settle(&mut actor, T0 + 2);
    no_halt(&seen);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    assert_eq!(snapshots(&rig), vec![1]);
    assert_eq!(restored(&rig), 3);
}

#[test]
fn with_staging_gone_and_level_zero_retained_away_boot_is_a_first_sync() {
    let (mut rig, _) = caught_up();
    // Level 1 holds transaction 1; the replica's level-0 copy is gone.
    assert_eq!(level_one_max(&rig), Some(1));
    let newest_staged = FileKey::new(Level::ZERO, TxidRange::single(Txid::MIN));
    block_on(rig.remote.inner.delete(&[newest_staged])).unwrap();
    rig.staging = Counting::default();
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::FirstSync)
        })
    );
}

#[test]
fn with_staging_gone_a_wal_that_restarted_snapshots_as_a_staged_newest_staged_file_would() {
    let (mut rig, mut actor) = wiped();
    rig.wal.force_restart();
    rig.commit(3);
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::GenerationChanged)
        })
    );
}

#[test]
fn with_staging_gone_a_replica_newest_staged_file_naming_another_transaction_halts_corrupt() {
    let (mut rig, mut actor) = wiped();
    // The newest staged file whose header names transaction 1 under key 2. The upload task's
    // read-back judges the newest key first, as it did before the newest staged file was read.
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let one = FileKey::new(Level::ZERO, TxidRange::single(Txid::MIN));
    let bytes = block_on(rig.remote.inner.read(&one, 0, all)).unwrap();
    block_on(async {
        let mut up = rig
            .remote
            .inner
            .begin_upload(&newest_staged_file_key(), ts(T0))
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap();
    });
    let seen = rig.follow(&mut actor, T0 + 2, 5);
    assert!(
        seen.contains(&Tick::Halted(HaltCause::RemoteCorrupt)),
        "{seen:?}"
    );
}
