//! The boot read-back: the newest file of each level above 0 is verified on
//! the first maintenance listing after a boot.

use super::*;
use crate::plan::{KeyTarget, PlanOutcome, plan};

/// The remote's files at every level above 0, lowest level first.
fn upper(rig: &Rig) -> Vec<FileInfo> {
    block_on(Inventory::load(&rig.remote.inner))
        .unwrap()
        .files()
        .iter()
        .filter(|f| f.key.level() != Level::ZERO)
        .copied()
        .collect()
}

/// The newest file of each level above 0 that holds one.
fn newest_upper(rig: &Rig) -> Vec<FileInfo> {
    Level::ALL
        .iter()
        .skip(1)
        .filter_map(|level| Rig::files(&rig.remote, *level).last().copied())
        .collect()
}

/// Rewrites the replica's object with one byte changed: same size, same created-at time.
fn rot(rig: &Rig, info: &FileInfo) {
    let store = &rig.remote.inner;
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bad = flipped(block_on(store.read(&info.key, 0, all)).unwrap());
    block_on(async {
        let mut up = store.begin_upload(&info.key, ts(T0)).await.unwrap();
        up.write(&bad).await.unwrap();
        up.commit().await.unwrap();
    });
}

fn decodes(rig: &Rig, key: &FileKey) -> bool {
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bytes = block_on(rig.remote.inner.read(key, 0, all)).unwrap();
    let mut decoder = ReadDecoder::new(&bytes[..]);
    loop {
        match decoder.next_event() {
            Ok(Some(_)) => {}
            Ok(None) => return decoder.finish().is_ok(),
            Err(_) => return false,
        }
    }
}

fn repaired(seen: &[Tick]) -> Vec<FileKey> {
    seen.iter()
        .filter_map(|tick| match event(tick) {
            Some(Event::Maintained {
                event: MaintainedEvent::Repaired(key),
                ..
            }) => Some(*key),
            Some(_) | None => None,
        })
        .collect()
}

/// A replica with files at levels 1 and 9 and three transactions.
fn replica() -> Rig {
    let (mut rig, mut actor) = caught_up();
    for n in 2..=3 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    assert!(!Rig::files(&rig.remote, Level::new(1).unwrap()).is_empty());
    assert!(!Rig::files(&rig.remote, Level::SNAPSHOT).is_empty());
    rig
}

#[test]
fn a_boot_reads_back_the_newest_file_of_each_level_once() {
    let mut rig = replica();
    let newest = newest_upper(&rig);
    assert!(newest.len() >= 2, "{newest:?}");
    let snapshot = newest.last().unwrap().key;
    let before: Vec<usize> = newest.iter().map(|f| rig.remote.reads_of(&f.key)).collect();
    let mut actor = Actor::new(cfg());
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(repaired(&seen).is_empty(), "{seen:?}");
    for (info, before) in newest.iter().zip(before) {
        assert!(
            rig.remote.reads_of(&info.key) > before,
            "{} was not read back",
            info.key
        );
    }
    // Nothing reads a snapshot but the read-back, and it runs once per boot.
    let once = rig.remote.reads_of(&snapshot);
    commit_and_settle(&mut rig, &mut actor, 4, T0 + 3);
    rig.follow(&mut actor, T0 + 60_000, 50);
    assert_eq!(rig.remote.reads_of(&snapshot), once);
    let snapshot = Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap().key;
    let before = rig.remote.reads_of(&snapshot);
    let mut again = Actor::new(cfg());
    rig.settle(&mut again, T0 + 60_000);
    assert_eq!(rig.remote.reads_of(&snapshot), before + 1);
}

#[test]
fn a_rotten_newest_file_the_lower_levels_cover_is_deleted_and_rebuilt() {
    let mut rig = replica();
    let one = Level::new(1).unwrap();
    let bad = *Rig::files(&rig.remote, one).last().unwrap();
    rot(&rig, &bad);
    assert!(!decodes(&rig, &bad.key));
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    no_stop(&seen);
    assert_eq!(repaired(&seen), vec![bad.key], "{seen:?}");
    let rebuilt = Rig::files(&rig.remote, one);
    let last = rebuilt.last().unwrap();
    assert!(
        last.key.range().max() >= bad.key.range().max(),
        "{rebuilt:?}"
    );
    for info in upper(&rig) {
        assert!(decodes(&rig, &info.key), "{}", info.key);
    }
    assert_eq!(restored(&rig), 3);
}

#[test]
fn a_rotten_snapshot_the_lower_levels_cover_is_deleted_and_taken_again() {
    let mut rig = replica();
    let bad = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    rot(&rig, &bad);
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    no_stop(&seen);
    assert_eq!(repaired(&seen), vec![bad.key], "{seen:?}");
    let snapshots = Rig::files(&rig.remote, Level::SNAPSHOT);
    assert!(!snapshots.is_empty(), "the snapshot is due again at once");
    assert!(snapshots.iter().all(|info| decodes(&rig, &info.key)));
    assert_eq!(restored(&rig), 3);
}

#[test]
fn a_rotten_newest_file_nothing_else_covers_stops_and_stays() {
    let mut rig = replica();
    let bad = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    let others: Vec<FileKey> = block_on(Inventory::load(&rig.remote.inner))
        .unwrap()
        .files()
        .iter()
        .map(|f| f.key)
        .filter(|key| *key != bad.key)
        .collect();
    block_on(rig.remote.inner.delete(&others)).unwrap();
    rot(&rig, &bad);
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    assert_eq!(
        seen.last(),
        Some(&Tick::Stopped(StopCause::RemoteCorrupt)),
        "{seen:?}"
    );
    assert!(repaired(&seen).is_empty());
    assert!(
        Rig::files(&rig.remote, Level::SNAPSHOT).contains(&bad),
        "a file nothing else covers is never deleted"
    );
}

/// One read of a sound snapshot returns a flipped byte. A read that fails
/// verification is evidence about the read, not the object: nothing is
/// deleted, and the next maintenance tick reads it again and finds it sound.
#[test]
fn one_bad_read_of_a_sound_snapshot_deletes_nothing() {
    let mut rig = replica();
    let before = upper(&rig);
    let snapshot = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    rig.remote.flip_next_read(snapshot.key);
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    no_stop(&seen);
    assert!(repaired(&seen).is_empty(), "{seen:?}");
    assert!(!rig.remote.deleted().contains(&snapshot.key));
    assert!(upper(&rig).contains(&snapshot), "{before:?}");
}

#[test]
fn a_failed_read_back_retries_and_deletes_nothing() {
    let mut rig = replica();
    let before = upper(&rig);
    let mut actor = Actor::new(cfg());
    assert!(matches!(
        event(&rig.tick(&mut actor, T0 + 2)),
        Some(Event::Booted { .. })
    ));
    rig.remote
        .inner
        .inject_failure(StoreOp::Read, transient(StoreOp::Read));
    let seen = rig.settle(&mut actor, T0 + 2);
    assert!(
        seen.iter().any(|tick| matches!(
            event(tick),
            Some(Event::Retrying {
                fault: Fault::StoreTransient,
                ..
            })
        )),
        "{seen:?}"
    );
    assert_eq!(upper(&rig), before);
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    no_stop(&seen);
    assert!(repaired(&seen).is_empty());
    for info in newest_upper(&rig) {
        assert!(rig.remote.reads_of(&info.key) > 0 || !before.contains(&info));
    }
}

#[test]
fn the_cover_test_is_the_restore_planner() {
    let rig = replica();
    let inventory = block_on(Inventory::load(&rig.remote.inner)).unwrap();
    let mut verdicts = Vec::new();
    for info in upper(&rig) {
        let rest = Inventory::from_files(
            inventory
                .files()
                .iter()
                .filter(|f| f.key != info.key)
                .copied(),
        )
        .unwrap();
        let covered = matches!(
            plan(&rest, KeyTarget::Txid(info.key.range().max())),
            PlanOutcome::Plan(_)
        );
        assert_eq!(
            crate::maintain::restores_without(&inventory, &info.key),
            covered,
            "{}",
            info.key
        );
        verdicts.push(covered);
    }
    assert!(verdicts.contains(&true));
    let alone = Inventory::from_files(upper(&rig).into_iter().take(1)).unwrap();
    let only = alone.files()[0].key;
    assert!(!crate::maintain::restores_without(&alone, &only));
}
