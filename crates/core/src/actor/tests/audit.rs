//! The boot read-back: the newest file of each level above 0 is verified on
//! the first maintenance listing after a boot.

use super::*;
use crate::plan::{KeyTarget, PlanOutcome, plan};
use crate::store::WriteMode;

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

/// Rewrites the replica's object with its file checksum zeroed: same size, same created-at
/// time. The read-back reads the header and the tail only, so a body the store took wrong is
/// the store's to refuse (signed uploads, `docs/decisions/D272.md`); a bad tail it catches.
pub(super) fn rot(rig: &Rig, info: &FileInfo) {
    rewrite(rig, info, |bytes| {
        let at = bytes.len().saturating_sub(8);
        bytes[at..].fill(0);
    });
}

/// Rewrites the replica's object cut short by a third, as a torn upload leaves it.
fn cut(rig: &Rig, info: &FileInfo) {
    rewrite(rig, info, |bytes| {
        bytes.truncate(bytes.len().saturating_sub(bytes.len() / 3))
    });
}

fn rewrite(rig: &Rig, info: &FileInfo, change: impl FnOnce(&mut Vec<u8>)) {
    let store = &rig.remote.inner;
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let mut bad = block_on(store.read(&info.key, 0, all)).unwrap();
    change(&mut bad);
    block_on(async {
        let mut up = store
            .begin_upload(&info.key, ts(T0), WriteMode::Replace)
            .await
            .unwrap();
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
    // Level 1 starts above the first snapshot, and takes the two commits in the next window.
    rig.settle(&mut actor, T0 + 60_000);
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

/// A newest file cut short has page bytes where its tail belongs: the read-back of its header
/// and tail catches it, and the lower levels rebuild it.
#[test]
fn a_newest_file_cut_short_is_caught_and_rebuilt() {
    let mut rig = replica();
    let one = Level::new(1).unwrap();
    let bad = *Rig::files(&rig.remote, one).last().unwrap();
    cut(&rig, &bad);
    let mut actor = Actor::new(cfg());
    let seen = rig.follow(&mut actor, T0 + 60_000, 50);
    no_stop(&seen);
    assert_eq!(repaired(&seen), vec![bad.key], "{seen:?}");
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

/// One read of a sound snapshot returns bytes that do not decode. A read that fails
/// verification is evidence about the read, not the object: nothing is
/// deleted, and the next maintenance tick reads it again and finds it sound.
#[test]
fn one_bad_read_of_a_sound_snapshot_deletes_nothing() {
    let mut rig = replica();
    let before = upper(&rig);
    let snapshot = *Rig::files(&rig.remote, Level::SNAPSHOT).last().unwrap();
    // The read-back reads the header first: that read answers with bytes no header decodes.
    let nth = rig.remote.reads_of(&snapshot.key) + 1;
    rig.remote
        .other_bytes_on_read(snapshot.key, nth, vec![0; lsr_ltx::HEADER_LEN]);
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

fn refused() -> StoreError {
    StoreError::Corrupt {
        key: None,
        cause: crate::store::CorruptCause::ChecksumMismatch { part: None },
    }
}

/// Ticks at `now` until a write the store refused against its checksum ends a tick.
fn until_refused(rig: &mut Rig, actor: &mut Actor, now: i64) {
    for _ in 0..50 {
        let tick = rig.tick(actor, now);
        if let Some(Event::Retrying {
            fault: Fault::UploadChecksumMismatch,
            ..
        }) = event(&tick)
        {
            return;
        }
    }
    panic!("no write was refused");
}

/// A compaction output the store took damaged and then refused against its checksum: the
/// refusal deletes nothing (the key may hold an earlier attempt's sound output), and the
/// read-back it owes finds the file rotten, removes it, and level 1 is built again.
#[test]
fn a_damaged_output_the_store_refused_is_read_back_and_rebuilt() {
    let (mut rig, mut actor) = caught_up();
    for n in 2..=3 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    let one = Level::new(1).unwrap();
    let before = Rig::files(&rig.remote, one);
    rig.remote.inner.inject_failure_after_publish(refused());
    until_refused(&mut rig, &mut actor, T0 + 60_000);
    assert!(
        actor.engine.maintain.read_back_owed,
        "the refusal owes a read-back"
    );
    let left: Vec<FileInfo> = Rig::files(&rig.remote, one)
        .into_iter()
        .filter(|info| !before.contains(info))
        .collect();
    let [bad] = left.as_slice() else {
        panic!("the refused output is still listed: {left:?}");
    };
    rot(&rig, bad);
    let seen = rig.follow(&mut actor, T0 + 60_000, 100);
    no_stop(&seen);
    assert_eq!(repaired(&seen), vec![bad.key], "{seen:?}");
    for info in upper(&rig) {
        assert!(decodes(&rig, &info.key), "{}", info.key);
    }
    assert_eq!(restored(&rig), 3);
}

/// The same refusal of a sound output: the read-back finds it sound and nothing is deleted.
#[test]
fn a_sound_output_the_store_refused_stays_and_is_read_back() {
    let (mut rig, mut actor) = caught_up();
    for n in 2..=3 {
        commit_and_settle(&mut rig, &mut actor, n, T0 + 1);
    }
    let one = Level::new(1).unwrap();
    let before = Rig::files(&rig.remote, one);
    rig.remote.inner.inject_failure_after_publish(refused());
    until_refused(&mut rig, &mut actor, T0 + 60_000);
    assert!(
        actor.engine.maintain.read_back_owed,
        "the refusal owes a read-back"
    );
    let left: Vec<FileInfo> = Rig::files(&rig.remote, one)
        .into_iter()
        .filter(|info| !before.contains(info))
        .collect();
    let [kept] = left.as_slice() else {
        panic!("the refused output is still listed: {left:?}");
    };
    let reads = rig.remote.reads_of(&kept.key);
    let seen = rig.follow(&mut actor, T0 + 60_000, 100);
    no_stop(&seen);
    assert!(repaired(&seen).is_empty(), "{seen:?}");
    assert!(rig.remote.reads_of(&kept.key) > reads, "it was read back");
    assert!(!rig.remote.deleted().contains(&kept.key));
    assert_eq!(restored(&rig), 3);
}
