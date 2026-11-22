//! A staged file reaches the replica only with a witness bound to its bytes
//! (`crate::witness`): made by this process, or matched against the live WAL
//! at boot. A whole, valid file put in its place is refused.

use lsr_ltx::Encoder;

use super::*;

/// Rewrites staged `n` as a whole, valid LTX file under the same key and
/// header, with page 1 filled with `fill`: what a staging backup put back,
/// or any replacement that verifies, leaves. Returns its bytes.
fn forge(rig: &Rig, n: u64, fill: u8) -> Vec<u8> {
    let store = &rig.staging.inner;
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let bytes = block_on(store.read(&key, 0, all)).unwrap();
    let mut decoder = ReadDecoder::new(&bytes[..]);
    let mut pages = Vec::new();
    while let Some(event) = decoder.next_event().unwrap() {
        if let lsr_ltx::Event::Page(page) = event {
            let mut data = page.data().to_vec();
            if page.pgno() == pg(1) {
                data.fill(fill);
            }
            pages.push((page.pgno(), data));
        }
    }
    let header = *decoder.finish().unwrap().header();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    for (pgno, data) in &pages {
        encoder.write_page(*pgno, data).unwrap();
    }
    let forged = encoder.finish(None).unwrap().0;
    assert_ne!(forged, bytes);
    block_on(async {
        let mut up = store.begin_upload(&key, ts(T0)).await.unwrap();
        up.write(&forged).await.unwrap();
        up.commit().await.unwrap();
    });
    forged
}

/// Whether the replica's level-0 object for `n` is `bytes`.
fn replica_holds(rig: &Rig, n: u64, bytes: &[u8]) -> bool {
    let key = FileKey::new(Level::ZERO, TxidRange::single(Txid::new(n).unwrap()));
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    block_on(rig.remote.inner.read(&key, 0, all)).is_ok_and(|held| held == bytes)
}

/// A file staged by this engine and replaced, while it waits for its upload,
/// by another valid file under the same key: the upload's own read does not
/// match the witness, the file is dropped like a lost one, and a snapshot
/// takes its place.
#[test]
fn a_staged_file_replaced_under_a_running_engine_is_never_uploaded() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2], T0 + 1);
    assert_eq!(ranges(&rig.remote), vec![(1, 1)]);
    let forged = forge(&rig, 2, 0xEE);
    let seen = rig.follow(&mut actor, T0 + 2, 50);
    no_stop(&seen);
    assert!(!replica_holds(&rig, 2, &forged), "{seen:?}");
    assert!(seen.iter().any(|tick| matches!(
        event(tick),
        Some(Event::Resnapshot(SnapshotCause::NewestStagedFileUnreadable))
    )));
    // Maintenance may have compacted level 0 meanwhile; the newest state is 2.
    assert_eq!(restored(&rig), 2);
}

/// A pending file replaced while the engine was down is not what the live
/// WAL holds for its range. It and every file after it go, and the WAL,
/// which still holds their frames, stages them again as one file: no
/// snapshot, and nothing forged reaches the replica.
#[test]
fn a_replaced_pending_file_is_regenerated_from_the_wal_at_boot() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3, 4], T0 + 1);
    assert_eq!(staged(&rig), vec![2, 3, 4]);
    let forged = forge(&rig, 3, 0xEE);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    assert_eq!(staged(&rig), vec![2]);
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert!(!replica_holds(&rig, 3, &forged));
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    assert_eq!(snapshots(&rig), vec![1]);
    assert_eq!(restored(&rig), 4);
}

/// A plain crash with files pending in the live WAL: every one is witnessed
/// and uploaded with the bytes the engine before the crash staged.
#[test]
fn pending_files_in_the_live_wal_are_uploaded_as_staged() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2, 3], T0 + 1);
    let all = ReadLen::new(ReadLen::MAX_RAW).unwrap();
    let before: Vec<Vec<u8>> = Rig::files(&rig.staging, Level::ZERO)
        .iter()
        .map(|info| block_on(rig.staging.inner.read(&info.key, 0, all)).unwrap())
        .collect();
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(event(&boot), Some(&Event::Booted { cause: None }));
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2), (3, 3)]);
    assert!(replica_holds(&rig, 2, &before[0]));
    assert!(replica_holds(&rig, 3, &before[1]));
    assert_eq!(snapshots(&rig), vec![1]);
}

/// The replica's tail and the pending file were staged in a WAL generation
/// the application has since checkpointed away: nothing live vouches for
/// the pending file, so it is deleted unpublished, the boot names it, and
/// the snapshot takes the transaction after the replica's newest.
#[test]
fn pending_files_from_a_gone_generation_are_dropped_and_named() {
    let (mut rig, mut actor) = caught_up();
    stage_held(&mut rig, &mut actor, &[2], T0 + 1);
    rig.wal.force_restart();
    rig.commit(3);
    let mut actor = Actor::new(cfg());
    let boot = rig.tick(&mut actor, T0 + 2);
    assert_eq!(
        event(&boot),
        Some(&Event::Booted {
            cause: Some(SnapshotCause::StagedUnwitnessed {
                from: Txid::new(2).unwrap()
            })
        })
    );
    let seen = rig.settle(&mut actor, T0 + 2);
    no_stop(&seen);
    assert_eq!(ranges(&rig.remote), vec![(1, 1), (2, 2)]);
    assert_eq!(snapshots(&rig), vec![1, 2]);
    assert_eq!(restored(&rig), 3);
}
