use lsr_core::conformance::block_on;
use lsr_core::{
    Level, MemStore, ObjectStore, PageLimit, ReadLen, SeededEntropy, StoreError, StoreOp, Upload,
};
use lsr_ltx::Timestamp;

use super::*;
use crate::exec::{Polls, RunEnd, count_polls, run_until};
use crate::testkit::{fkey, ltx, origin, put};

fn listed<S: ObjectStore>(store: &S) -> usize {
    block_on(store.list(Level::new(0).unwrap(), None, PageLimit::MAX))
        .unwrap()
        .files
        .len()
}

fn boom() -> StoreError {
    StoreError::Transient {
        op: StoreOp::Upload,
        kind: std::io::ErrorKind::Other,
    }
}

async fn upload<S: ObjectStore>(store: &S, n: u64) -> Result<(), StoreError> {
    let mut up = store
        .begin_upload(&fkey(0, n, n), Timestamp::from_unix_millis(5))
        .await?;
    up.write(&ltx(n, n, 1, 1, None)).await?;
    up.commit().await?;
    Ok(())
}

#[test]
fn yield_makes_every_store_call_a_crash_point() {
    let store = YieldEachCall::new(MemStore::new());
    let (done, polls) = count_polls(upload(&store, 1));
    done.unwrap();
    assert_eq!(polls, 3, "begin, write and commit are one await point each");
    let (_, polls) = count_polls(store.list(Level::new(0).unwrap(), None, PageLimit::MAX));
    assert_eq!(polls, 1);
    let one = ReadLen::new(1).unwrap();
    let (_, polls) = count_polls(store.read(&fkey(0, 1, 1), 0, one));
    assert_eq!(polls, 1);
    let (_, polls) = count_polls(store.delete(&[fkey(0, 1, 1)]));
    assert_eq!(polls, 1);
}

#[test]
fn a_crash_before_the_commit_publishes_nothing() {
    let raw = MemStore::new();
    let store = YieldEachCall::new(raw.clone());
    assert!(matches!(
        run_until(upload(&store, 1), Polls::new(2)),
        RunEnd::Crashed { polls: 2 }
    ));
    assert_eq!(listed(&raw), 0);
    assert!(matches!(
        run_until(upload(&store, 1), Polls::new(3)),
        RunEnd::Done(Ok(()))
    ));
    assert_eq!(listed(&raw), 1);
}

#[test]
fn faults_wrapper_leaves_raw_store_clean() {
    let raw = MemStore::new();
    put(&raw, 0, 1, 1, &ltx(1, 1, 1, 7, None));
    let faults = Faults::new(
        raw.clone(),
        vec![(StoreOp::Read, 0, FaultShape::FlipByteOnRead { at: 3 })],
        1,
    );
    let len = ReadLen::new(8).unwrap();
    let clean = block_on(raw.read(&fkey(0, 1, 1), 0, len)).unwrap();
    let flipped = block_on(faults.read(&fkey(0, 1, 1), 0, len)).unwrap();
    assert_ne!(clean, flipped);
    assert_eq!(clean.get(3).map(|b| b ^ 1), flipped.get(3).copied());
    assert_eq!(block_on(raw.read(&fkey(0, 1, 1), 0, len)).unwrap(), clean);
    // One-shot: the next read through the wrapper is clean again.
    assert_eq!(
        block_on(faults.read(&fkey(0, 1, 1), 0, len)).unwrap(),
        clean
    );
}

#[test]
fn err_before_publish_is_invisible() {
    let raw = MemStore::new();
    let recorder = Recorder::new(raw.clone());
    let log = recorder.log();
    // Upload calls count begin (0), write (1), commit (2).
    let faults = Faults::new(
        recorder,
        vec![(StoreOp::Upload, 2, FaultShape::Error(boom()))],
        1,
    );
    assert_eq!(block_on(upload(&faults, 1)), Err(boom()));
    assert_eq!(listed(&raw), 0);
    assert!(log.is_empty(), "the recorder saw no commit");
}

#[test]
fn recorder_records_err_after_publish() {
    let raw = MemStore::new();
    let recorder = Recorder::new(raw.clone());
    let log = recorder.log();
    let faults = Faults::new(
        recorder,
        vec![(StoreOp::Upload, 0, FaultShape::ErrorAfterPublish(boom()))],
        1,
    );
    let wal = Some(origin(1, 1));
    let bytes = ltx(1, 1, 1, 1, wal);
    let result = block_on(async {
        let mut up = faults
            .begin_upload(&fkey(0, 1, 1), Timestamp::from_unix_millis(5))
            .await?;
        up.write(&bytes).await?;
        up.commit().await
    });
    assert_eq!(result, Err(boom()), "the caller saw a failure");
    assert_eq!(listed(&raw), 1, "yet the object is published");
    let events = log.events();
    assert_eq!(events.len(), 1);
    let RecorderEvent::Put {
        key, origin: got, ..
    } = &events[0]
    else {
        panic!("expected a put, got {events:?}");
    };
    assert_eq!(*key, fkey(0, 1, 1));
    assert_eq!(*got, wal);
}

#[test]
fn a_size_mismatch_publishes_and_overstates_the_size() {
    let raw = MemStore::new();
    let faults = Faults::new(
        raw.clone(),
        vec![(StoreOp::Upload, 0, FaultShape::SizeMismatch)],
        1,
    );
    let bytes = ltx(1, 1, 1, 1, None);
    let info = block_on(async {
        let mut up = faults
            .begin_upload(&fkey(0, 1, 1), Timestamp::from_unix_millis(5))
            .await?;
        up.write(&bytes).await?;
        up.commit().await
    })
    .unwrap();
    assert_eq!(
        info.size,
        u64::try_from(bytes.len()).unwrap().saturating_add(1)
    );
    assert_eq!(listed(&raw), 1);
}

#[test]
fn a_rate_fault_fires_from_its_call_on_and_never_exhausts() {
    let raw = MemStore::new();
    let faults = Faults::new(
        raw,
        vec![(
            StoreOp::List,
            1,
            FaultShape::Rate {
                per_mille: 1000,
                err: boom(),
            },
        )],
        3,
    );
    let list = || block_on(faults.list(Level::new(0).unwrap(), None, PageLimit::MAX));
    list().unwrap();
    assert_eq!(list().unwrap_err(), boom());
    assert_eq!(list().unwrap_err(), boom());
    assert_eq!(faults.handle().calls(StoreOp::List), 3);
}

#[test]
fn stage_power_loss_drops_or_tears_only_volatile() {
    let mut dropped = 0;
    let mut torn = 0;
    for seed in 0..40 {
        let raw = MemStore::new();
        let volatile = Volatile::new(raw.clone());
        let handle = volatile.handle();
        let durable = ltx(1, 1, 1, 1, None);
        let fresh = ltx(2, 2, 1, 2, None);
        block_on(async {
            let mut up = volatile
                .begin_upload(&fkey(0, 1, 1), Timestamp::from_unix_millis(5))
                .await
                .unwrap();
            up.write(&durable).await.unwrap();
            up.commit().await.unwrap();
        });
        put(&raw, 0, 2, 2, &fresh);
        handle.mark_volatile(fkey(0, 2, 2));
        let entropy = SeededEntropy::new(seed);
        let lost = block_on(handle.power_loss(&raw, &entropy)).unwrap();
        assert_eq!(lost.len(), 1);
        assert_eq!(lost[0].0, fkey(0, 2, 2));
        assert!(handle.volatile().is_empty());
        let len = ReadLen::new(1 << 16).unwrap();
        assert_eq!(
            block_on(raw.read(&fkey(0, 1, 1), 0, len)).unwrap(),
            durable,
            "a durable file is untouched"
        );
        match lost[0].1 {
            Lost::Dropped => {
                dropped += 1;
                assert_eq!(listed(&raw), 1);
            }
            Lost::Torn { kept } => {
                torn += 1;
                let left = block_on(raw.read(&fkey(0, 2, 2), 0, len)).unwrap();
                assert_eq!(u64::try_from(left.len()).unwrap(), kept);
                assert!(left.len() < fresh.len());
            }
            Lost::Restored => panic!("nothing was unlinked"),
        }
    }
    assert!(dropped > 0 && torn > 0, "{dropped} dropped, {torn} torn");
}

#[test]
fn a_late_error_leaves_the_key_volatile_until_a_later_commit_syncs() {
    let raw = MemStore::new();
    let volatile = Volatile::new(raw.clone());
    let handle = volatile.handle();
    let faults = Faults::new(
        volatile,
        vec![(StoreOp::Upload, 0, FaultShape::ErrorAfterPublish(boom()))],
        1,
    )
    .volatile_on_late_error(handle.clone());
    assert_eq!(block_on(upload(&faults, 1)), Err(boom()));
    assert_eq!(handle.volatile(), vec![fkey(0, 1, 1)]);
    block_on(upload(&faults, 2)).unwrap();
    assert!(handle.volatile().is_empty());
}

/// The boot's scratch removal reaches the store under every simulator wrapper, with its count
/// and with no fault drawn.
#[test]
fn remove_scratch_is_forwarded_by_every_wrapper() {
    let raw = MemStore::new();
    raw.plant_scratch(1);
    assert_eq!(
        block_on(YieldEachCall::new(raw.clone()).remove_scratch()),
        Ok(1)
    );
    raw.plant_scratch(2);
    assert_eq!(block_on(Recorder::new(raw.clone()).remove_scratch()), Ok(2));
    raw.plant_scratch(3);
    assert_eq!(block_on(Volatile::new(raw.clone()).remove_scratch()), Ok(3));
    raw.plant_scratch(4);
    let faults = Faults::new(
        raw.clone(),
        vec![(StoreOp::Delete, 0, FaultShape::Error(boom()))],
        1,
    );
    assert_eq!(block_on(faults.remove_scratch()), Ok(4));
    assert_eq!(raw.scratch(), 0);
}

/// A staged file the engine unlinked, with no commit (a directory sync) after it.
fn unlinked_with_no_sync(revert: bool) -> (MemStore, VolatileHandle, Vec<u8>) {
    let raw = MemStore::new();
    let volatile = Volatile::new(raw.clone()).unsynced_deletes();
    let handle = volatile.handle();
    if revert {
        handle.revert_unsynced_entries();
    }
    let bytes = ltx(1, 1, 1, 1, None);
    block_on(async {
        let mut up = volatile
            .begin_upload(&fkey(0, 1, 1), Timestamp::from_unix_millis(5))
            .await
            .unwrap();
        up.write(&bytes).await.unwrap();
        up.commit().await.unwrap();
        volatile.delete(&[fkey(0, 1, 1)]).await.unwrap();
    });
    assert_eq!(handle.unsynced_unlinks(), vec![fkey(0, 1, 1)]);
    (raw, handle, bytes)
}

/// Under the block law an unlink the directory never synced is undone or kept, on a coin per
/// entry: undone, the file is back with its bytes and created-at time.
#[test]
fn an_unsynced_unlink_comes_back_or_stays_done_under_the_block_law() {
    let (mut back, mut gone) = (0, 0);
    for seed in 0..32 {
        let (raw, handle, bytes) = unlinked_with_no_sync(true);
        let lost = block_on(handle.power_loss(&raw, &SeededEntropy::new(seed))).unwrap();
        assert!(
            handle.unsynced_unlinks().is_empty(),
            "the loss settles every entry"
        );
        if lost == vec![(fkey(0, 1, 1), Lost::Restored)] {
            back += 1;
            let len = ReadLen::new(1 << 16).unwrap();
            assert_eq!(block_on(raw.read(&fkey(0, 1, 1), 0, len)).unwrap(), bytes);
            let info = block_on(raw.list(Level::new(0).unwrap(), None, PageLimit::MAX))
                .unwrap()
                .files[0];
            assert_eq!(
                block_on(raw.created_at(&info)).unwrap().at(),
                Timestamp::from_unix_millis(5)
            );
        } else {
            assert!(lost.is_empty(), "{lost:?}");
            gone += 1;
            assert_eq!(listed(&raw), 0);
        }
    }
    assert!(back > 0 && gone > 0, "{back} back, {gone} gone");
}

/// Without the block law, as under the all-or-nothing law, an unlink stays done.
#[test]
fn an_unsynced_unlink_stays_done_without_the_block_law() {
    for seed in 0..8 {
        let (raw, handle, _) = unlinked_with_no_sync(false);
        let lost = block_on(handle.power_loss(&raw, &SeededEntropy::new(seed))).unwrap();
        assert!(lost.is_empty());
        assert_eq!(listed(&raw), 0);
    }
}

/// A later commit syncs the directory: the unlink before it is durable.
#[test]
fn a_commit_after_an_unlink_makes_it_durable() {
    for seed in 0..8 {
        let raw = MemStore::new();
        let volatile = Volatile::new(raw.clone()).unsynced_deletes();
        let handle = volatile.handle();
        handle.revert_unsynced_entries();
        block_on(upload(&volatile, 1)).unwrap();
        block_on(volatile.delete(&[fkey(0, 1, 1)])).unwrap();
        block_on(upload(&volatile, 2)).unwrap();
        assert!(handle.unsynced_unlinks().is_empty());
        let lost = block_on(handle.power_loss(&raw, &SeededEntropy::new(seed))).unwrap();
        assert!(lost.is_empty());
        assert_eq!(listed(&raw), 1, "only the second file");
    }
}

/// The directory store syncs each directory it deleted from, so a delete it returned from is
/// durable: no power loss brings the file back.
#[test]
fn a_delete_syncs_the_directory_as_the_directory_store_does() {
    for seed in 0..16 {
        let raw = MemStore::new();
        let volatile = Volatile::new(raw.clone());
        let handle = volatile.handle();
        handle.revert_unsynced_entries();
        block_on(upload(&volatile, 1)).unwrap();
        block_on(volatile.delete(&[fkey(0, 1, 1)])).unwrap();
        assert!(handle.unsynced_unlinks().is_empty());
        let lost = block_on(handle.power_loss(&raw, &SeededEntropy::new(seed))).unwrap();
        assert!(lost.is_empty(), "{lost:?}");
        assert_eq!(listed(&raw), 0);
    }
}
