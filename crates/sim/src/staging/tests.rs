use lsr_core::{CreatedAt, ListedTime, SeededEntropy, StoreOp};
use lsr_ltx::{Txid, TxidRange};

use super::*;
use crate::exec::count_polls;
use crate::store::FaultShape;

fn key(n: u64) -> FileKey {
    let txid = Txid::new(n).unwrap();
    FileKey::new(Level::ZERO, TxidRange::new(txid, txid).unwrap())
}

fn entry(n: u64, bytes: &[u8]) -> (FileKey, (FileInfo, Vec<u8>)) {
    let info = FileInfo {
        key: key(n),
        size: u64::try_from(bytes.len()).unwrap(),
        listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(5))),
    };
    (key(n), (info, bytes.to_vec()))
}

#[test]
fn a_loss_the_engine_was_warned_of_goes_to_the_model_and_any_other_is_a_contract_break() {
    let before: Namespace = [entry(1, b"abcd"), entry(2, b"abcd"), entry(3, b"abcd")]
        .into_iter()
        .chain([entry(4, b"abcd"), entry(5, b"abcd")])
        .collect();
    let after: Namespace = [entry(2, b"ab"), entry(3, b"abcd"), entry(5, b"abXd")]
        .into_iter()
        .chain([entry(6, b"back")])
        .collect();
    let warned = [key(1), key(2)].into_iter().collect();
    let (lost, reverted) = judge(&before, &after, &warned);
    assert_eq!(
        lost,
        [(key(1), Lost::Dropped), (key(2), Lost::Torn { kept: 2 })]
    );
    assert_eq!(
        reverted,
        [
            Reverted::Lost(key(4)),
            Reverted::Changed(key(5)),
            Reverted::Returned(key(6))
        ]
    );
}

async fn put(store: &StagingRaw, n: u64, bytes: &[u8]) {
    let mut up = store
        .begin_upload(&key(n), Timestamp::from_unix_millis(7), WriteMode::Replace)
        .await
        .unwrap();
    up.write(bytes).await.unwrap();
    up.commit().await.unwrap();
}

/// The store as shipped makes every commit and delete durable, under either law.
#[test]
fn the_directory_store_keeps_every_commit_and_delete_across_a_power_loss() {
    for law in [
        CrashLaw::AllOrNothing,
        CrashLaw::drawn(3),
        CrashLaw::drawn(4),
    ] {
        let store = StagingRaw::fs(law);
        let volatile = VolatileHandle::default();
        let entropy = SeededEntropy::new(1);
        block_on(async {
            put(&store, 1, b"one").await;
            put(&store, 2, b"two").await;
            store.delete(&[key(1)]).await.unwrap();
            let lost = store.power_loss(&volatile, &entropy).await.unwrap();
            assert!(lost.is_empty());
        });
        let (reverted, losses) = store.take_log();
        assert_eq!(reverted, [], "{law:?}");
        assert_eq!(losses.len(), 1);
        let listed = block_on(store.list(Level::ZERO, None, PageLimit::MAX)).unwrap();
        let keys: Vec<FileKey> = listed.files.iter().map(|f| f.key).collect();
        assert_eq!(keys, [key(2)]);
    }
}

/// A scratch file a crashed upload left is neither listed nor judged: the store ignores it, and
/// a power loss may take it.
#[test]
fn an_upload_dropped_before_its_commit_leaves_nothing_the_judge_sees() {
    let store = StagingRaw::fs(CrashLaw::AllOrNothing);
    block_on(async {
        let mut up = store
            .begin_upload(&key(1), Timestamp::from_unix_millis(7), WriteMode::Replace)
            .await
            .unwrap();
        up.write(b"half").await.unwrap();
        drop(up);
        store
            .power_loss(&VolatileHandle::default(), &SeededEntropy::new(1))
            .await
            .unwrap();
    });
    let (reverted, losses) = store.take_log();
    assert_eq!(reverted, []);
    assert!(losses.first().is_some_and(|l| l.undone > 0), "{losses:?}");
}

/// The directory store hands a send its file held open through every wrapper the simulator
/// puts in front of staging, and a read through it is to each of them what a read by range
/// is: one yield, one read counted, one call of the fault schedule. `MemStore` has nothing to
/// hold open, and is read by range.
#[test]
fn a_file_held_open_is_yielded_counted_and_faulted_read_by_read() {
    let raw = StagingRaw::fs(CrashLaw::AllOrNothing);
    block_on(put(&raw, 1, b"0123456789"));
    let recorder = Recorder::new(Volatile::new(raw));
    let log = recorder.log();
    let refused = StoreError::Transient {
        op: StoreOp::Read,
        kind: std::io::ErrorKind::Other,
    };
    let store = Faults::new(
        YieldEachCall::new(recorder),
        vec![
            (StoreOp::Read, 1, FaultShape::Error(refused.clone())),
            (StoreOp::Read, 2, FaultShape::FlipByteOnRead { at: 3 }),
        ],
        1,
    );
    let four = ReadLen::new(4).unwrap();
    let (held, polls) = count_polls(store.open(&key(1)));
    assert_eq!(polls, 0, "the open is no crash point of its own");
    let mut held = held
        .unwrap()
        .expect("the directory store holds a file open");
    let (first, polls) = count_polls(held.read(0, four));
    assert_eq!((first, polls), (Ok(b"0123".to_vec()), 1));
    assert_eq!(block_on(held.read(4, four)), Err(refused));
    // The byte at 3 comes back with its lowest bit flipped, and the file is noted.
    assert_eq!(block_on(held.read(2, four)), Ok(b"2245".to_vec()));
    assert_eq!(store.handle().flipped(), [key(1)]);
    assert_eq!(block_on(held.read(8, four)), Ok(b"89".to_vec()));
    // The refused read never reached the store.
    assert_eq!(log.counts().reads, 3);

    let mem = StagingRaw::Mem(MemStore::new());
    block_on(put(&mem, 1, b"0123456789"));
    let mem = Faults::new(
        YieldEachCall::new(Recorder::new(Volatile::new(mem))),
        Vec::new(),
        1,
    );
    assert!(block_on(mem.open(&key(1))).unwrap().is_none());
}
