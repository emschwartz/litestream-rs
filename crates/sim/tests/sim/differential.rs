//! The simulator's store doubles against the real store, on the same operation lists.
//!
//! The simulator's staging store is `MemStore` in most runs and the real directory store
//! (`lsr-store-fs`) on the in-memory filesystem (`lsr_sim::memfs`) in the rest. Each double hides
//! whatever it does differently from the code it stands for. So one drawn list of store calls
//! (uploads, aborted uploads, deletes, paged listings, ranged reads, created-at reads) runs on
//! three stores, and every answer must be the same, errors included:
//!
//! - `FsStore` on a temporary directory of the host's filesystem: the real code on a real disk;
//! - `FsStore` on `MemFs`: the real code on the simulator's filesystem, which holds `MemFs` to
//!   the host's;
//! - `MemStore`: the simulator's double of a store, which this holds to the directory store.
//!
//! Timestamps are drawn on whole milliseconds on both sides of the epoch (the directory store
//! keeps created-at in the mtime, and the host keeps nanoseconds).

#[cfg(test)]
mod diff {
    use std::fmt::Debug;

    use lsr_core::conformance::block_on;
    use lsr_core::{
        FileInfo, FileKey, Level, ListedTime, MemStore, ObjectStore, PageLimit, ReadLen, Upload,
        WriteMode,
    };
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use lsr_sim::crash_law::CrashLaw;
    use lsr_sim::memfs::MemFs;
    use lsr_store_fs::FsStore;
    use proptest::prelude::*;

    #[derive(Clone, Copy, Debug)]
    struct Key {
        level: u8,
        min: u64,
        span: u64,
    }

    impl Key {
        fn key(self) -> FileKey {
            let min = Txid::new(self.min).unwrap();
            let max = Txid::new(self.min.checked_add(self.span).unwrap()).unwrap();
            FileKey::new(
                Level::new(self.level).unwrap(),
                TxidRange::new(min, max).unwrap(),
            )
        }
    }

    #[derive(Clone, Debug)]
    enum Op {
        Put {
            key: Key,
            bytes: Vec<u8>,
            created_ms: i64,
            abort: bool,
        },
        Delete(Vec<Key>),
        List {
            level: u8,
            after: Option<Key>,
            limit: u16,
        },
        Read {
            key: Key,
            offset: u64,
            len: u32,
        },
        CreatedAt {
            key: Key,
            size: Option<u64>,
        },
        ReadWithCreatedAt {
            key: Key,
            len: u32,
        },
    }

    fn key() -> impl Strategy<Value = Key> {
        (prop::sample::select(vec![0_u8, 1, 9]), 1_u64..6, 0_u64..3)
            .prop_map(|(level, min, span)| Key { level, min, span })
    }

    fn op() -> impl Strategy<Value = Op> {
        let created = prop_oneof![-5_000_i64..5_000, 1_600_000_000_000_i64..1_800_000_000_000];
        prop_oneof![
            4 => (key(), prop::collection::vec(any::<u8>(), 0..40), created, prop::bool::weighted(0.1))
                .prop_map(|(key, bytes, created_ms, abort)| Op::Put { key, bytes, created_ms, abort }),
            2 => prop::collection::vec(key(), 0..4).prop_map(Op::Delete),
            3 => (prop::sample::select(vec![0_u8, 1, 9]), prop::option::of(key()), 1_u16..4)
                .prop_map(|(level, after, limit)| Op::List { level, after, limit }),
            2 => (key(), 0_u64..50, 1_u32..50).prop_map(|(key, offset, len)| Op::Read { key, offset, len }),
            1 => (key(), prop::option::of(0_u64..40)).prop_map(|(key, size)| Op::CreatedAt { key, size }),
            1 => (key(), 1_u32..50).prop_map(|(key, len)| Op::ReadWithCreatedAt { key, len }),
        ]
    }

    /// The `FileInfo` a caller holds for `key`: from a listing of the store, or a made-up one
    /// of the given size when the key is not listed (a stale listing).
    fn info<S: ObjectStore>(store: &S, key: FileKey, size: Option<u64>) -> FileInfo {
        let listed = block_on(store.list(key.level(), None, PageLimit::MAX))
            .ok()
            .and_then(|l| l.files.into_iter().find(|f| f.key == key));
        match (listed, size) {
            (Some(found), None) => found,
            (Some(found), Some(size)) => FileInfo { size, ..found },
            (None, size) => FileInfo {
                key,
                size: size.unwrap_or(3),
                listed: ListedTime::LastModified(Timestamp::from_unix_millis(0)),
            },
        }
    }

    fn apply<S: ObjectStore>(store: &S, op: &Op) -> String {
        fn show<T: Debug>(value: T) -> String {
            format!("{value:?}")
        }
        match op {
            Op::Put {
                key,
                bytes,
                created_ms,
                abort,
            } => block_on(async {
                let mut up = store
                    .begin_upload(
                        &key.key(),
                        Timestamp::from_unix_millis(*created_ms),
                        WriteMode::Replace,
                    )
                    .await?;
                up.write(bytes).await?;
                if *abort {
                    up.abort().await.map(|()| None)
                } else {
                    up.commit().await.map(Some)
                }
            })
            .map_or_else(show, show),
            Op::Delete(keys) => {
                let keys: Vec<FileKey> = keys.iter().map(|k| k.key()).collect();
                show(block_on(store.delete(&keys)))
            }
            Op::List {
                level,
                after,
                limit,
            } => show(block_on(store.list(
                Level::new(*level).unwrap(),
                after.map(|k| k.key().range()),
                PageLimit::new(*limit).unwrap(),
            ))),
            Op::Read { key, offset, len } => show(block_on(store.read(
                &key.key(),
                *offset,
                ReadLen::new(*len).unwrap(),
            ))),
            Op::CreatedAt { key, size } => {
                show(block_on(store.created_at(&info(store, key.key(), *size))))
            }
            Op::ReadWithCreatedAt { key, len } => {
                show(block_on(store.read_with_created_at(
                    &info(store, key.key(), None),
                    ReadLen::new(*len).unwrap(),
                )))
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]

        /// `FsStore` gives the same answers on `MemFs` as on the host's filesystem, and
        /// `MemStore` gives the same answers as `FsStore`.
        #[test]
        fn the_doubles_answer_as_the_real_store_does(ops in prop::collection::vec(op(), 1..40)) {
            let dir = tempfile::tempdir().unwrap();
            let real = FsStore::new(dir.path().join("staging"));
            let memfs = FsStore::with_fs("/staging", MemFs::new(CrashLaw::AllOrNothing));
            let mem = MemStore::new();
            for (step, op) in ops.iter().enumerate() {
                let want = apply(&real, op);
                prop_assert_eq!(&apply(&memfs, op), &want, "MemFs, step {}: {:?}", step, op);
                prop_assert_eq!(&apply(&mem, op), &want, "MemStore, step {}: {:?}", step, op);
            }
        }
    }
}
