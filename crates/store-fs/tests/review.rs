//! FsStore defects found by the blind review.

#[cfg(test)]
mod review {

    use std::fs::{self, File};
    use std::time::{Duration, UNIX_EPOCH};

    use lsr_core::conformance::block_on;
    use lsr_core::{
        FileKey, Inventory, KeyTarget, Level, ObjectStore, PageLimit, PlanOutcome, ReadLen, Upload,
        plan,
    };
    use lsr_ltx::{Timestamp, Txid, TxidRange};
    use lsr_store_fs::FsStore;

    fn key(level: u8, min: u64, max: u64) -> FileKey {
        let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
        FileKey::new(Level::new(level).unwrap(), range)
    }

    fn put(store: &FsStore, k: FileKey, bytes: &[u8]) {
        block_on(async {
            let mut up = store
                .begin_upload(&k, Timestamp::from_unix_millis(1_000))
                .await
                .unwrap();
            up.write(bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }

    /// A process that crashed mid-upload left its scratch file; the restarted
    /// process (same pid, as pid 1 in a container always is, counter back at 0)
    /// cannot begin an upload of the same key.
    #[test]
    fn a_restarted_process_can_upload_a_key_whose_scratch_a_crash_left() {
        let dir = tempfile::tempdir().unwrap();
        let target = key(0, 1, 1);
        let before_crash = FsStore::new(dir.path());
        block_on(async {
            let mut up = before_crash
                .begin_upload(&target, Timestamp::from_unix_millis(1))
                .await
                .unwrap();
            up.write(b"partial").await.unwrap();
            drop(up);
        });
        let after_restart = FsStore::new(dir.path());
        let got = block_on(after_restart.begin_upload(&target, Timestamp::from_unix_millis(2)));
        assert!(got.is_ok(), "begin_upload after restart: {:?}", got.err());
    }

    /// The store's own scratch from a dropped upload is reported as a stray
    /// foreign name, and nothing ever removes it.
    #[test]
    fn a_dropped_upload_leaves_no_foreign_name() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path());
        let target = key(0, 1, 1);
        block_on(async {
            let mut up = store
                .begin_upload(&target, Timestamp::from_unix_millis(1))
                .await
                .unwrap();
            up.write(b"half").await.unwrap();
        });
        let listing = block_on(store.list(Level::ZERO, None, PageLimit::MAX)).unwrap();
        assert_eq!(listing.skipped, 0, "own scratch counted as a foreign name");
    }

    /// The contract: a read at or past the end returns no bytes. An offset above
    /// i64::MAX makes lseek fail with EINVAL, surfaced as a retryable error.
    #[test]
    fn a_read_far_past_the_end_is_empty_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path());
        let target = key(0, 1, 1);
        put(&store, target, b"0123");
        let got = block_on(store.read(&target, u64::MAX, ReadLen::new(4).unwrap()));
        assert_eq!(got, Ok(Vec::new()));
    }

    /// Go lists a pre-epoch mtime with nanosecond precision and keeps a file for
    /// AsOf(t) when mtime < t. Truncating -1.5 ms toward zero gives -1 ms, so
    /// AsOf(-1 ms) drops a file Go keeps; flooring to -2 ms preserves the order.
    #[test]
    fn a_pre_epoch_sub_millisecond_mtime_floors() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path());
        let target = key(0, 1, 1);
        put(&store, target, b"x");
        let created_at = UNIX_EPOCH
            .checked_sub(Duration::from_micros(1_500))
            .unwrap();
        File::options()
            .write(true)
            .open(store.file_path(&target))
            .unwrap()
            .set_modified(created_at)
            .unwrap();
        let listing = block_on(store.list(Level::ZERO, None, PageLimit::MAX)).unwrap();
        let created = listing
            .files
            .first()
            .map(|f| f.created_at().unwrap().at().unix_millis());
        assert_eq!(created, Some(-2));
    }

    /// Go's file client lists a symlink named like a key (Readdir plus
    /// ParseFilename, no type check) and reads through it. This store calls it
    /// foreign, so Latest plans to an earlier state and says Plan: shorter than
    /// Go's restore of the same directory, and read() disagrees with list().
    #[cfg(unix)]
    #[test]
    fn a_symlinked_ltx_file_is_not_silently_dropped_from_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path());
        put(&store, key(9, 1, 1), b"snap");
        let real = dir.path().join("elsewhere.ltx");
        fs::write(&real, b"l0").unwrap();
        fs::create_dir_all(store.level_dir(Level::ZERO)).unwrap();
        std::os::unix::fs::symlink(&real, store.file_path(&key(0, 2, 2))).unwrap();

        let read = block_on(store.read(&key(0, 2, 2), 0, ReadLen::new(8).unwrap()));
        assert_eq!(read, Ok(b"l0".to_vec()), "read follows the link");
        let inv = block_on(Inventory::load(&store)).unwrap();
        let out = plan(&inv, KeyTarget::Latest);
        let reaches = if let PlanOutcome::Plan(chain) = &out {
            Some(chain.reaches().get())
        } else {
            None
        };
        assert_ne!(
            reaches,
            Some(1),
            "Latest silently stopped before the listed-by-Go file: {out:?}"
        );
    }
}
