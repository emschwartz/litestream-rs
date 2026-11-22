use super::*;
use lsr_core::conformance::{Failure, Harness, block_on};
use lsr_core::file_name;
use lsr_ltx::Txid;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;

struct FsHarness {
    dir: TempDir,
    store: FsStore,
}

impl FsHarness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path().join("replica"));
        Self { dir, store }
    }
}

impl Harness for FsHarness {
    type Store = FsStore;

    fn store(&self) -> &FsStore {
        &self.store
    }

    fn plant_unrecognized(&self, level: Level, name: &str) -> Result<(), Failure> {
        let dir = self.store.level_dir(level);
        fs::create_dir_all(&dir).map_err(|e| Failure(e.to_string()))?;
        fs::write(dir.join(name), b"stray").map_err(|e| Failure(e.to_string()))
    }
}

lsr_core::conformance_suite!(FsHarness::new);

fn key(level: u8, min: u64, max: u64) -> FileKey {
    let range = TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap();
    FileKey::new(Level::new(level).unwrap(), range)
}

fn all(store: &FsStore, level: u8) -> Listing {
    let level = Level::new(level).unwrap();
    block_on(store.list(level, None, PageLimit::new(1000).unwrap())).unwrap()
}

#[test]
fn reads_a_directory_laid_out_by_hand_as_go_lays_it_out() {
    let dir = tempfile::tempdir().unwrap();
    let level0 = dir.path().join("ltx").join("0");
    fs::create_dir_all(&level0).unwrap();
    let name = "0000000000000001-0000000000000002.ltx";
    let file = File::create(level0.join(name)).unwrap();
    (&file).write_all(b"LTX-bytes").unwrap();
    file.set_modified(to_system_time(Timestamp::from_unix_millis(1_700_000_000_456)).unwrap())
        .unwrap();
    drop(file);
    fs::create_dir_all(dir.path().join("ltx").join("9")).unwrap();
    fs::write(
        dir.path()
            .join("ltx")
            .join("9")
            .join("0000000000000001-0000000000000001.ltx"),
        b"snap",
    )
    .unwrap();

    let store = FsStore::new(dir.path());
    let listing = all(&store, 0);
    assert_eq!(
        listing.files,
        vec![FileInfo {
            key: key(0, 1, 2),
            size: 9,
            listed: ListedTime::Created(CreatedAt::mtime(Timestamp::from_unix_millis(
                1_700_000_000_456
            ))),
        }]
    );
    let bytes = block_on(store.read(&key(0, 1, 2), 4, ReadLen::new(100).unwrap())).unwrap();
    assert_eq!(bytes, b"bytes");
    assert_eq!(all(&store, 9).files.len(), 1);
    assert_eq!(all(&store, 3), Listing::empty());
}

#[test]
fn commit_lands_where_go_looks_with_the_created_at_as_mtime() {
    let h = FsHarness::new();
    let target = key(9, 1, 7);
    block_on(async {
        let mut up = h
            .store
            .begin_upload(&target, Timestamp::from_unix_millis(5_000))
            .await
            .unwrap();
        up.write(b"data").await.unwrap();
        up.commit().await.unwrap();
    });
    let path = h
        .dir
        .path()
        .join("replica/ltx/9/0000000000000001-0000000000000007.ltx");
    assert_eq!(fs::read(&path).unwrap(), b"data");
    let mtime = fs::metadata(&path).unwrap().modified().unwrap();
    assert_eq!(
        from_system_time(mtime),
        Some(Timestamp::from_unix_millis(5_000))
    );
    let leftovers: Vec<_> = fs::read_dir(path.parent().unwrap()).unwrap().collect();
    assert_eq!(leftovers.len(), 1, "scratch file removed by the rename");
}

#[test]
fn fs_commit_size_mismatch_is_caught_before_rename() {
    let h = FsHarness::new();
    let target = key(9, 1, 7);
    let err = block_on(async {
        let mut up = h
            .store
            .begin_upload(&target, Timestamp::from_unix_millis(5_000))
            .await
            .unwrap();
        up.write(b"data").await.unwrap();
        up.file.set_len(2).unwrap();
        up.commit().await.unwrap_err()
    });
    assert!(
        matches!(
            err,
            StoreError::Corrupt {
                cause: CorruptCause::CommitSizeMismatch {
                    wrote: 4,
                    stored: 2,
                    removed: true
                },
                ..
            }
        ),
        "{err:?}"
    );
    let dir = h.dir.path().join("replica/ltx/9");
    assert_eq!(fs::read_dir(dir).unwrap().count(), 0, "nothing published");
}

#[test]
fn a_crash_between_scratch_write_and_rename_is_invisible() {
    let h = FsHarness::new();
    let target = key(0, 1, 1);
    // The process dies after writing the scratch file: model it by dropping
    // the upload, which leaves the file behind exactly as a crash would.
    block_on(async {
        let mut up = h
            .store
            .begin_upload(&target, Timestamp::from_unix_millis(1))
            .await
            .unwrap();
        up.write(b"partial").await.unwrap();
    });
    let go_style = h
        .store
        .level_dir(Level::ZERO)
        .join(format!("{}.tmp", target.file_name()));
    fs::write(go_style, b"go partial").unwrap();

    let listing = all(&h.store, 0);
    assert!(listing.files.is_empty());
    assert_eq!(
        listing.skipped, 0,
        "scratch files are not unrecognized names"
    );
    assert_eq!(
        block_on(h.store.read(&target, 0, ReadLen::new(8).unwrap())),
        Err(StoreError::NotFound { key: target })
    );
    block_on(async {
        let mut up = h
            .store
            .begin_upload(&target, Timestamp::from_unix_millis(2))
            .await
            .unwrap();
        up.write(b"whole").await.unwrap();
        up.commit().await.unwrap();
    });
    assert_eq!(all(&h.store, 0).files.len(), 1);
}

#[test]
fn a_directory_named_like_a_key_fails_the_listing_and_the_read() {
    let h = FsHarness::new();
    let target = key(0, 1, 1);
    fs::create_dir_all(h.store.file_path(&target)).unwrap();
    let corrupt = StoreError::Corrupt {
        key: Some(target),
        cause: CorruptCause::NotARegularFile,
    };
    let level = Level::ZERO;
    assert_eq!(
        block_on(h.store.list(level, None, PageLimit::new(10).unwrap())),
        Err(corrupt)
    );
    assert_eq!(
        block_on(h.store.read(&target, 0, ReadLen::new(1).unwrap())),
        Err(StoreError::Corrupt {
            key: Some(target),
            cause: CorruptCause::NotARegularFile
        })
    );
}

#[cfg(unix)]
#[test]
fn a_symlink_to_a_regular_file_is_that_file() {
    let h = FsHarness::new();
    let real = key(0, 1, 1);
    let linked = key(0, 2, 2);
    block_on(async {
        let mut up = h
            .store
            .begin_upload(&real, Timestamp::from_unix_millis(7))
            .await
            .unwrap();
        up.write(b"bytes").await.unwrap();
        up.commit().await.unwrap();
    });
    std::os::unix::fs::symlink(h.store.file_path(&real), h.store.file_path(&linked)).unwrap();
    let listing = all(&h.store, 0);
    let keys: Vec<_> = listing.files.iter().map(|f| f.key).collect();
    assert_eq!(keys, [real, linked]);
    assert_eq!(listing.files[1].size, 5);
    assert_eq!(
        block_on(h.store.read(&linked, 0, ReadLen::new(9).unwrap())),
        Ok(b"bytes".to_vec())
    );
}

#[cfg(unix)]
#[test]
fn a_dangling_symlink_named_like_a_key_fails_the_listing() {
    let h = FsHarness::new();
    let target = key(0, 1, 1);
    fs::create_dir_all(h.store.level_dir(Level::ZERO)).unwrap();
    std::os::unix::fs::symlink("/nonexistent/lsr", h.store.file_path(&target)).unwrap();
    assert_eq!(
        block_on(h.store.list(Level::ZERO, None, PageLimit::new(10).unwrap())),
        Err(StoreError::Corrupt {
            key: Some(target),
            cause: CorruptCause::NotARegularFile
        })
    );
}

#[cfg(unix)]
#[test]
fn reading_a_fifo_named_like_a_key_is_refused_not_hung() {
    let h = FsHarness::new();
    let target = key(0, 1, 1);
    fs::create_dir_all(h.store.level_dir(Level::ZERO)).unwrap();
    let made = std::process::Command::new("mkfifo")
        .arg(h.store.file_path(&target))
        .status()
        .unwrap();
    assert!(made.success());
    assert_eq!(
        block_on(h.store.read(&target, 0, ReadLen::new(1).unwrap())),
        Err(StoreError::Corrupt {
            key: Some(target),
            cause: CorruptCause::NotARegularFile
        })
    );
}

#[test]
fn an_upload_skips_scratch_names_a_crash_left_and_gives_up_when_all_are_taken() {
    let h = FsHarness::new();
    let target = key(0, 1, 1);
    let dir = h.store.level_dir(Level::ZERO);
    fs::create_dir_all(&dir).unwrap();
    let name = |serial: u64| {
        dir.join(format!(
            "{}.{}-{serial}.tmp",
            target.file_name(),
            std::process::id()
        ))
    };
    fs::write(name(0), b"left by a crash").unwrap();
    block_on(
        h.store
            .begin_upload(&target, Timestamp::from_unix_millis(1)),
    )
    .unwrap();
    for serial in 0..200 {
        fs::write(name(serial), b"x").unwrap();
    }
    let err = block_on(
        h.store
            .begin_upload(&target, Timestamp::from_unix_millis(1)),
    );
    assert_eq!(
        err.err(),
        Some(StoreError::ScratchExhausted { key: target })
    );
}

#[test]
fn missing_directories_are_synced_into_their_parents_top_down() {
    let dir = tempfile::tempdir().unwrap();
    let leaf = dir.path().join("a").join("b").join("c");
    let mut synced = Vec::new();
    create_dirs_durably(&RealFs, &leaf, &mut |path| {
        synced.push(path.to_path_buf());
        Ok(())
    })
    .unwrap();
    assert!(leaf.is_dir());
    let expected = [
        dir.path().to_path_buf(),
        dir.path().join("a"),
        dir.path().join("a").join("b"),
    ];
    assert_eq!(synced, expected);
    synced.clear();
    create_dirs_durably(&RealFs, &leaf, &mut |path| {
        synced.push(path.to_path_buf());
        Ok(())
    })
    .unwrap();
    assert!(synced.is_empty(), "nothing to make, nothing to sync");
}

#[test]
fn a_failed_directory_sync_fails_the_upload_before_it_can_commit() {
    let dir = tempfile::tempdir().unwrap();
    let err = create_dirs_durably(&RealFs, &dir.path().join("x"), &mut |_| {
        Err(io::Error::other("disk"))
    });
    assert!(err.is_err());
}

#[test]
fn timestamps_before_the_epoch_survive_and_absurd_ones_are_refused() {
    for millis in [-86_400_000, 0, 1_700_000_000_000] {
        let ts = Timestamp::from_unix_millis(millis);
        assert_eq!(from_system_time(to_system_time(ts).unwrap()), Some(ts));
    }
    assert_eq!(signed_millis(false, Duration::MAX), None);
    assert_eq!(signed_millis(true, Duration::MAX), None);
    assert_eq!(signed_millis(true, Duration::from_millis(5)), Some(-5));
    assert_eq!(signed_millis(true, Duration::from_micros(1500)), Some(-2));
}

#[test]
fn begin_upload_with_an_unrepresentable_created_at_is_corrupt_not_a_panic() {
    let h = FsHarness::new();
    let target = key(0, 1, 1);
    let err = block_on(
        h.store
            .begin_upload(&target, Timestamp::from_unix_millis(i64::MIN)),
    );
    // Representable on some platforms; where it is not, the error is typed.
    if let Err(err) = err {
        assert_eq!(
            err,
            StoreError::Corrupt {
                key: Some(target),
                cause: CorruptCause::TimestampOutOfRange
            }
        );
    }
}

#[test]
fn name_helpers_agree_with_core() {
    let target = key(2, 3, 4);
    assert_eq!(
        h_path_tail(&target),
        format!("ltx/2/{}", file_name(target.range()))
    );
}

fn h_path_tail(key: &FileKey) -> String {
    let store = FsStore::new("/r");
    store
        .file_path(key)
        .strip_prefix("/r")
        .unwrap()
        .to_string_lossy()
        .into_owned()
}
