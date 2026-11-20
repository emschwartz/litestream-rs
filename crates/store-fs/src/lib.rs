//! [`ObjectStore`] on a directory, in the layout Litestream's file replica uses.
//!
//! `<root>/ltx/<level>/<min>-<max>.ltx` with the level in decimal (not the
//! four-hex-digit form S3 keys use; Go's file client and S3 client differ, see
//! `file/replica_client.go`). Go's `litestream restore` reads a directory this
//! store wrote and this store reads one Go wrote.
//!
//! A commit writes a scratch file next to the target (`<name>.<pid>-<n>.tmp`,
//! created exclusively, so two uploads to one key never share bytes), stamps
//! its mtime with `created_at` as Go does (`created_at` is the file's mtime),
//! fsyncs it, renames it into place and fsyncs the directory. Go stamps the
//! mtime after the rename; stamping first means no reader ever sees the file
//! with the wrong time. A scratch file left by a crash or a dropped upload ends
//! in `.tmp`, is not an LTX name, and is counted as a foreign name by `list`.
//!
//! Blocking: every future here does its file I/O inside `poll` and is ready at
//! once. That suits tests, the compat harness and a restore into a local
//! directory; a caller on an async executor should run it on a blocking thread.
//! `list` reads a whole level directory into memory, as Go does, so its memory
//! is proportional to the entries in one level, not to the page.

use std::fs::{self, DirEntry, File, OpenOptions};
use std::future::{Future, ready};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lsr_core::{
    CorruptCause, FileInfo, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, StoreError,
    StoreOp, Upload, build_listing, parse_file_name,
};
use lsr_ltx::{Timestamp, TxidRange};

/// Where Go's file replica keeps LTX files, under the replica path.
const LTX_DIR: &str = "ltx";

/// A replica kept in a directory.
#[derive(Debug)]
pub struct FsStore {
    root: PathBuf,
    scratch_counter: AtomicU64,
}

impl FsStore {
    /// A store rooted at the replica path. Touches no disk until used.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            scratch_counter: AtomicU64::new(0),
        }
    }

    /// The replica path.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory holding one level.
    #[must_use]
    pub fn level_dir(&self, level: Level) -> PathBuf {
        self.root.join(LTX_DIR).join(level.to_string())
    }

    /// The path of one object.
    #[must_use]
    pub fn file_path(&self, key: &FileKey) -> PathBuf {
        self.level_dir(key.level()).join(key.file_name())
    }

    fn list_now(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let entries = match fs::read_dir(self.level_dir(level)) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Listing::empty()),
            Err(err) => return Err(StoreError::from_io(StoreOp::List, None, &err)),
        };
        let mut files = Vec::new();
        let mut foreign = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|err| StoreError::from_io(StoreOp::List, None, &err))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            match classify(level, &entry, &name)? {
                Entry::File(info) => files.push(info),
                Entry::Foreign => foreign.push(name),
                Entry::Gone => {}
            }
        }
        Ok(build_listing(files, &foreign, after, limit))
    }

    fn read_now(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let fail = |err: io::Error| StoreError::from_io(StoreOp::Read, Some(*key), &err);
        let mut file = File::open(self.file_path(key)).map_err(fail)?;
        if !file.metadata().map_err(fail)?.is_file() {
            return Err(StoreError::Corrupt {
                key: Some(*key),
                cause: CorruptCause::NotARegularFile,
            });
        }
        file.seek(SeekFrom::Start(offset)).map_err(fail)?;
        let mut bytes = Vec::new();
        file.take(len.get()).read_to_end(&mut bytes).map_err(fail)?;
        Ok(bytes)
    }

    fn begin_now(&self, key: &FileKey, created_at: Timestamp) -> Result<FsUpload, StoreError> {
        let fail = |err: io::Error| StoreError::from_io(StoreOp::Upload, None, &err);
        let stamp = to_system_time(created_at).ok_or(StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::TimestampOutOfRange,
        })?;
        let dir = self.level_dir(key.level());
        fs::create_dir_all(&dir).map_err(fail)?;
        let serial = self.scratch_counter.fetch_add(1, Ordering::Relaxed);
        let scratch = dir.join(format!(
            "{}.{}-{serial}.tmp",
            key.file_name(),
            std::process::id()
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&scratch)
            .map_err(fail)?;
        Ok(FsUpload {
            key: *key,
            dir,
            target: self.file_path(key),
            scratch,
            file,
            stamp,
            created_at,
        })
    }

    fn delete_now(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        for key in keys {
            match fs::remove_file(self.file_path(key)) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(StoreError::from_io(StoreOp::Delete, None, &err)),
            }
        }
        Ok(())
    }
}

enum Entry {
    File(FileInfo),
    Foreign,
    /// Deleted between the directory read and the stat; a concurrent
    /// compaction does this, and the file is simply no longer there.
    Gone,
}

fn classify(level: Level, entry: &DirEntry, name: &str) -> Result<Entry, StoreError> {
    let Ok(range) = parse_file_name(name) else {
        return Ok(Entry::Foreign);
    };
    let key = FileKey::new(level, range);
    let meta = match entry.metadata() {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Entry::Gone),
        Err(err) => return Err(StoreError::from_io(StoreOp::List, Some(key), &err)),
    };
    if !meta.is_file() {
        return Ok(Entry::Foreign);
    }
    let modified = meta
        .modified()
        .map_err(|err| StoreError::from_io(StoreOp::List, Some(key), &err))?;
    let created_at = from_system_time(modified).ok_or(StoreError::Corrupt {
        key: Some(key),
        cause: CorruptCause::TimestampOutOfRange,
    })?;
    Ok(Entry::File(FileInfo {
        key,
        size: meta.len(),
        created_at,
    }))
}

fn to_system_time(ts: Timestamp) -> Option<SystemTime> {
    let millis = ts.unix_millis();
    let offset = Duration::from_millis(millis.unsigned_abs());
    if millis >= 0 {
        UNIX_EPOCH.checked_add(offset)
    } else {
        UNIX_EPOCH.checked_sub(offset)
    }
}

fn from_system_time(time: SystemTime) -> Option<Timestamp> {
    let (negative, offset) = match time.duration_since(UNIX_EPOCH) {
        Ok(after) => (false, after),
        Err(before) => (true, before.duration()),
    };
    signed_millis(negative, offset).map(Timestamp::from_unix_millis)
}

fn signed_millis(negative: bool, offset: Duration) -> Option<i64> {
    let millis = i64::try_from(offset.as_millis()).ok()?;
    if negative {
        millis.checked_neg()
    } else {
        Some(millis)
    }
}

impl ObjectStore for FsStore {
    type Upload = FsUpload;

    fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        ready(self.list_now(level, after, limit))
    }

    fn read(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        ready(self.read_now(key, offset, len))
    }

    fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> impl Future<Output = Result<FsUpload, StoreError>> + Send {
        ready(self.begin_now(key, created_at))
    }

    fn delete(&self, keys: &[FileKey]) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(self.delete_now(keys))
    }
}

/// An [`FsStore`] upload: an open scratch file beside the target.
#[derive(Debug)]
pub struct FsUpload {
    key: FileKey,
    dir: PathBuf,
    target: PathBuf,
    scratch: PathBuf,
    file: File,
    stamp: SystemTime,
    created_at: Timestamp,
}

impl FsUpload {
    fn commit_now(self) -> Result<FileInfo, StoreError> {
        let fail = |err: io::Error| StoreError::from_io(StoreOp::Upload, None, &err);
        let result = (|| {
            self.file.set_modified(self.stamp)?;
            self.file.sync_all()?;
            let size = self.file.metadata()?.len();
            fs::rename(&self.scratch, &self.target)?;
            // The rename is not durable until its directory entry is.
            File::open(&self.dir)?.sync_all()?;
            Ok(size)
        })();
        match result {
            Ok(size) => Ok(FileInfo {
                key: self.key,
                size,
                created_at: self.created_at,
            }),
            Err(err) => {
                // Best effort: a scratch file that survives is invisible anyway.
                let _removed = fs::remove_file(&self.scratch);
                Err(fail(err))
            }
        }
    }
}

impl Upload for FsUpload {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(
            self.file
                .write_all(chunk)
                .map_err(|err| StoreError::from_io(StoreOp::Upload, None, &err)),
        )
    }

    fn commit(self) -> impl Future<Output = Result<FileInfo, StoreError>> + Send {
        ready(self.commit_now())
    }

    fn abort(self) -> impl Future<Output = Result<(), StoreError>> + Send {
        ready(match fs::remove_file(&self.scratch) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StoreError::from_io(StoreOp::Upload, None, &err)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsr_core::conformance::{Failure, Harness, block_on};
    use lsr_core::file_name;
    use lsr_ltx::Txid;
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

        fn plant_foreign(&self, level: Level, name: &str) -> Result<(), Failure> {
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
                created_at: Timestamp::from_unix_millis(1_700_000_000_456),
            }]
        );
        let bytes = block_on(store.read(&key(0, 1, 2), 4, ReadLen::new(100).unwrap())).unwrap();
        assert_eq!(bytes, b"bytes");
        assert_eq!(all(&store, 9).files.len(), 1);
        assert_eq!(all(&store, 3), Listing::empty());
    }

    #[test]
    fn commit_lands_where_go_looks_with_the_stamp_as_mtime() {
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
            listing.skipped, 2,
            "both scratch files are counted, not listed"
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
    fn a_directory_named_like_a_key_is_foreign_and_unreadable() {
        let h = FsHarness::new();
        let target = key(0, 1, 1);
        fs::create_dir_all(h.store.file_path(&target)).unwrap();
        let listing = all(&h.store, 0);
        assert_eq!((listing.files.len(), listing.skipped), (0, 1));
        assert_eq!(
            block_on(h.store.read(&target, 0, ReadLen::new(1).unwrap())),
            Err(StoreError::Corrupt {
                key: Some(target),
                cause: CorruptCause::NotARegularFile
            })
        );
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
    }

    #[test]
    fn begin_upload_with_an_unrepresentable_stamp_is_corrupt_not_a_panic() {
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
}
