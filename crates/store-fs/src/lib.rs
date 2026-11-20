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
//! with the wrong time.
//!
//! Scratch names: the counter restarts with every store and the pid repeats
//! across restarts (it is 1 in a container), so a name can already be taken by
//! a crashed upload's leftover. `begin_upload` then tries the next counter
//! value, a bounded number of times, and gives up with
//! [`StoreError::ScratchExhausted`]. A scratch file (this store's, or Go's
//! `<name>.tmp`) is neither an object nor a foreign name: `list` neither
//! returns nor counts it. Nothing removes one automatically, because a scratch
//! file may belong to an upload still running in another process; an operator
//! clears them.
//!
//! Directories are made durable too: a level directory the store creates, and
//! each directory above it that it creates, has its parent fsynced before the
//! upload that needed it can commit. A crash window remains only for a
//! directory an earlier, crashed process made and never synced.
//!
//! Types: a name that parses as a key is the file it names, and a symlink to a
//! regular file is that file, in `list` and `read` alike (Go's reads follow
//! links). A key-named entry that is anything else (a directory, a FIFO, a
//! dangling link) makes `list` fail with [`CorruptCause::NotARegularFile`]: a
//! name that claims to be history and cannot be read is never skipped.
//! `read` checks the type before it opens, so a FIFO cannot block it.
//!
//! A commit that returns an error may still have published the object (the
//! rename can land before a directory fsync fails): see the `ObjectStore`
//! contract; list again.
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

/// How many scratch names an upload tries before giving up.
const SCRATCH_TRIES: u32 = 64;

/// Creates `dir` and any missing ancestors, top down, calling `sync` on the
/// parent of each directory it made so the new entry is durable. A directory
/// that turns out to exist (another creator won the race) gets the same sync.
fn create_dirs_durably(
    dir: &Path,
    sync: &mut impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut at = dir;
    while !at.try_exists()? {
        missing.push(at);
        match at.parent() {
            Some(parent) => at = parent,
            None => break,
        }
    }
    for path in missing.into_iter().rev() {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync(parent)?;
    }
    Ok(())
}

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
        let not_a_file = || StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::NotARegularFile,
        };
        let path = self.file_path(key);
        // Before the open: opening a FIFO blocks until a writer appears.
        if !fs::metadata(&path).map_err(fail)?.is_file() {
            return Err(not_a_file());
        }
        let mut file = File::open(&path).map_err(fail)?;
        let meta = file.metadata().map_err(fail)?;
        if !meta.is_file() {
            return Err(not_a_file());
        }
        // Past the end is empty at every offset; seeking beyond i64::MAX fails.
        if offset >= meta.len() {
            return Ok(Vec::new());
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
        create_dirs_durably(&dir, &mut |path| File::open(path)?.sync_all()).map_err(fail)?;
        let (scratch, file) = self.open_scratch(&dir, key)?;
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

    /// Creates the key's scratch file under the first free name.
    fn open_scratch(&self, dir: &Path, key: &FileKey) -> Result<(PathBuf, File), StoreError> {
        for _ in 0..SCRATCH_TRIES {
            let serial = self.scratch_counter.fetch_add(1, Ordering::Relaxed);
            let scratch = dir.join(format!(
                "{}.{}-{serial}.tmp",
                key.file_name(),
                std::process::id()
            ));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&scratch)
            {
                Ok(file) => return Ok((scratch, file)),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(StoreError::from_io(StoreOp::Upload, None, &err)),
            }
        }
        Err(StoreError::ScratchExhausted { key: *key })
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
    let not_a_file = StoreError::Corrupt {
        key: Some(key),
        cause: CorruptCause::NotARegularFile,
    };
    // Follows symlinks, as the open in `read` does.
    let meta = match fs::metadata(entry.path()) {
        Ok(meta) => meta,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            let dangling = entry.file_type().is_ok_and(|kind| kind.is_symlink());
            return if dangling {
                Err(not_a_file)
            } else {
                Ok(Entry::Gone)
            };
        }
        Err(err) => return Err(StoreError::from_io(StoreOp::List, Some(key), &err)),
    };
    if !meta.is_file() {
        return Err(not_a_file);
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

/// Whole milliseconds, floored: a time before the epoch rounds away from zero,
/// as Go's `UnixMilli` does, so `created < t` keeps its answer for every whole
/// millisecond `t`.
fn signed_millis(negative: bool, offset: Duration) -> Option<i64> {
    let millis = i64::try_from(offset.as_millis()).ok()?;
    if !negative {
        return Some(millis);
    }
    let partial = !offset.subsec_nanos().is_multiple_of(1_000_000);
    let rounded_up = if partial {
        millis.checked_add(1)?
    } else {
        millis
    };
    rounded_up.checked_neg()
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
        assert_eq!(listing.skipped, 0, "scratch files are not foreign names");
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
        create_dirs_durably(&leaf, &mut |path| {
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
        create_dirs_durably(&leaf, &mut |path| {
            synced.push(path.to_path_buf());
            Ok(())
        })
        .unwrap();
        assert!(synced.is_empty(), "nothing to make, nothing to sync");
    }

    #[test]
    fn a_failed_directory_sync_fails_the_upload_before_it_can_commit() {
        let dir = tempfile::tempdir().unwrap();
        let err = create_dirs_durably(&dir.path().join("x"), &mut |_| {
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
