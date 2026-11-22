//! [`ObjectStore`] on a directory, in the layout Litestream's file replica uses.
//!
//! `<root>/ltx/<level>/<min>-<max>.ltx` with the level in decimal (not the
//! four-hex-digit form S3 keys use; Go's file client and S3 client differ, see
//! `file/replica_client.go`). Go's `litestream restore` reads a directory this
//! store wrote and this store reads one Go wrote.
//!
//! A commit writes a scratch file next to the target (`<name>.<pid>-<n>.tmp`,
//! created exclusively, so two uploads to one key never share bytes), sets
//! its mtime to the created-at time as Go does (the created-at time is the file's mtime),
//! fsyncs it, renames it into place and fsyncs the directory. Go sets the
//! mtime after the rename; setting it first means no reader ever sees the file
//! with the wrong time.
//!
//! Scratch names: the counter restarts with every store and the pid repeats
//! across restarts (it is 1 in a container), so a name can already be taken by
//! a crashed upload's leftover. `begin_upload` then tries the next counter
//! value, a bounded number of times, and gives up with
//! [`StoreError::ScratchExhausted`]. A scratch file (this store's, or Go's
//! `<name>.tmp`) is neither an object nor an unrecognized name: `list` neither
//! returns nor counts it. Nothing removes one automatically, because a scratch
//! file may belong to an upload still running in another process; an operator
//! clears them.
//!
//! A delete removes its keys and then syncs each level directory it named, once per call, so
//! a deleted file cannot come back after a power loss; a directory sync costs about 0.14 ms
//! per call on ext4 (measured with the change that added it), and the engine deletes in
//! batches.
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

use journal::{Step, note};

use lsr_core::{
    CorruptCause, CreatedAt, FileInfo, FileKey, Level, ListedTime, Listing, ObjectStore, PageLimit,
    ReadLen, StoreError, StoreOp, Upload, build_listing, parse_file_name,
};
use lsr_ltx::{Timestamp, TxidRange};

/// Where Go's file replica keeps LTX files, under the replica path.
const LTX_DIR: &str = "ltx";

/// How many scratch names an upload tries before giving up.
const SCRATCH_TRIES: u32 = 64;

/// Creates `dir` and any missing ancestors, top down, calling `sync` on the
/// parent of each directory it made so the new entry is durable. A directory
/// that turns out to exist (another creator won the race) gets the same sync.
/// Syncs `file`'s bytes, and notes it in the test journal: one call, so the sync and the
/// note cannot part.
fn sync_file(file: &File, path: &Path) -> io::Result<()> {
    file.sync_all()?;
    note(|| Step::SyncFile(path.to_owned()));
    Ok(())
}

/// Syncs the entries of directory `dir`, and notes it.
fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()?;
    note(|| Step::SyncDir(dir.to_owned()));
    Ok(())
}

/// Renames `from` over `to`, and notes it.
fn rename(from: &Path, to: &Path) -> io::Result<()> {
    fs::rename(from, to)?;
    note(|| Step::Rename {
        from: from.to_owned(),
        to: to.to_owned(),
    });
    Ok(())
}

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
            Ok(()) => note(|| Step::CreateDir(path.to_owned())),
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
        let mut unrecognized = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|err| StoreError::from_io(StoreOp::List, None, &err))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            match classify(level, &entry, &name)? {
                Entry::File(info) => files.push(info),
                Entry::Unrecognized => unrecognized.push(name),
                Entry::Gone => {}
            }
        }
        Ok(build_listing(files, &unrecognized, after, limit))
    }

    fn read_now(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.read_open(key, offset, len).map(|(bytes, _meta)| bytes)
    }

    /// The bytes, and the metadata of the very file they came from.
    fn read_open(
        &self,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> Result<(Vec<u8>, fs::Metadata), StoreError> {
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
            return Ok((Vec::new(), meta));
        }
        file.seek(SeekFrom::Start(offset)).map_err(fail)?;
        let mut bytes = Vec::new();
        file.take(len.get()).read_to_end(&mut bytes).map_err(fail)?;
        Ok((bytes, meta))
    }

    fn created_at_now(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        let key = file.key;
        let meta = fs::metadata(self.file_path(&key))
            .map_err(|err| StoreError::from_io(StoreOp::Read, Some(key), &err))?;
        if !meta.is_file() {
            return Err(StoreError::Corrupt {
                key: Some(key),
                cause: CorruptCause::NotARegularFile,
            });
        }
        if meta.len() != file.size {
            return Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::InvalidData,
            });
        }
        mtime_created_at(&meta, key, StoreOp::Read)
    }

    fn read_with_created_at_now(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let (bytes, meta) = self.read_open(&file.key, 0, len)?;
        Ok((bytes, mtime_created_at(&meta, file.key, StoreOp::Read)?))
    }

    fn begin_now(&self, key: &FileKey, created_at: Timestamp) -> Result<FsUpload, StoreError> {
        let fail = |err: io::Error| StoreError::from_io(StoreOp::Upload, None, &err);
        let mtime = to_system_time(created_at).ok_or(StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::TimestampOutOfRange,
        })?;
        let dir = self.level_dir(key.level());
        create_dirs_durably(&dir, &mut sync_dir).map_err(fail)?;
        let (scratch, file) = self.open_scratch(&dir, key)?;
        Ok(FsUpload {
            key: *key,
            dir,
            target: self.file_path(key),
            scratch,
            file,
            mtime,
            created_at,
            written: 0,
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
                Ok(file) => {
                    note(|| Step::Create(scratch.clone()));
                    return Ok((scratch, file));
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Err(err) => return Err(StoreError::from_io(StoreOp::Upload, None, &err)),
            }
        }
        Err(StoreError::ScratchExhausted { key: *key })
    }

    /// Removes each key, then syncs the level directory of every key, once per directory per
    /// call: an unlink is durable only when its directory is, so without the sync a deleted
    /// file can come back after a power loss. Keys already gone are synced too, because the
    /// call that removed them may have failed before its sync, and a retry must not report
    /// them deleted on an unsynced directory. A level directory that does not exist holds
    /// nothing to sync. Every key is reported deleted only after the syncs.
    fn delete_now(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let fail = |err: &io::Error| StoreError::from_io(StoreOp::Delete, None, err);
        let mut dirs: Vec<PathBuf> = Vec::new();
        for key in keys {
            match fs::remove_file(self.file_path(key)) {
                Ok(()) => note(|| Step::Remove(self.file_path(key))),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(fail(&err)),
            }
            let dir = self.level_dir(key.level());
            if !dirs.contains(&dir) {
                dirs.push(dir);
            }
        }
        for dir in &dirs {
            match sync_dir(dir) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(fail(&err)),
            }
        }
        for key in keys {
            note(|| Step::Deleted(self.file_path(key)));
        }
        Ok(())
    }
}

enum Entry {
    File(FileInfo),
    Unrecognized,
    /// Deleted between the directory read and the stat; a concurrent
    /// compaction does this, and the file is simply no longer there.
    Gone,
}

fn classify(level: Level, entry: &DirEntry, name: &str) -> Result<Entry, StoreError> {
    let Ok(range) = parse_file_name(name) else {
        return Ok(Entry::Unrecognized);
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
    let created_at = mtime_created_at(&meta, key, StoreOp::List)?;
    Ok(Entry::File(FileInfo {
        key,
        size: meta.len(),
        listed: ListedTime::Created(created_at),
    }))
}

/// The file's mtime as its created-at time: this store keeps the writer's created-at time there.
fn mtime_created_at(
    meta: &fs::Metadata,
    key: FileKey,
    op: StoreOp,
) -> Result<CreatedAt, StoreError> {
    let modified = meta
        .modified()
        .map_err(|err| StoreError::from_io(op, Some(key), &err))?;
    from_system_time(modified)
        .map(CreatedAt::mtime)
        .ok_or(StoreError::Corrupt {
            key: Some(key),
            cause: CorruptCause::TimestampOutOfRange,
        })
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

    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<CreatedAt, StoreError>> + Send {
        ready(self.created_at_now(file))
    }

    fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send {
        ready(self.read_with_created_at_now(file, len))
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
    mtime: SystemTime,
    created_at: Timestamp,
    written: u64,
}

impl FsUpload {
    fn commit_now(self) -> Result<FileInfo, StoreError> {
        let fail = |err: io::Error| StoreError::from_io(StoreOp::Upload, None, &err);
        let result = (|| {
            self.file.set_modified(self.mtime)?;
            sync_file(&self.file, &self.scratch)?;
            let size = self.file.metadata()?.len();
            if size != self.written {
                return Ok(Err(size));
            }
            rename(&self.scratch, &self.target)?;
            // The rename is not durable until its directory entry is.
            sync_dir(&self.dir)?;
            Ok(Ok(size))
        })();
        match result {
            // Caught before the rename, so nothing wrong was ever published.
            Ok(Err(stored)) => {
                let removed = fs::remove_file(&self.scratch).is_ok();
                Err(StoreError::Corrupt {
                    key: Some(self.key),
                    cause: CorruptCause::CommitSizeMismatch {
                        wrote: self.written,
                        stored,
                        removed,
                    },
                })
            }
            Ok(Ok(size)) => {
                note(|| Step::Committed(self.target.clone()));
                Ok(FileInfo {
                    key: self.key,
                    size,
                    listed: ListedTime::Created(CreatedAt::mtime(self.created_at)),
                })
            }
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
        ready(match self.file.write_all(chunk) {
            Ok(()) => {
                note(|| Step::Write(self.scratch.clone()));
                self.written = self
                    .written
                    .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
                Ok(())
            }
            Err(err) => Err(StoreError::from_io(StoreOp::Upload, None, &err)),
        })
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

mod journal;

#[cfg(test)]
mod tests;
