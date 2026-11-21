//! The local filesystem side of Litestream.
//!
//! Today that is [FileSink], where a restore builds its database.
//!
//! A restore writes a scratch file `<target>.restore` next to the target,
//! never the target. [FileSink::create] opens it without truncating, takes an
//! exclusive advisory lock, then truncates: two restores of one target cannot
//! interleave, and a scratch left by a crashed restore (the lock died with its
//! process) is reclaimed instead of lingering at up to the database's size.
//!
//! Journal files: SQLite replays a stale `<target>-wal` and treats a stale
//! `<target>-journal` as a hot journal, either of which corrupts the restored
//! pages on first open. A restore never deletes a file it does not own, so
//! [FileSink::create] refuses while any of the three journal files exists, and the
//! caller clears them first with [clear_journal_files] (the normal path: a database
//! deleted under SQLITE_FCNTL_PERSIST_WAL leaves its `-wal` behind).
//!
//! [FileSink::publish] makes the database visible, in this order: run the
//! SQLite check the sink was given ([FileSink::with_integrity_check], none by
//! default; see the [integrity] module), so a database SQLite finds invalid
//! is refused with an [IntegrityFailure] and its scratch removed; fsync the
//! scratch; check the target and all three journal files are still absent (else
//! `AlreadyExists` naming the path, nothing touched); hard-link the scratch to
//! the target, which fails when the target exists, so a database a rival
//! created meanwhile is never overwritten; fsync the directory; unlink the
//! scratch; fsync the directory. Nothing but the sink's own scratch is ever
//! removed. A failed publish says which way it left the target ([PublishError],
//! which is core's `PublishFailure`): a refused check
//! ([PublishError::Integrity]) or a step before the link leaves nothing at
//! the target ([PublishError::NotVisible]); a step after it leaves the target in
//! place with its data already fsynced, but the name, or the scratch's removal,
//! may not survive a crash ([PublishError::VisibleNotDurable]).
//! An fsync is never retried: after a failed fsync the kernel may have dropped
//! the dirty pages, and a second one would report success over lost data.
//!
//! The hard link needs a filesystem that supports them (every Unix one a
//! database lives on does).

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use lsr_core::DatabaseSink;
use lsr_ltx::{DbChecksum, DbPages, PageChecksum, PageNo, PageRef, PageSize};
use thiserror::Error;

pub mod integrity;

pub use integrity::{IntegrityCheck, IntegrityFailure};

/// The files SQLite keeps beside a database that must not outlive it.
const JOURNAL_SUFFIXES: [&str; 3] = ["-wal", "-shm", "-journal"];

/// Why a [FileSink] could not be created.
#[derive(Debug, Error)]
pub enum CreateError {
    /// The target path exists. A restore never overwrites a database.
    #[error("{} already exists; a restore never overwrites a database", .0.display())]
    TargetExists(PathBuf),
    /// A `-wal`, `-shm` or `-journal` file exists beside the absent target;
    /// SQLite would replay it over the restored database. Call
    /// [clear_journal_files] first if it is known stale.
    #[error("{} exists beside the restore target; clear it with clear_journal_files first", path.display())]
    JournalFile {
        /// The journal file.
        path: PathBuf,
    },
    /// Another restore of this target holds the scratch file.
    #[error("another restore of this target is running ({})", .0.display())]
    RestoreInProgress(PathBuf),
    /// A path could not be inspected.
    #[error("inspecting {}: {source}", path.display())]
    Inspect {
        /// The path.
        path: PathBuf,
        /// The I/O error.
        source: io::Error,
    },
    /// The scratch file could not be created.
    #[error("creating scratch file {}: {source}", path.display())]
    Scratch {
        /// The scratch path.
        path: PathBuf,
        /// The I/O error.
        source: io::Error,
    },
}

/// Why [clear_journal_files] failed.
#[derive(Debug, Error)]
pub enum ClearError {
    /// The target exists: its journal files belong to a live database.
    #[error("{} exists; its journal files are not stale", .0.display())]
    TargetExists(PathBuf),
    /// A path could not be inspected or removed.
    #[error("{}: {source}", path.display())]
    Io {
        /// The path.
        path: PathBuf,
        /// The I/O error.
        source: io::Error,
    },
}

/// One step of [FileSink::publish] after the SQLite check, for [PublishError].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PublishStep {
    /// SQLite's check of the scratch, which could not run.
    Check,
    /// The fsync of the scratch's data.
    SyncScratch,
    /// The check that the target and its journal files are still absent.
    CheckAbsent,
    /// The hard link of the scratch to the target.
    Link,
    /// The fsync of the directory that makes the target's name durable.
    SyncName,
    /// The unlink of the scratch's name.
    RemoveScratch,
    /// The fsync of the directory that makes the unlink durable.
    SyncRemoval,
}

impl PublishStep {
    /// Whether the target is visible once this step has failed.
    #[must_use]
    pub const fn after_link(self) -> bool {
        match self {
            Self::Check | Self::SyncScratch | Self::CheckAbsent | Self::Link => false,
            Self::SyncName | Self::RemoveScratch | Self::SyncRemoval => true,
        }
    }
}

impl std::fmt::Display for PublishStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Check => "running SQLite's check of the restored database",
            Self::SyncScratch => "syncing the restored data",
            Self::CheckAbsent => "checking the target is still absent",
            Self::Link => "linking the restored file into place",
            Self::SyncName => "syncing the directory after the link",
            Self::RemoveScratch => "removing the scratch file",
            Self::SyncRemoval => "syncing the directory after removing the scratch",
        })
    }
}

/// Why [FileSink::publish] failed, by what it left at the target.
#[derive(Debug, Error)]
pub enum PublishError {
    /// SQLite found the restored database invalid: nothing was made visible, and the
    /// scratch is removed.
    #[error("{0}; nothing was published")]
    Integrity(IntegrityFailure),
    /// Nothing was made visible: the target and its journal files are as they were.
    #[error("{} was not published ({step}): {source}", .target.display())]
    NotVisible {
        /// The target.
        target: PathBuf,
        /// The step that failed.
        step: PublishStep,
        /// The I/O error.
        source: io::Error,
    },
    /// The target exists with its data fsynced, but the step that failed means its name,
    /// or the removal of the scratch, may not survive a crash. It is not removed: it is a
    /// complete, verified database.
    #[error("{} is published but may not survive a crash ({step}): {source}", .target.display())]
    VisibleNotDurable {
        /// The target.
        target: PathBuf,
        /// The step that failed.
        step: PublishStep,
        /// The I/O error.
        source: io::Error,
    },
}

impl PublishError {
    fn at(target: &Path, step: PublishStep, source: io::Error) -> Self {
        let target = target.to_path_buf();
        if step.after_link() {
            Self::VisibleNotDurable {
                target,
                step,
                source,
            }
        } else {
            Self::NotVisible {
                target,
                step,
                source,
            }
        }
    }
}

impl lsr_core::PublishFailure for PublishError {
    fn left(&self) -> lsr_core::PublishLeft {
        match self {
            Self::Integrity(_) | Self::NotVisible { .. } => lsr_core::PublishLeft::Nothing,
            Self::VisibleNotDurable { .. } => lsr_core::PublishLeft::VisibleNotDurable,
        }
    }
}

/// The file system steps of a publish, behind a trait so a test can fail each one.
trait PublishFs {
    fn sync_scratch(&mut self) -> io::Result<()>;
    fn first_present(&self) -> io::Result<Option<PathBuf>>;
    fn link(&self) -> io::Result<()>;
    fn sync_dir(&self) -> io::Result<()>;
    fn remove_scratch(&self) -> io::Result<()>;
}

/// The publish steps after the SQLite check, in order, each failure tagged with its step.
fn publish_steps(fs: &mut impl PublishFs, target: &Path) -> Result<(), PublishError> {
    let at = |step| move |source| PublishError::at(target, step, source);
    fs.sync_scratch().map_err(at(PublishStep::SyncScratch))?;
    if let Some(path) = fs.first_present().map_err(at(PublishStep::CheckAbsent))? {
        let source = io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} appeared during the restore", path.display()),
        );
        return Err(at(PublishStep::CheckAbsent)(source));
    }
    fs.link().map_err(at(PublishStep::Link))?;
    fs.sync_dir().map_err(at(PublishStep::SyncName))?;
    fs.remove_scratch()
        .map_err(at(PublishStep::RemoveScratch))?;
    fs.sync_dir().map_err(at(PublishStep::SyncRemoval))
}

/// A database being restored into a scratch file beside its target.
#[derive(Debug)]
pub struct FileSink {
    file: File,
    scratch: PathBuf,
    target: PathBuf,
    integrity: IntegrityCheck,
    published: bool,
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

fn journal_files(target: &Path) -> [PathBuf; 3] {
    JOURNAL_SUFFIXES.map(|suffix| suffixed(target, suffix))
}

fn exists(path: &Path) -> io::Result<bool> {
    match path.symlink_metadata() {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

/// Removes the stale `-wal`, `-shm` and `-journal` files beside an absent
/// target, fsyncs the directory, and returns what it removed. The caller
/// decides to call this before a restore starts; publish never deletes.
///
/// # Errors
/// [ClearError::TargetExists] when the target exists; otherwise the failing
/// removal or fsync.
pub fn clear_journal_files(target: &Path) -> Result<Vec<PathBuf>, ClearError> {
    let io_at = |path: &Path| {
        let path = path.to_path_buf();
        move |source| ClearError::Io { path, source }
    };
    if exists(target).map_err(io_at(target))? {
        return Err(ClearError::TargetExists(target.to_path_buf()));
    }
    let mut removed = Vec::new();
    for path in journal_files(target) {
        match fs::remove_file(&path) {
            Ok(()) => removed.push(path),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_at(&path)(err)),
        }
    }
    sync_dir(target).map_err(io_at(target))?;
    Ok(removed)
}

/// Checks [target] the way [FileSink::create] does, changing nothing, and
/// returns the stale journal files beside it: what [clear_journal_files] would remove.
/// A dry run reports from this what a restore would meet.
///
/// # Errors
/// [CreateError::TargetExists] when the target exists, or
/// [CreateError::Inspect] when a path cannot be inspected.
pub fn preflight(target: &Path) -> Result<Vec<PathBuf>, CreateError> {
    let inspect = |path: &Path| {
        let path = path.to_path_buf();
        move |source| CreateError::Inspect { path, source }
    };
    if exists(target).map_err(inspect(target))? {
        return Err(CreateError::TargetExists(target.to_path_buf()));
    }
    let mut present = Vec::new();
    for path in journal_files(target) {
        if exists(&path).map_err(inspect(&path))? {
            present.push(path);
        }
    }
    Ok(present)
}

/// The first of the target and its journal files that exists.
fn first_present(target: &Path) -> io::Result<Option<PathBuf>> {
    for path in std::iter::once(target.to_path_buf()).chain(journal_files(target)) {
        if exists(&path)? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

impl FileSink {
    /// Starts a restore of [target]: opens and locks the scratch file
    /// `<target>.restore`, empty. Nothing exists at [target] or its journal files.
    ///
    /// # Errors
    /// [CreateError::TargetExists], [CreateError::Journal file],
    /// [CreateError::RestoreInProgress], or the I/O failure.
    pub fn create(target: &Path) -> Result<Self, CreateError> {
        let inspect = |path: &Path| {
            let path = path.to_path_buf();
            move |source| CreateError::Inspect { path, source }
        };
        if exists(target).map_err(inspect(target))? {
            return Err(CreateError::TargetExists(target.to_path_buf()));
        }
        for path in journal_files(target) {
            if exists(&path).map_err(inspect(&path))? {
                return Err(CreateError::JournalFile { path });
            }
        }
        let scratch = suffixed(target, ".restore");
        let scratch_fault = |source| CreateError::Scratch {
            path: scratch.clone(),
            source,
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&scratch)
            .map_err(scratch_fault)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Err(CreateError::RestoreInProgress(scratch)),
            Err(TryLockError::Error(source)) => return Err(scratch_fault(source)),
        }
        // The lock holder before us may have removed the name while we waited
        // to open it; a file that is no longer the scratch is not ours to use.
        let same = fs::metadata(&scratch).is_ok_and(|named| {
            file.metadata()
                .is_ok_and(|held| named.dev() == held.dev() && named.ino() == held.ino())
        });
        if !same {
            return Err(CreateError::RestoreInProgress(scratch));
        }
        file.set_len(0).map_err(scratch_fault)?;
        Ok(Self {
            file,
            scratch,
            target: target.to_path_buf(),
            integrity: IntegrityCheck::Off,
            published: false,
        })
    }

    /// Runs [check] on the scratch at [DatabaseSink::publish], before
    /// anything is made visible.
    #[must_use]
    pub const fn with_integrity_check(mut self, check: IntegrityCheck) -> Self {
        self.integrity = check;
        self
    }

    /// The scratch file's path, for diagnostics.
    #[must_use]
    pub fn scratch_path(&self) -> &Path {
        &self.scratch
    }
}

impl PublishFs for FileSink {
    fn sync_scratch(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn first_present(&self) -> io::Result<Option<PathBuf>> {
        first_present(&self.target)
    }

    fn link(&self) -> io::Result<()> {
        fs::hard_link(&self.scratch, &self.target)
    }

    fn sync_dir(&self) -> io::Result<()> {
        sync_dir(&self.target)
    }

    fn remove_scratch(&self) -> io::Result<()> {
        fs::remove_file(&self.scratch)
    }
}

fn sync_dir(target: &Path) -> io::Result<()> {
    let dir = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        Some(_) | None => Path::new("."),
    };
    File::open(dir)?.sync_all()
}

impl DatabaseSink for FileSink {
    fn write_page(&mut self, page: PageRef<'_>) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(page.offset()))?;
        self.file.write_all(page.data())
    }

    fn truncate(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<()> {
        self.file.set_len(pages.bytes(page_size))
    }

    fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)
    }

    fn checksum(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<DbChecksum> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut page = vec![0_u8; page_size.bytes()];
        let mut total = DbChecksum::EMPTY;
        for n in 1..=pages.get() {
            self.file.read_exact(&mut page)?;
            // The lock page is the one number PageNo refuses; its bytes are
            // read past and not counted.
            if let Some(pgno) = PageNo::new(n, page_size) {
                total = total.toggle(PageChecksum::of(pgno, &page));
            }
        }
        Ok(total)
    }

    type PublishError = PublishError;

    /// # Errors
    /// [PublishError::Integrity] when SQLite finds the database invalid, else the step
    /// that failed and whether the target is visible.
    fn publish(mut self) -> Result<(), PublishError> {
        let target = self.target.clone();
        integrity::check(&self.scratch, self.integrity).map_err(|err| match err {
            integrity::CheckError::Refused(failure) => PublishError::Integrity(failure),
            integrity::CheckError::Failed(source) => {
                PublishError::at(&target, PublishStep::Check, source)
            }
        })?;
        publish_steps(&mut self, &target)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for FileSink {
    fn drop(&mut self) {
        if !self.published {
            drop(fs::remove_file(&self.scratch));
        }
    }
}

#[cfg(test)]
mod publish_tests;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;

    use lsr_core::{FileKey, Level, conformance::block_on};
    use lsr_core::{MemStore, ObjectStore, RestoreError, RestoreTarget, Upload, restore};
    use lsr_ltx::{Encoder, Header, HeaderFields, Timestamp, Txid, TxidRange};
    use tempfile::TempDir;

    use super::*;

    fn ps(bytes: u32) -> PageSize {
        PageSize::new(bytes).unwrap()
    }

    fn pgno(n: u32) -> PageNo {
        PageNo::new(n, ps(512)).unwrap()
    }

    fn page(n: u32, data: &[u8]) -> PageRef<'_> {
        PageRef::new(pgno(n), ps(512), data).unwrap()
    }

    #[test]
    fn a_page_lands_where_its_page_size_puts_it() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        let size = ps(1024);
        let data = [5; 1024];
        let second = PageRef::new(PageNo::new(2, size).unwrap(), size, &data).unwrap();
        sink.write_page(second).unwrap();
        sink.publish().unwrap();
        let mut want = vec![0; 1024];
        want.extend([5; 1024]);
        assert_eq!(fs::read(&target).unwrap(), want);
    }

    fn names(dir: &TempDir) -> BTreeSet<String> {
        fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    #[test]
    fn create_refuses_a_target_that_exists() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        fs::write(&target, b"precious").unwrap();
        assert!(matches!(
            FileSink::create(&target),
            Err(CreateError::TargetExists(path)) if path == target
        ));
        assert_eq!(fs::read(&target).unwrap(), b"precious");
        let dangling = dir.path().join("link");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &dangling).unwrap();
        assert!(matches!(
            FileSink::create(&dangling),
            Err(CreateError::TargetExists(_))
        ));
        assert_eq!(
            names(&dir),
            BTreeSet::from(["db".to_owned(), "link".to_owned()])
        );
    }

    #[test]
    fn the_scratch_is_the_target_name_plus_restore() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let sink = FileSink::create(&target).unwrap();
        assert_eq!(sink.scratch_path(), dir.path().join("db.restore"));
        assert!(!target.exists());
    }

    #[test]
    fn a_second_restore_of_one_target_is_refused_while_the_first_lives() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut first = FileSink::create(&target).unwrap();
        first.write_page(page(1, &[1; 512])).unwrap();
        assert!(matches!(
            FileSink::create(&target),
            Err(CreateError::RestoreInProgress(path)) if path == dir.path().join("db.restore")
        ));
        // The refused attempt did not disturb the first restore's scratch.
        assert_eq!(fs::read(first.scratch_path()).unwrap(), vec![1; 512]);
        drop(first);
        assert!(names(&dir).is_empty());
        drop(FileSink::create(&target).unwrap());
    }

    #[test]
    fn a_scratch_left_by_a_crashed_restore_is_reclaimed() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        fs::write(dir.path().join("db.restore"), vec![9; 4096]).unwrap();
        let mut sink = FileSink::create(&target).unwrap();
        assert_eq!(fs::metadata(sink.scratch_path()).unwrap().len(), 0);
        sink.write_page(page(1, &[1; 512])).unwrap();
        sink.publish().unwrap();
        assert_eq!(fs::read(&target).unwrap(), vec![1; 512]);
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
    }

    #[test]
    fn the_scratch_is_removed_on_drop_unless_published() {
        let dir = TempDir::new().unwrap();
        let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
        sink.write_page(page(1, &[1; 512])).unwrap();
        assert_eq!(names(&dir).len(), 1);
        drop(sink);
        assert!(names(&dir).is_empty());
    }

    #[test]
    fn published_bytes_are_the_pages_written_and_only_the_target_remains() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(page(3, &[3; 512])).unwrap();
        sink.write_page(page(1, &[1; 512])).unwrap();
        sink.truncate(DbPages::new(4), ps(512)).unwrap();
        sink.publish().unwrap();
        let mut want = [1_u8; 512].to_vec();
        want.extend([0; 512]);
        want.extend([3; 512]);
        want.extend([0; 512]);
        assert_eq!(fs::read(&target).unwrap(), want);
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
    }

    #[test]
    fn truncate_sets_the_exact_length_and_growth_reads_as_zeros() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(page(2, &[9; 512])).unwrap();
        sink.truncate(DbPages::new(1), ps(512)).unwrap();
        sink.truncate(DbPages::new(2), ps(512)).unwrap();
        sink.publish().unwrap();
        assert_eq!(fs::read(&target).unwrap(), vec![0; 1024]);
    }

    #[test]
    fn checksum_reads_back_what_the_file_holds() {
        let dir = TempDir::new().unwrap();
        let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
        sink.write_page(page(3, &[3; 512])).unwrap();
        sink.truncate(DbPages::new(4), ps(512)).unwrap();
        let mut want = DbChecksum::EMPTY;
        for (n, seed) in [(1, 0_u8), (2, 0), (3, 3), (4, 0)] {
            want = want.toggle(PageChecksum::of(pgno(n), &[seed; 512]));
        }
        assert_eq!(sink.checksum(DbPages::new(4), ps(512)).unwrap(), want);
    }

    #[test]
    fn reset_empties_the_scratch() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(page(1, &[1; 512])).unwrap();
        sink.reset().unwrap();
        sink.write_page(page(1, &[2; 512])).unwrap();
        sink.publish().unwrap();
        assert_eq!(fs::read(&target).unwrap(), vec![2; 512]);
    }

    #[test]
    fn create_refuses_each_journal_file_and_removes_nothing() {
        for suffix in ["-wal", "-shm", "-journal"] {
            let dir = TempDir::new().unwrap();
            let target = dir.path().join("db");
            let journal_file = dir.path().join(format!("db{suffix}"));
            fs::write(&journal_file, b"stale").unwrap();
            assert!(matches!(
                FileSink::create(&target),
                Err(CreateError::JournalFile { path }) if path == journal_file
            ));
            assert_eq!(fs::read(&journal_file).unwrap(), b"stale");
            assert_eq!(names(&dir).len(), 1, "{suffix}: no scratch left behind");
        }
    }

    #[test]
    fn preflight_reports_what_create_and_clear_would_meet_and_touches_nothing() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        assert_eq!(preflight(&target).unwrap(), Vec::<PathBuf>::new());
        fs::write(dir.path().join("db-wal"), b"stale").unwrap();
        fs::write(dir.path().join("db-journal"), b"stale").unwrap();
        assert_eq!(
            preflight(&target).unwrap(),
            vec![dir.path().join("db-wal"), dir.path().join("db-journal")]
        );
        fs::write(&target, b"live").unwrap();
        assert!(matches!(
            preflight(&target),
            Err(CreateError::TargetExists(path)) if path == target
        ));
        assert_eq!(names(&dir).len(), 3, "nothing created or removed");
    }

    #[test]
    fn clear_journal_files_removes_the_three_and_reports_them() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        for suffix in ["-wal", "-shm", "-journal"] {
            fs::write(dir.path().join(format!("db{suffix}")), b"stale").unwrap();
        }
        fs::write(dir.path().join("db-other"), b"not ours").unwrap();
        let mut removed = clear_journal_files(&target).unwrap();
        removed.sort();
        assert_eq!(
            removed,
            ["db-journal", "db-shm", "db-wal"].map(|n| dir.path().join(n))
        );
        assert_eq!(names(&dir), BTreeSet::from(["db-other".to_owned()]));
        assert!(clear_journal_files(&target).unwrap().is_empty());
    }

    #[test]
    fn clear_journal_files_refuses_a_target_that_exists() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        fs::write(&target, b"live").unwrap();
        fs::write(dir.path().join("db-wal"), b"its wal").unwrap();
        assert!(matches!(
            clear_journal_files(&target),
            Err(ClearError::TargetExists(path)) if path == target
        ));
        assert_eq!(fs::read(dir.path().join("db-wal")).unwrap(), b"its wal");
    }

    /// Review: a stale rollback journal beside the target is a hot journal to
    /// SQLite, which rolls its old pages back over the restored database on
    /// the first open ("database disk image is malformed", reproduced with
    /// sqlite3). Create refuses; after clear_journal_files the restore publishes
    /// exactly the database and nothing else.
    #[test]
    fn a_stale_journal_blocks_the_restore_until_cleared() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        fs::write(dir.path().join("db-journal"), b"stale hot journal").unwrap();
        assert!(matches!(
            FileSink::create(&target),
            Err(CreateError::JournalFile { .. })
        ));
        clear_journal_files(&target).unwrap();
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(page(1, &[1; 512])).unwrap();
        sink.publish().unwrap();
        assert_eq!(fs::read(&target).unwrap(), vec![1; 512]);
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
    }

    /// A journal file that appears after create is a rival's (or a stale file's);
    /// publish refuses and leaves it, and the target, alone.
    #[test]
    fn a_journal_file_appearing_after_create_makes_publish_refuse() {
        for suffix in ["-wal", "-shm", "-journal"] {
            let dir = TempDir::new().unwrap();
            let target = dir.path().join("db");
            let mut sink = FileSink::create(&target).unwrap();
            sink.write_page(page(1, &[1; 512])).unwrap();
            let journal_file = dir.path().join(format!("db{suffix}"));
            fs::write(&journal_file, b"theirs").unwrap();
            let err = sink.publish().unwrap_err();
            assert!(already_exists(&err), "{err}");
            assert!(err.to_string().contains(&format!("db{suffix}")), "{err}");
            assert_eq!(fs::read(&journal_file).unwrap(), b"theirs");
            assert_eq!(names(&dir), BTreeSet::from([format!("db{suffix}")]));
        }
    }

    /// A refusal before the link because something is already at a path.
    fn already_exists(err: &PublishError) -> bool {
        matches!(
            err,
            PublishError::NotVisible { step: PublishStep::CheckAbsent, source, .. }
                if source.kind() == io::ErrorKind::AlreadyExists
        )
    }

    #[test]
    fn a_target_created_during_the_restore_is_not_overwritten() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(page(1, &[1; 512])).unwrap();
        fs::write(&target, b"someone else's").unwrap();
        fs::write(dir.path().join("db-wal"), b"their wal").unwrap();
        let err = sink.publish().unwrap_err();
        assert!(already_exists(&err), "{err}");
        assert_eq!(fs::read(&target).unwrap(), b"someone else's");
        // Their WAL belongs to their database: it is not ours to remove.
        assert_eq!(fs::read(dir.path().join("db-wal")).unwrap(), b"their wal");
        assert_eq!(
            names(&dir),
            BTreeSet::from(["db".to_owned(), "db-wal".to_owned()])
        );
    }

    fn txid(n: u64) -> Txid {
        Txid::new(n).unwrap()
    }

    fn snapshot_bytes(commit: u32, seed: u8) -> Vec<u8> {
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size: ps(512),
            commit,
            txids: TxidRange::single(txid(1)),
            timestamp: Timestamp::from_unix_millis(1000),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for n in 1..=commit {
            encoder.write_page(pgno(n), &[seed; 512]).unwrap();
        }
        encoder.finish(None).unwrap().0
    }

    fn replica(bytes: &[u8]) -> MemStore {
        let store = MemStore::new();
        block_on(async {
            let key = FileKey::new(Level::SNAPSHOT, TxidRange::single(txid(1)));
            let mut upload = store
                .begin_upload(&key, Timestamp::from_unix_millis(1000))
                .await
                .unwrap();
            upload.write(bytes).await.unwrap();
            upload.commit().await.unwrap();
        });
        store
    }

    #[test]
    fn a_restore_publishes_the_database_and_nothing_else() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let store = replica(&snapshot_bytes(3, 7));
        let sink = FileSink::create(&target).unwrap();
        let restored = block_on(restore(&store, RestoreTarget::Latest, sink)).unwrap();
        assert_eq!(restored.commit(), DbPages::new(3));
        assert_eq!(fs::read(&target).unwrap(), vec![7; 1536]);
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
    }

    /// A real SQLite database in WAL mode (its header says so, as a
    /// replicated one does), 512-byte pages, a table and an index over it.
    fn sqlite_pages() -> Vec<u8> {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("source.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size = 512; PRAGMA journal_mode = WAL;
             CREATE TABLE t (x TEXT); CREATE INDEX t_x ON t (x);",
        )
        .unwrap();
        for n in 0..200 {
            conn.execute("INSERT INTO t VALUES (?1)", [format!("{n:0>40}")])
                .unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.get(18..20), Some(&[2, 2][..]), "WAL-mode header");
        bytes
    }

    /// A snapshot holding exactly [db]'s pages: valid LTX whatever they say.
    fn snapshot_of(db: &[u8]) -> Vec<u8> {
        let commit = u32::try_from(db.len() / 512).unwrap();
        let header = Header::new(HeaderFields {
            tracked: false,
            page_size: ps(512),
            commit,
            txids: TxidRange::single(txid(1)),
            timestamp: Timestamp::from_unix_millis(1000),
            pre_apply: None,
            wal: None,
            node_id: 0,
        })
        .unwrap();
        let mut encoder = Encoder::new(Vec::new(), header).unwrap();
        for (n, page) in (1..=commit).zip(db.as_chunks::<512>().0) {
            encoder.write_page(pgno(n), page).unwrap();
        }
        encoder.finish(None).unwrap().0
    }

    /// Page 2, the table's root, overwritten: every LTX check passes, the
    /// b-tree is not one.
    fn corrupt_b_tree() -> Vec<u8> {
        let mut db = sqlite_pages();
        db[512..1024].fill(0xDE);
        db
    }

    fn checked_sink(target: &Path, check: IntegrityCheck) -> FileSink {
        FileSink::create(target)
            .unwrap()
            .with_integrity_check(check)
    }

    fn restore_with(db: &[u8], check: IntegrityCheck, target: &Path) -> Result<(), RestoreError> {
        let store = replica(&snapshot_of(db));
        let sink = checked_sink(target, check);
        block_on(restore(&store, RestoreTarget::Latest, sink)).map(drop)
    }

    fn integrity_failure(result: Result<(), RestoreError>) -> IntegrityFailure {
        match result {
            Err(RestoreError::Publish { source, .. }) => match source.downcast::<PublishError>() {
                Ok(failure) => match *failure {
                    PublishError::Integrity(failure) => failure,
                    other @ (PublishError::NotVisible { .. }
                    | PublishError::VisibleNotDurable { .. }) => {
                        panic!("not an integrity failure: {other}")
                    }
                },
                Err(source) => panic!("not the sink's error: {source}"),
            },
            other => panic!("expected a refused publish, got {other:?}"),
        }
    }

    #[test]
    fn every_check_publishes_a_valid_database_and_nothing_beside_it() {
        let db = sqlite_pages();
        for check in [
            IntegrityCheck::Off,
            IntegrityCheck::Quick,
            IntegrityCheck::Full,
        ] {
            let dir = TempDir::new().unwrap();
            let target = dir.path().join("db");
            restore_with(&db, check, &target).unwrap();
            assert_eq!(fs::read(&target).unwrap(), db, "{check:?}");
            // No -wal or -shm beside the scratch or the target, though the
            // header says WAL mode: the check opened it immutable.
            assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]), "{check:?}");
        }
    }

    #[test]
    fn quick_and_full_refuse_a_corrupt_b_tree_and_leave_nothing() {
        let db = corrupt_b_tree();
        for (check, pragma) in [
            (IntegrityCheck::Quick, "quick_check"),
            (IntegrityCheck::Full, "integrity_check"),
        ] {
            let dir = TempDir::new().unwrap();
            let target = dir.path().join("db");
            let failure = integrity_failure(restore_with(&db, check, &target));
            assert_eq!(failure.pragma(), pragma);
            assert!(
                failure.messages().iter().any(|m| m.contains("page 2")),
                "{failure}"
            );
            assert!(failure.to_string().contains(pragma), "{failure}");
            assert!(names(&dir).is_empty(), "{check:?}: {:?}", names(&dir));
        }
    }

    #[test]
    fn off_publishes_the_corrupt_b_tree_as_restored() {
        let db = corrupt_b_tree();
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        restore_with(&db, IntegrityCheck::Off, &target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), db);
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
    }

    /// Pages that are no database at all: SQLite refuses to open them, which
    /// is a verdict on the file, so the restore is refused, not failed.
    #[test]
    fn a_file_that_is_not_a_database_is_refused_like_a_failed_check() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let failure = integrity_failure(restore_with(&[7; 1536], IntegrityCheck::Quick, &target));
        assert_eq!(failure.messages().len(), 1);
        assert!(!failure.more());
        assert!(names(&dir).is_empty());
    }

    /// Only the first problems are kept, and a cut list says so.
    #[test]
    fn a_long_list_of_problems_keeps_the_first_few() {
        let mut db = sqlite_pages();
        let len = db.len();
        db[512..len].fill(0xDE);
        let dir = TempDir::new().unwrap();
        let failure = integrity_failure(restore_with(
            &db,
            IntegrityCheck::Full,
            &dir.path().join("db"),
        ));
        assert_eq!(failure.messages().len(), integrity::KEPT_MESSAGES);
        assert!(failure.more());
        assert!(failure.to_string().ends_with("; and more"), "{failure}");
    }

    /// The check opens the scratch by URI; a name with URI syntax in it is
    /// still that file.
    #[test]
    fn a_target_name_with_uri_syntax_is_checked_as_named() {
        let dir = TempDir::new().unwrap();
        let name = "a?mode=rwc#b%41 c";
        restore_with(
            &sqlite_pages(),
            IntegrityCheck::Quick,
            &dir.path().join(name),
        )
        .unwrap();
        let failure = integrity_failure(restore_with(
            &corrupt_b_tree(),
            IntegrityCheck::Quick,
            &dir.path().join("x?y"),
        ));
        assert!(!failure.messages().is_empty());
        assert_eq!(names(&dir), BTreeSet::from([name.to_owned()]));
    }

    /// The check runs on the bytes in the scratch at publish, whatever wrote
    /// them: a page changed after the restore's own writes is caught.
    #[test]
    fn the_check_reads_the_scratch_as_it_is_at_publish() {
        let db = sqlite_pages();
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target)
            .unwrap()
            .with_integrity_check(IntegrityCheck::Full);
        for (n, bytes) in (1..).zip(db.as_chunks::<512>().0) {
            sink.write_page(page(n, bytes)).unwrap();
        }
        sink.write_page(page(2, &[0xDE; 512])).unwrap();
        let err = sink.publish().unwrap_err();
        assert!(matches!(err, PublishError::Integrity(_)), "{err}");
        assert!(names(&dir).is_empty());
    }

    #[test]
    fn a_failed_restore_leaves_no_target_and_no_scratch() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut bytes = snapshot_bytes(3, 7);
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let store = replica(&bytes);
        let sink = FileSink::create(&target).unwrap();
        let result = block_on(restore(&store, RestoreTarget::Latest, sink));
        assert!(matches!(result, Err(RestoreError::File { .. })));
        assert!(names(&dir).is_empty());
    }
}
