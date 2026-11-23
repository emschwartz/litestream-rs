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
//!
//! Replacing ([FileSink::create_over], Go's `restore` over an empty output file, or any
//! file with `-force`): the restore still builds the scratch and the existing target is
//! untouched until publish, so a restore that fails leaves it as it was (Go deletes it
//! before restoring). Publish then runs the check, fsyncs the scratch, checks the target
//! is still replaceable, removes the target's journal files ([Existing::Any] only) and
//! fsyncs the directory, then renames the scratch over the target and fsyncs the
//! directory. The journal files go, durably, before the rename: otherwise power loss
//! could keep the rename and lose the unlinks, and SQLite would replay the old database's
//! WAL into the new one. A crash after the unlinks and before the rename leaves the old
//! database without its journals, which the caller asked to overwrite; restoring again
//! completes it.

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

/// What a restore may do with a file already at its target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Existing {
    /// Refuse it: the default, and the only mode that never removes a file it did not create.
    Refuse,
    /// Replace it if it is an empty regular file (Go's `restore` without `-force`); journal
    /// files beside it are still refused.
    Empty,
    /// Replace any regular file, and remove its journal files at publish (Go's `-force`).
    Any,
}

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
    /// Replacing: the check that the target may still be replaced.
    CheckReplaceable,
    /// Replacing: the removal of the target's journal files.
    RemoveJournals,
    /// Replacing: the fsync of the directory that makes the removals durable before the
    /// rename.
    SyncJournals,
    /// Replacing: the rename of the scratch over the target.
    Rename,
    /// The fsync of the directory that makes the unlink durable.
    SyncRemoval,
}

impl PublishStep {
    /// Whether the target is visible once this step has failed.
    #[must_use]
    pub const fn after_link(self) -> bool {
        match self {
            Self::Check
            | Self::SyncScratch
            | Self::CheckAbsent
            | Self::Link
            | Self::CheckReplaceable
            | Self::RemoveJournals
            | Self::SyncJournals
            | Self::Rename => false,
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
            Self::CheckReplaceable => "checking the target may still be replaced",
            Self::RemoveJournals => "removing the replaced database's journal files",
            Self::SyncJournals => "syncing the directory after removing the journal files",
            Self::Rename => "renaming the restored file over the target",
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

/// The file system steps of a replacing publish.
trait ReplaceFs {
    fn sync_scratch(&mut self) -> io::Result<()>;
    fn not_replaceable(&self) -> io::Result<Option<PathBuf>>;
    fn remove_journals(&self) -> io::Result<()>;
    fn sync_dir(&self) -> io::Result<()>;
    fn rename(&self) -> io::Result<()>;
}

/// A replacing publish after the SQLite check, in order (see the module doc).
fn replace_steps(fs: &mut impl ReplaceFs, target: &Path) -> Result<(), PublishError> {
    let at = |step| move |source| PublishError::at(target, step, source);
    fs.sync_scratch().map_err(at(PublishStep::SyncScratch))?;
    let check = PublishStep::CheckReplaceable;
    if let Some(path) = fs.not_replaceable().map_err(at(check))? {
        let source = io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} changed during the restore", path.display()),
        );
        return Err(at(check)(source));
    }
    fs.remove_journals()
        .map_err(at(PublishStep::RemoveJournals))?;
    fs.sync_dir().map_err(at(PublishStep::SyncJournals))?;
    fs.rename().map_err(at(PublishStep::Rename))?;
    fs.sync_dir().map_err(at(PublishStep::SyncName))
}

/// The first path that stops a replace of `target` under `existing`: a target that is not
/// a regular file (or, for [Existing::Empty], not empty), or for [Existing::Empty] a
/// journal file. `None` when the replace may go ahead.
fn blocking(target: &Path, existing: Existing) -> io::Result<Option<PathBuf>> {
    match target.symlink_metadata() {
        Ok(meta) => {
            let empty_enough = existing == Existing::Any || meta.len() == 0;
            if !meta.file_type().is_file() || !empty_enough {
                return Ok(Some(target.to_path_buf()));
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    if existing == Existing::Any {
        return Ok(None);
    }
    for path in journal_files(target) {
        if exists(&path)? {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// A database being restored into a scratch file beside its target.
#[derive(Debug)]
pub struct FileSink {
    file: File,
    scratch: PathBuf,
    target: PathBuf,
    integrity: IntegrityCheck,
    existing: Existing,
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
    preflight_over(target, Existing::Refuse)
}

/// [preflight] under a replace mode: a target the mode may replace passes, and under
/// [Existing::Any] journal files are not returned, since publish removes them.
///
/// # Errors
/// As [preflight].
pub fn preflight_over(target: &Path, existing: Existing) -> Result<Vec<PathBuf>, CreateError> {
    let inspect = |path: &Path| {
        let path = path.to_path_buf();
        move |source| CreateError::Inspect { path, source }
    };
    match existing {
        Existing::Refuse => {
            if exists(target).map_err(inspect(target))? {
                return Err(CreateError::TargetExists(target.to_path_buf()));
            }
        }
        Existing::Empty | Existing::Any => {
            if blocking(target, Existing::Any)
                .map_err(inspect(target))?
                .is_some()
                || (existing == Existing::Empty
                    && target.symlink_metadata().is_ok_and(|m| m.len() > 0))
            {
                return Err(CreateError::TargetExists(target.to_path_buf()));
            }
            if existing == Existing::Any {
                return Ok(Vec::new());
            }
        }
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
        Self::create_over(target, Existing::Refuse)
    }

    /// [FileSink::create], with what may be done with a file already at [target]: under
    /// [Existing::Empty] or [Existing::Any] it is left in place now and replaced at publish.
    ///
    /// # Errors
    /// As [FileSink::create]; [CreateError::TargetExists] when the target is not a file the
    /// mode may replace.
    pub fn create_over(target: &Path, existing: Existing) -> Result<Self, CreateError> {
        preflight_over(target, existing)?
            .first()
            .map_or(Ok(()), |path| {
                Err(CreateError::JournalFile { path: path.clone() })
            })?;
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
                .is_ok_and(|held| (named.dev(), named.ino()) == (held.dev(), held.ino()))
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
            existing,
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

impl ReplaceFs for FileSink {
    fn sync_scratch(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn not_replaceable(&self) -> io::Result<Option<PathBuf>> {
        blocking(&self.target, self.existing)
    }

    fn remove_journals(&self) -> io::Result<()> {
        if self.existing != Existing::Any {
            return Ok(());
        }
        for path in journal_files(&self.target) {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    fn sync_dir(&self) -> io::Result<()> {
        sync_dir(&self.target)
    }

    fn rename(&self) -> io::Result<()> {
        fs::rename(&self.scratch, &self.target)
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
        match self.existing {
            Existing::Refuse => publish_steps(&mut self, &target)?,
            Existing::Empty | Existing::Any => replace_steps(&mut self, &target)?,
        }
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
mod replace_tests;

#[cfg(test)]
mod tests;
