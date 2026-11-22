//! The engine's own files in the meta directory, beside Go's `ltx/` tree.
//!
//! - `lsr-lock`: one engine per meta directory. [`MetaLock::take`] holds an
//!   exclusive lock on it for the replicator thread's life. Two engines on one
//!   staging directory (an overlapping rolling deploy, `lsr replicate` run by
//!   hand beside the daemon, two databases configured with one `meta_dir`)
//!   would stage the same transaction twice under one key, and the
//!   other-writer detector compares the replica with that shared staging, so
//!   it could not see the second engine. The second start fails instead
//!   ([`crate::StartError::MetaLocked`]).
//! - `lsr-stop`: a stop for another writer that must outlive the process
//!   ([`lsr_core::StopRecord`]), read at start and written whenever the
//!   engine's record changes; `lsr-stop.new` is its scratch name.
//! - `lsr-takeover`: an operator's word that this database may replicate over
//!   what the replica holds, through one transaction
//!   ([`lsr_core::TakeoverRecord`]). Read at start; written by
//!   [`crate::take_over`] (`lsr take-over`) and by every restore, through the
//!   restored transaction; `lsr-takeover.new` is its scratch name.
//!
//! The lock is an open file description lock (`flock(2)`, which is what
//! `std::fs::File::try_lock` takes on Linux and macOS), on a file of its own.
//! Unlike a POSIX record lock (`fcntl`), closing some other descriptor of the
//! same file in this process (SQLite opens and closes many) cannot drop it,
//! and the file is never replaced, so the lock always names the inode a
//! second engine opens. It is released when the thread's last handle goes,
//! or when the process dies. Descriptors are opened close-on-exec, so an
//! `-exec` child does not inherit it.
//!
//! Where it excludes: processes on one host whose meta directory is on a
//! local filesystem (ext4, XFS, Btrfs, ZFS, tmpfs on Linux; APFS and HFS+ on
//! macOS), containers on one kernel sharing a volume included. Not promised:
//! network and FUSE filesystems (NFS, SMB, virtiofs, Docker Desktop's file
//! sharing), where a lock may be emulated, local to one client, or refused. A
//! filesystem that refuses locks fails the start
//! ([`crate::StartError::MetaLock`]) rather than running without exclusion.
//! Go takes no lock, so a Go process on the same meta directory is not
//! excluded.
//!
//! None of the names ends in `.tmp`: Go deletes every `*.tmp` under the meta
//! directory at open and on disk-full errors. Go neither reads nor removes
//! these files.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::Path;

use lsr_core::{StopRecord, TakeoverRecord};

use crate::error::StartError;

/// The file the engine locks.
pub(crate) const LOCK_FILE: &str = "lsr-lock";
/// The stop record.
pub(crate) const STOP_FILE: &str = "lsr-stop";
/// The stop record's scratch name, renamed over it.
const STOP_SCRATCH: &str = "lsr-stop.new";
/// The takeover record.
pub(crate) const TAKEOVER_FILE: &str = "lsr-takeover";
/// The takeover record's scratch name, renamed over it.
const TAKEOVER_SCRATCH: &str = "lsr-takeover.new";

/// The exclusive lock on a meta directory, held while this value lives.
#[derive(Debug)]
pub(crate) struct MetaLock {
    _file: File,
}

impl MetaLock {
    /// Takes the lock without waiting.
    ///
    /// # Errors
    /// [`StartError::MetaLocked`] when another engine holds it,
    /// [`StartError::MetaLock`] when the lock file cannot be opened or the
    /// filesystem refuses the lock.
    pub(crate) fn take(meta: &Path) -> Result<Self, StartError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(meta.join(LOCK_FILE))
            .map_err(|err| StartError::MetaLock { kind: err.kind() })?;
        match file.try_lock() {
            Ok(()) => Ok(Self { _file: file }),
            Err(TryLockError::WouldBlock) => Err(StartError::MetaLocked),
            Err(TryLockError::Error(err)) => Err(StartError::MetaLock { kind: err.kind() }),
        }
    }
}

/// The stop record an earlier process left, if any.
///
/// # Errors
/// [`StartError::StopRecordUnreadable`] or [`StartError::StopRecordInvalid`]
/// when the file exists and cannot be read or does not decode: a stop is never
/// taken as absent.
pub(crate) fn read_stop(meta: &Path) -> Result<Option<StopRecord>, StartError> {
    match fs::read(meta.join(STOP_FILE)) {
        Ok(bytes) => StopRecord::decode(&bytes)
            .map(Some)
            .map_err(StartError::StopRecordInvalid),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StartError::StopRecordUnreadable { kind: err.kind() }),
    }
}

/// Makes `record` what the next process reads: written through the scratch
/// name, synced and renamed, or removed; the directory is synced either way.
///
/// # Errors
/// The first failed file operation.
pub(crate) fn write_stop(meta: &Path, record: Option<StopRecord>) -> io::Result<()> {
    match record {
        Some(record) => replace(meta, STOP_FILE, STOP_SCRATCH, &record.encode()),
        None => remove(meta, STOP_FILE),
    }
}

/// The takeover record a take-over or a restore left, if any.
///
/// # Errors
/// [`StartError::TakeoverRecordUnreadable`] or
/// [`StartError::TakeoverRecordInvalid`] when the file exists and cannot be read
/// or does not decode.
pub(crate) fn read_takeover(meta: &Path) -> Result<Option<TakeoverRecord>, StartError> {
    match fs::read(meta.join(TAKEOVER_FILE)) {
        Ok(bytes) => TakeoverRecord::decode(&bytes)
            .map(Some)
            .map_err(StartError::TakeoverRecordInvalid),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(StartError::TakeoverRecordUnreadable { kind: err.kind() }),
    }
}

/// Records a takeover through `record`'s transaction in `meta` (created if
/// missing), and removes a stop record there: the operator chose to replicate
/// over what the replica holds.
///
/// # Errors
/// The first failed file operation.
pub(crate) fn write_takeover(meta: &Path, record: TakeoverRecord) -> io::Result<()> {
    fs::create_dir_all(meta)?;
    replace(meta, TAKEOVER_FILE, TAKEOVER_SCRATCH, &record.encode())?;
    remove(meta, STOP_FILE)
}

/// Writes `text` to `meta/name` through `scratch`: synced, renamed, and the
/// directory synced.
fn replace(meta: &Path, name: &str, scratch: &str, text: &str) -> io::Result<()> {
    let scratch = meta.join(scratch);
    let mut file = File::create(&scratch)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    fs::rename(&scratch, meta.join(name))?;
    File::open(meta)?.sync_all()
}

/// Removes `meta/name` if it is there, and syncs the directory.
fn remove(meta: &Path, name: &str) -> io::Result<()> {
    match fs::remove_file(meta.join(name)) {
        Ok(()) => File::open(meta)?.sync_all(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use lsr_ltx::Txid;

    use super::*;

    #[test]
    fn a_second_lock_on_one_meta_directory_is_refused_until_the_first_goes() {
        let dir = tempfile::tempdir().unwrap();
        let first = MetaLock::take(dir.path()).unwrap();
        assert!(matches!(
            MetaLock::take(dir.path()),
            Err(StartError::MetaLocked)
        ));
        drop(first);
        MetaLock::take(dir.path()).unwrap();
    }

    #[test]
    fn a_meta_directory_that_cannot_hold_the_lock_file_fails() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(matches!(
            MetaLock::take(&missing),
            Err(StartError::MetaLock {
                kind: io::ErrorKind::NotFound
            })
        ));
    }

    #[test]
    fn the_stop_record_round_trips_and_goes() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_stop(dir.path()).unwrap(), None);
        let record = StopRecord::other_writer(Txid::new(7));
        write_stop(dir.path(), Some(record)).unwrap();
        assert_eq!(read_stop(dir.path()).unwrap(), Some(record));
        assert!(!dir.path().join(STOP_SCRATCH).exists());
        write_stop(dir.path(), None).unwrap();
        assert_eq!(read_stop(dir.path()).unwrap(), None);
        write_stop(dir.path(), None).unwrap();
    }

    #[test]
    fn a_takeover_round_trips_and_clears_a_stop_record() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("meta");
        assert_eq!(read_takeover(&meta).unwrap(), None);
        fs::create_dir(&meta).unwrap();
        write_stop(&meta, Some(StopRecord::other_writer(Txid::new(3)))).unwrap();
        let record = TakeoverRecord::through(Txid::new(9).unwrap());
        write_takeover(&meta, record).unwrap();
        assert_eq!(read_takeover(&meta).unwrap(), Some(record));
        assert_eq!(read_stop(&meta).unwrap(), None);
        assert!(!meta.join(TAKEOVER_SCRATCH).exists());
    }

    #[test]
    fn a_takeover_record_that_does_not_decode_or_read_fails_the_start() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(TAKEOVER_FILE), b"lsr-takeover 1 0").unwrap();
        assert!(matches!(
            read_takeover(dir.path()),
            Err(StartError::TakeoverRecordInvalid(
                lsr_core::TakeoverRecordError::Unterminated
            ))
        ));
        fs::remove_file(dir.path().join(TAKEOVER_FILE)).unwrap();
        fs::create_dir(dir.path().join(TAKEOVER_FILE)).unwrap();
        assert!(matches!(
            read_takeover(dir.path()),
            Err(StartError::TakeoverRecordUnreadable { .. })
        ));
    }

    #[test]
    fn a_stop_record_that_does_not_decode_fails_the_start() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(STOP_FILE), b"lsr-stop 1 other-wr").unwrap();
        assert!(matches!(
            read_stop(dir.path()),
            Err(StartError::StopRecordInvalid(
                lsr_core::StopRecordError::Unterminated
            ))
        ));
    }

    #[test]
    fn a_stop_record_that_cannot_be_read_fails_the_start() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(STOP_FILE)).unwrap();
        assert!(matches!(
            read_stop(dir.path()),
            Err(StartError::StopRecordUnreadable { .. })
        ));
    }
}
