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
//! - `lsr-takeover`: an operator's word that this database may replicate over
//!   what the replica holds, through one transaction
//!   ([`lsr_core::TakeoverRecord`]). Read at start; written by
//!   [`crate::trust_local`] (`lsr trust-local`) and by every restore, through the
//!   restored transaction, and by the replicator thread when the engine asks
//!   for one before deleting staged files the replica holds, through what
//!   they vouched for; `lsr-takeover.new` is its scratch name.
//!
//! The lock is an open file description lock (`flock(2)`, which is what
//! `std::fs::File::try_lock` takes on Linux and macOS), on a file of its own.
//! Unlike a POSIX record lock (`fcntl`), closing some other descriptor of the
//! same file in this process (SQLite opens and closes many) cannot drop it,
//! and the file is never replaced, so the lock always names the inode a
//! second engine opens. It is released when the replicator thread ends, or
//! when the process dies. Descriptors are opened close-on-exec, so an
//! `-exec` child does not inherit it.
//!
//! The release is an explicit unlock, not the close alone. A child process
//! the host starts holds a copy of every descriptor from its fork until its
//! exec, and a lock of this kind lasts until the last copy closes: left to
//! the close, the lock outlived the replicator for that moment, and a
//! replicator started again in the same process was refused as a second
//! engine. An unlock through any copy releases it at once.
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

use lsr_core::TakeoverRecord;

use crate::error::StartError;
use crate::events::TrustRecordUnusable;

/// The file the engine locks.
pub(crate) const LOCK_FILE: &str = "lsr-lock";
/// The takeover record.
pub(crate) const TAKEOVER_FILE: &str = "lsr-takeover";
/// The takeover record's scratch name, renamed over it.
const TAKEOVER_SCRATCH: &str = "lsr-takeover.new";

/// The exclusive lock on a meta directory, held while this value lives.
#[derive(Debug)]
pub(crate) struct MetaLock {
    file: File,
}

impl Drop for MetaLock {
    /// Unlocks before the close (the module doc says why). An unlock that fails leaves the
    /// lock to the close, as before.
    fn drop(&mut self) {
        drop(self.file.unlock());
    }
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
            Ok(()) => Ok(Self { file }),
            Err(TryLockError::WouldBlock) => Err(StartError::MetaLocked),
            Err(TryLockError::Error(err)) => Err(StartError::MetaLock { kind: err.kind() }),
        }
    }
}

/// The takeover record `lsr trust-local`, a restore or the engine left, if any.
///
/// # Errors
/// [`TrustRecordUnusable`] when the file exists and cannot be read or does not
/// decode. The caller treats that as no record, and says so: it costs what
/// having no record costs (the boot builds only on what the WAL vouches
/// for), never a stop the WAL could have spared, and never a record taken
/// as given.
pub(crate) fn read_takeover(meta: &Path) -> Result<Option<TakeoverRecord>, TrustRecordUnusable> {
    match fs::read(meta.join(TAKEOVER_FILE)) {
        Ok(bytes) => TakeoverRecord::decode(&bytes)
            .map(Some)
            .map_err(TrustRecordUnusable::Invalid),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(TrustRecordUnusable::Unreadable { kind: err.kind() }),
    }
}

/// Records a takeover through `record`'s transaction in `meta` (created if
/// missing): written through the scratch name, synced, renamed, and the
/// directory synced. `lsr trust-local`, every restore and the engine's own
/// request before deleting staged files (`lsr_core::Actor::takeover_wanted`)
/// all write it here.
///
/// # Errors
/// The first failed file operation.
pub(crate) fn write_takeover(meta: &Path, record: TakeoverRecord) -> io::Result<()> {
    fs::create_dir_all(meta)?;
    replace(meta, TAKEOVER_FILE, TAKEOVER_SCRATCH, &record.encode())
}

/// Deletes the takeover record, synced: the engine spent it by confirming a put above it
/// (`lsr_core::Actor::takeover_spent`), and a restart must not hand it back. A record
/// already gone is no error.
///
/// # Errors
/// The first failed file operation.
pub(crate) fn remove_takeover(meta: &Path) -> io::Result<()> {
    match fs::remove_file(meta.join(TAKEOVER_FILE)) {
        Ok(()) => File::open(meta)?.sync_all(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
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

#[cfg(test)]
mod tests {
    use lsr_ltx::Txid;

    use super::*;

    /// A spent record is deleted, and deleting one already gone is no error.
    #[test]
    fn a_spent_takeover_record_is_removed_and_a_missing_one_is_no_error() {
        let dir = tempfile::tempdir().unwrap();
        let record = TakeoverRecord::through(Txid::new(4).unwrap());
        write_takeover(dir.path(), record).unwrap();
        assert!(dir.path().join(TAKEOVER_FILE).exists());
        remove_takeover(dir.path()).unwrap();
        assert!(!dir.path().join(TAKEOVER_FILE).exists());
        remove_takeover(dir.path()).unwrap();
    }

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

    /// A child process the host is starting holds a copy of the lock's descriptor until it
    /// execs. The lock goes with its holder, not with that last copy, or a replicator started
    /// again in the same process is refused as a second engine.
    #[test]
    fn the_lock_goes_with_its_holder_not_with_the_last_copy_of_its_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let first = MetaLock::take(dir.path()).unwrap();
        let copy = first.file.try_clone().unwrap();
        drop(first);
        MetaLock::take(dir.path()).unwrap();
        drop(copy);
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
    fn a_takeover_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let meta = dir.path().join("meta");
        assert_eq!(read_takeover(&meta).unwrap(), None);
        let record = TakeoverRecord::through(Txid::new(9).unwrap());
        write_takeover(&meta, record).unwrap();
        assert_eq!(read_takeover(&meta).unwrap(), Some(record));
        assert!(!meta.join(TAKEOVER_SCRATCH).exists());
    }

    #[test]
    fn a_takeover_record_that_does_not_decode_or_read_is_reported_unusable() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(TAKEOVER_FILE), b"lsr-takeover 1 0").unwrap();
        assert_eq!(
            read_takeover(dir.path()),
            Err(TrustRecordUnusable::Invalid(
                lsr_core::TakeoverRecordError::Unterminated
            ))
        );
        fs::remove_file(dir.path().join(TAKEOVER_FILE)).unwrap();
        fs::create_dir(dir.path().join(TAKEOVER_FILE)).unwrap();
        assert!(matches!(
            read_takeover(dir.path()),
            Err(TrustRecordUnusable::Unreadable { .. })
        ));
    }
}
