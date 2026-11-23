//! The host filesystem for the harness's staging and replica stores, with plain `fsync(2)`.
//!
//! `std::fs::File::sync_all` is `fsync` on Linux but `fcntl(F_FULLFSYNC)` on macOS, which asks
//! the drive to flush its cache: 8 ms a call on an Apple SSD against 0.1 ms for `fsync`. The
//! store syncs every file it writes and every directory it changes, so a cycle of the fault
//! matrix paid 135 s of that on macOS. The stores here only have to hold what the harness reads
//! back in the same run, and Linux CI already runs exactly `fsync`, so this keeps the call and
//! drops the drive flush. Everything else is [`RealFs`].

use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::time::SystemTime;

use lsr_store_fs::{DirEntryInfo, Fs, FsFile, Meta, RealFs};

/// [`RealFs`] whose syncs are `fsync(2)` on every platform.
#[derive(Clone, Copy, Debug, Default)]
pub struct HostFs;

fn fsync(file: &File) -> io::Result<()> {
    rustix::fs::fsync(file).map_err(io::Error::from)
}

impl Fs for HostFs {
    type File = HostFile;

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntryInfo>> {
        RealFs.read_dir(dir)
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        RealFs.metadata(path)
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        RealFs.try_exists(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        RealFs.create_dir(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        fsync(&RealFs.open(dir)?)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.rename(from, to)
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.hard_link(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        RealFs.create_new(path).map(HostFile)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        RealFs.open(path).map(HostFile)
    }
}

/// An open file of [`HostFs`].
#[derive(Debug)]
pub struct HostFile(File);

impl FsFile for HostFile {
    fn metadata(&self) -> io::Result<Meta> {
        FsFile::metadata(&self.0)
    }

    fn read_at(&mut self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        self.0.read_at(offset, len)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        FsFile::write_all(&mut self.0, bytes)
    }

    fn set_modified(&self, time: SystemTime) -> io::Result<()> {
        FsFile::set_modified(&self.0, time)
    }

    fn sync_all(&self) -> io::Result<()> {
        fsync(&self.0)
    }
}
