//! The filesystem under [`crate::FsStore`]: the calls the store makes, and [`RealFs`], which makes
//! them on the host's filesystem through `std::fs`.
//!
//! The seam exists so that the store itself, not a double of it, can run on an in-memory
//! filesystem that models what a power loss keeps (the simulator's `MemFs`). It is cut at the
//! calls the store makes and no wider: every path the store touches, every sync and every rename
//! goes through it, so a crash law applied below it sees exactly the durability steps the store
//! takes on a real disk.

use std::ffi::OsString;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::SystemTime;

/// What the store reads of a file's metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Meta {
    is_file: bool,
    len: u64,
    modified: SystemTime,
}

impl Meta {
    /// Metadata of an entry: whether it is a regular file, its length, its mtime.
    #[must_use]
    pub const fn new(is_file: bool, len: u64, modified: SystemTime) -> Self {
        Self {
            is_file,
            len,
            modified,
        }
    }

    /// Whether the entry is a regular file (after following symlinks).
    #[must_use]
    pub const fn is_file(&self) -> bool {
        self.is_file
    }

    /// The length in bytes.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether the length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The mtime, where this store keeps a file's created-at time.
    #[must_use]
    pub const fn modified(&self) -> SystemTime {
        self.modified
    }
}

/// One name in a directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntryInfo {
    name: OsString,
    is_symlink: bool,
}

impl DirEntryInfo {
    /// An entry named `name`; `is_symlink` when the entry itself is a link.
    #[must_use]
    pub const fn new(name: OsString, is_symlink: bool) -> Self {
        Self { name, is_symlink }
    }

    /// The entry's name within its directory.
    #[must_use]
    pub fn name(&self) -> &OsString {
        &self.name
    }

    /// Whether the entry is a symlink (not followed).
    #[must_use]
    pub const fn is_symlink(&self) -> bool {
        self.is_symlink
    }
}

/// The filesystem calls [`crate::FsStore`] makes. Each has the meaning of its `std::fs`
/// namesake, errors included: a caller matches on [`io::ErrorKind`] (`NotFound`,
/// `AlreadyExists`) exactly as it would on the real call.
pub trait Fs: Clone + Send + Sync + Debug {
    /// An open file.
    type File: FsFile;

    /// The names in `dir`, in no particular order.
    ///
    /// # Errors
    /// As `std::fs::read_dir`.
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntryInfo>>;

    /// The metadata of `path`, following symlinks.
    ///
    /// # Errors
    /// As `std::fs::metadata`.
    fn metadata(&self, path: &Path) -> io::Result<Meta>;

    /// Whether `path` exists.
    ///
    /// # Errors
    /// As `Path::try_exists`.
    fn try_exists(&self, path: &Path) -> io::Result<bool>;

    /// Creates the directory `path`, whose parent exists.
    ///
    /// # Errors
    /// As `std::fs::create_dir`.
    fn create_dir(&self, path: &Path) -> io::Result<()>;

    /// Makes the entries of directory `dir` durable.
    ///
    /// # Errors
    /// The sync's error.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;

    /// Renames `from` over `to`.
    ///
    /// # Errors
    /// As `std::fs::rename`.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Gives the file at `from` the second name `to`, failing with `AlreadyExists` when `to`
    /// is taken.
    ///
    /// # Errors
    /// As `std::fs::hard_link`; `Unsupported` where the filesystem has no hard links.
    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Removes the name `path`.
    ///
    /// # Errors
    /// As `std::fs::remove_file`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Creates `path` for writing, failing with `AlreadyExists` when the name is taken.
    ///
    /// # Errors
    /// As `OpenOptions::create_new`.
    fn create_new(&self, path: &Path) -> io::Result<Self::File>;

    /// Opens `path` for reading.
    ///
    /// # Errors
    /// As `File::open`.
    fn open(&self, path: &Path) -> io::Result<Self::File>;
}

/// The calls the store makes on an open file.
pub trait FsFile: Send + Debug {
    /// The open file's metadata.
    ///
    /// # Errors
    /// As `File::metadata`.
    fn metadata(&self) -> io::Result<Meta>;

    /// Up to `len` bytes from `offset`; fewer only at the end of the file.
    ///
    /// # Errors
    /// The read's error.
    fn read_at(&mut self, offset: u64, len: u64) -> io::Result<Vec<u8>>;

    /// Appends `bytes` at the file's position.
    ///
    /// # Errors
    /// As `Write::write_all`.
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Sets the mtime.
    ///
    /// # Errors
    /// As `File::set_modified`.
    fn set_modified(&self, time: SystemTime) -> io::Result<()>;

    /// Makes the file's bytes and metadata durable (not its directory entry).
    ///
    /// # Errors
    /// As `File::sync_all`.
    fn sync_all(&self) -> io::Result<()>;
}

/// The host's filesystem.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealFs;

fn meta(m: &fs::Metadata) -> io::Result<Meta> {
    Ok(Meta::new(m.is_file(), m.len(), m.modified()?))
}

impl Fs for RealFs {
    type File = File;

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntryInfo>> {
        fs::read_dir(dir)?
            .map(|entry| {
                let entry = entry?;
                let is_symlink = entry.file_type().is_ok_and(|kind| kind.is_symlink());
                Ok(DirEntryInfo::new(entry.file_name(), is_symlink))
            })
            .collect()
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        meta(&fs::metadata(path)?)
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        fs::create_dir(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::hard_link(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<File> {
        OpenOptions::new().write(true).create_new(true).open(path)
    }

    fn open(&self, path: &Path) -> io::Result<File> {
        File::open(path)
    }
}

impl FsFile for File {
    fn metadata(&self) -> io::Result<Meta> {
        meta(&File::metadata(self)?)
    }

    fn read_at(&mut self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        self.seek(SeekFrom::Start(offset))?;
        // Sized to what the file still holds, so a read holds its length and not the
        // doubled capacity `read_to_end` grows to from empty: a compaction keeps one
        // such read per input.
        let left = File::metadata(self)?.len().saturating_sub(offset);
        let mut bytes = Vec::with_capacity(usize::try_from(left.min(len)).unwrap_or(0));
        Read::by_ref(self).take(len).read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        Write::write_all(self, bytes)
    }

    fn set_modified(&self, time: SystemTime) -> io::Result<()> {
        File::set_modified(self, time)
    }

    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }
}
