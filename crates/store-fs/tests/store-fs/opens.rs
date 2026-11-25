//! A send reads its staged file through one handle, opened at its first byte: the calls the
//! filesystem sees for a file of many chunks, and what a send makes of a staged file that is
//! replaced or edited under it.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use lsr_core::conformance::block_on;
use lsr_core::{
    Did, Fault, FileKey, Level, ObjectStore, PageLimit, UploadFailed, UploadState, Uploaded,
    Witnesses, upload_next,
};
use lsr_ltx::TxidRange;
use lsr_store_fs::{DirEntryInfo, Fs, FsFile, FsStore, Meta, RealFs};

use crate::memory::{staged_database, txid};

/// What one read of a staged file for its upload asks for (core's `upload::CHUNK`).
const CHUNK: u64 = 256 * 1024;

/// What is done to a staged file under a send.
#[derive(Debug)]
enum Meddle {
    /// Another file is renamed over it.
    Replace { with: PathBuf, over: PathBuf },
    /// Its last byte is flipped where it lies.
    Edit(PathBuf),
}

/// The calls a filesystem was asked for, and a change to make before the read numbered `.0`.
#[derive(Debug, Default)]
struct Calls {
    /// `metadata` of a path.
    stats: u32,
    opens: u32,
    /// `metadata` of an open file.
    file_stats: u32,
    reads: u32,
    meddle: Option<(u32, Meddle)>,
}

/// The real filesystem, counting what a store asks of it.
#[derive(Clone, Debug, Default)]
struct Watched(Arc<Mutex<Calls>>);

impl Watched {
    fn calls(&self) -> MutexGuard<'_, Calls> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug)]
struct WatchedFile {
    file: <RealFs as Fs>::File,
    of: Watched,
}

impl Fs for Watched {
    type File = WatchedFile;

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntryInfo>> {
        RealFs.read_dir(dir)
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        let stats = self.calls().stats.saturating_add(1);
        self.calls().stats = stats;
        RealFs.metadata(path)
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        RealFs.try_exists(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        RealFs.create_dir(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        RealFs.sync_dir(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.rename(from, to)
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        RealFs.hard_link(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        RealFs.remove_file(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<Self::File> {
        let file = RealFs.create_new(path)?;
        Ok(WatchedFile {
            file,
            of: self.clone(),
        })
    }

    fn open(&self, path: &Path) -> io::Result<Self::File> {
        let opens = self.calls().opens.saturating_add(1);
        self.calls().opens = opens;
        let file = RealFs.open(path)?;
        Ok(WatchedFile {
            file,
            of: self.clone(),
        })
    }
}

impl FsFile for WatchedFile {
    fn metadata(&self) -> io::Result<Meta> {
        let stats = self.of.calls().file_stats.saturating_add(1);
        self.of.calls().file_stats = stats;
        FsFile::metadata(&self.file)
    }

    fn read_at(&mut self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let due = {
            let mut calls = self.of.calls();
            calls.reads = calls.reads.saturating_add(1);
            let reads = calls.reads;
            calls.meddle.take_if(|(at, _)| *at == reads)
        };
        match due {
            Some((_, Meddle::Replace { with, over })) => std::fs::rename(with, over)?,
            Some((_, Meddle::Edit(path))) => flip_last_byte(&path)?,
            None => {}
        }
        FsFile::read_at(&mut self.file, offset, len)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        FsFile::write_all(&mut self.file, bytes)
    }

    fn set_modified(&self, time: SystemTime) -> io::Result<()> {
        FsFile::set_modified(&self.file, time)
    }

    fn sync_all(&self) -> io::Result<()> {
        FsFile::sync_all(&self.file)
    }
}

fn flip_last_byte(path: &Path) -> io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let mut last = [0_u8; 1];
    file.seek(SeekFrom::End(-1))?;
    file.read_exact(&mut last)?;
    file.seek(SeekFrom::End(-1))?;
    Write::write_all(&mut file, &[!last[0]])
}

/// A staged database of 3,000 pages (about 12 MiB, so two slices and some fifty chunks), a
/// store that counts what reading it asks of the filesystem, and an empty replica.
struct Rig {
    _dirs: (tempfile::TempDir, tempfile::TempDir),
    staging: FsStore<Watched>,
    watched: Watched,
    replica: FsStore,
    witnesses: Witnesses,
    size: u64,
}

fn rig() -> Rig {
    let (dir, _staged, witnesses, size) = staged_database(3_000);
    let watched = Watched::default();
    let to = tempfile::tempdir().unwrap();
    Rig {
        staging: FsStore::with_fs(dir.path(), watched.clone()),
        watched,
        replica: FsStore::new(to.path()),
        witnesses,
        size,
        _dirs: (dir, to),
    }
}

fn key() -> FileKey {
    FileKey::new(Level::ZERO, TxidRange::new(txid(1), txid(1)).unwrap())
}

impl Rig {
    fn send(&self) -> Result<Uploaded, UploadFailed> {
        block_on(upload_next(
            UploadState::At(None),
            &self.staging,
            &self.replica,
            Some(txid(1)),
            &self.witnesses,
        ))
    }

    fn staged_path(&self) -> PathBuf {
        self.staging.file_path(&key())
    }

    fn replica_holds(&self) -> usize {
        block_on(self.replica.list(Level::ZERO, None, PageLimit::MAX))
            .unwrap()
            .files
            .len()
    }
}

/// One open for the file however many chunks it is read in, and one read a chunk. Opening
/// the file again for every chunk cost a stat of the path, an open, a stat of the file and a
/// close a chunk beside the read, and gave the kernel a new file to learn the read pattern
/// of each time.
#[test]
fn a_send_opens_its_staged_file_once_however_many_chunks_it_reads() {
    let rig = rig();
    assert!(matches!(rig.send().unwrap().did, Did::Put(_)));
    // Whole chunks, then the read that comes back short (or empty) and ends the file.
    let chunks = u32::try_from(rig.size / CHUNK).unwrap().saturating_add(1);
    assert!(chunks > 40, "{chunks}");
    let calls = rig.watched.calls();
    assert_eq!(calls.opens, 1, "{calls:?}");
    assert_eq!(calls.stats, 1, "{calls:?}");
    assert_eq!(calls.reads, chunks, "{calls:?}");
    // Asked once whether what was opened is a regular file.
    assert_eq!(calls.file_stats, 1, "{calls:?}");
}

/// The handle follows the file that was opened, not its name: a file renamed over the staged
/// one part-way through is not read, and what goes up is the file the send began with, every
/// chunk of it verified on the read that produced it.
#[test]
fn a_staged_file_replaced_under_a_send_goes_up_as_it_was_opened() {
    let rig = rig();
    let path = rig.staged_path();
    let original = std::fs::read(&path).unwrap();
    let other = path.with_extension("other");
    std::fs::copy(&path, &other).unwrap();
    flip_last_byte(&other).unwrap();
    rig.watched.calls().meddle = Some((
        3,
        Meddle::Replace {
            with: other,
            over: path,
        },
    ));
    assert!(matches!(rig.send().unwrap().did, Did::Put(_)));
    assert!(rig.watched.calls().meddle.is_none());
    let sent = std::fs::read(rig.replica.file_path(&key())).unwrap();
    assert!(sent == original, "the replica holds other bytes");
}

/// A staged file changed where it lies is the same open file with other bytes: the send
/// reads them, they do not verify, and nothing is published.
#[test]
fn a_staged_file_edited_under_a_send_is_refused_and_leaves_nothing_visible() {
    let rig = rig();
    rig.watched.calls().meddle = Some((3, Meddle::Edit(rig.staged_path())));
    let failed = rig.send().unwrap_err();
    assert_eq!(failed.fault, Fault::StagedLost);
    assert!(rig.watched.calls().meddle.is_none());
    assert_eq!(rig.replica_holds(), 0);
}
