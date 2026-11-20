//! The local filesystem side of Litestream.
//!
//! Today that is [FileSink], where a restore builds its database. Phase 2 adds
//! the meta directory and WAL reading here.
//!
//! A restore writes a scratch file next to the target, never the target. Only
//! [FileSink::publish] makes the database visible, in this order: fsync the
//! scratch file; check the target is still absent; remove a stale
//! `<target>-wal` and `<target>-shm` (SQLite would replay a stale WAL over
//! the restored pages); hard-link the scratch to the target, which fails when
//! the target exists, so a database created meanwhile is never overwritten;
//! unlink the scratch; fsync the directory. A step that fails after the link
//! unlinks the target again, so an error always means "not published". An
//! fsync is never retried: after a failed fsync the kernel may have dropped the
//! dirty pages, and a second one would report success over lost data.
//!
//! The hard link needs a filesystem that supports them (every Unix one a
//! database lives on does).

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

use lsr_core::DatabaseSink;
use lsr_ltx::{Checksum, PageNo, PageSize};
use thiserror::Error;

/// Scratch names tried before giving up: a name can only collide with a file a
/// crashed process of the same pid left behind.
const SCRATCH_TRIES: u32 = 8;

static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Why a [FileSink] could not be created.
#[derive(Debug, Error)]
pub enum CreateError {
    /// The target path exists. A restore never overwrites a database.
    #[error("{} already exists; a restore never overwrites a database", .0.display())]
    TargetExists(PathBuf),
    /// The target path could not be inspected.
    #[error("inspecting {}: {source}", path.display())]
    Inspect {
        /// The target.
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

/// A database being restored into a scratch file beside its target.
#[derive(Debug)]
pub struct FileSink {
    file: File,
    scratch: PathBuf,
    target: PathBuf,
    published: bool,
}

fn suffixed(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

fn exists(path: &Path) -> io::Result<bool> {
    match path.symlink_metadata() {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(err) if err.kind() != io::ErrorKind::NotFound => Err(err),
        Ok(()) | Err(_) => Ok(()),
    }
}

impl FileSink {
    /// Starts a restore of [target]: creates the scratch file
    /// `<target>.<pid>-<n>.restore` exclusively. Nothing exists at [target] yet.
    ///
    /// # Errors
    /// [CreateError::TargetExists] when anything is at [target].
    pub fn create(target: &Path) -> Result<Self, CreateError> {
        let inspect = |source| CreateError::Inspect {
            path: target.to_path_buf(),
            source,
        };
        if exists(target).map_err(inspect)? {
            return Err(CreateError::TargetExists(target.to_path_buf()));
        }
        let mut tries = 1_u32;
        loop {
            let n = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
            let scratch = suffixed(target, &format!(".{}-{n}.restore", process::id()));
            let opened = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&scratch);
            match opened {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        scratch,
                        target: target.to_path_buf(),
                        published: false,
                    });
                }
                Err(source)
                    if source.kind() == io::ErrorKind::AlreadyExists && tries < SCRATCH_TRIES =>
                {
                    tries = tries.saturating_add(1);
                }
                Err(source) => {
                    return Err(CreateError::Scratch {
                        path: scratch,
                        source,
                    });
                }
            }
        }
    }

    /// The scratch file's path, for diagnostics.
    #[must_use]
    pub fn scratch_path(&self) -> &Path {
        &self.scratch
    }

    fn link_and_sync(&self) -> io::Result<()> {
        fs::hard_link(&self.scratch, &self.target)?;
        let finished = fs::remove_file(&self.scratch).and_then(|()| sync_dir(&self.target));
        if finished.is_err() {
            // The target is ours: the link succeeded only because it was absent.
            drop(fs::remove_file(&self.target));
        }
        finished
    }
}

fn sync_dir(target: &Path) -> io::Result<()> {
    let dir = match target.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        Some(_) | None => Path::new("."),
    };
    File::open(dir)?.sync_all()
}

fn page_offset(pgno: PageNo, page_len: usize) -> io::Result<u64> {
    let len = u64::try_from(page_len).map_err(io::Error::other)?;
    u64::from(pgno.get().saturating_sub(1))
        .checked_mul(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "page offset overflows"))
}

impl DatabaseSink for FileSink {
    fn write_page(&mut self, pgno: PageNo, data: &[u8]) -> io::Result<()> {
        self.file
            .seek(SeekFrom::Start(page_offset(pgno, data.len())?))?;
        self.file.write_all(data)
    }

    fn truncate(&mut self, pages: u32, page_size: PageSize) -> io::Result<()> {
        self.file
            .set_len(u64::from(pages).saturating_mul(u64::from(page_size.get())))
    }

    fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)
    }

    fn checksum(&mut self, pages: u32, page_size: PageSize) -> io::Result<Checksum> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut page = vec![0_u8; page_size.bytes()];
        let mut total = Checksum::EMPTY;
        for n in 1..=pages {
            self.file.read_exact(&mut page)?;
            // The lock page is the one number PageNo refuses; its bytes are
            // read past and not counted.
            if let Some(pgno) = PageNo::new(n, page_size) {
                total = total.toggle(Checksum::of_page(pgno, &page));
            }
        }
        Ok(total)
    }

    fn publish(mut self) -> io::Result<()> {
        self.file.sync_all()?;
        if exists(&self.target)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the target appeared during the restore",
            ));
        }
        remove_if_present(&suffixed(&self.target, "-wal"))?;
        remove_if_present(&suffixed(&self.target, "-shm"))?;
        self.link_and_sync()?;
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
    fn the_scratch_is_named_for_the_target_and_unique_per_sink() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let (first, second) = (
            FileSink::create(&target).unwrap(),
            FileSink::create(&target).unwrap(),
        );
        assert_ne!(first.scratch_path(), second.scratch_path());
        for sink in [&first, &second] {
            let name = sink.scratch_path().file_name().unwrap().to_str().unwrap();
            assert!(
                name.starts_with(&format!("db.{}-", process::id())),
                "{name}"
            );
            assert!(name.ends_with(".restore"), "{name}");
        }
        assert!(!target.exists());
    }

    #[test]
    fn the_scratch_is_removed_on_drop_unless_published() {
        let dir = TempDir::new().unwrap();
        let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
        sink.write_page(pgno(1), &[1; 512]).unwrap();
        assert_eq!(names(&dir).len(), 1);
        drop(sink);
        assert!(names(&dir).is_empty());
    }

    #[test]
    fn published_bytes_are_the_pages_written_and_only_the_target_remains() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(pgno(3), &[3; 512]).unwrap();
        sink.write_page(pgno(1), &[1; 512]).unwrap();
        sink.truncate(4, ps(512)).unwrap();
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
        sink.write_page(pgno(2), &[9; 512]).unwrap();
        sink.truncate(1, ps(512)).unwrap();
        sink.truncate(2, ps(512)).unwrap();
        sink.publish().unwrap();
        assert_eq!(fs::read(&target).unwrap(), vec![0; 1024]);
    }

    #[test]
    fn checksum_reads_back_what_the_file_holds() {
        let dir = TempDir::new().unwrap();
        let mut sink = FileSink::create(&dir.path().join("db")).unwrap();
        sink.write_page(pgno(3), &[3; 512]).unwrap();
        sink.truncate(4, ps(512)).unwrap();
        let mut want = Checksum::EMPTY;
        for (n, seed) in [(1, 0_u8), (2, 0), (3, 3), (4, 0)] {
            want = want.toggle(Checksum::of_page(pgno(n), &[seed; 512]));
        }
        assert_eq!(sink.checksum(4, ps(512)).unwrap(), want);
    }

    #[test]
    fn reset_empties_the_scratch() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(pgno(1), &[1; 512]).unwrap();
        sink.reset().unwrap();
        sink.write_page(pgno(1), &[2; 512]).unwrap();
        sink.publish().unwrap();
        assert_eq!(fs::read(&target).unwrap(), vec![2; 512]);
    }

    #[test]
    fn publish_removes_a_stale_wal_and_shm() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        fs::write(dir.path().join("db-wal"), b"stale").unwrap();
        fs::write(dir.path().join("db-shm"), b"stale").unwrap();
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(pgno(1), &[1; 512]).unwrap();
        sink.publish().unwrap();
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
    }

    #[test]
    fn a_target_created_during_the_restore_is_not_overwritten() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("db");
        let mut sink = FileSink::create(&target).unwrap();
        sink.write_page(pgno(1), &[1; 512]).unwrap();
        fs::write(&target, b"someone else's").unwrap();
        fs::write(dir.path().join("db-wal"), b"their wal").unwrap();
        let err = sink.publish().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
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
        fs::write(dir.path().join("db-wal"), b"stale").unwrap();
        let store = replica(&snapshot_bytes(3, 7));
        let sink = FileSink::create(&target).unwrap();
        let restored = block_on(restore(&store, RestoreTarget::Latest, sink)).unwrap();
        assert_eq!(restored.commit(), 3);
        assert_eq!(fs::read(&target).unwrap(), vec![7; 1536]);
        assert_eq!(names(&dir), BTreeSet::from(["db".to_owned()]));
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
