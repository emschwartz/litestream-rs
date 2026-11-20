//! The dry run: the real [restore] of the latest state into a sink that
//! keeps a checksum per page, through a store that counts what it reads.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use lsr_ltx::{Checksum, PageNo, PageSize, Timestamp, TxidRange};

use super::VerifyError;
use crate::layout::{FileInfo, StoredStamp};
use crate::layout::{FileKey, Level};
use crate::plan::RestoreTarget;
use crate::restore::{DatabaseSink, RestoreError, restore};
use crate::store::{Listing, ObjectStore, PageLimit, ReadLen, StoreError};

/// A [DatabaseSink] that keeps each page's checksum and nothing else: 8 bytes
/// a page, no page bytes, and nothing is ever published.
#[derive(Default)]
struct DigestSink {
    pages: BTreeMap<u32, Checksum>,
}

impl DatabaseSink for DigestSink {
    fn write_page(&mut self, pgno: PageNo, data: &[u8]) -> io::Result<()> {
        self.pages.insert(pgno.get(), Checksum::of_page(pgno, data));
        Ok(())
    }

    fn truncate(&mut self, pages: u32, _page_size: PageSize) -> io::Result<()> {
        drop(self.pages.split_off(&pages.saturating_add(1)));
        Ok(())
    }

    fn reset(&mut self) -> io::Result<()> {
        self.pages.clear();
        Ok(())
    }

    fn checksum(&mut self, pages: u32, page_size: PageSize) -> io::Result<Checksum> {
        let zero_page = vec![0; page_size.bytes()];
        let mut total = Checksum::EMPTY;
        for n in 1..=pages {
            // The lock page is the one number PageNo refuses.
            let Some(pgno) = PageNo::new(n, page_size) else {
                continue;
            };
            let page = self
                .pages
                .get(&n)
                .copied()
                .unwrap_or_else(|| Checksum::of_page(pgno, &zero_page));
            total = total.toggle(page);
        }
        Ok(total)
    }

    fn publish(self) -> io::Result<()> {
        Ok(())
    }
}

/// A store that counts the files opened and bytes returned by reads.
struct Counting<'a, S> {
    inner: &'a S,
    files: AtomicUsize,
    bytes: AtomicU64,
}

impl<S: ObjectStore> ObjectStore for Counting<'_, S> {
    type Upload = S::Upload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.inner.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        let chunk = self.inner.read(key, offset, len).await?;
        if offset == 0 {
            self.files.fetch_add(1, Ordering::Relaxed);
        }
        let got = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.bytes.fetch_add(got, Ordering::Relaxed);
        Ok(chunk)
    }

    async fn stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        self.inner.stamp(file).await
    }

    async fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        let (chunk, stamp) = self.inner.read_with_stamp(file, len).await?;
        self.files.fetch_add(1, Ordering::Relaxed);
        let got = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.bytes.fetch_add(got, Ordering::Relaxed);
        Ok((chunk, stamp))
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.inner.begin_upload(key, created_at).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }
}

/// What the dry run did.
pub(super) struct DryRun {
    /// The failure, with the file it names when it names one.
    pub fault: Option<(Option<FileKey>, RestoreError)>,
    pub files_read: usize,
    pub bytes_read: u64,
}

/// Runs `restore(Latest)`. A listing or read failure says nothing about the
/// replica, so it stops the walk as in the per-file pass; every other failure
/// is the replica's. `last` is the plan's last file, which a checksum
/// mismatch of the whole database is blamed on.
pub(super) async fn latest_restore<S: ObjectStore>(
    store: &S,
    last: FileKey,
) -> Result<DryRun, VerifyError> {
    let counting = Counting {
        inner: store,
        files: AtomicUsize::new(0),
        bytes: AtomicU64::new(0),
    };
    let result = restore(&counting, RestoreTarget::Latest, DigestSink::default()).await;
    let fault = match result {
        Ok(_) => None,
        Err(RestoreError::Inventory(err)) => return Err(VerifyError::Inventory(err)),
        Err(RestoreError::Store(err)) => return Err(VerifyError::Store(err)),
        Err(err) => Some((named_file(&err, last), err)),
    };
    Ok(DryRun {
        fault,
        files_read: counting.files.load(Ordering::Relaxed),
        bytes_read: counting.bytes.load(Ordering::Relaxed),
    })
}

fn named_file(error: &RestoreError, last: FileKey) -> Option<FileKey> {
    match error {
        RestoreError::File { key, .. } => Some(*key),
        RestoreError::Unstable { missing, .. } => Some(*missing),
        RestoreError::DatabaseChecksum { .. } => Some(last),
        RestoreError::Inventory(_)
        | RestoreError::NoPlan(_)
        | RestoreError::Store(_)
        | RestoreError::Sink { .. } => None,
    }
}
