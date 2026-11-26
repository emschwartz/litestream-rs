//! The dry run: the real [restore] of the latest state into a sink that
//! keeps a checksum per page, through a store that counts what it reads.

use std::collections::BTreeMap;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use lsr_ltx::{DbChecksum, DbPages, PageChecksum, PageNo, PageRef, PageSize, Timestamp, TxidRange};

use super::VerifyError;
use crate::inventory::Inventory;
use crate::layout::{CreatedAt, FileInfo};
use crate::layout::{FileKey, Level};
use crate::plan::RestoreTarget;
use crate::restore::{
    DatabaseSink, MAX_ATTEMPTS, RestoreError, RestoreOptions, Restored, restore_within,
};
use crate::store::{Listing, ObjectStore, PageLimit, ReadLen, StoreError, WriteMode};

/// A [DatabaseSink] that keeps each page's checksum and nothing else: 8 bytes
/// a page, no page bytes, and nothing is ever published.
pub(crate) struct DigestSink<'a> {
    /// Page number to checksum.
    pub(crate) pages: &'a mut BTreeMap<u32, PageChecksum>,
    /// One page whose bytes are kept as well: its number, and where its latest write lands.
    pub(crate) keep: Option<(u32, &'a mut Option<Vec<u8>>)>,
}

impl DatabaseSink for DigestSink<'_> {
    fn write_page(&mut self, page: PageRef<'_>) -> io::Result<()> {
        self.pages.insert(page.pgno().get(), page.checksum());
        if let Some((n, kept)) = self.keep.as_mut()
            && *n == page.pgno().get()
        {
            **kept = Some(page.data().to_vec());
        }
        Ok(())
    }

    fn truncate(&mut self, pages: DbPages, _page_size: PageSize) -> io::Result<()> {
        let pages = pages.get();
        drop(self.pages.split_off(&pages.saturating_add(1)));
        if let Some((n, kept)) = self.keep.as_mut()
            && *n > pages
        {
            **kept = None;
        }
        Ok(())
    }

    fn reset(&mut self) -> io::Result<()> {
        self.pages.clear();
        if let Some((_, kept)) = self.keep.as_mut() {
            **kept = None;
        }
        Ok(())
    }

    fn checksum(&mut self, pages: DbPages, page_size: PageSize) -> io::Result<DbChecksum> {
        let zero_page = vec![0; page_size.bytes()];
        let mut total = DbChecksum::EMPTY;
        for n in 1..=pages.get() {
            // The lock page is the one number PageNo refuses.
            let Some(pgno) = PageNo::new(n, page_size) else {
                continue;
            };
            let page = self
                .pages
                .get(&n)
                .copied()
                .unwrap_or_else(|| PageChecksum::of(pgno, &zero_page));
            total = total.toggle(page);
        }
        Ok(total)
    }

    type PublishError = std::convert::Infallible;

    fn publish(self) -> Result<(), Self::PublishError> {
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

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        let (chunk, created_at) = self.inner.read_with_created_at(file, len).await?;
        self.files.fetch_add(1, Ordering::Relaxed);
        let got = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        self.bytes.fetch_add(got, Ordering::Relaxed);
        Ok((chunk, created_at))
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        len: Option<u64>,
    ) -> Result<Self::Upload, StoreError> {
        self.inner
            .begin_sized_upload(key, created_at, mode, len)
            .await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.inner.delete(keys).await
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
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
    listed: &Inventory,
    last: FileKey,
) -> Result<DryRun, VerifyError> {
    let mut pages = BTreeMap::new();
    let (result, files_read, bytes_read) =
        digest_restore(store, listed, RestoreTarget::Latest, &mut pages).await;
    let fault = match result {
        Ok(_) => None,
        Err(RestoreError::Inventory(err)) => return Err(VerifyError::Inventory(err)),
        Err(RestoreError::Store(err)) => return Err(VerifyError::Store(err)),
        Err(err) => Some((named_file(&err, last), err)),
    };
    Ok(DryRun {
        fault,
        files_read,
        bytes_read,
    })
}

/// The real restore of `target`, planned over `listed`, into `pages` (one checksum a page,
/// nothing published), with the files and bytes it read.
pub(super) async fn digest_restore<S: ObjectStore>(
    store: &S,
    listed: &Inventory,
    target: RestoreTarget,
    pages: &mut BTreeMap<u32, PageChecksum>,
) -> (Result<Restored, RestoreError>, usize, u64) {
    let counting = Counting {
        inner: store,
        files: AtomicUsize::new(0),
        bytes: AtomicU64::new(0),
    };
    let result = restore_within(
        &counting,
        Some(listed),
        target,
        RestoreOptions::default(),
        DigestSink { pages, keep: None },
        MAX_ATTEMPTS,
    )
    .await;
    (
        result,
        counting.files.load(Ordering::Relaxed),
        counting.bytes.load(Ordering::Relaxed),
    )
}

fn named_file(error: &RestoreError, last: FileKey) -> Option<FileKey> {
    match error {
        RestoreError::File { key, .. } => Some(*key),
        RestoreError::Unstable { missing, .. } => Some(*missing),
        RestoreError::DatabaseChecksum { .. } => Some(last),
        RestoreError::CreatedAtRepeated { key } => Some(*key),
        RestoreError::Inventory(_)
        | RestoreError::NoPlan(_)
        | RestoreError::Store(_)
        | RestoreError::Sink { .. }
        | RestoreError::Publish { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::block_on;
    use crate::mem::MemStore;

    /// The dry run's counting wrapper forwards the boot's scratch removal and its count.
    #[test]
    fn remove_scratch_is_forwarded() {
        let mem = MemStore::new();
        mem.plant_scratch(7);
        let counting = Counting {
            inner: &mem,
            files: AtomicUsize::new(0),
            bytes: AtomicU64::new(0),
        };
        assert_eq!(block_on(counting.remove_scratch()), Ok(7));
        assert_eq!(mem.scratch(), 0);
    }
}
