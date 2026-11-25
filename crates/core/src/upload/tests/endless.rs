//! A staging directory with more files than anything should hold in memory: a listing that
//! never ends, whose files never verify. A boot pass that read the whole listing before
//! looking at the first file would never get to the first file; these show it never needs to.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use super::*;
use crate::fault::role::{Replica, Staging};
use crate::layout::{CreatedAt, ListedTime};
use crate::witness::check_pending;

/// Pages served before the listing gives up with an error, so a pass that reads it all fails
/// instead of running forever.
const PAGES: u32 = 50;

/// One level-0 file per transaction, from 1 on, without end. Every read finds nothing.
#[derive(Clone, Default)]
struct Endless {
    pages: Arc<AtomicU32>,
}

impl Endless {
    fn pages(&self) -> u32 {
        self.pages.load(Ordering::SeqCst)
    }
}

impl ObjectStore for Endless {
    type Upload = MemUpload;

    /// Leaves no scratch.
    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    fn list(
        &self,
        _level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> impl Future<Output = Result<Listing, StoreError>> + Send {
        let served = self.pages.fetch_add(1, Ordering::SeqCst);
        let first = after.map_or(1, |range| range.max().get().saturating_add(1));
        let count = u64::try_from(limit.get()).unwrap();
        let listing = (served < PAGES).then(|| {
            let files: Vec<FileInfo> = (first..first.saturating_add(count))
                .map(|n| FileInfo {
                    key: key(n),
                    size: 1,
                    listed: ListedTime::Created(CreatedAt::mtime(created_at(n))),
                })
                .collect();
            let next = files.last().map(|info| info.key.range());
            Listing {
                files,
                next,
                skipped: 0,
            }
        });
        async move { listing.ok_or_else(transient) }
    }

    fn read(
        &self,
        key: &FileKey,
        _offset: u64,
        _len: ReadLen,
    ) -> impl Future<Output = Result<Vec<u8>, StoreError>> + Send {
        let key = *key;
        async move { Err(StoreError::NotFound { key }) }
    }

    fn created_at(
        &self,
        file: &FileInfo,
    ) -> impl Future<Output = Result<CreatedAt, StoreError>> + Send {
        let key = file.key;
        async move { Err(StoreError::NotFound { key }) }
    }

    fn read_with_created_at(
        &self,
        file: &FileInfo,
        _len: ReadLen,
    ) -> impl Future<Output = Result<(Vec<u8>, CreatedAt), StoreError>> + Send {
        let key = file.key;
        async move { Err(StoreError::NotFound { key }) }
    }

    async fn begin_sized_upload(
        &self,
        _key: &FileKey,
        _created_at: Timestamp,
        _mode: crate::store::WriteMode,
        _len: Option<u64>,
    ) -> Result<MemUpload, StoreError> {
        Err(transient())
    }

    async fn delete(&self, _keys: &[FileKey]) -> Result<(), StoreError> {
        Ok(())
    }
}

/// The sweep stops at the first staged file that does not verify; it reads no further into
/// the listing than the page that holds it.
#[test]
fn the_sweep_judges_the_first_staged_file_without_listing_them_all() {
    let staging = Endless::default();
    let swept = block_on(sweep_staged(&Staging(&staging), None, None));
    assert_eq!(swept, Ok(Swept::Suspect(key(1))));
    assert_eq!(staging.pages(), 1);
}

/// Boot refuses the first pending file and every one after it when it does not verify. The
/// pass lists no more than the witness cap's worth of files (one past it), however many are
/// staged.
#[test]
fn the_witness_pass_refuses_from_the_first_pending_file_without_listing_them_all() {
    let staging = Endless::default();
    let pending = block_on(check_pending(
        &Staging(&staging),
        &Replica(&MemStore::new()),
        None,
    ))
    .unwrap();
    assert_eq!(pending.refused, Some(txid(1)));
    assert!(pending.tokens.is_empty());
    let page = PageLimit::MAX.get();
    let cap_pages = crate::witness::MAX_WITNESSED
        .saturating_add(1)
        .div_ceil(page);
    assert_eq!(usize::try_from(staging.pages()).unwrap(), cap_pages);
}

/// A drop across several listing pages deletes every file from `from` on, newest first, so a
/// crash part way always leaves the oldest of them, which the next boot finds again.
#[test]
fn a_drop_across_pages_deletes_newest_first() {
    let staging = Probe::default();
    let count = u64::try_from(PageLimit::MAX.get())
        .unwrap()
        .saturating_mul(2)
        .saturating_add(500);
    for n in 1..=count {
        put_bytes(&staging.inner, key(n), b"x");
    }
    block_on(drop_staged_from(&Staging(&staging), txid(2))).unwrap();
    let deleted: Vec<u64> = staging
        .log()
        .deletes
        .iter()
        .map(|key| key.range().max().get())
        .collect();
    let want: Vec<u64> = (2..=count).rev().collect();
    assert_eq!(deleted, want);
    assert_eq!(files(&staging.inner).len(), 1);
}
