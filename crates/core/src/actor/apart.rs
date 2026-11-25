//! A staging directory with a local course's files kept apart in it: the store a local course
//! runs over, and the move of its files up to level 0 once a boot has listed the replica.
//! `super::local`'s docs say why they are kept apart.

use std::num::NonZeroUsize;

use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::compact::CompactError;
use crate::fault::Fault;
use crate::fault::role::Role;
use crate::layout::{CreatedAt, FileInfo, FileKey, Level};
use crate::publish::{Unpublished, Writer, remove_staged};
use crate::store::{
    BatchKind, Listing, ObjectStore, PageLimit, ReadLen, StoreError, Upload, WriteMode, exists,
};
use crate::upload::{StagedPages, same_bytes};

/// The staging directory as a local course sees it: level 0 through `base`, and above it the
/// files in the course's slot under the level-0 keys the engine knows them by. Every other
/// level passes through.
#[derive(Debug)]
pub(super) struct Apart<'a, L> {
    inner: &'a L,
    /// The newest level-0 file as the course began; `None` when level 0 held none, and
    /// every file is the slot's.
    base: Option<Txid>,
}

impl<'a, L> Apart<'a, L> {
    pub(super) const fn over(inner: &'a L, base: Option<Txid>) -> Self {
        Self { inner, base }
    }

    /// Where the directory holds the file the course calls `key`.
    fn held(&self, key: &FileKey) -> FileKey {
        if key.level() == Level::ZERO && Some(key.range().max()) > self.base {
            FileKey::new(Level::LOCAL_COURSE, key.range())
        } else {
            *key
        }
    }

    fn held_info(&self, file: &FileInfo) -> FileInfo {
        FileInfo {
            key: self.held(&file.key),
            ..*file
        }
    }
}

/// A file listed in or written to the course's slot, under the level-0 key the course
/// knows it by.
fn named(file: FileInfo) -> FileInfo {
    if file.key.level() == Level::LOCAL_COURSE {
        FileInfo {
            key: FileKey::new(Level::ZERO, file.key.range()),
            ..file
        }
    } else {
        file
    }
}

/// A write through [`Apart`]: what it commits is named as the course names it.
#[derive(Debug)]
pub(super) struct ApartUpload<U>(U);

impl<U: Upload> Upload for ApartUpload<U> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.0.write(chunk).await
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        self.0.commit().await.map(named)
    }

    async fn abort(self) -> Result<(), StoreError> {
        self.0.abort().await
    }
}

impl<L: ObjectStore> ObjectStore for Apart<'_, L> {
    type Upload = ApartUpload<L::Upload>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        if level != Level::ZERO {
            return self.inner.list(level, after, limit).await;
        }
        let past = match self.base {
            Some(base) => after.is_some_and(|after| after.max() >= base),
            None => true,
        };
        if past {
            let mut page = self.inner.list(Level::LOCAL_COURSE, after, limit).await?;
            page.files = page.files.into_iter().map(named).collect();
            return Ok(page);
        }
        let mut page = self.inner.list(Level::ZERO, after, limit).await?;
        if page.next.is_none() {
            // Level 0 ends here, and the slot's files above the base follow it.
            let base = self.base.map(TxidRange::single);
            let more = self
                .inner
                .list(Level::LOCAL_COURSE, base, PageLimit::ONE)
                .await?;
            if !more.files.is_empty() {
                page.next = base;
            }
        }
        Ok(page)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.inner.read(&self.held(key), offset, len).await
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.inner.created_at(&self.held_info(file)).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.inner
            .read_with_created_at(&self.held_info(file), len)
            .await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        let upload = self
            .inner
            .begin_upload(&self.held(key), created_at, mode)
            .await?;
        Ok(ApartUpload(upload))
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let held: Vec<FileKey> = keys.iter().map(|key| self.held(key)).collect();
        self.inner.delete(&held).await
    }

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        self.inner.remove_scratch().await
    }

    fn batch_width(&self, kind: BatchKind) -> NonZeroUsize {
        self.inner.batch_width(kind)
    }
}

impl<L: Role> Role for Apart<'_, L> {
    fn fault(err: &StoreError) -> Fault {
        L::fault(err)
    }

    fn compact_input(err: StoreError) -> CompactError {
        L::compact_input(err)
    }

    const COMPACT_WINDOW: ReadLen = L::COMPACT_WINDOW;
}

/// Moves every file in the course's slot up to level 0, oldest first.
pub(super) async fn raise_course<L: Role>(staging: &L) -> Result<(), Fault> {
    let course = Apart::over(staging, None);
    let mut pages = StagedPages::from(None);
    while let Some(page) = pages.next(&course).await? {
        for file in &page {
            raise(&course, staging, file).await?;
        }
    }
    Ok(())
}

/// Moves one of the course's files up to level 0: copied unless a copy is there, compared
/// on a second read of both, and only then deleted from the slot. A crash anywhere leaves a
/// state this runs over again, and a copy that differs is deleted, so one read that lied
/// costs a boot attempt and no file.
async fn raise<L: Role>(course: &Apart<'_, L>, staging: &L, file: &FileInfo) -> Result<(), Fault> {
    let key = &file.key;
    if !exists(staging, key).await.map_err(|err| L::fault(&err))? {
        copy(course, staging, file).await?;
    }
    if !same_bytes(course, staging, key).await? {
        remove_staged(staging, &[*key])
            .await
            .map_err(|err| L::fault(&err))?;
        return Err(Fault::LocalIo);
    }
    remove_staged(course, &[*key])
        .await
        .map_err(|err| L::fault(&err))
}

/// Writes the course's file `file` at its level-0 key, with the created-at time it was
/// staged under.
async fn copy<L: Role>(course: &Apart<'_, L>, staging: &L, file: &FileInfo) -> Result<(), Fault> {
    let key = &file.key;
    let stamp = course
        .created_at(file)
        .await
        .map_err(|err| L::fault(&err))?;
    let mut to = Writer::begin(staging, key, stamp.at(), WriteMode::New)
        .await
        .map_err(|err| L::fault(&err))?;
    let len = ReadLen::CHUNK;
    let mut offset = 0_u64;
    loop {
        let moved = match course.read(key, offset, len).await {
            Ok(chunk) => to.write(&chunk).await.map(|()| chunk.len()),
            Err(err) => Err(err),
        };
        let read = match moved {
            Ok(read) => u64::try_from(read).unwrap_or(u64::MAX),
            Err(err) => {
                // Abort only releases scratch: nothing is visible either way.
                drop(to.abort().await);
                return Err(L::fault(&err));
            }
        };
        offset = offset.saturating_add(read);
        if read < len.get() {
            break;
        }
    }
    match to.commit().await {
        // Taken: a boot attempt that died here before left the copy, which the caller
        // compares.
        Ok(_) | Err(Unpublished::Taken(_)) => Ok(()),
        Err(other) => Err(L::fault(&other.into_error())),
    }
}
