//! `S3Upload`: one object being written, as a single PUT or a multipart upload.
//!
//! Writes accumulate in one buffer of at most one part (the memory bound). An
//! object that ends below the part size is sent as one PUT, as Go does (rc.go:814).
//! The write that fills the buffer starts a multipart upload (the stamp rides on
//! CreateMultipartUpload), and each full buffer is uploaded as the next part,
//! sequentially (ruling F5). A failed part fails the upload and the caller
//! restarts it; nothing is retried here.
//!
//! A single PUT is judged by the status, and then by the `ETag`: for a PUT
//! without server-side encryption the ETag is the object's MD5, so a different
//! one means the bytes stored are not the bytes sent. The object is already
//! visible then, so it is deleted (best effort) before the error returns, as the
//! file store does for a size mismatch; the uploader writes the same key again
//! without listing. A completed multipart object with a wrong-shaped ETag is
//! treated the same way; a wrong part ETag leaves nothing visible, so the upload
//! id goes on the orphan list instead.
//!
//! Dropping a multipart upload does no I/O: its id goes on the orphan list
//! (`orphans.rs`).

use std::io;
use std::sync::Arc;

use lsr_core::{
    CorruptCause, FileInfo, FileKey, ListedStamp, PartNumber, StoreError, StoreOp, StoredStamp,
    Upload,
};
use lsr_ltx::Timestamp;

use crate::multipart::completed_etag_fits;
use crate::request::UploadId;
use crate::response::{EtagCheck, check_etag};
use crate::store::Inner;
use crate::transport::Transport;

/// The HTTP status a store-side "too large" is reported as: an upload that
/// would need more than 10000 parts.
const PAYLOAD_TOO_LARGE: u16 = 413;

/// Where an upload is. `Done` is after a commit, an abort or a failure: the
/// upload id, if there was one, is already with the caller or on the orphan list.
enum UploadState {
    Buffering(Vec<u8>),
    Multipart {
        id: UploadId,
        /// The ETag of part n is the (n - 1)th entry.
        etags: Vec<String>,
        buf: Vec<u8>,
    },
    Done,
}

/// One object being written. Dropping it does no I/O.
pub struct S3Upload<T> {
    inner: Arc<Inner<T>>,
    key: FileKey,
    stamp: Timestamp,
    written: u64,
    state: UploadState,
}

impl<T> std::fmt::Debug for S3Upload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (multipart, buffered) = match &self.state {
            UploadState::Buffering(buf) => (false, buf.len()),
            UploadState::Multipart { buf, .. } => (true, buf.len()),
            UploadState::Done => (false, 0),
        };
        f.debug_struct("S3Upload")
            .field("key", &self.key)
            .field("multipart", &multipart)
            .field("buffered", &buffered)
            .finish_non_exhaustive()
    }
}

impl<T> S3Upload<T> {
    pub(crate) fn new(inner: Arc<Inner<T>>, key: FileKey, stamp: Timestamp) -> Self {
        Self {
            inner,
            key,
            stamp,
            written: 0,
            state: UploadState::Buffering(Vec::new()),
        }
    }

    /// The buffer's capacity, so a test can prove the memory bound.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn buffer_capacity(&self) -> usize {
        match &self.state {
            UploadState::Buffering(buf) | UploadState::Multipart { buf, .. } => buf.capacity(),
            UploadState::Done => 0,
        }
    }

    /// Moves a live multipart id to the orphan list and ends the upload.
    fn orphan(&mut self) {
        if let UploadState::Multipart { id, .. } =
            std::mem::replace(&mut self.state, UploadState::Done)
        {
            self.inner.orphans.push(self.key, id);
        }
    }
}

impl<T> Drop for S3Upload<T> {
    fn drop(&mut self) {
        self.orphan();
    }
}

/// An upload used after it failed or finished: no HTTP exchange happened.
fn ended() -> StoreError {
    StoreError::Refused {
        op: StoreOp::Upload,
        status: 0,
    }
}

/// Appends `bytes` (which fit under `part`) growing the buffer geometrically
/// but never past one part, so a part-sized buffer is the most memory held.
fn append_bounded(buf: &mut Vec<u8>, bytes: &[u8], part: usize) {
    let need = buf.len().saturating_add(bytes.len());
    if need > buf.capacity() {
        let target = need
            .max(buf.capacity().saturating_mul(2))
            .max(4096)
            .min(part);
        buf.reserve_exact(target.saturating_sub(buf.len()));
    }
    buf.extend_from_slice(bytes);
}

impl<T: Transport> Inner<T> {
    /// One PUT of `body` under `key`; see the module docs for the `ETag` rule.
    pub(crate) fn put(
        &self,
        key: &FileKey,
        stamp: Timestamp,
        body: Vec<u8>,
    ) -> Result<FileInfo, StoreError> {
        let op = StoreOp::Upload;
        let wrote = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let request = self.request(op, |b, now| b.put_object(now, key, body, stamp))?;
        let sent = request.content_md5();
        let response = self.exec(op, Some(key), request)?;
        match sent.map_or(EtagCheck::NotMd5, |md5| check_etag(&response, md5)) {
            EtagCheck::Matches | EtagCheck::NotMd5 => Ok(FileInfo {
                key: *key,
                size: wrote,
                listed: ListedStamp::Stored(StoredStamp::metadata(stamp)),
            }),
            EtagCheck::Missing => Err(StoreError::Transient {
                op,
                kind: io::ErrorKind::InvalidData,
            }),
            EtagCheck::Mismatch => Err(self.delete_bad_object(key, None)),
        }
    }

    /// Deletes an object the store acknowledged wrongly, best effort, and
    /// returns the error to report. Whether the delete lands changes nothing
    /// the caller can do now.
    fn delete_bad_object(&self, key: &FileKey, part: Option<PartNumber>) -> StoreError {
        match self.delete(&[*key]) {
            Ok(()) | Err(_) => {}
        }
        StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::ChecksumMismatch { part },
        }
    }
}

impl<T: Transport> S3Upload<T> {
    fn part_len(&self) -> usize {
        usize::try_from(self.inner.part_size.get()).unwrap_or(usize::MAX)
    }

    /// Fills the buffer from `chunk`, uploading each part as it fills.
    fn append(&mut self, mut chunk: &[u8]) -> Result<(), StoreError> {
        let start = self.inner.now();
        let part = self.part_len();
        self.written = self
            .written
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        while !chunk.is_empty() {
            let (UploadState::Buffering(buf) | UploadState::Multipart { buf, .. }) =
                &mut self.state
            else {
                return Err(ended());
            };
            let (head, rest) = chunk.split_at(part.saturating_sub(buf.len()).min(chunk.len()));
            append_bounded(buf, head, part);
            chunk = rest;
            if buf.len() >= part {
                self.start_multipart()?;
                self.send_part(start)?;
            }
        }
        Ok(())
    }

    /// The first full buffer turns a buffered upload into a multipart one.
    fn start_multipart(&mut self) -> Result<(), StoreError> {
        if !matches!(self.state, UploadState::Buffering(_)) {
            return Ok(());
        }
        let UploadState::Buffering(buf) = std::mem::replace(&mut self.state, UploadState::Done)
        else {
            return Err(ended());
        };
        let id = self.inner.create_upload(&self.key, self.stamp)?;
        self.state = UploadState::Multipart {
            id,
            etags: Vec::new(),
            buf,
        };
        Ok(())
    }

    /// Uploads the buffer as the next part. Any failure ends the upload and
    /// hands its id to the orphan list.
    fn send_part(&mut self, start: Timestamp) -> Result<(), StoreError> {
        let UploadState::Multipart { id, etags, buf } = &mut self.state else {
            return Ok(());
        };
        let outcome = PartNumber::new(u16::try_from(etags.len().saturating_add(1)).unwrap_or(0))
            .ok_or(StoreError::Refused {
                op: StoreOp::Upload,
                status: PAYLOAD_TOO_LARGE,
            })
            .and_then(|number| {
                self.inner.within_budget(StoreOp::Upload, start)?;
                self.inner
                    .put_part(&self.key, id, number, std::mem::take(buf))
            });
        match outcome {
            Ok(etag) => {
                etags.push(etag);
                Ok(())
            }
            Err(err) => {
                self.orphan();
                Err(err)
            }
        }
    }

    /// The last part (when anything is left), then CompleteMultipartUpload and
    /// the ETag check on the finished object.
    fn finish_multipart(&mut self) -> Result<FileInfo, StoreError> {
        let start = self.inner.now();
        if matches!(&self.state, UploadState::Multipart { buf, .. } if !buf.is_empty()) {
            self.send_part(start)?;
        }
        let UploadState::Multipart { id, etags, .. } = &self.state else {
            return Err(ended());
        };
        let completed = self
            .inner
            .within_budget(StoreOp::Upload, start)
            .and_then(|()| self.inner.complete_upload(&self.key, id, etags));
        let parts = etags.len();
        let etag = match completed {
            Ok(etag) => etag,
            Err(err) => {
                self.orphan();
                return Err(err);
            }
        };
        // Completed: the id is spent, so nothing is left to orphan.
        self.state = UploadState::Done;
        if !completed_etag_fits(&etag, parts) {
            return Err(self.inner.delete_bad_object(&self.key, None));
        }
        Ok(FileInfo {
            key: self.key,
            size: self.written,
            listed: ListedStamp::Stored(StoredStamp::metadata(self.stamp)),
        })
    }
}

impl<T: Transport> Upload for S3Upload<T> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.append(chunk)
    }

    async fn commit(mut self) -> Result<FileInfo, StoreError> {
        if let UploadState::Buffering(buf) = &mut self.state {
            let body = std::mem::take(buf);
            self.state = UploadState::Done;
            return self.inner.put(&self.key, self.stamp, body);
        }
        self.finish_multipart()
    }

    async fn abort(mut self) -> Result<(), StoreError> {
        match std::mem::replace(&mut self.state, UploadState::Done) {
            UploadState::Multipart { id, .. } => {
                let aborted = self.inner.abort_upload(&self.key, &id);
                if aborted.is_err() {
                    self.inner.orphans.push(self.key, id);
                }
                aborted
            }
            UploadState::Buffering(_) | UploadState::Done => Ok(()),
        }
    }
}
