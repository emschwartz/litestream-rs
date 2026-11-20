//! `S3Upload`: the single-PUT upload.
//!
//! Writes accumulate in memory, bounded by the configured part size; `commit`
//! sends one PUT. Until multipart upload exists, an object that would reach
//! the part size is refused with a typed error rather than truncated or sent
//! in a form the store cannot yet finish (ruling F6).
//!
//! A PUT is judged by the status, and then by the `ETag`: for a single PUT
//! without server-side encryption the ETag is the object's MD5, so a
//! different one means the bytes stored are not the bytes sent. The object is
//! already visible then, so it is deleted (best effort) before the error
//! returns, as the file store does for a size mismatch; the uploader writes the
//! same key again without listing.

use std::io;
use std::sync::Arc;

use lsr_core::{CorruptCause, FileInfo, FileKey, StoreError, StoreOp, Upload};
use lsr_ltx::Timestamp;

use crate::response::{EtagCheck, check_etag};
use crate::store::Inner;
use crate::transport::Transport;

/// The HTTP status a store-side "too large" is reported as.
const PAYLOAD_TOO_LARGE: u16 = 413;

/// One object being written. Dropping it does no I/O: nothing was sent.
pub struct S3Upload<T> {
    inner: Arc<Inner<T>>,
    key: FileKey,
    stamp: Timestamp,
    buffer: Vec<u8>,
}

impl<T> std::fmt::Debug for S3Upload<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Upload")
            .field("key", &self.key)
            .field("buffered", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

impl<T> S3Upload<T> {
    pub(crate) fn new(inner: Arc<Inner<T>>, key: FileKey, stamp: Timestamp) -> Self {
        Self {
            inner,
            key,
            stamp,
            buffer: Vec::new(),
        }
    }
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
                created_at: stamp,
            }),
            EtagCheck::Missing => Err(StoreError::Transient {
                op,
                kind: io::ErrorKind::InvalidData,
            }),
            EtagCheck::Mismatch => {
                // Whether the delete lands changes nothing the caller can do now.
                match self.delete(&[*key]) {
                    Ok(()) | Err(_) => {}
                }
                Err(StoreError::Corrupt {
                    key: Some(*key),
                    cause: CorruptCause::ChecksumMismatch { part: None },
                })
            }
        }
    }
}

impl<T: Transport> Upload for S3Upload<T> {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        let total =
            u64::try_from(self.buffer.len().saturating_add(chunk.len())).unwrap_or(u64::MAX);
        if total >= self.inner.part_size.get() {
            return Err(StoreError::Refused {
                op: StoreOp::Upload,
                status: PAYLOAD_TOO_LARGE,
            });
        }
        self.buffer.extend_from_slice(chunk);
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        self.inner.put(&self.key, self.stamp, self.buffer)
    }

    async fn abort(self) -> Result<(), StoreError> {
        Ok(())
    }
}
