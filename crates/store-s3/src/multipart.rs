//! The multipart half of an upload: the three requests and what each answer
//! must prove.
//!
//! Every part carries Content-MD5 and its returned ETag must be the quoted hex
//! MD5 of the part (a mismatch or a missing ETag is `ChecksumMismatch` with the
//! part number, F7). The completed object's ETag is `<32 hex>-<parts>` on S3
//! and MinIO; it is checked for that shape and the part count only, because
//! the part digests it is made of were each verified already.

use lsr_core::{CorruptCause, FileKey, PartNumber, StoreError, StoreOp};
use lsr_ltx::Timestamp;

use crate::classify::classify;
use crate::request::UploadId;
use crate::response::{EtagCheck, check_etag, parse_complete, parse_create};
use crate::store::Inner;
use crate::transport::Transport;

/// Whether a completed object's ETag has the multipart shape for `parts` parts.
pub(crate) fn completed_etag_fits(etag: &str, parts: usize) -> bool {
    let text = etag.trim().trim_matches('"');
    let Some((digest, count)) = text.split_once('-') else {
        return false;
    };
    digest.len() == 32
        && digest.bytes().all(|b| b.is_ascii_hexdigit())
        && count.parse::<usize>().is_ok_and(|n| n == parts)
}

impl<T: Transport> Inner<T> {
    /// CreateMultipartUpload; the stamp rides on the request so the completed
    /// object carries it.
    pub(crate) fn create_upload(
        &self,
        key: &FileKey,
        stamp: Timestamp,
    ) -> Result<UploadId, StoreError> {
        let op = StoreOp::Upload;
        let request = self.request(op, |b, now| b.create_multipart(now, key, stamp))?;
        let response = self.exec(op, Some(key), request)?;
        parse_create(&response).map_err(|err| err.into_store(op, Some(*key)))
    }

    /// One UploadPart; returns the ETag to list at completion.
    pub(crate) fn put_part(
        &self,
        key: &FileKey,
        id: &UploadId,
        number: PartNumber,
        body: Vec<u8>,
    ) -> Result<String, StoreError> {
        let op = StoreOp::Upload;
        let request = self.request(op, |b, now| b.upload_part(now, key, id, number, body))?;
        let sent = request.md5();
        let response = self.send(op, request.into_request())?;
        classify(op, Some(key), Some(number), &response)?;
        let mismatch = || StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::ChecksumMismatch { part: Some(number) },
        };
        match check_etag(&response, sent) {
            EtagCheck::Matches | EtagCheck::NotMd5 => response
                .header("etag")
                .map(str::to_owned)
                .ok_or_else(mismatch),
            EtagCheck::Missing | EtagCheck::Mismatch => Err(mismatch()),
        }
    }

    /// CompleteMultipartUpload; returns the completed object's ETag. S3 can
    /// answer 200 with an error document, which fails the parse here.
    pub(crate) fn complete_upload(
        &self,
        key: &FileKey,
        id: &UploadId,
        etags: &[String],
    ) -> Result<String, StoreError> {
        let op = StoreOp::Upload;
        let request = self.request(op, |b, now| b.complete_multipart(now, key, id, etags))?;
        let response = self.exec(op, Some(key), request)?;
        parse_complete(&response).map_err(|err| err.into_store(op, Some(*key)))
    }
}
