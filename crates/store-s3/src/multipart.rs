//! The multipart half of an upload: the three requests and what each answer
//! must prove.
//!
//! Every part signs its SHA-256, which the store checks against the body it received
//! (`XAmzContentSHA256Mismatch` otherwise), and must answer with an ETag, which the
//! complete names it by (a missing one is `ChecksumMismatch` with the part number). What
//! the ETag says is not compared with anything: as in Go, the signed hash is the check. The
//! completed object's ETag is `<32 hex>-<parts>` on S3, MinIO and Tigris, but nothing obliges
//! a provider to that form. Only an ETag of that form naming another part count is evidence
//! of a wrong object; any other form is taken as it comes.
//!
//! A create-only upload sends `If-None-Match: *` on the complete, where S3
//! checks it. The answers that are not plain:
//! - 412 `PreconditionFailed`, as a status or inside a 200's error document:
//!   the key is taken, [`StoreError::AlreadyExists`]; the upload stays open
//!   and goes on the orphan list.
//! - `NotImplemented` (501): the provider has no conditional writes.
//!   [`StoreError::CreateOnlyUnsupported`]: nothing is published, the upload
//!   goes on the orphan list, and the caller decides whether to write the key
//!   as a replace (`docs/decisions.md` D79). The store never drops the
//!   condition itself.
//! - `NoSuchUpload`: the id is spent, which is what a complete retried after
//!   it succeeded answers (a retrying proxy or client). A HEAD settles it: the
//!   object with the size written, the created-at time sent and an ETag that
//!   does not name another part count is this upload. Anything else is
//!   `Transient`, so the caller lists and reads back rather than trusting
//!   either answer.
//!
//! Parts are sent one request each on the store's lanes (`upload.rs`, `lanes.rs`); each is
//! judged alone when its upload collects its answer, and its ETag is kept by its number.

use lsr_core::{CorruptCause, FileKey, PartNumber, StoreError, StoreOp, WriteMode};
use lsr_ltx::Timestamp;

use crate::classify::{S3Code, classify, error_in_ok_body, invalid_data, is_not_implemented};
use crate::request::{Precondition, UploadId};
use crate::response::{parse_complete, parse_create, parse_head};
use crate::store::Inner;
use crate::transport::{HttpResponse, Transport};

/// Whether a completed object's ETag can belong to an upload of `parts`
/// parts: `false` only for the multipart form `<32 hex>-<n>` with another `n`.
pub(crate) fn completed_etag_fits(etag: &str, parts: usize) -> bool {
    let text = etag.trim().trim_matches('"');
    let Some((digest, count)) = text.split_once('-') else {
        return true;
    };
    let multipart_form = digest.len() == 32
        && digest.bytes().all(|b| b.is_ascii_hexdigit())
        && !count.is_empty()
        && count.bytes().all(|b| b.is_ascii_digit());
    !multipart_form || count.parse::<usize>().is_ok_and(|n| n == parts)
}

/// What a completed upload is known by, to recognise it again on a HEAD.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Sent {
    /// The bytes written.
    pub(crate) written: u64,
    /// The created-at time sent on the create.
    pub(crate) created_at: Timestamp,
    /// The part count.
    pub(crate) parts: usize,
}

/// The HTTP status a store-side "too large" is reported as: an upload that
/// would need more than 10000 parts.
pub(crate) const PAYLOAD_TOO_LARGE: u16 = 413;

/// What one UploadPart's answer must prove: a success status, and an ETag to name the part
/// by in the complete.
pub(crate) fn judge_part(
    key: &FileKey,
    number: PartNumber,
    response: &HttpResponse,
) -> Result<String, StoreError> {
    classify(StoreOp::Upload, Some(key), Some(number), response)?;
    response
        .header("etag")
        .map(str::to_owned)
        .ok_or(StoreError::Corrupt {
            key: Some(*key),
            cause: CorruptCause::ChecksumMismatch { part: Some(number) },
        })
}

impl<T: Transport> Inner<T> {
    /// CreateMultipartUpload; the created-at time rides on the request so the completed
    /// object carries it.
    pub(crate) fn create_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<UploadId, StoreError> {
        let op = StoreOp::Upload;
        let request = self.request(op, |b, now| b.create_multipart(now, key, created_at))?;
        let response = self.exec(op, Some(key), request)?;
        parse_create(&response).map_err(|err| err.into_store(op, Some(*key)))
    }

    /// CompleteMultipartUpload; returns the completed object's ETag. See the
    /// module docs for the answers that are not plain.
    pub(crate) fn complete_upload(
        &self,
        key: &FileKey,
        id: &UploadId,
        etags: &[String],
        mode: WriteMode,
        sent: Sent,
    ) -> Result<String, StoreError> {
        let op = StoreOp::Upload;
        let precondition = self.precondition(mode);
        let response = self.send_complete(key, id, etags, precondition)?;
        if precondition == Precondition::KeyIsFree && is_not_implemented(&response) {
            return Err(StoreError::CreateOnlyUnsupported { op, key: *key });
        }
        if S3Code::from_body(response.body()) == Some(S3Code::NoSuchUpload) {
            return self.settle_spent_upload(key, sent);
        }
        classify(op, Some(key), None, &response)?;
        if let Some(err) = error_in_ok_body(op, key, &response) {
            return Err(err);
        }
        parse_complete(&response).map_err(|err| err.into_store(op, Some(*key)))
    }

    fn send_complete(
        &self,
        key: &FileKey,
        id: &UploadId,
        etags: &[String],
        precondition: Precondition,
    ) -> Result<crate::transport::HttpResponse, StoreError> {
        let op = StoreOp::Upload;
        let request = self.request(op, |b, now| {
            b.complete_multipart_if(now, key, id, etags, precondition)
        })?;
        let response = self.send(op, request)?;
        if response.status() == 412 {
            self.tally_taken();
        }
        Ok(response)
    }

    /// The upload id is spent: the object a HEAD finds is this upload's when
    /// it matches what was sent, and its ETag is returned; anything else is
    /// for the caller to list and read back.
    fn settle_spent_upload(&self, key: &FileKey, sent: Sent) -> Result<String, StoreError> {
        let op = StoreOp::Upload;
        let ambiguous = invalid_data(op);
        let request = self.request(op, |b, now| b.head(now, key))?;
        let response = self.send(op, request)?;
        if !(200..300).contains(&response.status()) {
            return Err(ambiguous);
        }
        let Ok(head) = parse_head(&response) else {
            return Err(ambiguous);
        };
        let etag = response.header("etag").map(str::to_owned);
        match etag {
            Some(etag)
                if head.size == sent.written
                    && head.created_at == Some(sent.created_at)
                    && completed_etag_fits(&etag, sent.parts) =>
            {
                Ok(etag)
            }
            Some(_) | None => Err(ambiguous),
        }
    }
}
