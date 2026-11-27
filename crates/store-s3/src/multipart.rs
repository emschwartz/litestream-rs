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

use std::num::NonZeroU8;
use std::time::Duration;

use lsr_core::{
    Class, CorruptCause, FileKey, PartNumber, RetryPolicy, StoreError, StoreOp, WriteMode,
    classify as retry_class,
};
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

/// How a part is sent again: Go's uploader sends each request through the AWS SDK's retryer,
/// which litestream sets to 8 attempts with waits of at most one second
/// (`.upstream/litestream/s3/replica_client.go:605`, `newTransportRetryer`).
const PART_RETRY: RetryPolicy = RetryPolicy {
    attempts: NonZeroU8::MIN.saturating_add(7),
    base: Duration::from_millis(100),
    cap: Duration::from_secs(1),
};

/// How long to wait before sending a part again after its `failures`-th failure (1-based),
/// or `None` when the failure is the part's last: the attempts are used up, the error is not
/// one a repeat can mend, or the store named a wait longer than the cap. The wait has no
/// jitter: the lanes of one upload do not fail in step for long, and the store holds no
/// entropy source.
pub(crate) fn part_retry_wait(failures: u32, err: &StoreError) -> Option<Duration> {
    if failures >= u32::from(PART_RETRY.attempts.get()) {
        return None;
    }
    match retry_class(err) {
        Class::Retry => Some(PART_RETRY.bound(failures)),
        Class::RetryAfter(wait) if wait <= PART_RETRY.cap => Some(wait),
        Class::RetryAfter(_) | Class::Missing | Class::Permanent => None,
    }
}

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
        let response = self.exec(op, Some(key), |b, now| {
            b.create_multipart(now, key, created_at)
        })?;
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
        let response = self.call(op, |b, now| {
            b.complete_multipart_if(now, key, id, etags, precondition)
        })?;
        self.refuse_unsupported_or_tally(precondition, key, &response)?;
        if S3Code::from_body(response.body()) == Some(S3Code::NoSuchUpload) {
            return self.settle_spent_upload(key, sent);
        }
        classify(op, Some(key), None, &response)?;
        if let Some(err) = error_in_ok_body(op, key, &response) {
            return Err(err);
        }
        parse_complete(&response).map_err(|err| err.into_store(op, Some(*key)))
    }

    /// What a write's answer says about its precondition: a store that does not implement
    /// create-only publishes nothing and checks nothing, and the caller decides whether to
    /// replace (D79; the store never drops the condition itself); a 412 is a taken key,
    /// counted. A response is one or the other, never both.
    pub(crate) fn refuse_unsupported_or_tally(
        &self,
        precondition: Precondition,
        key: &FileKey,
        response: &HttpResponse,
    ) -> Result<(), StoreError> {
        if precondition == Precondition::KeyIsFree && is_not_implemented(response) {
            return Err(StoreError::CreateOnlyUnsupported {
                op: StoreOp::Upload,
                key: *key,
            });
        }
        if response.status() == 412 {
            self.tally_taken();
        }
        Ok(())
    }

    /// The upload id is spent: the object a HEAD finds is this upload's when
    /// it matches what was sent, and its ETag is returned; anything else is
    /// for the caller to list and read back.
    fn settle_spent_upload(&self, key: &FileKey, sent: Sent) -> Result<String, StoreError> {
        let op = StoreOp::Upload;
        let ambiguous = invalid_data(op);
        let response = self.call(op, |b, now| b.head(now, key))?;
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
