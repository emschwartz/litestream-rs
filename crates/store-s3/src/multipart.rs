//! The multipart half of an upload: the three requests and what each answer
//! must prove.
//!
//! Every part carries Content-MD5 and its returned ETag must be the quoted hex
//! MD5 of the part (a mismatch or a missing ETag is `ChecksumMismatch` with the
//! part number, F7). The completed object's ETag is `<32 hex>-<parts>` on S3,
//! MinIO and Tigris, but nothing obliges a provider to that form, and it is no
//! MD5 of the object in any case. Only an ETag of that form naming another part
//! count is evidence of a wrong object; any other form is taken as it comes,
//! because the part digests the object is made of were each verified already.
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
//! Parts go out as batches of up to the `concurrency` setting
//! ([`Transport::send_many`]); each part is judged alone, exactly as when
//! parts went one at a time, and the ETags are kept in part order.

use lsr_core::{CorruptCause, FileKey, PartNumber, Reread, StoreError, StoreOp, WriteMode};
use lsr_ltx::Timestamp;

use crate::again::Sealed;
use crate::classify::{S3Code, classify, error_in_ok_body, invalid_data, is_not_implemented};
use crate::request::{Precondition, UploadId};
use crate::response::{EtagCheck, check_etag, parse_complete, parse_create, parse_head};
use crate::store::Inner;
use crate::transport::{HttpResponse, StreamedRequest, Transport};

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

/// What one UploadPart's answer must prove: a success status, and an ETag
/// that is the quoted MD5 of the part sent.
fn judge_part(
    key: &FileKey,
    number: PartNumber,
    sent: [u8; 16],
    response: &HttpResponse,
) -> Result<String, StoreError> {
    classify(StoreOp::Upload, Some(key), Some(number), response)?;
    let mismatch = || StoreError::Corrupt {
        key: Some(*key),
        cause: CorruptCause::ChecksumMismatch { part: Some(number) },
    };
    match check_etag(response, sent) {
        EtagCheck::Matches | EtagCheck::NotMd5 => response
            .header("etag")
            .map(str::to_owned)
            .ok_or_else(mismatch),
        EtagCheck::Missing | EtagCheck::Mismatch => Err(mismatch()),
    }
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

    /// UploadPart for each body, numbered from `first`, as one batch;
    /// returns the ETags to list at completion, in part order. Every part is
    /// judged alone (status, then its ETag against its MD5); the first failure
    /// in part order is the result. A number past the part cap fails before
    /// anything is sent.
    pub(crate) fn put_parts(
        &self,
        key: &FileKey,
        id: &UploadId,
        first: usize,
        bodies: Vec<Vec<u8>>,
    ) -> Result<Vec<String>, StoreError> {
        let op = StoreOp::Upload;
        let last = first.saturating_add(bodies.len()).saturating_sub(1);
        let number = |n: usize| {
            PartNumber::new(u16::try_from(n).unwrap_or(0)).ok_or(StoreError::Refused {
                op,
                status: PAYLOAD_TOO_LARGE,
            })
        };
        number(last)?;
        let numbered = bodies
            .into_iter()
            .enumerate()
            .map(|(at, body)| (first.saturating_add(at), body));
        self.batch(
            op,
            self.concurrency,
            numbered,
            |(n, body)| {
                let number = number(n)?;
                let request =
                    self.request(op, |b, now| b.upload_part(now, key, id, number, body))?;
                let sent = request.md5();
                Ok((request.into_request(), (number, sent)))
            },
            |(number, sent), response| judge_part(key, number, sent, &response),
        )
        .into_iter()
        .collect()
    }

    /// [`Self::put_parts`] for parts the upload let go of: each request carries the length
    /// and the MD5 taken when the part was first handed over, and its body is read again
    /// through `again` while it is sent (`again.rs`). A part whose second read gave nothing
    /// fails with the source's own error, whatever its request then came to; otherwise it is
    /// judged as any part is. The first failure in part order is the result.
    pub(crate) fn put_parts_from(
        &self,
        key: &FileKey,
        id: &UploadId,
        first: usize,
        parts: &[Sealed],
        again: &dyn Reread,
    ) -> Result<Vec<String>, StoreError> {
        let op = StoreOp::Upload;
        let mut numbers = Vec::with_capacity(parts.len());
        let mut requests = Vec::with_capacity(parts.len());
        for (at, part) in parts.iter().enumerate() {
            let raw = u16::try_from(first.saturating_add(at)).unwrap_or(0);
            let number = PartNumber::new(raw).ok_or(StoreError::Refused {
                op,
                status: PAYLOAD_TOO_LARGE,
            })?;
            let sized = (part.len(), part.md5());
            requests.push(self.request(op, |b, now| {
                b.upload_part_streamed(now, key, id, number, sized)
            })?);
            numbers.push(number);
        }
        let mut bodies: Vec<_> = parts.iter().map(|part| part.body(again)).collect();
        let streamed = requests
            .into_iter()
            .zip(parts)
            .zip(bodies.iter_mut())
            .map(|((request, part), body)| {
                StreamedRequest::new(request.into_request(), part.len(), body)
            })
            .collect();
        let sent = self.send_streamed(op, self.concurrency, streamed);
        let mut etags = Vec::with_capacity(parts.len());
        for (((response, body), part), number) in
            sent.into_iter().zip(bodies).zip(parts).zip(numbers)
        {
            if let Some(unread) = body.failed() {
                return Err(unread);
            }
            etags.push(judge_part(key, number, part.md5(), &response?)?);
        }
        Ok(etags)
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
