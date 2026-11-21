//! The multipart half of an upload: the three requests and what each answer
//! must prove.
//!
//! Every part carries Content-MD5 and its returned ETag must be the quoted hex
//! MD5 of the part (a mismatch or a missing ETag is `ChecksumMismatch` with the
//! part number, F7). The completed object's ETag is `<32 hex>-<parts>` on S3
//! and MinIO; it is checked for that shape and the part count only, because
//! the part digests it is made of were each verified already.
//!
//! Parts go out as batches of up to the `concurrency` setting
//! ([`Transport::send_many`]); each part is judged alone, exactly as when
//! parts went one at a time, and the ETags are kept in part order.

use lsr_core::{CorruptCause, FileKey, PartNumber, StoreError, StoreOp};
use lsr_ltx::Timestamp;

use crate::classify::classify;
use crate::request::UploadId;
use crate::response::{EtagCheck, check_etag, parse_complete, parse_create};
use crate::store::Inner;
use crate::transport::{HttpResponse, Transport};

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
            |(number, sent), response| judge_part(key, number, sent, response),
        )
        .into_iter()
        .collect()
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
