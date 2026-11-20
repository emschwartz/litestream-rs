//! Reading the bodies and headers of successful responses.
//!
//! [`classify`](crate::classify) decides whether a response is a success; these
//! parsers read what a success holds. A response that is 2xx but unreadable is a
//! [`ResponseError`], which converts to a retryable or corrupt [`StoreError`].

use std::io;

use lsr_core::{
    CorruptCause, FileKey, Level, StoreError, StoreOp, is_scratch_name, parse_file_name,
};
use lsr_ltx::Timestamp;
use md5::{Digest as _, Md5};
use rusty_s3::actions::{
    CompleteMultipartUploadResponse, CreateMultipartUpload, DeleteObjectsResponse, ListObjectsV2,
};
use thiserror::Error;

use crate::classify::S3Code;
use crate::config::Prefix;
use crate::request::{UploadId, level_prefix};
use crate::transport::HttpResponse;

/// The metadata header a file's stamp is stored in.
const STAMP_HEADER: &str = "x-amz-meta-litestream-timestamp";

/// Why a 2xx response could not be used.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum ResponseError {
    /// The body is not the XML or the headers are not the fields expected.
    #[error("response is malformed")]
    Malformed,
    /// A listed key is not under the level's prefix.
    #[error("listed key is outside the requested prefix")]
    KeyOutsidePrefix,
    /// Listed keys are not strictly ascending.
    #[error("listing is out of order")]
    NotAscending,
    /// The stamp header is present and does not parse.
    #[error("stamp header does not parse")]
    BadStamp,
}

impl ResponseError {
    /// The store error for this fault. A bad stamp is the object's fault, the
    /// rest a response worth asking for again.
    #[must_use]
    pub fn into_store(self, op: StoreOp, key: Option<FileKey>) -> StoreError {
        match self {
            Self::BadStamp => StoreError::Corrupt {
                key,
                cause: CorruptCause::BadStamp,
            },
            Self::Malformed | Self::KeyOutsidePrefix | Self::NotAscending => {
                StoreError::Transient {
                    op,
                    kind: io::ErrorKind::InvalidData,
                }
            }
        }
    }
}

/// One LTX file in a listing.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ListedFile {
    /// The file's key.
    pub key: FileKey,
    /// Its size from the listing.
    pub size: u64,
    /// `LastModified`, for files stored without a stamp (Go does the same).
    pub last_modified: Option<Timestamp>,
}

/// One page of a level, split into LTX files and foreign names.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ListPage {
    /// LTX files, ascending.
    pub files: Vec<ListedFile>,
    /// Names under the level that are not LTX keys (scratch names excluded).
    pub foreign: Vec<String>,
    /// The token for the next page when the listing is truncated.
    pub next_token: Option<String>,
}

/// Parses a ListObjectsV2 response for `level`.
///
/// # Errors
/// [`ResponseError`] for bad XML, a truncated page without a token, a key
/// outside the level's prefix or keys not in ascending order.
pub fn parse_list(
    r: &HttpResponse,
    prefix: &Prefix,
    level: Level,
) -> Result<ListPage, ResponseError> {
    let text = std::str::from_utf8(r.body()).map_err(|_bytes| ResponseError::Malformed)?;
    let parsed = ListObjectsV2::parse_response(text).map_err(|_xml| ResponseError::Malformed)?;
    let lead = level_prefix(prefix, level);
    let mut page = ListPage {
        files: Vec::new(),
        foreign: Vec::new(),
        next_token: parsed.next_continuation_token,
    };
    let mut previous: Option<&str> = None;
    for entry in &parsed.contents {
        let name = entry
            .key
            .strip_prefix(lead.as_str())
            .ok_or(ResponseError::KeyOutsidePrefix)?;
        if previous.is_some_and(|last| last >= entry.key.as_str()) {
            return Err(ResponseError::NotAscending);
        }
        previous = Some(entry.key.as_str());
        if is_scratch_name(name) {
            continue;
        }
        match parse_file_name(name) {
            Ok(range) => page.files.push(ListedFile {
                key: FileKey::new(level, range),
                size: entry.size.ok_or(ResponseError::Malformed)?,
                last_modified: entry
                    .last_modified
                    .as_deref()
                    .and_then(|text| Timestamp::parse_rfc3339(text).ok()),
            }),
            Err(_foreign) => page.foreign.push(name.to_owned()),
        }
    }
    Ok(page)
}

/// What a HEAD tells.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Head {
    /// `Content-Length`.
    pub size: u64,
    /// The stored stamp, `None` when the object has no stamp metadata.
    pub stamp: Option<Timestamp>,
}

/// Parses a HEAD response.
///
/// # Errors
/// [`ResponseError::Malformed`] without a numeric `Content-Length`,
/// [`ResponseError::BadStamp`] when the stamp is present and does not parse.
pub fn parse_head(r: &HttpResponse) -> Result<Head, ResponseError> {
    let size = r
        .header("content-length")
        .and_then(|text| text.trim().parse().ok())
        .ok_or(ResponseError::Malformed)?;
    let stamp = match r.header(STAMP_HEADER) {
        None => None,
        Some(text) => Some(Timestamp::parse_rfc3339(text).map_err(|_bad| ResponseError::BadStamp)?),
    };
    Ok(Head { size, stamp })
}

/// Parses a CreateMultipartUpload response.
///
/// # Errors
/// [`ResponseError::Malformed`].
pub fn parse_create(r: &HttpResponse) -> Result<UploadId, ResponseError> {
    let text = std::str::from_utf8(r.body()).map_err(|_bytes| ResponseError::Malformed)?;
    let parsed =
        CreateMultipartUpload::parse_response(text).map_err(|_xml| ResponseError::Malformed)?;
    Ok(UploadId::new(parsed.upload_id().to_owned()))
}

/// Parses a CompleteMultipartUpload response and returns the object's ETag.
/// S3 can answer 200 with an error document, which fails here.
///
/// # Errors
/// [`ResponseError::Malformed`].
pub fn parse_complete(r: &HttpResponse) -> Result<String, ResponseError> {
    let text = std::str::from_utf8(r.body()).map_err(|_bytes| ResponseError::Malformed)?;
    let parsed =
        CompleteMultipartUploadResponse::parse(text).map_err(|_xml| ResponseError::Malformed)?;
    Ok(parsed.etag().to_owned())
}

/// Counts the entries of a DeleteObjects result that failed for a reason other
/// than the key being gone already.
///
/// # Errors
/// [`ResponseError::Malformed`].
pub fn parse_delete_failures(r: &HttpResponse) -> Result<usize, ResponseError> {
    let text = std::str::from_utf8(r.body()).map_err(|_bytes| ResponseError::Malformed)?;
    let parsed = DeleteObjectsResponse::parse(text).map_err(|_xml| ResponseError::Malformed)?;
    Ok(parsed
        .errors
        .iter()
        .filter(|entry| S3Code::from_name(&entry.code) != S3Code::NoSuchKey)
        .count())
}

/// How a response's ETag compares with the MD5 that was sent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EtagCheck {
    /// The ETag is the quoted hex MD5 of the body.
    Matches,
    /// The ETag has the shape of an MD5 and is a different one.
    Mismatch,
    /// The ETag is not an MD5 (SSE-KMS, multipart form): nothing to compare;
    /// the Content-MD5 the server checked still stands.
    NotMd5,
    /// No ETag came back.
    Missing,
}

/// Compares the response's ETag with `sent`.
#[must_use]
pub fn check_etag(r: &HttpResponse, sent: [u8; 16]) -> EtagCheck {
    let Some(raw) = r.header("etag") else {
        return EtagCheck::Missing;
    };
    let text = raw.trim().trim_matches('"');
    if text.len() != 32 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return EtagCheck::NotMd5;
    }
    if text.eq_ignore_ascii_case(&hex(&sent)) {
        EtagCheck::Matches
    } else {
        EtagCheck::Mismatch
    }
}

/// Lowercase hex, the form of a part's ETag.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|byte| [byte >> 4, byte & 0x0f])
        .filter_map(|nibble| char::from_digit(u32::from(nibble), 16))
        .collect()
}

/// The quoted hex MD5 of `body`, as a part's ETag reads on S3.
#[must_use]
pub fn quoted_md5(body: &[u8]) -> String {
    format!("\"{}\"", hex(&Md5::digest(body)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsr_ltx::{Txid, TxidRange};

    const NS: &str = "xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"";

    fn ok(body: &str) -> HttpResponse {
        HttpResponse::new(200, vec![], body.as_bytes().to_vec(), true)
    }

    fn with_headers(headers: &[(&str, &str)]) -> HttpResponse {
        let headers = headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        HttpResponse::new(200, headers, vec![], true)
    }

    fn listing(keys: &[&str], tail: &str) -> HttpResponse {
        let contents: String = keys
            .iter()
            .map(|k| format!("<Contents><Key>{k}</Key><LastModified>2026-11-20T13:49:45.000Z</LastModified><Size>7</Size></Contents>"))
            .collect();
        ok(&format!(
            "<ListBucketResult {NS}><Name>b</Name>{contents}{tail}</ListBucketResult>"
        ))
    }

    fn prefix() -> Prefix {
        Prefix::new("p").unwrap()
    }

    fn name(n: u64) -> String {
        format!("p/0000/{n:016x}-{n:016x}.ltx")
    }

    #[test]
    fn list_splits_files_foreign_and_scratch() {
        let a = name(1);
        let scratch = format!("{}.tmp", name(2));
        let foreign = "p/0000/README".to_owned();
        let c = name(3);
        let r = listing(
            &[&a, &scratch, &c, &foreign.replace("README", "zzz")],
            "<IsTruncated>false</IsTruncated>",
        );
        let page = parse_list(&r, &prefix(), Level::ZERO).unwrap();
        let id = |n| {
            let t = Txid::new(n).unwrap();
            FileKey::new(Level::ZERO, TxidRange::new(t, t).unwrap())
        };
        assert_eq!(
            page.files.iter().map(|f| f.key).collect::<Vec<_>>(),
            vec![id(1), id(3)]
        );
        assert_eq!(page.files.first().map(|f| f.size), Some(7));
        assert_eq!(
            page.files
                .first()
                .and_then(|f| f.last_modified)
                .map(|t| t.to_string())
                .as_deref(),
            Some("2026-11-20T13:49:45.000Z")
        );
        assert_eq!(page.foreign, vec!["zzz".to_owned()]);
        assert_eq!(page.next_token, None);
    }

    #[test]
    fn list_follows_a_token_and_refuses_a_truncated_page_without_one() {
        let a = name(1);
        let page = parse_list(
            &listing(
                &[&a],
                "<IsTruncated>true</IsTruncated><NextContinuationToken>tok</NextContinuationToken>",
            ),
            &prefix(),
            Level::ZERO,
        )
        .unwrap();
        assert_eq!(page.next_token.as_deref(), Some("tok"));
        assert_eq!(
            parse_list(
                &listing(&[&a], "<IsTruncated>true</IsTruncated>"),
                &prefix(),
                Level::ZERO
            ),
            Err(ResponseError::Malformed)
        );
    }

    #[test]
    fn list_refuses_bad_pages() {
        let (a, b) = (name(1), name(2));
        assert_eq!(
            parse_list(&listing(&["q/0000/x"], ""), &prefix(), Level::ZERO),
            Err(ResponseError::KeyOutsidePrefix)
        );
        assert_eq!(
            parse_list(&listing(&[&b, &a], ""), &prefix(), Level::ZERO),
            Err(ResponseError::NotAscending)
        );
        assert_eq!(
            parse_list(&listing(&[&a, &a], ""), &prefix(), Level::ZERO),
            Err(ResponseError::NotAscending)
        );
        assert_eq!(
            parse_list(&ok("not xml"), &prefix(), Level::ZERO),
            Err(ResponseError::Malformed)
        );
    }

    #[test]
    fn head_reads_size_and_stamp() {
        let h = parse_head(&with_headers(&[
            ("Content-Length", "42"),
            (
                "x-amz-meta-litestream-timestamp",
                "2025-01-02T03:04:05.123456789Z",
            ),
        ]))
        .unwrap();
        assert_eq!(h.size, 42);
        assert_eq!(
            h.stamp.map(|t| t.to_string()).as_deref(),
            Some("2025-01-02T03:04:05.123Z")
        );
        assert_eq!(
            parse_head(&with_headers(&[("content-length", "0")])),
            Ok(Head {
                size: 0,
                stamp: None
            })
        );
        assert_eq!(
            parse_head(&with_headers(&[])),
            Err(ResponseError::Malformed)
        );
        assert_eq!(
            parse_head(&with_headers(&[
                ("content-length", "1"),
                ("x-amz-meta-litestream-timestamp", "yesterday")
            ])),
            Err(ResponseError::BadStamp)
        );
    }

    #[test]
    fn create_complete_and_delete_bodies() {
        let created = ok(&format!(
            "<InitiateMultipartUploadResult {NS}><Bucket>b</Bucket><Key>k</Key><UploadId>up-1</UploadId></InitiateMultipartUploadResult>"
        ));
        assert_eq!(parse_create(&created).unwrap().as_str(), "up-1");
        let done = ok(&format!(
            "<CompleteMultipartUploadResult {NS}><Location>l</Location><Bucket>b</Bucket><Key>k</Key><ETag>&quot;abc-2&quot;</ETag></CompleteMultipartUploadResult>"
        ));
        assert_eq!(parse_complete(&done).unwrap(), "\"abc-2\"");
        let in_band = ok("<Error><Code>InternalError</Code><Message>m</Message></Error>");
        assert_eq!(parse_complete(&in_band), Err(ResponseError::Malformed));
        assert_eq!(parse_create(&in_band), Err(ResponseError::Malformed));
        let empty = ok(&format!("<DeleteResult {NS}></DeleteResult>"));
        assert_eq!(parse_delete_failures(&empty), Ok(0));
        let mixed = ok(&format!(
            "<DeleteResult {NS}><Error><Key>a</Key><Code>NoSuchKey</Code><Message>m</Message></Error><Error><Key>b</Key><Code>AccessDenied</Code><Message>m</Message></Error></DeleteResult>"
        ));
        assert_eq!(parse_delete_failures(&mixed), Ok(1));
    }

    #[test]
    fn etag_check_cases() {
        let sent: [u8; 16] = Md5::digest(b"hello").into();
        let quoted = quoted_md5(b"hello");
        assert_eq!(quoted, "\"5d41402abc4b2a76b9719d911017c592\"");
        assert_eq!(
            check_etag(&with_headers(&[("ETag", &quoted)]), sent),
            EtagCheck::Matches
        );
        assert_eq!(
            check_etag(
                &with_headers(&[("etag", "5D41402ABC4B2A76B9719D911017C592")]),
                sent
            ),
            EtagCheck::Matches
        );
        assert_eq!(
            check_etag(&with_headers(&[("etag", &quoted_md5(b"other"))]), sent),
            EtagCheck::Mismatch
        );
        assert_eq!(
            check_etag(&with_headers(&[("etag", "\"abc-2\"")]), sent),
            EtagCheck::NotMd5
        );
        assert_eq!(check_etag(&with_headers(&[]), sent), EtagCheck::Missing);
    }

    #[test]
    fn response_faults_map_to_store_errors() {
        let k = None;
        assert_eq!(
            ResponseError::BadStamp.into_store(StoreOp::List, k),
            StoreError::Corrupt {
                key: None,
                cause: CorruptCause::BadStamp
            }
        );
        assert_eq!(
            ResponseError::Malformed.into_store(StoreOp::List, k),
            StoreError::Transient {
                op: StoreOp::List,
                kind: io::ErrorKind::InvalidData
            }
        );
    }
}
