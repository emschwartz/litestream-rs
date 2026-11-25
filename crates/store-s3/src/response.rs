//! Reading the bodies and headers of successful responses.
//!
//! [`classify`](crate::classify) decides whether a response is a success; these
//! parsers read what a success holds. A response that is 2xx but unreadable is a
//! [`ResponseError`], which converts to a retryable or corrupt [`StoreError`].

use std::collections::BTreeSet;
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
use crate::request::{UploadId, level_prefix, object_key};
use crate::transport::HttpResponse;

/// The metadata header a file's created-at time is stored in.
const CREATED_AT_HEADER: &str = "x-amz-meta-litestream-timestamp";

/// Why a 2xx response could not be used.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum ResponseError {
    /// The body is not the XML or the headers are not the fields expected.
    #[error("response is malformed")]
    Malformed,
    /// A listed key is not under the level's prefix.
    #[error("listed key is outside the requested prefix")]
    KeyOutsidePrefix,
    /// A key is listed twice, or a later page lists a file at or before one already passed:
    /// a cursor that went back, so what the pages hold cannot be trusted whole.
    #[error("listing repeats a key or goes back")]
    RepeatedKey,
    /// The created-at time header is present and does not parse.
    #[error("litestream-timelitestream-timestamp header does not parse")]
    BadCreatedAt,
}

impl ResponseError {
    /// The store error for this fault. A bad created-at time is the object's fault, the
    /// rest a response worth asking for again.
    #[must_use]
    pub fn into_store(self, op: StoreOp, key: Option<FileKey>) -> StoreError {
        match self {
            Self::BadCreatedAt => StoreError::Corrupt {
                key,
                cause: CorruptCause::BadCreatedAt,
            },
            Self::Malformed | Self::KeyOutsidePrefix | Self::RepeatedKey => StoreError::Transient {
                op,
                kind: io::ErrorKind::InvalidData,
            },
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
    /// `LastModified`: what a listing lists for a file. It is never a created-at time.
    pub last_modified: Timestamp,
}

/// A name under a level that is not an LTX key, and where the provider listed it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Unrecognized {
    /// The name, without the level's prefix.
    pub name: String,
    /// How many of the page's files the provider listed before it. A provider that lists a
    /// name ahead of the names it extends (R2) puts such a stray after those files, and the
    /// next listing, which starts after a file, finds it there.
    pub files_before: usize,
}

/// One page of a level, split into LTX files and unrecognized names.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ListPage {
    /// LTX files, ascending whatever order the provider listed them in.
    pub files: Vec<ListedFile>,
    /// Names under the level that are not LTX keys (scratch names excluded), in the order
    /// listed.
    pub unrecognized: Vec<Unrecognized>,
    /// The token for the next page when the listing is truncated.
    pub next_token: Option<String>,
}

/// Parses a ListObjectsV2 response for `level`. The keys may come in any order (R2 lists a name
/// after the longer names that extend it): the files are returned ascending, and each
/// unrecognized name says how many files the provider listed before it.
///
/// # Errors
/// [`ResponseError`] for bad XML, a truncated page without a token, a key
/// outside the level's prefix or a key listed twice.
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
        unrecognized: Vec::new(),
        next_token: parsed.next_continuation_token,
    };
    let mut seen = BTreeSet::new();
    for entry in &parsed.contents {
        let name = entry
            .key
            .strip_prefix(lead.as_str())
            .ok_or(ResponseError::KeyOutsidePrefix)?;
        if !seen.insert(name) {
            return Err(ResponseError::RepeatedKey);
        }
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
                    .and_then(|text| Timestamp::parse_rfc3339(text).ok())
                    .ok_or(ResponseError::Malformed)?,
            }),
            Err(_unrecognized) => page.unrecognized.push(Unrecognized {
                name: name.to_owned(),
                files_before: page.files.len(),
            }),
        }
    }
    // Fixed-width names never extend one another, so a provider that reorders only names that
    // extend others lists the files ascending already; sorting costs nothing there and makes
    // the page correct for one that does not.
    page.files.sort_by_key(|file| file.key);
    Ok(page)
}

/// What a HEAD tells.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Head {
    /// `Content-Length`.
    pub size: u64,
    /// The stored created-at time, `None` when the object has no created-at time metadata.
    pub created_at: Option<Timestamp>,
}

/// Parses a HEAD response.
///
/// # Errors
/// [`ResponseError::Malformed`] without a numeric `Content-Length`,
/// [`ResponseError::BadCreatedAt`] when the created-at time is present and does not parse.
pub fn parse_head(r: &HttpResponse) -> Result<Head, ResponseError> {
    let size = r
        .header("content-length")
        .and_then(|text| text.trim().parse().ok())
        .ok_or(ResponseError::Malformed)?;
    let created_at = parse_created_at_header(r)?;
    Ok(Head { size, created_at })
}

/// The stored created-at time on any response that carries the object's metadata (a
/// HEAD or a GET), `None` when the object has none.
///
/// # Errors
/// [`ResponseError::BadCreatedAt`] when the created-at time is present and does not parse.
pub(crate) fn parse_created_at_header(
    r: &HttpResponse,
) -> Result<Option<Timestamp>, ResponseError> {
    match r.header(CREATED_AT_HEADER) {
        None => Ok(None),
        Some(text) => Timestamp::parse_rfc3339(text)
            .map(Some)
            .map_err(|_bad| ResponseError::BadCreatedAt),
    }
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
    parse_delete_failure_codes(r).map(|codes| codes.len())
}

/// The code of each entry of a DeleteObjects result that failed for a reason
/// other than the key being gone already, in the order the result lists them.
/// At most one per key the request named, so the batch cap bounds it.
///
/// # Errors
/// [`ResponseError::Malformed`].
pub fn parse_delete_failure_codes(r: &HttpResponse) -> Result<Vec<S3Code>, ResponseError> {
    let text = std::str::from_utf8(r.body()).map_err(|_bytes| ResponseError::Malformed)?;
    let parsed = DeleteObjectsResponse::parse(text).map_err(|_xml| ResponseError::Malformed)?;
    Ok(parsed
        .errors
        .iter()
        .map(|entry| S3Code::from_name(&entry.code))
        .filter(|code| *code != S3Code::NoSuchKey)
        .collect())
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
    fn list_splits_files_unrecognized_and_scratch() {
        let a = name(1);
        let scratch = format!("{}.tmp", name(2));
        let unrecognized = "p/0000/README".to_owned();
        let c = name(3);
        let r = listing(
            &[&a, &scratch, &c, &unrecognized.replace("README", "zzz")],
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
                .map(|f| f.last_modified.to_string())
                .as_deref(),
            Some("2026-11-20T13:49:45.000Z")
        );
        assert_eq!(
            page.unrecognized,
            vec![Unrecognized {
                name: "zzz".to_owned(),
                files_before: 2
            }]
        );
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
        let a = name(1);
        assert_eq!(
            parse_list(&listing(&["q/0000/x"], ""), &prefix(), Level::ZERO),
            Err(ResponseError::KeyOutsidePrefix)
        );
        assert_eq!(
            parse_list(&listing(&[&a, &a], ""), &prefix(), Level::ZERO),
            Err(ResponseError::RepeatedKey)
        );
        assert_eq!(
            parse_list(&ok("not xml"), &prefix(), Level::ZERO),
            Err(ResponseError::Malformed)
        );
    }

    #[test]
    fn head_reads_size_and_created_at() {
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
            h.created_at.map(|t| t.to_string()).as_deref(),
            Some("2025-01-02T03:04:05.123Z")
        );
        assert_eq!(
            parse_head(&with_headers(&[("content-length", "0")])),
            Ok(Head {
                size: 0,
                created_at: None
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
            Err(ResponseError::BadCreatedAt)
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
            ResponseError::BadCreatedAt.into_store(StoreOp::List, k),
            StoreError::Corrupt {
                key: None,
                cause: CorruptCause::BadCreatedAt
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

/// The text of each `<tag>` element in `xml`, unescaped; enough for S3's flat listings.
#[must_use]
pub(crate) fn xml_text(xml: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    xml.split(open.as_str())
        .skip(1)
        .filter_map(|rest| rest.split_once(close.as_str()).map(|(text, _)| text))
        .map(|text| {
            text.replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&apos;", "'")
                .replace("&amp;", "&")
        })
        .collect()
}

/// One page of ListMultipartUploads: the uploads this store began (a full key that is
/// `<prefix>/<level>/<file name>` for a level and a file name it parses), and where the
/// next page starts when the listing is truncated.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct UploadPage {
    /// The uploads whose key is one of this store's object keys.
    pub ours: Vec<(FileKey, UploadId)>,
    /// `key-marker` and `upload-id-marker` for the next page.
    pub next: Option<(String, String)>,
}

/// Parses a ListMultipartUploads answer; a key that is not one of this store's object keys
/// (another prefix, a stray name) is not ours and is left out.
///
/// # Errors
/// [`ResponseError::Malformed`] for a body that is not the listing.
pub(crate) fn parse_uploads(
    r: &HttpResponse,
    prefix: &Prefix,
) -> Result<UploadPage, ResponseError> {
    let xml = std::str::from_utf8(r.body()).map_err(|_bytes| ResponseError::Malformed)?;
    if !xml.contains("<ListMultipartUploadsResult") {
        return Err(ResponseError::Malformed);
    }
    let mut page = UploadPage::default();
    for upload in xml_text(xml, "Upload") {
        let key = xml_text(&upload, "Key")
            .pop()
            .ok_or(ResponseError::Malformed)?;
        let id = xml_text(&upload, "UploadId")
            .pop()
            .ok_or(ResponseError::Malformed)?;
        if let Some(file) = own_key(prefix, &key) {
            page.ours.push((file, UploadId::new(id)));
        }
    }
    if xml_text(xml, "IsTruncated").first().map(String::as_str) == Some("true") {
        let next = |tag: &str| xml_text(xml, tag).pop().ok_or(ResponseError::Malformed);
        page.next = Some((next("NextKeyMarker")?, next("NextUploadIdMarker")?));
    }
    Ok(page)
}

/// The file key a full object key names, when it is exactly what [`object_key`] gives.
fn own_key(prefix: &Prefix, key: &str) -> Option<FileKey> {
    let rest = key.strip_prefix(prefix.as_str())?.strip_prefix('/')?;
    let (level, name) = rest.split_once('/')?;
    let level = Level::ALL
        .into_iter()
        .find(|l| format!("{:04x}", l.get()) == level)?;
    let file = FileKey::new(level, parse_file_name(name).ok()?);
    (object_key(prefix, &file) == key).then_some(file)
}
