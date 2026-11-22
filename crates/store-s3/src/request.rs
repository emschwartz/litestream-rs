//! Requests as data: what to send, already signed, with no I/O.
//!
//! Every request is presigned by rusty-s3 (query-string signature, payload
//! `UNSIGNED-PAYLOAD`; Go signs the payload, rc.go:132-139, so a body altered
//! in flight is caught only by the Content-MD5 each write carries) with a
//! 300 s expiry from the `now` the caller read off its clock. Every header
//! sent is one inserted through the signer, so every header is signed; the
//! tests check that for every builder. No `x-amz-checksum-*` header and no
//! aws-chunked encoding is ever sent (Go removes the trailer: rc.go:1039-1041).
//!
//! A write (a PUT or one part of a multipart upload) is a [`WriteRequest`]: the
//! request plus the MD5 its `Content-MD5` header promises. The store's `ETag`
//! is checked against that digest, and a write request cannot exist without
//! one, so "no digest was sent" cannot pass as "the `ETag` is not an MD5".
//!
//! Nothing here prints a URL, a header value or a body: [`HttpRequest`]'s
//! `Debug` is the method and the path, because the query holds the signature
//! and, for temporary credentials, the session token.

use std::fmt;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use lsr_core::{FileKey, Level, PageLimit, PartNumber, ReadLen};
use lsr_ltx::{Timestamp, TxidRange};
use md5::{Digest as _, Md5};
use rusty_s3::actions::ObjectIdentifier;
use rusty_s3::{Bucket, Credentials, S3Action};
use thiserror::Error;
use url::Url;

use crate::config::{ConfigError, Prefix, Provider, S3Config};
use crate::credentials::S3Credentials;

#[cfg(feature = "testing")]
mod raw;
#[cfg(feature = "testing")]
mod tamper;

#[cfg(feature = "testing")]
pub use raw::{OpenUpload, abandon_upload, abort_upload, open_uploads};

/// How long a presigned URL stays valid.
const EXPIRES_IN: Duration = Duration::from_secs(300);

/// Go's `MaxKeys`: the most one DeleteObjects may name.
pub(crate) const MAX_DELETE_KEYS: usize = 1000;

// S3's DeleteObjects cap, and room for a whole delete job (core keeps one under
// `MAX_DELETE`) in one request.
const _: () = assert!(MAX_DELETE_KEYS <= 1000);
const _: () = assert!(MAX_DELETE_KEYS >= lsr_core::MAX_DELETE);

/// The object metadata key Go stores a file's timestamp under.
const CREATED_AT_HEADER: &str = "x-amz-meta-litestream-timestamp";

const USER_AGENT: &str = concat!("litestream-rs/", env!("CARGO_PKG_VERSION"));

/// An HTTP method a request uses.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    /// `GET`.
    Get,
    /// `HEAD`.
    Head,
    /// `PUT`.
    Put,
    /// `POST`.
    Post,
    /// `DELETE`.
    Delete,
}

/// A request ready to send: signed URL, the exact headers to send, the body.
#[derive(Clone, PartialEq, Eq)]
pub struct HttpRequest {
    method: Method,
    url: Url,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl HttpRequest {
    /// A request to `url` with these headers and no body, for transport tests.
    #[cfg(test)]
    pub(crate) fn for_test(method: Method, url: Url, headers: Vec<(&'static str, String)>) -> Self {
        Self {
            method,
            url,
            headers,
            body: Vec::new(),
        }
    }

    /// The method.
    #[must_use]
    pub fn method(&self) -> Method {
        self.method
    }

    /// The presigned URL. Holds the signature: never log it.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The headers to send, names lowercase, exactly these and no others.
    #[must_use]
    pub fn headers(&self) -> &[(&'static str, String)] {
        &self.headers
    }

    /// One header's value.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(have, _)| *have == name)
            .map(|(_, value)| value.as_str())
    }

    /// The body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "HttpRequest {{ method: {:?}, path: {:?} }}",
            self.method,
            self.url.path()
        )
    }
}

/// A write: the request and the MD5 of its body that its `Content-MD5` header
/// carries. Only the builders of writes make one, and a plain request has no
/// digest to offer in its place:
///
/// ```compile_fail
/// # use lsr_store_s3::HttpRequest;
/// fn sent(request: &HttpRequest) -> Option<[u8; 16]> {
///     request.content_md5()
/// }
/// ```
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct WriteRequest {
    request: HttpRequest,
    md5: [u8; 16],
}

impl WriteRequest {
    /// The MD5 of the body the request promises. Read it before the request
    /// is sent: it is what the store's `ETag` is checked against.
    #[must_use]
    pub const fn md5(&self) -> [u8; 16] {
        self.md5
    }

    /// The request, to send.
    #[must_use]
    pub fn into_request(self) -> HttpRequest {
        self.request
    }
}

/// A write is a request with a digest: everything a request shows, it shows.
impl std::ops::Deref for WriteRequest {
    type Target = HttpRequest;

    fn deref(&self) -> &HttpRequest {
        &self.request
    }
}

impl From<WriteRequest> for HttpRequest {
    fn from(write: WriteRequest) -> Self {
        write.request
    }
}

/// The id a store gave a multipart upload.
#[derive(Clone, PartialEq, Eq)]
pub struct UploadId(String);

impl UploadId {
    /// Wraps an id read from a create response.
    #[must_use]
    pub fn new(id: String) -> Self {
        Self(id)
    }

    /// The id as sent in the query of later requests.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UploadId(..)")
    }
}

/// Why a request could not be built. Both are the caller's fault, so neither
/// is retryable.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum RequestError {
    /// The clock reads an instant a signature cannot carry.
    #[error("clock reads an instant outside the signable range")]
    ClockOutOfRange,
    /// A batch has no entries or more than the service takes.
    #[error("batch of {len} is outside 1 to {max}")]
    BadBatch {
        /// How many were given.
        len: usize,
        /// The most the request takes.
        max: usize,
    },
}

/// The key of an object: `<prefix>/<level as 4 hex digits>/<file name>`, Go's
/// `c.Path + "/" + fmt.Sprintf("%04x/%s", level, filename)` (rc.go:702).
#[must_use]
pub fn object_key(prefix: &Prefix, key: &FileKey) -> String {
    format!("{}{}", level_prefix(prefix, key.level()), key.file_name())
}

/// What every key of a level starts with, trailing slash included (rc.go:1575).
#[must_use]
pub fn level_prefix(prefix: &Prefix, level: Level) -> String {
    format!("{}/{:04x}/", prefix.as_str(), level.get())
}

/// The key a listing seeks past to start after `cursor`: the cursor's own full key.
#[must_use]
pub(crate) fn start_after(prefix: &Prefix, level: Level, cursor: TxidRange) -> String {
    format!("{}{}.ltx", level_prefix(prefix, level), cursor)
}

/// `Range: bytes=<offset>-<end>`. The end saturates at `u64::MAX`, which is
/// valid HTTP; Go computes `off+size-1` unchecked (rc.go:693-707).
#[must_use]
pub(crate) fn range_header(offset: u64, len: ReadLen) -> String {
    let end = offset.saturating_add(len.get().saturating_sub(1));
    format!("bytes={offset}-{end}")
}

fn md5_of(body: &[u8]) -> [u8; 16] {
    Md5::digest(body).into()
}

type Headers = Vec<(&'static str, String)>;

impl Method {
    const fn to_signer(self) -> rusty_s3::Method {
        match self {
            Self::Get => rusty_s3::Method::Get,
            Self::Head => rusty_s3::Method::Head,
            Self::Put => rusty_s3::Method::Put,
            Self::Post => rusty_s3::Method::Post,
            Self::Delete => rusty_s3::Method::Delete,
        }
    }
}

/// Builds the signed requests of one replica.
pub struct RequestBuilder {
    bucket: Bucket,
    credentials: Credentials,
    prefix: Prefix,
    provider: Provider,
}

impl fmt::Debug for RequestBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestBuilder")
            .field("bucket", &self.bucket.name())
            .field("provider", &self.provider)
            .finish_non_exhaustive()
    }
}

impl RequestBuilder {
    /// # Errors
    /// [`ConfigError::BadUrl`] when the endpoint cannot address the bucket in
    /// the chosen style (virtual-host style with an IP address).
    pub fn new(config: &S3Config, credentials: &S3Credentials) -> Result<Self, ConfigError> {
        let bucket = Bucket::new(
            config.endpoint().url().clone(),
            config.style().to_signer(),
            config.bucket().as_str().to_owned(),
            config.region().as_str().to_owned(),
        )
        .map_err(|_unaddressable| ConfigError::BadUrl)?;
        Ok(Self {
            bucket,
            credentials: credentials.to_signer(),
            prefix: config.prefix().clone(),
            provider: config.provider(),
        })
    }

    fn base_headers(&self) -> Headers {
        let mut headers = vec![("user-agent", USER_AGENT.to_owned())];
        if self.provider == Provider::Tigris {
            headers.push(("x-tigris-consistent", "true".to_owned()));
        }
        headers
    }

    /// Signs `action` with the base headers and `extra`, returning the URL
    /// and the headers that must accompany it.
    fn sign<'a, A: S3Action<'a>>(
        &self,
        action: &mut A,
        now: Timestamp,
        extra: Headers,
    ) -> Result<(Url, Headers), RequestError> {
        let at = jiff::Timestamp::from_millisecond(now.unix_millis())
            .map_err(|_range| RequestError::ClockOutOfRange)?;
        let mut headers = self.base_headers();
        headers.extend(extra);
        for (name, value) in &headers {
            action.headers_mut().insert(*name, value.clone());
        }
        Ok((action.sign_with_time(EXPIRES_IN, &at), headers))
    }

    fn finish(method: Method, (url, headers): (Url, Headers), body: Vec<u8>) -> HttpRequest {
        HttpRequest {
            method,
            url,
            headers,
            body,
        }
    }

    /// A PUT of `body` with `Content-Length` and `Content-MD5` added to `extra`.
    fn write<'a, A: S3Action<'a>>(
        &self,
        action: &mut A,
        now: Timestamp,
        body: Vec<u8>,
        mut extra: Headers,
    ) -> Result<WriteRequest, RequestError> {
        let md5 = md5_of(&body);
        extra.splice(
            0..0,
            [
                ("content-length", body.len().to_string()),
                ("content-md5", STANDARD.encode(md5)),
            ],
        );
        let signed = self.sign(action, now, extra)?;
        Ok(WriteRequest {
            request: Self::finish(Method::Put, signed, body),
            md5,
        })
    }

    fn object(&self, key: &FileKey) -> String {
        object_key(&self.prefix, key)
    }

    /// Signs a request rusty-s3 has no action for: any method and query, `headers` after the
    /// store's own, each one signed. `object` is a full key; `None` addresses the bucket.
    pub(crate) fn sign_raw(
        &self,
        now: Timestamp,
        method: Method,
        object: Option<&str>,
        query: &[(&str, &str)],
        headers: Headers,
        body: Vec<u8>,
    ) -> Result<HttpRequest, RequestError> {
        let at = jiff::Timestamp::from_millisecond(now.unix_millis())
            .map_err(|_range| RequestError::ClockOutOfRange)?;
        let base = match object {
            Some(key) => self.bucket.object_url(key),
            None => self.bucket.base_url().clone(),
        };
        let mut all = self.base_headers();
        all.extend(headers);
        // The signer merges the query with its own sorted `X-Amz-*` pairs and assumes both
        // are sorted; an unsorted one signs a canonical request the server does not rebuild.
        let mut query = query.to_vec();
        query.sort_unstable();
        let url = rusty_s3::signing::sign(
            &at,
            method.to_signer(),
            base,
            self.credentials.key(),
            self.credentials.secret(),
            self.credentials.token(),
            self.bucket.region(),
            EXPIRES_IN.as_secs(),
            query.iter().copied(),
            all.iter().map(|(name, value)| (*name, value.as_str())),
        );
        Ok(HttpRequest {
            method,
            url,
            headers: all,
            body,
        })
    }

    /// ListMultipartUploads under this store's prefix (`<prefix>/`), after `markers`
    /// (`key-marker`, `upload-id-marker`) when given. rusty-s3 has no action for it. MinIO
    /// answers a prefix that is not a whole object key with no uploads, so it is asked for
    /// the bucket's uploads and the caller keeps its own.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn list_uploads(
        &self,
        now: Timestamp,
        markers: Option<(&str, &str)>,
    ) -> Result<HttpRequest, RequestError> {
        let prefix = format!("{}/", self.prefix.as_str());
        let mut query = vec![("uploads", "")];
        if self.provider != Provider::Minio {
            query.push(("prefix", prefix.as_str()));
        }
        if let Some((key, id)) = markers {
            query.push(("key-marker", key));
            query.push(("upload-id-marker", id));
        }
        self.sign_raw(now, Method::Get, None, &query, Vec::new(), Vec::new())
    }

    /// ListObjectsV2 of one level, `max_keys` entries, after `after` when given.
    /// Departure from Go: it seeks with `start-after`; Go lists from the start
    /// and filters client-side (rc.go:1572-1771).
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn list_page(
        &self,
        now: Timestamp,
        level: Level,
        after: Option<TxidRange>,
        max_keys: PageLimit,
    ) -> Result<HttpRequest, RequestError> {
        let mut action = self.bucket.list_objects_v2(Some(&self.credentials));
        action.with_prefix(level_prefix(&self.prefix, level));
        action.with_max_keys(max_keys.get());
        if let Some(cursor) = after {
            action.with_start_after(start_after(&self.prefix, level, cursor));
        }
        let signed = self.sign(&mut action, now, Vec::new())?;
        Ok(Self::finish(Method::Get, signed, Vec::new()))
    }

    /// The next page of a listing: `token` is the previous page's
    /// `NextContinuationToken`. It carries no `start-after`: the token already
    /// holds the position.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn list_continue(
        &self,
        now: Timestamp,
        level: Level,
        token: &str,
        max_keys: PageLimit,
    ) -> Result<HttpRequest, RequestError> {
        let mut action = self.bucket.list_objects_v2(Some(&self.credentials));
        action.with_prefix(level_prefix(&self.prefix, level));
        action.with_max_keys(max_keys.get());
        action.with_continuation_token(token);
        let signed = self.sign(&mut action, now, Vec::new())?;
        Ok(Self::finish(Method::Get, signed, Vec::new()))
    }

    /// HEAD of one object: its size and stored created-at time.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn head(&self, now: Timestamp, key: &FileKey) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let mut action = self.bucket.head_object(Some(&self.credentials), &object);
        let signed = self.sign(&mut action, now, Vec::new())?;
        Ok(Self::finish(Method::Head, signed, Vec::new()))
    }

    /// A ranged GET of `len` bytes from `offset`.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn get_range(
        &self,
        now: Timestamp,
        key: &FileKey,
        offset: u64,
        len: ReadLen,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let mut action = self.bucket.get_object(Some(&self.credentials), &object);
        let extra = vec![("range", range_header(offset, len))];
        let signed = self.sign(&mut action, now, extra)?;
        Ok(Self::finish(Method::Get, signed, Vec::new()))
    }

    /// A single PUT with `Content-Length` (also when 0), `Content-MD5` and the
    /// created-at time. The created-at time is millisecond RFC 3339 with `Z`: the same instant Go's
    /// RFC3339Nano writes (rc.go:892-898), and Go's parser reads it.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn put_object(
        &self,
        now: Timestamp,
        key: &FileKey,
        body: Vec<u8>,
        created_at: Timestamp,
    ) -> Result<WriteRequest, RequestError> {
        let object = self.object(key);
        let mut action = self.bucket.put_object(Some(&self.credentials), &object);
        self.write(
            &mut action,
            now,
            body,
            vec![(CREATED_AT_HEADER, created_at.to_string())],
        )
    }

    /// Starts a multipart upload; the created-at time is set here, on the object.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn create_multipart(
        &self,
        now: Timestamp,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let mut action = self
            .bucket
            .create_multipart_upload(Some(&self.credentials), &object);
        let signed = self.sign(
            &mut action,
            now,
            vec![(CREATED_AT_HEADER, created_at.to_string())],
        )?;
        Ok(Self::finish(Method::Post, signed, Vec::new()))
    }

    /// One part, with `Content-Length` and `Content-MD5`.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn upload_part(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
        part: PartNumber,
        body: Vec<u8>,
    ) -> Result<WriteRequest, RequestError> {
        let object = self.object(key);
        let mut action = self.bucket.upload_part(
            Some(&self.credentials),
            &object,
            part.get(),
            upload.as_str(),
        );
        self.write(&mut action, now, body, Vec::new())
    }

    /// Completes an upload from its parts' ETags, in part order (the n-th is part n).
    ///
    /// # Errors
    /// [`RequestError::BadBatch`] for no parts or more than 10000;
    /// [`RequestError::ClockOutOfRange`].
    pub fn complete_multipart(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
        etags: &[String],
    ) -> Result<HttpRequest, RequestError> {
        let max = usize::from(PartNumber::MAX_RAW);
        if etags.is_empty() || etags.len() > max {
            return Err(RequestError::BadBatch {
                len: etags.len(),
                max,
            });
        }
        let object = self.object(key);
        let mut action = self.bucket.complete_multipart_upload(
            Some(&self.credentials),
            &object,
            upload.as_str(),
            etags.iter().map(String::as_str),
        );
        let signed = {
            let size = action.clone().body().len();
            self.sign(&mut action, now, vec![("content-length", size.to_string())])?
        };
        Ok(Self::finish(
            Method::Post,
            signed,
            action.body().into_bytes(),
        ))
    }

    /// Abandons an upload.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn abort_multipart(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let mut action =
            self.bucket
                .abort_multipart_upload(Some(&self.credentials), &object, upload.as_str());
        let signed = self.sign(&mut action, now, Vec::new())?;
        Ok(Self::finish(Method::Delete, signed, Vec::new()))
    }

    /// Deletes up to `MAX_DELETE_KEYS` objects in quiet mode, `Content-MD5`
    /// over the XML body (rc.go:921-954, :1066-1105).
    ///
    /// # Errors
    /// [`RequestError::BadBatch`] for none or more than 1000;
    /// [`RequestError::ClockOutOfRange`].
    pub fn delete_objects(
        &self,
        now: Timestamp,
        keys: &[FileKey],
    ) -> Result<HttpRequest, RequestError> {
        if keys.is_empty() || keys.len() > MAX_DELETE_KEYS {
            return Err(RequestError::BadBatch {
                len: keys.len(),
                max: MAX_DELETE_KEYS,
            });
        }
        let objects: Vec<ObjectIdentifier> = keys
            .iter()
            .map(|key| ObjectIdentifier::new(self.object(key)))
            .collect();
        let mut action = self
            .bucket
            .delete_objects(Some(&self.credentials), objects.iter());
        action.set_quiet(true);
        let (body, content_md5) = action.clone().body_with_md5();
        let extra = vec![
            ("content-length", body.len().to_string()),
            ("content-md5", content_md5),
        ];
        let signed = self.sign(&mut action, now, extra)?;
        Ok(Self::finish(Method::Post, signed, body.into_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::SecretString;
    use lsr_core::parse_file_name;
    use lsr_ltx::Txid;
    use proptest::prelude::*;

    const NOW: i64 = 1_369_353_600_000; // 2013-05-24T00:00:00Z, the AWS documentation's example instant.

    fn range(min: u64, max: u64) -> TxidRange {
        TxidRange::new(Txid::new(min).unwrap(), Txid::new(max).unwrap()).unwrap()
    }

    fn key(level: u8, min: u64, max: u64) -> FileKey {
        FileKey::new(Level::new(level).unwrap(), range(min, max))
    }

    fn builder(endpoint: &str) -> RequestBuilder {
        let config = S3Config::new(
            crate::config::Endpoint::new(endpoint).unwrap(),
            crate::config::BucketName::new("examplebucket").unwrap(),
            Prefix::new("db/one").unwrap(),
        );
        let credentials = S3Credentials::new(
            "AKIAIOSFODNN7EXAMPLE".to_owned(),
            SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned()),
        );
        RequestBuilder::new(&config, &credentials).unwrap()
    }

    fn minio() -> RequestBuilder {
        builder("http://localhost:9000")
    }

    fn now() -> Timestamp {
        Timestamp::from_unix_millis(NOW)
    }

    fn upload() -> UploadId {
        UploadId::new("abc".to_owned())
    }

    /// One request from every builder.
    fn every_request(b: &RequestBuilder) -> Vec<HttpRequest> {
        let k = key(1, 1, 5);
        let etags = vec!["\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned()];
        vec![
            b.list_page(now(), Level::ZERO, Some(range(1, 2)), PageLimit::MAX)
                .unwrap(),
            b.head(now(), &k).unwrap(),
            b.get_range(now(), &k, 10, ReadLen::new(100).unwrap())
                .unwrap(),
            b.put_object(now(), &k, b"hello".to_vec(), now())
                .unwrap()
                .into(),
            b.create_multipart(now(), &k, now()).unwrap(),
            b.upload_part(
                now(),
                &k,
                &upload(),
                PartNumber::new(1).unwrap(),
                vec![1, 2, 3],
            )
            .unwrap()
            .into(),
            b.complete_multipart(now(), &k, &upload(), &etags).unwrap(),
            b.abort_multipart(now(), &k, &upload()).unwrap(),
            b.delete_objects(now(), &[k]).unwrap(),
        ]
    }

    fn query(req: &HttpRequest, name: &str) -> Option<String> {
        req.url()
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.into_owned())
    }

    #[test]
    fn object_key_matches_go() {
        // Go: c.Path + "/" + fmt.Sprintf("%04x/%s", level, filename), rc.go:702.
        let prefix = Prefix::new("db/one").unwrap();
        let cases = [
            (0, 1, 1, "db/one/0000/0000000000000001-0000000000000001.ltx"),
            (
                1,
                1,
                0x20,
                "db/one/0001/0000000000000001-0000000000000020.ltx",
            ),
            (
                9,
                0xff,
                u64::MAX,
                "db/one/0009/00000000000000ff-ffffffffffffffff.ltx",
            ),
        ];
        for (level, min, max, want) in cases {
            assert_eq!(object_key(&prefix, &key(level, min, max)), want);
        }
        assert_eq!(
            level_prefix(&prefix, Level::new(9).unwrap()),
            "db/one/0009/"
        );
        assert_eq!(
            start_after(&prefix, Level::ZERO, range(3, 7)),
            "db/one/0000/0000000000000003-0000000000000007.ltx"
        );
    }

    #[test]
    fn range_header_saturates() {
        let one = ReadLen::new(1).unwrap();
        let big = ReadLen::new(16 * 1024 * 1024).unwrap();
        assert_eq!(range_header(0, one), "bytes=0-0");
        assert_eq!(range_header(10, one), "bytes=10-10");
        assert_eq!(range_header(0, big), "bytes=0-16777215");
        assert_eq!(
            range_header(u64::MAX - 1, one),
            format!("bytes={0}-{0}", u64::MAX - 1)
        );
        assert_eq!(
            range_header(u64::MAX - 1, big),
            format!("bytes={}-{}", u64::MAX - 1, u64::MAX)
        );
        assert_eq!(
            range_header(u64::MAX, big),
            format!("bytes={0}-{0}", u64::MAX)
        );
    }

    #[test]
    fn presigned_query_has_the_fixed_clock_and_expiry() {
        for req in every_request(&minio()) {
            assert_eq!(
                query(&req, "X-Amz-Date").as_deref(),
                Some("20130524T000000Z")
            );
            assert_eq!(query(&req, "X-Amz-Expires").as_deref(), Some("300"));
            assert_eq!(
                query(&req, "X-Amz-Algorithm").as_deref(),
                Some("AWS4-HMAC-SHA256")
            );
            assert_eq!(query(&req, "X-Amz-Signature").map(|s| s.len()), Some(64));
        }
    }

    #[test]
    fn signing_is_deterministic_and_covers_the_range_header() {
        let b = minio();
        let k = key(0, 1, 5);
        let len = ReadLen::new(10).unwrap();
        let one = b.get_range(now(), &k, 0, len).unwrap();
        let again = b.get_range(now(), &k, 0, len).unwrap();
        let other = b.get_range(now(), &k, 1, len).unwrap();
        assert_eq!(one.url(), again.url());
        assert_ne!(
            query(&one, "X-Amz-Signature"),
            query(&other, "X-Amz-Signature")
        );
    }

    #[test]
    fn every_header_sent_is_signed() {
        for builder in [minio(), builder("https://fly.storage.tigris.dev")] {
            for req in every_request(&builder) {
                let signed = query(&req, "X-Amz-SignedHeaders").unwrap();
                let signed: Vec<&str> = signed.split(';').collect();
                assert!(signed.contains(&"host"));
                for (name, _) in req.headers() {
                    assert!(
                        signed.contains(name),
                        "{name} is sent but not signed ({:?})",
                        req.method()
                    );
                }
            }
        }
    }

    #[test]
    fn provider_headers_and_no_checksum_headers() {
        for req in every_request(&minio()) {
            assert_eq!(req.header("user-agent"), Some(USER_AGENT));
            assert_eq!(req.header("x-tigris-consistent"), None);
        }
        for req in every_request(&builder("https://fly.storage.tigris.dev")) {
            assert_eq!(req.header("x-tigris-consistent"), Some("true"));
            assert!(
                req.headers()
                    .iter()
                    .all(|(name, _)| !name.starts_with("x-amz-checksum"))
            );
            assert!(req.header("content-encoding").is_none());
        }
    }

    /// The digest an `ETag` is checked against is the MD5 of the body that
    /// goes out, and the `Content-MD5` header the store checks says the same.
    #[test]
    fn every_write_carries_the_digest_of_its_own_body() {
        let b = minio();
        let k = key(1, 1, 5);
        for body in [Vec::new(), b"hello".to_vec(), vec![7; 70_000]] {
            let put = b.put_object(now(), &k, body.clone(), now()).unwrap();
            let part = b
                .upload_part(
                    now(),
                    &k,
                    &upload(),
                    PartNumber::new(2).unwrap(),
                    body.clone(),
                )
                .unwrap();
            for write in [put, part] {
                assert_eq!(write.md5(), md5_of(&body));
                assert_eq!(write.body(), &body[..]);
                let header = STANDARD.encode(write.md5());
                assert_eq!(write.header("content-md5"), Some(header.as_str()));
                let sent: HttpRequest = write.clone().into();
                assert_eq!(sent, write.into_request());
            }
        }
    }

    #[test]
    fn put_carries_length_md5_and_created_at() {
        let k = key(0, 1, 1);
        let created_at = Timestamp::from_unix_millis(1_795_182_585_566);
        let req = minio()
            .put_object(now(), &k, b"hello".to_vec(), created_at)
            .unwrap();
        assert_eq!(req.method(), Method::Put);
        assert_eq!(req.header("content-length"), Some("5"));
        assert_eq!(req.header("content-md5"), Some("XUFAKrxLKna5cZ2REBfFkg=="));
        assert_eq!(
            req.header(CREATED_AT_HEADER),
            Some("2026-11-20T13:49:45.566Z")
        );
        assert_eq!(
            crate::response::hex(&req.md5()),
            "5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(req.body(), b"hello");
        let empty = minio()
            .put_object(now(), &k, Vec::new(), created_at)
            .unwrap();
        assert_eq!(empty.header("content-length"), Some("0"));
        assert_eq!(
            empty.header("content-md5"),
            Some("1B2M2Y8AsgTpgAmY7PhCfg==")
        );
    }

    #[test]
    fn list_page_seeks_with_start_after() {
        let b = minio();
        let first = b
            .list_page(now(), Level::ZERO, None, PageLimit::MAX)
            .unwrap();
        assert_eq!(query(&first, "prefix").as_deref(), Some("db/one/0000/"));
        assert_eq!(query(&first, "max-keys").as_deref(), Some("1000"));
        assert_eq!(query(&first, "start-after"), None);
        assert_eq!(query(&first, "delimiter"), None);
        let next = b
            .list_page(now(), Level::ZERO, Some(range(3, 7)), PageLimit::MAX)
            .unwrap();
        assert_eq!(
            query(&next, "start-after").as_deref(),
            Some("db/one/0000/0000000000000003-0000000000000007.ltx")
        );
    }

    #[test]
    fn multipart_part_and_complete_shapes() {
        let b = minio();
        let k = key(1, 1, 5);
        let part = b
            .upload_part(
                now(),
                &k,
                &upload(),
                PartNumber::new(3).unwrap(),
                vec![0; 4],
            )
            .unwrap();
        assert_eq!(query(&part, "partNumber").as_deref(), Some("3"));
        assert_eq!(query(&part, "uploadId").as_deref(), Some("abc"));
        assert!(part.header("content-md5").is_some());
        let etags = vec!["\"a\"".to_owned(), "\"b\"".to_owned()];
        let done = b.complete_multipart(now(), &k, &upload(), &etags).unwrap();
        let body = String::from_utf8(done.body().to_vec()).unwrap();
        assert!(
            body.find("<PartNumber>1</PartNumber>").unwrap()
                < body.find("<PartNumber>2</PartNumber>").unwrap()
        );
        assert_eq!(
            done.header("content-length"),
            Some(body.len().to_string().as_str())
        );
        let create = b.create_multipart(now(), &k, now()).unwrap();
        assert!(create.header(CREATED_AT_HEADER).is_some());
    }

    #[test]
    fn batches_are_bounded() {
        let b = minio();
        let k = key(0, 1, 1);
        assert_eq!(
            b.delete_objects(now(), &[]).unwrap_err(),
            RequestError::BadBatch { len: 0, max: 1000 }
        );
        let many = vec![k; 1001];
        assert_eq!(
            b.delete_objects(now(), &many).unwrap_err(),
            RequestError::BadBatch {
                len: 1001,
                max: 1000
            }
        );
        let full = vec![k; 1000];
        let req = b.delete_objects(now(), &full).unwrap();
        assert_eq!(req.method(), Method::Post);
        assert!(req.header("content-md5").is_some());
        assert!(String::from_utf8_lossy(req.body()).contains("<Quiet>true</Quiet>"));
        assert_eq!(
            b.complete_multipart(now(), &k, &upload(), &[]).unwrap_err(),
            RequestError::BadBatch {
                len: 0,
                max: 10_000
            }
        );
    }

    /// S3 takes 10000 parts in one completion and no more.
    #[test]
    fn complete_multipart_takes_the_part_cap_and_no_more() {
        let b = minio();
        let etags = |n: usize| vec!["\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned(); n];
        b.complete_multipart(now(), &key(0, 1, 1), &upload(), &etags(10_000))
            .unwrap();
        assert_eq!(
            b.complete_multipart(now(), &key(0, 1, 1), &upload(), &etags(10_001))
                .unwrap_err(),
            RequestError::BadBatch {
                len: 10_001,
                max: 10_000
            }
        );
    }

    #[test]
    fn clock_out_of_range_is_an_error() {
        let far = Timestamp::from_unix_millis(i64::MAX);
        assert_eq!(
            minio().head(far, &key(0, 1, 1)).unwrap_err(),
            RequestError::ClockOutOfRange
        );
    }

    #[test]
    fn debug_is_method_and_path_only() {
        let req = minio().head(now(), &key(0, 1, 1)).unwrap();
        let shown = format!("{req:?}");
        assert!(shown.starts_with("HttpRequest { method: Head, path: "));
        assert!(
            !shown.contains("Signature") && !shown.contains('?') && !shown.contains("user-agent")
        );
    }

    proptest! {
        #[test]
        fn object_key_round_trips(level in 0u8..=9, min in 1u64.., len in 0u64..1000) {
            let max = min.saturating_add(len);
            let prefix = Prefix::new("p/q").unwrap();
            let k = key(level, min, max);
            let full = object_key(&prefix, &k);
            let name = full.strip_prefix(level_prefix(&prefix, k.level()).as_str()).unwrap();
            prop_assert_eq!(parse_file_name(name).unwrap(), k.range());
        }

        #[test]
        fn range_header_is_a_valid_range(offset in any::<u64>(), len in 1u32..=ReadLen::MAX_RAW) {
            let text = range_header(offset, ReadLen::new(len).unwrap());
            let (start, end) = text.strip_prefix("bytes=").unwrap().split_once('-').unwrap();
            let (start, end): (u64, u64) = (start.parse().unwrap(), end.parse().unwrap());
            prop_assert_eq!(start, offset);
            prop_assert!(end >= start);
        }
    }
}
