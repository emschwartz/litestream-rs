//! Requests as data: what to send, already signed, with no I/O.
//!
//! Every request is signed in its `Authorization` header (`sigv4.rs`) at the `now` the
//! caller read off its clock, with the SHA-256 of its body in `x-amz-content-sha256` and in
//! the signature, as Go's SDK signs by default (rc.go:132-139). The store checks the body
//! against that hash, so a body altered in flight is refused (`XAmzContentSHA256Mismatch`);
//! a request with no body signs the empty body's hash. Every header sent but
//! `authorization` is signed; the tests check that for every builder. No
//! `x-amz-checksum-*` header and no aws-chunked encoding is ever sent (Go removes the
//! trailer: rc.go:1039-1041), and `Content-MD5` only on a DeleteObjects, where S3 requires
//! one and Go sends it.
//!
//! Nothing here prints a URL, a header value or a body: [`HttpRequest`]'s
//! `Debug` is the method and the path, because the headers hold the signature
//! and, for temporary credentials, the session token.

use std::fmt;

use lsr_core::{FileKey, Level, PageLimit, PartNumber, ReadLen};
use lsr_ltx::{Timestamp, TxidRange};
use rusty_s3::actions::ObjectIdentifier;
use rusty_s3::{Bucket, Credentials};
use thiserror::Error;
use url::Url;

use crate::config::{ConfigError, Prefix, Provider, S3Config};
use crate::credentials::S3Credentials;
use crate::sigv4::{Key, PayloadHash, authorization, canonical_query};

#[cfg(feature = "testing")]
mod raw;
#[cfg(feature = "testing")]
mod tamper;

#[cfg(feature = "testing")]
pub use raw::{OpenUpload, abandon_upload, abort_upload, open_uploads};

/// Go's `MaxKeys`: the most one DeleteObjects may name.
pub(crate) const MAX_DELETE_KEYS: usize = 1000;

// S3's DeleteObjects cap, and room for a whole delete job (core keeps one under
// `MAX_DELETE`) in one request.
const _: () = assert!(MAX_DELETE_KEYS <= 1000);
const _: () = assert!(MAX_DELETE_KEYS >= lsr_core::MAX_DELETE);

/// The object metadata key Go stores a file's timestamp under.
pub(crate) const CREATED_AT_HEADER: &str = "x-amz-meta-litestream-timestamp";

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
    /// An unsigned request, for the credential sources (STS, container, instance metadata).
    pub(crate) fn new(
        method: Method,
        url: Url,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            method,
            url,
            headers,
            body,
        }
    }

    /// A request as a test server read it off a socket, to hand to [`FakeS3`](crate::FakeS3)
    /// behind a real HTTP listener.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn from_wire(
        method: Method,
        url: Url,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
    ) -> Self {
        Self::new(method, url, headers, body)
    }

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

    /// The same request to another URL, for tests that point it at a local socket.
    #[cfg(test)]
    pub(crate) fn with_url(mut self, url: Url) -> Self {
        self.url = url;
        self
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

    /// Gives a request built without its body the body read for it
    /// ([`StreamedRequest::into_buffered`](crate::StreamedRequest::into_buffered)).
    pub(crate) fn set_body(&mut self, body: Vec<u8>) {
        self.body = body;
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

type Headers = Vec<(&'static str, String)>;

/// A condition a write asks the bucket to check before it publishes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Precondition {
    /// No condition: the write replaces whatever the key holds.
    None,
    /// `If-None-Match: *`: publish only into a free key, else 412
    /// `PreconditionFailed` and nothing changes.
    KeyIsFree,
}

impl Precondition {
    fn header(self) -> Option<(&'static str, String)> {
        match self {
            Self::None => None,
            Self::KeyIsFree => Some(("if-none-match", "*".to_owned())),
        }
    }
}

impl Method {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::Post => "POST",
            Self::Delete => "DELETE",
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

    /// The same replica, signed with other credentials: what a refresh builds.
    pub(crate) fn with_credentials(&self, credentials: &S3Credentials) -> Self {
        Self {
            bucket: self.bucket.clone(),
            credentials: credentials.to_signer(),
            prefix: self.prefix.clone(),
            provider: self.provider,
        }
    }

    fn base_headers(&self) -> Headers {
        let mut headers = vec![("user-agent", USER_AGENT.to_owned())];
        if self.provider == Provider::Tigris {
            headers.push(("x-tigris-consistent", "true".to_owned()));
        }
        headers
    }

    fn object(&self, key: &FileKey) -> String {
        object_key(&self.prefix, key)
    }

    /// Signs a request: `method` on `object` (a full key; `None` addresses the bucket) with
    /// `query`, the store's own headers and `headers`, and a body whose SHA-256 is `payload`
    /// (`body` itself, or empty when it is read while the request is sent,
    /// [`StreamedRequest`](crate::StreamedRequest)). Every header sent but `authorization` is
    /// signed, `host` too, which the client sends from the URL.
    pub(crate) fn sign(
        &self,
        now: Timestamp,
        (method, object): (Method, Option<&str>),
        query: &[(&str, &str)],
        headers: Headers,
        (payload, body): (PayloadHash, Vec<u8>),
    ) -> Result<HttpRequest, RequestError> {
        let at = jiff::Timestamp::from_millisecond(now.unix_millis())
            .map_err(|_range| RequestError::ClockOutOfRange)?;
        let amz_date = at.strftime("%Y%m%dT%H%M%SZ").to_string();
        let mut url = match object {
            Some(key) => self.bucket.object_url(key),
            None => self.bucket.base_url().clone(),
        };
        let query = canonical_query(query);
        url.set_query(Some(query.as_str()).filter(|query| !query.is_empty()));
        let mut all = self.base_headers();
        all.extend(headers);
        all.push(("x-amz-content-sha256", payload.to_hex()));
        all.push(("x-amz-date", amz_date.clone()));
        if let Some(token) = self.credentials.token().filter(|token| !token.is_empty()) {
            all.push(("x-amz-security-token", token.to_owned()));
        }
        all.sort_by_key(|(name, _)| *name);
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };
        let mut signed: Vec<(&str, &str)> = all
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        signed.push(("host", host.as_str()));
        signed.sort_by_key(|(name, _)| *name);
        let key = Key {
            id: self.credentials.key(),
            secret: self.credentials.secret(),
            region: self.bucket.region(),
        };
        let authorization = authorization(&key, &amz_date, method.as_str(), &url, &signed, payload);
        all.push(("authorization", authorization));
        Ok(HttpRequest {
            method,
            url,
            headers: all,
            body,
        })
    }

    /// A PUT of `body` to `object`: [`Self::write_of`] with the body's own length and hash.
    #[cfg(any(test, feature = "testing"))]
    fn write(
        &self,
        now: Timestamp,
        (object, query): (&str, &[(&str, &str)]),
        body: Vec<u8>,
        extra: Headers,
    ) -> Result<HttpRequest, RequestError> {
        let sized = (
            u64::try_from(body.len()).unwrap_or(u64::MAX),
            PayloadHash::of(&body),
        );
        self.write_of(now, (object, query), sized, body, extra)
    }

    /// A PUT that declares a body of `len` bytes whose SHA-256 is `payload`: `body` is that
    /// body, or empty when it is read while the request is sent.
    fn write_of(
        &self,
        now: Timestamp,
        (object, query): (&str, &[(&str, &str)]),
        (len, payload): (u64, PayloadHash),
        body: Vec<u8>,
        mut extra: Headers,
    ) -> Result<HttpRequest, RequestError> {
        extra.push(("content-length", len.to_string()));
        self.sign(
            now,
            (Method::Put, Some(object)),
            query,
            extra,
            (payload, body),
        )
    }

    /// A request with no body.
    fn bare(
        &self,
        now: Timestamp,
        target: (Method, Option<&str>),
        query: &[(&str, &str)],
        headers: Headers,
    ) -> Result<HttpRequest, RequestError> {
        self.sign(
            now,
            target,
            query,
            headers,
            (PayloadHash::EMPTY, Vec::new()),
        )
    }

    /// ListMultipartUploads under this store's prefix (`<prefix>/`), after `markers`
    /// (`key-marker`, `upload-id-marker`) when given. MinIO answers a prefix that is not a
    /// whole object key with no uploads, so it is asked for the bucket's uploads and the
    /// caller keeps its own.
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
        self.bare(now, (Method::Get, None), &query, Vec::new())
    }

    /// ListObjectsV2 of one level, `max_keys` entries, after `after` when given, or from
    /// `token`, the previous page's `NextContinuationToken` (which already holds the
    /// position, so no `start-after` goes with it). Keys come url-encoded.
    fn list(
        &self,
        now: Timestamp,
        level: Level,
        (after, token): (Option<TxidRange>, Option<&str>),
        max_keys: PageLimit,
    ) -> Result<HttpRequest, RequestError> {
        let prefix = level_prefix(&self.prefix, level);
        let max = max_keys.get().to_string();
        let after = after.map(|cursor| start_after(&self.prefix, level, cursor));
        let mut query = vec![
            ("list-type", "2"),
            ("encoding-type", "url"),
            ("prefix", prefix.as_str()),
            ("max-keys", max.as_str()),
        ];
        query.extend(after.as_deref().map(|after| ("start-after", after)));
        query.extend(token.map(|token| ("continuation-token", token)));
        self.bare(now, (Method::Get, None), &query, Vec::new())
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
        self.list(now, level, (after, None), max_keys)
    }

    /// The next page of a listing: `token` is the previous page's
    /// `NextContinuationToken`.
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
        self.list(now, level, (None, Some(token)), max_keys)
    }

    /// HEAD of one object: its size and stored created-at time.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn head(&self, now: Timestamp, key: &FileKey) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        self.bare(now, (Method::Head, Some(&object)), &[], Vec::new())
    }

    /// A GET of the object from `offset` to its end: no `Range` header from the first byte, as
    /// Go's whole-object GET sends, else `bytes=offset-`.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn get_from(
        &self,
        now: Timestamp,
        key: &FileKey,
        offset: u64,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let range = (offset > 0).then(|| ("range", format!("bytes={offset}-")));
        self.bare(
            now,
            (Method::Get, Some(&object)),
            &[],
            range.into_iter().collect(),
        )
    }

    /// A GET of bytes `offset` to `end` (exclusive): `bytes=offset-(end-1)`, the span one
    /// stream of a restore reads (`ObjectStore::read_span`). An empty span asks for its first byte.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn get_span(
        &self,
        now: Timestamp,
        key: &FileKey,
        offset: u64,
        end: u64,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let last = end.saturating_sub(1).max(offset);
        self.bare(
            now,
            (Method::Get, Some(&object)),
            &[],
            vec![("range", format!("bytes={offset}-{last}"))],
        )
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
        let extra = vec![("range", range_header(offset, len))];
        self.bare(now, (Method::Get, Some(&object)), &[], extra)
    }

    /// A single PUT with `Content-Length` (also when 0), the body's SHA-256 and the
    /// created-at time. The created-at time is millisecond RFC 3339 with `Z`: the same instant Go's
    /// RFC3339Nano writes (rc.go:892-898), and Go's parser reads it.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    #[cfg(any(test, feature = "testing"))]
    pub fn put_object(
        &self,
        now: Timestamp,
        key: &FileKey,
        body: Vec<u8>,
        created_at: Timestamp,
    ) -> Result<HttpRequest, RequestError> {
        self.put_object_if(now, key, body, created_at, Precondition::None)
    }

    /// [`Self::put_object`], with `precondition` sent (and signed) when it
    /// names one.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    #[cfg(any(test, feature = "testing"))]
    pub fn put_object_if(
        &self,
        now: Timestamp,
        key: &FileKey,
        body: Vec<u8>,
        created_at: Timestamp,
        precondition: Precondition,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let mut extra = vec![(CREATED_AT_HEADER, created_at.to_string())];
        extra.extend(precondition.header());
        self.write(now, (&object, &[]), body, extra)
    }

    /// [`Self::put_object_if`] for a body read while the request is sent: `len` bytes whose
    /// SHA-256 is `payload`, taken over a read checked against the first one. The request
    /// has no body of its own ([`StreamedRequest`](crate::StreamedRequest)).
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn put_object_of(
        &self,
        now: Timestamp,
        key: &FileKey,
        sized: (u64, PayloadHash),
        created_at: Timestamp,
        precondition: Precondition,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let mut extra = vec![(CREATED_AT_HEADER, created_at.to_string())];
        extra.extend(precondition.header());
        self.write_of(now, (&object, &[]), sized, Vec::new(), extra)
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
        let extra = vec![(CREATED_AT_HEADER, created_at.to_string())];
        self.bare(
            now,
            (Method::Post, Some(&object)),
            &[("uploads", "1")],
            extra,
        )
    }

    /// One part, with `Content-Length` and the body's SHA-256.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    #[cfg(any(test, feature = "testing"))]
    pub fn upload_part(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
        part: PartNumber,
        body: Vec<u8>,
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let number = part.get().to_string();
        let query = [
            ("partNumber", number.as_str()),
            ("uploadId", upload.as_str()),
        ];
        self.write(now, (&object, &query), body, Vec::new())
    }

    /// [`Self::upload_part`] for a body read while the request is sent: `len` bytes whose
    /// SHA-256 is `payload`, taken over a read checked against the first one.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn upload_part_of(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
        part: PartNumber,
        sized: (u64, PayloadHash),
    ) -> Result<HttpRequest, RequestError> {
        let object = self.object(key);
        let number = part.get().to_string();
        let query = [
            ("partNumber", number.as_str()),
            ("uploadId", upload.as_str()),
        ];
        self.write_of(now, (&object, &query), sized, Vec::new(), Vec::new())
    }

    /// Completes an upload from its parts' ETags, in part order (the n-th is part n).
    ///
    /// # Errors
    /// [`RequestError::BadBatch`] for no parts or more than 10000;
    /// [`RequestError::ClockOutOfRange`].
    #[cfg(any(test, feature = "testing"))]
    pub fn complete_multipart(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
        etags: &[String],
    ) -> Result<HttpRequest, RequestError> {
        self.complete_multipart_if(now, key, upload, etags, Precondition::None)
    }

    /// [`Self::complete_multipart`], with `precondition` sent (and signed)
    /// when it names one: S3 checks it when the upload completes, not when it
    /// starts.
    ///
    /// # Errors
    /// [`RequestError::BadBatch`] and [`RequestError::ClockOutOfRange`], as
    /// [`Self::complete_multipart`].
    pub fn complete_multipart_if(
        &self,
        now: Timestamp,
        key: &FileKey,
        upload: &UploadId,
        etags: &[String],
        precondition: Precondition,
    ) -> Result<HttpRequest, RequestError> {
        let max = usize::from(PartNumber::MAX_RAW);
        if etags.is_empty() || etags.len() > max {
            return Err(RequestError::BadBatch {
                len: etags.len(),
                max,
            });
        }
        let object = self.object(key);
        let body = self
            .bucket
            .complete_multipart_upload(
                None,
                &object,
                upload.as_str(),
                etags.iter().map(String::as_str),
            )
            .body()
            .into_bytes();
        let mut extra = vec![("content-length", body.len().to_string())];
        extra.extend(precondition.header());
        self.sign(
            now,
            (Method::Post, Some(&object)),
            &[("uploadId", upload.as_str())],
            extra,
            (PayloadHash::of(&body), body),
        )
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
        let query = [("uploadId", upload.as_str())];
        self.bare(now, (Method::Delete, Some(&object)), &query, Vec::new())
    }

    /// Deletes up to `MAX_DELETE_KEYS` objects in quiet mode. S3 requires a digest on this
    /// request whatever the payload signing, and Go sends `Content-MD5` over the XML body
    /// (rc.go:921-954, :1066-1105), so this does too.
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
        let mut action = self.bucket.delete_objects(None, objects.iter());
        action.set_quiet(true);
        let (body, content_md5) = action.body_with_md5();
        let extra = vec![
            ("content-length", body.len().to_string()),
            ("content-md5", content_md5),
        ];
        let body = body.into_bytes();
        self.sign(
            now,
            (Method::Post, None),
            &[("delete", "1")],
            extra,
            (PayloadHash::of(&body), body),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UrlStyle;
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
            b.put_object(now(), &k, b"hello".to_vec(), now()).unwrap(),
            b.create_multipart(now(), &k, now()).unwrap(),
            b.upload_part(
                now(),
                &k,
                &upload(),
                PartNumber::new(1).unwrap(),
                vec![1, 2, 3],
            )
            .unwrap(),
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

    /// With no path Go's expression is `"" + "/" + "0000/<file>"`: the key starts with a slash,
    /// and so does the listing prefix (rc.go:702, 1326, 1575).
    #[test]
    fn object_key_matches_go_with_no_path() {
        let bare = Prefix::new("").unwrap();
        assert_eq!(
            object_key(&bare, &key(0, 1, 1)),
            "/0000/0000000000000001-0000000000000001.ltx"
        );
        assert_eq!(
            object_key(&bare, &key(9, 0xff, u64::MAX)),
            "/0009/00000000000000ff-ffffffffffffffff.ltx"
        );
        assert_eq!(level_prefix(&bare, Level::new(9).unwrap()), "/0009/");
        assert_eq!(
            start_after(&bare, Level::ZERO, range(3, 7)),
            "/0000/0000000000000003-0000000000000007.ltx"
        );
    }

    /// A bare bucket, addressed in either style.
    fn bare(style: UrlStyle) -> RequestBuilder {
        let config = S3Config::new(
            crate::config::Endpoint::new("http://localhost:9000").unwrap(),
            crate::config::BucketName::new("examplebucket").unwrap(),
            Prefix::new("").unwrap(),
        )
        .with_style(style)
        // Minio is asked for every upload and the caller filters; this one sends the prefix.
        .with_provider(Provider::Other);
        let credentials = S3Credentials::new(
            "AKIAIOSFODNN7EXAMPLE".to_owned(),
            SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned()),
        );
        RequestBuilder::new(&config, &credentials).unwrap()
    }

    /// With no path the object URL has an empty first segment, `/examplebucket//0000/<file>` in
    /// path style and `//0000/<file>` in virtual-host style, and the listing and the upload
    /// listing ask for `/0000/` and `/`. Go's keys are the same.
    #[test]
    fn a_bare_bucket_keeps_the_leading_slash_in_every_request() {
        let file = "0000000000000001-0000000000000001.ltx";
        let k = key(0, 1, 1);
        for (style, path) in [
            (UrlStyle::Path, format!("/examplebucket//0000/{file}")),
            (UrlStyle::VirtualHost, format!("//0000/{file}")),
        ] {
            let b = bare(style);
            let list = b
                .list_page(now(), Level::ZERO, Some(range(1, 1)), PageLimit::MAX)
                .unwrap();
            assert_eq!(
                query(&list, "prefix").as_deref(),
                Some("/0000/"),
                "{style:?}"
            );
            assert_eq!(
                query(&list, "start-after"),
                Some(format!("/0000/{file}")),
                "{style:?}"
            );
            let more = b
                .list_continue(now(), Level::ZERO, "t", PageLimit::MAX)
                .unwrap();
            assert_eq!(
                query(&more, "prefix").as_deref(),
                Some("/0000/"),
                "{style:?}"
            );
            let uploads = b.list_uploads(now(), None).unwrap();
            assert_eq!(query(&uploads, "prefix").as_deref(), Some("/"), "{style:?}");

            let etags = vec!["\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned()];
            let objects = [
                b.head(now(), &k).unwrap(),
                b.get_range(now(), &k, 0, ReadLen::new(1).unwrap()).unwrap(),
                b.put_object(now(), &k, b"x".to_vec(), now()).unwrap(),
                b.create_multipart(now(), &k, now()).unwrap(),
                b.upload_part(now(), &k, &upload(), PartNumber::new(1).unwrap(), vec![1])
                    .unwrap(),
                b.complete_multipart(now(), &k, &upload(), &etags).unwrap(),
                b.abort_multipart(now(), &k, &upload()).unwrap(),
            ];
            for request in &objects {
                assert_eq!(request.url().path(), path, "{style:?} {request:?}");
            }
            let delete = b.delete_objects(now(), &[k, key(1, 2, 3)]).unwrap();
            let body = String::from_utf8(delete.body().to_vec()).unwrap();
            assert!(
                body.contains(&format!("<Key>/0000/{file}</Key>"))
                    && body.contains("<Key>/0001/0000000000000002-0000000000000003.ltx</Key>"),
                "{style:?} {body}"
            );
        }
    }

    /// The `Authorization` a request would carry if it were signed again from what it sends:
    /// its URL, every header but `authorization`, and `host` from the URL.
    fn resign(req: &HttpRequest, url: &Url, headers: &[(&str, &str)]) -> String {
        let host = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap());
        let mut all: Vec<(&str, &str)> = headers.to_vec();
        all.push(("host", host.as_str()));
        all.sort_by_key(|(name, _)| *name);
        let key = Key {
            id: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
        };
        let payload = PayloadHash::of(req.body());
        let date = req.header("x-amz-date").unwrap();
        authorization(&key, date, req.method().as_str(), url, &all, payload)
    }

    fn sent_headers(req: &HttpRequest) -> Vec<(&str, &str)> {
        req.headers()
            .iter()
            .filter(|(name, _)| *name != "authorization")
            .map(|(name, value)| (*name, value.as_str()))
            .collect()
    }

    /// SigV4 signs the path as sent, and S3 does not normalise it, so the empty segment must be
    /// in the canonical URI. The signature of the URL with the slash collapsed is another one.
    #[test]
    fn the_signature_covers_the_empty_segment() {
        let b = bare(UrlStyle::Path);
        let sent = b
            .raw(
                now(),
                Method::Get,
                Some("/0000/f.ltx"),
                &[],
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
        assert_eq!(sent.url().path(), "/examplebucket//0000/f.ltx");
        let headers = sent_headers(&sent);
        let auth = sent.header("authorization").unwrap();
        assert_eq!(resign(&sent, sent.url(), &headers), auth, "signed as sent");
        let mut collapsed = sent.url().clone();
        collapsed.set_path("/examplebucket/0000/f.ltx");
        assert_ne!(
            resign(&sent, &collapsed, &headers),
            auth,
            "the path is signed"
        );
    }

    /// A server rebuilds the signed headers sorted by name, so the signer sorts them first: an
    /// unsorted list made AWS answer SignatureDoesNotMatch.
    #[test]
    fn a_raw_request_signs_its_headers_in_name_order() {
        let b = bare(UrlStyle::Path);
        let given = vec![
            ("x-amz-meta-a", "1".to_owned()),
            ("if-none-match", "*".to_owned()),
        ];
        let sent = b
            .raw(now(), Method::Put, Some("k"), &[], given, b"x".to_vec())
            .unwrap();
        let names: Vec<&str> = sent_headers(&sent).iter().map(|(name, _)| *name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "sent in name order");
        assert_eq!(
            sent.headers().last().map(|(name, _)| *name),
            Some("authorization")
        );
        let auth = sent.header("authorization").unwrap();
        let mut signed = sent_headers(&sent);
        assert_eq!(
            resign(&sent, sent.url(), &signed),
            auth,
            "signed in name order"
        );
        // The same headers named in another order sign another request.
        signed.reverse();
        let mut url = sent.url().clone();
        url.set_query(None);
        let host = format!("{}:{}", url.host_str().unwrap(), url.port().unwrap());
        signed.push(("host", host.as_str()));
        let key = Key {
            id: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            region: "us-east-1",
        };
        let date = sent.header("x-amz-date").unwrap();
        let reordered = authorization(&key, date, "PUT", &url, &signed, PayloadHash::of(b"x"));
        assert_ne!(reordered, auth, "the order is signed");
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
    fn every_request_is_signed_in_its_header_at_the_fixed_clock() {
        for req in every_request(&minio()) {
            assert_eq!(req.header("x-amz-date"), Some("20130524T000000Z"));
            let auth = req.header("authorization").unwrap();
            assert!(
                auth.starts_with(
                    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders="
                ),
                "{auth}"
            );
            assert_eq!(
                auth.rsplit_once("Signature=").map(|(_, s)| s.len()),
                Some(64)
            );
            // Nothing of the signature rides in the URL.
            assert!(
                req.url()
                    .query_pairs()
                    .all(|(name, _)| !name.starts_with("X-Amz-"))
            );
            assert_eq!(
                resign(&req, req.url(), &sent_headers(&req)),
                auth,
                "{req:?}"
            );
        }
    }

    /// Every request signs the SHA-256 of the body it sends; one with no body signs the empty
    /// body's hash.
    #[test]
    fn every_request_signs_the_hash_of_its_own_body() {
        for req in every_request(&minio()) {
            let want = PayloadHash::of(req.body()).to_hex();
            assert_eq!(
                req.header("x-amz-content-sha256"),
                Some(want.as_str()),
                "{req:?}"
            );
            if req.body().is_empty() {
                assert_eq!(want, PayloadHash::EMPTY.to_hex());
            }
        }
    }

    /// Temporary credentials put their session token in a signed header, never in the URL.
    #[test]
    fn a_session_token_is_a_signed_header() {
        let config = S3Config::new(
            crate::config::Endpoint::new("http://localhost:9000").unwrap(),
            crate::config::BucketName::new("examplebucket").unwrap(),
            Prefix::new("db/one").unwrap(),
        );
        let credentials = S3Credentials::new(
            "AKIAIOSFODNN7EXAMPLE".to_owned(),
            SecretString::new("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned()),
        )
        .with_session_token(SecretString::new("the-token".to_owned()));
        let b = RequestBuilder::new(&config, &credentials).unwrap();
        let req = b.head(now(), &key(0, 1, 1)).unwrap();
        assert_eq!(req.header("x-amz-security-token"), Some("the-token"));
        assert!(!req.url().as_str().contains("the-token"));
        assert!(req.signed_headers().contains(&"x-amz-security-token"));
        let plain = minio().head(now(), &key(0, 1, 1)).unwrap();
        assert_eq!(plain.header("x-amz-security-token"), None);
    }

    #[test]
    fn signing_is_deterministic_and_covers_the_range_header() {
        let b = minio();
        let k = key(0, 1, 5);
        let len = ReadLen::new(10).unwrap();
        let one = b.get_range(now(), &k, 0, len).unwrap();
        let again = b.get_range(now(), &k, 0, len).unwrap();
        let other = b.get_range(now(), &k, 1, len).unwrap();
        assert_eq!(one, again);
        assert_ne!(one.header("authorization"), other.header("authorization"));
    }

    #[test]
    fn every_header_sent_is_signed() {
        for builder in [minio(), builder("https://fly.storage.tigris.dev")] {
            for req in every_request(&builder) {
                let signed = req.signed_headers();
                assert!(signed.contains(&"host"));
                for (name, _) in req.headers() {
                    assert!(
                        *name == "authorization" || signed.contains(name),
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

    /// A body read while it is sent signs the hash it was given, and says so in its header;
    /// a buffered one signs its own. Neither carries `Content-MD5`, as Go's do not.
    #[test]
    fn a_streamed_write_signs_the_hash_it_was_given() {
        let b = minio();
        let k = key(1, 1, 5);
        let hash = PayloadHash::of(b"the part");
        let part = b
            .upload_part_of(now(), &k, &upload(), PartNumber::new(2).unwrap(), (8, hash))
            .unwrap();
        let put = b
            .put_object_of(now(), &k, (8, hash), now(), Precondition::KeyIsFree)
            .unwrap();
        for write in [part, put] {
            assert!(write.body().is_empty());
            assert_eq!(write.header("content-length"), Some("8"));
            assert_eq!(
                write.header("x-amz-content-sha256"),
                Some(hash.to_hex().as_str())
            );
            assert_eq!(write.header("content-md5"), None);
            let mut sent = write.clone();
            sent.set_body(b"the part".to_vec());
            let auth = write.header("authorization").unwrap();
            assert_eq!(resign(&sent, sent.url(), &sent_headers(&sent)), auth);
        }
    }

    #[test]
    fn put_carries_length_hash_and_created_at() {
        let k = key(0, 1, 1);
        let created_at = Timestamp::from_unix_millis(1_795_182_585_566);
        let req = minio()
            .put_object(now(), &k, b"hello".to_vec(), created_at)
            .unwrap();
        assert_eq!(req.method(), Method::Put);
        assert_eq!(req.header("content-length"), Some("5"));
        assert_eq!(
            req.header("x-amz-content-sha256"),
            Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
        assert_eq!(req.header("content-md5"), None);
        assert_eq!(
            req.header(CREATED_AT_HEADER),
            Some("2026-11-20T13:49:45.566Z")
        );
        assert_eq!(req.body(), b"hello");
        let empty = minio()
            .put_object(now(), &k, Vec::new(), created_at)
            .unwrap();
        assert_eq!(empty.header("content-length"), Some("0"));
        assert_eq!(
            empty.header("x-amz-content-sha256"),
            Some(PayloadHash::EMPTY.to_hex().as_str())
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
        assert_eq!(part.header("content-md5"), None);
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
            for text in ["p/q", ""] {
                let prefix = Prefix::new(text).unwrap();
                let k = key(level, min, max);
                let full = object_key(&prefix, &k);
                let name = full.strip_prefix(level_prefix(&prefix, k.level()).as_str()).unwrap();
                prop_assert_eq!(parse_file_name(name).unwrap(), k.range());
                // The empty prefix is the same key under a leading slash, not a different shape.
                prop_assert_eq!(full, format!("{text}/{level:04x}/{}", k.file_name()));
            }
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
