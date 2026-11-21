//! An in-memory S3 that answers the nine request shapes the store builds.
//!
//! It reproduces what the MinIO spike measured (docs/spikes.md): a wrong
//! Content-MD5 is 400 BadDigest and a malformed one 400 InvalidDigest; any
//! range on a zero-byte object, or one starting at or past the end, is 416
//! InvalidRange, while a range that straddles the end is clipped to a 206; a
//! listing url-encodes its keys; of two multipart uploads to one key the last
//! one completed survives; a quiet DeleteObjects of a missing key reports
//! nothing. A multipart upload's non-final parts under 5 MiB are refused
//! (EntityTooSmall), as S3 does.
//!
//! It does not check signatures. It checks the signing rule MinIO enforces both
//! ways: every header sent must be named in `X-Amz-SignedHeaders`, and every
//! header named there (but `host`) must be sent, else 400 AccessDenied.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::ops::Bound;
use std::sync::{Mutex, MutexGuard, PoisonError};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use lsr_core::Clock;
use lsr_ltx::Timestamp;
use md5::{Digest as _, Md5};
use percent_encoding::percent_decode_str;

use crate::config::{S3Config, UrlStyle};
use crate::fault::{Shape, error_response};
use crate::request::HttpRequest;
use crate::response::{hex, quoted_md5};
use crate::transport::{HttpResponse, Transport, TransportError};

/// S3's smallest part, the last one excepted.
const MIN_PART: usize = 5 * 1024 * 1024;

/// ListObjectsV2's ceiling and default.
const MAX_KEYS: usize = 1000;

const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

const METADATA_PREFIX: &str = "x-amz-meta-";

#[derive(Clone)]
struct Object {
    body: Vec<u8>,
    etag: String,
    meta: Vec<(String, String)>,
    modified: Timestamp,
}

struct Pending {
    key: String,
    /// When CreateMultipartUpload ran: S3's `LastModified` for the finished object (ruling Q3).
    initiated: Timestamp,
    meta: Vec<(String, String)>,
    parts: BTreeMap<u16, Vec<u8>>,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<String, Object>,
    uploads: BTreeMap<String, Pending>,
    next_upload: u64,
    denied_deletes: BTreeSet<String>,
}

/// An in-memory bucket behind the [`Transport`] trait.
pub struct FakeS3 {
    bucket: String,
    style: UrlStyle,
    clock: Box<dyn Clock>,
    state: Mutex<State>,
}

impl fmt::Debug for FakeS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeS3")
            .field("bucket", &self.bucket)
            .field("objects", &self.state().objects.len())
            .finish_non_exhaustive()
    }
}

impl FakeS3 {
    /// An empty bucket named and addressed as `config` says. `clock` created-at times
    /// each object's `LastModified`.
    #[must_use]
    pub fn new(config: &S3Config, clock: impl Clock + 'static) -> Self {
        Self {
            bucket: config.bucket().as_str().to_owned(),
            style: config.style(),
            clock: Box::new(clock),
            state: Mutex::new(State::default()),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stores an object directly: a stray name, or an LTX file written without
    /// metadata. `meta` holds `(name, value)` pairs with names as sent, such
    /// as `x-amz-meta-litestream-timestamp`.
    pub fn put_raw(&self, key: &str, body: Vec<u8>, meta: &[(&str, &str)]) {
        let object = Object {
            etag: quoted_md5(&body),
            body,
            meta: meta
                .iter()
                .map(|(name, value)| (name.to_ascii_lowercase(), (*value).to_owned()))
                .collect(),
            modified: self.clock.now(),
        };
        self.state().objects.insert(key.to_owned(), object);
    }

    /// Sets what LIST and HEAD report as an object's last-modified time, which
    /// is not the stored created-at time and may be anything.
    pub fn set_last_modified(&self, key: &str, at: Timestamp) {
        if let Some(object) = self.state().objects.get_mut(key) {
            object.modified = at;
        }
    }

    /// An object's bytes, `None` when absent.
    #[must_use]
    pub fn body_of(&self, key: &str) -> Option<Vec<u8>> {
        self.state()
            .objects
            .get(key)
            .map(|object| object.body.clone())
    }

    /// Every key, in listing order.
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.state().objects.keys().cloned().collect()
    }

    /// How many multipart uploads are begun and neither completed nor aborted.
    #[must_use]
    pub fn open_uploads(&self) -> usize {
        self.state().uploads.len()
    }

    /// Makes DeleteObjects report `AccessDenied` for `key` and leave it in place.
    pub fn deny_delete(&self, key: &str) {
        self.state().denied_deletes.insert(key.to_owned());
    }

    fn handle(&self, request: &HttpRequest) -> HttpResponse {
        if let Err(refused) = check_signed_headers(request) {
            return refused;
        }
        let Some(shape) = Shape::of(request) else {
            return error_response(400, Some("InvalidRequest"), None);
        };
        let Some(key) = self.target_key(request) else {
            return error_response(404, Some("NoSuchBucket"), None);
        };
        let mut state = self.state();
        match shape {
            Shape::List => self.list(&state, request),
            Shape::Head => head(&state, &key),
            Shape::Get => get(&state, request, &key),
            Shape::Put => self.put(&mut state, request, key),
            Shape::CreateMultipart => {
                create(&mut state, request, &self.bucket, key, self.clock.now())
            }
            Shape::UploadPart => upload_part(&mut state, request, &key),
            Shape::Complete => self.complete(&mut state, request, &key),
            Shape::Abort => abort(&mut state, request),
            Shape::DeleteObjects => delete_objects(&mut state, request),
        }
    }

    /// The object key the URL names, `None` when it names another bucket.
    fn target_key(&self, request: &HttpRequest) -> Option<String> {
        let path = request.url().path().strip_prefix('/')?;
        let encoded = match self.style {
            UrlStyle::Path => {
                let (bucket, rest) = path.split_once('/').unwrap_or((path, ""));
                (bucket == self.bucket).then_some(rest)?
            }
            UrlStyle::VirtualHost => path,
        };
        Some(percent_decode_str(encoded).decode_utf8().ok()?.into_owned())
    }

    fn put(&self, state: &mut State, request: &HttpRequest, key: String) -> HttpResponse {
        if let Err(refused) = check_md5(request, true) {
            return refused;
        }
        let object = Object {
            etag: quoted_md5(request.body()),
            body: request.body().to_vec(),
            meta: metadata(request),
            modified: self.clock.now(),
        };
        let etag = object.etag.clone();
        state.objects.insert(key, object);
        reply(200, vec![("etag", etag)], Vec::new())
    }

    fn complete(&self, state: &mut State, request: &HttpRequest, key: &str) -> HttpResponse {
        let Some(id) = query(request, "uploadId") else {
            return error_response(400, Some("InvalidArgument"), None);
        };
        let Some(upload) = state.uploads.get(&id).filter(|up| up.key == key) else {
            return error_response(404, Some("NoSuchUpload"), None);
        };
        let listed = parse_parts(request.body());
        let joined = match join_parts(upload, &listed) {
            Ok(joined) => joined,
            Err(refused) => return refused,
        };
        let object = Object {
            etag: joined.etag.clone(),
            body: joined.body,
            meta: upload.meta.clone(),
            modified: upload.initiated,
        };
        state.objects.insert(key.to_owned(), object);
        state.uploads.remove(&id);
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult xmlns=\"{NS}\"><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
            xml_escape(&self.bucket),
            xml_escape(key),
            joined.etag.replace('"', "&#34;"),
        );
        reply(200, vec![("etag", joined.etag)], xml.into_bytes())
    }

    fn list(&self, state: &State, request: &HttpRequest) -> HttpResponse {
        let prefix = query(request, "prefix").unwrap_or_default();
        let max = query(request, "max-keys")
            .and_then(|text| text.parse::<usize>().ok())
            .map_or(MAX_KEYS, |asked| asked.min(MAX_KEYS));
        let after = match query(request, "continuation-token") {
            Some(token) => match decode_token(&token) {
                Some(key) => Some(key),
                None => return error_response(400, Some("InvalidArgument"), None),
            },
            None => query(request, "start-after"),
        };
        let from = after.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut hits = state
            .objects
            .range::<str, _>((from, Bound::Unbounded))
            .filter(|(key, _)| key.starts_with(&prefix));
        let page: Vec<_> = hits.by_ref().take(max).collect();
        let more = hits.next().is_some();
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"{NS}\"><Name>{}</Name><KeyCount>{}</KeyCount><MaxKeys>{max}</MaxKeys><EncodingType>url</EncodingType><IsTruncated>{more}</IsTruncated>",
            xml_escape(&self.bucket),
            page.len(),
        );
        if let (true, Some((last, _))) = (more, page.last()) {
            xml.push_str(&format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                hex(last.as_bytes())
            ));
        }
        for (key, object) in &page {
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                url_encode(key),
                whole_seconds(object.modified),
                object.etag.replace('"', "&#34;"),
                object.body.len(),
            ));
        }
        xml.push_str("</ListBucketResult>");
        reply(200, Vec::new(), xml.into_bytes())
    }
}

impl Transport for FakeS3 {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        Ok(self.handle(&request))
    }
}

fn reply(status: u16, headers: Vec<(&str, String)>, body: Vec<u8>) -> HttpResponse {
    let headers = headers
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect();
    HttpResponse::new(status, headers, body, true)
}

fn query(request: &HttpRequest, name: &str) -> Option<String> {
    request
        .url()
        .query_pairs()
        .find(|(have, _)| have == name)
        .map(|(_, value)| value.into_owned())
}

fn metadata(request: &HttpRequest) -> Vec<(String, String)> {
    request
        .headers()
        .iter()
        .filter(|(name, _)| name.starts_with(METADATA_PREFIX))
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

/// MinIO's signing rule, both directions (docs/spikes.md).
fn check_signed_headers(request: &HttpRequest) -> Result<(), HttpResponse> {
    let denied = || error_response(400, Some("AccessDenied"), None);
    let signed = query(request, "X-Amz-SignedHeaders").ok_or_else(denied)?;
    let named: BTreeSet<&str> = signed.split(';').collect();
    if request
        .headers()
        .iter()
        .any(|(name, _)| !named.contains(name))
    {
        return Err(denied());
    }
    if named
        .iter()
        .any(|name| *name != "host" && request.header(name).is_none())
    {
        return Err(denied());
    }
    Ok(())
}

/// Checks `Content-MD5` against the body. A write need not carry one; a
/// DeleteObjects must (`required`).
fn check_md5(request: &HttpRequest, optional: bool) -> Result<(), HttpResponse> {
    let Some(text) = request.header("content-md5") else {
        return if optional {
            Ok(())
        } else {
            Err(error_response(400, Some("InvalidRequest"), None))
        };
    };
    let sent = STANDARD
        .decode(text)
        .ok()
        .filter(|bytes| bytes.len() == 16)
        .ok_or_else(|| error_response(400, Some("InvalidDigest"), None))?;
    if sent.as_slice() == Md5::digest(request.body()).as_slice() {
        Ok(())
    } else {
        Err(error_response(400, Some("BadDigest"), None))
    }
}

/// S3 lists `LastModified` to the second, so it never equals a millisecond created-at time.
fn whole_seconds(at: Timestamp) -> Timestamp {
    Timestamp::from_unix_millis(at.unix_millis().div_euclid(1000).saturating_mul(1000))
}

/// A HEAD's `Last-Modified` is an HTTP-date (`Fri, 20 Nov 2026 13:49:45 GMT`).
fn http_date(at: Timestamp) -> String {
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let iso = whole_seconds(at).to_string();
    let field = |from: usize, to: usize| iso.get(from..to).unwrap_or("00");
    // 1970-01-01 was a Thursday.
    let days = at.unix_millis().div_euclid(86_400_000);
    let weekday = usize::try_from(days.saturating_add(4).rem_euclid(7))
        .ok()
        .and_then(|day| DAYS.get(day));
    let month = field(5, 7)
        .parse::<usize>()
        .ok()
        .and_then(|month| MONTHS.get(month.saturating_sub(1)));
    format!(
        "{}, {} {} {} {} GMT",
        weekday.unwrap_or(&"Thu"),
        field(8, 10),
        month.unwrap_or(&"Jan"),
        field(0, 4),
        field(11, 19),
    )
}

fn object_headers(object: &Object, length: usize) -> Vec<(&str, String)> {
    let mut headers = vec![
        ("etag", object.etag.clone()),
        ("content-length", length.to_string()),
        ("last-modified", http_date(object.modified)),
    ];
    headers.extend(
        object
            .meta
            .iter()
            .map(|(name, value)| (name.as_str(), value.clone())),
    );
    headers
}

fn head(state: &State, key: &str) -> HttpResponse {
    match state.objects.get(key) {
        Some(object) => reply(200, object_headers(object, object.body.len()), Vec::new()),
        None => error_response(404, None, None),
    }
}

fn get(state: &State, request: &HttpRequest, key: &str) -> HttpResponse {
    let Some(object) = state.objects.get(key) else {
        return error_response(404, Some("NoSuchKey"), None);
    };
    let Some(range) = request.header("range") else {
        return reply(
            200,
            object_headers(object, object.body.len()),
            object.body.clone(),
        );
    };
    serve_range(object, range)
}

fn serve_range(object: &Object, range: &str) -> HttpResponse {
    let unsatisfiable = || error_response(416, Some("InvalidRange"), None);
    let Some((start, end)) = parse_range(range) else {
        return unsatisfiable();
    };
    let size = u64::try_from(object.body.len()).unwrap_or(u64::MAX);
    if start >= size {
        return unsatisfiable();
    }
    let last = end.unwrap_or(u64::MAX).min(size.saturating_sub(1));
    let (Ok(from), Ok(to)) = (usize::try_from(start), usize::try_from(last)) else {
        return unsatisfiable();
    };
    let Some(slice) = object.body.get(from..=to) else {
        return unsatisfiable();
    };
    let mut headers = object_headers(object, slice.len());
    headers.push(("content-range", format!("bytes {start}-{last}/{size}")));
    reply(206, headers, slice.to_vec())
}

fn parse_range(text: &str) -> Option<(u64, Option<u64>)> {
    let (start, end) = text.strip_prefix("bytes=")?.split_once('-')?;
    let end = if end.is_empty() {
        None
    } else {
        Some(end.parse().ok()?)
    };
    Some((start.parse().ok()?, end))
}

fn create(
    state: &mut State,
    request: &HttpRequest,
    bucket: &str,
    key: String,
    initiated: Timestamp,
) -> HttpResponse {
    state.next_upload = state.next_upload.saturating_add(1);
    let id = format!("upload-{}", state.next_upload);
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult xmlns=\"{NS}\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(&key),
    );
    state.uploads.insert(
        id,
        Pending {
            key,
            initiated,
            meta: metadata(request),
            parts: BTreeMap::new(),
        },
    );
    reply(200, Vec::new(), xml.into_bytes())
}

fn upload_part(state: &mut State, request: &HttpRequest, key: &str) -> HttpResponse {
    let number = query(request, "partNumber").and_then(|text| text.parse::<u16>().ok());
    let Some(number) = number.filter(|n| (1..=10_000).contains(n)) else {
        return error_response(400, Some("InvalidArgument"), None);
    };
    let id = query(request, "uploadId").unwrap_or_default();
    let Some(upload) = state.uploads.get_mut(&id).filter(|up| up.key == key) else {
        return error_response(404, Some("NoSuchUpload"), None);
    };
    if let Err(refused) = check_md5(request, true) {
        return refused;
    }
    upload.parts.insert(number, request.body().to_vec());
    reply(200, vec![("etag", quoted_md5(request.body()))], Vec::new())
}

fn abort(state: &mut State, request: &HttpRequest) -> HttpResponse {
    let id = query(request, "uploadId").unwrap_or_default();
    match state.uploads.remove(&id) {
        Some(_) => reply(204, Vec::new(), Vec::new()),
        None => error_response(404, Some("NoSuchUpload"), None),
    }
}

struct Joined {
    body: Vec<u8>,
    etag: String,
}

/// The `(part number, ETag)` pairs a CompleteMultipartUpload body lists.
fn parse_parts(body: &[u8]) -> Vec<(u16, String)> {
    let text = String::from_utf8_lossy(body);
    text.split("<Part>")
        .skip(1)
        .filter_map(|part| {
            let number = between(part, "<PartNumber>", "</PartNumber>")?
                .trim()
                .parse()
                .ok()?;
            let etag = xml_unescape(between(part, "<ETag>", "</ETag>")?);
            Some((number, etag.trim_matches('"').to_owned()))
        })
        .collect()
}

/// Validates a completion against the stored parts and concatenates them.
fn join_parts(upload: &Pending, listed: &[(u16, String)]) -> Result<Joined, HttpResponse> {
    let invalid = |code| error_response(400, Some(code), None);
    if listed.is_empty() || listed.windows(2).any(|pair| pair.first() >= pair.get(1)) {
        return Err(invalid("InvalidPartOrder"));
    }
    let mut body = Vec::new();
    let mut digests = Vec::new();
    for (position, (number, etag)) in listed.iter().enumerate() {
        let part = upload
            .parts
            .get(number)
            .ok_or_else(|| invalid("InvalidPart"))?;
        let digest = Md5::digest(part);
        if hex(&digest) != *etag {
            return Err(invalid("InvalidPart"));
        }
        let is_last = position.saturating_add(1) == listed.len();
        if !is_last && part.len() < MIN_PART {
            return Err(invalid("EntityTooSmall"));
        }
        body.extend_from_slice(part);
        digests.extend_from_slice(&digest);
    }
    let etag = format!("\"{}-{}\"", hex(&Md5::digest(&digests)), listed.len());
    Ok(Joined { body, etag })
}

fn delete_objects(state: &mut State, request: &HttpRequest) -> HttpResponse {
    if let Err(refused) = check_md5(request, false) {
        return refused;
    }
    let text = String::from_utf8_lossy(request.body());
    let quiet = text.contains("<Quiet>true</Quiet>");
    let mut xml =
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"{NS}\">");
    for entry in text.split("<Object>").skip(1) {
        let Some(raw) = between(entry, "<Key>", "</Key>") else {
            return error_response(400, Some("MalformedXML"), None);
        };
        let key = xml_unescape(raw);
        let shown = xml_escape(&key);
        if state.denied_deletes.contains(&key) {
            xml.push_str(&format!(
                "<Error><Key>{shown}</Key><Code>AccessDenied</Code><Message>Access Denied.</Message></Error>"
            ));
            continue;
        }
        state.objects.remove(&key);
        if !quiet {
            xml.push_str(&format!("<Deleted><Key>{shown}</Key></Deleted>"));
        }
    }
    xml.push_str("</DeleteResult>");
    reply(200, Vec::new(), xml.into_bytes())
}

fn decode_token(token: &str) -> Option<String> {
    let bytes: Option<Vec<u8>> = token
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

/// Go's `url.QueryEscape` with `/` kept, which is how MinIO writes keys under
/// `encoding-type=url`: a space is `+`, a literal `+` is `%2B`.
fn url_encode(key: &str) -> String {
    let mut out = String::new();
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(char::from(byte));
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn between<'a>(text: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let (_, rest) = text.split_once(open)?;
    rest.split_once(close).map(|(inner, _)| inner)
}

fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn xml_unescape(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some((head, tail)) = rest.split_once('&') {
        out.push_str(head);
        match tail.split_once(';') {
            Some((entity, after)) => {
                out.push_str(&entity_text(entity));
                rest = after;
            }
            None => {
                out.push('&');
                rest = tail;
            }
        }
    }
    out.push_str(rest);
    out
}

fn entity_text(entity: &str) -> String {
    let numeric =
        entity
            .strip_prefix('#')
            .and_then(|digits| match digits.strip_prefix(['x', 'X']) {
                Some(hexits) => u32::from_str_radix(hexits, 16).ok(),
                None => digits.parse().ok(),
            });
    match (entity, numeric.and_then(char::from_u32)) {
        (_, Some(found)) => found.to_string(),
        ("amp", None) => "&".to_owned(),
        ("lt", None) => "<".to_owned(),
        ("gt", None) => ">".to_owned(),
        ("quot", None) => "\"".to_owned(),
        ("apos", None) => "'".to_owned(),
        (other, None) => format!("&{other};"),
    }
}
