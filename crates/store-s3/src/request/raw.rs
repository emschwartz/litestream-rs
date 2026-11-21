//! Test-only: requests the store never makes, signed exactly as the store signs its own
//! (the same credentials, base headers and the provider's consistency header), for suites
//! against a real server; and the listing of open multipart uploads a cleanup needs, which no
//! store call does. The S3 conformance suite and the Tigris runs of `lsr` share these, so their
//! signing cannot drift apart.

use lsr_core::Clock;
use lsr_ltx::Timestamp;
use thiserror::Error;

use super::{EXPIRES_IN, HttpRequest, Method, RequestBuilder, RequestError, UploadId};
use crate::response::parse_create;
use crate::transport::{HttpResponse, Transport, TransportError};

/// The most pages of open uploads [`open_uploads`] follows before it gives up.
pub const MAX_UPLOAD_PAGES: usize = 100;

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

impl RequestBuilder {
    /// A signed request the builder has no call for: any method and query, `headers`
    /// after the store's own. `object` is a full key; `None` addresses the bucket.
    ///
    /// # Errors
    /// [`RequestError::ClockOutOfRange`].
    pub fn raw(
        &self,
        now: Timestamp,
        method: Method,
        object: Option<&str>,
        query: &[(&str, &str)],
        headers: Vec<(&'static str, String)>,
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
}

/// Why a raw request or the upload listing failed.
#[derive(Debug, Error)]
pub enum RawError {
    /// The request could not be signed.
    #[error("{0}")]
    Request(#[from] RequestError),
    /// No response arrived.
    #[error("{0}")]
    Transport(#[from] TransportError),
    /// The server answered outside 2xx.
    #[error("status {status}")]
    Status {
        /// The status.
        status: u16,
    },
    /// The listing is not the XML expected.
    #[error("the multipart upload listing is malformed")]
    Malformed,
    /// More than [`MAX_UPLOAD_PAGES`] pages of open uploads.
    #[error("more than {MAX_UPLOAD_PAGES} pages of open uploads")]
    TooManyPages,
}

/// One open multipart upload: its full key and id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenUpload {
    /// The full key.
    pub key: String,
    /// The upload's id.
    pub id: UploadId,
}

/// Sends a raw request and requires a 2xx answer.
///
/// # Errors
/// [`RawError`].
pub fn send_raw(
    transport: &dyn Transport,
    builder: &RequestBuilder,
    clock: &dyn Clock,
    method: Method,
    object: Option<&str>,
    query: &[(&str, &str)],
    body: Vec<u8>,
) -> Result<HttpResponse, RawError> {
    let request = builder.raw(clock.now(), method, object, query, Vec::new(), body)?;
    let response = transport.send(request)?;
    if (200..300).contains(&response.status()) {
        Ok(response)
    } else {
        Err(RawError::Status {
            status: response.status(),
        })
    }
}

/// Every multipart upload open under `prefix` (a full key prefix), following the listing's
/// markers. Keys outside `prefix` are dropped, in case a server ignores it.
///
/// # Errors
/// [`RawError`].
pub fn open_uploads(
    transport: &dyn Transport,
    builder: &RequestBuilder,
    clock: &dyn Clock,
    prefix: &str,
) -> Result<Vec<OpenUpload>, RawError> {
    let mut open = Vec::new();
    let mut markers: Option<(String, String)> = None;
    for _page in 0..MAX_UPLOAD_PAGES {
        let mut query = vec![("uploads", ""), ("prefix", prefix)];
        if let Some((key, id)) = &markers {
            query.push(("key-marker", key.as_str()));
            query.push(("upload-id-marker", id.as_str()));
        }
        let response = send_raw(
            transport,
            builder,
            clock,
            Method::Get,
            None,
            &query,
            Vec::new(),
        )?;
        let xml = std::str::from_utf8(response.body()).map_err(|_bytes| RawError::Malformed)?;
        for upload in xml_text(xml, "Upload") {
            let key = xml_text(&upload, "Key").pop().ok_or(RawError::Malformed)?;
            let id = xml_text(&upload, "UploadId")
                .pop()
                .ok_or(RawError::Malformed)?;
            if key.starts_with(prefix) {
                open.push(OpenUpload {
                    key,
                    id: UploadId::new(id),
                });
            }
        }
        if xml_text(xml, "IsTruncated").first().map(String::as_str) != Some("true") {
            return Ok(open);
        }
        let next = |tag: &str| xml_text(xml, tag).pop().ok_or(RawError::Malformed);
        markers = Some((next("NextKeyMarker")?, next("NextUploadIdMarker")?));
    }
    Err(RawError::TooManyPages)
}

/// Aborts one open upload.
///
/// # Errors
/// [`RawError`].
pub fn abort_upload(
    transport: &dyn Transport,
    builder: &RequestBuilder,
    clock: &dyn Clock,
    upload: &OpenUpload,
) -> Result<(), RawError> {
    let query = [("uploadId", upload.id.as_str())];
    let key = Some(upload.key.as_str());
    send_raw(
        transport,
        builder,
        clock,
        Method::Delete,
        key,
        &query,
        Vec::new(),
    )
    .map(drop)
}

/// Starts a multipart upload of `key` (a full key), sends one part and leaves it open, as
/// a process stopped mid-upload leaves one.
///
/// # Errors
/// [`RawError`].
pub fn abandon_upload(
    transport: &dyn Transport,
    builder: &RequestBuilder,
    clock: &dyn Clock,
    key: &str,
    part: Vec<u8>,
) -> Result<OpenUpload, RawError> {
    let query = [("uploads", "")];
    let created = send_raw(
        transport,
        builder,
        clock,
        Method::Post,
        Some(key),
        &query,
        Vec::new(),
    )?;
    let id = parse_create(&created).map_err(|_xml| RawError::Malformed)?;
    let query = [("partNumber", "1"), ("uploadId", id.as_str())];
    send_raw(
        transport,
        builder,
        clock,
        Method::Put,
        Some(key),
        &query,
        part,
    )?;
    Ok(OpenUpload {
        key: key.to_owned(),
        id,
    })
}

/// The text of each `<tag>` element in `xml`, unescaped; enough for S3's flat listings.
#[must_use]
pub fn xml_text(xml: &str, tag: &str) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BucketName, Endpoint, Prefix, S3Config};
    use crate::credentials::{S3Credentials, SecretString};

    /// A raw request carries the store's own headers, each one signed, as the store's
    /// requests do; the query reaches the URL whatever order it was given in.
    #[test]
    fn a_raw_request_is_signed_as_the_store_signs() {
        let config = S3Config::new(
            Endpoint::new("https://fly.storage.tigris.dev").unwrap(),
            BucketName::new("b").unwrap(),
            Prefix::new("p").unwrap(),
        );
        let credentials = S3Credentials::new("AK".to_owned(), SecretString::new("s".to_owned()));
        let builder = RequestBuilder::new(&config, &credentials).unwrap();
        let request = builder
            .raw(
                Timestamp::from_unix_millis(1_700_000_000_000),
                Method::Get,
                None,
                &[("uploads", ""), ("prefix", "p/")],
                vec![("x-extra", "1".to_owned())],
                Vec::new(),
            )
            .unwrap();
        assert_eq!(request.header("x-tigris-consistent"), Some("true"));
        assert_eq!(request.header("x-extra"), Some("1"));
        let signed = request
            .url()
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .unwrap();
        for (name, _) in request.headers() {
            assert!(
                signed.split(';').any(|s| s == *name),
                "{name} unsigned: {signed}"
            );
        }
        let query = request.url().query().unwrap();
        assert!(
            query.contains("prefix=p%2F") && query.contains("uploads"),
            "{query}"
        );
    }

    #[test]
    fn xml_text_reads_each_element_unescaped() {
        let xml = "<a><Key>x&amp;y</Key></a><a><Key>&lt;z&gt;</Key></a><Key>unclosed";
        assert_eq!(xml_text(xml, "Key"), ["x&y", "<z>"]);
        assert!(xml_text(xml, "Missing").is_empty());
    }
}
