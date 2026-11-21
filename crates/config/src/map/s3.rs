//! The one reading of an `s3://` replica URL, for the daemon's config and for the
//! command line's `--replica`: Go's `ParseReplicaURLWithQuery` and `s3.ParseHost`
//! (`replica_url.go`, `s3/replica_client.go:1820-1871` at 041a8c3), then Go's
//! precedence of `NewS3ReplicaClientFromConfig` (`cmd/litestream/main.go:1540-1640`).
//!
//! **Host.** The bucket, as Go's `ParseHost` reads it for plain names, AWS
//! virtual hosts (`bucket.s3.region.amazonaws.com`), and MinIO's `bucket.host:port`
//! or a bare `host:port` (an `http://` endpoint, path style). Another provider's
//! host spelling (DigitalOcean, Backblaze, Filebase, Scaleway, Neon) and access
//! point ARNs are refused with a pointer to `bucket` plus `endpoint`, since v1 does
//! not derive their endpoints. Userinfo is refused: credentials never come from a
//! URL (Go ignores it, which would quietly use other credentials).
//!
//! **Query.** `endpoint`, `region`, `forcePathStyle` (and `force-path-style`),
//! `partSize` (`part-size`) and `concurrency` are honoured; `skipVerify=true` and
//! a `storageClass` are refused; the signing and MD5 keys are accepted with a
//! note, since they change nothing in v1. Go's `concurrency` is the parts of one
//! upload in flight; here it bounds every batch of requests the store sends
//! (parts, reads ahead, listing levels, stamps), and zero leaves the default. A key Go does not read is refused.
//!
//! **Precedence.** [`S3Overrides`] (a replica entry's keys, or the command line's
//! flags) win over the URL's parts and query. Path style follows Go: on when an
//! endpoint is given, unless set; otherwise as the provider needs.
//!
//! No problem holds the URL, its userinfo or a query value.

use std::num::NonZeroUsize;

use lsr_store_s3::{BucketName, Endpoint, PartSize, Prefix, Region, S3Config, UrlStyle};
use percent_encoding::percent_decode_str;
use url::Url;

use super::ReplicaProblem;
use crate::expand::clean;

/// What an `s3://` URL says, before anything overrides it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct S3Url {
    bucket: Option<String>,
    path: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    path_style: Option<bool>,
    path_style_set: bool,
    part_size: Option<u64>,
    concurrency: Option<u64>,
    notes: Vec<&'static str>,
}

impl S3Url {
    /// Reads the URL's host, path and query, as Go does.
    ///
    /// # Errors
    /// A [`ReplicaProblem`] naming what is wrong, never any part of the URL but a query key.
    pub fn parse(text: &str) -> Result<Self, ReplicaProblem> {
        if text.to_ascii_lowercase().starts_with("s3://arn:") {
            return Err(ReplicaProblem::AccessPoint);
        }
        let url = Url::parse(text).map_err(|_unparsed| ReplicaProblem::BadUrl)?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(ReplicaProblem::UserInfo);
        }
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or("")),
            None => url.host_str().unwrap_or("").to_owned(),
        };
        let mut out = parse_host(&host)?;
        let path = percent_decode_str(url.path())
            .decode_utf8()
            .map(|p| clean(&p))
            .map_err(|_bytes| ReplicaProblem::BadUrl)?;
        let path = path.trim_start_matches('/');
        out.path = (!path.is_empty() && path != ".").then(|| path.to_owned());
        for (name, value) in url.query_pairs() {
            if let Some(note) = read_query(&name, &value, &mut out)? {
                out.notes.push(note);
            }
        }
        Ok(out)
    }

    /// The query keys accepted that change nothing in v1, one line each.
    #[must_use]
    pub fn notes(&self) -> &[&'static str] {
        &self.notes
    }
}

/// The settings that win over a URL's: a replica entry's keys, or the command line's
/// flags. Empty text counts as unset, as Go's empty strings do.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct S3Overrides<'a> {
    /// `bucket`.
    pub bucket: Option<&'a str>,
    /// `path`, the key prefix.
    pub path: Option<&'a str>,
    /// `region`.
    pub region: Option<&'a str>,
    /// `endpoint`; a bare host gets Go's scheme.
    pub endpoint: Option<&'a str>,
    /// `force-path-style`.
    pub force_path_style: Option<bool>,
    /// `part-size`, in bytes.
    pub part_size: Option<u64>,
    /// `concurrency`: requests in flight per batch; zero is unset.
    pub concurrency: Option<u64>,
}

/// The store's configuration from a URL (if any) and the settings that override it.
///
/// # Errors
/// [`ReplicaProblem::BucketRequired`] when neither names a bucket, or
/// [`ReplicaProblem::S3`] when the store refuses the result.
pub fn s3_config(url: Option<S3Url>, over: &S3Overrides<'_>) -> Result<S3Config, ReplicaProblem> {
    let url = url.unwrap_or_default();
    let text = |own: Option<&str>, url: Option<String>| {
        own.map(str::to_owned).filter(|s| !s.is_empty()).or(url)
    };
    let bucket = text(over.bucket, url.bucket).ok_or(ReplicaProblem::BucketRequired)?;
    let path = text(over.path, url.path).unwrap_or_default();
    let region = text(over.region, url.region);
    let endpoint = text(over.endpoint, url.endpoint).map(|e| ensure_scheme(&e));
    // Go: path style when an endpoint is given; the override, else the URL's, wins.
    let (style, style_set) = match (over.force_path_style, url.path_style) {
        (Some(own), _) => (own, true),
        (None, Some(from_url)) => (from_url || over.endpoint.is_some(), url.path_style_set),
        (None, None) => (endpoint.is_some(), false),
    };
    let config =
        build(&bucket, &path, region, endpoint, (style, style_set)).map_err(ReplicaProblem::S3)?;
    let config = match over.part_size.or(url.part_size) {
        None => config,
        Some(bytes) => PartSize::new(bytes)
            .map(|size| config.with_part_size(size))
            .map_err(ReplicaProblem::S3)?,
    };
    let concurrency = over
        .concurrency
        .or(url.concurrency)
        .and_then(|n| usize::try_from(n).ok())
        .and_then(NonZeroUsize::new);
    Ok(match concurrency {
        Some(n) => config.with_concurrency(n),
        None => config,
    })
}

fn build(
    bucket: &str,
    path: &str,
    region: Option<String>,
    endpoint: Option<String>,
    (path_style, set): (bool, bool),
) -> Result<S3Config, lsr_store_s3::ConfigError> {
    let region = Region::new(region.as_deref().unwrap_or(lsr_store_s3::DEFAULT_REGION))?;
    let endpoint = match endpoint {
        Some(text) => Endpoint::new(&text)?,
        None => Endpoint::new(&format!("https://s3.{}.amazonaws.com", region.as_str()))?,
    };
    let config = S3Config::new(endpoint, BucketName::new(bucket)?, Prefix::new(path)?);
    // Unset, the provider's own need (MinIO, an address) still turns path style on, as Go's.
    let path_style = path_style || (!set && config.style() == UrlStyle::Path);
    Ok(config.with_region(region).with_style(if path_style {
        UrlStyle::Path
    } else {
        UrlStyle::VirtualHost
    }))
}

/// Go's `EnsureEndpointScheme`: http for local hosts, https otherwise.
pub(super) fn ensure_scheme(endpoint: &str) -> String {
    let lower = endpoint.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") {
        return endpoint.to_owned();
    }
    let host = endpoint.split(['/', ':']).next().unwrap_or("");
    let local = host == "localhost"
        || host == "127.0.0.1"
        || host.ends_with(".local")
        || host.ends_with(".localhost")
        || [
            "192.168.", "10.", "172.16.", "172.17.", "172.18.", "172.19.", "172.2", "172.30.",
            "172.31.",
        ]
        .iter()
        .any(|prefix| host.starts_with(prefix));
    format!("{}://{endpoint}", if local { "http" } else { "https" })
}

/// Applies one query key to `out`; a note when it has no effect in v1.
fn read_query(
    name: &str,
    value: &str,
    out: &mut S3Url,
) -> Result<Option<&'static str>, ReplicaProblem> {
    let not_in_v1 = |why: &'static str| ReplicaProblem::QueryNotInV1 {
        name: name.to_owned(),
        why,
    };
    let truthy = matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "1" | "t" | "yes"
    );
    match name {
        "endpoint" if !value.is_empty() => {
            out.endpoint = Some(ensure_scheme(value));
            if !out.path_style_set {
                out.path_style = Some(true);
            }
        }
        "region" if !value.is_empty() => out.region = Some(value.to_owned()),
        "forcePathStyle" | "force-path-style" if !value.is_empty() => {
            out.path_style = Some(truthy);
            out.path_style_set = true;
        }
        "skipVerify" if value == "true" => {
            return Err(not_in_v1("v1 always verifies TLS certificates"));
        }
        "storageClass" | "storage-class" if !value.is_empty() => {
            return Err(not_in_v1("an S3 storage class"));
        }
        "partSize" | "part-size" => match value.parse::<u64>() {
            Ok(bytes) if bytes > 0 => out.part_size = Some(bytes),
            Ok(_) | Err(_) => {
                let name = name.to_owned();
                return Err(ReplicaProblem::QueryNotNumber { name });
            }
        },
        "signPayload" | "sign-payload" | "signAcceptEncoding" | "sign-accept-encoding" => {
            return Ok(Some(
                "the url's signing setting has no effect: v1's S3 client signs requests one fixed way",
            ));
        }
        "requireContentMD5"
        | "require-content-md5"
        | "putObjectContentMD5"
        | "put-object-content-md5" => {
            return Ok(Some(
                "the url's Content-MD5 setting has no effect: v1 always sends Content-MD5 on uploads",
            ));
        }
        "concurrency" => match value.parse::<u64>() {
            Ok(n) => out.concurrency = Some(n).filter(|n| *n > 0),
            Err(_) => {
                let name = name.to_owned();
                return Err(ReplicaProblem::QueryNotNumber { name });
            }
        },
        "endpoint" | "region" | "forcePathStyle" | "force-path-style" | "skipVerify"
        | "storageClass" | "storage-class" => {}
        other => {
            let name = other.to_owned();
            return Err(ReplicaProblem::QueryUnknown { name });
        }
    }
    Ok(None)
}

/// Go's `s3.ParseHost`, for the spellings v1 maps.
fn parse_host(host: &str) -> Result<S3Url, ReplicaProblem> {
    let mut out = S3Url::default();
    if host.contains(':') && !host.contains(".com") {
        out.path_style = Some(true);
        match host.split_once('.') {
            Some((bucket, rest)) => {
                out.bucket = Some(bucket.to_owned());
                out.endpoint = Some(format!("http://{rest}"));
                out.region = Some(lsr_store_s3::DEFAULT_REGION.to_owned());
            }
            None => out.endpoint = Some(format!("http://{host}")),
        }
        return Ok(out);
    }
    if let Some(rest) = host.strip_suffix(".amazonaws.com") {
        let (bucket, region) = match rest.strip_suffix(".s3") {
            Some(bucket) => (bucket, None),
            None => match rest.rsplit_once('.') {
                Some((before, region)) => match before.strip_suffix(".s3") {
                    Some(bucket) => (bucket, Some(region)),
                    None => (host, None),
                },
                None => (host, None),
            },
        };
        out.bucket = Some(bucket.to_owned()).filter(|b| !b.is_empty());
        out.region = region.map(str::to_owned);
        return Ok(out);
    }
    for (suffix, provider) in [
        (".digitaloceanspaces.com", "DigitalOcean Spaces"),
        (".backblazeb2.com", "Backblaze B2"),
        ("s3.filebase.com", "Filebase"),
        (".scw.cloud", "Scaleway"),
        (".neon.tech", "Neon"),
    ] {
        if host.ends_with(suffix) {
            return Err(ReplicaProblem::ProviderHost { provider });
        }
    }
    out.bucket = Some(host.to_owned()).filter(|b| !b.is_empty());
    Ok(out)
}
