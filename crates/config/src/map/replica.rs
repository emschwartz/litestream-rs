//! A replica entry to a store: Go's `newFileReplicaClientFromConfig` and
//! `NewS3ReplicaClientFromConfig` (`cmd/litestream/main.go:1461-1782`).
//!
//! **File.** `url` (`file://`) or `path`, not both; the path expanded as Go's
//! `expand`. A URL whose path holds escapes is Go's decoded path, unless only the
//! escaped spelling exists on disk (earlier Go versions wrote there), and both
//! existing is refused, as Go's `ResolveLegacyFileReplicaPath`.
//!
//! **S3.** `url` or `bucket` and `path`, Go's precedence: the file's keys win
//! over the URL's parts and query. The host is the bucket, as Go's `ParseHost`
//! reads it for plain names, AWS virtual hosts (`bucket.s3.region.amazonaws.com`)
//! and MinIO's `bucket.host:port`; another provider's host spelling (DigitalOcean,
//! Backblaze, Filebase, Scaleway, Neon) and access point ARNs are refused with a
//! pointer to `bucket` plus `endpoint`, since v1 does not derive their endpoints.
//! Query keys Go reads: `endpoint`, `region`, `forcePathStyle` (and
//! `force-path-style`), `partSize` (`part-size`) are honoured; `skipVerify=true`
//! and a `storageClass` are refused, as their file keys are; the signing, MD5 and
//! `concurrency` keys are accepted with a note, as their file keys are. A key Go
//! does not read is refused. Path style follows Go: on when an endpoint is
//! given, unless set; otherwise as the provider needs.

use std::io;
use std::path::{Path, PathBuf};

use lsr_store_s3::{
    BucketName, ConfigError as S3Error, Endpoint, PartSize, Prefix, Region, S3Config,
    S3Credentials, SecretString, UrlStyle,
};
use percent_encoding::percent_decode_str;
use url::Url;

use super::{Credentials, Found, MapProblemKind, Note, ReplicaPlan};
use crate::config::{ReplicaConfig, ReplicaKind};
use crate::error::KeyPath;
use crate::expand::{Env, clean, expand_path};

/// Why a replica entry gives no store. No variant holds a credential or a URL query.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReplicaProblem {
    /// `url` and `path` on a file replica.
    #[error("cannot specify url & path for file replica")]
    FileUrlAndPath,
    /// Neither.
    #[error("file replica path required")]
    FilePathRequired,
    /// `url` is not a `file://` URL.
    #[error("file replica url required")]
    NotFileUrl,
    /// Both spellings of an escaped file URL exist.
    #[error("file replica directories {} and {} both exist: earlier versions of litestream wrote to the second; merge or remove one of them", .decoded.display(), .legacy.display())]
    LegacyBoth {
        /// The decoded path.
        decoded: PathBuf,
        /// The escaped path.
        legacy: PathBuf,
    },
    /// The legacy path could not be looked at.
    #[error("cannot inspect {}: {kind:?}", .path.display())]
    Inspect {
        /// The path.
        path: PathBuf,
        /// What the file system said.
        kind: io::ErrorKind,
    },
    /// `url` and `path` on an S3 replica.
    #[error("cannot specify url & path for s3 replica")]
    S3UrlAndPath,
    /// `url` and `bucket`.
    #[error("cannot specify url & bucket for s3 replica")]
    S3UrlAndBucket,
    /// No bucket anywhere.
    #[error("bucket required for s3 replica")]
    BucketRequired,
    /// The URL does not parse.
    #[error("the replica url does not parse")]
    BadUrl,
    /// A host Go splits into bucket and a provider endpoint v1 does not derive.
    #[error(
        "the url host names a provider endpoint ({provider}); give the bucket alone as the host, or as `bucket`, and the endpoint as `endpoint`"
    )]
    ProviderHost {
        /// The provider.
        provider: &'static str,
    },
    /// An S3 access point ARN.
    #[error("S3 access point ARNs are not in v1; give the bucket and endpoint")]
    AccessPoint,
    /// A query key that asks for something v1 does not do.
    #[error("url parameter {name} is not in v1 ({why})")]
    QueryNotInV1 {
        /// The key.
        name: String,
        /// Why.
        why: &'static str,
    },
    /// A query key Go does not read.
    #[error("url parameter {name} is not one Go reads")]
    QueryUnknown {
        /// The key.
        name: String,
    },
    /// A query value that is not a number where one is needed.
    #[error("url parameter {name} must be a positive integer")]
    QueryNotNumber {
        /// The key.
        name: String,
    },
    /// One of `access-key-id` and `secret-access-key` without the other.
    #[error("access-key-id and secret-access-key must be given together")]
    HalfCredentials,
    /// The S3 store refuses the result.
    #[error("{0}")]
    S3(S3Error),
}

fn refuse(at: &KeyPath, found: &mut Found, problem: ReplicaProblem) {
    found.problem(at, MapProblemKind::Replica(problem));
}

/// The store for one replica entry.
pub(super) fn map(
    at: &KeyPath,
    replica: &ReplicaConfig,
    env: &dyn Env,
    found: &mut Found,
) -> Option<ReplicaPlan> {
    match replica.kind() {
        ReplicaKind::File => file(at, replica, env, found).map(ReplicaPlan::File),
        ReplicaKind::S3 => s3(at, replica, found),
    }
}

fn file(
    at: &KeyPath,
    replica: &ReplicaConfig,
    env: &dyn Env,
    found: &mut Found,
) -> Option<PathBuf> {
    let expand = |path: &str, found: &mut Found| match expand_path(path, env) {
        Ok(path) => Some(path),
        Err(error) => {
            found.problem(at, MapProblemKind::Path(error));
            None
        }
    };
    match (replica.url(), replica.path()) {
        (Some(_), Some(_)) => {
            refuse(at, found, ReplicaProblem::FileUrlAndPath);
            None
        }
        (None, None) => {
            refuse(at, found, ReplicaProblem::FilePathRequired);
            None
        }
        (None, Some("")) => {
            refuse(at, found, ReplicaProblem::FilePathRequired);
            None
        }
        (None, Some(path)) => expand(path, found),
        (Some(url), None) => {
            let Some((decoded, legacy)) = file_url(url) else {
                refuse(at, found, ReplicaProblem::NotFileUrl);
                return None;
            };
            let decoded = expand(&decoded, found)?;
            let legacy = expand(&legacy, found)?;
            legacy_or(at, decoded, legacy, found)
        }
    }
}

/// Go's `fileReplicaURLPaths`: the decoded path and the escaped one, both cleaned.
fn file_url(text: &str) -> Option<(String, String)> {
    let url = Url::parse(text).ok()?;
    if url.scheme() != "file" {
        return None;
    }
    let host = url
        .host_str()
        .filter(|h| !h.is_empty())
        .map(|h| format!("/{h}"));
    let escaped = format!("{}{}", host.unwrap_or_default(), url.path());
    let decoded = percent_decode_str(&escaped)
        .decode_utf8()
        .ok()?
        .into_owned();
    Some((clean(&decoded), clean(&escaped)))
}

/// Go's `ResolveLegacyFileReplicaPath`.
fn legacy_or(
    at: &KeyPath,
    decoded: PathBuf,
    legacy: PathBuf,
    found: &mut Found,
) -> Option<PathBuf> {
    if decoded == legacy {
        return Some(decoded);
    }
    let exists = |path: &Path, found: &mut Found| match std::fs::symlink_metadata(path) {
        Ok(_) => Some(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Some(false),
        Err(error) => {
            let (path, kind) = (path.to_owned(), error.kind());
            refuse(at, found, ReplicaProblem::Inspect { path, kind });
            None
        }
    };
    if !exists(&legacy, found)? {
        return Some(decoded);
    }
    if exists(&decoded, found)? {
        refuse(at, found, ReplicaProblem::LegacyBoth { decoded, legacy });
        return None;
    }
    Some(legacy)
}

/// What the URL said, before the file's keys override it.
#[derive(Default)]
struct FromUrl {
    bucket: Option<String>,
    path: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    path_style: Option<bool>,
    path_style_set: bool,
    part_size: Option<u64>,
}

fn s3(at: &KeyPath, replica: &ReplicaConfig, found: &mut Found) -> Option<ReplicaPlan> {
    let s3 = replica.s3();
    if replica.url().is_some() && replica.path().is_some() {
        refuse(at, found, ReplicaProblem::S3UrlAndPath);
        return None;
    }
    if replica.url().is_some() && s3.bucket().is_some() {
        refuse(at, found, ReplicaProblem::S3UrlAndBucket);
        return None;
    }
    let from_url = match replica.url() {
        Some(url) => s3_url(&at.key("url"), url, found)?,
        None => FromUrl::default(),
    };
    let text = |own: Option<&str>, url: Option<String>| {
        own.map(str::to_owned).filter(|s| !s.is_empty()).or(url)
    };
    let bucket = text(s3.bucket(), from_url.bucket);
    let path = text(replica.path(), from_url.path).unwrap_or_default();
    let region = text(s3.region(), from_url.region);
    let endpoint = text(s3.endpoint(), from_url.endpoint).map(|e| ensure_scheme(&e));
    let Some(bucket) = bucket else {
        refuse(at, found, ReplicaProblem::BucketRequired);
        return None;
    };
    // Go: path style when an endpoint is given; the file's key, else the URL's, wins.
    let (style, style_set) = match (s3.force_path_style(), from_url.path_style) {
        (Some(own), _) => (own, true),
        (None, Some(url)) => (url || s3.endpoint().is_some(), from_url.path_style_set),
        (None, None) => (endpoint.is_some(), false),
    };
    let built = build(&bucket, &path, region, endpoint, (style, style_set));
    let part_size = s3.part_size().or(from_url.part_size);
    let built = built.and_then(|config| match part_size {
        None => Ok(config),
        Some(bytes) => PartSize::new(bytes).map(|size| config.with_part_size(size)),
    });
    let config = match built {
        Ok(config) => config,
        Err(error) => {
            refuse(at, found, ReplicaProblem::S3(error));
            return None;
        }
    };
    let credentials = credentials(at, replica, found)?;
    Some(ReplicaPlan::S3 {
        config: Box::new(config),
        credentials,
    })
}

fn build(
    bucket: &str,
    path: &str,
    region: Option<String>,
    endpoint: Option<String>,
    (path_style, set): (bool, bool),
) -> Result<S3Config, S3Error> {
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

fn credentials(at: &KeyPath, replica: &ReplicaConfig, found: &mut Found) -> Option<Credentials> {
    let s3 = replica.s3();
    fn given(secret: Option<&crate::config::Secret>) -> Option<&crate::config::Secret> {
        secret.filter(|s| !s.expose().is_empty())
    }
    match (given(s3.access_key_id()), given(s3.secret_access_key())) {
        (Some(id), Some(secret)) => Some(Credentials::FromConfig(S3Credentials::new(
            id.expose().to_owned(),
            SecretString::new(secret.expose().to_owned()),
        ))),
        (None, None) => Some(Credentials::FromEnvironment),
        (Some(_), None) | (None, Some(_)) => {
            refuse(at, found, ReplicaProblem::HalfCredentials);
            None
        }
    }
}

/// Go's `EnsureEndpointScheme`: http for local hosts, https otherwise.
fn ensure_scheme(endpoint: &str) -> String {
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

/// The URL's parts and query, as Go reads them.
fn s3_url(at: &KeyPath, text: &str, found: &mut Found) -> Option<FromUrl> {
    if text.to_ascii_lowercase().starts_with("s3://arn:") {
        refuse(at, found, ReplicaProblem::AccessPoint);
        return None;
    }
    let Ok(url) = Url::parse(text) else {
        refuse(at, found, ReplicaProblem::BadUrl);
        return None;
    };
    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or("")),
        None => url.host_str().unwrap_or("").to_owned(),
    };
    let mut out = match parse_host(&host) {
        Ok(out) => out,
        Err(problem) => {
            refuse(at, found, problem);
            return None;
        }
    };
    let decoded = percent_decode_str(url.path())
        .decode_utf8()
        .map(|p| clean(&p));
    let Ok(path) = decoded else {
        refuse(at, found, ReplicaProblem::BadUrl);
        return None;
    };
    let path = path.trim_start_matches('/');
    out.path = (!path.is_empty() && path != ".").then(|| path.to_owned());
    for (name, value) in url.query_pairs() {
        query(at, &name, &value, &mut out, found)?;
    }
    Some(out)
}

/// One query key; `None` when it is refused.
fn query(
    at: &KeyPath,
    name: &str,
    value: &str,
    out: &mut FromUrl,
    found: &mut Found,
) -> Option<()> {
    match read_query(name, value, out) {
        Ok(None) => Some(()),
        Ok(Some(text)) => {
            found.notes.push(Note {
                at: at.clone(),
                text,
            });
            Some(())
        }
        Err(problem) => {
            refuse(at, found, problem);
            None
        }
    }
}

/// Applies one query key to `out`; a note when it has no effect in v1.
fn read_query(
    name: &str,
    value: &str,
    out: &mut FromUrl,
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
        "concurrency" => {
            return Ok(Some(
                "the url's concurrency has no effect: v1 uploads one part at a time",
            ));
        }
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
fn parse_host(host: &str) -> Result<FromUrl, ReplicaProblem> {
    let mut out = FromUrl::default();
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

/// The replica both plans write to, named without credentials, when it is one place.
pub(super) fn same(a: &ReplicaPlan, b: &ReplicaPlan) -> Option<String> {
    match (a, b) {
        (ReplicaPlan::File(a), ReplicaPlan::File(b)) => (a == b).then(|| a.display().to_string()),
        (ReplicaPlan::S3 { config: a, .. }, ReplicaPlan::S3 { config: b, .. }) => {
            let one = a.endpoint() == b.endpoint()
                && a.bucket() == b.bucket()
                && a.prefix() == b.prefix();
            one.then(|| name(a))
        }
        (ReplicaPlan::File(_), ReplicaPlan::S3 { .. })
        | (ReplicaPlan::S3 { .. }, ReplicaPlan::File(_)) => None,
    }
}

/// `s3://<bucket>/<prefix>`.
pub(super) fn name(config: &S3Config) -> String {
    format!(
        "s3://{}/{}",
        config.bucket().as_str(),
        config.prefix().as_str()
    )
}
