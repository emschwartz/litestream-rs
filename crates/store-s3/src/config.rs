//! What identifies a replica on an S3-compatible service, and how a URL
//! spells it.
//!
//! The URL form is `s3://<bucket>/<prefix>?endpoint=<http(s) url>&region=<r>&forcePathStyle=<bool>&partSize=<bytes>`.
//! It is narrower than Go's on purpose: the host is the bucket verbatim (Go
//! also splits provider hostnames such as `bucket.s3.amazonaws.com`), a
//! query key this parser does not know is refused (Go ignores it, so a
//! misspelt `forcePathStyle` would silently change behaviour), and userinfo is
//! refused because credentials come from [`crate::S3Credentials`], never from a
//! URL. No error here carries any part of the URL.

use std::fmt;
use std::time::Duration;

use percent_encoding::percent_decode_str;
use thiserror::Error;
use url::{Host, Url};

/// The region used when none is given (Go: `DefaultRegion`).
pub const DEFAULT_REGION: &str = "us-east-1";

/// Why a configuration was refused. No variant carries the URL, its userinfo
/// or its query.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum ConfigError {
    /// The text is not a usable URL, or a query value is malformed.
    #[error("malformed replica url")]
    BadUrl,
    /// The scheme is not `s3` (replica URL) or not `http`/`https` (endpoint).
    #[error("unsupported url scheme")]
    UnsupportedScheme,
    /// The prefix is empty. Go would build keys with a leading slash; a replica
    /// at the root of a bucket is refused instead.
    #[error("the replica prefix is empty")]
    EmptyPrefix,
    /// The prefix has a leading or trailing slash, an empty segment or a control character.
    #[error("the replica prefix is not a clean path")]
    BadPrefix,
    /// The part size is below S3's 5 MiB minimum.
    #[error("part size {bytes} is below the 5 MiB minimum")]
    PartSizeTooSmall {
        /// The size that was asked for.
        bytes: u64,
    },
    /// The URL names no bucket.
    #[error("no bucket named")]
    MissingBucket,
    /// The bucket name is not made of letters, digits, dots, dashes and underscores.
    #[error("bucket name has characters a bucket cannot have")]
    BadBucket,
    /// The region is empty or has characters other than letters, digits and dashes.
    #[error("bad region")]
    BadRegion,
    /// The query holds a key this parser does not know.
    #[error("unknown url parameter")]
    UnknownParameter,
}

/// How the bucket is addressed in the URL.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum UrlStyle {
    /// `https://endpoint/bucket/key`.
    Path,
    /// `https://bucket.endpoint/key`.
    VirtualHost,
}

impl UrlStyle {
    pub(crate) fn to_signer(self) -> rusty_s3::UrlStyle {
        match self {
            Self::Path => rusty_s3::UrlStyle::Path,
            Self::VirtualHost => rusty_s3::UrlStyle::VirtualHost,
        }
    }
}

/// The service a host belongs to, from the host alone (Go: `replica_url.go`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Provider {
    /// Tigris. Every request asks for consistent reads.
    Tigris,
    /// Amazon S3.
    Aws,
    /// Cloudflare R2.
    R2,
    /// A host with an explicit port that is no known cloud: MinIO and its kin.
    Minio,
    /// Anything else.
    Other,
}

/// Suffixes of clouds Go's MinIO test excludes, besides the ones with their own variant.
const OTHER_CLOUDS: [&str; 5] = [
    ".digitaloceanspaces.com",
    ".backblazeb2.com",
    ".filebase.com",
    ".scw.cloud",
    ".supabase.co",
];

impl Provider {
    /// Decides from a host name and whether the endpoint names a port
    /// (rc.go:275-289, replica_url.go).
    #[must_use]
    pub fn from_host(host: &str, has_port: bool) -> Self {
        if host == "fly.storage.tigris.dev" || host == "t3.storage.dev" {
            Self::Tigris
        } else if host.ends_with(".r2.cloudflarestorage.com") {
            Self::R2
        } else if host.ends_with(".amazonaws.com") {
            Self::Aws
        } else if has_port
            && !host.ends_with(".neon.tech")
            && !OTHER_CLOUDS.iter().any(|cloud| host.ends_with(cloud))
        {
            Self::Minio
        } else {
            Self::Other
        }
    }
}

/// An http or https endpoint with a host and nothing else secret-shaped: no
/// userinfo, no query, no fragment.
#[derive(Clone, PartialEq, Eq)]
pub struct Endpoint(Url);

impl Endpoint {
    /// Parses an endpoint URL.
    ///
    /// # Errors
    /// [`ConfigError::UnsupportedScheme`] for anything but http and https,
    /// [`ConfigError::BadUrl`] for no host, userinfo, a query or a fragment.
    pub fn new(text: &str) -> Result<Self, ConfigError> {
        let url = Url::parse(text).map_err(|_unparsed| ConfigError::BadUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ConfigError::UnsupportedScheme);
        }
        let clean = url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none();
        if clean {
            Ok(Self(url))
        } else {
            Err(ConfigError::BadUrl)
        }
    }

    /// The endpoint as a URL.
    #[must_use]
    pub fn url(&self) -> &Url {
        &self.0
    }

    fn host_is_address_or_local(&self) -> bool {
        match self.0.host() {
            Some(Host::Domain(name)) => name == "localhost",
            Some(Host::Ipv4(_) | Host::Ipv6(_)) => true,
            None => false,
        }
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Endpoint({}://{}{})",
            self.0.scheme(),
            self.0.host_str().unwrap_or(""),
            self.0
                .port()
                .map(|port| format!(":{port}"))
                .unwrap_or_default()
        )
    }
}

fn name_is_clean(text: &str, allowed: impl Fn(char) -> bool) -> bool {
    !text.is_empty() && text.chars().all(allowed)
}

/// A bucket name.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BucketName(String);

impl BucketName {
    /// # Errors
    /// [`ConfigError::MissingBucket`] for an empty name, [`ConfigError::BadBucket`]
    /// for characters outside letters, digits, `.`, `-` and `_`.
    pub fn new(name: &str) -> Result<Self, ConfigError> {
        if name.is_empty() {
            return Err(ConfigError::MissingBucket);
        }
        name_is_clean(name, |c| {
            c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')
        })
        .then(|| Self(name.to_owned()))
        .ok_or(ConfigError::BadBucket)
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A signing region.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Region(String);

impl Region {
    /// # Errors
    /// [`ConfigError::BadRegion`] for an empty name or one with characters
    /// other than ASCII letters, digits and dashes.
    pub fn new(name: &str) -> Result<Self, ConfigError> {
        name_is_clean(name, |c| c.is_ascii_alphanumeric() || c == '-')
            .then(|| Self(name.to_owned()))
            .ok_or(ConfigError::BadRegion)
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The key prefix every object of the replica lives under: non-empty, no
/// leading or trailing `/`, no empty segment, no control character.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Prefix(String);

impl Prefix {
    /// # Errors
    /// [`ConfigError::EmptyPrefix`] for `""`, [`ConfigError::BadPrefix`] for the rest.
    pub fn new(text: &str) -> Result<Self, ConfigError> {
        if text.is_empty() {
            return Err(ConfigError::EmptyPrefix);
        }
        let clean = !text.starts_with('/')
            && !text.ends_with('/')
            && !text.contains("//")
            && !text.chars().any(char::is_control);
        if clean {
            Ok(Self(text.to_owned()))
        } else {
            Err(ConfigError::BadPrefix)
        }
    }

    /// The prefix, without a trailing slash.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Bytes per multipart part: at least S3's 5 MiB minimum.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct PartSize(u64);

impl PartSize {
    /// S3's minimum and Go's default.
    pub const MIN: Self = Self(5 * 1024 * 1024);
    /// Go's default on Tigris (`DefaultTigrisPartSize`).
    pub const TIGRIS: Self = Self(16 * 1024 * 1024);

    /// # Errors
    /// [`ConfigError::PartSizeTooSmall`] below 5 MiB.
    pub fn new(bytes: u64) -> Result<Self, ConfigError> {
        if bytes < Self::MIN.0 {
            Err(ConfigError::PartSizeTooSmall { bytes })
        } else {
            Ok(Self(bytes))
        }
    }

    /// The size in bytes.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Where a replica lives and how to talk to it. Holds no credentials.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct S3Config {
    endpoint: Endpoint,
    bucket: BucketName,
    prefix: Prefix,
    region: Region,
    style: UrlStyle,
    provider: Provider,
    part_size: PartSize,
    deadline: Duration,
}

/// How long one request may take end to end unless told otherwise.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(60);

impl S3Config {
    /// A configuration with the defaults the endpoint's provider gets:
    /// region `us-east-1`, path style for MinIO, bare addresses and
    /// `localhost` (virtual-host style cannot name them), virtual-host style
    /// otherwise, 16 MiB parts on Tigris and 5 MiB elsewhere.
    #[must_use]
    pub fn new(endpoint: Endpoint, bucket: BucketName, prefix: Prefix) -> Self {
        let provider = Provider::from_host(
            endpoint.url().host_str().unwrap_or(""),
            endpoint.url().port().is_some(),
        );
        let path_style = provider == Provider::Minio || endpoint.host_is_address_or_local();
        Self {
            endpoint,
            bucket,
            prefix,
            region: Region(DEFAULT_REGION.to_owned()),
            style: if path_style {
                UrlStyle::Path
            } else {
                UrlStyle::VirtualHost
            },
            provider,
            part_size: if provider == Provider::Tigris {
                PartSize::TIGRIS
            } else {
                PartSize::MIN
            },
            deadline: DEFAULT_DEADLINE,
        }
    }

    /// Parses an `s3://` replica URL; see the module docs for the form.
    ///
    /// # Errors
    /// A [`ConfigError`] naming what is wrong, never the URL itself.
    pub fn from_url(text: &str) -> Result<Self, ConfigError> {
        let url = Url::parse(text).map_err(|_unparsed| ConfigError::BadUrl)?;
        if url.scheme() != "s3" {
            return Err(ConfigError::UnsupportedScheme);
        }
        if !url.username().is_empty() || url.password().is_some() || url.port().is_some() {
            return Err(ConfigError::BadUrl);
        }
        let bucket = BucketName::new(url.host_str().unwrap_or(""))?;
        let path = url.path().strip_prefix('/').unwrap_or_else(|| url.path());
        let decoded = percent_decode_str(path)
            .decode_utf8()
            .map_err(|_bytes| ConfigError::BadPrefix)?;
        let prefix = Prefix::new(&decoded)?;
        let params = Params::read(&url)?;
        let region = match params.region {
            Some(name) => Region::new(&name)?,
            None => Region(DEFAULT_REGION.to_owned()),
        };
        let endpoint = match params.endpoint {
            Some(text) => Endpoint::new(&text)?,
            None => Endpoint::new(&format!("https://s3.{}.amazonaws.com", region.as_str()))?,
        };
        let mut config = Self::new(endpoint, bucket, prefix).with_region(region);
        if let Some(path_style) = params.path_style {
            config.style = if path_style {
                UrlStyle::Path
            } else {
                UrlStyle::VirtualHost
            };
        }
        if let Some(bytes) = params.part_size {
            config.part_size = PartSize::new(bytes)?;
        }
        Ok(config)
    }

    /// Replaces the region.
    #[must_use]
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = region;
        self
    }

    /// Replaces the addressing style.
    #[must_use]
    pub fn with_style(mut self, style: UrlStyle) -> Self {
        self.style = style;
        self
    }

    /// Replaces the provider the host decided; the provider only changes the
    /// headers sent.
    #[must_use]
    pub fn with_provider(mut self, provider: Provider) -> Self {
        self.provider = provider;
        self
    }

    /// Replaces the multipart part size.
    #[must_use]
    pub fn with_part_size(mut self, part_size: PartSize) -> Self {
        self.part_size = part_size;
        self
    }

    /// Replaces the per-request deadline.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// The endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The bucket.
    #[must_use]
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    /// The key prefix.
    #[must_use]
    pub fn prefix(&self) -> &Prefix {
        &self.prefix
    }

    /// The signing region.
    #[must_use]
    pub fn region(&self) -> &Region {
        &self.region
    }

    /// The addressing style.
    #[must_use]
    pub fn style(&self) -> UrlStyle {
        self.style
    }

    /// The provider.
    #[must_use]
    pub fn provider(&self) -> Provider {
        self.provider
    }

    /// The multipart part size.
    #[must_use]
    pub fn part_size(&self) -> PartSize {
        self.part_size
    }

    /// The per-request deadline the transport enforces.
    #[must_use]
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
}

/// The query of a replica URL.
struct Params {
    endpoint: Option<String>,
    region: Option<String>,
    path_style: Option<bool>,
    part_size: Option<u64>,
}

impl Params {
    fn read(url: &Url) -> Result<Self, ConfigError> {
        let mut params = Self {
            endpoint: None,
            region: None,
            path_style: None,
            part_size: None,
        };
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "endpoint" => params.endpoint = Some(value.into_owned()),
                "region" => params.region = Some(value.into_owned()),
                "forcePathStyle" | "force-path-style" => {
                    params.path_style = Some(match value.as_ref() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(ConfigError::BadUrl),
                    });
                }
                "partSize" | "part-size" => {
                    params.part_size =
                        Some(value.parse().map_err(|_malformed| ConfigError::BadUrl)?);
                }
                _ => return Err(ConfigError::UnknownParameter),
            }
        }
        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_refuses_empty_and_slashes() {
        assert_eq!(Prefix::new(""), Err(ConfigError::EmptyPrefix));
        for bad in ["/", "a/", "/a", "a//b", "a\nb"] {
            assert_eq!(Prefix::new(bad), Err(ConfigError::BadPrefix), "{bad:?}");
        }
        assert_eq!(Prefix::new("a").map(|p| p.0), Ok("a".to_owned()));
        assert_eq!(Prefix::new("a/b").map(|p| p.0), Ok("a/b".to_owned()));
    }

    #[test]
    fn provider_comes_from_the_host() {
        let cases = [
            ("fly.storage.tigris.dev", false, Provider::Tigris),
            ("t3.storage.dev", false, Provider::Tigris),
            ("s3.us-west-2.amazonaws.com", false, Provider::Aws),
            ("acct.r2.cloudflarestorage.com", false, Provider::R2),
            ("localhost", true, Provider::Minio),
            ("minio.internal", true, Provider::Minio),
            ("minio.internal", false, Provider::Other),
            ("s3.fr-par.scw.cloud", true, Provider::Other),
        ];
        for (host, port, want) in cases {
            assert_eq!(Provider::from_host(host, port), want, "{host}");
        }
    }

    #[test]
    fn from_url_reads_the_documented_form() {
        let config = S3Config::from_url(
            "s3://my-bucket/db/one?endpoint=http%3A%2F%2Flocalhost%3A9000&region=eu-west-1&partSize=8388608",
        )
        .unwrap();
        assert_eq!(config.bucket().as_str(), "my-bucket");
        assert_eq!(config.prefix().as_str(), "db/one");
        assert_eq!(config.region().as_str(), "eu-west-1");
        assert_eq!(config.provider(), Provider::Minio);
        assert_eq!(config.style(), UrlStyle::Path);
        assert_eq!(config.part_size().get(), 8_388_608);
    }

    #[test]
    fn from_url_defaults_to_aws_virtual_host() {
        let config = S3Config::from_url("s3://b/p").unwrap();
        assert_eq!(
            config.endpoint().url().as_str(),
            "https://s3.us-east-1.amazonaws.com/"
        );
        assert_eq!(config.provider(), Provider::Aws);
        assert_eq!(config.style(), UrlStyle::VirtualHost);
        assert_eq!(config.part_size(), PartSize::MIN);
    }

    #[test]
    fn tigris_gets_its_part_size() {
        let config =
            S3Config::from_url("s3://b/p?endpoint=https%3A%2F%2Ffly.storage.tigris.dev").unwrap();
        assert_eq!(config.provider(), Provider::Tigris);
        assert_eq!(config.part_size(), PartSize::TIGRIS);
    }

    #[test]
    fn from_url_refuses_each_bad_shape() {
        let cases = [
            ("not a url", ConfigError::BadUrl),
            ("https://b/p", ConfigError::UnsupportedScheme),
            ("s3://b", ConfigError::EmptyPrefix),
            ("s3://b/", ConfigError::EmptyPrefix),
            ("s3://b/p/", ConfigError::BadPrefix),
            ("s3:///p", ConfigError::MissingBucket),
            ("s3://b%20c/p", ConfigError::BadBucket),
            ("s3://b/p?region=", ConfigError::BadRegion),
            (
                "s3://b/p?partSize=1024",
                ConfigError::PartSizeTooSmall { bytes: 1024 },
            ),
            ("s3://b/p?partSize=big", ConfigError::BadUrl),
            ("s3://b/p?forcePathStyle=yes", ConfigError::BadUrl),
            ("s3://b/p?skipVerify=true", ConfigError::UnknownParameter),
            (
                "s3://b/p?endpoint=ftp%3A%2F%2Fh",
                ConfigError::UnsupportedScheme,
            ),
            ("s3://user:pw@b/p", ConfigError::BadUrl),
            ("s3://b:9000/p", ConfigError::BadUrl),
        ];
        for (text, want) in cases {
            assert_eq!(S3Config::from_url(text), Err(want), "{text}");
        }
    }

    #[test]
    fn endpoint_refuses_userinfo_query_and_fragment() {
        for bad in ["http://u:p@h", "http://h?x=1", "http://h#f"] {
            assert_eq!(Endpoint::new(bad), Err(ConfigError::BadUrl), "{bad}");
        }
        assert_eq!(
            format!("{:?}", Endpoint::new("http://h:9/base").unwrap()),
            "Endpoint(http://h:9)"
        );
    }

    #[test]
    fn part_size_has_a_floor() {
        assert_eq!(
            PartSize::new(5 * 1024 * 1024 - 1),
            Err(ConfigError::PartSizeTooSmall {
                bytes: 5 * 1024 * 1024 - 1
            })
        );
        assert_eq!(PartSize::new(5 * 1024 * 1024), Ok(PartSize::MIN));
    }
}
