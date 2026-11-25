//! What identifies a replica on an S3-compatible service: endpoint, bucket,
//! prefix, region, addressing style, provider, part size and multipart
//! concurrency, each a type that holds only a usable value.
//!
//! Reading an `s3://` URL is not here: `lsr-config`'s `S3Url` is the one parser,
//! Go's grammar, for the daemon's config and the command line alike. No error
//! here carries any part of a URL.

use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;

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
    /// The endpoint's scheme is not `http` or `https`.
    #[error("unsupported url scheme")]
    UnsupportedScheme,
    /// The prefix has a leading or trailing slash, an empty segment, a `.` or `..` segment or a
    /// control character. The `url` type cannot carry a dot segment: it and the HTTP clients built on it
    /// drop it, so the request would name another object than the key says.
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
    /// The region is empty, or holds whitespace, a `/` or a control character, which a
    /// SigV4 credential scope cannot carry.
    #[error("bad region")]
    BadRegion,
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
    /// [`ConfigError::BadRegion`] for an empty name, or one holding whitespace, a `/`
    /// or a control character. Go accepts any string and signs with it; those are the
    /// ones no request signed with them can succeed with (`docs/decisions.md` D45), so
    /// providers' own names (`fr-par`, `auto`, `us_east`) all pass.
    pub fn new(name: &str) -> Result<Self, ConfigError> {
        name_is_clean(name, |c| !c.is_whitespace() && !c.is_control() && c != '/')
            .then(|| Self(name.to_owned()))
            .ok_or(ConfigError::BadRegion)
    }

    /// The name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The key prefix every object of the replica lives under: empty (the replica is the whole
/// bucket, `s3://bucket/`), or a path with no leading or trailing `/`, no empty segment, no
/// `.` or `..` segment and no control character.
///
/// The empty prefix is Go's too: its keys are `c.Path + "/" + "0000/<file>"`, so with no
/// path they start with a slash (`/0000/<file>.ltx`) and the object URL has an empty first
/// segment (`/bucket//0000/<file>.ltx`). Nothing may normalise that slash away.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Prefix(String);

impl Prefix {
    /// # Errors
    /// [`ConfigError::BadPrefix`] for a path that is not clean; `""` is the bare bucket.
    pub fn new(text: &str) -> Result<Self, ConfigError> {
        if text.is_empty() {
            return Ok(Self(String::new()));
        }
        let clean = !text.starts_with('/')
            && !text.ends_with('/')
            && !text.contains("//")
            && !text.split('/').any(|segment| matches!(segment, "." | ".."))
            && !text.chars().any(char::is_control);
        if clean {
            Ok(Self(text.to_owned()))
        } else {
            Err(ConfigError::BadPrefix)
        }
    }

    /// The prefix, without a trailing slash; empty for the bare bucket.
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
    pub const fn get(self) -> u64 {
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
    concurrency: Option<NonZeroUsize>,
    deadline: Duration,
}

/// How long one request may take end to end unless told otherwise.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(60);

/// Multipart parts in flight unless told otherwise (Go: `DefaultConcurrency`).
pub(crate) const DEFAULT_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(4);

/// Multipart parts in flight on Cloudflare R2 unless told otherwise. Go forces
/// 2 there; here a configured value wins, because the cap behind it (about 2
/// parts per upload ID, acknowledged by Cloudflare in 2022 and never documented
/// as lifted) is a default's worth of caution, not a rule the user may not test.
/// Tigris has no special case: it documents no limit and its own docs use 8
/// concurrent parts, so it gets `DEFAULT_CONCURRENCY` (Go's 2 there was a
/// conservative default with no measured limit).
pub(crate) const R2_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(1);

/// Creation-time HEADs in flight at once. Go allows 50 (`MetadataConcurrency`),
/// a figure taken from AWS's 5,500 HEAD/s per prefix and never measured on R2
/// or Tigris, so this is lower. Not configurable, as in Go. Memory is this many
/// small responses.
pub const CREATION_TIME_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(15);

/// Ranged reads (a restore reading ahead) and listing pages (the levels of an
/// inventory) in flight at once. Go does neither side by side, so there is no
/// Go value; not configurable. Memory is this many read lengths for reads.
pub const READ_CONCURRENCY: NonZeroUsize = NonZeroUsize::MIN.saturating_add(15);

/// A count as bytes, for the memory bounds below; a `usize` fits a `u64` on every target
/// (`lsr-ltx` refuses to build elsewhere).
#[expect(
    clippy::as_conversions,
    reason = "const context has no From for usize; usize is at most 64 bits"
)]
const fn widen(n: usize) -> u64 {
    n as u64
}

/// Bytes of one restore read, restated from `lsr-core` (which keeps it private) for the bound
/// below; the gate in `tests/store-s3/memory.rs` fails when the two disagree.
const RESTORE_READ: u64 = 1 << 20;

// What a multipart upload and its batches may hold, stated where the constants are.
// S3 refuses a part under 5 MiB (EntityTooSmall) except the last.
const _: () = assert!(PartSize::MIN.get() >= 5 * 1024 * 1024);
const _: () = assert!(PartSize::TIGRIS.get() >= PartSize::MIN.get());
// R2's default is the cautious one.
const _: () = assert!(R2_CONCURRENCY.get() <= DEFAULT_CONCURRENCY.get());
// One batch of read-ahead HEADs never asks for more than a decision names ahead.
const _: () = assert!(CREATION_TIME_CONCURRENCY.get() <= lsr_core::MAX_AHEAD);
// Memory in the worst case with the defaults: an upload buffers at most `concurrency`
// parts (5 x 16 MiB on Tigris, 80 MiB), and a restore holds at most `READ_CONCURRENCY`
// reads of the 1 MiB the engine asks for (16 MiB). What each really holds, parts and bodies
// copied nowhere, is pinned through the real client by `tests/store-s3/memory.rs`.
const _: () = assert!(widen(DEFAULT_CONCURRENCY.get()) * PartSize::TIGRIS.get() <= 128 << 20);
const _: () = assert!(widen(READ_CONCURRENCY.get()) * RESTORE_READ <= 16 << 20);

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
            concurrency: None,
            deadline: DEFAULT_DEADLINE,
        }
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

    /// Replaces how many parts of one multipart upload may be in flight at
    /// once, Go's `concurrency`. It governs nothing else: reads, listings and
    /// HEADs have their own fixed limits. Memory held by an upload is at most
    /// this many parts (`concurrency` times the part size). One sends every
    /// part after the one before it.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = Some(concurrency);
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

    /// How many parts of one multipart upload may be in flight: the
    /// configured value, else `R2_CONCURRENCY` on R2, else
    /// `DEFAULT_CONCURRENCY`. Decided here, not when set, so the provider
    /// may change after.
    #[must_use]
    pub fn concurrency(&self) -> NonZeroUsize {
        match (self.concurrency, self.provider) {
            (Some(set), _) => set,
            (None, Provider::R2) => R2_CONCURRENCY,
            (None, _) => DEFAULT_CONCURRENCY,
        }
    }

    /// The per-request deadline the transport enforces.
    #[must_use]
    pub fn deadline(&self) -> Duration {
        self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go signs with any region string; only what a SigV4 credential scope cannot carry
    /// is refused, so providers' own names pass.
    #[test]
    fn a_region_is_refused_only_when_no_request_could_be_signed_with_it() {
        for name in ["us-east-1", "auto", "fr_par", "eu.west", "Region9"] {
            assert!(Region::new(name).is_ok(), "{name}");
        }
        for name in ["", "a b", "a/b", "tab\there", "nl\n"] {
            assert_eq!(Region::new(name), Err(ConfigError::BadRegion), "{name:?}");
        }
    }

    #[test]
    fn prefix_accepts_empty_and_refuses_unclean_paths() {
        assert_eq!(Prefix::new("").map(|p| p.0), Ok(String::new()));
        for bad in [
            "/", "a/", "/a", "a//b", "a\nb", ".", "..", "a/./b", "a/..", "./a",
        ] {
            assert_eq!(Prefix::new(bad), Err(ConfigError::BadPrefix), "{bad:?}");
        }
        assert_eq!(Prefix::new("a").map(|p| p.0), Ok("a".to_owned()));
        assert_eq!(Prefix::new("a/b").map(|p| p.0), Ok("a/b".to_owned()));
        assert_eq!(
            Prefix::new("a.b/..c/d.").map(|p| p.0),
            Ok("a.b/..c/d.".to_owned())
        );
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
    fn tigris_gets_its_part_size() {
        let config = S3Config::new(
            Endpoint::new("https://fly.storage.tigris.dev").unwrap(),
            BucketName::new("b").unwrap(),
            Prefix::new("p").unwrap(),
        );
        assert_eq!(config.provider(), Provider::Tigris);
        assert_eq!(config.part_size(), PartSize::TIGRIS);
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

    fn config_at(host: &str) -> S3Config {
        let endpoint = Endpoint::new(&format!("https://{host}")).unwrap();
        S3Config::new(
            endpoint,
            BucketName::new("b").unwrap(),
            Prefix::new("p").unwrap(),
        )
    }

    fn n(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    /// Multipart parts in flight: 5, R2 2 unless set, a set value always wins.
    #[test]
    fn multipart_concurrency_defaults_per_provider() {
        let generic = config_at("s3.us-east-1.amazonaws.com");
        let tigris = config_at("fly.storage.tigris.dev");
        let r2 = config_at("acct.r2.cloudflarestorage.com");
        assert_eq!(generic.concurrency().get(), 5);
        assert_eq!(tigris.concurrency().get(), 5);
        assert_eq!(r2.concurrency().get(), 2);
        assert_eq!(generic.with_concurrency(n(9)).concurrency().get(), 9);
        assert_eq!(tigris.with_concurrency(n(9)).concurrency().get(), 9);
        assert_eq!(r2.with_concurrency(n(9)).concurrency().get(), 9);
    }

    /// The fixed limits of the other kinds of request.
    #[test]
    fn other_kinds_have_fixed_limits() {
        assert_eq!(CREATION_TIME_CONCURRENCY.get(), 16);
        assert_eq!(READ_CONCURRENCY.get(), 16);
    }
}
