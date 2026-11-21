//! A replica entry to a store: Go's `newFileReplicaClientFromConfig` and
//! `NewS3ReplicaClientFromConfig` (`cmd/litestream/main.go:1461-1782`).
//!
//! **File.** `url` (`file://`) or `path`, not both; the path expanded as Go's
//! `expand`. A URL whose path holds escapes is Go's decoded path, unless only the
//! escaped spelling exists on disk (earlier Go versions wrote there), and both
//! existing is refused, as Go's `ResolveLegacyFileReplicaPath`.
//!
//! **S3.** `url` or `bucket` and `path`, Go's precedence: the file's keys win
//! over the URL's parts and query. The URL is read by the one `s3://` parser
//! (`s3.rs`, which the command line's `--replica` uses too), so the daemon and
//! `lsr restore` accept the same URLs.

use std::io;
use std::path::{Path, PathBuf};

use lsr_store_s3::{ConfigError as S3Error, S3Config, S3Credentials, SecretString};
use percent_encoding::percent_decode_str;
use url::Url;

use super::s3::{S3Overrides, S3Url, s3_config};
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
    /// The URL carries a user or password; credentials never come from a URL.
    #[error(
        "the replica url carries credentials; give them as access-key-id and secret-access-key, or in the environment"
    )]
    UserInfo,
    /// A host Go splits into bucket and a provider endpoint litestream-rs does not derive.
    #[error(
        "the url host names a provider endpoint ({provider}); give the bucket alone as the host, or as `bucket`, and the endpoint as `endpoint`"
    )]
    ProviderHost {
        /// The provider.
        provider: &'static str,
    },
    /// An S3 access point ARN.
    #[error("S3 access point ARNs are not in litestream-rs; give the bucket and endpoint")]
    AccessPoint,
    /// A query key that asks for something litestream-rs does not do.
    #[error("url parameter {name} is not in litestream-rs ({why})")]
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
    let url = match replica.url().map(S3Url::parse) {
        None => None,
        Some(Ok(url)) => Some(url),
        Some(Err(problem)) => {
            refuse(&at.key("url"), found, problem);
            return None;
        }
    };
    for text in url.iter().flat_map(S3Url::notes) {
        let (at, text) = (at.key("url"), *text);
        found.notes.push(Note { at, text });
    }
    let over = S3Overrides {
        bucket: s3.bucket(),
        path: replica.path(),
        region: s3.region(),
        endpoint: s3.endpoint(),
        force_path_style: s3.force_path_style(),
        part_size: s3.part_size(),
        concurrency: s3.concurrency(),
    };
    let config = match s3_config(url, &over) {
        Ok(config) => config,
        Err(problem) => {
            refuse(at, found, problem);
            return None;
        }
    };
    let credentials = credentials(at, replica, found)?;
    Some(ReplicaPlan::S3 {
        config: Box::new(config),
        credentials,
    })
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
