//! Opening the store a [`ReplicaPlan`] names: the one place a plan becomes something a
//! replicator can write to, shared by the daemon and by an application that starts the
//! library itself.

use std::sync::Arc;

use lsr_replica::DynStore;
use lsr_store_fs::FsStore;
use lsr_store_s3::{CredentialChain, CredentialError, S3Config, S3Store};

use crate::map::{Credentials, ReplicaPlan};

/// Why a replica's store could not be opened. Each variant is the source's own message, so the
/// log line a person reads is the source's words.
#[derive(Debug, thiserror::Error)]
pub enum OpenStoreError {
    /// No usable S3 credentials.
    #[error("{0}")]
    Credentials(#[from] CredentialError),
    /// The S3 config cannot address its bucket.
    #[error("{0}")]
    Store(#[from] lsr_store_s3::ConfigError),
}

/// A replica's store, and what opening it had to put off.
pub struct Opened {
    /// The store.
    pub store: Arc<dyn DynStore>,
    /// Why the credentials could not be fetched now, when the store fetches them with its first
    /// requests instead ([`open_store`]); `None` when they were in hand or none are needed.
    pub deferred: Option<CredentialError>,
}

impl std::fmt::Debug for Opened {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Opened")
            .field("deferred", &self.deferred)
            .finish_non_exhaustive()
    }
}

/// The store for a replica; S3 credentials are resolved now, from the file or the
/// environment and shared files (Go copies `LITESTREAM_*` to `AWS_*`; the chain reads both).
///
/// A credential source that fails to answer now (the instance metadata service not up yet, a
/// token endpoint down) does not refuse the store: it fetches them again with each request
/// until they come, as Go's SDK does, and the replica's health reports the failure meanwhile
/// ([`open_store_noting`] says it was deferred).
///
/// # Errors
/// [`OpenStoreError`]: credentials the configuration cannot yield (no source named, a profile
/// that cannot be used), or a config that cannot address its bucket.
pub fn open_store(replica: &ReplicaPlan) -> Result<Arc<dyn DynStore>, OpenStoreError> {
    open_store_noting(replica).map(|opened| opened.store)
}

/// [`open_store`], saying whether the credentials were deferred and why.
///
/// # Errors
/// As [`open_store`].
pub fn open_store_noting(replica: &ReplicaPlan) -> Result<Opened, OpenStoreError> {
    match replica {
        ReplicaPlan::File(dir) => Ok(Opened {
            store: Arc::new(
                if std::thread::available_parallelism().is_ok_and(|n| n.get() > 1) {
                    FsStore::new(dir).with_write_behind()
                } else {
                    FsStore::new(dir)
                },
            ),
            deferred: None,
        }),
        ReplicaPlan::S3 {
            config,
            credentials,
        } => {
            let chain = match credentials {
                Credentials::FromConfig(given) => {
                    CredentialChain::from_process().with_explicit(given.clone())
                }
                Credentials::FromEnvironment => CredentialChain::from_process(),
            }
            .with_region(config.region().as_str());
            open_s3(config, chain)
        }
    }
}

/// The S3 store over what `chain` resolves now, or over `chain` itself when a source it asked
/// failed to answer.
fn open_s3(config: &S3Config, chain: CredentialChain) -> Result<Opened, OpenStoreError> {
    match chain.resolve() {
        Ok(resolved) => Ok(Opened {
            store: Arc::new(S3Store::resolved(config.clone(), &resolved)?),
            deferred: None,
        }),
        Err(error) if error.fetch_failure().is_some() => Ok(Opened {
            store: Arc::new(S3Store::deferred(config.clone(), chain)?),
            deferred: Some(error),
        }),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use lsr_core::FakeClock;
    use lsr_ltx::Timestamp;
    use lsr_store_s3::{
        BucketName, Endpoint, HttpRequest, HttpResponse, Prefix, Transport, TransportError,
    };

    /// An instance metadata service that cannot be reached.
    struct Down;

    impl Transport for Down {
        fn send(&self, _request: HttpRequest) -> Result<HttpResponse, TransportError> {
            Err(TransportError::Connect)
        }
    }

    fn s3() -> S3Config {
        S3Config::new(
            Endpoint::new("http://localhost:9000").unwrap(),
            BucketName::new("bucket").unwrap(),
            Prefix::new("db").unwrap(),
        )
    }

    /// A chain with nothing but the instance metadata service, switched off when `off`.
    fn chain(off: bool) -> CredentialChain {
        let mut vars: HashMap<&str, &str> = HashMap::from([
            ("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:9"),
            ("AWS_CONFIG_FILE", "/nonexistent/config"),
            ("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials"),
        ]);
        if off {
            vars.insert("AWS_EC2_METADATA_DISABLED", "true");
        }
        CredentialChain::with_env(move |name| vars.get(name).map(|v| (*v).to_owned()))
            .with_transport(
                Arc::new(Down),
                Arc::new(FakeClock::new(Timestamp::from_unix_millis(0))),
            )
    }

    /// A source that is down when the process starts costs it the credentials for now, not the
    /// database: the store opens, fetches them with its requests, and says why it deferred.
    #[test]
    fn a_source_that_is_down_at_start_opens_a_store_that_asks_again() {
        let opened = open_s3(&s3(), chain(false)).unwrap();
        assert!(
            matches!(&opened.deferred, Some(CredentialError::Missing(_))),
            "{opened:?}"
        );
    }

    /// A configuration that cannot yield credentials is refused at open with its words: asking
    /// again cannot change it.
    #[test]
    fn a_configuration_that_cannot_yield_credentials_is_refused() {
        let err = open_s3(&s3(), chain(true)).unwrap_err();
        assert!(matches!(err, OpenStoreError::Credentials(_)), "{err:?}");
    }

    #[test]
    fn a_store_that_cannot_open_says_why_in_the_sources_words() {
        let error = OpenStoreError::from(lsr_store_s3::ConfigError::BadUrl);
        assert!(matches!(error, OpenStoreError::Store(_)));
        assert_eq!(error.to_string(), "malformed replica url");
        let dir = ReplicaPlan::File(std::path::PathBuf::from("/replica"));
        open_store(&dir).unwrap();
    }
}
