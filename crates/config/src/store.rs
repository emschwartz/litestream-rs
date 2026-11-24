//! Opening the store a [`ReplicaPlan`] names: the one place a plan becomes something a
//! replicator can write to, shared by the daemon and by an application that starts the
//! library itself.

use std::sync::Arc;

use lsr_replica::DynStore;
use lsr_store_fs::FsStore;
use lsr_store_s3::{CredentialChain, CredentialError, S3Store};

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

/// The store for a replica; S3 credentials are resolved now, from the file or the
/// environment and shared files (Go copies `LITESTREAM_*` to `AWS_*`; the chain reads both).
///
/// # Errors
/// [`OpenStoreError`]: no usable credentials, or a config that cannot address its bucket.
pub fn open_store(replica: &ReplicaPlan) -> Result<Arc<dyn DynStore>, OpenStoreError> {
    match replica {
        ReplicaPlan::File(dir) => Ok(Arc::new(FsStore::new(dir))),
        ReplicaPlan::S3 {
            config,
            credentials,
        } => {
            let chain = match credentials {
                Credentials::FromConfig(given) => {
                    CredentialChain::from_process().with_explicit(given.clone())
                }
                Credentials::FromEnvironment => CredentialChain::from_process(),
            };
            let resolved = chain.with_region(config.region().as_str()).resolve()?;
            let store = S3Store::resolved((**config).clone(), &resolved)?;
            Ok(Arc::new(store))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_that_cannot_open_says_why_in_the_sources_words() {
        let error = OpenStoreError::from(lsr_store_s3::ConfigError::BadUrl);
        assert!(matches!(error, OpenStoreError::Store(_)));
        assert_eq!(error.to_string(), "malformed replica url");
        let dir = ReplicaPlan::File(std::path::PathBuf::from("/replica"));
        open_store(&dir).unwrap();
    }
}
