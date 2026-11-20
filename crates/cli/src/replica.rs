//! The S3 half of opening a replica: credentials from the environment, the
//! store, and the retry layer around it.
//!
//! The store is wrapped twice. [`Counted`] sits directly on the S3 store and
//! adds up how many listed files had no stored stamp (the store reports that per
//! `list` call, and an inventory makes many), counting only calls that
//! succeeded so a retried page is counted once. [`Retrying`] sits outside it
//! with the default policy; its sleeper blocks the thread, which is all a
//! one-shot command needs and what the no-reactor `block_on` can drive.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use lsr_core::{
    Entropy, FileKey, Level, Listing, ObjectStore, PageLimit, ReadLen, RetryPolicy, Retrying,
    Sleeper, StoreError,
};
use lsr_ltx::{Timestamp, TxidRange};
use lsr_store_s3::{
    S3Config, S3Credentials, S3Store, S3Upload, SecretString, Transport, UreqTransport,
};

use crate::run::CliError;
use lsr_core::{FileInfo, StoredStamp};

/// Sleeps by blocking the thread; the futures it returns never yield.
#[derive(Debug)]
pub struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    #[expect(
        clippy::disallowed_methods,
        reason = "this is the real sleeper the retry layer takes as a `Sleeper`"
    )]
    async fn sleep(&self, wait: Duration) {
        thread::sleep(wait);
    }
}

/// Jitter from the standard library's randomly keyed hasher; retry waits need
/// spread, not secrecy.
#[derive(Debug)]
pub struct OsEntropy {
    state: RandomState,
    draws: AtomicU64,
}

impl OsEntropy {
    fn new() -> Self {
        Self {
            state: RandomState::new(),
            draws: AtomicU64::new(0),
        }
    }
}

impl Entropy for OsEntropy {
    fn below(&self, upper: u64) -> u64 {
        let draw = self.draws.fetch_add(1, Ordering::Relaxed);
        self.state.hash_one(draw).checked_rem(upper).unwrap_or(0)
    }
}

/// An S3 store that reports how many stamp reads had to take the object's
/// `LastModified` because it carried no stored stamp.
#[derive(Debug)]
pub struct Counted<T: Transport> {
    store: S3Store<T>,
}

impl<T: Transport> Counted<T> {
    /// Stamps read so far from objects that carried none.
    pub fn fell_back(&self) -> u64 {
        self.store.stamp_stats().fell_back
    }
}

impl<T: Transport> ObjectStore for Counted<T> {
    type Upload = S3Upload<T>;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.store.list(level, after, limit).await
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.store.read(key, offset, len).await
    }

    async fn stamp(&self, file: &FileInfo) -> Result<StoredStamp, StoreError> {
        self.store.stamp(file).await
    }

    async fn read_with_stamp(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, StoredStamp), StoreError> {
        self.store.read_with_stamp(file, len).await
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
    ) -> Result<Self::Upload, StoreError> {
        self.store.begin_upload(key, created_at).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.store.delete(keys).await
    }
}

/// The store a command runs against for an S3 replica.
pub type Remote<T = UreqTransport> = Retrying<Counted<T>, ThreadSleeper, OsEntropy>;

/// Wraps a store in the counter and the retry layer.
pub fn remote<T: Transport>(store: S3Store<T>) -> Remote<T> {
    Retrying::new(
        Counted { store },
        ThreadSleeper,
        OsEntropy::new(),
        RetryPolicy::DEFAULT,
    )
}

/// The variable holding the access key id.
pub const ACCESS_KEY_VAR: &str = "AWS_ACCESS_KEY_ID";
/// The variable holding the secret access key.
pub const SECRET_KEY_VAR: &str = "AWS_SECRET_ACCESS_KEY";
/// The optional variable holding a session token.
pub const SESSION_TOKEN_VAR: &str = "AWS_SESSION_TOKEN";

/// Credentials from `env` (the process environment in production). An unset
/// or empty variable is missing; the error names the variable, never a value.
///
/// # Errors
/// [`CliError::MissingCredentials`] for the key id or the secret.
pub fn credentials(env: impl Fn(&str) -> Option<String>) -> Result<S3Credentials, CliError> {
    let need = |var: &'static str| {
        env(var)
            .filter(|value| !value.is_empty())
            .ok_or(CliError::MissingCredentials { var })
    };
    let credentials = S3Credentials::new(
        need(ACCESS_KEY_VAR)?,
        SecretString::new(need(SECRET_KEY_VAR)?),
    );
    Ok(
        match env(SESSION_TOKEN_VAR).filter(|token| !token.is_empty()) {
            Some(token) => credentials.with_session_token(SecretString::new(token)),
            None => credentials,
        },
    )
}

/// The store for an S3 replica over HTTP(S), credentials from the environment.
///
/// # Errors
/// [`CliError::MissingCredentials`], or [`CliError::S3Config`] when the
/// endpoint cannot address the bucket.
pub fn open_s3(config: S3Config) -> Result<Remote, CliError> {
    let credentials = credentials(|var| std::env::var(var).ok())?;
    let store = S3Store::new(config, credentials).map_err(CliError::S3Config)?;
    Ok(remote(store))
}

#[cfg(test)]
mod tests;
