//! The S3 half of opening a replica: credentials from the AWS environment
//! variables and shared files, the store, and the retry layer around it.
//!
//! The store is wrapped once, in [`Retrying`] with the default policy; its sleeper
//! blocks the thread, which is all a one-shot command needs and what the
//! no-reactor `block_on` can drive. How many created-at times fell back to the object's
//! `LastModified` is the store's own count (`S3Store::created_at_stats`), which counts a
//! file once however many times a read of it was retried.

use std::thread;
use std::time::Duration;

use lsr_core::{RetryPolicy, Retrying, Sleeper};
use lsr_replica::OsEntropy;
use lsr_store_s3::{CredentialChain, S3Config, S3Store, Transport, UreqTransport};

use crate::run::CliError;

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

/// The store a command runs against for an S3 replica.
pub type Remote<T = UreqTransport> = Retrying<S3Store<T>, ThreadSleeper, OsEntropy>;

/// Wraps a store in the retry layer.
pub fn remote<T: Transport>(store: S3Store<T>) -> Remote<T> {
    Retrying::new(
        store,
        ThreadSleeper,
        OsEntropy::default(),
        RetryPolicy::DEFAULT,
    )
}

/// The store for an S3 replica over HTTP(S), credentials from the AWS
/// environment variables or shared files ([`CredentialChain`]).
///
/// # Errors
/// [`CliError::Credentials`], or [`CliError::S3Config`] when the endpoint
/// cannot address the bucket.
pub fn open_s3(config: S3Config) -> Result<Remote, CliError> {
    let credentials = CredentialChain::from_process()
        .resolve()
        .map_err(CliError::Credentials)?
        .into_credentials();
    let store = S3Store::new(config, credentials).map_err(CliError::S3Config)?;
    Ok(remote(store))
}
