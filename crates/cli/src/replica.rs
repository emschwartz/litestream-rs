//! The S3 half of opening a replica: credentials from the AWS environment
//! variables and shared files, the store, and the retry layer around it.
//!
//! The store is wrapped once, in [`Retrying`] with the default policy; its sleeper
//! blocks the thread, which is all a one-shot command needs and what the
//! no-reactor `block_on` can drive. How many created-at times fell back to the object's
//! `LastModified` is the store's own count (`S3Store::created_at_stats`), which counts a
//! file once however many times a read of it was retried.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use lsr_core::{RetryPolicy, Retrying, Sleeper};
use lsr_replica::OsEntropy;
use lsr_store_s3::{
    CredentialChain, S3Config, S3Credentials, S3Store, SystemClock, Transport, UreqTransport,
};

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
    open_s3_with(config, None)
}

/// [`open_s3`], with a config file's keys as the chain's explicit values (as the daemon
/// builds it, so the shared files are still read first).
///
/// # Errors
/// As [`open_s3`].
pub fn open_s3_with(config: S3Config, keys: Option<S3Credentials>) -> Result<Remote, CliError> {
    Ok(remote(resolved_store(config, keys)?))
}

/// How long a restore keeps listing the replica before it gives up. A person is waiting on a
/// restore in the middle of a recovery, so an endpoint that takes connections and never
/// answers must fail in minutes, not the policy's five tries of a minute each for every level
/// (about half an hour). The longest a listing phase can take is this plus one request's
/// deadline, since a request in flight is not cut short. Go retries nothing here, so any
/// bound is no worse than Go.
pub const RESTORE_LISTING_BUDGET: Duration = Duration::from_secs(120);

/// [`open_s3_with`] for a restore: the same store, with every listing held to
/// [`RESTORE_LISTING_BUDGET`] (`Retrying::listing_expired` says when it ran out). Replication
/// keeps the policy alone.
///
/// # Errors
/// As [`open_s3`].
pub fn open_s3_for_restore(
    config: S3Config,
    keys: Option<S3Credentials>,
) -> Result<Remote, CliError> {
    restore_remote(resolved_store(config, keys)?, RESTORE_LISTING_BUDGET)
}

/// [`remote`] with the listing budget `budget`.
pub(crate) fn restore_remote<T: Transport>(
    store: S3Store<T>,
    budget: Duration,
) -> Result<Remote<T>, CliError> {
    Ok(Retrying::with_listing_budget(
        store,
        ThreadSleeper,
        OsEntropy::default(),
        RetryPolicy::DEFAULT,
        Arc::new(SystemClock),
        budget,
    ))
}

fn resolved_store(
    config: S3Config,
    keys: Option<S3Credentials>,
) -> Result<S3Store<UreqTransport>, CliError> {
    let chain = match keys {
        Some(keys) => CredentialChain::from_process().with_explicit(keys),
        None => CredentialChain::from_process(),
    };
    let resolved = chain
        .with_region(config.region().as_str())
        .resolve()
        .map_err(CliError::Credentials)?;
    S3Store::resolved(config, &resolved).map_err(CliError::S3Config)
}
