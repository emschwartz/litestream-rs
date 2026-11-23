//! Temporary credentials and the source that fetches them again.
//!
//! A static key never changes. Credentials from the web-identity, container, instance-metadata
//! or process sources carry an expiry, so the store keeps the source and fetches again before
//! the credentials it signs with run out: when fewer than [`REFRESH_BEFORE`] remain. Go's SDK
//! fetches when the cached credentials have expired (its `CredentialsCache` with no expiry
//! window); fetching a little earlier means no request is ever signed with credentials that
//! expire in flight, and changes nothing a replica can show.
//!
//! A failed fetch keeps the credentials still in hand while they are valid and is tried
//! again on the next request; once they have expired, the request fails with the fetch's
//! error, classified as the store classifies any refusal or outage.

use std::fmt;
use std::time::Duration;

use lsr_ltx::Timestamp;

use crate::chain::CredentialError;
use crate::credentials::S3Credentials;

/// How long before expiry the store fetches again.
pub const REFRESH_BEFORE: Duration = Duration::from_secs(5 * 60);

/// Credentials and the instant they stop working, if they do.
#[derive(Clone, Debug)]
pub struct Expiring {
    credentials: S3Credentials,
    expires: Option<Timestamp>,
}

impl Expiring {
    /// Credentials that never expire.
    #[must_use]
    pub fn forever(credentials: S3Credentials) -> Self {
        Self {
            credentials,
            expires: None,
        }
    }

    /// Credentials valid until `expires`.
    #[must_use]
    pub fn until(credentials: S3Credentials, expires: Timestamp) -> Self {
        Self {
            credentials,
            expires: Some(expires),
        }
    }

    /// The credentials.
    #[must_use]
    pub fn credentials(&self) -> &S3Credentials {
        &self.credentials
    }

    /// When they stop working; `None` for never.
    #[must_use]
    pub fn expires(&self) -> Option<Timestamp> {
        self.expires
    }

    /// Whether they should be fetched again at `now`.
    #[must_use]
    pub fn due(&self, now: Timestamp) -> bool {
        self.expires
            .is_some_and(|expires| now.saturating_add(REFRESH_BEFORE) >= expires)
    }

    /// Whether they still work at `now`.
    #[must_use]
    pub fn valid(&self, now: Timestamp) -> bool {
        self.expires.is_none_or(|expires| now < expires)
    }
}

/// Where temporary credentials come from, asked again before they expire.
pub trait CredentialSource: Send + Sync + fmt::Debug {
    /// Fetches credentials now.
    ///
    /// # Errors
    /// [`CredentialError`] when the source refuses or cannot be reached.
    fn fetch(&self) -> Result<Expiring, CredentialError>;

    /// The source's name, for a log line: `web identity`, `container`, ...
    fn name(&self) -> &'static str;
}
