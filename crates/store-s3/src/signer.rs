//! The credentials the store signs with, fetched again before they expire.
//!
//! Static keys build one [`RequestBuilder`] for the store's life. Temporary credentials
//! (from the chain's web-identity, container, instance-metadata or process sources) come
//! with their source: each request first checks whether they are due
//! ([`Expiring::due`]) and, if so, fetches once under the lock and rebuilds the builder.
//! A failed fetch keeps the credentials in hand while they are still valid, so an outage of
//! the source shorter than the refresh margin changes nothing; once they have expired the
//! request fails with the fetch's error, `Transient` when waiting may fix it and
//! `Denied { Credentials }` otherwise, so the replica's health reports it.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use lsr_core::{Denied, StoreError, StoreOp};
use lsr_ltx::Timestamp;

use crate::chain::CredentialError;
use crate::config::{ConfigError, S3Config};
use crate::request::RequestBuilder;
use crate::source::{CredentialSource, Expiring};

/// The builder for the credentials in hand, and how to replace them.
pub(crate) struct Signer {
    state: Mutex<State>,
    source: Option<Arc<dyn CredentialSource>>,
}

struct State {
    builder: Arc<RequestBuilder>,
    current: Expiring,
}

impl Signer {
    pub(crate) fn new(
        config: &S3Config,
        current: Expiring,
        source: Option<Arc<dyn CredentialSource>>,
    ) -> Result<Self, ConfigError> {
        let builder = Arc::new(RequestBuilder::new(config, current.credentials())?);
        Ok(Self {
            state: Mutex::new(State { builder, current }),
            source,
        })
    }

    /// The builder to sign a request made at `now` with.
    pub(crate) fn builder(
        &self,
        op: StoreOp,
        now: Timestamp,
    ) -> Result<Arc<RequestBuilder>, StoreError> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(source) = &self.source else {
            return Ok(Arc::clone(&state.builder));
        };
        if !state.current.due(now) {
            return Ok(Arc::clone(&state.builder));
        }
        match source.fetch() {
            Ok(fresh) => {
                state.builder = Arc::new(state.builder.with_credentials(fresh.credentials()));
                state.current = fresh;
                Ok(Arc::clone(&state.builder))
            }
            Err(_) if state.current.valid(now) => Ok(Arc::clone(&state.builder)),
            Err(err) => Err(fetch_error(op, &err)),
        }
    }
}

/// A failed fetch, once the credentials in hand have expired.
fn fetch_error(op: StoreOp, err: &CredentialError) -> StoreError {
    match err.fetch_failure() {
        Some(source) if source.transient() => StoreError::Transient {
            op,
            kind: io::ErrorKind::NotConnected,
        },
        Some(_) | None => StoreError::Denied {
            op,
            why: Denied::Credentials,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use lsr_core::{FileKey, Level};
    use lsr_ltx::{Txid, TxidRange};

    use super::*;
    use crate::chain::SourceError;
    use crate::config::{BucketName, Endpoint, Prefix};
    use crate::credentials::{S3Credentials, SecretString};

    const START_MS: i64 = 1_700_000_000_000;
    const HOUR: Duration = Duration::from_secs(3600);

    fn at(offset: Duration) -> Timestamp {
        Timestamp::from_unix_millis(START_MS).saturating_add(offset)
    }

    fn keys(id: &str) -> S3Credentials {
        S3Credentials::new(id.to_owned(), SecretString::new("secret".to_owned()))
    }

    /// A source that answers from a list, one answer per fetch, and counts them.
    #[derive(Debug)]
    struct Listed {
        answers: Mutex<Vec<Result<Expiring, CredentialError>>>,
        fetched: AtomicUsize,
    }

    impl CredentialSource for Listed {
        fn fetch(&self) -> Result<Expiring, CredentialError> {
            self.fetched.fetch_add(1, Ordering::SeqCst);
            self.answers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(0)
        }

        fn name(&self) -> &'static str {
            "listed"
        }
    }

    fn listed(answers: Vec<Result<Expiring, CredentialError>>) -> Arc<Listed> {
        Arc::new(Listed {
            answers: Mutex::new(answers),
            fetched: AtomicUsize::new(0),
        })
    }

    fn signer(current: Expiring, source: Option<Arc<Listed>>) -> Signer {
        let config = S3Config::new(
            Endpoint::new("http://localhost:9000").unwrap(),
            BucketName::new("bucket").unwrap(),
            Prefix::new("db").unwrap(),
        );
        let source: Option<Arc<dyn CredentialSource>> = match source {
            Some(listed) => Some(listed),
            None => None,
        };
        Signer::new(&config, current, source).unwrap()
    }

    /// The key id a request signed now carries.
    fn signs_with(signer: &Signer, now: Timestamp) -> Result<String, StoreError> {
        let builder = signer.builder(StoreOp::Read, now)?;
        Ok(key_of(&builder, now))
    }

    fn key_of(builder: &RequestBuilder, now: Timestamp) -> String {
        let one = Txid::new(1).unwrap();
        let key = FileKey::new(Level::ZERO, TxidRange::new(one, one).unwrap());
        let request = builder.head(now, &key).unwrap();
        let credential = request
            .url()
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-Credential")
            .map(|(_, value)| value.into_owned())
            .unwrap();
        credential.split('/').next().unwrap().to_owned()
    }

    fn unreachable() -> CredentialError {
        CredentialError::Source(SourceError::Unreachable {
            provider: "listed",
            why: String::new(),
        })
    }

    #[test]
    fn static_keys_never_fetch() {
        let source = listed(Vec::new());
        let signer = signer(Expiring::forever(keys("static")), Some(Arc::clone(&source)));
        assert_eq!(signs_with(&signer, at(HOUR * 1000)).unwrap(), "static");
        assert_eq!(source.fetched.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn fetches_once_within_the_margin_before_expiry() {
        let source = listed(vec![Ok(Expiring::until(keys("second"), at(HOUR * 2)))]);
        let signer = signer(
            Expiring::until(keys("first"), at(HOUR)),
            Some(Arc::clone(&source)),
        );
        let margin = crate::source::REFRESH_BEFORE;
        let before = HOUR
            .saturating_sub(margin)
            .saturating_sub(Duration::from_secs(1));
        assert_eq!(signs_with(&signer, at(before)).unwrap(), "first");
        assert_eq!(source.fetched.load(Ordering::SeqCst), 0);
        assert_eq!(
            signs_with(&signer, at(HOUR.saturating_sub(margin))).unwrap(),
            "second"
        );
        assert_eq!(signs_with(&signer, at(HOUR)).unwrap(), "second");
        assert_eq!(source.fetched.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_failed_fetch_keeps_valid_credentials_and_tries_again() {
        let source = listed(vec![
            Err(unreachable()),
            Ok(Expiring::until(keys("second"), at(HOUR * 2))),
        ]);
        let signer = signer(
            Expiring::until(keys("first"), at(HOUR)),
            Some(Arc::clone(&source)),
        );
        let due = HOUR.saturating_sub(Duration::from_secs(60));
        assert_eq!(signs_with(&signer, at(due)).unwrap(), "first");
        assert_eq!(signs_with(&signer, at(due)).unwrap(), "second");
        assert_eq!(source.fetched.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn expired_credentials_fail_by_the_fetch_error() {
        let refused = CredentialError::Source(SourceError::Status {
            provider: "listed",
            status: 403,
            code: "AccessDenied".to_owned(),
        });
        let server = CredentialError::Source(SourceError::Status {
            provider: "listed",
            status: 503,
            code: String::new(),
        });
        let source = listed(vec![Err(unreachable()), Err(server), Err(refused)]);
        let signer = signer(
            Expiring::until(keys("first"), at(HOUR)),
            Some(Arc::clone(&source)),
        );
        for _ in 0..2 {
            assert!(matches!(
                signs_with(&signer, at(HOUR)),
                Err(StoreError::Transient {
                    op: StoreOp::Read,
                    ..
                })
            ));
        }
        assert!(matches!(
            signs_with(&signer, at(HOUR)),
            Err(StoreError::Denied {
                why: Denied::Credentials,
                ..
            })
        ));
    }
}
