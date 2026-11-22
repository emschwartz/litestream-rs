//! Credentials that cannot be printed by accident.
//!
//! [`SecretString`] has no `Display`, no `Deref` and a `Debug` that prints nothing
//! of its contents; the only way to the bytes is the crate-private
//! [`SecretString::expose`], called where a request is signed. The memory is
//! zeroed on drop.

use std::fmt;

use zeroize::Zeroizing;

/// A secret: a secret access key or a session token.
///
/// There is no `Display`:
///
/// ```compile_fail
/// let secret = lsr_store_s3::SecretString::new("hunter2".to_owned());
/// let _ = format!("{secret}");
/// ```
///
/// `Debug` is allowed and prints no contents:
///
/// ```
/// let secret = lsr_store_s3::SecretString::new("hunter2".to_owned());
/// assert_eq!(format!("{secret:?}"), "SecretString(..)");
/// ```
#[derive(Clone)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    /// Wraps a secret.
    #[must_use]
    pub fn new(secret: String) -> Self {
        Self(Zeroizing::new(secret))
    }

    /// The secret's bytes, for signing only.
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretString(..)")
    }
}

/// An access key id with its secret and, for temporary credentials, a session
/// token.
///
/// There is no `Display` and nothing serializes it:
///
/// ```compile_fail
/// use lsr_store_s3::{S3Credentials, SecretString};
/// let credentials = S3Credentials::new("AKIA".to_owned(), SecretString::new("s".to_owned()));
/// let _ = format!("{credentials}");
/// ```
#[derive(Clone)]
pub struct S3Credentials {
    access_key_id: String,
    secret: SecretString,
    session_token: Option<SecretString>,
}

impl S3Credentials {
    /// Long-lived credentials.
    #[must_use]
    pub fn new(access_key_id: String, secret: SecretString) -> Self {
        Self {
            access_key_id,
            secret,
            session_token: None,
        }
    }

    /// Adds the session token that temporary credentials carry.
    #[must_use]
    pub fn with_session_token(mut self, token: SecretString) -> Self {
        self.session_token = Some(token);
        self
    }

    /// The key id, secret and session token in the clear, for the Go differential
    /// (`lsr-compat`), which compares them with what the AWS SDK resolved from fake keys.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn exposed(&self) -> (&str, &str, Option<&str>) {
        (
            &self.access_key_id,
            self.secret.expose(),
            self.session_token.as_ref().map(SecretString::expose),
        )
    }

    /// The signer's form of these credentials.
    pub(crate) fn to_signer(&self) -> rusty_s3::Credentials {
        match &self.session_token {
            Some(token) => rusty_s3::Credentials::new_with_token(
                self.access_key_id.as_str(),
                self.secret.expose(),
                token.expose(),
            ),
            None => rusty_s3::Credentials::new(self.access_key_id.as_str(), self.secret.expose()),
        }
    }
}

/// Prints the first two characters of the key id, so two configurations can
/// be told apart in a log, and nothing else.
impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let head: String = self.access_key_id.chars().take(2).collect();
        write!(f, "S3Credentials {{ access_key_id: \"{head}..\", .. }}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_shows_two_characters_of_the_key_id_only() {
        let credentials = S3Credentials::new(
            "AKIAIOSFODNN7EXAMPLE".to_owned(),
            SecretString::new("wJalrXUtnFEMI".to_owned()),
        )
        .with_session_token(SecretString::new("token-bytes".to_owned()));
        assert_eq!(
            format!("{credentials:?}"),
            "S3Credentials { access_key_id: \"AK..\", .. }"
        );
    }

    #[test]
    fn signer_form_carries_the_token() {
        let credentials = S3Credentials::new("AK".to_owned(), SecretString::new("s".to_owned()))
            .with_session_token(SecretString::new("t".to_owned()));
        assert_eq!(credentials.to_signer().token(), Some("t"));
        assert_eq!(credentials.to_signer().secret(), "s");
    }
}
