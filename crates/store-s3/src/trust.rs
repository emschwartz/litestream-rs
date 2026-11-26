//! Which certificate authorities the store's own HTTP clients trust.
//!
//! - **Linux and the BSDs.** The system's roots, found by `rustls-native-certs`
//!   (`SSL_CERT_FILE` and `SSL_CERT_DIR` when set, else the usual bundle and
//!   directories), so a self-hosted MinIO behind a private CA or a TLS-intercepting
//!   proxy works; Mozilla's roots when it finds none. Go reads a different list of
//!   files, so on an unusual system the two trust sets can differ.
//! - **macOS and Windows.** The operating system's verifier through
//!   `rustls-platform-verifier`, as Go asks it there: its revocation checks and
//!   system trust policy apply, and neither variable is read.
//!
//! The roots are loaded each time a client is built, which is once per store.
//! An application that passes its own `reqwest::Client` keeps whatever that
//! client trusts.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use rustls::crypto::CryptoProvider;
#[cfg(all(unix, not(target_vendor = "apple")))]
use rustls::pki_types::CertificateDer;

use crate::ConfigError;

/// What a client the store builds verifies server certificates against.
#[derive(Debug, Clone)]
pub(crate) enum Roots {
    /// The certificates the system holds: at least one.
    #[cfg(all(unix, not(target_vendor = "apple")))]
    Certs(Vec<CertificateDer<'static>>),
    /// Mozilla's roots, because the system held none.
    #[cfg(all(unix, not(target_vendor = "apple")))]
    Mozilla,
    /// The operating system's verifier.
    #[cfg(not(all(unix, not(target_vendor = "apple"))))]
    Platform,
}

/// The system's roots, or Mozilla's when it has none. A certificate that cannot be read
/// is skipped.
#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) fn system() -> Roots {
    let loaded = rustls_native_certs::load_native_certs();
    if loaded.certs.is_empty() {
        Roots::Mozilla
    } else {
        Roots::Certs(loaded.certs)
    }
}

/// The operating system's verifier.
#[cfg(not(all(unix, not(target_vendor = "apple"))))]
pub(crate) fn system() -> Roots {
    Roots::Platform
}

/// The crypto provider of the clients the store builds, by rustls's own rule: the one the
/// application installed as the process default, else the one provider rustls's crate features
/// enable in this build, which rustls installs as the default.
///
/// rustls exposes no way to ask for that second case, so it is asked the way rustls's own
/// builders ask, and its panic when the features name none or two providers is caught and
/// reported as the error it stands for.
///
/// # Errors
/// [`ConfigError::NoTlsProvider`] when nothing is installed and rustls cannot pick.
pub(crate) fn crypto_provider() -> Result<Arc<CryptoProvider>, ConfigError> {
    if CryptoProvider::get_default().is_none() {
        let probed = catch_unwind(AssertUnwindSafe(rustls::ClientConfig::builder));
        drop(probed);
    }
    CryptoProvider::get_default()
        .map(Arc::clone)
        .ok_or(ConfigError::NoTlsProvider)
}

/// The rustls configuration for a reqwest client the store builds, on `provider`,
/// the one ureq gets too.
///
/// # Errors
/// When the protocol versions or the platform verifier cannot be set up.
#[cfg(feature = "reqwest")]
pub(crate) fn rustls_config(
    roots: Roots,
    provider: Arc<CryptoProvider>,
) -> Result<rustls::ClientConfig, rustls::Error> {
    let builder = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?;
    let builder = match roots {
        #[cfg(all(unix, not(target_vendor = "apple")))]
        Roots::Certs(certs) => {
            let mut store = rustls::RootCertStore::empty();
            let _ignored = store.add_parsable_certificates(certs);
            builder.with_root_certificates(store)
        }
        #[cfg(all(unix, not(target_vendor = "apple")))]
        Roots::Mozilla => builder.with_root_certificates(rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        }),
        #[cfg(not(all(unix, not(target_vendor = "apple"))))]
        Roots::Platform => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(rustls_platform_verifier::Verifier::new(
                provider,
            )?)),
    };
    Ok(builder.with_no_client_auth())
}

#[cfg(test)]
mod tests;
