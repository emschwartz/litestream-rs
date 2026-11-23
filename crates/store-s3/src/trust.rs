//! Which certificate authorities the store's own HTTP clients trust: the
//! system's, so a self-hosted MinIO behind a private CA or a TLS-intercepting
//! proxy works, and Mozilla's when the system holds none.
//!
//! `rustls-native-certs` finds the system roots: `SSL_CERT_FILE` and
//! `SSL_CERT_DIR` when set, else the operating system's usual bundle and
//! directories, or its certificate store on macOS and Windows. Go reads a
//! different list of files and, on macOS and Windows, asks the operating
//! system's verifier instead of loading roots, so on an unusual system the
//! two trust sets can differ.
//!
//! The roots are loaded each time a client is built, which is once per store.
//! An application that passes its own `reqwest::Client` keeps whatever that
//! client trusts.

use rustls::pki_types::CertificateDer;

/// What a client the store builds verifies server certificates against.
#[derive(Debug, Clone)]
pub(crate) enum Roots {
    /// The certificates the system holds: at least one.
    Certs(Vec<CertificateDer<'static>>),
    /// Mozilla's roots, because the system held none.
    Mozilla,
}

/// The system's roots, or Mozilla's when it has none. A certificate the system
/// holds but cannot be read is skipped, as Go skips one it cannot parse.
pub(crate) fn system() -> Roots {
    let loaded = rustls_native_certs::load_native_certs();
    if loaded.certs.is_empty() {
        Roots::Mozilla
    } else {
        Roots::Certs(loaded.certs)
    }
}

/// The rustls configuration for a reqwest client the store builds, on ring,
/// the provider ureq uses too.
///
/// # Errors
/// When the protocol versions cannot be set up.
#[cfg(feature = "reqwest")]
pub(crate) fn rustls_config(roots: Roots) -> Result<rustls::ClientConfig, rustls::Error> {
    use std::sync::Arc;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?;
    let store = match roots {
        Roots::Certs(certs) => {
            let mut store = rustls::RootCertStore::empty();
            let _ignored = store.add_parsable_certificates(certs);
            store
        }
        Roots::Mozilla => rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        },
    };
    Ok(builder.with_root_certificates(store).with_no_client_auth())
}

#[cfg(test)]
mod tests;
