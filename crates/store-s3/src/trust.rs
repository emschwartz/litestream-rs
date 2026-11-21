//! Which certificate authorities the store's own HTTP clients trust: the ones
//! Go's `crypto/x509` trusts, so a Go user's endpoint (a self-hosted MinIO
//! behind a private CA, a TLS-intercepting proxy) works unchanged.
//!
//! Go (`root_unix.go`, `root_linux.go`, `root_bsd.go`; litestream links
//! `golang.org/x/crypto/x509roots/fallback`):
//! - **Linux and the BSDs.** Go reads the first file of a fixed list that it
//!   can read (Debian's `/etc/ssl/certs/ca-certificates.crt`, Fedora's bundle
//!   and so on), then every file in each directory of a fixed list
//!   (`/etc/ssl/certs`, `/etc/pki/tls/certs` on Linux). `SSL_CERT_FILE`
//!   replaces the file list with that one file and `SSL_CERT_DIR` (colon
//!   separated) replaces the directory list; each leaves the other list alone,
//!   and an empty value counts as unset. A symlink to a file in the same
//!   directory (OpenSSL's hash links) is skipped, and so is anything that does
//!   not parse as a certificate. When nothing was found, the fallback package
//!   gives Mozilla's roots. [`system`] does all of this in the same order.
//! - **macOS and Windows.** Go hands the chain to the operating system
//!   (Security.framework, CryptoAPI) and reads neither variable. The store
//!   uses `rustls-platform-verifier`, which asks the same verifier; the
//!   fallback never applies there.
//!
//! Go loads the roots once per process; the store loads them each time a
//! client is built, which is once per store. A host that passes its own
//! `reqwest::Client` keeps whatever that client trusts.

#[cfg(all(unix, not(target_vendor = "apple")))]
use std::ffi::OsString;
#[cfg(all(unix, not(target_vendor = "apple")))]
use std::path::{Path, PathBuf};

#[cfg(all(unix, not(target_vendor = "apple")))]
use rustls::pki_types::CertificateDer;

/// What a client the store builds verifies server certificates against.
#[derive(Debug, Clone)]
pub(crate) enum Roots {
    /// The certificates the system's files held: at least one, each one rustls
    /// accepts as a trust anchor.
    #[cfg(all(unix, not(target_vendor = "apple")))]
    Certs(Vec<CertificateDer<'static>>),
    /// Mozilla's roots, because the system's files held none.
    #[cfg(all(unix, not(target_vendor = "apple")))]
    Mozilla,
    /// The operating system's verifier.
    #[cfg(not(all(unix, not(target_vendor = "apple"))))]
    Platform,
}

/// The roots Go would trust in this process.
#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) fn system() -> Roots {
    let paths = Paths::resolve(
        std::env::var_os(CERT_FILE_VAR),
        std::env::var_os(CERT_DIR_VAR),
        DEFAULT_FILES,
        DEFAULT_DIRS,
    );
    paths.load()
}

/// The roots Go would trust in this process.
#[cfg(not(all(unix, not(target_vendor = "apple"))))]
pub(crate) fn system() -> Roots {
    Roots::Platform
}

/// Replaces the list of bundle files, as in Go and OpenSSL.
#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) const CERT_FILE_VAR: &str = "SSL_CERT_FILE";
/// Replaces the list of certificate directories (colon separated).
#[cfg(all(unix, not(target_vendor = "apple")))]
pub(crate) const CERT_DIR_VAR: &str = "SSL_CERT_DIR";

/// Go's `certFiles` (`root_linux.go`).
#[cfg(target_os = "linux")]
pub(crate) const DEFAULT_FILES: &[&str] = &[
    "/etc/ssl/certs/ca-certificates.crt",
    "/etc/pki/tls/certs/ca-bundle.crt",
    "/etc/ssl/ca-bundle.pem",
    "/etc/pki/tls/cacert.pem",
    "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
    "/etc/ssl/cert.pem",
];
/// Go's `certDirectories` (`root_linux.go`).
#[cfg(target_os = "linux")]
pub(crate) const DEFAULT_DIRS: &[&str] = &["/etc/ssl/certs", "/etc/pki/tls/certs"];

/// Go's `certFiles` (`root_bsd.go`).
#[cfg(any(
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
pub(crate) const DEFAULT_FILES: &[&str] = &[
    "/usr/local/etc/ssl/cert.pem",
    "/etc/ssl/cert.pem",
    "/usr/local/share/certs/ca-root-nss.crt",
    "/etc/openssl/certs/ca-certificates.crt",
];
/// Go's `certDirectories` (`root_bsd.go`).
#[cfg(any(
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
pub(crate) const DEFAULT_DIRS: &[&str] = &[
    "/etc/ssl/certs",
    "/usr/local/share/certs",
    "/etc/openssl/certs",
];

/// Other Unix systems: no defaults here, so only the variables or the fallback.
#[cfg(all(
    unix,
    not(target_vendor = "apple"),
    not(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd"
    ))
))]
pub(crate) const DEFAULT_FILES: &[&str] = &[];
/// Other Unix systems: no defaults here, so only the variables or the fallback.
#[cfg(all(
    unix,
    not(target_vendor = "apple"),
    not(any(
        target_os = "linux",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd"
    ))
))]
pub(crate) const DEFAULT_DIRS: &[&str] = &[];

/// The files and directories to read, after the variables have had their say.
#[cfg(all(unix, not(target_vendor = "apple")))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Paths {
    files: Vec<PathBuf>,
    dirs: Vec<PathBuf>,
}

#[cfg(all(unix, not(target_vendor = "apple")))]
impl Paths {
    /// Go's choice: a set variable replaces its own list only.
    pub(crate) fn resolve(
        file_var: Option<OsString>,
        dir_var: Option<OsString>,
        default_files: &[&str],
        default_dirs: &[&str],
    ) -> Self {
        let files = match file_var.filter(|value| !value.is_empty()) {
            Some(file) => vec![PathBuf::from(file)],
            None => default_files.iter().map(PathBuf::from).collect(),
        };
        let dirs = match dir_var.filter(|value| !value.is_empty()) {
            // Go splits on ':' and keeps empty parts, which then fail to open and are skipped.
            Some(dirs) => std::env::split_paths(&dirs).collect(),
            None => default_dirs.iter().map(PathBuf::from).collect(),
        };
        Self { files, dirs }
    }

    /// Reads the roots, falling back to Mozilla's when none were found.
    pub(crate) fn load(&self) -> Roots {
        let mut found = Found::default();
        // Only the first file that can be read, even when it holds no certificate.
        if let Some(pem) = self.files.iter().find_map(|file| std::fs::read(file).ok()) {
            found.add_pem(&pem);
        }
        for dir in &self.dirs {
            let Ok(entries) = std::fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                if is_same_dir_symlink(&entry.path()) {
                    continue;
                }
                if let Ok(pem) = std::fs::read(entry.path()) {
                    found.add_pem(&pem);
                }
            }
        }
        found.into_roots()
    }
}

/// Go's `isSameDirSymlink`: a link whose target names no directory.
#[cfg(all(unix, not(target_vendor = "apple")))]
fn is_same_dir_symlink(path: &Path) -> bool {
    let is_link = std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink());
    is_link
        && std::fs::read_link(path)
            .is_ok_and(|target| !target.as_os_str().as_encoded_bytes().contains(&b'/'))
}

/// Certificates collected so far, each one checked as a trust anchor.
#[cfg(all(unix, not(target_vendor = "apple")))]
struct Found {
    certs: Vec<CertificateDer<'static>>,
    anchors: rustls::RootCertStore,
}

#[cfg(all(unix, not(target_vendor = "apple")))]
impl Default for Found {
    fn default() -> Self {
        Self {
            certs: Vec::new(),
            anchors: rustls::RootCertStore::empty(),
        }
    }
}

#[cfg(all(unix, not(target_vendor = "apple")))]
impl Found {
    /// Go's `AppendCertsFromPEM`: every CERTIFICATE block that parses; other
    /// blocks and text between them are ignored. One difference: Go reads on
    /// past a malformed block, here it ends the file.
    fn add_pem(&mut self, pem: &[u8]) {
        use rustls::pki_types::pem::PemObject;
        for cert in CertificateDer::pem_slice_iter(pem).map_while(Result::ok) {
            if self.anchors.add(cert.clone()).is_ok() {
                self.certs.push(cert);
            }
        }
    }

    fn into_roots(mut self) -> Roots {
        // A bundle and the directory that links to it hold the same roots twice.
        self.certs
            .sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
        self.certs.dedup();
        if self.certs.is_empty() {
            Roots::Mozilla
        } else {
            Roots::Certs(self.certs)
        }
    }
}

/// The rustls configuration for a reqwest client the store builds, on ring,
/// the provider ureq uses too.
///
/// # Errors
/// When the protocol versions or the platform verifier cannot be set up.
#[cfg(feature = "reqwest")]
pub(crate) fn rustls_config(roots: Roots) -> Result<rustls::ClientConfig, rustls::Error> {
    use std::sync::Arc;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
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
