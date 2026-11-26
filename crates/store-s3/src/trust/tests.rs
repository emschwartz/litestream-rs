//! The store's trust rules, checked against a local TLS server whose certificate
//! chains to a private certificate authority made here with the `openssl`
//! command (on Linux and macOS by default). Nothing leaves 127.0.0.1.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use url::Url;

use crate::request::{HttpRequest, Method};
use crate::transport::{Transport, TransportError};

const CNF: &str = "\
[req]
distinguished_name = dn
prompt = no
[dn]
CN = lsr test ca
[test_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
[test_leaf]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = IP:127.0.0.1
";

const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
const DEADLINE: Duration = Duration::from_secs(10);

fn openssl(dir: &Path, args: &[&str]) {
    let out = Command::new("openssl")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("the openssl command");
    assert!(
        out.status.success(),
        "openssl {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A self-signed CA certificate named `<name>.pem` in `dir`, its key beside it.
fn make_ca(dir: &Path, name: &str) -> PathBuf {
    std::fs::write(dir.join("t.cnf"), CNF).expect("write config");
    let key = format!("{name}.key");
    let pem = format!("{name}.pem");
    openssl(
        dir,
        &[
            "ecparam",
            "-name",
            "prime256v1",
            "-genkey",
            "-noout",
            "-out",
            &key,
        ],
    );
    openssl(
        dir,
        &[
            "req",
            "-new",
            "-x509",
            "-config",
            "t.cnf",
            "-extensions",
            "test_ca",
            "-key",
            &key,
            "-days",
            "2",
            "-out",
            &pem,
        ],
    );
    dir.join(pem)
}

/// A private CA and a leaf for 127.0.0.1 that it signed.
struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path();
        make_ca(path, "ca");
        openssl(
            path,
            &[
                "ecparam",
                "-name",
                "prime256v1",
                "-genkey",
                "-noout",
                "-out",
                "leaf.key",
            ],
        );
        openssl(
            path,
            &[
                "req",
                "-new",
                "-config",
                "t.cnf",
                "-key",
                "leaf.key",
                "-subj",
                "/CN=127.0.0.1",
                "-out",
                "leaf.csr",
            ],
        );
        openssl(
            path,
            &[
                "x509",
                "-req",
                "-in",
                "leaf.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-set_serial",
                "2",
                "-days",
                "2",
                "-extfile",
                "t.cnf",
                "-extensions",
                "test_leaf",
                "-out",
                "leaf.pem",
            ],
        );
        Self { dir }
    }

    fn ca(&self) -> PathBuf {
        self.dir.path().join("ca.pem")
    }

    /// Serves HTTPS on 127.0.0.1: one 200 per connection whose handshake
    /// completes; a client that refuses the certificate just goes away.
    fn serve(&self) -> Url {
        let chain = CertificateDer::pem_file_iter(self.dir.path().join("leaf.pem"))
            .expect("leaf")
            .collect::<Result<Vec<_>, _>>()
            .expect("leaf certificate");
        let key = PrivateKeyDer::from_pem_file(self.dir.path().join("leaf.key")).expect("key");
        let config = Arc::new(
            rustls::ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("versions")
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .expect("server config"),
        );
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let Ok(conn) = rustls::ServerConnection::new(Arc::clone(&config)) else {
                    continue;
                };
                let mut tls = rustls::StreamOwned::new(conn, stream);
                let mut buf = [0_u8; 4096];
                if tls.read(&mut buf).is_ok_and(|n| n > 0) {
                    let _wrote = tls.write_all(OK);
                    tls.conn.send_close_notify();
                    let _flushed = tls.flush();
                }
            }
        });
        Url::parse(&format!("https://{addr}/key")).expect("url")
    }
}

fn get(transport: &dyn Transport, url: &Url) -> Result<u16, TransportError> {
    transport
        .send(HttpRequest::for_test(Method::Get, url.clone(), Vec::new()))
        .map(|response| response.status())
}

/// Child half of `the_default_clients_read_the_process_environment`: run by
/// that test in a process of its own, with the variables it sets. Run alone,
/// without them, it has nothing to check.
#[test]
#[ignore = "run by the_default_clients_read_the_process_environment in a child process"]
fn env_child() {
    let Some(url) = std::env::var_os("LSR_TRUST_CHILD_URL") else {
        return;
    };
    let url = Url::parse(&url.to_string_lossy()).expect("url");
    let want = match std::env::var("LSR_TRUST_CHILD_EXPECT").as_deref() {
        Ok("trusted") => Ok(200),
        _ => Err(TransportError::Tls),
    };
    #[cfg(feature = "ureq")]
    assert_eq!(
        get(
            &crate::UreqTransport::new(DEADLINE).expect("a TLS provider"),
            &url
        ),
        want,
        "ureq"
    );
    #[cfg(feature = "reqwest")]
    assert_eq!(
        get(
            &crate::ReqwestTransport::new(DEADLINE).expect("transport"),
            &url
        ),
        want,
        "reqwest"
    );
}

/// What `run_child` sets: `SSL_CERT_FILE` and `SSL_CERT_DIR` as given, or unset.
#[derive(Default, Clone, Copy)]
struct Vars<'a> {
    file: Option<&'a Path>,
    dir: Option<&'a Path>,
}

/// Runs `env_child` in a fresh process with the variables as given, expecting
/// `expect` from every client the store builds.
fn run_child(url: &Url, vars: Vars<'_>, expect: &str) {
    let mut child = Command::new(std::env::current_exe().expect("test binary"));
    child
        .args([
            "--exact",
            "trust::tests::env_child",
            "--ignored",
            "--nocapture",
        ])
        .env("LSR_TRUST_CHILD_URL", url.as_str())
        .env("LSR_TRUST_CHILD_EXPECT", expect)
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR");
    if let Some(file) = vars.file {
        child.env("SSL_CERT_FILE", file);
    }
    if let Some(dir) = vars.dir {
        child.env("SSL_CERT_DIR", dir);
    }
    let out = child.output().expect("run the child");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "child expecting {expect}: {stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `UreqTransport::new` and `ReqwestTransport::new` read the real variables: a private CA
/// the system lacks is refused, and trusted once `SSL_CERT_FILE` or `SSL_CERT_DIR` names it.
#[test]
fn the_default_clients_read_the_process_environment() {
    let pki = Pki::new();
    let url = pki.serve();
    run_child(&url, Vars::default(), "refused");
    // macOS and Windows ask the operating system and read neither variable.
    let with_var = if cfg!(all(unix, not(target_vendor = "apple"))) {
        "trusted"
    } else {
        "refused"
    };
    let ca = pki.ca();
    run_child(
        &url,
        Vars {
            file: Some(&ca),
            dir: None,
        },
        with_var,
    );
    run_child(
        &url,
        Vars {
            file: None,
            dir: Some(pki.dir.path()),
        },
        with_var,
    );
}

/// Variables that name nothing leave no system roots, so Mozilla's are the fallback (Linux
/// and the BSDs): they still refuse the private CA.
#[test]
fn variables_naming_nothing_fall_back_to_mozilla_roots_and_refuse_the_private_ca() {
    let pki = Pki::new();
    let missing = pki.dir.path().join("absent");
    run_child(
        &pki.serve(),
        Vars {
            file: Some(&missing),
            dir: Some(&missing),
        },
        "refused",
    );
}

/// An application's reqwest client on native-tls, which refuses the private CA: the
/// failure is a native-tls error, not a rustls one, and is still `Tls` (a
/// certificate problem the operator fixes, never a connection to retry).
#[cfg(feature = "native-tls")]
#[test]
fn a_native_tls_app_client_refusing_the_certificate_is_tls() {
    let pki = Pki::new();
    let client = reqwest::Client::builder()
        .tls_backend_native()
        .build()
        .expect("client");
    let transport = crate::ReqwestTransport::with_client(client, DEADLINE).expect("transport");
    assert_eq!(get(&transport, &pki.serve()), Err(TransportError::Tls));
}

/// Child half of `an_installed_default_provider_is_the_one_the_clients_use`: a process of its
/// own, because installing a process default happens once and stays. Run alone, without the
/// variables, it has nothing to check.
#[cfg(feature = "ureq")]
#[test]
#[ignore = "run by an_installed_default_provider_is_the_one_the_clients_use in a child process"]
fn provider_child() {
    let Some(url) = std::env::var_os("LSR_PROVIDER_CHILD_URL") else {
        return;
    };
    let url = Url::parse(&url.to_string_lossy()).expect("url");
    let installed = std::env::var("LSR_PROVIDER_CHILD_MODE").as_deref() == Ok("installed");
    if installed {
        // A provider that verifies no signature: a client on it refuses every certificate.
        let mut provider = rustls::crypto::aws_lc_rs::default_provider();
        provider.signature_verification_algorithms = rustls::crypto::WebPkiSupportedAlgorithms {
            all: &[],
            mapping: &[],
        };
        provider.install_default().expect("install");
        let chosen = super::crypto_provider().expect("provider");
        assert!(Arc::ptr_eq(
            &chosen,
            rustls::crypto::CryptoProvider::get_default().expect("default")
        ));
    }
    let want = if !installed && cfg!(all(unix, not(target_vendor = "apple"))) {
        Ok(200)
    } else {
        Err(TransportError::Tls)
    };
    let transport = crate::UreqTransport::new(DEADLINE).expect("a TLS provider");
    assert_eq!(get(&transport, &url), want);
}

/// An application that installs its own provider as rustls's process default gets TLS on that
/// one: the control child trusts the private CA and handshakes (Linux and the BSDs; elsewhere
/// the operating system's verifier refuses the CA either way), the child with a provider that
/// verifies no signature refuses it.
#[cfg(feature = "ureq")]
#[test]
fn an_installed_default_provider_is_the_one_the_clients_use() {
    let pki = Pki::new();
    let url = pki.serve();
    let ca = pki.ca();
    for mode in ["control", "installed"] {
        let out = Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "trust::tests::provider_child",
                "--ignored",
                "--nocapture",
            ])
            .env("LSR_PROVIDER_CHILD_URL", url.as_str())
            .env("LSR_PROVIDER_CHILD_MODE", mode)
            .env("SSL_CERT_FILE", &ca)
            .env_remove("SSL_CERT_DIR")
            .output()
            .expect("run the child");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success() && stdout.contains("1 passed"),
            "{mode} child: {stdout}{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

/// What tells one provider from another: its key exchange groups (aws-lc-rs leads with the
/// post-quantum hybrid).
fn groups(provider: &rustls::crypto::CryptoProvider) -> Vec<String> {
    provider
        .kx_groups
        .iter()
        .map(|group| format!("{:?}", group.name()))
        .collect()
}

/// With nothing installed, the clients take the one provider rustls's features enable (the dev
/// dependency enables aws-lc-rs only).
#[test]
fn the_one_provider_rustls_enables_is_used_when_nothing_is_installed() {
    let chosen = super::crypto_provider().expect("provider");
    assert_eq!(
        groups(&chosen),
        groups(&rustls::crypto::aws_lc_rs::default_provider())
    );
}
