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
                rustls::crypto::ring::default_provider(),
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
        get(&crate::UreqTransport::new(DEADLINE), &url),
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
    let ca = pki.ca();
    run_child(
        &url,
        Vars {
            file: Some(&ca),
            dir: None,
        },
        "trusted",
    );
    run_child(
        &url,
        Vars {
            file: None,
            dir: Some(pki.dir.path()),
        },
        "trusted",
    );
}

/// Variables that name nothing leave no system roots, so Mozilla's are the fallback: they
/// are not empty (a public site would verify) and still refuse the private CA.
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
