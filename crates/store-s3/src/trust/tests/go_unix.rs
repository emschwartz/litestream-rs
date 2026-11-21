//! Linux and the BSDs, where Go reads the root files itself: which files and
//! directories count, what the variables replace, the Mozilla fallback, and a
//! handshake through each source.

use super::*;

/// Roots from these lists, read as Go reads them, with no variable set.
fn load(files: &[&Path], dirs: &[&Path]) -> Roots {
    load_with(None, None, files, dirs)
}

fn load_with(
    file_var: Option<&str>,
    dir_var: Option<&str>,
    files: &[&Path],
    dirs: &[&Path],
) -> Roots {
    let files: Vec<&str> = files.iter().map(|p| p.to_str().expect("utf-8")).collect();
    let dirs: Vec<&str> = dirs.iter().map(|p| p.to_str().expect("utf-8")).collect();
    Paths::resolve(
        file_var.map(OsString::from),
        dir_var.map(OsString::from),
        &files,
        &dirs,
    )
    .load()
}

/// The DER of every certificate in a PEM file.
fn ders(pem: &Path) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_file_iter(pem)
        .expect("pem")
        .collect::<Result<Vec<_>, _>>()
        .expect("certificates")
}

/// The certificates the roots hold, sorted; empty for the fallback.
fn held(roots: &Roots) -> Vec<CertificateDer<'static>> {
    match roots {
        Roots::Certs(certs) => certs.clone(),
        Roots::Mozilla => Vec::new(),
    }
}

fn sorted(mut certs: Vec<CertificateDer<'static>>) -> Vec<CertificateDer<'static>> {
    certs.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    certs
}

/// Four CAs: `a.pem` and `b.pem` as bundle files, `d1/` and `d2/` each a
/// directory holding one more.
struct Layout {
    dir: tempfile::TempDir,
}

impl Layout {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        make_ca(dir.path(), "a");
        make_ca(dir.path(), "b");
        for sub in ["d1", "d2"] {
            let path = dir.path().join(sub);
            std::fs::create_dir(&path).expect("mkdir");
            make_ca(&path, sub);
            std::fs::remove_file(path.join(format!("{sub}.key"))).expect("rm key");
            std::fs::remove_file(path.join("t.cnf")).expect("rm cnf");
        }
        Self { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn cert(&self, name: &str) -> Vec<CertificateDer<'static>> {
        let file = match name {
            "d1" | "d2" => self.path(name).join(format!("{name}.pem")),
            _ => self.path(&format!("{name}.pem")),
        };
        ders(&file)
    }

    fn certs(&self, names: &[&str]) -> Vec<CertificateDer<'static>> {
        sorted(names.iter().flat_map(|name| self.cert(name)).collect())
    }
}

#[test]
fn only_the_first_readable_default_file_is_read_and_every_default_dir() {
    let l = Layout::new();
    let missing = l.path("missing.pem");
    let roots = load(
        &[&missing, &l.path("a.pem"), &l.path("b.pem")],
        &[&l.path("d1"), &l.path("d2")],
    );
    assert_eq!(held(&roots), l.certs(&["a", "d1", "d2"]));
}

#[test]
fn ssl_cert_file_replaces_the_default_files_and_keeps_the_default_dirs() {
    let l = Layout::new();
    let b = l.path("b.pem");
    let roots = load_with(b.to_str(), None, &[&l.path("a.pem")], &[&l.path("d1")]);
    assert_eq!(held(&roots), l.certs(&["b", "d1"]));
}

#[test]
fn ssl_cert_dir_replaces_the_default_dirs_and_splits_on_colons() {
    let l = Layout::new();
    let var = format!("{}:{}", l.path("d1").display(), l.path("d2").display());
    let roots = load_with(None, Some(&var), &[&l.path("a.pem")], &[&l.path("missing")]);
    assert_eq!(held(&roots), l.certs(&["a", "d1", "d2"]));
}

#[test]
fn an_empty_variable_counts_as_unset() {
    let l = Layout::new();
    let roots = load_with(Some(""), Some(""), &[&l.path("a.pem")], &[&l.path("d1")]);
    assert_eq!(held(&roots), l.certs(&["a", "d1"]));
}

#[test]
fn a_bundle_linked_from_a_dir_counts_once_and_junk_is_ignored() {
    let l = Layout::new();
    let d1 = l.path("d1");
    std::os::unix::fs::symlink(l.path("a.pem"), d1.join("a-link.pem")).expect("link");
    std::os::unix::fs::symlink("d1.pem", d1.join("0123abcd.0")).expect("hash link");
    std::fs::write(d1.join("README"), "not a certificate\n").expect("junk");
    std::fs::write(
        d1.join("bad.pem"),
        "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
    )
    .expect("bad pem");
    let roots = load(&[&l.path("a.pem")], &[&d1]);
    assert_eq!(held(&roots), l.certs(&["a", "d1"]));
}

/// Go's TestX509FallbackRoots: with both variables naming nothing that
/// exists, Go's pool is the bundled Mozilla set, never empty.
#[test]
fn nothing_found_falls_back_to_mozilla_roots() {
    let l = Layout::new();
    let file = l.path("nonexistent/cert.pem");
    let dir = l.path("nonexistent/certs");
    let roots = load_with(
        file.to_str(),
        dir.to_str(),
        &[&l.path("a.pem")],
        &[&l.path("d1")],
    );
    assert!(matches!(roots, Roots::Mozilla), "{roots:?}");
    let junk = l.path("junk.pem");
    std::fs::write(&junk, "no certificate here\n").expect("junk");
    assert!(matches!(load(&[&junk], &[]), Roots::Mozilla));
}

/// What a client would trust here with no variable set: the system's files.
fn system_files() -> Roots {
    Paths::resolve(None, None, DEFAULT_FILES, DEFAULT_DIRS).load()
}

fn file_var(pki: &Pki) -> Roots {
    let ca = pki.ca();
    Paths::resolve(Some(ca.into_os_string()), None, DEFAULT_FILES, DEFAULT_DIRS).load()
}

fn dir_var(pki: &Pki) -> Roots {
    let dir = pki.dir.path().join("cadir");
    std::fs::create_dir(&dir).expect("mkdir");
    std::fs::copy(pki.ca(), dir.join("ca.pem")).expect("copy");
    Paths::resolve(
        None,
        Some(dir.into_os_string()),
        DEFAULT_FILES,
        DEFAULT_DIRS,
    )
    .load()
}

#[cfg(feature = "ureq")]
#[test]
fn ureq_refuses_a_private_ca_the_system_files_lack() {
    let pki = Pki::new();
    let transport = crate::UreqTransport::with_roots(DEADLINE, system_files());
    assert_eq!(get(&transport, &pki.serve()), Err(TransportError::Tls));
}

#[cfg(feature = "ureq")]
#[test]
fn ureq_trusts_a_private_ca_named_by_ssl_cert_file() {
    let pki = Pki::new();
    let transport = crate::UreqTransport::with_roots(DEADLINE, file_var(&pki));
    assert_eq!(get(&transport, &pki.serve()), Ok(200));
}

#[cfg(feature = "ureq")]
#[test]
fn ureq_trusts_a_private_ca_in_ssl_cert_dir() {
    let pki = Pki::new();
    let transport = crate::UreqTransport::with_roots(DEADLINE, dir_var(&pki));
    assert_eq!(get(&transport, &pki.serve()), Ok(200));
}

#[cfg(feature = "reqwest")]
#[test]
fn reqwest_refuses_a_private_ca_the_system_files_lack() {
    let pki = Pki::new();
    let transport =
        crate::ReqwestTransport::with_roots(DEADLINE, system_files()).expect("transport");
    assert_eq!(get(&transport, &pki.serve()), Err(TransportError::Tls));
}

#[cfg(feature = "reqwest")]
#[test]
fn reqwest_trusts_a_private_ca_named_by_ssl_cert_file() {
    let pki = Pki::new();
    let transport =
        crate::ReqwestTransport::with_roots(DEADLINE, file_var(&pki)).expect("transport");
    assert_eq!(get(&transport, &pki.serve()), Ok(200));
}

#[cfg(feature = "reqwest")]
#[test]
fn reqwest_trusts_a_private_ca_in_ssl_cert_dir() {
    let pki = Pki::new();
    let transport =
        crate::ReqwestTransport::with_roots(DEADLINE, dir_var(&pki)).expect("transport");
    assert_eq!(get(&transport, &pki.serve()), Ok(200));
}

#[cfg(feature = "reqwest")]
#[test]
fn reqwest_falls_back_to_mozilla_roots_and_still_refuses_the_private_ca() {
    let pki = Pki::new();
    let transport =
        crate::ReqwestTransport::with_roots(DEADLINE, Roots::Mozilla).expect("transport");
    assert_eq!(get(&transport, &pki.serve()), Err(TransportError::Tls));
}
