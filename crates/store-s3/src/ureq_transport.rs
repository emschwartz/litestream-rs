//! The blocking HTTP client behind [`Transport`]: one [`ureq::Agent`].
//!
//! Policy lives in the agent's configuration, not in calls: a non-2xx status
//! is a response (classification belongs to [`classify`](crate::classify)),
//! redirects are not followed (a redirect from a presigned URL is a
//! misconfigured endpoint, and the retry layers decide what to do), nothing is
//! retried, and [`timeout_global`] bounds each request end to end. TLS is
//! rustls on ring, trusting what Go's `crypto/x509` trusts (the system store,
//! with `SSL_CERT_FILE` and `SSL_CERT_DIR` on Linux; see the `trust` module);
//! ureq speaks HTTP/1.1 only.
//!
//! A batch runs on scoped threads ([`send_on_threads`]) over the one agent, so
//! its requests share the connection pool and each keeps the deadline.
//!
//! Response bodies are read whole but bounded: a body over `MAX_BODY` is an
//! error, so a server that ignores a Range header cannot make a read allocate
//! an object's size.

use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

#[cfg(all(unix, not(target_vendor = "apple")))]
use std::sync::Arc;

use ureq::http;
#[cfg(all(unix, not(target_vendor = "apple")))]
use ureq::tls::Certificate;
use ureq::tls::RootCerts;

use crate::request::{HttpRequest, Method};
use crate::transport::{
    HttpResponse, MAX_BODY, Transport, TransportError, map_io, send_on_threads,
};
use crate::trust::{self, Roots};

/// A [`Transport`] over one shared connection pool.
#[derive(Clone)]
pub struct UreqTransport {
    agent: ureq::Agent,
}

impl std::fmt::Debug for UreqTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UreqTransport").finish_non_exhaustive()
    }
}

impl UreqTransport {
    /// A client whose every request fails as [`TransportError::Timeout`] after
    /// `deadline`. On Linux and the BSDs it trusts the system's root certificates
    /// (`SSL_CERT_FILE` and `SSL_CERT_DIR` when set), or Mozilla's roots when the system
    /// has none; that set can differ from Go's on an unusual system. On macOS and Windows
    /// it asks the operating system's verifier.
    #[must_use]
    pub fn new(deadline: Duration) -> Self {
        let tls = ureq::tls::TlsConfig::builder()
            .root_certs(root_certs(trust::system()))
            .build();
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .max_redirects_will_error(false)
            .timeout_global(Some(deadline))
            .tls_config(tls)
            .build();
        Self {
            agent: ureq::Agent::new_with_config(config),
        }
    }

    fn build(request: &HttpRequest) -> Result<http::Request<Vec<u8>>, TransportError> {
        let mut builder = http::Request::builder()
            .method(method_name(request.method()))
            .uri(request.url().as_str());
        for (name, value) in request.headers() {
            // ureq frames the body from its length, which is the value that was signed.
            if *name != "content-length" {
                builder = builder.header(*name, value.as_str());
            }
        }
        builder
            .body(request.body().to_vec())
            .map_err(|_malformed| TransportError::Io(io::ErrorKind::InvalidInput))
    }
}

fn root_certs(roots: Roots) -> RootCerts {
    match roots {
        #[cfg(all(unix, not(target_vendor = "apple")))]
        Roots::Certs(certs) => RootCerts::Specific(Arc::new(
            certs
                .iter()
                .map(|cert| Certificate::from_der(cert.as_ref()).to_owned())
                .collect(),
        )),
        #[cfg(all(unix, not(target_vendor = "apple")))]
        Roots::Mozilla => RootCerts::WebPki,
        #[cfg(not(all(unix, not(target_vendor = "apple"))))]
        Roots::Platform => RootCerts::PlatformVerifier,
    }
}

fn method_name(method: Method) -> &'static str {
    match method {
        Method::Get => "GET",
        Method::Head => "HEAD",
        Method::Put => "PUT",
        Method::Post => "POST",
        Method::Delete => "DELETE",
    }
}

impl Transport for UreqTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let head_only = request.method() == Method::Head;
        let mut response = self
            .agent
            .run(Self::build(&request)?)
            .map_err(|err| map_error(&err))?;
        let status = response.status().as_u16();
        let headers: Vec<(String, String)> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_owned(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_BODY)
            .read_to_vec()
            .map_err(|err| map_error(&err))?;
        let declared = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<u64>().ok());
        let complete = head_only || declared.is_none_or(|n| u64::try_from(body.len()) == Ok(n));
        Ok(HttpResponse::new(status, headers, body, complete))
    }

    fn send_many(
        &self,
        requests: Vec<HttpRequest>,
        limit: NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        send_on_threads(self, requests, limit)
    }
}

/// Maps a ureq failure to a kind; the error's own text can carry the URL.
/// `ureq::Error` is non-exhaustive, so this tests variants instead of matching
/// them: anything unrecognised is an opaque I/O failure.
fn map_error(err: &ureq::Error) -> TransportError {
    if matches!(err, ureq::Error::Timeout(_)) {
        TransportError::Timeout
    } else if matches!(
        err,
        ureq::Error::HostNotFound | ureq::Error::ConnectionFailed
    ) {
        TransportError::Connect
    } else if matches!(err, ureq::Error::Tls(_) | ureq::Error::Rustls(_)) {
        TransportError::Tls
    } else if let ureq::Error::Io(io_err) = err
        && io_err
            .get_ref()
            .is_some_and(|inner| inner.is::<rustls::Error>())
    {
        // A refused certificate reaches ureq as rustls' error inside an `io::Error`.
        TransportError::Tls
    } else if let ureq::Error::Io(io_err) = err {
        map_io(io_err.kind())
    } else if matches!(err, ureq::Error::BodyExceedsLimit(_)) {
        TransportError::Io(io::ErrorKind::InvalidData)
    } else {
        TransportError::Io(io::ErrorKind::Other)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    fn serve_once(reply: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0_u8; 2048];
            let _read = stream.read(&mut buf);
            let _wrote = stream.write_all(reply);
        });
        format!("http://{addr}/")
    }

    fn status_of(url: &str) -> Result<u16, TransportError> {
        let transport = UreqTransport::new(Duration::from_secs(5));
        let request = http::Request::builder()
            .uri(url)
            .body(Vec::new())
            .map_err(|_bad| TransportError::Io(io::ErrorKind::InvalidInput))?;
        let response = transport
            .agent
            .run(request)
            .map_err(|err| map_error(&err))?;
        Ok(response.status().as_u16())
    }

    #[test]
    fn a_redirect_is_a_response_not_followed() {
        let url = serve_once(
            b"HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:1/\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
        assert_eq!(status_of(&url), Ok(302));
    }

    /// Accepts one connection and holds it open without answering.
    fn serve_silence() -> (String, std::sync::mpsc::Sender<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (done, wait) = std::sync::mpsc::channel::<()>();
        thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept");
            let _released = wait.recv();
        });
        (format!("http://{addr}/"), done)
    }

    fn get(url: &str) -> HttpRequest {
        HttpRequest::for_test(Method::Get, url::Url::parse(url).expect("url"), Vec::new())
    }

    const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";

    #[test]
    fn a_batch_answers_in_order_and_each_request_keeps_its_deadline() {
        let (silent, release) = serve_silence();
        let transport = UreqTransport::new(Duration::from_millis(300));
        let batch = vec![get(&serve_once(OK)), get(&silent), get(&serve_once(OK))];
        let got: Vec<Result<Vec<u8>, TransportError>> = transport
            .send_many(batch, NonZeroUsize::new(3).expect("limit"))
            .into_iter()
            .map(|r| r.map(|resp| resp.body().to_vec()))
            .collect();
        assert_eq!(
            got,
            vec![
                Ok(b"ok".to_vec()),
                Err(TransportError::Timeout),
                Ok(b"ok".to_vec())
            ]
        );
        let _released = release.send(());
    }

    #[test]
    fn a_connection_closed_early_is_an_error() {
        assert_ne!(status_of(&serve_once(b"")), Ok(200));
    }
}
