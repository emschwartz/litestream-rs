//! The blocking HTTP client behind [`Transport`]: one [`ureq::Agent`].
//!
//! Policy lives in the agent's configuration, not in calls: a non-2xx status
//! is a response (classification belongs to [`classify`](crate::classify)),
//! redirects are not followed (a redirect from a presigned URL is a
//! misconfigured endpoint, and the retry layers decide what to do), nothing is
//! retried, and each request is bounded by progress, not by one deadline (see below). TLS is
//! rustls on ring, trusting what Go's `crypto/x509` trusts (the system store,
//! with `SSL_CERT_FILE` and `SSL_CERT_DIR` on Linux; see the `trust` module);
//! ureq speaks HTTP/1.1 only.
//!
//! Bounds (D235): the deadline given to [`UreqTransport::new`] is the longest the request may go
//! with no progress, as ureq measures it: connecting, each socket read, each socket write, and the
//! wait for the response head once the body is sent. A request is also capped in all at the deadline
//! plus the time its body, and the body of its answer by the length it declares, need at 16 KiB a
//! second ([`Bounds`]), so a peer that takes a byte a minute still ends.
//!
//! A batch runs on scoped threads ([`send_on_threads`]) and the parts of uploads on the
//! store's sender threads ([`Sender`]), all over the one agent, so their requests share the
//! connection pool and each keeps its bounds.
//!
//! Response bodies are read whole but bounded: a body over `MAX_BODY` is an
//! error, so a server that ignores a Range header cannot make a read allocate
//! an object's size.

use std::io::{self, Read};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use ureq::http;
#[cfg(all(unix, not(target_vendor = "apple")))]
use ureq::tls::Certificate;
use ureq::tls::RootCerts;

use crate::lanes::{Lanes, Sender};
use crate::request::{HttpRequest, Method};
use crate::transport::{Bounds, Opened};
use crate::transport::{
    HttpResponse, MAX_BODY, StreamedRequest, Transport, TransportError, map_io, send_on_threads,
};
use crate::trust::{self, Roots};

/// Idle connections the pool keeps: the widest batch the store sends at once.
const POOL_IDLE: usize = crate::CREATION_TIME_CONCURRENCY.get();

/// Bytes of a connection's read buffer, and the largest response head: S3's heads are under
/// 2 KiB, and a body is read through the buffer in as many reads as it takes. Reads cost no CPU
/// at 32 KiB that 128 KiB saves (a 1 GiB restore on a local MinIO: 0.97 s of CPU either way).
const CONN_BUFFER: usize = 32 * 1024;

/// Bytes of a connection's write buffer, ureq's own default: a part body leaves through it, so
/// at 32 KiB a 1 GiB upload spent 0.14 s more CPU (2.12 s against 1.98 s, medians of five
/// interleaved runs) in four times the writes.
const WRITE_BUFFER: usize = 128 * 1024;

/// A [`Transport`] over one shared connection pool.
#[derive(Clone)]
pub struct UreqTransport {
    agent: ureq::Agent,
    bounds: Bounds,
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
        let bounds = Bounds::new(deadline);
        let stall = Some(bounds.stall());
        let config = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .max_redirects_will_error(false)
            // A request with no body ends within the stall bound in all, as one deadline made
            // every request end. A body raises the cap per request (`UreqTransport::capped`).
            .timeout_global(stall)
            .timeout_connect(stall)
            .timeout_recv_response(stall)
            .timeout_per_read(stall)
            .timeout_per_write(stall)
            // ureq keeps 3 idle connections per host, fewer than the batches the store sends
            // (up to `CREATION_TIME_CONCURRENCY`), so most requests of a restore opened a
            // socket and, over TLS, a handshake. The pool holds as many as the widest batch.
            .max_idle_connections(POOL_IDLE)
            .max_idle_connections_per_host(POOL_IDLE)
            // Each connection, idle ones too, holds an input and an output buffer of this
            // size (ureq's 128 KiB each): 16 of them were 4 MiB of an idle process.
            .input_buffer_size(CONN_BUFFER)
            .output_buffer_size(WRITE_BUFFER)
            .max_response_header_size(CONN_BUFFER)
            .tls_config(tls)
            .build();
        Self {
            agent: ureq::Agent::new_with_config(config),
            bounds,
        }
    }

    /// The same transport with another floor under its uploads' speed, for a test.
    #[cfg(test)]
    fn with_floor(mut self, bytes_per_second: std::num::NonZeroU64) -> Self {
        self.bounds = self.bounds.with_floor(bytes_per_second);
        self
    }

    /// `request` with the cap on its whole life that a body of `body_len` bytes allows: the
    /// agent's own bounds only cut a request that makes no progress.
    fn capped<S: ureq::AsSendBody>(
        &self,
        request: http::Request<S>,
        body_len: u64,
    ) -> http::Request<S> {
        // A GET's answer is read under its own cap once its length is declared
        // (`read_body`); this one is the ceiling over request and answer together.
        let answer = if request.method() == http::Method::GET {
            MAX_BODY
        } else {
            0
        };
        self.agent
            .configure_request(request)
            .timeout_global(Some(self.bounds.cap(body_len.saturating_add(answer))))
            .build()
    }

    /// The request as ureq takes it. The body is borrowed from `request`, not copied: a
    /// multipart part is megabytes, and an upload sends a batch of them at once.
    fn build(request: &HttpRequest) -> Result<http::Request<&[u8]>, TransportError> {
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
            .body(request.body())
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

/// A response's headers as received.
fn head_of(response: &http::Response<ureq::Body>) -> Vec<(String, String)> {
    response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
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

/// Reads the response body, bounded by `MAX_BODY`, into a buffer sized to the `expected`
/// length the server declared. Growing one from empty doubles it, so a 4 MiB ranged
/// read would hold an 8 MiB allocation, and a restore keeps eight of them in flight.
///
/// The body may take the time `expected` bytes need at the floor rate, from its first byte
/// (`Bounds::cap`); a read that makes no progress ends sooner, at ureq's per-read bound.
#[expect(
    clippy::disallowed_methods,
    reason = "a socket timeout runs on the real clock, which no injected clock reaches"
)]
fn read_body(
    response: &mut http::Response<ureq::Body>,
    expected: u64,
    bounds: Bounds,
) -> Result<Vec<u8>, TransportError> {
    let allowed = bounds.cap(expected);
    let began = std::time::Instant::now();
    let mut body = Vec::with_capacity(usize::try_from(expected).unwrap_or(0));
    let mut reader = response.body_mut().with_config().limit(MAX_BODY).reader();
    let mut buf = [0_u8; CONN_BUFFER];
    loop {
        let read = reader
            .read(&mut buf)
            .map_err(|err| map_error(&ureq::Error::from(err)))?;
        let Some(chunk) = buf.get(..read).filter(|chunk| !chunk.is_empty()) else {
            return Ok(body);
        };
        body.extend_from_slice(chunk);
        if began.elapsed() > allowed {
            return Err(TransportError::Timeout);
        }
    }
}

impl UreqTransport {
    /// One request and its whole answer.
    fn exchange(
        &self,
        request: http::Request<impl ureq::AsSendBody>,
        head_only: bool,
    ) -> Result<HttpResponse, TransportError> {
        let mut response = self.agent.run(request).map_err(|err| map_error(&err))?;
        let status = response.status().as_u16();
        let headers = head_of(&response);
        let declared = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse::<u64>().ok());
        // A HEAD answer declares the length of a body it does not send.
        let expected = declared
            .filter(|_| !head_only)
            .map_or(0, |n| n.min(MAX_BODY));
        let body = read_body(&mut response, expected, self.bounds)?;
        let complete = head_only || declared.is_none_or(|n| u64::try_from(body.len()) == Ok(n));
        Ok(HttpResponse::new(status, headers, body, complete))
    }
}

/// A streamed GET's body: read from the connection as the caller asks, each socket read bounded
/// by ureq's per-read stall bound, and the whole body by `allowed` from its head, the time its
/// declared length needs at the floor rate (`Bounds::cap`). A 4 GiB body may take the stall
/// bound plus about 73 hours; a body that stops moving fails at the stall bound.
struct Streamed {
    reader: ureq::BodyReader<'static>,
    began: std::time::Instant,
    allowed: Duration,
}

impl crate::transport::Body for Streamed {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        if self.began.elapsed() > self.allowed {
            return Err(TransportError::Timeout);
        }
        self.reader
            .read(buf)
            .map_err(|err| map_error(&ureq::Error::from(err)))
    }
}

impl Transport for UreqTransport {
    #[expect(
        clippy::disallowed_methods,
        reason = "a socket timeout runs on the real clock, which no injected clock reaches"
    )]
    fn open(&self, request: HttpRequest) -> Result<Opened<'_>, TransportError> {
        // No ceiling over the whole exchange, which would cut a long body: connecting, sending
        // and the wait for the head each have the stall bound, and the body its own cap below.
        let built = self
            .agent
            .configure_request(Self::build(&request)?)
            .timeout_global(None)
            .build();
        let response = self.agent.run(built).map_err(|err| map_error(&err))?;
        let head = HttpResponse::new(
            response.status().as_u16(),
            head_of(&response),
            Vec::new(),
            true,
        );
        let body = Streamed {
            allowed: self
                .bounds
                .cap(response.body().content_length().unwrap_or(0)),
            reader: response.into_body().into_reader(),
            began: std::time::Instant::now(),
        };
        Ok((head, Box::new(body)))
    }

    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let head_only = request.method() == Method::Head;
        let len = u64::try_from(request.body().len()).unwrap_or(u64::MAX);
        self.exchange(self.capped(Self::build(&request)?, len), head_only)
    }

    /// The body is read from the request's reader as it goes: the parts of an upload that
    /// kept no buffer. The `Content-Length` that was signed frames it, so a body that fails
    /// part-way leaves the server short of a whole one.
    fn send_streamed(
        &self,
        mut streamed: StreamedRequest<'_>,
    ) -> Result<HttpResponse, TransportError> {
        let mut builder = http::Request::builder()
            .method(method_name(streamed.request().method()))
            .uri(streamed.request().url().as_str());
        for (name, value) in streamed.request().headers() {
            builder = builder.header(*name, value.as_str());
        }
        let len = streamed.len();
        let body: &mut dyn io::Read = streamed.body();
        let request = builder
            .body(ureq::SendBody::from_reader(body))
            .map_err(|_malformed| TransportError::Io(io::ErrorKind::InvalidInput))?;
        self.exchange(self.capped(request, len), false)
    }

    fn lanes(&self, most: NonZeroUsize) -> Arc<dyn Lanes> {
        Arc::new(Sender::new(most))
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

    const MIB: usize = 1024 * 1024;

    /// Half a MiB a second: the floor the upload tests hold a request to, so that a cap that
    /// scales with the body is seconds here and not minutes.
    fn floor() -> std::num::NonZeroU64 {
        std::num::NonZeroU64::new(524_288).expect("nonzero")
    }

    fn upload(url: &str, len: usize) -> HttpRequest {
        HttpRequest::new(
            Method::Put,
            url::Url::parse(url).expect("url"),
            vec![("content-length", len.to_string())],
            vec![7; len],
        )
    }

    /// The server reads 4 MiB at about 1 MiB a second, so the request takes about 4 s on a
    /// 3 s stall bound (the socket buffers hold some of it, a second's worth at most): it never stalls, and it is not cut for taking longer than the bound.
    #[test]
    fn an_upload_that_keeps_moving_outlives_the_stall_bound() {
        use crate::transport::slow_servers::serve_paced_reader;
        let (url, served) = serve_paced_reader(4 * MIB, Duration::from_millis(62));
        let transport = UreqTransport::new(Duration::from_secs(3)).with_floor(floor());
        let response = transport
            .send(upload(&url, 4 * MIB))
            .expect("an upload that moves is not a timeout");
        assert_eq!(response.status(), 200);
        assert_eq!(served.join().expect("server"), 4 * MIB);
    }

    /// A server that never reads fails the write at the stall bound, not at the cap the body's
    /// length allows (64 s here).
    #[test]
    fn a_body_the_server_never_reads_times_out_at_the_stall_bound() {
        use crate::transport::slow_servers::{serve_never_reading, timed};
        let (url, release) = serve_never_reading();
        let transport = UreqTransport::new(Duration::from_millis(300)).with_floor(floor());
        let (err, took) = timed(|| transport.send(upload(&url, 32 * MIB)).err());
        assert_eq!(err, Some(TransportError::Timeout));
        assert!(took < Duration::from_secs(5), "{took:?}");
        let _released = release.send(());
    }

    /// A download that keeps moving is not cut for taking longer than the deadline: 1 MiB comes
    /// in 16 KiB pieces 31 ms apart (about 2 s) on a 1 s deadline.
    #[test]
    fn a_download_that_keeps_moving_outlives_the_stall_bound() {
        use crate::transport::slow_servers::serve_paced_writer;
        let url = serve_paced_writer(MIB, 16 * 1024, Duration::from_millis(31));
        let transport = UreqTransport::new(Duration::from_secs(1)).with_floor(floor());
        let response = transport
            .send(get(&url))
            .expect("a download that moves is not a timeout");
        assert_eq!(response.body().len(), MIB);
        assert!(response.body_complete());
    }

    /// A server that takes the whole body and never answers fails at the stall bound after it.
    #[test]
    fn a_server_that_never_answers_times_out_at_the_stall_bound() {
        use crate::transport::slow_servers::timed;
        let (url, release) = serve_silence();
        let transport = UreqTransport::new(Duration::from_millis(300)).with_floor(floor());
        let (err, took) = timed(|| transport.send(get(&url)).err());
        assert_eq!(err, Some(TransportError::Timeout));
        assert!(took < Duration::from_secs(5), "{took:?}");
        let _released = release.send(());
    }

    /// A server that answers every request on a connection (keep-alive) and counts the
    /// connections it accepted.
    /// A keep-alive server that answers no request until `together` requests wait for an
    /// answer at once, so a batch of that width is in flight whole, on as many connections,
    /// however the client's threads are scheduled. Counts the connections it accepted.
    fn serve_keep_alive(
        together: usize,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let accepted = std::sync::Arc::new(AtomicUsize::new(0));
        let count = std::sync::Arc::clone(&accepted);
        let gate = std::sync::Arc::new(std::sync::Barrier::new(together));
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let _seen = count.fetch_add(1, Ordering::SeqCst);
                let gate = std::sync::Arc::clone(&gate);
                thread::spawn(move || {
                    let mut stream = stream;
                    let mut head = Vec::new();
                    let mut byte = [0_u8; 1];
                    while stream.read(&mut byte).is_ok_and(|n| n == 1) {
                        head.push(byte[0]);
                        if head.ends_with(b"\r\n\r\n") {
                            head.clear();
                            let _leader = gate.wait();
                            let reply = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
                            if stream.write_all(reply).is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        (format!("http://{addr}/"), accepted)
    }

    /// A batch as wide as the widest the store sends finds its connections again in the next
    /// one: the pool keeps as many idle as a batch runs, so a restore does not open a socket
    /// (and, over TLS, a handshake) for most of its requests.
    #[test]
    fn a_batch_reuses_the_connections_the_last_batch_left() {
        use std::sync::atomic::Ordering;
        let width = crate::READ_CONCURRENCY;
        let (url, accepted) = serve_keep_alive(width.get());
        let transport = UreqTransport::new(Duration::from_secs(5));
        for _round in 0..4 {
            let batch = (0..width.get()).map(|_| get(&url)).collect();
            let answers = transport.send_many(batch, width);
            assert!(
                answers
                    .iter()
                    .all(|a| a.as_ref().is_ok_and(|r| r.status() == 200))
            );
        }
        let opened = accepted.load(Ordering::SeqCst);
        assert!(
            opened <= width.get(),
            "{opened} connections for four batches of {width}"
        );
    }

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

    /// Accepts one connection, reads what it is sent until the client stops or `want` body
    /// bytes arrived, answers 200 and hands back everything it read.
    fn serve_put(want: usize) -> (String, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let served = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut got = Vec::new();
            let mut buf = [0_u8; 64 * 1024];
            loop {
                let head = got.windows(4).position(|w| w == b"\r\n\r\n");
                if head.is_some_and(|at| got.len().saturating_sub(at).saturating_sub(4) >= want) {
                    break;
                }
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => return got,
                    Ok(read) => got.extend_from_slice(&buf[..read]),
                }
            }
            let _wrote = stream.write_all(OK);
            got
        });
        (format!("http://{addr}/"), served)
    }

    fn put(url: &str, len: usize) -> HttpRequest {
        let headers = vec![("content-length", len.to_string())];
        HttpRequest::for_test(Method::Put, url::Url::parse(url).expect("url"), headers)
    }

    /// A body that gives `whole` in reads of at most 1000 bytes and then fails or ends.
    struct Gives {
        whole: Vec<u8>,
        given: usize,
        then_fails: bool,
    }

    impl Read for Gives {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let rest = &self.whole[self.given..];
            if rest.is_empty() && self.then_fails {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }
            let count = rest.len().min(buf.len()).min(1000);
            buf[..count].copy_from_slice(&rest[..count]);
            self.given = self.given.saturating_add(count);
            Ok(count)
        }
    }

    /// A streamed request reaches the socket with the length it declared and the bytes its
    /// body gave, read as they are sent.
    #[test]
    fn a_streamed_body_arrives_whole_under_its_declared_length() {
        let whole: Vec<u8> = (0..300_000_u32).map(|n| n.to_le_bytes()[0]).collect();
        let (url, served) = serve_put(whole.len());
        let transport = UreqTransport::new(Duration::from_secs(5));
        let mut body = Gives {
            whole: whole.clone(),
            given: 0,
            then_fails: false,
        };
        let streamed = StreamedRequest::new(put(&url, whole.len()), 300_000, &mut body);
        let response = transport.send_streamed(streamed).expect("sent");
        assert_eq!(response.body(), b"ok");
        let got = served.join().expect("server");
        let head = got
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("head")
            .saturating_add(4);
        let text = String::from_utf8_lossy(&got[..head]).to_ascii_lowercase();
        assert!(text.contains("content-length: 300000\r\n"), "{text}");
        assert!(!text.contains("transfer-encoding"), "{text}");
        assert!(got[head..] == whole[..]);
    }

    /// A body that fails part-way fails the request, and the server was sent fewer bytes
    /// than the request declared: no server takes that as the whole body.
    #[test]
    fn a_streamed_body_that_fails_leaves_the_server_short_of_its_length() {
        let (url, served) = serve_put(300_000);
        let transport = UreqTransport::new(Duration::from_secs(5));
        let mut body = Gives {
            whole: vec![7; 100_000],
            given: 0,
            then_fails: true,
        };
        let streamed = StreamedRequest::new(put(&url, 300_000), 300_000, &mut body);
        let sent = transport.send_streamed(streamed);
        assert!(sent.is_err(), "{sent:?}");
        let got = served.join().expect("server");
        let head = got
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map_or(0, |at| at.saturating_add(4));
        let body = got.len().saturating_sub(head);
        assert!(body <= 100_000, "{body} body bytes");
    }

    /// A bare bucket's empty path segment survives the `url` type, the `http` crate and ureq, in
    /// both URL styles and every request form.
    #[test]
    fn a_bare_buckets_leading_slash_reaches_the_socket() {
        use crate::config::UrlStyle;
        use crate::transport::wire_proof::{assert_bare_bucket_arrived, sent_by};
        let transport = UreqTransport::new(Duration::from_secs(5));
        for style in [UrlStyle::Path, UrlStyle::VirtualHost] {
            assert_bare_bucket_arrived(&sent_by(&transport, style), style);
        }
    }

    #[test]
    fn a_connection_closed_early_is_an_error() {
        assert_ne!(status_of(&serve_once(b"")), Ok(200));
    }
}
