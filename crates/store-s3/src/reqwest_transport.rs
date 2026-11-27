//! The [`Transport`] over `reqwest`, for applications that already build it (feature `reqwest`).
//!
//! Three constructors, by how much of the application's stack to share:
//! - [`ReqwestTransport::new`]: the store's own client and its own
//!   single-threaded Tokio runtime. Nothing is shared.
//! - [`ReqwestTransport::with_client`]: the application's `reqwest::Client` (its pool,
//!   proxy and TLS roots), driven on the store's own runtime.
//! - [`ReqwestTransport::with_client_on`]: the application's client on the application's
//!   runtime, through a `tokio::runtime::Handle`.
//!
//! The store is blocking (it runs on its replicator thread), so each request
//! is driven to completion with `block_on`. Called from inside a Tokio runtime,
//! where `block_on` panics, the request runs on a scoped thread instead: slower,
//! never a panic.
//!
//! A batch is one `block_on` over a Tokio `JoinSet` on the same runtime, with
//! at most `limit` requests spawned at a time; each keeps its own timeout.
//!
//! The policy matches [`UreqTransport`](crate::UreqTransport): a non-2xx status
//! is a response, nothing is retried, and a body over `MAX_BODY` is an error. The deadline
//! bounds a connect and each chunk of an answer; a request is capped in all at the deadline plus
//! the time its body, and its answer's declared length, need at 16 KiB a second (D235). reqwest cannot bound a
//! stalled write or the wait for a response head by progress, so those are bounded by that cap,
//! not by the deadline alone as under [`UreqTransport`](crate::UreqTransport). The own
//! client follows no redirects. An application client keeps its own redirect policy;
//! reqwest's default follows up to ten, and a presigned URL redirected to
//! another host fails its signature there, so the store sees a 403 instead of
//! the redirect. Build the client with `redirect(Policy::none())` to see the
//! redirect itself.
//!
//! TLS on the own client is rustls on the provider the application or the crate features
//! choose (see [`UreqTransport::new`](crate::UreqTransport::new)), set up by the store and
//! handed to reqwest whole, trusting what Go's `crypto/x509` trusts, exactly as
//! [`UreqTransport`](crate::UreqTransport) does (the `trust` module). This
//! crate enables reqwest's `rustls-no-provider` only, so it brings no second
//! crypto provider (reqwest's default is aws-lc-rs, a C build). An application client
//! keeps its own TLS and its own roots; it needs a provider from the application's
//! reqwest features (reqwest's default `rustls` feature has one) or one the
//! application installed as the process default, or reqwest panics building it. A
//! application client on native-tls needs this crate's `native-tls` feature for its
//! TLS failures to be [`TransportError::Tls`] rather than a connection error.

use std::collections::VecDeque;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Builder, Handle, Runtime};
use tokio::task::JoinSet;

use crate::lanes::{Lanes, Sender};
use crate::request::{HttpRequest, Method};
use crate::transport::{
    BodyCap, Bounds, Declared, HttpResponse, Opened, Transport, TransportError,
};
use crate::transport::{MAX_BODY, PANICKED, map_io};
use crate::trust;

/// Why a [`ReqwestTransport`] could not be built.
#[derive(Debug, thiserror::Error)]
pub enum ReqwestSetupError {
    /// The client could not be built (TLS backend initialisation).
    #[error("http client could not be built")]
    Client,
    /// No TLS crypto provider: none is installed and rustls cannot pick one (see
    /// [`ConfigError::NoTlsProvider`](crate::ConfigError::NoTlsProvider)).
    #[error("no TLS crypto provider: install one, or enable exactly one of rustls's providers")]
    NoTlsProvider,
    /// The store's own runtime could not be started.
    #[error("tokio runtime could not be started ({0})")]
    Runtime(io::ErrorKind),
}

/// Where requests are driven.
enum Driver {
    /// A single-threaded runtime the transport owns.
    Own(Runtime),
    /// The application's runtime.
    App(Handle),
}

impl Driver {
    fn own() -> Result<Self, ReqwestSetupError> {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .map(Self::Own)
            .map_err(|err| ReqwestSetupError::Runtime(err.kind()))
    }

    /// Drives `future` to completion. A single-threaded runtime's I/O and timers
    /// run only under `Runtime::block_on`, never `Handle::block_on`, so the own
    /// runtime is driven through itself.
    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match self {
            Self::Own(runtime) => runtime.block_on(future),
            Self::App(handle) => handle.block_on(future),
        }
    }
}

/// A [`Transport`] over a `reqwest::Client`.
pub struct ReqwestTransport {
    client: reqwest::Client,
    /// Shared with the bodies of streamed GETs, which outlive the call that opened them.
    driver: Arc<Driver>,
    bounds: Bounds,
}

impl std::fmt::Debug for ReqwestTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let driver = match *self.driver {
            Driver::Own(_) => "own runtime",
            Driver::App(_) => "application runtime",
        };
        f.debug_struct("ReqwestTransport")
            .field("driver", &driver)
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}

impl ReqwestTransport {
    /// The store's own client and runtime; every request ends within `deadline`.
    /// It trusts the certificate authorities
    /// [`UreqTransport::new`](crate::UreqTransport::new) describes.
    ///
    /// # Errors
    /// [`ReqwestSetupError`] when the client or the runtime cannot be built.
    pub fn new(deadline: Duration) -> Result<Self, ReqwestSetupError> {
        let provider =
            trust::crypto_provider().map_err(|_none| ReqwestSetupError::NoTlsProvider)?;
        let tls = trust::rustls_config(trust::system(), provider)
            .map_err(|_tls| ReqwestSetupError::Client)?;
        let bounds = Bounds::new(deadline);
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            // A connect that makes no progress ends at the stall bound. reqwest has no bound
            // on a stalled write, and its read timeout starts when the request does and so
            // would cut a long upload: the cap on each request (`build`) is the bound.
            .connect_timeout(bounds.stall())
            .tls_backend_preconfigured(tls)
            .build()
            .map_err(|_build| ReqwestSetupError::Client)?;
        Ok(Self {
            client,
            driver: Arc::new(Driver::own()?),
            bounds,
        })
    }

    /// The application's client, on the store's own runtime.
    ///
    /// # Errors
    /// [`ReqwestSetupError::Runtime`] when the runtime cannot be started.
    pub fn with_client(
        client: reqwest::Client,
        deadline: Duration,
    ) -> Result<Self, ReqwestSetupError> {
        Ok(Self {
            client,
            driver: Arc::new(Driver::own()?),
            bounds: Bounds::new(deadline),
        })
    }

    /// The application's client on the application's runtime.
    #[must_use]
    pub fn with_client_on(client: reqwest::Client, runtime: Handle, deadline: Duration) -> Self {
        Self {
            client,
            driver: Arc::new(Driver::App(runtime)),
            bounds: Bounds::new(deadline),
        }
    }

    /// The same transport with another floor under its uploads' speed, for a test.
    #[cfg(test)]
    fn with_floor(mut self, bytes_per_second: std::num::NonZeroU64) -> Self {
        self.bounds = self.bounds.with_floor(bytes_per_second);
        self
    }

    fn build(&self, request: &HttpRequest) -> reqwest::RequestBuilder {
        let mut builder = self
            .client
            .request(method(request.method()), request.url().as_str())
            .timeout(self.bounds.for_request(request))
            .body(request.body().to_vec());
        // Every header is sent as signed, `content-length` included: hyper keeps a
        // given length (it equals the body's), and without it an empty PUT goes
        // out with no length at all, which Tigris refuses with 411.
        for (name, value) in request.headers() {
            builder = builder.header(*name, value.as_str());
        }
        builder
    }
}

fn method(method: Method) -> reqwest::Method {
    match method {
        Method::Get => reqwest::Method::GET,
        Method::Head => reqwest::Method::HEAD,
        Method::Put => reqwest::Method::PUT,
        Method::Post => reqwest::Method::POST,
        Method::Delete => reqwest::Method::DELETE,
    }
}

impl Driver {
    /// Drives `future` to completion. Inside a runtime, where `block_on`
    /// panics, it runs on a fresh thread, which is outside every runtime; a panic there is
    /// [`PANICKED`].
    fn drive<T, F>(&self, future: F) -> Result<T, TransportError>
    where
        T: Send,
        F: std::future::Future<Output = Result<T, TransportError>> + Send,
    {
        if Handle::try_current().is_ok() {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| self.block_on(future))
                    .join()
                    .unwrap_or(Err(PANICKED))
            })
        } else {
            self.block_on(future)
        }
    }
}

/// A streamed GET's body ([`Transport::open`]): each chunk driven to completion as the caller
/// asks, no later than the stall bound after the one before, and the whole body within the cap
/// its declared length allows from its head.
struct Streamed {
    driver: Arc<Driver>,
    stall: Duration,
    response: reqwest::Response,
    /// What is left of the chunk being handed out.
    pending: VecDeque<u8>,
    cap: BodyCap,
}

impl crate::transport::Body for Streamed {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        if self.pending.is_empty() {
            self.cap.check()?;
            let stall = self.stall;
            let response = &mut self.response;
            let next = self.driver.drive(async move {
                tokio::time::timeout(stall, response.chunk())
                    .await
                    .map_err(|_elapsed| TransportError::Timeout)?
                    .map_err(|err| map_error(&err))
                    .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
            })?;
            let Some(chunk) = next else { return Ok(0) };
            self.pending = chunk.into();
        }
        io::Read::read(&mut self.pending, buf).map_err(|err| TransportError::Io(err.kind()))
    }
}

impl Transport for ReqwestTransport {
    fn open(&self, request: HttpRequest) -> Result<Opened, TransportError> {
        // No cap over the whole exchange, which would cut a long body: the request up to its
        // head has the stall bound, and the body its own cap from there.
        let builder = self.build(&request).timeout(Duration::MAX);
        let stall = self.bounds.stall();
        let response = self.driver.drive(async move {
            tokio::time::timeout(stall, builder.send())
                .await
                .map_err(|_elapsed| TransportError::Timeout)?
                .map_err(|err| map_error(&err))
        })?;
        let head = HttpResponse::new(
            response.status().as_u16(),
            head_of(&response),
            Vec::new(),
            true,
        );
        let cap = BodyCap::start(self.bounds, response.content_length().unwrap_or(0));
        let body = Streamed {
            driver: Arc::clone(&self.driver),
            stall,
            response,
            pending: VecDeque::new(),
            cap,
        };
        Ok((head, Box::new(body)))
    }

    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let head_only = request.method() == Method::Head;
        let sent = u64::try_from(request.body().len()).unwrap_or(u64::MAX);
        let exchange = exchange(self.build(&request), head_only, self.bounds, sent);
        self.driver.drive(exchange)
    }

    /// Each sender thread drives its own request with `block_on`, as the replicator thread
    /// does. A part's body is read whole before it is sent (the trait's default).
    fn lanes(&self, most: NonZeroUsize) -> Arc<dyn Lanes> {
        Arc::new(Sender::new(most))
    }

    fn send_many(
        &self,
        requests: Vec<HttpRequest>,
        limit: NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        let count = requests.len();
        let builders: Vec<_> = requests
            .iter()
            .map(|request| {
                let sent = u64::try_from(request.body().len()).unwrap_or(u64::MAX);
                (self.build(request), request.method() == Method::Head, sent)
            })
            .collect();
        self.driver
            .drive(async { Ok(exchange_many(builders, limit, self.bounds).await) })
            .unwrap_or_else(|err| (0..count).map(|_| Err(err)).collect())
    }
}

/// Runs every exchange with at most `limit` spawned at once and returns the
/// results in request order.
async fn exchange_many(
    builders: Vec<(reqwest::RequestBuilder, bool, u64)>,
    limit: NonZeroUsize,
    bounds: Bounds,
) -> Vec<Result<HttpResponse, TransportError>> {
    let tasks = builders
        .into_iter()
        .map(|(builder, head_only, sent)| exchange(builder, head_only, bounds, sent))
        .collect();
    bounded(tasks, limit)
        .await
        .into_iter()
        .map(|slot| slot.unwrap_or(Err(PANICKED)))
        .collect()
}

/// Runs `tasks` on the current runtime with at most `limit` spawned at once.
/// Each output lands in its task's slot; a task that panicked leaves `None`.
async fn bounded<F>(tasks: Vec<F>, limit: NonZeroUsize) -> Vec<Option<F::Output>>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let mut slots: Vec<Option<F::Output>> = (0..tasks.len()).map(|_| None).collect();
    let mut queue = tasks.into_iter().enumerate();
    let mut running = JoinSet::new();
    loop {
        while running.len() < limit.get() {
            let Some((at, task)) = queue.next() else {
                break;
            };
            running.spawn(async move { (at, task.await) });
        }
        match running.join_next().await {
            Some(Ok((at, output))) => {
                if let Some(slot) = slots.get_mut(at) {
                    *slot = Some(output);
                }
            }
            Some(Err(_panicked)) => {}
            None => break,
        }
    }
    slots
}

/// One request and its whole answer, in two phases (D235): the request, up to the response head,
/// within the cap its body's length allows; then the answer's body within the cap its declared
/// length allows, no chunk later than the stall bound after the one before.
async fn exchange(
    builder: reqwest::RequestBuilder,
    head_only: bool,
    bounds: Bounds,
    sent: u64,
) -> Result<HttpResponse, TransportError> {
    let mut response = tokio::time::timeout(bounds.cap(sent), builder.send())
        .await
        .map_err(|_elapsed| TransportError::Timeout)?
        .map_err(|err| map_error(&err))?;
    let status = response.status().as_u16();
    let headers = head_of(&response);
    let declared = Declared::new(response.content_length(), head_only);
    let mut body = Vec::new();
    let read = async {
        loop {
            let next = tokio::time::timeout(bounds.stall(), response.chunk())
                .await
                .map_err(|_elapsed| TransportError::Timeout)?
                .map_err(|err| map_error(&err))?;
            let Some(chunk) = next else {
                return Ok(());
            };
            body.extend_from_slice(&chunk);
            if u64::try_from(body.len()).map_or(true, |len| len > MAX_BODY) {
                return Err(TransportError::Io(io::ErrorKind::InvalidData));
            }
        }
    };
    tokio::time::timeout(bounds.cap(declared.expected()), read)
        .await
        .map_err(|_elapsed| TransportError::Timeout)??;
    let complete = declared.complete(body.len());
    Ok(HttpResponse::new(status, headers, body, complete))
}

/// A response's headers as received.
fn head_of(response: &reqwest::Response) -> Vec<(String, String)> {
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

/// Maps a reqwest failure to a kind; the error's own text carries the URL.
fn map_error(err: &reqwest::Error) -> TransportError {
    if err.is_timeout() {
        return TransportError::Timeout;
    }
    if is_tls(err) {
        return TransportError::Tls;
    }
    if err.is_connect() {
        return TransportError::Connect;
    }
    io_kind(err).map_or(TransportError::Io(io::ErrorKind::Other), map_io)
}

/// The next error down: an `io::Error`'s wrapped error itself (its `source`
/// skips past it), else the error's `source`.
fn below<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a (dyn std::error::Error + 'static)> {
    match err.downcast_ref::<io::Error>().and_then(io::Error::get_ref) {
        Some(inner) => Some(inner),
        None => err.source(),
    }
}

/// Whether a TLS library's error is anywhere in the chain, so a certificate
/// failure is `Tls` here as it is under ureq: rustls's (hyper-rustls nests it in
/// two `io::Error`s), and with the `native-tls` feature, native-tls's, from a
/// application client built on it. reqwest has no TLS kind of its own to ask for, so
/// the chain is matched by type.
fn is_tls(err: &reqwest::Error) -> bool {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(here) = current {
        if is_tls_library_error(here) {
            return true;
        }
        current = below(here);
    }
    false
}

/// One link of the chain is a TLS library's own error.
fn is_tls_library_error(err: &(dyn std::error::Error + 'static)) -> bool {
    #[cfg(feature = "native-tls")]
    if err.downcast_ref::<native_tls::Error>().is_some() {
        return true;
    }
    err.downcast_ref::<rustls::Error>().is_some()
}

/// The kind of the innermost `io::Error` in the chain.
fn io_kind(err: &reqwest::Error) -> Option<io::ErrorKind> {
    let mut kind = None;
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(here) = current {
        if let Some(io_err) = here.downcast_ref::<io::Error>() {
            kind = Some(io_err.kind());
        }
        current = below(here);
    }
    kind
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use url::Url;

    use super::*;

    /// Serves one connection: reads the request head, writes `reply`, closes.
    fn serve_once(reply: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0_u8; 4096];
            let _read = stream.read(&mut buf);
            let _wrote = stream.write_all(reply);
        });
        format!("http://{addr}/key")
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
        (format!("http://{addr}/key"), done)
    }

    fn request(method: Method, url: &str) -> HttpRequest {
        HttpRequest::for_test(
            method,
            Url::parse(url).expect("url"),
            vec![
                ("x-test", "1".to_owned()),
                ("content-length", "0".to_owned()),
            ],
        )
    }

    fn own() -> ReqwestTransport {
        ReqwestTransport::new(Duration::from_secs(5)).expect("transport")
    }

    /// An application's client: with only `rustls-no-provider` enabled, reqwest needs
    /// a rustls config (or a process default provider) to build one.
    fn app_client() -> reqwest::Client {
        let provider = trust::crypto_provider().expect("a TLS provider");
        let tls = trust::rustls_config(trust::system(), provider).expect("tls config");
        reqwest::Client::builder()
            .tls_backend_preconfigured(tls)
            .build()
            .expect("client")
    }

    const OK: &[u8] =
        b"HTTP/1.1 200 OK\r\netag: \"abc\"\r\ncontent-length: 5\r\nconnection: close\r\n\r\nhello";

    #[test]
    fn a_response_comes_back_whole() {
        let response = own()
            .send(request(Method::Get, &serve_once(OK)))
            .expect("send");
        assert_eq!(response.status(), 200);
        assert_eq!(response.header("ETag"), Some("\"abc\""));
        assert_eq!(response.body(), b"hello");
        assert!(response.body_complete());
    }

    #[test]
    fn an_error_status_is_a_response() {
        let url = serve_once(
            b"HTTP/1.1 503 Slow Down\r\nretry-after: 2\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
        let response = own().send(request(Method::Get, &url)).expect("send");
        assert_eq!(response.status(), 503);
        assert_eq!(response.header("retry-after"), Some("2"));
    }

    #[test]
    fn a_redirect_is_a_response_not_followed() {
        let url = serve_once(
            b"HTTP/1.1 307 Temporary Redirect\r\nlocation: http://127.0.0.1:1/\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        );
        let response = own().send(request(Method::Get, &url)).expect("send");
        assert_eq!(response.status(), 307);
    }

    /// A bare bucket's empty path segment survives the `url` type and reqwest, in both URL
    /// styles and every request form.
    #[test]
    fn a_bare_buckets_leading_slash_reaches_the_socket() {
        use crate::config::UrlStyle;
        use crate::transport::wire_proof::{assert_bare_bucket_arrived, sent_by};
        let transport = own();
        for style in [UrlStyle::Path, UrlStyle::VirtualHost] {
            assert_bare_bucket_arrived(&sent_by(&transport, style), style);
        }
    }

    #[test]
    fn a_head_is_complete_without_a_body() {
        let url =
            serve_once(b"HTTP/1.1 200 OK\r\ncontent-length: 1234\r\nconnection: close\r\n\r\n");
        let response = own().send(request(Method::Head, &url)).expect("send");
        assert!(response.body_complete());
        assert!(response.body().is_empty());
    }

    #[test]
    fn an_empty_put_sends_its_length() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (head, seen) = std::sync::mpsc::channel::<String>();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut buf = [0_u8; 4096];
            let read = stream.read(&mut buf).unwrap_or(0);
            let _sent = head.send(String::from_utf8_lossy(&buf[..read]).to_ascii_lowercase());
            let _wrote = stream.write_all(OK);
        });
        let response = own()
            .send(request(Method::Put, &format!("http://{addr}/key")))
            .expect("send");
        assert_eq!(response.status(), 200);
        let head = seen.recv().expect("request head");
        assert_eq!(head.matches("content-length: 0\r\n").count(), 1, "{head}");
    }

    #[test]
    fn a_connection_closed_early_is_an_error() {
        let _closed = own()
            .send(request(Method::Get, &serve_once(b"")))
            .unwrap_err();
    }

    #[test]
    fn a_truncated_body_is_never_a_whole_response() {
        let url =
            serve_once(b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\nconnection: close\r\n\r\nabc");
        match own().send(request(Method::Get, &url)) {
            Ok(response) => assert!(!response.body_complete()),
            Err(err) => assert!(
                matches!(err, TransportError::ConnectionReset | TransportError::Io(_)),
                "{err:?}"
            ),
        }
    }

    #[test]
    fn the_deadline_bounds_a_silent_server() {
        let (url, release) = serve_silence();
        let transport = ReqwestTransport::new(Duration::from_millis(200)).expect("transport");
        assert_eq!(
            transport.send(request(Method::Get, &url)).err(),
            Some(TransportError::Timeout)
        );
        let _released = release.send(());
    }

    const MIB: usize = 1024 * 1024;

    /// Half a MiB a second: the floor the upload tests hold a request to, so that a cap that
    /// scales with the body is seconds here and not minutes.
    fn floor() -> std::num::NonZeroU64 {
        std::num::NonZeroU64::new(524_288).expect("nonzero")
    }

    fn upload(url: &str, len: usize) -> HttpRequest {
        HttpRequest::new(
            Method::Put,
            Url::parse(url).expect("url"),
            vec![("content-length", len.to_string())],
            vec![7; len],
        )
    }

    /// The server reads 4 MiB at about 1 MiB a second, so the request takes about 4 s on a
    /// 3 s stall bound (the socket buffers hold some of it, a second's worth at most): it is not cut for taking longer than the bound.
    #[test]
    fn an_upload_that_keeps_moving_outlives_the_stall_bound() {
        use crate::transport::slow_servers::serve_paced_reader;
        let (url, served) = serve_paced_reader(4 * MIB, Duration::from_millis(62));
        let transport = ReqwestTransport::new(Duration::from_secs(3))
            .expect("transport")
            .with_floor(floor());
        let response = transport
            .send(upload(&url, 4 * MIB))
            .expect("an upload that moves is not a timeout");
        assert_eq!(response.status(), 200);
        assert_eq!(served.join().expect("server"), 4 * MIB);
    }

    /// A server that never reads fails the request at the cap its body's length allows
    /// (here 300 ms and 8 MiB at 8 MiB a second; reqwest has no bound on a stalled write).
    #[test]
    fn a_body_the_server_never_reads_times_out_at_the_cap() {
        use crate::transport::slow_servers::{serve_never_reading, timed};
        let (url, release) = serve_never_reading();
        // 8 MiB at 8 MiB a second: a cap of 1.3 s.
        let transport = ReqwestTransport::new(Duration::from_millis(300))
            .expect("transport")
            .with_floor(std::num::NonZeroU64::new(8 * 1_048_576).expect("nonzero"));
        let (err, took) = timed(|| transport.send(upload(&url, 8 * MIB)).err());
        assert_eq!(err, Some(TransportError::Timeout));
        assert!(took < Duration::from_secs(15), "{took:?}");
        let _released = release.send(());
    }

    /// A download that keeps moving is not cut for taking longer than the deadline: 1 MiB comes
    /// in 16 KiB pieces 31 ms apart (about 2 s) on a 1 s deadline.
    #[test]
    fn a_download_that_keeps_moving_outlives_the_stall_bound() {
        use crate::transport::slow_servers::serve_paced_writer;
        let url = serve_paced_writer(MIB, 16 * 1024, Duration::from_millis(31));
        let transport = ReqwestTransport::new(Duration::from_secs(1))
            .expect("transport")
            .with_floor(floor());
        let response = transport
            .send(request(Method::Get, &url))
            .expect("a download that moves is not a timeout");
        assert_eq!(response.body().len(), MIB);
        assert!(response.body_complete());
    }

    #[test]
    fn a_batch_answers_in_order_and_each_request_keeps_its_deadline() {
        let (silent, release) = serve_silence();
        let transport = ReqwestTransport::new(Duration::from_millis(300)).expect("transport");
        let batch = vec![
            request(Method::Get, &serve_once(OK)),
            request(Method::Get, &silent),
            request(Method::Get, &serve_once(OK)),
        ];
        let got = transport.send_many(batch, NonZeroUsize::new(3).expect("limit"));
        let kinds: Vec<Result<Vec<u8>, TransportError>> = got
            .into_iter()
            .map(|r| r.map(|resp| resp.body().to_vec()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                Ok(b"hello".to_vec()),
                Err(TransportError::Timeout),
                Ok(b"hello".to_vec())
            ]
        );
        let _released = release.send(());
    }

    #[test]
    fn bounded_tasks_reach_the_limit_and_never_pass_it() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        for limit in [1_usize, 3, 8] {
            let in_flight = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let tasks: Vec<_> = (0..20_usize)
                .map(|n| {
                    let (in_flight, peak) = (Arc::clone(&in_flight), Arc::clone(&peak));
                    async move {
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        for _ in 0..4 {
                            tokio::task::yield_now().await;
                        }
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                        n
                    }
                })
                .collect();
            let got = runtime.block_on(bounded(tasks, NonZeroUsize::new(limit).expect("limit")));
            assert_eq!(got, (0..20).map(Some).collect::<Vec<_>>());
            assert_eq!(peak.load(Ordering::SeqCst), limit, "limit {limit}");
        }
    }

    #[test]
    fn a_tls_handshake_with_a_plain_server_is_tls() {
        let url = serve_once(OK).replacen("http://", "https://", 1);
        assert_eq!(
            own().send(request(Method::Get, &url)).err(),
            Some(TransportError::Tls)
        );
    }

    #[test]
    fn an_app_client_on_its_own_runtime() {
        let client = app_client();
        let transport =
            ReqwestTransport::with_client(client, Duration::from_secs(5)).expect("transport");
        let response = transport
            .send(request(Method::Get, &serve_once(OK)))
            .expect("send");
        assert_eq!(response.body(), b"hello");
    }

    #[test]
    fn an_app_client_on_the_app_runtime() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let transport = ReqwestTransport::with_client_on(
            app_client(),
            runtime.handle().clone(),
            Duration::from_secs(5),
        );
        let response = transport
            .send(request(Method::Get, &serve_once(OK)))
            .expect("send");
        assert_eq!(response.body(), b"hello");
    }

    #[test]
    fn a_call_from_inside_a_runtime_does_not_panic() {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let transport = own();
        let url = serve_once(OK);
        let response = runtime.block_on(async { transport.send(request(Method::Get, &url)) });
        assert_eq!(response.expect("send").body(), b"hello");
    }
}
