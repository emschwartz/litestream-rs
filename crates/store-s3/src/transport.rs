//! The seam between the sans-IO code and a real HTTP client.
//!
//! A [`Transport`] sends one [`HttpRequest`] and returns the response it got,
//! whatever the status. Only a failure to get a response at all is an error,
//! and [`TransportError`] carries kinds only, never a URL, a header or a body.
//!
//! [`Transport::send_many`] sends a batch of reads, listings or HEADs with at most
//! `limit` requests in flight and returns one result per request, in order. Its
//! default sends them one after another, so the in-memory and fault-injecting
//! transports stay deterministic; the socket clients override it. A blocking client
//! gets concurrency from [`send_on_threads`]: up to `limit` scoped threads for the
//! batch, all joined before it returns, so no thread outlives the call. Each request
//! keeps its own deadline, and what a status means is still judged per request by
//! the layers above.
//!
//! The parts of an upload are not a batch: each is sent when it is whole, on the
//! lanes [`Transport::lanes`] gives the store (`lanes.rs`), while the upload builds
//! the next.

use std::fmt;
use std::io;
#[cfg(any(feature = "ureq", feature = "reqwest"))]
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, PoisonError};
#[cfg(any(feature = "ureq", feature = "reqwest"))]
use std::time::Duration;

// Under `--cfg shuttle` (`just shuttle`) the batch runs on shuttle's scheduler, which explores
// thread interleavings; every other build uses std.
#[cfg(shuttle)]
use shuttle::{sync::Mutex, thread};
#[cfg(not(shuttle))]
use std::sync::Mutex;
#[cfg(not(shuttle))]
use std::thread;

use thiserror::Error;

use crate::lanes::{Inline, Lanes};
use crate::request::HttpRequest;
#[cfg(any(feature = "ureq", feature = "reqwest"))]
use crate::request::Method;

/// A write whose body is read while it is sent: a part, or a small object, that the upload
/// did not keep and reads again from its source (`again.rs`). The request carries every
/// header to send, `Content-Length` among them, and no body of its own.
pub struct StreamedRequest<'a> {
    request: HttpRequest,
    len: u64,
    body: &'a mut (dyn io::Read + Send),
}

impl<'a> StreamedRequest<'a> {
    /// `request`, to be sent with the `len` bytes `body` yields.
    pub(crate) fn new(request: HttpRequest, len: u64, body: &'a mut (dyn io::Read + Send)) -> Self {
        Self { request, len, body }
    }

    /// The request: method, URL and headers. Its own body is empty.
    #[must_use]
    pub fn request(&self) -> &HttpRequest {
        &self.request
    }

    /// How many bytes the body is: the `Content-Length` the request declares.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// Whether the body is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The body, to read exactly [`Self::len`] bytes from. An error from it means the
    /// request must fail: it never ends early without one.
    pub fn body(&mut self) -> &mut (dyn io::Read + Send) {
        &mut *self.body
    }

    /// The same request with its body read whole into memory, for a transport that sends
    /// only buffers.
    ///
    /// # Errors
    /// [`TransportError::Io`] with the kind of the body's failure, `UnexpectedEof` when it
    /// ended before its length, or `OutOfMemory` when there is no room for it.
    pub fn into_buffered(self) -> Result<HttpRequest, TransportError> {
        let Self {
            mut request,
            len,
            body,
        } = self;
        let room =
            usize::try_from(len).map_err(|_wide| TransportError::Io(io::ErrorKind::OutOfMemory))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(room)
            .map_err(|_full| TransportError::Io(io::ErrorKind::OutOfMemory))?;
        let mut limited = io::Read::take(body, len);
        io::Read::read_to_end(&mut limited, &mut bytes)
            .map_err(|err| TransportError::Io(err.kind()))?;
        if bytes.len() != room {
            return Err(TransportError::Io(io::ErrorKind::UnexpectedEof));
        }
        request.set_body(bytes);
        Ok(request)
    }
}

impl fmt::Debug for StreamedRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamedRequest")
            .field("request", &self.request)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

/// A response as received: status, headers, body.
#[derive(Clone, PartialEq, Eq)]
pub struct HttpResponse {
    status: u16,
    // The fault layer rewrites a response it forwarded.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
    pub(crate) body_complete: bool,
}

impl HttpResponse {
    /// A response. `body_complete` is `false` when the connection ended before
    /// the body reached its `Content-Length`.
    #[must_use]
    pub fn new(
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
        body_complete: bool,
    ) -> Self {
        Self {
            status,
            headers,
            body,
            body_complete,
        }
    }

    /// The HTTP status.
    #[must_use]
    pub fn status(&self) -> u16 {
        self.status
    }

    /// The first header named `name`, compared case-insensitively.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(have, _)| have.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The body.
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// The body, moved out of the response: a caller that keeps it need not copy it.
    #[must_use]
    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    /// Every header, as received.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// Whether the whole body arrived.
    #[must_use]
    pub fn body_complete(&self) -> bool {
        self.body_complete
    }
}

/// Prints the status and the body length: headers carry ETags and metadata,
/// bodies carry listings.
impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("body_len", &self.body.len())
            .field("body_complete", &self.body_complete)
            .finish_non_exhaustive()
    }
}

/// A response body read while the caller uses it ([`Transport::open`]).
pub trait Body: Send {
    /// Up to `buf.len()` more bytes; 0 only at the end of the body, which may be short of the
    /// length the response declared when the connection ended early.
    ///
    /// # Errors
    /// [`TransportError`]: the connection failed, or the body passed its time bound.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError>;
}

/// A body already held whole: [`Transport::open`]'s default.
struct Held(io::Cursor<Vec<u8>>);

impl Body for Held {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, TransportError> {
        io::Read::read(&mut self.0, buf).map_err(|err| TransportError::Io(err.kind()))
    }
}

/// What [`Transport::open`] got: the status and headers (an empty body), and the body to read.
/// The body borrows nothing, so a reader can hold it between ticks (a compaction part-way).
pub type Opened = (HttpResponse, Box<dyn Body>);

/// Why no response came back.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Error)]
pub enum TransportError {
    /// The request's deadline passed.
    #[error("request timed out")]
    Timeout,
    /// The peer reset or closed the connection.
    #[error("connection reset")]
    ConnectionReset,
    /// No connection could be made (DNS, refused, unreachable).
    #[error("could not connect")]
    Connect,
    /// The TLS handshake or certificate check failed.
    #[error("tls failure")]
    Tls,
    /// Any other I/O failure, by kind.
    #[error("i/o error ({0})")]
    Io(io::ErrorKind),
}

/// How long a request may take, by what it moves rather than by the clock alone.
///
/// A request fails as [`TransportError::Timeout`] when it makes no progress for `stall`: no
/// connection, no response head after its body was sent, or (where the client can tell) no write
/// or read. Progress is not enough on its own, because a peer that takes a byte a minute never
/// stalls, so a whole request also has a cap: `stall` plus the time the bytes it moves need at `floor`
/// bytes a second: an upload's body, a download's declared length. A request that moves no bytes
/// is capped at `stall`, as it was when one deadline bounded every request (D235).
#[cfg(any(feature = "ureq", feature = "reqwest"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Bounds {
    stall: Duration,
    floor: NonZeroU64,
}

#[cfg(any(feature = "ureq", feature = "reqwest"))]
impl Bounds {
    /// The slowest upload that is not a failure: 16 KiB a second, so a 5 MiB part may take
    /// 320 s and a 64 MiB one about 68 minutes (Go has no floor, only 24 hours in all).
    const DEFAULT_FLOOR: NonZeroU64 = NonZeroU64::MIN.saturating_add(16 * 1024 - 1);

    /// Bounds that fail a request after `stall` without progress.
    pub(crate) const fn new(stall: Duration) -> Self {
        Self {
            stall,
            floor: Self::DEFAULT_FLOOR,
        }
    }

    /// The same bounds with a different floor, for a test that cannot wait minutes.
    #[cfg(test)]
    pub(crate) const fn with_floor(self, bytes_per_second: NonZeroU64) -> Self {
        Self {
            floor: bytes_per_second,
            ..self
        }
    }

    /// How long a request may go without progress.
    pub(crate) const fn stall(self) -> Duration {
        self.stall
    }

    /// The most a request with a body of `body_len` bytes may take in all.
    pub(crate) fn cap(self, body_len: u64) -> Duration {
        let nanos = u128::from(body_len)
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(self.floor.get()))
            .unwrap_or(u128::MAX);
        self.stall.saturating_add(Duration::from_nanos(
            u64::try_from(nanos).unwrap_or(u64::MAX),
        ))
    }

    /// The most `request` may take in all: its body, plus for a GET the largest answer that
    /// is held whole (an answer's own length is capped again once it is declared).
    pub(crate) fn for_request(self, request: &HttpRequest) -> Duration {
        let answer = if request.method() == Method::Get {
            MAX_BODY
        } else {
            0
        };
        self.cap(
            u64::try_from(request.body().len())
                .unwrap_or(u64::MAX)
                .saturating_add(answer),
        )
    }
}

/// The time a response body may take from its head, for the length it declares
/// ([`Bounds::cap`]). Checked between reads: a body that stops moving fails at the client's
/// own stall bound instead.
#[cfg(any(feature = "ureq", feature = "reqwest"))]
pub(crate) struct BodyCap {
    began: std::time::Instant,
    allowed: Duration,
}

#[cfg(any(feature = "ureq", feature = "reqwest"))]
impl BodyCap {
    /// Starts the clock for a body of `declared` bytes.
    #[expect(
        clippy::disallowed_methods,
        reason = "a socket timeout runs on the real clock, which no injected clock reaches"
    )]
    pub(crate) fn start(bounds: Bounds, declared: u64) -> Self {
        Self {
            began: std::time::Instant::now(),
            allowed: bounds.cap(declared),
        }
    }

    /// [`TransportError::Timeout`] once the body has taken longer than it may.
    pub(crate) fn check(&self) -> Result<(), TransportError> {
        if self.began.elapsed() > self.allowed {
            Err(TransportError::Timeout)
        } else {
            Ok(())
        }
    }
}

/// The length a response declares and whether it has a body at all, for a client that reads
/// whole answers.
#[cfg(any(feature = "ureq", feature = "reqwest"))]
#[derive(Clone, Copy)]
pub(crate) struct Declared {
    len: Option<u64>,
    /// A HEAD answer declares the length of a body it does not send.
    head_only: bool,
}

#[cfg(any(feature = "ureq", feature = "reqwest"))]
impl Declared {
    pub(crate) const fn new(len: Option<u64>, head_only: bool) -> Self {
        Self { len, head_only }
    }

    /// The bytes to expect, at most `MAX_BODY`.
    pub(crate) fn expected(self) -> u64 {
        self.len
            .filter(|_| !self.head_only)
            .map_or(0, |n| n.min(MAX_BODY))
    }

    /// Whether `body_len` bytes are all the answer declared.
    pub(crate) fn complete(self, body_len: usize) -> bool {
        self.head_only || self.len.is_none_or(|n| u64::try_from(body_len) == Ok(n))
    }
}

/// The largest response body held whole: twice the largest ranged read (a test pins
/// the relation), which is room for a listing page or a delete result and no more. A
/// streamed body ([`Transport::open`]) is read a piece at a time and has no such limit.
pub(crate) const MAX_BODY: u64 = 33_554_432;

/// Maps an I/O failure kind to a transport kind, for every [`Transport`] built on sockets.
#[cfg(any(feature = "ureq", feature = "reqwest"))]
pub(crate) fn map_io(kind: io::ErrorKind) -> TransportError {
    if matches!(kind, io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock) {
        TransportError::Timeout
    } else if matches!(
        kind,
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof
    ) {
        TransportError::ConnectionReset
    } else if matches!(
        kind,
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NotConnected
            | io::ErrorKind::AddrNotAvailable
    ) {
        TransportError::Connect
    } else {
        TransportError::Io(kind)
    }
}

/// A blocking HTTP client. It follows no redirects and retries nothing: both
/// are decisions for the layers above.
pub trait Transport: Send + Sync + 'static {
    /// Sends `request`.
    ///
    /// # Errors
    /// [`TransportError`] when no response arrived.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;

    /// Sends a GET whose answer's body the caller reads as it uses it, however long it is: a
    /// restore's whole object. A transport over sockets overrides it, so the body is read from
    /// the connection, bounded by progress and by its declared length at the floor rate, never
    /// by `MAX_BODY`. The default answers with [`Self::send`], the body held whole: what the
    /// in-memory and fault-injecting transports need.
    ///
    /// # Errors
    /// [`TransportError`] when no response arrived.
    fn open(&self, request: HttpRequest) -> Result<Opened, TransportError> {
        let mut response = self.send(request)?;
        let body = std::mem::take(&mut response.body);
        // The head is whole; a body cut short shows as fewer bytes than declared.
        response.body_complete = true;
        Ok((response, Box::new(Held(io::Cursor::new(body)))))
    }

    /// Sends every request with at most `limit` in flight and returns their
    /// results in request order. The default sends them one at a time.
    fn send_many(
        &self,
        requests: Vec<HttpRequest>,
        _limit: NonZeroUsize,
    ) -> Vec<Result<HttpResponse, TransportError>> {
        requests
            .into_iter()
            .map(|request| self.send(request))
            .collect()
    }

    /// [`Self::send`] for a request whose body is read while it is sent. The default reads
    /// the body whole and sends it as any request, so a transport that does not override it
    /// sends what it always sent and holds one body for it.
    ///
    /// # Errors
    /// [`TransportError`] when no response arrived, or the body's own failure: the request is
    /// then not sent, or ends short of the length it declared.
    fn send_streamed(&self, request: StreamedRequest<'_>) -> Result<HttpResponse, TransportError> {
        self.send(request.into_buffered()?)
    }

    /// The lanes the parts of this store's uploads are sent on, at most `most` at once
    /// (`lanes.rs`). The default has no thread: a part is sent by the call that started it,
    /// so the in-memory and fault-injecting transports stay deterministic. The socket clients
    /// give a [`Sender`](crate::Sender).
    fn lanes(&self, most: NonZeroUsize) -> Arc<dyn Lanes> {
        let _ = most;
        Arc::new(Inline)
    }
}

/// What a request whose send panicked ends as, whichever thread ran it.
pub(crate) const PANICKED: TransportError = TransportError::Io(io::ErrorKind::Other);

/// The stack of a batch worker thread: an HTTPS request through ureq and rustls needs 48 KiB
/// (see `lsr-replica`'s `stack` module), and this is about five times that.
// Shuttle's threads have no stack size to set.
#[cfg(not(shuttle))]
pub(crate) const BATCH_STACK: usize = 256 * 1024;

type Slots = Vec<Option<Result<HttpResponse, TransportError>>>;

/// Starts one batch helper in `scope`; `None` when the OS refuses the thread.
#[cfg(not(shuttle))]
fn spawn_helper<'scope, F: FnOnce() + Send + 'scope>(
    scope: &'scope thread::Scope<'scope, '_>,
    work: F,
) -> Option<thread::ScopedJoinHandle<'scope, ()>> {
    thread::Builder::new()
        .name("s3-batch".to_owned())
        .stack_size(BATCH_STACK)
        .spawn_scoped(scope, work)
        .ok()
}

/// Shuttle's scoped threads have no builder, and its threads never fail to start.
#[cfg(shuttle)]
fn spawn_helper<'scope, F: FnOnce() + Send + 'scope>(
    scope: &'scope thread::Scope<'scope, '_>,
    work: F,
) -> Option<thread::ScopedJoinHandle<'scope, ()>> {
    Some(scope.spawn(work))
}

/// [`Transport::send_many`] for a blocking transport: the calling thread and up to `limit - 1`
/// scoped threads (`s3-batch`, `BATCH_STACK` each) take requests from one queue in order. Every
/// thread is joined before this returns. A thread the OS refuses only narrows the batch. A
/// request whose send panicked is reported in its slot as an I/O failure, and the thread goes on.
pub fn send_on_threads<T: Transport + ?Sized>(
    transport: &T,
    requests: Vec<HttpRequest>,
    limit: NonZeroUsize,
) -> Vec<Result<HttpResponse, TransportError>> {
    let send = |request| transport.send(request);
    let count = requests.len();
    let helpers = limit.get().min(count).saturating_sub(1);
    if helpers == 0 {
        return requests.into_iter().map(send).collect();
    }
    let queue = Mutex::new(requests.into_iter().enumerate());
    let slots: Mutex<Slots> = Mutex::new((0..count).map(|_| None).collect());
    let work = || {
        loop {
            let next = queue.lock().unwrap_or_else(PoisonError::into_inner).next();
            let Some((at, request)) = next else { break };
            let sent = panic::catch_unwind(AssertUnwindSafe(|| send(request)));
            let result = sent.unwrap_or(Err(PANICKED));
            if let Some(slot) = slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_mut(at)
            {
                *slot = Some(result);
            }
        }
    };
    thread::scope(|scope| {
        let handles: Vec<_> = (0..helpers)
            .filter_map(|_| spawn_helper(scope, work))
            .collect();
        work();
        for handle in handles {
            // Sends cannot panic out of `work`, so a join has nothing to report.
            match handle.join() {
                Ok(()) | Err(_) => {}
            }
        }
    });
    slots
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner)
        .into_iter()
        .map(|slot| slot.unwrap_or(Err(PANICKED)))
        .collect()
}

#[cfg(test)]
pub(crate) mod slow_servers;
#[cfg(test)]
pub(crate) mod wire_proof;

#[cfg(all(test, any(feature = "ureq", feature = "reqwest")))]
mod tests {
    use lsr_core::ReadLen;

    use super::*;

    #[test]
    fn body_cap_is_twice_the_largest_read() {
        assert_eq!(MAX_BODY, 2 * u64::from(ReadLen::MAX_RAW));
    }

    #[test]
    fn a_requests_cap_grows_with_its_body_at_the_floor() {
        let bounds = Bounds::new(Duration::from_secs(60));
        assert_eq!(bounds.stall(), Duration::from_secs(60));
        assert_eq!(bounds.cap(0), Duration::from_secs(60));
        assert_eq!(bounds.cap(5 * 1024 * 1024), Duration::from_secs(380));
        let fast = bounds.with_floor(NonZeroU64::new(1_000_000).expect("nonzero"));
        assert_eq!(fast.cap(1_500_000), Duration::from_millis(61_500));
        // A restore streams a 4 GiB snapshot in one GET: 2^32 bytes at 16 KiB a second is
        // 262 144 s (about 73 hours) past the stall bound, so only a stall or a crawl cuts it.
        assert_eq!(bounds.cap(4 << 30), Duration::from_secs(60 + 262_144));
        // A length no body has saturates instead of wrapping.
        assert!(fast.cap(u64::MAX) > Duration::from_secs(1_000_000_000));
    }

    #[test]
    fn io_kinds_map_to_transport_kinds() {
        assert_eq!(map_io(io::ErrorKind::TimedOut), TransportError::Timeout);
        assert_eq!(
            map_io(io::ErrorKind::BrokenPipe),
            TransportError::ConnectionReset
        );
        assert_eq!(
            map_io(io::ErrorKind::ConnectionRefused),
            TransportError::Connect
        );
        assert_eq!(
            map_io(io::ErrorKind::PermissionDenied),
            TransportError::Io(io::ErrorKind::PermissionDenied)
        );
    }
}

// Not under shuttle: its gauge blocks on std's condition variable, which shuttle cannot schedule.
#[cfg(test)]
#[cfg(not(shuttle))]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the gauge counts a few dozen requests"
)]
mod batch_tests {
    use std::sync::Condvar;

    use url::Url;

    use super::*;
    use crate::request::Method;

    /// Counts requests in flight. Each send waits until `limit` are in flight
    /// at once (or every request has started), so a batch that can reach its
    /// limit does, and the peak shows whether it ever passed it. A path that
    /// starts `/fail` gets an error, `/panic` a panic.
    struct Gauge {
        limit: usize,
        total: usize,
        state: Mutex<(usize, usize, usize, bool)>,
        changed: Condvar,
    }

    impl Gauge {
        fn new(limit: usize, total: usize) -> Self {
            Self {
                limit,
                total,
                state: Mutex::new((0, 0, 0, false)),
                changed: Condvar::new(),
            }
        }

        fn peak(&self) -> usize {
            self.state.lock().unwrap().2
        }
    }

    impl Transport for Gauge {
        #[expect(
            clippy::panic_in_result_fn,
            reason = "a `/panic` path panics on purpose"
        )]
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let (in_flight, started, peak, released) = &mut *state;
            *in_flight += 1;
            *started += 1;
            *peak = (*peak).max(*in_flight);
            if *in_flight >= self.limit || *started == self.total {
                *released = true;
            }
            self.changed.notify_all();
            let mut state = self
                .changed
                .wait_while(state, |s| !s.3)
                .unwrap_or_else(PoisonError::into_inner);
            state.0 -= 1;
            drop(state);
            let path = request.url().path().to_owned();
            if path.starts_with("/fail") {
                return Err(TransportError::ConnectionReset);
            }
            assert!(!path.starts_with("/panic"), "worker panic");
            Ok(HttpResponse::new(200, Vec::new(), path.into_bytes(), true))
        }
    }

    fn batch(paths: &[&str]) -> Vec<HttpRequest> {
        paths
            .iter()
            .map(|path| {
                let url = Url::parse(&format!("http://127.0.0.1{path}")).unwrap();
                HttpRequest::for_test(Method::Get, url, Vec::new())
            })
            .collect()
    }

    fn bodies(
        results: &[Result<HttpResponse, TransportError>],
    ) -> Vec<Result<String, TransportError>> {
        results
            .iter()
            .map(|r| {
                r.as_ref()
                    .map(|resp| String::from_utf8(resp.body().to_vec()).unwrap())
                    .map_err(|err| *err)
            })
            .collect()
    }

    #[test]
    fn a_batch_reaches_its_limit_and_never_passes_it() {
        let paths: Vec<String> = (0..23).map(|n| format!("/{n}")).collect();
        let names: Vec<&str> = paths.iter().map(String::as_str).collect();
        for limit in [1, 2, 3, 8] {
            let gauge = Gauge::new(limit, names.len());
            let got = send_on_threads(&gauge, batch(&names), NonZeroUsize::new(limit).unwrap());
            assert_eq!(gauge.peak(), limit, "limit {limit}");
            let want: Vec<Result<String, TransportError>> = paths.iter().cloned().map(Ok).collect();
            assert_eq!(bodies(&got), want, "results come back in request order");
        }
    }

    #[test]
    fn a_failed_request_is_reported_in_its_own_slot() {
        let gauge = Gauge::new(4, 5);
        let got = send_on_threads(
            &gauge,
            batch(&["/a", "/fail", "/b", "/panic", "/c"]),
            NonZeroUsize::new(4).unwrap(),
        );
        assert_eq!(
            bodies(&got),
            vec![
                Ok("/a".to_owned()),
                Err(TransportError::ConnectionReset),
                Ok("/b".to_owned()),
                Err(TransportError::Io(io::ErrorKind::Other)),
                Ok("/c".to_owned()),
            ]
        );
    }

    #[test]
    fn the_default_batch_is_sequential() {
        let gauge = Gauge::new(1, 3);
        let got = gauge.send_many(batch(&["/x", "/y", "/z"]), NonZeroUsize::new(8).unwrap());
        assert_eq!(gauge.peak(), 1);
        assert_eq!(bodies(&got).len(), 3);
    }
}

/// Schedule exploration of [`send_on_threads`] (`just shuttle`): many random interleavings, which
/// the real-thread tests above sample only by luck. Each run checks the invariants its doc promises.
#[cfg(test)]
#[cfg(shuttle)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the probe counts a few requests"
)]
mod shuttle_tests {
    use shuttle::sync::atomic::{AtomicUsize, Ordering};
    use url::Url;

    use super::*;
    use crate::request::Method;

    fn iterations() -> usize {
        std::env::var("SHUTTLE_ITERATIONS")
            .ok()
            .and_then(|n| n.parse().ok())
            .unwrap_or(200)
    }

    /// A transport that yields inside every send, so the scheduler may switch threads there.
    /// It counts sends per request, the peak in flight, and finished sends.
    struct Probe {
        sent: Vec<AtomicUsize>,
        in_flight: AtomicUsize,
        peak: AtomicUsize,
        finished: AtomicUsize,
    }

    impl Probe {
        fn new(count: usize) -> Self {
            Self {
                sent: (0..count).map(|_| AtomicUsize::new(0)).collect(),
                in_flight: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                finished: AtomicUsize::new(0),
            }
        }
    }

    impl Transport for Probe {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            let path = request.url().path().to_owned();
            let at: usize = path
                .trim_start_matches("/fail")
                .trim_start_matches("/panic")
                .trim_start_matches('/')
                .parse()
                .unwrap_or(usize::MAX);
            if let Some(count) = self.sent.get(at) {
                count.fetch_add(1, Ordering::SeqCst);
            }
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            thread::yield_now();
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.finished.fetch_add(1, Ordering::SeqCst);
            if path.starts_with("/fail") {
                return Err(TransportError::ConnectionReset);
            }
            Ok(HttpResponse::new(200, Vec::new(), path.into_bytes(), true))
        }
    }

    fn request(path: &str) -> HttpRequest {
        let url = Url::parse(&format!("http://127.0.0.1{path}")).unwrap();
        HttpRequest::for_test(Method::Get, url, Vec::new())
    }

    /// Request `n` is `/n` or `/fail/n`: its index rides in the path. (No panicking request here:
    /// shuttle's panic hook fails the schedule on any panic, even one `send_on_threads` catches.
    /// The std test `a_failed_request_is_reported_in_its_own_slot` covers that path.)
    fn path(n: usize) -> String {
        match n % 5 {
            1 | 3 => format!("/fail/{n}"),
            _ => format!("/{n}"),
        }
    }

    fn check(count: usize, limit: usize) {
        let probe = Probe::new(count);
        let requests = (0..count).map(|n| request(&path(n))).collect();
        let got = send_on_threads(&probe, requests, NonZeroUsize::new(limit).unwrap());
        assert_eq!(got.len(), count, "one result per request");
        for (n, result) in got.iter().enumerate() {
            let want = path(n);
            match result {
                Ok(response) => {
                    assert_eq!(
                        response.body(),
                        want.as_bytes(),
                        "slot {n} holds its own answer"
                    );
                }
                Err(err) => {
                    assert!(want.starts_with("/fail"), "slot {n} failed without cause");
                    assert_eq!(
                        *err,
                        TransportError::ConnectionReset,
                        "slot {n}'s error is its own"
                    );
                }
            }
        }
        for (n, sent) in probe.sent.iter().enumerate() {
            assert_eq!(
                sent.load(Ordering::SeqCst),
                1,
                "request {n} was sent exactly once"
            );
        }
        assert!(
            probe.peak.load(Ordering::SeqCst) <= limit,
            "never more than `limit` in flight"
        );
        assert_eq!(
            probe.in_flight.load(Ordering::SeqCst),
            0,
            "nothing in flight after return"
        );
        assert_eq!(
            probe.finished.load(Ordering::SeqCst),
            count,
            "every send ended before return"
        );
    }

    #[test]
    fn a_batch_loses_nothing_and_passes_no_limit_under_any_schedule() {
        for (count, limit) in [(5, 2), (6, 3), (4, 8), (7, 1)] {
            shuttle::check_random(move || check(count, limit), iterations());
        }
    }
}
