//! The seam between the sans-IO code and a real HTTP client.
//!
//! A [`Transport`] sends one [`HttpRequest`] and returns the response it got,
//! whatever the status. Only a failure to get a response at all is an error,
//! and [`TransportError`] carries kinds only, never a URL, a header or a body.

use std::fmt;
use std::io;

use thiserror::Error;

use crate::request::HttpRequest;

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

/// The largest response body read: twice the largest ranged read (a test pins
/// the relation), which is room for a listing page or a delete result and no more.
#[cfg(any(feature = "ureq", feature = "reqwest"))]
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
pub trait Transport: Send + Sync {
    /// Sends `request`.
    ///
    /// # Errors
    /// [`TransportError`] when no response arrived.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;
}

#[cfg(all(test, any(feature = "ureq", feature = "reqwest")))]
mod tests {
    use lsr_core::ReadLen;

    use super::*;

    #[test]
    fn body_cap_is_twice_the_largest_read() {
        assert_eq!(MAX_BODY, 2 * u64::from(ReadLen::MAX_RAW));
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
