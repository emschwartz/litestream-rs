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

/// A blocking HTTP client. It follows no redirects and retries nothing: both
/// are decisions for the layers above.
pub trait Transport: Send + Sync {
    /// Sends `request`.
    ///
    /// # Errors
    /// [`TransportError`] when no response arrived.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;
}
