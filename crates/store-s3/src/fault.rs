//! The recorded-fault layer: a [`Transport`] wrapper that answers named
//! requests with a scripted failure, for the cases a healthy bucket never
//! produces.
//!
//! A fault applies to the next request of its [`Shape`], once, in push order.
//! Each variant models one provider behaviour: a throttle (503 SlowDown,
//! 429 with Retry-After), Tigris shedding load with 408 (Go rc.go:596-626), a
//! 500 that arrives after the object was published, a body that ends before
//! its `Content-Length`, an error document inside a 200, `Pass` to aim a fault at a
//! later request of the same shape, and Tigris accepting a Content-MD5 or checksum it did
//! not verify, so the ETag it returns belongs to other bytes. Nothing here
//! prints a URL, a header value or a body.

use std::collections::VecDeque;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::request::{HttpRequest, Method};
use crate::response::quoted_md5;
use crate::transport::{HttpResponse, Transport, TransportError};

/// The ten request shapes [`RequestBuilder`](crate::RequestBuilder) builds.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Shape {
    /// ListObjectsV2.
    List,
    /// HEAD of one object.
    Head,
    /// GET of one object.
    Get,
    /// A single PUT.
    Put,
    /// CreateMultipartUpload.
    CreateMultipart,
    /// One part of a multipart upload.
    UploadPart,
    /// CompleteMultipartUpload.
    Complete,
    /// AbortMultipartUpload.
    Abort,
    /// DeleteObjects.
    DeleteObjects,
    /// ListMultipartUploads.
    ListUploads,
}

impl Shape {
    /// The shape of `request`, `None` for a request no builder makes.
    #[must_use]
    pub fn of(request: &HttpRequest) -> Option<Self> {
        let has = |name: &str| request.url().query_pairs().any(|(have, _)| have == name);
        match request.method() {
            Method::Get if has("list-type") => Some(Self::List),
            Method::Get if has("uploads") => Some(Self::ListUploads),
            Method::Get => Some(Self::Get),
            Method::Head => Some(Self::Head),
            Method::Put if has("partNumber") => Some(Self::UploadPart),
            Method::Put => Some(Self::Put),
            Method::Post if has("uploads") => Some(Self::CreateMultipart),
            Method::Post if has("uploadId") => Some(Self::Complete),
            Method::Post if has("delete") => Some(Self::DeleteObjects),
            Method::Delete if has("uploadId") => Some(Self::Abort),
            Method::Post | Method::Delete => None,
        }
    }
}

/// One scripted failure.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Fault {
    /// Answer with `status` instead of forwarding: the request never reaches
    /// the bucket. `code` is the S3 error code in the body, `retry_after` the
    /// header's text.
    Status {
        /// The request shape it applies to.
        on: Shape,
        /// The HTTP status to answer.
        status: u16,
        /// The S3 error code in the body, if any.
        code: Option<&'static str>,
        /// The `Retry-After` header text, if any.
        retry_after: Option<&'static str>,
    },
    /// Forward the request, then answer `status` with an empty body: the
    /// object is published and the caller sees an error (a lost acknowledgement).
    AfterPublish {
        /// The request shape it applies to.
        on: Shape,
        /// The HTTP status the caller sees.
        status: u16,
    },
    /// Forward, then cut the body to its first `keep` bytes and mark it
    /// incomplete, as a client reports a body that ends before its `Content-Length`.
    TruncateBody {
        /// The request shape it applies to.
        on: Shape,
        /// How many body bytes survive.
        keep: usize,
    },
    /// Forward, then rewrite the ETag to the MD5 of other bytes: a store that
    /// accepted a digest it did not verify.
    WrongEtag {
        /// The request shape it applies to.
        on: Shape,
    },
    /// Answer 200 with an S3 error document instead of forwarding: S3 can fail
    /// a CompleteMultipartUpload after it sent the 200 status line, so the
    /// error arrives in the body. The request never reaches the bucket.
    ErrorInOkBody {
        /// The request shape it applies to.
        on: Shape,
        /// The S3 error code in the body.
        code: &'static str,
    },
    /// Forward untouched. Consumes one request of the shape, so a script can
    /// aim the next fault at the second request of that shape.
    Pass {
        /// The request shape it applies to.
        on: Shape,
    },
    /// No response at all.
    Transport {
        /// The request shape it applies to.
        on: Shape,
        /// What the client reports.
        err: TransportError,
    },
}

impl Fault {
    /// The shape this fault waits for.
    #[must_use]
    pub fn on(&self) -> Shape {
        match self {
            Self::Status { on, .. }
            | Self::AfterPublish { on, .. }
            | Self::TruncateBody { on, .. }
            | Self::WrongEtag { on }
            | Self::ErrorInOkBody { on, .. }
            | Self::Pass { on }
            | Self::Transport { on, .. } => *on,
        }
    }
}

/// What the wrapper saw of one request. The path is the URL's path only (the
/// query holds the signature).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RequestShape {
    /// The shape, `None` for a request no builder makes.
    pub shape: Option<Shape>,
    /// The URL path, percent-encoded.
    pub path: String,
    /// The body length.
    pub body_len: usize,
}

#[derive(Debug, Default)]
struct Script {
    pending: VecDeque<Fault>,
    seen: Vec<RequestShape>,
}

/// A [`Transport`] that plays its script over `inner`.
#[derive(Debug)]
pub struct FaultTransport<T> {
    inner: T,
    script: Mutex<Script>,
}

impl<T: Transport> FaultTransport<T> {
    /// Wraps `inner` with an empty script.
    #[must_use]
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            script: Mutex::new(Script::default()),
        }
    }

    /// The wrapped transport.
    #[must_use]
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Queues a fault for the next request of its shape.
    pub fn push(&self, fault: Fault) {
        self.script().pending.push_back(fault);
    }

    /// Faults pushed and not yet used.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.script().pending.len()
    }

    /// Every request seen so far, oldest first.
    #[must_use]
    pub fn requests(&self) -> Vec<RequestShape> {
        self.script().seen.clone()
    }

    fn script(&self) -> MutexGuard<'_, Script> {
        self.script.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn record_and_take(&self, request: &HttpRequest, shape: Option<Shape>) -> Option<Fault> {
        let mut script = self.script();
        script.seen.push(RequestShape {
            shape,
            path: request.url().path().to_owned(),
            body_len: request.body().len(),
        });
        let at = script
            .pending
            .iter()
            .position(|fault| Some(fault.on()) == shape)?;
        script.pending.remove(at)
    }
}

impl<T: Transport> Transport for FaultTransport<T> {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let shape = Shape::of(&request);
        let Some(fault) = self.record_and_take(&request, shape) else {
            return self.inner.send(request);
        };
        match fault {
            Fault::Status {
                status,
                code,
                retry_after,
                ..
            } => Ok(error_response(status, code, retry_after)),
            Fault::Transport { err, .. } => Err(err),
            Fault::Pass { .. } => self.inner.send(request),
            Fault::ErrorInOkBody { code, .. } => Ok(error_response(200, Some(code), None)),
            Fault::AfterPublish { status, .. } => {
                self.inner.send(request)?;
                Ok(error_response(status, None, None))
            }
            Fault::TruncateBody { keep, .. } => {
                let mut response = self.inner.send(request)?;
                response.body.truncate(keep);
                response.body_complete = false;
                Ok(response)
            }
            Fault::WrongEtag { .. } => {
                let mut response = self.inner.send(request)?;
                let other = quoted_md5(b"bytes the store never saw");
                response
                    .headers
                    .retain(|(name, _)| !name.eq_ignore_ascii_case("etag"));
                response.headers.push(("etag".to_owned(), other));
                Ok(response)
            }
        }
    }
}

/// An S3 error response. With no `code` the body is empty, as on a HEAD.
pub(crate) fn error_response(
    status: u16,
    code: Option<&str>,
    retry_after: Option<&str>,
) -> HttpResponse {
    let mut headers = Vec::new();
    if let Some(text) = retry_after {
        headers.push(("retry-after".to_owned(), text.to_owned()));
    }
    let body = match code {
        Some(code) => {
            headers.push(("content-type".to_owned(), "application/xml".to_owned()));
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>fault</Message></Error>"
            )
            .into_bytes()
        }
        None => Vec::new(),
    };
    HttpResponse::new(status, headers, body, true)
}
