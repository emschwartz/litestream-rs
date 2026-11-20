//! What a response means: S3 status and error code to [`StoreError`].
//!
//! Pure functions over [`HttpResponse`]. Nothing here keeps a URL, a header
//! value or a body: the result holds a status, a [`FileKey`] or a closed
//! [`S3Code`].

use std::io;
use std::time::Duration;

use lsr_core::{CorruptCause, FileKey, PartNumber, StoreError, StoreOp};

use crate::transport::{HttpResponse, TransportError};

/// The S3 error codes that change a decision; every other code is `Other`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum S3Code {
    /// `NoSuchKey`.
    NoSuchKey,
    /// `NoSuchUpload`: the multipart upload id is unknown (finished or aborted).
    NoSuchUpload,
    /// `NoSuchBucket`.
    NoSuchBucket,
    /// `SlowDown`.
    SlowDown,
    /// `AccessDenied`.
    AccessDenied,
    /// `InvalidAccessKeyId`.
    InvalidAccessKeyId,
    /// `SignatureDoesNotMatch`.
    SignatureDoesNotMatch,
    /// `ExpiredToken`.
    ExpiredToken,
    /// `RequestTimeTooSkewed`.
    RequestTimeTooSkewed,
    /// `BadDigest`: the body does not match its Content-MD5.
    BadDigest,
    /// `InvalidDigest`: the Content-MD5 itself is malformed.
    InvalidDigest,
    /// `InvalidRange`.
    InvalidRange,
    /// Any code not listed.
    Other,
}

impl S3Code {
    /// The code for a name from an `<Code>` element.
    #[must_use]
    pub fn from_name(name: &str) -> Self {
        match name {
            "NoSuchKey" => Self::NoSuchKey,
            "NoSuchUpload" => Self::NoSuchUpload,
            "NoSuchBucket" => Self::NoSuchBucket,
            "SlowDown" => Self::SlowDown,
            "AccessDenied" => Self::AccessDenied,
            "InvalidAccessKeyId" => Self::InvalidAccessKeyId,
            "SignatureDoesNotMatch" => Self::SignatureDoesNotMatch,
            "ExpiredToken" => Self::ExpiredToken,
            "RequestTimeTooSkewed" => Self::RequestTimeTooSkewed,
            "BadDigest" => Self::BadDigest,
            "InvalidDigest" => Self::InvalidDigest,
            "InvalidRange" => Self::InvalidRange,
            _ => Self::Other,
        }
    }

    /// The code in an error body, `None` when the body has no `<Code>`
    /// (HEAD responses have no body).
    #[must_use]
    pub fn from_body(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let (_, rest) = text.split_once("<Code>")?;
        let (name, _) = rest.split_once("</Code>")?;
        Some(Self::from_name(name.trim()))
    }

    fn is_auth(self) -> bool {
        matches!(
            self,
            Self::AccessDenied
                | Self::InvalidAccessKeyId
                | Self::SignatureDoesNotMatch
                | Self::ExpiredToken
                | Self::RequestTimeTooSkewed
        )
    }
}

/// `Retry-After` as whole seconds. The HTTP-date form is ignored.
fn retry_after(r: &HttpResponse) -> Option<Duration> {
    let seconds: u64 = r.header("retry-after")?.trim().parse().ok()?;
    Some(Duration::from_secs(seconds))
}

/// Whether a response is S3 saying the range starts at or past the end
/// (status 416; spike 8s finding 6). A read treats it as zero bytes.
#[must_use]
pub fn is_range_past_end(r: &HttpResponse) -> bool {
    r.status() == 416
}

/// Judges a response. `key` is the object a keyed request named and `part`
/// the multipart part, when there was one.
///
/// # Errors
/// The [`StoreError`] the status and code map to; see the table in the tests.
pub fn classify(
    op: StoreOp,
    key: Option<&FileKey>,
    part: Option<PartNumber>,
    r: &HttpResponse,
) -> Result<(), StoreError> {
    let status = r.status();
    if (200..300).contains(&status) {
        return if r.body_complete() {
            Ok(())
        } else {
            Err(StoreError::Transient {
                op,
                kind: io::ErrorKind::UnexpectedEof,
            })
        };
    }
    let code = S3Code::from_body(r.body());
    if status == 429 || code == Some(S3Code::SlowDown) {
        return Err(StoreError::Throttled {
            op,
            retry_after: retry_after(r),
        });
    }
    match status {
        401 | 403 => Err(StoreError::Auth { op }),
        _ if code.is_some_and(S3Code::is_auth) => Err(StoreError::Auth { op }),
        404 => Err(not_found(op, key, code, status)),
        400 if code == Some(S3Code::BadDigest) => Err(StoreError::Corrupt {
            key: key.copied(),
            cause: CorruptCause::ChecksumMismatch { part },
        }),
        408 => Err(StoreError::Transient {
            op,
            kind: io::ErrorKind::TimedOut,
        }),
        501 => Err(StoreError::Refused { op, status }),
        500..=599 => Err(StoreError::Transient {
            op,
            kind: io::ErrorKind::Other,
        }),
        _ => Err(StoreError::Refused { op, status }),
    }
}

fn not_found(op: StoreOp, key: Option<&FileKey>, code: Option<S3Code>, status: u16) -> StoreError {
    match (key, code) {
        (_, Some(S3Code::NoSuchBucket)) => StoreError::Auth { op },
        (Some(key), None | Some(S3Code::NoSuchKey)) => StoreError::NotFound { key: *key },
        _ => StoreError::Refused { op, status },
    }
}

/// Maps a failure to get any response.
#[must_use]
pub fn classify_transport(op: StoreOp, err: &TransportError) -> StoreError {
    match *err {
        TransportError::Timeout => StoreError::Transient {
            op,
            kind: io::ErrorKind::TimedOut,
        },
        TransportError::ConnectionReset => StoreError::Transient {
            op,
            kind: io::ErrorKind::ConnectionReset,
        },
        TransportError::Connect => StoreError::Transient {
            op,
            kind: io::ErrorKind::ConnectionRefused,
        },
        TransportError::Tls => StoreError::Auth { op },
        TransportError::Io(kind) => StoreError::Transient { op, kind },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lsr_core::Level;
    use lsr_ltx::{Txid, TxidRange};

    fn key() -> FileKey {
        let id = Txid::new(1).unwrap();
        FileKey::new(Level::ZERO, TxidRange::new(id, id).unwrap())
    }

    fn error_body(code: &str) -> Vec<u8> {
        format!("<?xml version=\"1.0\"?><Error><Code>{code}</Code><Message>m</Message></Error>")
            .into_bytes()
    }

    fn resp(status: u16, code: Option<&str>, retry: Option<&str>) -> HttpResponse {
        let headers = retry
            .map(|v| ("Retry-After".to_owned(), v.to_owned()))
            .into_iter()
            .collect();
        HttpResponse::new(
            status,
            headers,
            code.map(error_body).unwrap_or_default(),
            true,
        )
    }

    const OP: StoreOp = StoreOp::Read;
    const TRANSIENT: StoreError = StoreError::Transient {
        op: OP,
        kind: io::ErrorKind::Other,
    };

    fn throttled(secs: Option<u64>) -> StoreError {
        StoreError::Throttled {
            op: OP,
            retry_after: secs.map(Duration::from_secs),
        }
    }

    type Cases = Vec<(HttpResponse, Result<(), StoreError>)>;

    fn check(cases: Cases) {
        let (k, part) = (key(), PartNumber::new(2));
        for (response, want) in cases {
            assert_eq!(
                classify(OP, Some(&k), part, &response),
                want,
                "{response:?}"
            );
        }
    }

    #[test]
    fn classify_success_retry_and_throttle() {
        let date = "Wed, 21 Oct 2015 07:28:00 GMT";
        check(vec![
            (resp(200, None, None), Ok(())),
            (resp(206, None, None), Ok(())),
            (resp(204, None, None), Ok(())),
            (
                HttpResponse::new(200, vec![], vec![1], false),
                Err(StoreError::Transient {
                    op: OP,
                    kind: io::ErrorKind::UnexpectedEof,
                }),
            ),
            (
                resp(408, None, None),
                Err(StoreError::Transient {
                    op: OP,
                    kind: io::ErrorKind::TimedOut,
                }),
            ),
            (resp(500, Some("InternalError"), None), Err(TRANSIENT)),
            (resp(502, None, None), Err(TRANSIENT)),
            (resp(503, None, None), Err(TRANSIENT)),
            (resp(503, Some("ServiceUnavailable"), None), Err(TRANSIENT)),
            (resp(504, None, None), Err(TRANSIENT)),
            (resp(503, Some("SlowDown"), None), Err(throttled(None))),
            (
                resp(503, Some("SlowDown"), Some("3")),
                Err(throttled(Some(3))),
            ),
            (resp(429, None, Some("0")), Err(throttled(Some(0)))),
            (resp(429, None, Some(date)), Err(throttled(None))),
            (resp(429, None, None), Err(throttled(None))),
        ]);
    }

    #[test]
    fn classify_auth() {
        check(vec![
            (resp(401, None, None), Err(StoreError::Auth { op: OP })),
            (
                resp(403, Some("AccessDenied"), None),
                Err(StoreError::Auth { op: OP }),
            ),
            (
                resp(403, Some("InvalidAccessKeyId"), None),
                Err(StoreError::Auth { op: OP }),
            ),
            (
                resp(403, Some("SignatureDoesNotMatch"), None),
                Err(StoreError::Auth { op: OP }),
            ),
            (
                resp(403, Some("ExpiredToken"), None),
                Err(StoreError::Auth { op: OP }),
            ),
            (
                resp(403, Some("RequestTimeTooSkewed"), None),
                Err(StoreError::Auth { op: OP }),
            ),
        ]);
    }

    #[test]
    fn classify_missing_corrupt_and_refused() {
        let k = key();
        let part = PartNumber::new(2);
        check(vec![
            (
                resp(404, Some("NoSuchKey"), None),
                Err(StoreError::NotFound { key: k }),
            ),
            (resp(404, None, None), Err(StoreError::NotFound { key: k })),
            (
                resp(404, Some("NoSuchBucket"), None),
                Err(StoreError::Auth { op: OP }),
            ),
            (
                resp(404, Some("NoSuchUpload"), None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 404,
                }),
            ),
            (
                resp(400, Some("BadDigest"), None),
                Err(StoreError::Corrupt {
                    key: Some(k),
                    cause: CorruptCause::ChecksumMismatch { part },
                }),
            ),
            (
                resp(400, Some("InvalidDigest"), None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 400,
                }),
            ),
            (
                resp(400, Some("MalformedXML"), None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 400,
                }),
            ),
            (
                resp(413, None, None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 413,
                }),
            ),
            (
                resp(416, Some("InvalidRange"), None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 416,
                }),
            ),
            (
                resp(501, Some("NotImplemented"), None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 501,
                }),
            ),
            (
                resp(301, None, None),
                Err(StoreError::Refused {
                    op: OP,
                    status: 301,
                }),
            ),
        ]);
    }

    #[test]
    fn unkeyed_404_is_refused_not_not_found() {
        assert_eq!(
            classify(
                StoreOp::List,
                None,
                None,
                &resp(404, Some("NoSuchKey"), None)
            ),
            Err(StoreError::Refused {
                op: StoreOp::List,
                status: 404
            })
        );
    }

    #[test]
    fn range_past_end_is_only_416() {
        assert!(is_range_past_end(&resp(416, Some("InvalidRange"), None)));
        assert!(!is_range_past_end(&resp(206, None, None)));
    }

    #[test]
    fn transport_errors_map_by_kind() {
        let op = StoreOp::Upload;
        let cases = [
            (
                TransportError::Timeout,
                StoreError::Transient {
                    op,
                    kind: io::ErrorKind::TimedOut,
                },
            ),
            (
                TransportError::ConnectionReset,
                StoreError::Transient {
                    op,
                    kind: io::ErrorKind::ConnectionReset,
                },
            ),
            (
                TransportError::Connect,
                StoreError::Transient {
                    op,
                    kind: io::ErrorKind::ConnectionRefused,
                },
            ),
            (TransportError::Tls, StoreError::Auth { op }),
            (
                TransportError::Io(io::ErrorKind::BrokenPipe),
                StoreError::Transient {
                    op,
                    kind: io::ErrorKind::BrokenPipe,
                },
            ),
        ];
        for (err, want) in cases {
            assert_eq!(classify_transport(op, &err), want);
        }
    }

    #[test]
    fn code_is_read_from_the_body() {
        assert_eq!(
            S3Code::from_body(&error_body("SlowDown")),
            Some(S3Code::SlowDown)
        );
        assert_eq!(S3Code::from_body(&error_body("Weird")), Some(S3Code::Other));
        assert_eq!(S3Code::from_body(b""), None);
        assert_eq!(S3Code::from_body(&[0xff, 0xfe]), None);
    }
}
