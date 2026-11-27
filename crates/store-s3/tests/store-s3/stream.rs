//! A restore's stream over scripted answers: what the store does with a server that ignores the
//! `Range` it was sent, answers another range, or ends the body before its length.

use crate::common;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
mod cases {
    use std::collections::VecDeque;
    use std::io;
    use std::sync::{Arc, Mutex, PoisonError};

    use super::common::*;
    use lsr_core::conformance::block_on;
    use lsr_core::{FakeClock, ObjectStore, ReadLen, StoreError, StoreOp};
    use lsr_store_s3::{HttpRequest, HttpResponse, S3Store, Transport, TransportError};

    /// Answers each request with the next scripted response and keeps each one's `Range`.
    struct Script {
        answers: Mutex<VecDeque<HttpResponse>>,
        ranges: Arc<Mutex<Vec<Option<String>>>>,
    }

    impl Transport for Script {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
            self.ranges
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(request.header("range").map(str::to_owned));
            self.answers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .pop_front()
                .ok_or(TransportError::Io(io::ErrorKind::UnexpectedEof))
        }
    }

    fn answer(status: u16, headers: &[(&str, &str)], body: &[u8]) -> HttpResponse {
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        HttpResponse::new(status, headers, body.to_vec(), true)
    }

    type Pieces = Vec<Result<Vec<u8>, StoreError>>;

    /// The pieces of three bytes a stream from `from` hands out over `answers`, up to the first
    /// short one or the first failure, and the `Range` each request carried.
    fn pieces(from: u64, answers: Vec<HttpResponse>) -> (Pieces, Vec<Option<String>>) {
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let store = S3Store::with_transport(
            config(),
            credentials(),
            Script {
                answers: Mutex::new(answers.into()),
                ranges: Arc::clone(&ranges),
            },
            FakeClock::new(now()),
        )
        .unwrap();
        let len = ReadLen::new(3).unwrap();
        let mut out = Vec::new();
        {
            let mut stream = store.read_stream(key(0, 1, 1), from);
            loop {
                let piece = block_on(stream.next(len));
                let last = piece.as_ref().map_or(true, |bytes| bytes.len() < 3);
                out.push(piece);
                if last {
                    break;
                }
            }
        }
        let ranges = ranges
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        (out, ranges)
    }

    /// The pieces of three bytes a span from `from` to `end` hands out over `answers`.
    fn span_pieces(
        from: u64,
        end: u64,
        answers: Vec<HttpResponse>,
    ) -> (Pieces, Vec<Option<String>>) {
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let store = S3Store::with_transport(
            config(),
            credentials(),
            Script {
                answers: Mutex::new(answers.into()),
                ranges: Arc::clone(&ranges),
            },
            FakeClock::new(now()),
        )
        .unwrap();
        let len = ReadLen::new(3).unwrap();
        let mut out = Vec::new();
        {
            let mut stream = store.read_span(key(0, 1, 1), from, end);
            loop {
                let piece = block_on(stream.next(len));
                let last = piece.as_ref().map_or(true, |bytes| bytes.len() < 3);
                out.push(piece);
                if last {
                    break;
                }
            }
        }
        let ranges = ranges
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        (out, ranges)
    }

    #[test]
    fn a_span_asks_for_its_bytes_alone() {
        let reply = answer(
            206,
            &[("content-length", "4"), ("content-range", "bytes 2-5/9")],
            b"cdef",
        );
        let (got, ranges) = span_pieces(2, 6, vec![reply]);
        assert_eq!(got, [Ok(b"cde".to_vec()), Ok(b"f".to_vec())]);
        assert_eq!(ranges, [Some("bytes=2-5".to_owned())]);
    }

    /// A server that ignores the span's `Range` sends the whole object: the span is its bytes
    /// from the start to the end, and nothing past it.
    #[test]
    fn a_whole_object_answered_to_a_span_is_cut_to_the_span() {
        let reply = answer(200, &[("content-length", "9")], b"abcdefghi");
        let (got, _) = span_pieces(2, 6, vec![reply]);
        assert_eq!(got, [Ok(b"cde".to_vec()), Ok(b"f".to_vec())]);
        let reply = answer(200, &[], b"abcdefghi");
        let (got, _) = span_pieces(0, 4, vec![reply]);
        assert_eq!(got, [Ok(b"abc".to_vec()), Ok(b"d".to_vec())]);
    }

    /// A span whose body ends early fails, as a stream's does. The span reads ahead a whole
    /// piece at a time, so the bytes before the cut in that piece are not handed out; a caller
    /// goes on from the bytes it was handed, as from any failed piece.
    #[test]
    fn a_span_cut_short_fails() {
        let reply = answer(
            206,
            &[("content-length", "6"), ("content-range", "bytes 0-5/9")],
            b"abcd",
        );
        let (got, _) = span_pieces(0, 6, vec![reply]);
        assert_eq!(
            got,
            [Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::UnexpectedEof,
            })]
        );
    }

    /// No answer at all: the span's first piece is the failure.
    #[test]
    fn a_span_with_no_answer_fails() {
        let (got, _) = span_pieces(0, 6, vec![]);
        assert!(
            matches!(got.as_slice(), [Err(StoreError::Transient { .. })]),
            "{got:?}"
        );
    }

    #[test]
    fn a_stream_from_the_start_sends_no_range() {
        let (got, ranges) = pieces(0, vec![answer(200, &[("content-length", "4")], b"abcd")]);
        assert_eq!(got, [Ok(b"abc".to_vec()), Ok(b"d".to_vec())]);
        assert_eq!(ranges, [None]);
    }

    #[test]
    fn a_stream_past_the_start_asks_for_the_rest() {
        let reply = answer(
            206,
            &[("content-length", "2"), ("content-range", "bytes 4-5/6")],
            b"ef",
        );
        let (got, ranges) = pieces(4, vec![reply]);
        assert_eq!(got, [Ok(b"ef".to_vec())]);
        assert_eq!(ranges, [Some("bytes=4-".to_owned())]);
    }

    /// A server that ignores the `Range` sends the whole object; the bytes before the offset
    /// are not the stream's.
    #[test]
    fn a_whole_object_answered_to_a_range_is_read_from_the_offset() {
        let reply = answer(200, &[("content-length", "9")], b"abcdefghi");
        let (got, _) = pieces(4, vec![reply]);
        assert_eq!(got, [Ok(b"efg".to_vec()), Ok(b"hi".to_vec())]);
    }

    /// A 206 for another range would hand back the wrong bytes under the right key.
    #[test]
    fn an_answer_for_another_range_is_refused() {
        let reply = answer(
            206,
            &[("content-length", "9"), ("content-range", "bytes 0-8/9")],
            b"abcdefghi",
        );
        let (got, _) = pieces(4, vec![reply]);
        assert_eq!(
            got,
            [Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::InvalidData,
            })]
        );
    }

    #[test]
    fn a_stream_from_the_end_is_empty() {
        let (got, _) = pieces(9, vec![answer(416, &[], b"")]);
        assert_eq!(got, [Ok(Vec::new())]);
    }

    /// A body that ends before the length it declared is a failure, never the end of the object.
    #[test]
    fn a_body_short_of_its_length_fails() {
        let reply = answer(200, &[("content-length", "9")], b"abcd");
        let (got, _) = pieces(0, vec![reply]);
        assert_eq!(
            got,
            [
                Ok(b"abc".to_vec()),
                Err(StoreError::Transient {
                    op: StoreOp::Read,
                    kind: io::ErrorKind::UnexpectedEof,
                })
            ]
        );
    }

    /// The same, where the body ends before the offset a whole object was skipped to.
    #[test]
    fn a_whole_object_that_ends_before_the_offset_fails() {
        let reply = answer(200, &[("content-length", "9")], b"ab");
        let (got, _) = pieces(4, vec![reply]);
        assert_eq!(
            got,
            [Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::UnexpectedEof,
            })]
        );
    }

    #[test]
    fn a_missing_object_is_not_found() {
        let reply = answer(404, &[], b"<Error><Code>NoSuchKey</Code></Error>");
        let (got, _) = pieces(0, vec![reply]);
        assert!(matches!(got.as_slice(), [Err(StoreError::NotFound { .. })]));
    }
}
