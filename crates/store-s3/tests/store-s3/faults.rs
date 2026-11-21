//! The recorded-fault layer over FakeS3, at the Transport level: each case
//! builds requests with the request builders, sends them through
//! `FaultTransport<FakeS3>` and reads the result with the response classifier and
//! parsers. Cases that need the store's five operations and `Retrying`'s
//! sleep are in `store.rs`.

use crate::common;
use crate::wire;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
mod cases {
    use std::io;
    use std::time::Duration;

    use super::common::*;
    use super::wire::*;
    use lsr_core::{
        Class, CorruptCause, Level, ReadLen, RetryPolicy, StoreError, StoreOp,
        classify as retry_class,
    };
    use lsr_store_s3::{
        EtagCheck, Fault, FaultTransport, HttpResponse, Shape, TransportError, check_etag,
        classify, classify_transport, parse_delete_failures, parse_head, parse_list,
    };

    fn faulty() -> FaultTransport<lsr_store_s3::FakeS3> {
        FaultTransport::new(fake())
    }

    fn status(
        on: Shape,
        status: u16,
        code: Option<&'static str>,
        after: Option<&'static str>,
    ) -> Fault {
        Fault::Status {
            on,
            status,
            code,
            retry_after: after,
        }
    }

    fn put_verdict(t: &FaultTransport<lsr_store_s3::FakeS3>) -> Result<(), StoreError> {
        let k = key(0, 1, 1);
        classify(StoreOp::Upload, Some(&k), None, &put(t, &k, b"x", 0))
    }

    fn list_page(t: &FaultTransport<lsr_store_s3::FakeS3>, max: u16) -> HttpResponse {
        send(
            t,
            builder()
                .list_page(now(), Level::ZERO, None, limit(max))
                .unwrap(),
        )
    }

    fn prefix() -> lsr_store_s3::Prefix {
        config().prefix().clone()
    }

    #[test]
    fn throttle_then_success() {
        // 503 SlowDown and 429 are Throttled with the header's seconds.
        for (code, want) in [(Some("SlowDown"), 503), (None, 429)] {
            let t = faulty();
            t.push(status(Shape::Put, want, code, Some("1")));
            let err = put_verdict(&t).unwrap_err();
            assert_eq!(
                err,
                StoreError::Throttled {
                    op: StoreOp::Upload,
                    retry_after: Some(Duration::from_secs(1))
                }
            );
            assert_eq!(retry_class(&err), Class::RetryAfter(Duration::from_secs(1)));
            assert!(
                t.inner().keys().is_empty(),
                "a throttled request never reached the bucket"
            );
            assert_eq!(put_verdict(&t), Ok(()), "the next request is clean");
            assert_eq!(t.inner().keys().len(), 1);
        }
    }

    #[test]
    fn retry_after_past_the_cap_is_named_in_full() {
        let t = faulty();
        t.push(status(Shape::Put, 503, Some("SlowDown"), Some("60")));
        let err = put_verdict(&t).unwrap_err();
        assert_eq!(
            retry_class(&err),
            Class::RetryAfter(Duration::from_secs(60))
        );
        assert!(
            RetryPolicy::DEFAULT.cap < Duration::from_secs(60),
            "Retrying returns it at once"
        );
    }

    #[test]
    fn status_408_is_transient() {
        let t = faulty();
        t.push(status(Shape::Put, 408, None, None));
        let err = put_verdict(&t).unwrap_err();
        assert_eq!(
            err,
            StoreError::Transient {
                op: StoreOp::Upload,
                kind: io::ErrorKind::TimedOut
            }
        );
        assert_eq!(retry_class(&err), Class::Retry);
    }

    #[test]
    fn connection_reset_is_transient_and_tls_failure_is_auth() {
        let t = faulty();
        let k = key(0, 1, 1);
        for (err, want) in [
            (
                TransportError::ConnectionReset,
                StoreError::Transient {
                    op: StoreOp::Read,
                    kind: io::ErrorKind::ConnectionReset,
                },
            ),
            (TransportError::Tls, StoreError::Auth { op: StoreOp::Read }),
        ] {
            t.push(Fault::Transport {
                on: Shape::Get,
                err,
            });
            let request = builder()
                .get_range(now(), &k, 0, ReadLen::new(1).unwrap())
                .unwrap();
            let got = lsr_store_s3::Transport::send(&t, request).unwrap_err();
            assert_eq!(got, err);
            assert_eq!(classify_transport(StoreOp::Read, &got), want);
        }
    }

    #[test]
    fn a_500_after_publish_leaves_the_object_visible() {
        let t = faulty();
        t.push(Fault::AfterPublish {
            on: Shape::Put,
            status: 500,
        });
        let k = key(0, 1, 1);
        let err = classify(
            StoreOp::Upload,
            Some(&k),
            None,
            &put(&t, &k, b"data", 1_700_000_000_123),
        );
        assert_eq!(
            err,
            Err(StoreError::Transient {
                op: StoreOp::Upload,
                kind: io::ErrorKind::Other
            })
        );
        let head = parse_head(&send(&t, builder().head(now(), &k).unwrap())).unwrap();
        assert_eq!(
            (head.size, head.stamp.unwrap().unix_millis()),
            (4, 1_700_000_000_123)
        );
        let page = parse_list(&list_page(&t, 10), &prefix(), Level::ZERO).unwrap();
        assert_eq!(page.files.len(), 1);
    }

    #[test]
    fn a_truncated_get_is_transient_never_a_short_read() {
        let t = faulty();
        let k = key(0, 1, 1);
        put(&t, &k, &vec![9_u8; 4096], 0);
        t.push(Fault::TruncateBody {
            on: Shape::Get,
            keep: 100,
        });
        let request = builder()
            .get_range(now(), &k, 0, ReadLen::new(4096).unwrap())
            .unwrap();
        let response = send(&t, request);
        assert_eq!(
            (
                response.status(),
                response.body().len(),
                response.body_complete()
            ),
            (206, 100, false)
        );
        assert_eq!(
            classify(StoreOp::Read, Some(&k), None, &response),
            Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::UnexpectedEof
            })
        );
    }

    #[test]
    fn a_truncated_list_page_is_an_error() {
        let t = faulty();
        put(&t, &key(0, 1, 1), b"x", 0);
        t.push(Fault::TruncateBody {
            on: Shape::List,
            keep: 120,
        });
        let response = list_page(&t, 10);
        assert!(classify(StoreOp::List, None, None, &response).is_err());
        assert!(
            parse_list(&response, &prefix(), Level::ZERO).is_err(),
            "cut mid-XML"
        );

        let no_token = HttpResponse::new(
            200,
            vec![],
            b"<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><IsTruncated>true</IsTruncated></ListBucketResult>".to_vec(),
            true,
        );
        assert!(
            parse_list(&no_token, &prefix(), Level::ZERO).is_err(),
            "IsTruncated without a token"
        );
    }

    #[test]
    fn page_failure_mid_listing_yields_no_partial_listing() {
        let t = faulty();
        for min in 1..=4 {
            put(&t, &key(0, min, min), b"x", 0);
        }
        let first = parse_list(&list_page(&t, 2), &prefix(), Level::ZERO).unwrap();
        assert!(first.next_token.is_some());
        t.push(status(Shape::List, 500, Some("InternalError"), None));
        let second = list_page(&t, 2);
        assert!(text(&second).contains("InternalError"));
        assert_eq!(
            classify(StoreOp::List, None, None, &second),
            Err(StoreError::Transient {
                op: StoreOp::List,
                kind: io::ErrorKind::Other
            })
        );
    }

    #[test]
    fn a_wrong_etag_on_put_is_visible_as_a_mismatch_and_never_retried() {
        let t = faulty();
        t.push(Fault::WrongEtag { on: Shape::Put });
        let k = key(0, 1, 1);
        let request = builder()
            .put_object(now(), &k, b"data".to_vec(), now())
            .unwrap();
        let md5 = request.md5();
        let response = send(&t, request);
        assert_eq!(response.status(), 200);
        assert_eq!(check_etag(&response, md5), EtagCheck::Mismatch);
        assert_eq!(
            t.inner().keys().len(),
            1,
            "the object is already visible, so the store must delete it"
        );
        let cause = StoreError::Corrupt {
            key: Some(k),
            cause: CorruptCause::ChecksumMismatch { part: None },
        };
        assert_eq!(retry_class(&cause), Class::Permanent);
    }

    #[test]
    fn a_file_deleted_between_list_and_head_is_not_found() {
        let t = faulty();
        let (a, b) = (key(0, 1, 1), key(0, 2, 2));
        put(&t, &a, b"x", 0);
        put(&t, &b, b"x", 0);
        let page = parse_list(&list_page(&t, 10), &prefix(), Level::ZERO).unwrap();
        assert_eq!(page.files.len(), 2);
        send(&t, builder().delete_objects(now(), &[a]).unwrap());
        let head = send(&t, builder().head(now(), &a).unwrap());
        assert_eq!(
            classify(StoreOp::List, Some(&a), None, &head),
            Err(StoreError::NotFound { key: a })
        );
        assert_eq!(
            classify(
                StoreOp::List,
                Some(&b),
                None,
                &send(&t, builder().head(now(), &b).unwrap())
            ),
            Ok(())
        );
    }

    #[test]
    fn a_delete_error_entry_counts_unless_the_key_was_already_gone() {
        let t = faulty();
        let k = key(0, 1, 1);
        put(&t, &k, b"x", 0);
        t.inner()
            .deny_delete(&lsr_store_s3::object_key(&prefix(), &k));
        let denied = send(&t, builder().delete_objects(now(), &[k]).unwrap());
        assert_eq!(parse_delete_failures(&denied), Ok(1));
        let gone = HttpResponse::new(
            200,
            vec![],
            b"<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Error><Key>a</Key><Code>NoSuchKey</Code><Message>m</Message></Error></DeleteResult>".to_vec(),
            true,
        );
        assert_eq!(parse_delete_failures(&gone), Ok(0));
    }

    #[test]
    fn faults_apply_once_per_shape_in_push_order() {
        let t = faulty();
        t.push(status(Shape::Put, 500, None, None));
        t.push(status(Shape::Put, 503, None, None));
        t.push(status(Shape::Head, 404, None, None));
        let k = key(0, 1, 1);
        let codes = [
            put(&t, &k, b"x", 0).status(),
            send(&t, builder().head(now(), &k).unwrap()).status(),
            put(&t, &k, b"x", 0).status(),
            put(&t, &k, b"x", 0).status(),
        ];
        assert_eq!(codes, [500, 404, 503, 200]);
        assert_eq!(t.pending(), 0);
        let shapes: Vec<_> = t.requests().iter().map(|seen| seen.shape).collect();
        assert_eq!(
            shapes,
            [
                Some(Shape::Put),
                Some(Shape::Head),
                Some(Shape::Put),
                Some(Shape::Put)
            ]
        );
    }

    #[test]
    fn every_request_shape_is_recognised() {
        let t = faulty();
        let k = key(1, 1, 5);
        let b = builder();
        let id = lsr_store_s3::UploadId::new("u".to_owned());
        let part = lsr_core::PartNumber::new(1).unwrap();
        let etags = vec!["\"x\"".to_owned()];
        let all = [
            (
                b.list_page(now(), Level::ZERO, None, limit(1)).unwrap(),
                Shape::List,
            ),
            (b.head(now(), &k).unwrap(), Shape::Head),
            (
                b.get_range(now(), &k, 0, ReadLen::new(1).unwrap()).unwrap(),
                Shape::Get,
            ),
            (
                b.put_object(now(), &k, vec![], now()).unwrap().into(),
                Shape::Put,
            ),
            (
                b.create_multipart(now(), &k, now()).unwrap(),
                Shape::CreateMultipart,
            ),
            (
                b.upload_part(now(), &k, &id, part, vec![]).unwrap().into(),
                Shape::UploadPart,
            ),
            (
                b.complete_multipart(now(), &k, &id, &etags).unwrap(),
                Shape::Complete,
            ),
            (b.abort_multipart(now(), &k, &id).unwrap(), Shape::Abort),
            (b.delete_objects(now(), &[k]).unwrap(), Shape::DeleteObjects),
        ];
        for (request, want) in all {
            assert_eq!(Shape::of(&request), Some(want));
            drop(lsr_store_s3::Transport::send(&t, request));
        }
        assert_eq!(t.requests().len(), 9);
    }

    #[test]
    fn fault_errors_never_print_secrets() {
        let t = faulty();
        let k = key(0, 1, 1);
        let mut shown = Vec::new();
        for fault in [
            status(Shape::Put, 503, Some("SlowDown"), Some("1")),
            status(Shape::Put, 408, None, None),
            status(Shape::Put, 403, Some("AccessDenied"), None),
            status(Shape::Put, 400, Some("BadDigest"), None),
            Fault::AfterPublish {
                on: Shape::Put,
                status: 500,
            },
            Fault::WrongEtag { on: Shape::Put },
            Fault::TruncateBody {
                on: Shape::Put,
                keep: 0,
            },
        ] {
            t.push(fault.clone());
            let response = put(&t, &k, b"x", 0);
            shown.push(format!(
                "{fault:?} {response:?} {:?}",
                classify(StoreOp::Upload, Some(&k), None, &response)
            ));
        }
        for err in [
            TransportError::Timeout,
            TransportError::ConnectionReset,
            TransportError::Connect,
            TransportError::Tls,
        ] {
            shown.push(format!(
                "{err:?} {err} {:?}",
                classify_transport(StoreOp::Upload, &err)
            ));
        }
        shown.push(format!("{t:?} {:?} {:?}", t.requests(), t.inner()));
        for text in shown {
            for bad in ["CANARY", "wJalr", "Signature", "Security-Token", "X-Amz"] {
                assert!(!text.contains(bad), "printed {bad}: {text}");
            }
        }
    }
}
