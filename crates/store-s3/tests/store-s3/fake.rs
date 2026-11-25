//! FakeS3 against the behaviours the MinIO spike measured (docs/spikes.md).

use crate::common;
use crate::wire;

// The helpers are test code only by cfg, which is what clippy.toml's allow-*-in-tests reads.
#[cfg(test)]
mod cases {
    use super::common::*;
    use super::wire::*;
    use lsr_core::{CorruptCause, FileKey, Level, PartNumber, ReadLen, StoreError, StoreOp};
    use lsr_store_s3::{
        EtagCheck, RequestBuilder, Transport, UploadId, check_etag, classify, object_key,
        parse_complete, parse_create, parse_delete_failures, parse_head, parse_list, quoted_md5,
    };
    use md5::{Digest as _, Md5};

    const MIB5: usize = 5 * 1024 * 1024;

    fn level_prefix() -> lsr_store_s3::Prefix {
        config().prefix().clone()
    }

    #[test]
    fn put_then_head_and_get_agree() {
        let fake = fake();
        let k = key(1, 1, 5);
        let put = put(&fake, &k, b"hello", 1_700_000_000_123);
        assert_eq!(put.status(), 200);
        let request = builder()
            .put_object(now(), &k, b"hello".to_vec(), now())
            .unwrap();
        assert_eq!(check_etag(&put, request.md5()), EtagCheck::Matches);

        let head = send(&fake, builder().head(now(), &k).unwrap());
        let parsed = parse_head(&head).unwrap();
        assert_eq!(parsed.size, 5);
        assert_eq!(parsed.created_at.unwrap().unix_millis(), 1_700_000_000_123);

        let got = send(
            &fake,
            builder()
                .get_range(now(), &k, 0, ReadLen::new(5).unwrap())
                .unwrap(),
        );
        assert_eq!((got.status(), got.body()), (206, &b"hello"[..]));
    }

    #[test]
    fn wrong_digest_is_bad_digest_and_malformed_is_invalid() {
        let fake = fake();
        let k = key(1, 1, 5);
        let wrong = builder()
            .put_object(now(), &k, b"hello".to_vec(), now())
            .unwrap();
        let altered = wrong.clone().tamper_body(b"hellO".to_vec());
        let response = send(&fake, altered);
        assert_eq!(response.status(), 400);
        assert_eq!(
            classify(StoreOp::Upload, Some(&k), None, &response),
            Err(StoreError::Corrupt {
                key: Some(k),
                cause: CorruptCause::ChecksumMismatch { part: None }
            })
        );
        assert!(fake.keys().is_empty(), "a refused write publishes nothing");

        let malformed = wrong.tamper_header("content-md5", Some("not base64!".to_owned()));
        let response = send(&fake, malformed);
        assert_eq!(response.status(), 400);
        assert_eq!(
            classify(StoreOp::Upload, Some(&k), None, &response),
            Err(StoreError::Refused {
                op: StoreOp::Upload,
                status: 400
            })
        );
    }

    #[test]
    fn signing_rule_is_enforced_both_ways() {
        let fake = fake();
        let k = key(1, 1, 5);
        let request = builder()
            .put_object(now(), &k, b"x".to_vec(), now())
            .unwrap();
        let unsigned = request
            .clone()
            .tamper_header("x-amz-meta-extra", Some("1".to_owned()));
        let missing = request.clone().tamper_header("content-md5", None);
        for bad in [unsigned, missing] {
            let response = send(&fake, bad);
            assert_eq!(response.status(), 400);
            assert!(text(&response).contains("AccessDenied"));
        }
        assert_eq!(send(&fake, request).status(), 200);
    }

    #[test]
    fn ranges_match_minio() {
        let fake = fake();
        let k = key(1, 1, 5);
        put(&fake, &k, b"0123456789", 0);
        let get = |offset: u64, len: u64| {
            send(
                &fake,
                builder()
                    .get_range(
                        now(),
                        &k,
                        offset,
                        ReadLen::new(u32::try_from(len).unwrap()).unwrap(),
                    )
                    .unwrap(),
            )
        };
        for (offset, len) in [(100, 101), (10, 11)] {
            let response = get(offset, len);
            assert_eq!(response.status(), 416, "bytes={offset}");
            assert!(text(&response).contains("InvalidRange"));
        }
        let clipped = get(5, 96);
        assert_eq!(clipped.status(), 206);
        assert_eq!(clipped.header("content-range"), Some("bytes 5-9/10"));
        assert_eq!(clipped.body(), b"56789");
        let whole = get(0, 10);
        assert_eq!((whole.status(), whole.body().len()), (206, 10));
    }

    #[test]
    fn zero_byte_object_has_no_satisfiable_range() {
        let fake = fake();
        let k = key(1, 1, 5);
        assert_eq!(put(&fake, &k, b"", 0).status(), 200);
        let head = parse_head(&send(&fake, builder().head(now(), &k).unwrap())).unwrap();
        assert_eq!(head.size, 0);
        for len in [1, ReadLen::MAX_RAW] {
            let response = send(
                &fake,
                builder()
                    .get_range(now(), &k, 0, ReadLen::new(len).unwrap())
                    .unwrap(),
            );
            assert_eq!(response.status(), 416, "len {len}");
        }
    }

    #[test]
    fn missing_key_is_404() {
        let fake = fake();
        let k = key(1, 1, 5);
        let get = send(
            &fake,
            builder()
                .get_range(now(), &k, 0, ReadLen::new(1).unwrap())
                .unwrap(),
        );
        assert_eq!(
            classify(StoreOp::Read, Some(&k), None, &get),
            Err(StoreError::NotFound { key: k })
        );
        let head = send(&fake, builder().head(now(), &k).unwrap());
        assert_eq!(
            classify(StoreOp::List, Some(&k), None, &head),
            Err(StoreError::NotFound { key: k })
        );
    }

    #[test]
    fn listing_url_encodes_and_keys_round_trip() {
        let fake = fake();
        let odd = [
            "foo bar/a+b.ltx",
            "plus+key",
            "pct%20lit",
            "ünï",
            "amp&lt<>.ltx",
        ];
        for name in odd {
            fake.put_raw(&format!("db/0000/{name}"), vec![1], &[]);
        }
        let response = send(
            &fake,
            builder()
                .list_page(now(), Level::ZERO, None, limit(1000))
                .unwrap(),
        );
        assert!(text(&response).contains("<EncodingType>url</EncodingType>"));
        let page = parse_list(&response, &level_prefix(), Level::ZERO).unwrap();
        let mut want: Vec<String> = odd.iter().map(|name| (*name).to_owned()).collect();
        want.sort();
        let listed: Vec<String> = page
            .unrecognized
            .into_iter()
            .map(|stray| stray.name)
            .collect();
        assert_eq!(listed, want, "a bytewise provider lists by raw key bytes");
        assert!(page.next_token.is_none());
    }

    #[test]
    fn listing_pages_with_tokens_and_seeks_with_start_after() {
        let fake = fake();
        for min in 1..=5 {
            put(&fake, &key(0, min, min), &[0], 0);
        }
        put(&fake, &key(1, 1, 5), &[0], 0);
        let list = |after, max| {
            let response = send(
                &fake,
                builder()
                    .list_page(now(), Level::ZERO, after, limit(max))
                    .unwrap(),
            );
            parse_list(&response, &level_prefix(), Level::ZERO).unwrap()
        };
        let first = list(None, 2);
        assert_eq!(first.files.len(), 2);
        assert!(
            first.next_token.is_some(),
            "other levels never leak in, level 0 has 5"
        );

        let seek = list(Some(range(2, 2)), 1000);
        let mins: Vec<u64> = seek
            .files
            .iter()
            .map(|f| f.key.range().min().get())
            .collect();
        assert_eq!(mins, vec![3, 4, 5]);
        assert!(seek.next_token.is_none());
        assert!(
            seek.files
                .iter()
                .all(|f| f.size == 1 && f.last_modified.unix_millis() > 0)
        );
    }

    #[test]
    fn token_continues_where_the_page_stopped() {
        let fake = fake();
        for min in 1..=5 {
            put(&fake, &key(0, min, min), &[0], 0);
        }
        let page_after = |token: Option<&str>| {
            let mut request = builder()
                .list_page(now(), Level::ZERO, None, limit(2))
                .unwrap();
            if let Some(token) = token {
                request = request.tamper_query("continuation-token", token);
            }
            parse_list(&send(&fake, request), &level_prefix(), Level::ZERO).unwrap()
        };
        let mut mins = Vec::new();
        let mut token = None;
        loop {
            let page = page_after(token.as_deref());
            mins.extend(page.files.iter().map(|f| f.key.range().min().get()));
            token = page.next_token;
            if token.is_none() {
                break;
            }
        }
        assert_eq!(mins, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn quiet_delete_of_a_missing_key_reports_nothing() {
        let fake = fake();
        let present = key(0, 1, 1);
        let missing = key(0, 2, 2);
        put(&fake, &present, b"x", 0);
        let response = send(
            &fake,
            builder()
                .delete_objects(now(), &[present, missing])
                .unwrap(),
        );
        assert_eq!(response.status(), 200);
        assert_eq!(parse_delete_failures(&response), Ok(0));
        assert!(fake.keys().is_empty());
    }

    #[test]
    fn delete_without_or_with_a_wrong_digest_is_refused() {
        let fake = fake();
        let k = key(0, 1, 1);
        put(&fake, &k, b"x", 0);
        let request = builder().delete_objects(now(), &[k]).unwrap();
        let missing = send(&fake, request.clone().tamper_header("content-md5", None));
        assert_eq!(missing.status(), 400);
        let wrong = send(&fake, request.tamper_body(b"<Delete></Delete>".to_vec()));
        assert!(text(&wrong).contains("BadDigest"));
        assert_eq!(fake.keys().len(), 1);
    }

    #[test]
    fn denied_delete_is_an_error_entry_and_leaves_the_object() {
        let fake = fake();
        let (kept, gone) = (key(0, 1, 1), key(0, 2, 2));
        put(&fake, &kept, b"x", 0);
        put(&fake, &gone, b"x", 0);
        fake.deny_delete(&object_key(&level_prefix(), &kept));
        let response = send(
            &fake,
            builder().delete_objects(now(), &[kept, gone]).unwrap(),
        );
        assert_eq!(parse_delete_failures(&response), Ok(1));
        assert_eq!(fake.keys(), vec![object_key(&level_prefix(), &kept)]);
    }

    fn begin(fake: &impl Transport, k: &FileKey) -> UploadId {
        let response = send(fake, builder().create_multipart(now(), k, now()).unwrap());
        parse_create(&response).unwrap()
    }

    fn part(fake: &impl Transport, k: &FileKey, id: &UploadId, n: u16, body: Vec<u8>) -> String {
        let b: RequestBuilder = builder();
        let response = send(
            fake,
            b.upload_part(now(), k, id, PartNumber::new(n).unwrap(), body)
                .unwrap(),
        );
        assert_eq!(response.status(), 200);
        response.header("etag").unwrap().to_owned()
    }

    #[test]
    fn multipart_etag_is_the_md5_of_part_md5s() {
        let fake = fake();
        let k = key(1, 1, 5);
        let id = begin(&fake, &k);
        let (big, tail) = (vec![7_u8; MIB5], b"123456789".to_vec());
        let etags = vec![
            part(&fake, &k, &id, 1, big.clone()),
            part(&fake, &k, &id, 2, tail.clone()),
        ];
        assert_eq!(etags.first().unwrap(), &quoted_md5(&big));
        let done = send(
            &fake,
            builder()
                .complete_multipart(now(), &k, &id, &etags)
                .unwrap(),
        );
        assert_eq!(done.status(), 200);
        let mut digests = Md5::digest(&big).to_vec();
        digests.extend_from_slice(&Md5::digest(&tail));
        let want = format!("\"{}-2\"", lsr_store_s3::hex(&Md5::digest(&digests)));
        assert_eq!(parse_complete(&done).unwrap(), want);
        assert_eq!(done.header("etag"), Some(want.as_str()));
        let head = parse_head(&send(&fake, builder().head(now(), &k).unwrap())).unwrap();
        assert_eq!(head.size, 5_242_889);
        assert_eq!(
            head.created_at,
            Some(now()),
            "the created-at time set at create is on the object"
        );
        assert_eq!(fake.open_uploads(), 0);
    }

    #[test]
    fn last_completed_upload_wins_not_the_last_begun() {
        let fake = fake();
        let k = key(1, 1, 5);
        let a = begin(&fake, &k);
        let b = begin(&fake, &k);
        let ea = vec![part(&fake, &k, &a, 1, b"from A".to_vec())];
        let eb = vec![part(&fake, &k, &b, 1, b"from B".to_vec())];
        assert_eq!(
            send(
                &fake,
                builder().complete_multipart(now(), &k, &b, &eb).unwrap()
            )
            .status(),
            200
        );
        assert_eq!(
            send(
                &fake,
                builder().complete_multipart(now(), &k, &a, &ea).unwrap()
            )
            .status(),
            200
        );
        assert_eq!(
            fake.body_of(&object_key(&level_prefix(), &k)),
            Some(b"from A".to_vec())
        );
        // a plain PUT after also overwrites
        put(&fake, &k, b"from PUT", 0);
        assert_eq!(
            fake.body_of(&object_key(&level_prefix(), &k)),
            Some(b"from PUT".to_vec())
        );
    }

    #[test]
    fn small_non_final_part_is_refused_and_abort_forgets_the_upload() {
        let fake = fake();
        let k = key(1, 1, 5);
        let id = begin(&fake, &k);
        let etags = vec![
            part(&fake, &k, &id, 1, b"tiny".to_vec()),
            part(&fake, &k, &id, 2, b"tail".to_vec()),
        ];
        let refused = send(
            &fake,
            builder()
                .complete_multipart(now(), &k, &id, &etags)
                .unwrap(),
        );
        assert_eq!(refused.status(), 400);
        assert!(text(&refused).contains("EntityTooSmall"));
        assert_eq!(
            send(&fake, builder().abort_multipart(now(), &k, &id).unwrap()).status(),
            204
        );
        assert_eq!(fake.open_uploads(), 0);
        let again = send(&fake, builder().abort_multipart(now(), &k, &id).unwrap());
        assert_eq!(again.status(), 404);
        let late = send(
            &fake,
            builder()
                .complete_multipart(now(), &k, &id, &etags)
                .unwrap(),
        );
        assert!(text(&late).contains("NoSuchUpload"));
        assert!(fake.keys().is_empty(), "nothing was published");
    }

    #[test]
    fn bad_part_digest_is_refused_before_it_is_stored() {
        let fake = fake();
        let k = key(1, 1, 5);
        let id = begin(&fake, &k);
        let request = builder()
            .upload_part(now(), &k, &id, PartNumber::new(1).unwrap(), b"abc".to_vec())
            .unwrap()
            .tamper_body(b"abd".to_vec());
        let response = send(&fake, request);
        assert_eq!(
            classify(
                StoreOp::Upload,
                Some(&k),
                Some(PartNumber::new(1).unwrap()),
                &response
            ),
            Err(StoreError::Corrupt {
                key: Some(k),
                cause: CorruptCause::ChecksumMismatch {
                    part: Some(PartNumber::new(1).unwrap())
                }
            })
        );
    }
}
