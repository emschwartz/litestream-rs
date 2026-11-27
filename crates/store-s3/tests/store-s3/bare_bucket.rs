//! A replica with no path inside the bucket (`s3://bucket`), as under Go: every key starts
//! with a slash (`/0000/<file>.ltx`), and no layer may collapse it, encode it or drop it.

use crate::common;
use crate::rig;

#[cfg(test)]
mod cases {
    use super::common::*;
    use super::rig::*;
    use lsr_core::conformance::block_on;
    use lsr_core::{FileKey, Level, ObjectStore, PageLimit, ReadLen, Upload, WriteMode};
    use lsr_ltx::Timestamp;
    use lsr_store_s3::{Shape, UrlStyle, object_key};

    mod conformance {
        use super::*;

        lsr_core::conformance_suite!(bare);
    }

    const PART: usize = 5 * 1024 * 1024;

    fn bare() -> FakeHarness {
        FakeHarness::with_config(bare_config())
    }

    fn name(k: &FileKey) -> String {
        k.file_name()
    }

    fn write_all(h: &FakeHarness, k: &FileKey, bytes: &[u8]) {
        block_on(async {
            let mut up = h
                .store
                .begin_upload(k, Timestamp::from_unix_millis(5), WriteMode::Replace)
                .await
                .unwrap();
            up.write(bytes).await.unwrap();
            up.commit().await.unwrap();
        });
    }

    #[test]
    fn a_bare_prefix_is_valid_and_keys_start_with_a_slash() {
        let config = bare_config();
        assert_eq!(config.prefix().as_str(), "");
        assert_eq!(
            object_key(config.prefix(), &key(0, 1, 1)),
            "/0000/0000000000000001-0000000000000001.ltx"
        );
    }

    #[test]
    fn the_fake_holds_exactly_go_s_keys() {
        let h = bare();
        write_all(&h, &key(0, 1, 1), b"zero");
        write_all(&h, &key(1, 1, 4), b"one");
        write_all(&h, &key(9, 5, 5), b"nine");
        assert_eq!(
            h.wire.inner().keys(),
            [
                "/0000/0000000000000001-0000000000000001.ltx",
                "/0001/0000000000000001-0000000000000004.ltx",
                "/0009/0000000000000005-0000000000000005.ltx",
            ]
        );
    }

    #[test]
    fn put_get_and_ranged_get_round_trip() {
        let h = bare();
        let k = key(0, 1, 1);
        write_all(&h, &k, b"abcdefghij");
        assert_eq!(
            block_on(h.store.read(&k, 0, ReadLen::new(10).unwrap())).unwrap(),
            b"abcdefghij"
        );
        assert_eq!(
            block_on(h.store.read(&k, 3, ReadLen::new(4).unwrap())).unwrap(),
            b"defg"
        );
        assert_eq!(
            h.wire.inner().body_of(&format!("/0000/{}", name(&k))),
            Some(b"abcdefghij".to_vec())
        );
    }

    #[test]
    fn listing_one_level_and_every_level_finds_the_files() {
        let h = bare();
        write_all(&h, &key(0, 1, 1), b"a");
        write_all(&h, &key(0, 2, 2), b"b");
        write_all(&h, &key(2, 1, 2), b"c");
        let level = |l: Level| block_on(h.store.list(l, None, PageLimit::MAX)).unwrap();
        assert_eq!(level(Level::ZERO).files.len(), 2);
        let all: usize = Level::ALL.into_iter().map(|l| level(l).files.len()).sum();
        assert_eq!(all, 3);
        assert_eq!(level(Level::new(2).unwrap()).files.len(), 1);
        assert_eq!(level(Level::new(1).unwrap()).files.len(), 0);
        assert!(level(Level::ZERO).skipped == 0);
    }

    #[test]
    fn a_batch_delete_removes_the_slash_keys() {
        let h = bare();
        let keys = [key(0, 1, 1), key(0, 2, 2), key(1, 1, 2)];
        for k in &keys {
            write_all(&h, k, b"x");
        }
        h.wire.inner().put_raw("/notes.txt", b"stay".to_vec(), &[]);
        block_on(h.store.delete(&keys)).unwrap();
        assert_eq!(h.wire.inner().keys(), ["/notes.txt"]);
        assert_eq!(count(&h.wire, Shape::DeleteObjects), 1);
    }

    #[test]
    fn a_multipart_upload_starts_parts_completes_and_aborts_on_slash_keys() {
        let h = bare();
        let k = key(0, 1, 1);
        let bytes: Vec<u8> = (0..=250).cycle().take(PART + 7).collect();
        write_all(&h, &k, &bytes);
        assert_eq!(count(&h.wire, Shape::CreateMultipart), 1);
        assert_eq!(count(&h.wire, Shape::Complete), 1);
        assert_eq!(h.wire.inner().keys(), [format!("/0000/{}", name(&k))]);
        assert_eq!(
            h.wire.inner().size_of(&format!("/0000/{}", name(&k))),
            Some(bytes.len())
        );

        let aborted = key(0, 2, 2);
        block_on(async {
            let mut up = h
                .store
                .begin_upload(&aborted, Timestamp::from_unix_millis(5), WriteMode::Replace)
                .await
                .unwrap();
            up.write(&bytes).await.unwrap();
            assert_eq!(h.wire.inner().open_uploads(), 1);
            up.abort().await.unwrap();
        });
        assert_eq!(count(&h.wire, Shape::Abort), 1);
        assert_eq!(h.wire.inner().open_uploads(), 0);
        assert_eq!(h.wire.inner().keys(), [format!("/0000/{}", name(&k))]);
    }

    /// The orphan sweep lists the bucket's open uploads and keeps the ones whose key is
    /// `/<level>/<file>`; a key without the leading slash is not ours.
    #[test]
    fn the_orphan_listing_finds_the_slash_keys_and_only_those() {
        let h = bare();
        block_on(async {
            let mut up = h
                .store
                .begin_upload(
                    &key(0, 1, 1),
                    Timestamp::from_unix_millis(5),
                    WriteMode::Replace,
                )
                .await
                .unwrap();
            up.write(&vec![7; PART]).await.unwrap();
        });
        let builder = lsr_store_s3::RequestBuilder::new(&bare_config(), &credentials()).unwrap();
        let clock = lsr_core::FakeClock::new(now());
        for foreign in ["/notes.ltx", "0000/0000000000000002-0000000000000002.ltx"] {
            lsr_store_s3::abandon_upload(&*h.wire, &builder, &clock, foreign, vec![7; PART])
                .unwrap();
        }
        assert_eq!(h.wire.inner().open_uploads(), 3);
        let next = lsr_store_s3::S3Store::with_transport(
            bare_config(),
            credentials(),
            Shared(std::sync::Arc::clone(&h.wire)),
            lsr_core::FakeClock::new(now()),
        )
        .unwrap();
        assert_eq!(block_on(next.remove_scratch()), Ok(1));
        assert_eq!(h.wire.inner().open_uploads(), 2, "the foreign uploads stay");
    }

    /// Every request goes out with the empty segment unchanged, in both URL styles.
    #[test]
    fn every_request_form_keeps_the_empty_segment_in_its_url() {
        for (style, lead) in [
            (UrlStyle::Path, "/bucket//0000/"),
            (UrlStyle::VirtualHost, "//0000/"),
        ] {
            let config = bare_config().with_style(style);
            let b = lsr_store_s3::RequestBuilder::new(&config, &credentials()).unwrap();
            let k = key(0, 1, 1);
            let want = format!("{lead}{}", name(&k));
            let urls = [
                b.put_object(now(), &k, b"x".to_vec(), now()).unwrap(),
                b.head(now(), &k).unwrap(),
                b.get_range(now(), &k, 0, ReadLen::new(1).unwrap()).unwrap(),
                b.create_multipart(now(), &k, now()).unwrap(),
            ];
            for request in urls {
                assert_eq!(request.url().path(), want, "{style:?}");
            }
            let list = b.list_page(now(), Level::ZERO, None, limit(10)).unwrap();
            let prefix = list
                .url()
                .query_pairs()
                .find(|(n, _)| n == "prefix")
                .map(|(_, v)| v.into_owned());
            assert_eq!(prefix.as_deref(), Some("/0000/"), "{style:?}");
        }
    }
}
