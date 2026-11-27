//! The order a provider lists names in (issue 342). Cloudflare R2 lists a name that is the start
//! of others after all of them (`X.ltx.tmp`, `X.ltx`, `X`); AWS S3, Tigris and MinIO list bytewise.
//! The store accepts any order inside a page and still refuses a listing it cannot trust.

use crate::common;
use crate::rig;

#[cfg(test)]
mod cases {
    use std::collections::VecDeque;
    use std::io;
    use std::sync::{Mutex, PoisonError};

    use super::common::*;
    use super::rig::*;
    use lsr_core::conformance::block_on;
    use lsr_core::{FakeClock, FileKey, Level, Listing, ObjectStore, StoreError, StoreOp};
    use lsr_store_s3::{
        HttpRequest, HttpResponse, S3Store, Transport, TransportError, level_prefix,
    };
    use proptest::prelude::*;

    /// The shared suite on a bucket that lists as R2 does.
    mod conformance {
        use super::*;

        lsr_core::conformance_suite!(r2);
    }

    fn r2() -> FakeHarness {
        let h = FakeHarness::new();
        h.wire.inner().list_like_r2();
        h
    }

    fn file_name(n: u64) -> String {
        key(0, n, n).file_name()
    }

    /// Answers each request with the next scripted response and counts them.
    struct Script(Mutex<VecDeque<HttpResponse>>);

    impl Transport for Script {
        fn send(&self, _request: HttpRequest) -> Result<HttpResponse, TransportError> {
            let mut queue = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            queue
                .pop_front()
                .ok_or(TransportError::Io(io::ErrorKind::UnexpectedEof))
        }
    }

    fn page(names: &[String], token: Option<&str>) -> HttpResponse {
        let lead = level_prefix(config().prefix(), Level::ZERO);
        let contents: String = names
            .iter()
            .map(|name| {
                format!(
                    "<Contents><Key>{lead}{name}</Key><LastModified>2026-11-20T13:49:45.000Z</LastModified><Size>7</Size></Contents>"
                )
            })
            .collect();
        let tail = token.map_or_else(
            || "<IsTruncated>false</IsTruncated>".to_owned(),
            |t| format!("<IsTruncated>true</IsTruncated><NextContinuationToken>{t}</NextContinuationToken>"),
        );
        let body = format!(
            "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name>{contents}{tail}</ListBucketResult>"
        );
        HttpResponse::new(200, Vec::new(), body.into_bytes(), true)
    }

    fn scripted(pages: Vec<HttpResponse>) -> S3Store<Script> {
        S3Store::with_transport(
            config(),
            credentials(),
            Script(Mutex::new(pages.into())),
            FakeClock::new(now()),
        )
        .unwrap()
    }

    fn list(store: &impl ObjectStore, raw: u16) -> Result<Listing, StoreError> {
        block_on(store.list(Level::ZERO, None, limit(raw)))
    }

    fn refused() -> StoreError {
        StoreError::Transient {
            op: StoreOp::List,
            kind: io::ErrorKind::InvalidData,
        }
    }

    /// The names the issue recorded, as R2 lists them, with their files.
    #[test]
    fn a_stray_name_that_extends_a_file_name_lists_on_r2() {
        let h = r2();
        let (one, two) = (file_name(1), file_name(2));
        let bare = one.trim_end_matches(".ltx").to_owned();
        let lead = level_prefix(config().prefix(), Level::ZERO);
        for name in [&one, &two, &format!("{one}.tmp"), &bare, &"0".to_owned()] {
            h.wire
                .inner()
                .put_raw(&format!("{lead}{name}"), b"x".to_vec(), &[]);
        }
        let got = list(&h.store, 1000).unwrap();
        let keys: Vec<FileKey> = got.files.iter().map(|f| f.key).collect();
        assert_eq!(keys, [key(0, 1, 1), key(0, 2, 2)]);
        assert_eq!(
            got.skipped, 2,
            "the bare name and \"0\", not the scratch file"
        );
        assert_eq!(got.next, None);
    }

    fn names() -> Vec<String> {
        let (one, two, three) = (file_name(1), file_name(2), file_name(3));
        let bare = one.trim_end_matches(".ltx").to_owned();
        vec![
            format!("{one}.tmp"),
            two,
            "0".to_owned(),
            one,
            bare,
            "00".to_owned(),
            three,
            "zz".to_owned(),
        ]
    }

    #[test]
    fn a_page_in_any_order_lists_the_same() {
        let sorted = {
            let mut sorted = names();
            sorted.sort();
            list(&scripted(vec![page(&sorted, None)]), 1000).unwrap()
        };
        assert_eq!(sorted.files.len(), 3);
        assert_eq!(sorted.skipped, 4);
        let mut shuffled = names();
        for turn in 0..shuffled.len() {
            shuffled.rotate_left(turn);
            let got = list(&scripted(vec![page(&shuffled, None)]), 1000).unwrap();
            assert_eq!(got, sorted, "{shuffled:?}");
            shuffled.reverse();
            let got = list(&scripted(vec![page(&shuffled, None)]), 1000).unwrap();
            assert_eq!(got, sorted, "{shuffled:?}");
        }
    }

    #[test]
    fn a_listing_that_goes_back_to_a_file_it_passed_is_refused() {
        let (a, b, c) = (file_name(1), file_name(2), file_name(3));
        let store = scripted(vec![page(&[b, c], Some("t1")), page(&[a], None)]);
        assert_eq!(list(&store, 3), Err(refused()));
    }

    #[test]
    fn a_file_listed_again_on_the_next_page_is_refused() {
        let (a, b) = (file_name(1), file_name(2));
        let store = scripted(vec![
            page(&[a.clone(), b.clone()], Some("t1")),
            page(&[b, file_name(3)], None),
        ]);
        assert_eq!(list(&store, 5), Err(refused()));
    }

    #[test]
    fn a_stray_name_listed_again_on_the_next_page_is_refused() {
        let (a, b) = (file_name(1), file_name(2));
        let store = scripted(vec![
            page(&[a, "README".to_owned()], Some("t1")),
            page(&["README".to_owned(), b], None),
        ]);
        assert_eq!(list(&store, 5), Err(refused()));
    }

    #[test]
    fn a_name_listed_twice_in_one_page_is_refused() {
        let a = file_name(1);
        let store = scripted(vec![page(&[a.clone(), a], None)]);
        assert_eq!(list(&store, 5), Err(refused()));
        let store = scripted(vec![page(&["x".to_owned(), "x".to_owned()], None)]);
        assert_eq!(list(&store, 5), Err(refused()));
    }

    #[test]
    fn a_continuation_token_sent_back_is_refused() {
        let (a, b) = (file_name(1), file_name(2));
        let store = scripted(vec![page(&[a], Some("t1")), page(&[b], Some("t1"))]);
        assert_eq!(list(&store, 5), Err(refused()));
    }

    #[test]
    fn a_key_outside_the_level_is_refused() {
        let body = page(&[], None);
        let text = String::from_utf8(body.body().to_vec()).unwrap().replace(
            "<IsTruncated>",
            "<Contents><Key>db/0001/x</Key><LastModified>2026-11-20T13:49:45.000Z</LastModified><Size>7</Size></Contents><IsTruncated>",
        );
        let response = HttpResponse::new(200, Vec::new(), text.into_bytes(), true);
        assert_eq!(list(&scripted(vec![response]), 5), Err(refused()));
    }

    /// A population of files and the stray names that extend one another.
    fn population() -> impl Strategy<Value = Vec<String>> {
        let files = proptest::collection::btree_set(1u64..40, 0..9);
        let strays = proptest::sample::subsequence(
            vec!["0", "00", "README", "zz", "0000000000000002", "a", "a1"],
            0..6,
        );
        let extras = proptest::sample::subsequence(vec![1u64, 2, 3, 4, 5, 6, 7, 8, 9], 0..4);
        (files, strays, extras).prop_map(|(files, strays, extras)| {
            let mut names: Vec<String> = files.iter().map(|n| file_name(*n)).collect();
            names.extend(strays.into_iter().map(str::to_owned));
            for n in extras {
                let base = file_name(n);
                names.push(base.trim_end_matches(".ltx").to_owned());
                names.push(format!("{base}.bak"));
                names.push(format!("{base}.tmp"));
            }
            names.sort();
            names.dedup();
            names
        })
    }

    proptest! {
        /// Any order of one page lists the same files and cursor as the sorted page, and the same
        /// count of unrecognized names whenever the page ends the listing. Where the page is cut
        /// by its limit, the count follows the order the provider listed (R2 lists a name after
        /// the names that extend it, and its next listing finds it there), so it may differ.
        #[test]
        fn any_permutation_of_a_page_gives_the_same_listing(
            (names, shuffled) in population().prop_flat_map(|names| {
                let shuffled = Just(names.clone()).prop_shuffle();
                (Just(names), shuffled)
            }),
            raw in 1u16..12,
        ) {
            let want = list(&scripted(vec![page(&names, None)]), raw).unwrap();
            let got = list(&scripted(vec![page(&shuffled, None)]), raw).unwrap();
            prop_assert_eq!(&got.files, &want.files);
            prop_assert_eq!(got.next, want.next);
            if got.next.is_none() {
                prop_assert_eq!(got.skipped, want.skipped);
            }
        }

        /// A bucket that lists as R2 does yields the files and the count a bytewise one does,
        /// page after page.
        #[test]
        fn r2_order_and_bytewise_order_page_to_the_same_inventory(
            names in population(),
            raw in 1u16..12,
        ) {
            let walk = |r2: bool| {
                let h = FakeHarness::new();
                if r2 {
                    h.wire.inner().list_like_r2();
                }
                let lead = level_prefix(config().prefix(), Level::ZERO);
                for name in &names {
                    h.wire.inner().put_raw(&format!("{lead}{name}"), b"x".to_vec(), &[]);
                }
                let (mut keys, mut skipped, mut cursor) = (Vec::new(), 0usize, None);
                for _ in 0..100 {
                    let got = block_on(h.store.list(Level::ZERO, cursor, limit(raw))).unwrap();
                    keys.extend(got.files.iter().map(|f| f.key));
                    skipped = skipped.saturating_add(got.skipped);
                    match got.next {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
                (keys, skipped)
            };
            prop_assert_eq!(walk(true), walk(false));
        }
    }
}
