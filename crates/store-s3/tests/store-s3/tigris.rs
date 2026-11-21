//! The S3 store against the real Tigris bucket (`just test-tigris`), task 8t.
//!
//! Every test is ignored: the recipe sources the credentials file, names a
//! per-run prefix under the bucket and runs them with `--run-ignored only`.
//! Each probe prints `OBS` lines (status, error code, the header that proves the
//! fact; redacted by `RealHarness::dump`) and asserts the behaviour docs/spikes.md
//! records for it, so a provider change shows up here first. The cases every
//! provider passes come from `common/real.rs`.

use std::time::Instant;

use lsr_core::conformance::block_on;
use lsr_core::{Clock, PartNumber, ReadLen};
use lsr_ltx::Timestamp;
use lsr_store_s3::{
    EtagCheck, S3Code, UploadId, check_etag, parse_complete, parse_create, parse_head, quoted_md5,
};

use crate::real::{self, RealHarness, STAMP_HEADER, Server, WallClock, at, body};

fn server() -> Server {
    let var = |name: &str| {
        std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set: run just test-tigris"))
    };
    Server {
        name: "tigris",
        endpoint: var("AWS_ENDPOINT_URL_S3"),
        bucket: std::env::var("LSR_TIGRIS_BUCKET").unwrap_or_else(|_| "litestream-rs-test".into()),
        region: var("AWS_REGION"),
        access: var("AWS_ACCESS_KEY_ID"),
        secret: var("AWS_SECRET_ACCESS_KEY"),
        root: Some(var("LSR_TIGRIS_PREFIX")),
    }
}

fn connect() -> Option<RealHarness> {
    Some(RealHarness::connect(&server()))
}

crate::real::shared_cases!(connect, all());

fn h() -> RealHarness {
    RealHarness::connect(&server())
}

fn obs(h: &RealHarness, what: &str, raw: &real::Raw) {
    eprintln!("OBS {what}: {}", h.dump(raw));
}

/// Prints the observation, then pins its status and error code.
fn expect(what: &str, r: &lsr_store_s3::HttpResponse, status: u16, code: Option<S3Code>) {
    obs_response(what, r);
    assert_eq!(r.status(), status, "{what}");
    assert_eq!(S3Code::from_body(r.body()), code, "{what}");
}

fn obs_response(what: &str, r: &lsr_store_s3::HttpResponse) {
    let mut line = format!("status={}", r.status());
    for name in [
        "etag",
        "content-range",
        "content-length",
        STAMP_HEADER,
        "date",
        "last-modified",
    ] {
        if let Some(v) = r.header(name) {
            line.push_str(&format!(" {name}={v}"));
        }
    }
    if let Some(code) = S3Code::from_body(r.body()) {
        line.push_str(&format!(" code={code:?}"));
    }
    eprintln!("OBS {what}: {line}");
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item1_unsigned_payload_with_signed_meta_and_consistent_headers() {
    let h = h();
    let key = h.key(0, 1, 1);
    let stamp = at("2024-05-01T12:00:00.123Z");
    let put = h
        .builder()
        .put_object(WallClock.now(), &key, b"hello".to_vec(), stamp)
        .unwrap();
    expect(
        "item1 signed put (UNSIGNED-PAYLOAD, meta + consistent signed)",
        &h.send(put),
        200,
        None,
    );
    let head = h.send(h.builder().head(WallClock.now(), &key).unwrap());
    obs_response("item1 head", &head);
    // A header sent but not signed, and a signed one not sent.
    let unsigned = h
        .builder()
        .put_object(WallClock.now(), &h.key(0, 2, 2), b"hi".to_vec(), stamp)
        .unwrap()
        .tamper_header("x-amz-meta-extra", Some("1".to_owned()));
    expect(
        "item1 header sent, not signed",
        &h.send(unsigned),
        200,
        None,
    );
    let missing = h
        .builder()
        .put_object(WallClock.now(), &h.key(0, 3, 3), b"hi".to_vec(), stamp)
        .unwrap()
        .tamper_header(STAMP_HEADER, None);
    expect(
        "item1 signed meta header not sent",
        &h.send(missing),
        403,
        Some(S3Code::SignatureDoesNotMatch),
    );
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item2_item3_content_md5_and_etag_shapes() {
    let h = h();
    let key = h.key(0, 1, 1);
    let wrong = h
        .builder()
        .put_object(
            WallClock.now(),
            &key,
            b"hello".to_vec(),
            Timestamp::from_unix_millis(1),
        )
        .unwrap()
        .tamper_body(b"other".to_vec());
    let sent = h.send(wrong);
    expect(
        "item2 put with wrong Content-MD5",
        &sent,
        400,
        Some(S3Code::BadDigest),
    );
    let read = block_on(lsr_core::ObjectStore::read(
        &h.store,
        &key,
        0,
        ReadLen::new(16).unwrap(),
    ));
    eprintln!("OBS item2 object after the wrong-digest put: {read:?}");
    assert!(matches!(read, Err(lsr_core::StoreError::NotFound { .. })));
    let good = h
        .builder()
        .put_object(
            WallClock.now(),
            &h.key(0, 2, 2),
            b"hello".to_vec(),
            Timestamp::from_unix_millis(1),
        )
        .unwrap();
    let sent = h.send(good);
    obs_response("item3 put etag", &sent);
    let md5 = {
        let want = quoted_md5(b"hello");
        eprintln!("OBS item3 expected etag {want}");
        sent.header("etag") == Some(want.as_str())
    };
    assert!(md5, "a single PUT's ETag is the hex MD5");
    let _ = (check_etag, EtagCheck::Matches);
}

fn start_multipart(h: &RealHarness, key: &lsr_core::FileKey, stamp: Timestamp) -> UploadId {
    let created = h.send(
        h.builder()
            .create_multipart(WallClock.now(), key, stamp)
            .unwrap(),
    );
    obs_response("create multipart", &created);
    parse_create(&created).unwrap()
}

fn put_part(
    h: &RealHarness,
    key: &lsr_core::FileKey,
    upload: &UploadId,
    n: u16,
    bytes: Vec<u8>,
) -> String {
    let part = h.send(
        h.builder()
            .upload_part(
                WallClock.now(),
                key,
                upload,
                PartNumber::new(n).unwrap(),
                bytes,
            )
            .unwrap(),
    );
    obs_response(&format!("part {n}"), &part);
    part.header("etag").unwrap().to_owned()
}

fn complete(
    h: &RealHarness,
    key: &lsr_core::FileKey,
    upload: &UploadId,
    etags: &[String],
) -> lsr_store_s3::HttpResponse {
    let done = h.send(
        h.builder()
            .complete_multipart(WallClock.now(), key, upload, etags)
            .unwrap(),
    );
    obs_response("complete multipart", &done);
    done
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item2_item3_wrong_part_digest_and_complete_etag() {
    let h = h();
    let key = h.key(0, 1, 1);
    let upload = start_multipart(&h, &key, Timestamp::from_unix_millis(1));
    let wrong = h
        .builder()
        .upload_part(
            WallClock.now(),
            &key,
            &upload,
            PartNumber::new(1).unwrap(),
            b"hello".to_vec(),
        )
        .unwrap()
        .tamper_body(b"other".to_vec());
    expect(
        "item2 part with wrong Content-MD5",
        &h.send(wrong),
        400,
        Some(S3Code::BadDigest),
    );
    let etag = put_part(&h, &key, &upload, 1, b"hello".to_vec());
    let done = complete(&h, &key, &upload, std::slice::from_ref(&etag));
    let whole = parse_complete(&done).unwrap();
    eprintln!("OBS item3 part etag {etag}; complete etag {whole}");
    assert!(
        whole.trim_matches('"').ends_with("-1"),
        "multipart ETag is md5-N"
    );
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item4_listing_pages_encoding_and_metadata() {
    let h = h();
    let list = |extra: &[(&str, &str)]| {
        let prefix = format!("{}/", h.prefix.as_str());
        let mut query = vec![("list-type", "2"), ("prefix", prefix.as_str())];
        query.extend_from_slice(extra);
        h.raw("GET", None, &query, &[], b"")
    };
    for name in ["a b+c&d=e", "caf\u{e9}", "plain"] {
        h.raw_put(
            &format!("{}/{name}", h.prefix.as_str()),
            b"x",
            &[(STAMP_HEADER, "2024-05-01T12:00:00.123Z")],
        );
    }
    let raw = list(&[("encoding-type", "url")]);
    obs(&h, "item4 encoding-type=url", &raw);
    eprintln!(
        "OBS item4 body {}",
        raw.text().chars().take(900).collect::<String>()
    );
    let raw = list(&[]);
    eprintln!(
        "OBS item4 without encoding-type {}",
        raw.text().chars().take(900).collect::<String>()
    );
    let raw = list(&[("metadata", "true")]);
    eprintln!(
        "OBS Q4 metadata=true {}",
        raw.text().chars().take(1200).collect::<String>()
    );
    let raw = list(&[("max-keys", "2")]);
    eprintln!(
        "OBS item4 max-keys=2 truncated={:?} token={:?}",
        raw.tag("IsTruncated"),
        raw.tag("NextContinuationToken").map(|t| t.len())
    );
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item5_range_past_the_end() {
    let h = h();
    h.put(
        &h.key(0, 1, 1),
        at("2024-05-01T12:00:00.123Z"),
        b"0123456789",
    );
    h.put(&h.key(0, 2, 2), at("2024-05-01T12:00:00.123Z"), b"");
    for (n, off) in [(1_u64, 10_u64), (1, 100), (1, 9), (2, 0)] {
        let r = h.send(
            h.builder()
                .get_range(
                    WallClock.now(),
                    &h.key(0, n, n),
                    off,
                    ReadLen::new(16).unwrap(),
                )
                .unwrap(),
        );
        obs_response(&format!("item5 key {n} offset {off}"), &r);
        assert_eq!(r.status() == 416, lsr_store_s3::is_range_past_end(&r));
        assert_eq!(r.status(), if off == 9 { 206 } else { 416 });
    }
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item6_delete_objects() {
    let h = h();
    let keys: Vec<_> = (1..=3).map(|n| h.key(0, n, n)).collect();
    for key in &keys {
        h.put(key, Timestamp::from_unix_millis(1), b"x");
    }
    let mut batch = keys.clone();
    batch.push(h.key(0, 99, 99));
    let done = h.send(h.builder().delete_objects(WallClock.now(), &batch).unwrap());
    obs_response("item6 delete 3 present + 1 missing, quiet", &done);
    eprintln!(
        "OBS item6 body {}",
        String::from_utf8_lossy(done.body())
            .chars()
            .take(500)
            .collect::<String>()
    );
    assert_eq!(
        lsr_store_s3::parse_delete_failures(&done),
        Ok(0),
        "a missing key is not a failure"
    );
    assert!(h.list_all(0, 10).files.is_empty());
}

fn overlap(h: &RealHarness, n: u64, complete_first: char) -> Vec<u8> {
    let key = h.key(0, n, n);
    let a = start_multipart(h, &key, at("2024-05-01T12:00:00.000Z"));
    let b = start_multipart(h, &key, at("2024-05-01T12:00:01.000Z"));
    let ea = put_part(h, &key, &a, 1, b"AAAA".to_vec());
    let eb = put_part(h, &key, &b, 1, b"BBBB".to_vec());
    if complete_first == 'A' {
        complete(h, &key, &a, &[ea]);
        complete(h, &key, &b, &[eb]);
    } else {
        complete(h, &key, &b, &[eb]);
        complete(h, &key, &a, &[ea]);
    }
    h.read_back(&key, 4)
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item7_two_uploads_to_one_key() {
    let h = h();
    let one = overlap(&h, 1, 'A');
    assert_eq!(one, b"BBBB", "item7: the last completed wins");
    let two = overlap(&h, 2, 'B');
    assert_eq!(two, b"AAAA", "item7: not the last initiated");
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
#[expect(clippy::disallowed_methods, reason = "measures real request latency")]
fn item9_head_latency_from_this_machine() {
    let h = h();
    let keys: Vec<_> = (1..=20).map(|n| h.key(0, n, n)).collect();
    for key in &keys {
        h.put(key, Timestamp::from_unix_millis(1), b"x");
    }
    let mut times = Vec::new();
    let total = Instant::now();
    for key in keys.iter().cycle().take(1000) {
        let start = Instant::now();
        let r = h.send(h.builder().head(WallClock.now(), key).unwrap());
        assert_eq!(r.status(), 200);
        parse_head(&r).unwrap();
        times.push(start.elapsed());
    }
    let total = total.elapsed();
    times.sort_unstable();
    let at = |p: usize| {
        times
            .get(times.len().saturating_mul(p) / 100)
            .copied()
            .unwrap_or_default()
    };
    eprintln!(
        "OBS item9 1000 sequential HEADs: p50={:?} p95={:?} total={:?}",
        at(50),
        at(95),
        total
    );
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn q2_q5_metadata_on_ranged_get_and_copy() {
    let h = h();
    let key = h.key(0, 1, 1);
    h.put(&key, at("2024-05-01T12:00:00.123Z"), b"0123456789");
    let r = h.send(
        h.builder()
            .get_range(WallClock.now(), &key, 2, ReadLen::new(4).unwrap())
            .unwrap(),
    );
    obs_response("Q2 ranged GET", &r);
    assert_eq!(
        r.header(STAMP_HEADER),
        Some("2024-05-01T12:00:00.123Z"),
        "Q2: metadata rides on a 206"
    );
    let source = format!("/{}/{}", h.server.bucket, h.object(&key));
    let dest = h.object(&h.key(0, 2, 2));
    let copy = h.raw(
        "PUT",
        Some(&dest),
        &[],
        &[("x-amz-copy-source", source.as_str())],
        b"",
    );
    obs(&h, "Q5 copy (default directive)", &copy);
    let head = h.send(h.builder().head(WallClock.now(), &h.key(0, 2, 2)).unwrap());
    obs_response("Q5 head of the copy", &head);
    assert_eq!(
        head.header(STAMP_HEADER),
        Some("2024-05-01T12:00:00.123Z"),
        "Q5: a copy keeps metadata"
    );
    let dest3 = h.object(&h.key(0, 3, 3));
    let copy = h.raw(
        "PUT",
        Some(&dest3),
        &[],
        &[
            ("x-amz-copy-source", source.as_str()),
            ("x-amz-metadata-directive", "REPLACE"),
        ],
        b"",
    );
    obs(&h, "Q5 copy (REPLACE)", &copy);
    let head = h.send(h.builder().head(WallClock.now(), &h.key(0, 3, 3)).unwrap());
    obs_response("Q5 head of the REPLACE copy", &head);
    assert_eq!(head.header(STAMP_HEADER), None, "Q5: REPLACE drops it");
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn q3_multipart_last_modified_is_initiation_or_completion() {
    let h = h();
    let key = h.key(0, 1, 1);
    let upload = start_multipart(&h, &key, Timestamp::from_unix_millis(1));
    let started = http_date(&h);
    // Wait on the server's clock, not ours: poll until its Date has moved 3 s.
    let mut now = started;
    while now.saturating_sub(started) < 3 {
        now = http_date(&h);
    }
    let etag = put_part(&h, &key, &upload, 1, b"x".to_vec());
    complete(&h, &key, &upload, &[etag]);
    let done = http_date(&h);
    let page = h.send(
        h.builder()
            .list_page(
                WallClock.now(),
                lsr_core::Level::new(0).unwrap(),
                None,
                lsr_core::PageLimit::new(10).unwrap(),
            )
            .unwrap(),
    );
    let text = String::from_utf8_lossy(page.body()).into_owned();
    let listed = text
        .split_once("<LastModified>")
        .and_then(|(_, rest)| rest.split_once("</LastModified>"))
        .map(|(value, _)| value.to_owned())
        .unwrap();
    let modified = listed.parse::<jiff::Timestamp>().unwrap().as_second();
    eprintln!("OBS Q3 initiation={started} completion<={done} LastModified={modified}");
    assert!(
        modified >= started.saturating_add(3),
        "Q3: LastModified is the completion time"
    );
}

/// The server's clock in unix seconds from a cheap request's Date header.
fn http_date(h: &RealHarness) -> i64 {
    let raw = h.raw("HEAD", None, &[], &[], b"");
    let date = raw.header("date").unwrap();
    jiff::fmt::rfc2822::parse(date)
        .unwrap()
        .timestamp()
        .as_second()
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item8_abort_of_unknown_upload_and_multipart_listing() {
    let h = h();
    let key = h.key(0, 1, 1);
    let gone = h.raw(
        "DELETE",
        Some(&h.object(&key)),
        &[("uploadId", "no-such-id")],
        &[],
        b"",
    );
    obs(&h, "abort of an unknown upload", &gone);
    let upload = start_multipart(&h, &key, Timestamp::from_unix_millis(1));
    put_part(&h, &key, &upload, 1, body(real::PART));
    let listed = h.raw("GET", None, &[("uploads", "")], &[], b"");
    obs(&h, "ListMultipartUploads with one open upload", &listed);
    eprintln!(
        "OBS lmu body {}",
        listed
            .text()
            .replace('\n', " ")
            .chars()
            .take(700)
            .collect::<String>()
    );
    let prefixed = h.raw(
        "GET",
        None,
        &[("uploads", ""), ("prefix", h.prefix.as_str())],
        &[],
        b"",
    );
    eprintln!(
        "OBS lmu prefixed body {}",
        prefixed
            .text()
            .replace('\n', " ")
            .chars()
            .take(700)
            .collect::<String>()
    );
    let abort = h.send(
        h.builder()
            .abort_multipart(WallClock.now(), &key, &upload)
            .unwrap(),
    );
    obs_response("abort of the open upload", &abort);
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn item11_load_shedding_headers_seen_so_far() {
    let h = h();
    h.put(&h.key(0, 1, 1), Timestamp::from_unix_millis(1), &body(10));
    let names = h.counts.header_names.lock().unwrap().clone();
    let statuses = h.counts.statuses.lock().unwrap().clone();
    eprintln!("OBS item11 statuses seen {statuses:?}; response header names {names:?}");
}

/// The object's bytes by a raw GET, `None` when it is absent.
fn raw_body(h: &RealHarness, object: &str) -> Option<Vec<u8>> {
    let got = h.raw("GET", Some(object), &[], &[], b"");
    match got.status {
        200 => Some(got.body),
        404 => None,
        other => panic!("GET {other}: {}", h.dump(&got)),
    }
}

/// A raw CompleteMultipartUpload of one part, with `headers` signed.
fn raw_complete(
    h: &RealHarness,
    object: &str,
    upload: &UploadId,
    etag: &str,
    headers: &[(&'static str, &str)],
) -> real::Raw {
    let xml = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>\
             <ETag>{etag}</ETag></Part></CompleteMultipartUpload>"
    );
    h.raw(
        "POST",
        Some(object),
        &[("uploadId", upload.as_str())],
        headers,
        xml.as_bytes(),
    )
}

// G4 (docs/proposals/beyond-go.md): what a conditional write does on Tigris.
// Measured only; the store sends no conditional header (D6).

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn g4_if_none_match_on_put() {
    let h = h();
    let object = h.object(&h.key(0, 1, 1));
    let star = [("if-none-match", "*")];
    let first = h.raw("PUT", Some(&object), &[], &star, b"first");
    obs(&h, "G4 PUT If-None-Match:* to a new key", &first);
    let second = h.raw("PUT", Some(&object), &[], &star, b"second");
    obs(&h, "G4 PUT If-None-Match:* to an existing key", &second);
    let after = raw_body(&h, &object);
    eprintln!(
        "OBS G4 object after both: {:?}",
        after.as_deref().map(String::from_utf8_lossy)
    );
    assert_eq!(first.status, 200, "G4: a new key is written");
    assert_eq!(second.status, 412, "G4: an existing key refuses");
    assert_eq!(second.tag("Code").as_deref(), Some("PreconditionFailed"));
    assert_eq!(after.as_deref(), Some(&b"first"[..]), "G4: unchanged");
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn g4_if_none_match_on_complete_multipart() {
    let h = h();
    let key = h.key(0, 1, 1);
    let object = h.object(&key);
    let star = [("if-none-match", "*")];
    let a = start_multipart(&h, &key, Timestamp::from_unix_millis(1));
    let ea = put_part(&h, &key, &a, 1, b"AAAA".to_vec());
    let b = start_multipart(&h, &key, Timestamp::from_unix_millis(2));
    let eb = put_part(&h, &key, &b, 1, b"BBBB".to_vec());
    let first = raw_complete(&h, &object, &a, &ea, &star);
    obs(&h, "G4 Complete If-None-Match:* to a new key", &first);
    let second = raw_complete(&h, &object, &b, &eb, &star);
    obs(
        &h,
        "G4 Complete If-None-Match:* to an existing key",
        &second,
    );
    let after = raw_body(&h, &object);
    eprintln!(
        "OBS G4 object after both: {:?}",
        after.as_deref().map(String::from_utf8_lossy)
    );
    let open = h.open_uploads();
    eprintln!("OBS G4 open uploads after the refused complete: {open}");
    let abort = h.send(
        h.builder()
            .abort_multipart(WallClock.now(), &key, &b)
            .unwrap(),
    );
    obs_response("G4 abort of the refused upload", &abort);
    let put = h.raw("PUT", Some(&h.object(&h.key(0, 2, 2))), &[], &[], b"x");
    let c = start_multipart(&h, &h.key(0, 2, 2), Timestamp::from_unix_millis(3));
    let ec = put_part(&h, &h.key(0, 2, 2), &c, 1, b"CCCC".to_vec());
    let over_put = raw_complete(&h, &h.object(&h.key(0, 2, 2)), &c, &ec, &star);
    obs(
        &h,
        "G4 Complete If-None-Match:* over a single PUT",
        &over_put,
    );
    let _ = h.send(
        h.builder()
            .abort_multipart(WallClock.now(), &h.key(0, 2, 2), &c)
            .unwrap(),
    );
    assert_eq!(put.status, 200);
    assert_eq!(first.status, 200, "G4: a new key completes");
    assert!(
        first.tag("Code").is_none(),
        "G4: no error document in the 200"
    );
    assert_eq!(
        second.status, 412,
        "G4: an existing key refuses the complete"
    );
    assert_eq!(second.tag("Code").as_deref(), Some("PreconditionFailed"));
    assert_eq!(after.as_deref(), Some(&b"AAAA"[..]), "G4: unchanged");
    assert_eq!(
        over_put.status, 412,
        "G4: a PUT's object counts as existing"
    );
}

#[test]
#[ignore = "needs a Tigris bucket (just test-tigris)"]
fn g4_if_match_on_put() {
    let h = h();
    let object = h.object(&h.key(0, 1, 1));
    let v1 = h.raw("PUT", Some(&object), &[], &[], b"v1");
    let e1 = v1.header("etag").unwrap().to_owned();
    let right = h.raw(
        "PUT",
        Some(&object),
        &[],
        &[("if-match", e1.as_str())],
        b"v2",
    );
    obs(&h, "G4 PUT If-Match with the current ETag", &right);
    let after_right = raw_body(&h, &object);
    let wrong = h.raw(
        "PUT",
        Some(&object),
        &[],
        &[("if-match", e1.as_str())],
        b"v3",
    );
    obs(&h, "G4 PUT If-Match with a stale ETag", &wrong);
    let after_wrong = raw_body(&h, &object);
    let missing = h.object(&h.key(0, 2, 2));
    let absent = h.raw(
        "PUT",
        Some(&missing),
        &[],
        &[("if-match", e1.as_str())],
        b"v4",
    );
    obs(&h, "G4 PUT If-Match to an absent key", &absent);
    let after_absent = raw_body(&h, &missing);
    eprintln!(
        "OBS G4 after right {:?}, after wrong {:?}, absent key {:?}",
        after_right.as_deref().map(String::from_utf8_lossy),
        after_wrong.as_deref().map(String::from_utf8_lossy),
        after_absent.as_deref().map(String::from_utf8_lossy),
    );
    assert_eq!(right.status, 200, "G4: the current ETag writes");
    assert_eq!(after_right.as_deref(), Some(&b"v2"[..]));
    assert_eq!(wrong.status, 412, "G4: a stale ETag refuses");
    assert_eq!(wrong.tag("Code").as_deref(), Some("PreconditionFailed"));
    assert_eq!(after_wrong.as_deref(), Some(&b"v2"[..]), "G4: unchanged");
    assert_eq!(absent.status, 404, "G4: If-Match on an absent key");
    assert_eq!(absent.tag("Code").as_deref(), Some("NoSuchKey"));
    assert_eq!(after_absent, None, "G4: nothing written");
}
