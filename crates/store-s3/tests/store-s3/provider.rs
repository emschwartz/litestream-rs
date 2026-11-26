//! The S3 store against one real provider's bucket: Tigris, AWS S3 or Cloudflare R2
//! (`just test-provider <tigris|aws|r2>`; `just test-tigris` is the Tigris case).
//!
//! Every test is ignored: the recipe (`scripts/provider.sh`) sources the provider's credentials
//! file, names a per-run prefix under the bucket, runs them with `--run-ignored only` and afterwards
//! deletes the prefix and aborts its multipart uploads, failing if either lists non-empty. The cases
//! every provider must pass (the conformance suite and `common/real.rs`) run here through
//! `shared_cases!`; the rest are the safety-critical checks of docs/release-bar.md R9: each is
//! one named test with a plain assertion and prints `OBS` lines (status, error code) that record
//! what the provider answered.
//!
//! The checks, by the engine decision that rests on each:
//! - create-only writes (the second-writer refusal): a PUT and a multipart Complete with
//!   `If-None-Match: *` refuse an existing key with an answer the store reads as "already there",
//!   and two racing writers never both win;
//! - consistency (listing decisions and restore): a write is listed and readable at once from a
//!   second connection; a long listing pages with no repeat and no gap; a delete shows in the
//!   next listing;
//! - error classification (every retry decision): wrong keys, a missing bucket or object, an
//!   expired, future-dated or damaged signature each map to one class;
//! - abandoned multipart uploads are visible to ListMultipartUploads and can be aborted.
//!
//! Not here: a request cut mid-body needs a TCP fault proxy (R9(c), toxiproxy against MinIO).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Barrier, Mutex};

use lsr_core::conformance::block_on;
use lsr_core::needs::real_bucket;
use lsr_core::{
    Class, Clock, Denied, FileKey, Level, ObjectStore, PageLimit, ReadLen, StoreError, StoreOp,
    Upload, WriteMode, classify as class_of,
};
use lsr_ltx::{Timestamp, Txid, TxidRange};
use lsr_store_s3::{
    BucketName, Endpoint, HttpRequest, Precondition, Prefix, Region, RequestBuilder, S3Config,
    S3Credentials, S3Store, SecretString, Transport, UploadId, UreqTransport, abort_upload,
    classify, error_in_ok_body, open_uploads,
};

use crate::real::{Counting, Raw, RealHarness, Server, WallClock};

/// The run's bucket, from the variables `scripts/provider.sh` exports.
pub fn server() -> Server {
    let bucket = real_bucket();
    Server {
        name: bucket.provider,
        endpoint: bucket.endpoint,
        bucket: bucket.bucket,
        region: bucket.region,
        access: bucket.access,
        secret: bucket.secret,
        root: Some(bucket.prefix),
    }
}

fn connect() -> Option<RealHarness> {
    Some(RealHarness::connect(&server()))
}

/// A harness on a fresh prefix of the run's bucket.
pub fn h() -> RealHarness {
    RealHarness::connect(&server())
}

crate::real::shared_cases!(connect, all());

fn ts(millis: i64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

fn obs(h: &RealHarness, what: &str, raw: &Raw) {
    eprintln!("OBS {what}: {}", h.dump(raw));
}

fn obs_response(h: &RealHarness, what: &str, r: &lsr_store_s3::HttpResponse) {
    obs(
        h,
        what,
        &Raw {
            status: r.status(),
            headers: r.headers().to_vec(),
            body: r.body().to_vec(),
        },
    );
}

/// The object's bytes by a raw GET, `None` when it is absent.
pub fn raw_body(h: &RealHarness, object: &str) -> Option<Vec<u8>> {
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
) -> Raw {
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

/// A multipart upload of `key` with one small part (the last part may be any size), left open:
/// its id and the part's ETag. No `OBS` line, so a loop stays quiet.
fn open_with_one_part(h: &RealHarness, key: &FileKey, bytes: &[u8]) -> (UploadId, String) {
    let created = h.send(
        h.builder()
            .create_multipart(WallClock.now(), key, ts(1))
            .unwrap(),
    );
    let upload = lsr_store_s3::parse_create(&created).unwrap();
    let part = h.send(
        h.builder()
            .upload_part(
                WallClock.now(),
                key,
                &upload,
                lsr_core::PartNumber::new(1).unwrap(),
                bytes.to_vec(),
            )
            .unwrap(),
    );
    assert_eq!(part.status(), 200, "part of {key:?}");
    let etag = part.header("etag").unwrap().to_owned();
    (upload, etag)
}

fn abort(h: &RealHarness, key: &FileKey, upload: &UploadId) {
    let done = h.send(
        h.builder()
            .abort_multipart(WallClock.now(), key, upload)
            .unwrap(),
    );
    assert!(
        done.status() == 204 || done.status() == 404,
        "abort answered {}",
        done.status()
    );
}

/// How the store reads the answer to a complete: the status's error, else the error in a 2xx
/// body, else `None` (the complete succeeded).
fn judged(key: &FileKey, r: &lsr_store_s3::HttpResponse) -> Option<StoreError> {
    classify(StoreOp::Upload, Some(key), None, r)
        .err()
        .or_else(|| error_in_ok_body(StoreOp::Upload, key, r))
}

// Check 1: create-only PUT.

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn create_only_put_refuses_an_existing_key_as_already_there() {
    let h = h();
    let key = h.key(0, 1, 1);
    let object = h.object(&key);
    let star = [("if-none-match", "*")];
    let first = h.raw("PUT", Some(&object), &[], &star, b"first");
    obs(&h, "create-only PUT to a new key", &first);
    let second = h.raw("PUT", Some(&object), &[], &star, b"second");
    obs(&h, "create-only PUT to an existing key", &second);
    assert_eq!(first.status, 200, "a new key is written");
    assert_eq!(second.status, 412, "an existing key refuses");
    assert_eq!(second.tag("Code").as_deref(), Some("PreconditionFailed"));
    assert_eq!(raw_body(&h, &object).as_deref(), Some(&b"first"[..]));

    // The engine's reading of the same answer, built by its own request builder.
    let third = h.send(
        h.builder()
            .put_object_if(
                WallClock.now(),
                &key,
                b"third".to_vec(),
                ts(1),
                Precondition::KeyIsFree,
            )
            .unwrap(),
    );
    assert_eq!(
        classify(StoreOp::Upload, Some(&key), None, &third),
        Err(StoreError::AlreadyExists { key })
    );
    let err = block_on(async {
        let mut upload = h
            .store
            .begin_upload(&key, ts(2), WriteMode::New)
            .await
            .unwrap();
        upload.write(b"fourth").await.unwrap();
        upload.commit().await.unwrap_err()
    });
    assert_eq!(err, StoreError::AlreadyExists { key });
    assert_eq!(class_of(&err), Class::Permanent, "never retried");
    assert_eq!(h.store.write_stats().refused_taken, 1);
    assert_eq!(
        h.read_back(&key, 5),
        b"first",
        "the first writer's bytes stay"
    );
}

// Check 2: create-only multipart Complete.

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn create_only_complete_refuses_an_existing_key_as_already_there() {
    let h = h();
    let key = h.key(0, 1, 1);
    let object = h.object(&key);
    let star = [("if-none-match", "*")];
    let (a, ea) = open_with_one_part(&h, &key, b"AAAA");
    let (b, eb) = open_with_one_part(&h, &key, b"BBBB");
    let first = raw_complete(&h, &object, &a, &ea, &star);
    obs(&h, "create-only Complete to a new key", &first);
    let second = raw_complete(&h, &object, &b, &eb, &star);
    obs(&h, "create-only Complete to an existing key", &second);
    assert_eq!(first.status, 200, "a new key completes");
    assert!(first.tag("Code").is_none(), "no error document in the 200");
    assert_eq!(second.status, 412, "an existing key refuses the complete");
    assert_eq!(second.tag("Code").as_deref(), Some("PreconditionFailed"));
    assert_eq!(raw_body(&h, &object).as_deref(), Some(&b"AAAA"[..]));

    // Over an object a plain PUT wrote, and as the engine's own request reads the answer.
    let taken = h.key(0, 2, 2);
    h.put(&taken, ts(1), b"x");
    let (c, ec) = open_with_one_part(&h, &taken, b"CCCC");
    let refused = h.send(
        h.builder()
            .complete_multipart_if(WallClock.now(), &taken, &c, &[ec], Precondition::KeyIsFree)
            .unwrap(),
    );
    obs_response(&h, "create-only Complete over a plain PUT", &refused);
    assert_eq!(
        judged(&taken, &refused),
        Some(StoreError::AlreadyExists { key: taken })
    );
    // The refused uploads stay open for the orphan list; nothing else is left behind.
    abort(&h, &key, &b);
    abort(&h, &taken, &c);
    assert_eq!(h.open_uploads(), 0);
}

// Check 3: racing create-only writers.

/// What one racing writer ended with.
#[derive(Debug, PartialEq, Eq)]
enum Raced {
    Won,
    Lost,
}

/// Tallies of what the racers saw, for the `OBS` line.
#[derive(Default)]
struct Tally {
    answers: Mutex<BTreeMap<String, u64>>,
    retried: AtomicU64,
}

impl Tally {
    fn note(&self, what: String) {
        let mut answers = self.answers.lock().unwrap();
        let count = answers.entry(what).or_default();
        *count = count.saturating_add(1);
    }
}

/// One writer's create-only PUT through the store, retried the way the engine would on a
/// retryable answer (a provider may report a write in flight as one).
fn put_new(store: &S3Store<Counting>, key: &FileKey, body: &[u8], tally: &Tally) -> Raced {
    for _attempt in 0..8 {
        let done = block_on(async {
            let mut upload = store.begin_upload(key, ts(1), WriteMode::New).await?;
            upload.write(body).await?;
            upload.commit().await
        });
        match done {
            Ok(_) => {
                tally.note("200".to_owned());
                return Raced::Won;
            }
            Err(StoreError::AlreadyExists { .. }) => {
                tally.note("already there".to_owned());
                return Raced::Lost;
            }
            Err(err) if class_of(&err) == Class::Retry => {
                tally.note(format!("retryable: {err}"));
                tally.retried.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => panic!("create-only PUT: {err}"),
        }
    }
    panic!("create-only PUT stayed retryable for 8 attempts");
}

/// `writers` threads race a create-only PUT of one key each round; exactly one wins and
/// the object holds the winner's bytes.
fn race_puts(h: &RealHarness, writers: u8, rounds: u64) {
    let tally = Tally::default();
    for round in 1..=rounds {
        let key = h.key(0, round, round);
        let barrier = Barrier::new(usize::from(writers));
        let stores: Vec<_> = (0..writers).map(|_| h.next_process()).collect();
        let outcomes: Vec<Raced> = std::thread::scope(|scope| {
            let handles: Vec<_> = stores
                .iter()
                .zip(0..writers)
                .map(|(store, who)| {
                    let (barrier, tally) = (&barrier, &tally);
                    scope.spawn(move || {
                        barrier.wait();
                        put_new(store, &key, &[b'w', who], tally)
                    })
                })
                .collect();
            handles.into_iter().map(|j| j.join().unwrap()).collect()
        });
        let winners: Vec<u8> = (0..writers)
            .zip(&outcomes)
            .filter(|(_, o)| **o == Raced::Won)
            .map(|(who, _)| who)
            .collect();
        assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");
        let want = [b'w', winners.first().copied().unwrap()];
        assert_eq!(
            h.read_back(&key, 2),
            want,
            "round {round}: the winner's bytes"
        );
    }
    eprintln!(
        "OBS race PUT writers={writers} rounds={rounds}: answers {:?}, retried {}",
        tally.answers.lock().unwrap(),
        tally.retried.load(Ordering::Relaxed)
    );
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn two_create_only_puts_racing_on_one_key_have_exactly_one_winner() {
    race_puts(&h(), 2, 100);
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn four_create_only_puts_racing_on_one_key_have_exactly_one_winner() {
    race_puts(&h(), 4, 25);
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn two_create_only_completes_racing_on_one_key_have_exactly_one_winner() {
    let h = h();
    let tally = Tally::default();
    let rounds = 50;
    for round in 1..=rounds {
        let key = h.key(0, round, round);
        let barrier = Barrier::new(2);
        let results: Vec<(Raced, UploadId)> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2_u8)
                .map(|who| {
                    let (h, barrier, tally) = (&h, &barrier, &tally);
                    scope.spawn(move || {
                        let (upload, etag) = open_with_one_part(h, &key, &[b'w', who]);
                        barrier.wait();
                        for _attempt in 0..8 {
                            let done = h.send(
                                h.builder()
                                    .complete_multipart_if(
                                        WallClock.now(),
                                        &key,
                                        &upload,
                                        std::slice::from_ref(&etag),
                                        Precondition::KeyIsFree,
                                    )
                                    .unwrap(),
                            );
                            let code = lsr_store_s3::S3Code::from_body(done.body());
                            tally.note(format!("{} {code:?}", done.status()));
                            match judged(&key, &done) {
                                None => return (Raced::Won, upload),
                                Some(StoreError::AlreadyExists { .. }) => {
                                    return (Raced::Lost, upload);
                                }
                                Some(err) if class_of(&err) == Class::Retry => {
                                    tally.retried.fetch_add(1, Ordering::Relaxed);
                                }
                                Some(err) => panic!("create-only Complete: {err}"),
                            }
                        }
                        panic!("create-only Complete stayed retryable for 8 attempts");
                    })
                })
                .collect();
            handles.into_iter().map(|j| j.join().unwrap()).collect()
        });
        let winners: Vec<usize> = results
            .iter()
            .enumerate()
            .filter(|(_, (o, _))| *o == Raced::Won)
            .map(|(who, _)| who)
            .collect();
        assert_eq!(winners.len(), 1, "round {round}: winners {winners:?}");
        let who = u8::try_from(winners.first().copied().unwrap()).unwrap();
        assert_eq!(h.read_back(&key, 2), [b'w', who], "round {round}");
        for (outcome, upload) in &results {
            if *outcome == Raced::Lost {
                abort(&h, &key, upload);
            }
        }
    }
    eprintln!(
        "OBS race Complete rounds={rounds}: answers {:?}, retried {}",
        tally.answers.lock().unwrap(),
        tally.retried.load(Ordering::Relaxed)
    );
    assert_eq!(h.open_uploads(), 0);
}

// Check 4: read-after-write and list-after-write from a second connection.

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_second_connection_lists_and_reads_each_of_1000_writes_at_once() {
    let h = h();
    let reader = h.next_process();
    let one = ReadLen::new(1).unwrap();
    let rounds = 1000_u64;
    let (mut not_listed, mut not_read) = (0_u64, 0_u64);
    let keys: Vec<FileKey> = (1..=rounds).map(|n| h.key(0, n, n)).collect();
    for (n, key) in (1..=rounds).zip(&keys) {
        h.write_all(key, ts(1), b"x", 64);
        let after = n.checked_sub(1).and_then(Txid::new).map(TxidRange::single);
        let listed = block_on(reader.list(Level::ZERO, after, PageLimit::ONE))
            .unwrap()
            .files
            .iter()
            .any(|f| f.key == *key);
        let read = match block_on(reader.read(key, 0, one)) {
            Ok(_) => true,
            Err(StoreError::NotFound { .. }) => false,
            Err(err) => panic!("read: {err}"),
        };
        not_listed = not_listed.saturating_add(u64::from(!listed));
        not_read = not_read.saturating_add(u64::from(!read));
    }
    block_on(h.store.delete(&keys)).unwrap();
    eprintln!("OBS consistency {rounds} writes: not listed {not_listed}, not read {not_read}");
    assert_eq!(
        (not_listed, not_read),
        (0, 0),
        "a write was not seen at once"
    );
}

// Check 5: a long listing and a delete.

const LISTED: u64 = 2500;

/// Every key of the level through the store's own paging, at `limit` a page.
fn page_through(h: &RealHarness, limit: u16) -> Vec<FileKey> {
    let (mut keys, mut after) = (Vec::new(), None);
    loop {
        let page = block_on(
            h.store
                .list(Level::ZERO, after, PageLimit::new(limit).unwrap()),
        )
        .unwrap();
        keys.extend(page.files.iter().map(|f| f.key));
        match page.next {
            Some(cursor) => after = Some(cursor),
            None => return keys,
        }
    }
}

/// Every key under the prefix through the provider's own continuation tokens, 1000 a page.
fn raw_pages(h: &RealHarness) -> Vec<String> {
    let prefix = format!("{}/", h.prefix.as_str());
    let (mut keys, mut token) = (Vec::new(), None::<String>);
    loop {
        let mut query = vec![
            ("list-type", "2"),
            ("prefix", prefix.as_str()),
            ("max-keys", "1000"),
        ];
        if let Some(token) = &token {
            query.push(("continuation-token", token.as_str()));
        }
        let raw = h.raw("GET", None, &query, &[], b"");
        assert_eq!(raw.status, 200, "{}", h.dump(&raw));
        keys.extend(
            raw.text()
                .split("<Key>")
                .skip(1)
                .filter_map(|rest| rest.split_once("</Key>").map(|(k, _)| k.to_owned())),
        );
        match (
            raw.tag("IsTruncated").as_deref(),
            raw.tag("NextContinuationToken"),
        ) {
            (Some("true"), Some(next)) => token = Some(next),
            _ => return keys,
        }
    }
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_listing_of_2500_keys_pages_to_the_end_with_no_repeat_and_no_gap_then_a_delete_empties_it() {
    let h = h();
    let want: Vec<FileKey> = (1..=LISTED).map(|n| h.key(0, n, n)).collect();
    for key in &want {
        h.put(key, ts(1), b"x");
    }
    for limit in [1000, 333] {
        assert!(
            page_through(&h, limit) == want,
            "store paging at {limit} a page"
        );
    }
    let raw = raw_pages(&h);
    let mut sorted = raw.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        raw.len(),
        usize::try_from(LISTED).unwrap(),
        "no gap, no repeat"
    );
    assert!(
        raw == sorted,
        "the provider's own tokens: ascending and unique"
    );
    for chunk in want.chunks(1000) {
        block_on(h.store.delete(chunk)).unwrap();
    }
    let (after_store, after_raw) = (page_through(&h, 1000).len(), raw_pages(&h).len());
    eprintln!(
        "OBS delete-then-list: listed after the delete {after_store} (store), {after_raw} (raw)"
    );
    assert_eq!(
        (after_store, after_raw),
        (0, 0),
        "a delete shows in the next listing"
    );
}

/// The names under one prefix, in the order the provider listed them.
fn raw_names(h: &RealHarness, prefix: &str) -> Vec<String> {
    let raw = h.raw(
        "GET",
        None,
        &[("list-type", "2"), ("prefix", prefix)],
        &[],
        b"",
    );
    assert_eq!(raw.status, 200, "{}", h.dump(&raw));
    raw.text()
        .split("<Key>")
        .skip(1)
        .filter_map(|rest| rest.split_once("</Key>"))
        .map(|(key, _)| key.strip_prefix(prefix).unwrap_or(key).to_owned())
        .collect()
}

/// Records how the provider orders names that extend one another (`X`, `X.ltx`, `X.ltx.tmp`),
/// which only stray names produce, since the engine's own names have one fixed width. The
/// store refuses a page whose keys are out of bytewise order (never a shorter listing), so a
/// provider that orders them otherwise fails a listing that holds such a stray name. Asserts
/// only what the engine relies on: its own fixed-width names come back ascending.
#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn listing_order_of_names_that_extend_one_another_is_recorded() {
    let h = h();
    let prefix = format!("{}/order/", h.prefix.as_str());
    let engine = [
        "0000000000000001-0000000000000001.ltx",
        "0000000000000002-0000000000000003.ltx",
        "000000000000000a-000000000000000b.ltx",
    ];
    let strays = [
        "0",
        "00",
        "0000000000000001-0000000000000001.ltx.tmp",
        "0000000000000001-0000000000000001.ltx.bak",
        "README",
        "a",
        "a1",
    ];
    for name in engine.iter().chain(&strays) {
        h.raw_put(&format!("{prefix}{name}"), b"x", &[]);
    }
    let listed = raw_names(&h, &prefix);
    let mut bytewise = listed.clone();
    bytewise.sort();
    eprintln!("OBS listing order {listed:?}");
    eprintln!("OBS listing order is bytewise: {}", listed == bytewise);
    let ours: Vec<&str> = listed
        .iter()
        .map(String::as_str)
        .filter(|name| engine.contains(name))
        .collect();
    assert_eq!(ours, engine, "the engine's own names ascend");
}

// Check 6: error classification for the faults that can be produced for real.

/// A store over the run's bucket with other credentials or another bucket.
fn store_with(h: &RealHarness, access: &str, secret: &str, bucket: &str) -> S3Store<UreqTransport> {
    let config = other_config(h, bucket);
    let credentials = S3Credentials::new(access.to_owned(), SecretString::new(secret.to_owned()));
    let transport = UreqTransport::new(config.deadline()).expect("a TLS provider");
    S3Store::with_transport(config, credentials, transport, WallClock).unwrap()
}

fn other_config(h: &RealHarness, bucket: &str) -> S3Config {
    S3Config::new(
        Endpoint::new(&h.server.endpoint).unwrap(),
        BucketName::new(bucket).unwrap(),
        Prefix::new(h.prefix.as_str()).unwrap(),
    )
    .with_region(Region::new(&h.server.region).unwrap())
}

/// Sends one read as another identity or to another bucket and records the raw answer.
fn get_as(h: &RealHarness, what: &str, access: &str, secret: &str, bucket: &str) {
    let config = other_config(h, bucket);
    let credentials = S3Credentials::new(access.to_owned(), SecretString::new(secret.to_owned()));
    let answer = UreqTransport::new(config.deadline())
        .expect("a TLS provider")
        .send(
            RequestBuilder::new(&config, &credentials)
                .unwrap()
                .get_range(
                    WallClock.now(),
                    &h.key(0, 1, 1),
                    0,
                    ReadLen::new(16).unwrap(),
                )
                .unwrap(),
        )
        .unwrap();
    obs_response(h, what, &answer);
}

/// What a read and a list of the store answer, as errors.
fn read_and_list_errors(store: &S3Store<UreqTransport>, key: &FileKey) -> (StoreError, StoreError) {
    let read = block_on(store.read(key, 0, ReadLen::new(16).unwrap())).unwrap_err();
    let list = block_on(store.list(Level::ZERO, None, PageLimit::ONE)).unwrap_err();
    eprintln!("OBS classified: read {read:?}, list {list:?}");
    (read, list)
}

fn denied(why: Denied) -> impl Fn(&StoreError) -> bool {
    move |err| matches!(err, StoreError::Denied { why: got, .. } if *got == why)
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_wrong_secret_key_is_denied_as_credentials() {
    let h = h();
    get_as(
        &h,
        "GET with a wrong secret key",
        &h.server.access,
        "wrong-secret-0123456789abcdef",
        &h.server.bucket,
    );
    let store = store_with(
        &h,
        &h.server.access,
        "wrong-secret-0123456789abcdef",
        &h.server.bucket,
    );
    let (read, list) = read_and_list_errors(&store, &h.key(0, 1, 1));
    assert!(denied(Denied::Credentials)(&read), "{read:?}");
    assert!(denied(Denied::Credentials)(&list), "{list:?}");
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn an_unknown_access_key_id_is_denied_as_credentials() {
    let h = h();
    // The real id with its last character changed: the same length and shape, so a provider
    // that checks the shape (R2 refuses a short or odd id with a 400) reaches the lookup.
    let mut unknown = h.server.access.clone();
    let last = unknown.pop().unwrap();
    unknown.push(if last == '0' { '1' } else { '0' });
    let secret = "wrong-secret-0123456789abcdef";
    get_as(
        &h,
        "GET with an unknown access key id",
        &unknown,
        secret,
        &h.server.bucket,
    );
    let store = store_with(&h, &unknown, secret, &h.server.bucket);
    let (read, list) = read_and_list_errors(&store, &h.key(0, 1, 1));
    assert!(denied(Denied::Credentials)(&read), "{read:?}");
    assert!(denied(Denied::Credentials)(&list), "{list:?}");
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_missing_bucket_is_denied_as_bucket_missing() {
    let h = h();
    let missing = format!("lsr-no-such-bucket-{}", WallClock.now().unix_millis());
    get_as(
        &h,
        "GET in a missing bucket",
        &h.server.access,
        &h.server.secret,
        &missing,
    );
    let store = store_with(&h, &h.server.access, &h.server.secret, &missing);
    let (read, list) = read_and_list_errors(&store, &h.key(0, 1, 1));
    // A HEAD has no body, so the code is absent: recorded, since the store reads a bare 404 as a
    // missing object.
    let config = other_config(&h, &missing);
    let credentials = S3Credentials::new(
        h.server.access.clone(),
        SecretString::new(h.server.secret.clone()),
    );
    let head = UreqTransport::new(config.deadline())
        .expect("a TLS provider")
        .send(
            RequestBuilder::new(&config, &credentials)
                .unwrap()
                .head(WallClock.now(), &h.key(0, 1, 1))
                .unwrap(),
        )
        .unwrap();
    eprintln!(
        "OBS HEAD of a key in a missing bucket: status {} read as {:?}",
        head.status(),
        classify(StoreOp::Read, Some(&h.key(0, 1, 1)), None, &head)
    );
    assert!(denied(Denied::BucketMissing)(&read), "{read:?}");
    assert!(denied(Denied::BucketMissing)(&list), "{list:?}");
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_missing_object_is_not_found_and_deleting_it_is_not_an_error() {
    let h = h();
    let key = h.key(0, 1, 1);
    let read = block_on(h.store.read(&key, 0, ReadLen::new(16).unwrap()));
    assert_eq!(read, Err(StoreError::NotFound { key }));
    let head = h.send(h.builder().head(WallClock.now(), &key).unwrap());
    obs_response(&h, "HEAD of a missing object", &head);
    assert_eq!(
        classify(StoreOp::Read, Some(&key), None, &head),
        Err(StoreError::NotFound { key })
    );
    assert_eq!(block_on(h.store.delete(&[key])), Ok(()));
}

/// A request signed for `signed_at` instead of now, sent as is.
fn read_signed_at(h: &RealHarness, signed_at: i64) -> StoreError {
    let key = h.key(0, 1, 1);
    let request = h
        .builder()
        .get_range(ts(signed_at), &key, 0, ReadLen::new(16).unwrap())
        .unwrap();
    let response = h.send(request);
    obs_response(
        h,
        &format!(
            "GET signed at now{signed_at:+}",
            signed_at = signed_at.saturating_sub(WallClock.now().unix_millis())
        ),
        &response,
    );
    classify(StoreOp::Read, Some(&key), None, &response).unwrap_err()
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_signature_made_an_hour_ago_or_an_hour_ahead_is_denied_and_never_retried() {
    let h = h();
    let now = WallClock.now().unix_millis();
    for signed_at in [now.saturating_sub(3_600_000), now.saturating_add(3_600_000)] {
        let err = read_signed_at(&h, signed_at);
        eprintln!("OBS classified: {err:?}");
        assert!(denied(Denied::ClockSkew)(&err), "{err:?}");
        assert_eq!(class_of(&err), Class::Permanent);
    }
}

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn a_wrong_signature_is_denied_as_credentials_and_a_malformed_one_is_never_retried() {
    let h = h();
    let key = h.key(0, 1, 1);
    let read = |signature: &str| {
        let request: HttpRequest = h
            .builder()
            .get_range(WallClock.now(), &key, 0, ReadLen::new(16).unwrap())
            .unwrap()
            .tamper_query("X-Amz-Signature", signature);
        let response = h.send(request);
        obs_response(&h, &format!("GET with signature {signature:?}"), &response);
        classify(StoreOp::Read, Some(&key), None, &response).unwrap_err()
    };
    let wrong = read(&"0".repeat(64));
    assert!(denied(Denied::Credentials)(&wrong), "{wrong:?}");
    let malformed = read("not-a-signature");
    eprintln!("OBS classified: malformed signature {malformed:?}");
    assert_eq!(class_of(&malformed), Class::Permanent, "{malformed:?}");
}

// Check 7: an abandoned multipart upload is visible and can be cleaned up.

#[test]
#[ignore = "needs a real provider (just test-provider)"]
fn abandoned_multipart_uploads_are_listed_by_prefix_and_swept_by_the_next_boot() {
    let h = h();
    let (k1, k2) = (h.key(0, 1, 1), h.key(0, 2, 2));
    let abandoned = [
        open_with_one_part(&h, &k1, b"one"),
        open_with_one_part(&h, &k1, b"two"),
        open_with_one_part(&h, &k2, b"three"),
    ];
    let transport = UreqTransport::new(h.config.deadline()).expect("a TLS provider");
    let prefix = format!("{}/", h.prefix.as_str());
    let listed =
        |h: &RealHarness| open_uploads(&transport, &h.builder(), &WallClock, &prefix).unwrap();
    let open = listed(&h);
    eprintln!(
        "OBS abandoned: {} open uploads listed under the prefix",
        open.len()
    );
    assert_eq!(
        open.len(),
        abandoned.len(),
        "ListMultipartUploads with the prefix"
    );
    assert!(open.iter().all(|u| u.key.starts_with(&prefix)));
    assert!(
        h.list_all(0, 10).files.is_empty(),
        "invisible to a listing of objects"
    );
    // One is aborted by hand, as an operator would; the next process's boot sweeps the rest.
    let first = open.first().unwrap();
    abort_upload(&transport, &h.builder(), &WallClock, first).unwrap();
    assert_eq!(listed(&h).len(), abandoned.len().saturating_sub(1));
    let next = h.next_process();
    assert_eq!(block_on(next.remove_scratch()), Ok(2));
    assert!(listed(&h).is_empty(), "nothing left open");
    assert!(h.list_all(0, 10).files.is_empty());
}
