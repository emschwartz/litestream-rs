# Phase 0 spikes

| Spike | Status | Answer |
|---|---|---|
| Prior art: rustyriver | answered | Not usable as a code source: the repository states no licence (GitHub reports none). Its method is adopted: pinned upstream, golden fixtures, differential tests. |
| `rusty-s3` signing of `x-amz-meta-*` headers; multipart against Tigris | open | Needs bucket credentials. Folded into the `store-s3` task (docs/plans/phase-1-reading.md, task 8); until then `store-s3` is tested against a local fake only. |
| sqlx fork: raw handle in `after_connect` | deferred | A host-integration question (PLAN.md section 6), not needed by the library. |

## Task 8s: rusty-s3 against MinIO

Server: Chainguard's build of MinIO, `cgr.dev/chainguard/minio@sha256:e9cf6b99c226e9539ed0f14b6f00e37b07da153c7ba1f4f8709c807574b471ec` (`minio version RELEASE.2026-11-16T00-11-40Z`, commit ee9fee0715118245ff4d9149c102608653748196, go1.27.4 linux/arm64), pulled as `cgr.dev/chainguard/minio:latest` on 2026-11-20. MinIO no longer publishes images at quay.io or docker.io, so this is a third-party build. **If the digest stops resolving, 8c re-pins and these questions are re-run.** Entrypoint `/usr/bin/minio`, run as `server /tmp/data` with the public defaults `minioadmin`/`minioadmin`, path-style, region `us-east-1`, plain HTTP on 127.0.0.1. Every answer below is "verified on MinIO cgr.dev/chainguard/minio@sha256:e9cf6b99c226e9539ed0f14b6f00e37b07da153c7ba1f4f8709c807574b471ec" unless marked "open: needs Tigris (8t)".

Client: `rusty-s3` 0.12.0 (BSD-2-Clause), `ureq` 3.5.2, `http_status_as_error(false)`. Dependency count for those two alone (`cargo tree -e normal --prefix none | sort -u | wc -l`): 100 lines, 99 packages plus the root. Only licence outside the `deny.toml` allowlist: `webpki-roots` (CDLA-Permissive-2.0), already ruled in by F4 (`ring` is `Apache-2.0 AND ISC`, both allowed).

**Plan correction, features.** The plan says features `rustcrypto`, `xml`. `DeleteObjects`, `ObjectIdentifier` and `DeleteObjectsResponse` are behind rusty-s3's `full` feature (which also pulls `md-5`, `base64`, `serde`, `serde_json`), so 8b needs `rustcrypto` + `full`, not `xml` alone. The 99 packages above are measured with `full`; with `xml` alone it was 92.

1. **Signed metadata header: accepted, byte for byte.** `headers_mut().insert("x-amz-meta-litestream-timestamp", "2026-11-20T15:04:05.123456789Z")` puts `host;x-amz-meta-litestream-timestamp` in `X-Amz-SignedHeaders`; PUT with the header returned 200; HEAD returned the identical value (nanosecond fraction kept). MinIO enforces the signing rule both ways: the header sent but not in the signature, and the header signed but not sent, each returned 400 `AccessDenied` "There were headers present in the request which were not signed". Departure from Go recorded: rusty-s3 signs by query string with `UNSIGNED-PAYLOAD` (Go signs the payload), so a body altered in flight is caught only by Content-MD5 (F7).
2. **Wrong Content-MD5: 400 `BadDigest`** ("The Content-Md5 you specified did not match what we received."). A malformed value (not base64) is 400 `InvalidDigest`. The correct value is 200. The header must be in `headers_mut` (lowercase `content-md5`) so it is signed and sent. 8b maps BadDigest to a checksum cause and InvalidDigest to a refusal.
3. **Multipart: matches.** Create 200 with an UploadId. Each part's ETag header was the quoted lowercase hex MD5 of the part bytes (5 MiB part and a 9-byte tail, both matched). Complete returned 200, header and body ETag `"528dc964193f0e986d6713e3297a1f51-2"`, equal to the MD5 of the concatenated part MD5s plus `-2`. The body's ETag is XML-escaped (`&#34;` quotes); `CompleteMultipartUploadResponse::etag` is what 8c should read. HEAD size 5242889.
4. **Last commit wins.** Two PUTs to one key: the second survives. Two multipart uploads to one key, initiated A then B, completed B then A: **A survives** (the last completed, not the last initiated). Both completes returned 200. Not representative of providers that order by initiate time: open: needs Tigris (8t).
5. **Listing: keys round-trip.** `rusty-s3` sends `encoding-type=url`; MinIO URL-encodes keys in the XML (`q5/foo+bar/a%2Bb.ltx`, `q5/pct%2520lit`) and `ListObjectsV2::parse_response` decodes them: `"q5/foo bar/a+b.ltx"`, `"q5/plus+key"`, `"q5/pct%20lit"` and `"q5/ünï"` all came back exactly. `start_after` in the response is decoded too. `start-after` of `q5/pct%20lit` with `max-keys=2` returned `q5/plain`, `q5/plus+key`, `IsTruncated=true` and a `NextContinuationToken`, so seek plus paging works (F8). Order is by raw key bytes.
6. **Ranges.** `bytes=100-200` on a 10-byte object: 416 `InvalidRange` (body carries `ActualObjectSize` 10). `bytes=10-20` (start equals length): 416. `bytes=5-100`: **206**, clipped, `Content-Range: bytes 5-9/10`, 5 bytes (a range that straddles the end is not an error; the reader must check the length it got). `bytes=0-9`: 206 with 10 bytes. Zero-byte object: `bytes=0-0` and `bytes=0-` both 416 `InvalidRange`; a plain GET is 200 with an empty body.
7. **DeleteObjects quiet with a missing key: no `<Error>`.** Content-MD5 from `body_with_md5` accepted. One existing and one missing key, quiet: 200 with an empty `<DeleteResult>`; parsed `deleted` 0, `errors` 0.

Not covered here (open: needs Tigris, 8t): signature acceptance against Tigris's own region and host style, provider differences in question 4, and lowercase metadata header normalisation on a provider other than MinIO.

## Task 8t: the store against Tigris

Run on 2026-11-20 from one laptop, bucket `litestream-rs-test`, one run prefix, about 1,500 requests, under 100 MiB stored at any moment. Reproduce with `just test-tigris` (credentials file outside the repo; the tests are `#[ignore]`d and live in `crates/store-s3/tests/tigris.rs`, sharing 29 cases with the MinIO suite through `tests/common/real.rs`). Everything written was deleted and the bucket listed empty afterwards: no objects, no incomplete uploads, no lifecycle rule (item 8 never got that far).

| Item | Result | Evidence |
|---|---|---|
| 1 presigned UNSIGNED-PAYLOAD with signed meta and `x-tigris-consistent` | pass | PUT 200; a signed meta header that is not sent gives 403 `SignatureDoesNotMatch`; an extra header sent but not signed is accepted (200) |
| 2 Content-MD5 checked | pass (differs from the worry in the plan) | wrong digest on PUT and on UploadPart: 400 `BadDigest`, object absent afterwards (404) |
| 3 ETag is the hex MD5 | pass | single PUT `etag="5d41402a..."` equals MD5("hello"); a part's ETag the same; Complete returns `"<hex>-1"` |
| 4 listing | partial | 1,100 objects: a page of 1000 sets a continuation and costs 2 list requests, 0 HEADs (`listing_makes_no_head_requests`); paging, cursor and foreign-name cases pass. `encoding-type=url` with special-character keys is open: the raw listing probe got 403 `SignatureDoesNotMatch` from our test signer on a `prefix` query (the store's own listing signs fine), so keys with `+`, `&`, spaces were not exercised |
| 5 range past the end | pass | 416 `InvalidRange` for offset 10 and 100 of a 10-byte object and for offset 0 of an empty one; `is_range_past_end` is true; offset 9 is 206 `Content-Range: bytes 9-9/10` |
| 6 DeleteObjects | pass | quiet mode with Content-MD5: 200; one missing key of four is not reported as an error |
| 7 two uploads to one key | pass | the last completed wins, not the last initiated: complete A then B leaves B, complete B then A leaves A |
| 8 lifecycle abort-incomplete | blocked | the key gets 403 `AccessDenied` on both GetBucketLifecycleConfiguration and PutBucketLifecycleConfiguration; no rule written, no upload left for a later check |
| 9 HEAD latency from this laptop | measured | 1,000 sequential HEADs through our client: p50 84.7 ms, p95 175.8 ms, total 119.0 s. One home connection to `t3.storage.dev`, not a production vantage point |
| 10 production-shaped restore | open | not run (time); needs a generated Go v0.5.18 replica of a few hundred files, levels 0 to 3 and 9, copied up and restored by Rust and Go |
| 11 load shedding | open | not provoked. Statuses seen across the run: 200, 206, 400, 403, 404, 416; no 429, 503 or `Retry-After`. Docs not checked |

Provider questions:

- Q1 (what the plan left to the credentialed run for presigning): answered by item 1.
- Q2, metadata on a 206: yes. A ranged GET answers 206 with `x-amz-meta-litestream-timestamp` (and the ETag), so `read_with_stamp` needs no HEAD.
- Q3, multipart LastModified: the completion time, not the initiation. Initiated at server clock T, completed at T+3 s: the listing says T+3 s.
- Q4, metadata in the listing: open (see item 4; the probe was not valid).
- Q5, CopyObject: a copy keeps the user metadata under the default directive; `x-amz-metadata-directive: REPLACE` without metadata drops it.

Other findings, each pinned by a test:

1. Tigris starts multipart at 16 MiB in our store (`Provider::Tigris`). The shared cases use `part(h)` so a body is large enough to go multipart there; at 5 MiB the same cases pass by sending one PUT.
2. A multipart upload that has no part yet is not created at all, so a dropped upload below one part leaves nothing to abort. ListMultipartUploads does list an open upload (200, no `prefix` accepted from our raw signer).
3. Aborting an unknown upload is 404 `NoSuchUpload` (MinIO says 204). `classify(StoreOp::Upload, ..)` on that response is `Refused { status: 404 }`, not `Ok`; the store's abort path handles the 404 itself. Pinned by `abort_of_an_unknown_upload_is_not_a_failure`, which accepts 204 or 404.
4. Error bodies are S3 XML (`<Code>`); responses carry `server: nginx` and `x-amz-request-id`.

Left for the next run: items 4 (encoding), 8, 10, 11 and Q4; the raw listing helper in `real.rs` needs query-value encoding fixed before they are meaningful.
