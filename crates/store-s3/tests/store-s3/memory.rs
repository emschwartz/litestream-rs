//! Memory gates for the S3 store through the real HTTP client: the live heap of a multipart
//! upload and of a restore, measured while the transport that ships does the work.
//!
//! A test server on a loopback socket stands in for S3 (the in-memory `FakeS3` behind a
//! real listener, one thread per connection), so the client's own buffers, the request body
//! it builds and the response body it reads are all counted. The server's threads are left
//! out of the count (`exclude_thread`): what the far end holds is not the code under test's.
//! The counting is process-wide (`process_peak_of`) because the batch workers allocate on
//! threads of their own; each test is one process under nextest.
//!
//! The laws, stated from the configuration:
//!
//! - An upload holds at most `concurrency` parts (the batch being sent), whatever its size:
//!   the part buffers are the request bodies, so nothing copies them.
//! - A restore holds one read window: `READ_CONCURRENCY` responses of the 4 MiB the engine
//!   asks for, whatever the database's size, so each response holds its length and not the
//!   capacity a read grown from empty ends with.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "bounds arithmetic on small counts in test code"
)]

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{DatabaseSink, FakeClock, FileKey, ObjectStore, RestoreTarget, Upload, restore};
use lsr_ltx::{
    DbChecksum, DbPages, Encoder, Header, HeaderFields, PageNo, PageRef, PageSize, Timestamp, Txid,
    TxidRange,
};
use lsr_store_s3::{
    BucketName, Endpoint, FakeS3, HttpRequest, Method, PartSize, Prefix, READ_CONCURRENCY,
    S3Config, S3Store, Transport, object_key,
};
use url::Url;

use crate::common::{config, credentials, now};

#[path = "../../../core/tests/core/heap.rs"]
#[expect(
    dead_code,
    reason = "the file is shared with the other memory gates; this binary measures a process"
)]
mod heap;

use heap::{Counting, exclude_thread, process_peak_of};

#[global_allocator]
static HEAP: Counting = Counting;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Bytes one restore read asks of the store: core's `restore::CHUNK_BYTES`, restated so a
/// change there shows here.
const RESTORE_CHUNK: usize = 4 * MIB;

/// What a store may hold beyond the buffers the laws name: the client's per-request
/// buffers and the decoder's page, the multipart bookkeeping and the result.
const MARGIN: usize = 4 * MIB;

fn txid(n: u64) -> Txid {
    Txid::new(n).unwrap()
}

/// The status lines the server writes; the client reads the number.
const fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        206 => "Partial Content",
        _ => "Status",
    }
}

/// The header names the fake sees are `'static`: the server leaks the few it reads.
fn leak(name: &str) -> &'static str {
    Box::leak(name.to_owned().into_boxed_str())
}

/// Reads one request off `stream` and answers it from `fake`, then closes.
fn serve_one(mut stream: TcpStream, port: u16, fake: &FakeS3) -> io::Result<()> {
    let mut head = Vec::new();
    let mut buf = vec![0_u8; 64 * KIB];
    let end = loop {
        if let Some(at) = head.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        let read = stream.read(&mut buf)?;
        if read == 0 {
            return Ok(());
        }
        head.extend_from_slice(&buf[..read]);
    };
    let text = String::from_utf8_lossy(&head[..end]).into_owned();
    let mut lines = text.split("\r\n");
    let mut request_line = lines.next().unwrap_or("").split(' ');
    let method = match request_line.next().unwrap_or("") {
        "GET" => Method::Get,
        "HEAD" => Method::Head,
        "PUT" => Method::Put,
        "POST" => Method::Post,
        "DELETE" => Method::Delete,
        _ => return Err(io::Error::other("a method the store never sends")),
    };
    let target = request_line.next().unwrap_or("/");
    let url = Url::parse(&format!("http://127.0.0.1:{port}{target}")).map_err(io::Error::other)?;
    let signed: BTreeSet<String> = url
        .query_pairs()
        .find(|(name, _)| name == "X-Amz-SignedHeaders")
        .map(|(_, value)| value.split(';').map(str::to_owned).collect())
        .unwrap_or_default();
    let mut headers = Vec::new();
    let mut declared = 0_usize;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim().to_ascii_lowercase(), value.trim().to_owned());
        if name == "content-length" {
            declared = value.parse().unwrap_or(0);
        }
        if name != "host" && signed.contains(&name) {
            headers.push((leak(&name), value));
        }
    }
    let mut body = head.split_off(end);
    body.reserve_exact(declared.saturating_sub(body.len()));
    while body.len() < declared {
        let read = stream.read(&mut buf)?;
        if read == 0 {
            return Ok(());
        }
        body.extend_from_slice(&buf[..read]);
    }
    let reply = fake
        .send(HttpRequest::from_wire(method, url, headers, body))
        .map_err(io::Error::other)?;
    let mut out = format!("HTTP/1.1 {} {}\r\n", reply.status(), reason(reply.status()));
    for (name, value) in reply.headers() {
        if !name.eq_ignore_ascii_case("content-length") {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
    }
    let length = match method {
        Method::Head => reply
            .header("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        Method::Get | Method::Put | Method::Post | Method::Delete => reply.body().len(),
    };
    out.push_str(&format!(
        "content-length: {length}\r\nconnection: close\r\n\r\n"
    ));
    stream.write_all(out.as_bytes())?;
    if method != Method::Head {
        stream.write_all(reply.body())?;
    }
    stream.flush()
}

/// Serves `fake` on a loopback port until the process ends. Returns the port.
fn serve(fake: &Arc<FakeS3>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let fake = Arc::clone(fake);
    thread::spawn(move || {
        exclude_thread();
        for stream in listener.incoming().flatten() {
            let fake = Arc::clone(&fake);
            thread::spawn(move || {
                exclude_thread();
                // A client that hangs up early has nothing left to answer.
                if let Err(_hung_up) = serve_one(stream, port, &fake) {}
            });
        }
    });
    port
}

/// A store over the real client, pointed at the test server.
fn store_on(port: u16, part: usize, concurrency: usize) -> S3Store<lsr_store_s3::UreqTransport> {
    let config = S3Config::new(
        Endpoint::new(&format!("http://127.0.0.1:{port}")).unwrap(),
        BucketName::new("bucket").unwrap(),
        Prefix::new("db").unwrap(),
    )
    .with_part_size(PartSize::new(u64::try_from(part).unwrap()).unwrap())
    .with_concurrency(NonZeroUsize::new(concurrency).unwrap());
    S3Store::new(config, credentials()).unwrap()
}

fn fake() -> Arc<FakeS3> {
    Arc::new(FakeS3::new(&config(), FakeClock::new(now())))
}

/// Uploads `total` bytes in mebibyte writes and checks the peak against the law for a
/// store of this part size and concurrency.
fn assert_upload_holds_one_batch(part: usize, concurrency: usize, total: usize) {
    let fake = fake();
    let store = store_on(serve(&fake), part, concurrency);
    let key = FileKey::snapshot(txid(1));
    let written = vec![0xA5_u8; MIB];
    let (done, peak) = process_peak_of(|| {
        block_on(async {
            let mut upload = store
                .begin_upload(
                    &key,
                    Timestamp::from_unix_millis(1_700_000_000_000),
                    WriteMode::New,
                )
                .await
                .unwrap();
            for _ in 0..total / MIB {
                upload.write(&written).await.unwrap();
            }
            upload.commit().await
        })
    });
    assert_eq!(done.unwrap().size, u64::try_from(total).unwrap());
    assert_eq!(
        fake.size_of(&object_key(config().prefix(), &key)),
        Some(total)
    );
    let bound = concurrency * part + MARGIN;
    eprintln!(
        "upload part {} MiB x {concurrency}: {} KiB (bound {} KiB)",
        part / MIB,
        peak.extra >> 10,
        bound >> 10
    );
    assert!(
        peak.extra <= bound,
        "upload held {} bytes, over {bound} ({concurrency} parts x {part} + {MARGIN})",
        peak.extra
    );
    assert!(
        peak.extra >= (concurrency - 1) * part,
        "the gate saw {} bytes: it cannot see the part buffers",
        peak.extra
    );
}

#[test]
fn an_upload_through_the_real_client_holds_five_parts() {
    assert_upload_holds_one_batch(PartSize::MIN.get().try_into().unwrap(), 5, 56 * MIB);
}

#[test]
fn the_bound_follows_part_size_and_concurrency() {
    assert_upload_holds_one_batch(8 * MIB, 3, 56 * MIB);
}

/// A destination that keeps a page count and nothing else.
struct Discard {
    pages: Arc<AtomicU64>,
}

impl DatabaseSink for Discard {
    fn write_page(&mut self, _page: PageRef<'_>) -> io::Result<()> {
        self.pages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn truncate(&mut self, _pages: DbPages, _page_size: PageSize) -> io::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn checksum(&mut self, _pages: DbPages, _page_size: PageSize) -> io::Result<DbChecksum> {
        Err(io::Error::other(
            "untracked files never ask for the end state",
        ))
    }

    type PublishError = Infallible;

    fn publish(self) -> Result<(), Infallible> {
        Ok(())
    }
}

/// A snapshot of `pages` incompressible 4 KiB pages as the encoder writes it.
fn snapshot_bytes(pages: u32) -> Vec<u8> {
    let size = PageSize::new(4096).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit: pages,
        txids: TxidRange::new(txid(1), txid(1)).unwrap(),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut page = vec![0_u8; size.bytes()];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for pgno in 1..=pages {
        for word in page.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            word.copy_from_slice(&state.to_le_bytes()[..word.len()]);
        }
        encoder
            .write_page(PageNo::new(pgno, size).unwrap(), &page)
            .unwrap();
    }
    encoder.finish(None).unwrap().0
}

/// Restores a snapshot of `mib` MiB through the real client and returns the heap peak.
fn restore_peak(mib: usize) -> usize {
    let fake = fake();
    let bytes = snapshot_bytes(u32::try_from(mib * 256).unwrap());
    let key = FileKey::snapshot(txid(1));
    fake.put_raw(
        &object_key(config().prefix(), &key),
        bytes,
        &[("x-amz-meta-litestream-timestamp", &now().to_string())],
    );
    let store = store_on(serve(&fake), PartSize::MIN.get().try_into().unwrap(), 5);
    let pages = Arc::new(AtomicU64::new(0));
    let sink = Discard {
        pages: Arc::clone(&pages),
    };
    let (restored, peak) =
        process_peak_of(|| block_on(restore(&store, RestoreTarget::Latest, sink)));
    restored.unwrap();
    assert_eq!(
        pages.load(Ordering::Relaxed),
        u64::try_from(mib * 256).unwrap()
    );
    peak.extra
}

/// The most a restore may hold: `READ_CONCURRENCY` responses of the chunk it asks for in
/// flight, the one more the store copies a body into, and the margin.
fn restore_bound() -> usize {
    (READ_CONCURRENCY.get() + 1) * RESTORE_CHUNK + MARGIN
}

/// A restore through the real client holds its read window, and no more as the database
/// grows from 72 MiB to 144 MiB.
#[test]
fn a_restore_through_the_real_client_holds_one_read_window() {
    let window = READ_CONCURRENCY.get() * RESTORE_CHUNK;
    assert!(
        72 * MIB > window,
        "the database must be larger than the window"
    );
    let few = restore_peak(72);
    eprintln!(
        "restore 72 MiB: {} KiB (bound {} KiB)",
        few >> 10,
        restore_bound() >> 10
    );
    assert!(
        few <= restore_bound(),
        "restore held {few} bytes, over {}",
        restore_bound()
    );
    assert!(
        few >= window - RESTORE_CHUNK,
        "the gate saw {few} bytes: it cannot see the window"
    );
}
