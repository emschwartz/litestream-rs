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
//! - An upload holds at most `concurrency + 1` parts (the ones being sent and the one filling),
//!   whatever its size and with no other cap: the part buffers are the request bodies, so
//!   nothing copies them.
//! - An upload allocates each part's buffer once, at the part's size: a buffer regrown by doubling
//!   leaves a ladder of blocks of other sizes that macOS keeps mapped, so a long upload at a large
//!   part size held about its own length (measured: 6 GB for four uploads of 1.5 GiB at 100 MiB
//!   parts and a concurrency of 10), whatever the live count said.
//! - An upload written from a file it can read again (`Upload::write_from`) holds no part:
//!   a digest and a few tallies for each, and one 256 KiB read for each request being sent,
//!   whatever the part size.
//! - A restore holds one chunk: each file is one GET whose body is read from the connection
//!   4 MiB at a time as the decoder takes it, whatever the database's size.

#![expect(
    clippy::arithmetic_side_effects,
    reason = "bounds arithmetic on small counts in test code"
)]

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{
    Again, BoxFuture, CompactionInputs, DatabaseSink, FakeClock, FileKey, Level, MAX_INPUTS,
    ObjectStore, PageLimit, Reread, RestoreTarget, Seen, StoreError, StoreOp, Upload, compact,
    compact_staged, restore,
};
use lsr_ltx::{
    DbChecksum, DbPages, Encoder, Header, HeaderFields, PageNo, PageRef, PageSize, Timestamp, Txid,
    TxidRange,
};
use lsr_store_fs::FsStore;
use lsr_store_s3::{
    BucketName, Endpoint, FakeS3, HttpRequest, Method, PartSize, Prefix, S3Config, S3Store,
    Transport, object_key,
};
use url::Url;

use crate::common::{config, credentials, now};

#[path = "../../../core/tests/core/heap.rs"]
#[expect(
    dead_code,
    reason = "the file is shared with the other memory gates; this binary measures a process"
)]
mod heap;

use heap::{Counting, exclude_thread, large_allocs_of, process_peak_of};

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

/// Holds the answer to the first span of a file until the other spans of the window have been
/// requested and fully sent, so a restore's read-ahead is on the heap by construction: whether
/// a decoder that keeps pace with the fetches ever lets the window fill is a matter of
/// scheduling, and a gate that waits on it is flaky.
struct SpanGate {
    /// Spans answered besides the first, and whether the first was released by them.
    state: Mutex<(usize, bool)>,
    changed: Condvar,
    /// Spans that must be answered ahead of the first: the window's width less one.
    ahead: usize,
}

/// How long the first span waits for the others before the gate reports that they never came.
/// A liveness backstop, never the thing the test waits on.
const GATE_BACKSTOP: Duration = Duration::from_secs(30);

impl SpanGate {
    fn new(width: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((0, false)),
            changed: Condvar::new(),
            ahead: width - 1,
        })
    }

    /// Called before the answer to the first span is written.
    fn hold_first(&self) {
        let guard = self.state.lock().unwrap();
        let (mut guard, _) = self
            .changed
            .wait_timeout_while(guard, GATE_BACKSTOP, |(sent, _)| *sent < self.ahead)
            .unwrap();
        guard.1 = guard.0 >= self.ahead;
    }

    /// Called once the answer to another span is on the wire.
    fn sent_ahead(&self) {
        self.state.lock().unwrap().0 += 1;
        self.changed.notify_all();
    }

    /// Whether the first span waited for a full window of the others.
    fn window_filled(&self) -> bool {
        self.state.lock().unwrap().1
    }
}

/// Reads one request off `stream` and answers it from `fake`, then closes. A `gate` holds the
/// answer to a ranged read from the start of the object until the other ranges are sent.
fn serve_one(
    mut stream: TcpStream,
    port: u16,
    fake: &FakeS3,
    gate: Option<&SpanGate>,
) -> io::Result<()> {
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
    let lines: Vec<&str> = lines.collect();
    // The headers the client added itself (its own user-agent, accept-encoding) are not the
    // store's: only those its Authorization signs, and the Authorization, reach the fake.
    let signed: BTreeSet<String> = lines
        .iter()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("authorization"))
        .and_then(|(_, value)| value.split_once("SignedHeaders="))
        .and_then(|(_, rest)| rest.split(',').next())
        .map(|list| list.split(';').map(str::to_owned).collect())
        .unwrap_or_default();
    let mut headers = Vec::new();
    let mut declared = 0_usize;
    let mut first_span = false;
    let mut other_span = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim().to_ascii_lowercase(), value.trim().to_owned());
        if name == "range" {
            first_span = value.starts_with("bytes=0-");
            other_span = !first_span;
        }
        if name == "content-length" {
            declared = value.parse().unwrap_or(0);
        }
        if name == "authorization" || (name != "host" && signed.contains(&name)) {
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
    if let Some(gate) = gate.filter(|_| first_span && method == Method::Get) {
        gate.hold_first();
    }
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
    stream.flush()?;
    if let Some(gate) = gate.filter(|_| other_span && method == Method::Get) {
        gate.sent_ahead();
    }
    Ok(())
}

/// Serves `fake` on a loopback port until the process ends. Returns the port.
fn serve(fake: &Arc<FakeS3>) -> u16 {
    serve_gated(fake, None)
}

/// [`serve`], holding the first span of a file as `gate` says.
fn serve_gated(fake: &Arc<FakeS3>, gate: Option<Arc<SpanGate>>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let fake = Arc::clone(fake);
    thread::spawn(move || {
        exclude_thread();
        for stream in listener.incoming().flatten() {
            let fake = Arc::clone(&fake);
            let gate = gate.clone();
            thread::spawn(move || {
                exclude_thread();
                // A client that hangs up early has nothing left to answer.
                if let Err(_hung_up) = serve_one(stream, port, &fake, gate.as_deref()) {}
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
fn assert_upload_holds_its_parts(part: usize, concurrency: usize, total: usize) {
    let fake = fake();
    let store = store_on(serve(&fake), part, concurrency);
    let key = FileKey::snapshot(txid(1));
    let written = vec![0xA5_u8; MIB];
    let ((done, peak), large) = large_allocs_of(|| {
        process_peak_of(|| {
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
        })
    });
    assert_eq!(done.unwrap().size, u64::try_from(total).unwrap());
    assert_eq!(
        fake.size_of(&object_key(config().prefix(), &key)),
        Some(total)
    );
    // The parts in flight and the one more that fills or waits.
    let bound = (concurrency + 1) * part + MARGIN;
    eprintln!(
        "upload part {} MiB x {concurrency}: {} KiB (bound {} KiB)",
        part / MIB,
        peak.extra >> 10,
        bound >> 10
    );
    assert!(
        peak.extra <= bound,
        "upload held {} bytes, over {bound} ({concurrency} parts in flight and one more x {part} + {MARGIN})",
        peak.extra
    );
    assert!(
        peak.extra >= (concurrency - 1) * part,
        "the gate saw {} bytes: it cannot see the part buffers",
        peak.extra
    );
    // One block per part, and a few for the request and the result: a part regrown from
    // empty by doubling is a dozen blocks a part, which the live count above never shows.
    let parts = total.div_ceil(part);
    eprintln!(
        "upload part {} MiB x {concurrency}: {large} large blocks for {parts} parts",
        part / MIB
    );
    assert!(
        large <= parts + 4,
        "{large} blocks of {} KiB or more for {parts} parts: a part buffer is allocated once, whole",
        heap::LARGE_BLOCK >> 10
    );
}

#[test]
fn an_upload_through_the_real_client_holds_five_parts_in_flight_and_one_more() {
    assert_upload_holds_its_parts(PartSize::MIN.get().try_into().unwrap(), 5, 56 * MIB);
}

/// A part size above the default and an object of many more parts than the concurrency, the
/// shape where a per-part cost multiplies: twelve parts of 16 MiB, three at a time.
#[test]
fn a_long_upload_at_a_large_part_size_holds_its_parts_and_allocates_each_once() {
    assert_upload_holds_its_parts(16 * MIB, 3, 12 * 16 * MIB);
}

#[test]
fn the_bound_follows_part_size_and_concurrency() {
    assert_upload_holds_its_parts(8 * MIB, 3, 56 * MIB);
}

/// A file on disk as an upload reads it again: the bytes at an offset, given only when they
/// are what was seen when the upload was first handed them. Each part gets a handle of its
/// own.
struct OnDisk(File);

impl Reread for OnDisk {
    fn open(&self) -> BoxFuture<'_, Result<Box<dyn Again>, StoreError>> {
        let opened = self.0.try_clone().map_err(|err| StoreError::Transient {
            op: StoreOp::Read,
            kind: err.kind(),
        });
        Box::pin(async move {
            let reader: Box<dyn Again> = Box::new(Self(opened?));
            Ok(reader)
        })
    }
}

impl Again for OnDisk {
    fn read<'a>(
        &'a mut self,
        offset: u64,
        was: &'a Seen,
    ) -> BoxFuture<'a, Result<Vec<u8>, StoreError>> {
        Box::pin(async move {
            let failed = |kind| StoreError::Transient {
                op: StoreOp::Read,
                kind,
            };
            let mut bytes = vec![0_u8; usize::try_from(was.len()).unwrap()];
            self.0
                .read_exact_at(&mut bytes, offset)
                .map_err(|err| failed(err.kind()))?;
            if Seen::of(&bytes) == *was {
                Ok(bytes)
            } else {
                Err(failed(io::ErrorKind::InvalidData))
            }
        })
    }
}

/// What an upload written from a file may hold for each request being sent, whatever the
/// part size: the 256 KiB read again and the client's buffers (measured: 768 KiB a request).
const FROM_A_FILE_EACH: usize = 768 * KIB;

/// What it may hold besides: the tallies, the read buffer this test hands over (256 KiB) and
/// the results.
const FROM_A_FILE_MARGIN: usize = MIB;

/// Uploads a file of `total` bytes that never repeat, handing the store each 256 KiB with
/// the file to read again, and checks that the store held no part of it.
fn assert_upload_from_a_file_holds_no_part(part: usize, concurrency: usize, total: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("staged");
    {
        let mut file = File::create(&path).unwrap();
        let mut block = vec![0_u8; MIB];
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        for _ in 0..total / MIB {
            for word in block.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                word.copy_from_slice(&state.to_le_bytes());
            }
            file.write_all(&block).unwrap();
        }
    }
    let fake = fake();
    let store = store_on(serve(&fake), part, concurrency);
    let key = FileKey::snapshot(txid(1));
    // The peak is a high-water mark, so a part held for one request would show in it.
    let (done, peak) = process_peak_of(|| {
        let again = OnDisk(File::open(&path).unwrap());
        let mut first = File::open(&path).unwrap();
        let mut chunk = vec![0_u8; 256 * KIB];
        block_on(async {
            let mut upload = store
                .begin_upload(
                    &key,
                    Timestamp::from_unix_millis(1_700_000_000_000),
                    WriteMode::New,
                )
                .await
                .unwrap();
            for _ in 0..total / chunk.len() {
                first.read_exact(&mut chunk).unwrap();
                upload.write_from(&chunk, &again).await.unwrap();
            }
            upload.commit().await
        })
    });
    assert_eq!(done.unwrap().size, u64::try_from(total).unwrap());
    let stored = fake
        .body_of(&object_key(config().prefix(), &key))
        .expect("the object is in the bucket");
    assert!(
        stored == std::fs::read(&path).unwrap(),
        "the bucket holds other bytes than the file"
    );
    eprintln!(
        "upload from a file, part {} MiB x {concurrency}: {} KiB",
        part / MIB,
        peak.extra >> 10,
    );
    let bound = concurrency * FROM_A_FILE_EACH + FROM_A_FILE_MARGIN;
    assert!(
        peak.extra <= bound,
        "an upload from a file held {} bytes, over {bound}: {concurrency} parts of {part} are \
         {} and none of them is to be held",
        peak.extra,
        concurrency * part
    );
    assert!(
        bound < part,
        "the gate cannot tell a held part from the bound"
    );
}

/// The gate of `docs/decisions/D222.md`: twelve parts of 16 MiB, three at a time, from a
/// file. Held as buffers that is 48 MiB; from a file it is under 4 MiB.
#[test]
fn an_upload_from_a_file_at_a_large_part_size_holds_no_part() {
    assert_upload_from_a_file_holds_no_part(16 * MIB, 3, 12 * 16 * MIB);
}

/// The same law at the smallest part, five at a time, and for an object that ends on a
/// part that is not full: the bound follows the concurrency and never the part size.
#[test]
fn an_upload_from_a_file_holds_the_same_at_any_part_size() {
    assert_upload_from_a_file_holds_no_part(PartSize::MIN.get().try_into().unwrap(), 5, 56 * MIB);
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

/// A file of transaction `n` as the encoder writes it: a database of `commit` 4 KiB pages
/// of which it holds `pages`, all incompressible.
fn ltx_bytes(n: u64, commit: u32, pages: std::ops::RangeInclusive<u32>) -> Vec<u8> {
    let size = PageSize::new(4096).unwrap();
    let header = Header::new(HeaderFields {
        tracked: false,
        page_size: size,
        commit,
        txids: TxidRange::new(txid(n), txid(n)).unwrap(),
        timestamp: Timestamp::from_unix_millis(1_700_000_000_000),
        pre_apply: None,
        wal: None,
        node_id: 0,
    })
    .unwrap();
    let mut encoder = Encoder::new(Vec::new(), header).unwrap();
    let mut page = vec![0_u8; size.bytes()];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for pgno in pages {
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

/// Restores a snapshot of `mib` MiB through the real client reading `spans` spans of a file at
/// once, and returns the heap peak.
fn restore_peak(mib: usize, spans: usize, gate: Option<Arc<SpanGate>>) -> usize {
    let fake = fake();
    let pages = u32::try_from(mib * 256).unwrap();
    let bytes = ltx_bytes(1, pages, 1..=pages);
    let key = FileKey::snapshot(txid(1));
    fake.put_raw(
        &object_key(config().prefix(), &key),
        bytes,
        &[("x-amz-meta-litestream-timestamp", &now().to_string())],
    );
    let store = store_on(
        serve_gated(&fake, gate),
        PartSize::MIN.get().try_into().unwrap(),
        spans,
    );
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

/// What a restore holds beyond its window: the client's buffers, the decoder's page and the
/// result. Tighter than `MARGIN` so that one more chunk, a copy of a body, is over the bound.
const RESTORE_MARGIN: usize = 3 * MIB;

/// The most a restore may hold: the one chunk it takes from a file's stream, and the margin.
fn restore_bound() -> usize {
    RESTORE_CHUNK + RESTORE_MARGIN
}

/// A restore through the real client streams each file through one GET and holds one chunk of
/// it, and no more as the database grows from 72 MiB to 144 MiB: the body is read from the
/// connection as the decoder takes it, never held whole.
#[test]
fn a_restore_through_the_real_client_holds_one_chunk() {
    let few = restore_peak(72, 1, None);
    let many = restore_peak(144, 1, None);
    eprintln!(
        "restore 72 MiB: {} KiB, 144 MiB: {} KiB (bound {} KiB)",
        few >> 10,
        many >> 10,
        restore_bound() >> 10
    );
    for peak in [few, many] {
        assert!(
            peak <= restore_bound(),
            "restore held {peak} bytes, over {}",
            restore_bound()
        );
        assert!(
            peak >= RESTORE_CHUNK,
            "the gate saw {peak} bytes: it cannot see the chunk"
        );
    }
}

/// Bytes a restore's span covers: core's `restore::window::SPAN_BYTES`.
const SPAN: usize = 8 * MIB;

/// A restore reading five spans at once holds the spans read ahead, each its 8 MiB and the
/// mebibyte its thread is reading and the one being handed out, beside the chunk; and no more as
/// the database doubles. The server holds the first span back until the other four are sent,
/// so the window is full whatever the decoder's pace.
#[test]
fn a_restore_through_a_window_of_spans_holds_the_window() {
    let (gate_few, gate_many) = (SpanGate::new(5), SpanGate::new(5));
    let few = restore_peak(72, 5, Some(Arc::clone(&gate_few)));
    let many = restore_peak(144, 5, Some(Arc::clone(&gate_many)));
    let bound = 5 * (SPAN + 2 * MIB) + restore_bound();
    eprintln!(
        "restore in spans 72 MiB: {} KiB, 144 MiB: {} KiB (bound {} KiB)",
        few >> 10,
        many >> 10,
        bound >> 10
    );
    for peak in [few, many] {
        assert!(peak <= bound, "restore held {peak} bytes, over {bound}");
    }
    for gate in [&gate_few, &gate_many] {
        assert!(
            gate.window_filled(),
            "the first span was never held behind a window of spans: fewer than five were open"
        );
    }
    assert!(
        few >= 2 * SPAN,
        "the gate saw {few} bytes: no span was read ahead"
    );
}

/// A ranged read of 12 MiB (a compaction's replica reads are a mebibyte; the largest a
/// `ReadLen` allows is 16): the response body is held once, at its length. A body copied out of
/// the response, or read into a buffer grown from empty (16 MiB of capacity and an 8 MiB block
/// beside it while it grows), is over the bound.
#[test]
fn a_ranged_read_through_the_real_client_holds_its_body_once() {
    let fake = fake();
    let len = 12 * MIB;
    let key = FileKey::snapshot(txid(1));
    fake.put_raw(
        &object_key(config().prefix(), &key),
        vec![7; len + MIB],
        &[("x-amz-meta-litestream-timestamp", &now().to_string())],
    );
    let store = store_on(serve(&fake), PartSize::MIN.get().try_into().unwrap(), 5);
    let want = lsr_core::ReadLen::new(u32::try_from(len).unwrap()).unwrap();
    let (read, peak) = process_peak_of(|| block_on(store.read(&key, 0, want)));
    assert_eq!(read.unwrap().len(), len);
    eprintln!("ranged read of {} KiB: {} KiB", len >> 10, peak.extra >> 10);
    assert!(
        peak.extra <= len + RESTORE_MARGIN,
        "a read of {len} bytes held {}",
        peak.extra
    );
}

/// Pages each staged input holds, all of them distinct: `MAX_INPUTS` inputs make a 62.5 MiB
/// output, twelve parts, so the compaction uploads in batches as a busy database's does.
const INPUT_PAGES: u32 = 125;

/// What the engine's compaction reads a staged input in when it merges as many as one takes
/// (its share of the 4 MiB staging budget, restated: a change shows here), a page, and the
/// output buffer it flushes at.
const STAGED_WINDOW: usize = 4 * MIB / MAX_INPUTS;
const PAGE: usize = 4 * KIB;
const FLUSH: usize = MIB;

/// The memory of a replicating database's busiest moment: a level-1 compaction merging
/// `MAX_INPUTS` staged files while its output goes up as a multipart upload through the
/// real client. The two bounds add: the inputs' windows and one batch of parts.
#[test]
fn a_compaction_into_s3_holds_its_inputs_and_its_parts() {
    assert_compaction_holds_its_parts(PartSize::MIN.get().try_into().unwrap(), 5);
}

/// A merge of four staged files reads each in 128 KiB, the most a staging window is (the
/// budget would allow a mebibyte each): the files are over a mebibyte, so a window that grew
/// to it would show. The output is under one part, so the output it holds is its size.
#[test]
fn a_compaction_of_few_staged_files_into_s3_reads_each_in_a_staging_window() {
    let (done, peak) = staged_compaction_peak(4, 300, PartSize::MIN.get().try_into().unwrap(), 5);
    let output = usize::try_from(done.info().size).unwrap();
    assert!(output < 5 * MIB, "one part holds the output: {output}");
    let bound = 4 * (128 * KIB + PAGE) + FLUSH + output + MIB;
    eprintln!(
        "compaction of 4 files into s3: {} KiB (bound {} KiB)",
        peak >> 10,
        bound >> 10
    );
    assert!(peak <= bound, "held {peak} bytes, over {bound}");
}

/// The same at a part size above the default and a concurrency below it: the output is four
/// parts, two batches, so a part held past its batch shows.
#[test]
fn a_compaction_into_s3_at_a_large_part_size_holds_its_parts() {
    assert_compaction_holds_its_parts(16 * MIB, 2);
}

fn assert_compaction_holds_its_parts(part: usize, concurrency: usize) {
    let (done, peak) = staged_compaction_peak(MAX_INPUTS, INPUT_PAGES, part, concurrency);
    assert!(done.info().size > u64::try_from(concurrency * part).unwrap());
    // The parts in flight and one more, as an upload holds them by design (D232): the part the
    // compaction fills while the others go up. How long that one coexists with the batch is the
    // transport's timing, so a quiet machine measured 37.6 MiB at 16 MiB x 2 and a busy CI run
    // 54 MiB. A bound of the batch alone failed on CI; the upload tests above see a copied body.
    let bound = MAX_INPUTS * (STAGED_WINDOW + PAGE) + FLUSH + (concurrency + 1) * part + MARGIN;
    eprintln!(
        "compaction into s3: {} KiB (bound {} KiB)",
        peak >> 10,
        bound >> 10
    );
    assert!(
        peak <= bound,
        "compaction held {peak} bytes, over {bound} ({MAX_INPUTS} inputs x ({STAGED_WINDOW} + {PAGE}) + {FLUSH} + {concurrency} parts in flight and one more x {part} + {MARGIN})"
    );
}

/// Compacts `inputs` staged level-0 files of `pages` distinct pages each into level 1 of a
/// store that uploads `concurrency` parts of `part` bytes at once, and returns what it wrote
/// and the peak live heap above the start.
fn staged_compaction_peak(
    inputs: usize,
    pages: u32,
    part: usize,
    concurrency: usize,
) -> (lsr_core::Compacted, usize) {
    let from = tempfile::tempdir().unwrap();
    let staging = FsStore::new(from.path());
    let commit = pages * u32::try_from(inputs).unwrap();
    // From transaction 2: a file that starts at transaction 1 is a snapshot, and must hold
    // every page.
    for n in 2..=u64::try_from(inputs).unwrap() + 1 {
        let first = pages * u32::try_from(n - 2).unwrap() + 1;
        let bytes = ltx_bytes(n, commit, first..=first + pages - 1);
        let key = FileKey::new(Level::ZERO, TxidRange::new(txid(n), txid(n)).unwrap());
        block_on(async {
            let stamp = Timestamp::from_unix_millis(1_700_000_000_000);
            let mut upload = staging
                .begin_upload(&key, stamp, WriteMode::New)
                .await
                .unwrap();
            upload.write(&bytes).await.unwrap();
            upload.commit().await.unwrap();
        });
    }
    let files = block_on(staging.list(Level::ZERO, None, PageLimit::MAX))
        .unwrap()
        .files;
    let inputs = CompactionInputs::new(files).unwrap();
    let fake = fake();
    let dest = store_on(serve(&fake), part, concurrency);

    let (done, peak) = process_peak_of(|| {
        block_on(compact_staged(
            &staging,
            &dest,
            &inputs,
            Level::new(1).unwrap(),
        ))
    });

    let done = done.unwrap();
    assert_eq!(done.pages(), commit);
    (done, peak.extra)
}

/// Pages each replica input of [`compaction_from_s3_peak`] holds: about 400 KiB, more than
/// the window a merge of 32 inputs or more reads it in.
const REPLICA_INPUT_PAGES: u32 = 100;

/// What a merge holds for each replica input besides its window: a page waiting to be merged,
/// the decoder's page and frame, and the held GET's connection with the client's buffers
/// (32 KiB read and 128 KiB write, `docs/decisions/D212.md`). Measured: 187 KiB an input at
/// 64 inputs and at 128 (28.0 MiB and 39.4 MiB in all).
const PER_REPLICA_INPUT: usize = 208 * KIB;

/// The peak live heap of a compaction above level 1 that reads `inputs` level-1 files through
/// the real client, each over one GET held for the whole merge (Go's compactor holds one
/// open reader per input the same way), into a directory.
fn compaction_from_s3_peak(inputs: u64) -> usize {
    let fake = fake();
    let source = store_on(serve(&fake), PartSize::MIN.get().try_into().unwrap(), 5);
    let commit = REPLICA_INPUT_PAGES * u32::try_from(inputs).unwrap();
    for n in 2..=inputs + 1 {
        let first = REPLICA_INPUT_PAGES * u32::try_from(n - 2).unwrap() + 1;
        let bytes = ltx_bytes(n, commit, first..=first + REPLICA_INPUT_PAGES - 1);
        assert!(
            bytes.len() > 256 * KIB,
            "an input holds more than its window"
        );
        let key = FileKey::new(
            Level::new(1).unwrap(),
            TxidRange::new(txid(n), txid(n)).unwrap(),
        );
        block_on(async {
            let stamp = Timestamp::from_unix_millis(1_700_000_000_000);
            let mut upload = source
                .begin_upload(&key, stamp, WriteMode::New)
                .await
                .unwrap();
            upload.write(&bytes).await.unwrap();
            upload.commit().await.unwrap();
        });
    }
    let files = block_on(source.list(Level::new(1).unwrap(), None, PageLimit::MAX))
        .unwrap()
        .files;
    assert_eq!(files.len(), usize::try_from(inputs).unwrap());
    let inputs = CompactionInputs::new(files).unwrap();
    let to = tempfile::tempdir().unwrap();
    let dest = FsStore::new(to.path());
    let (done, peak) =
        process_peak_of(|| block_on(compact(&source, &dest, &inputs, Level::new(2).unwrap())));
    assert_eq!(done.unwrap().pages(), commit);
    peak.extra
}

/// A compaction above level 1 of as many replica inputs as one may take holds the replica's
/// 16 MiB budget of windows and, per input, a page or two and the held GET's connection: the
/// memory a whole 128-input merge costs, where Go holds one reader per input with no cap.
#[test]
fn a_compaction_from_s3_holds_its_budget_and_a_connection_per_input() {
    for inputs in [64, MAX_INPUTS] {
        let peak = compaction_from_s3_peak(u64::try_from(inputs).unwrap());
        let window = (16 * MIB / inputs).min(MIB);
        let bound = inputs * (window + PER_REPLICA_INPUT) + FLUSH + MARGIN;
        eprintln!(
            "compaction of {inputs} inputs from s3: {} KiB (bound {} KiB)",
            peak >> 10,
            bound >> 10
        );
        assert!(
            peak <= bound,
            "{inputs} inputs held {peak} bytes, over {bound} ({inputs} x ({window} + {PER_REPLICA_INPUT}) + {FLUSH} + {MARGIN})"
        );
        assert!(
            peak >= inputs * window / 2,
            "the gate saw {peak} bytes: it cannot see the windows"
        );
    }
}
