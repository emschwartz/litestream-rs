//! Servers that read a request body badly, for the transports' tests of what bounds an upload.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Bytes the paced server reads at a time.
const CHUNK: usize = 64 * 1024;

const OK: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";

/// Runs `work` and says how long it took on the real clock: the transports' bounds are real
/// socket timeouts, which no injected clock reaches.
#[expect(
    clippy::disallowed_methods,
    reason = "a test of a socket timeout measures real time"
)]
pub(crate) fn timed<T>(work: impl FnOnce() -> T) -> (T, Duration) {
    let began = std::time::Instant::now();
    let out = work();
    (out, began.elapsed())
}

/// A listener whose connections hold at most a few KiB unread, so that the kernel's buffers
/// do not swallow a body the server then takes seconds to read: without it a client sees its
/// whole body sent while the server is still reading, and waits on a response that comes
/// after the buffered rest. The size is set before `listen`, which accepted sockets inherit.
fn listen_with_a_small_buffer() -> TcpListener {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        socket.set_recv_buffer_size(16 * 1024).expect("buffer size");
        socket
            .bind("127.0.0.1:0".parse().expect("address"))
            .expect("bind");
        let listener = socket.listen(1).expect("listen").into_std().expect("std");
        listener.set_nonblocking(false).expect("blocking");
        listener
    })
}

/// A server that reads the body of one request steadily, `pause` after each 64 KiB, and then
/// answers 200. Hands back how many body bytes it read. It sleeps between reads because it is
/// the slow peer; the test thread waits on the request.
#[expect(
    clippy::disallowed_methods,
    reason = "the server is the slow peer: it paces its reads on the real clock"
)]
pub(crate) fn serve_paced_reader(want: usize, pause: Duration) -> (String, JoinHandle<usize>) {
    let listener = listen_with_a_small_buffer();
    let addr = listener.local_addr().expect("addr");
    let served = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut head = Vec::new();
        let mut buf = [0_u8; CHUNK];
        let mut body = 0_usize;
        let mut in_body = false;
        while !in_body {
            let read = match stream.read(&mut buf) {
                Ok(0) | Err(_) => return body,
                Ok(read) => read,
            };
            head.extend_from_slice(&buf[..read]);
            if let Some(at) = head.windows(4).position(|w| w == b"\r\n\r\n") {
                in_body = true;
                body = head.len().saturating_sub(at).saturating_sub(4);
            }
        }
        // A whole 64 KiB then a pause, whatever size the kernel hands a read: with a small
        // receive buffer (Linux's smallest is a few KiB) a pause per read would be far slower.
        while body < want {
            thread::sleep(pause);
            let take = CHUNK.min(want.saturating_sub(body));
            if stream.read_exact(&mut buf[..take]).is_err() {
                return body;
            }
            body = body.saturating_add(take);
        }
        let _wrote = stream.write_all(OK);
        body
    });
    (format!("http://{addr}/key"), served)
}

/// A server that accepts one connection, never reads from it and holds it until the sender
/// is used or dropped.
pub(crate) fn serve_never_reading() -> (String, Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (done, wait) = mpsc::channel::<()>();
    thread::spawn(move || {
        let (_stream, _) = listener.accept().expect("accept");
        let _released = wait.recv();
    });
    (format!("http://{addr}/key"), done)
}

/// A server that answers one request with `len` body bytes sent steadily, `chunk` bytes and then
/// `pause`, so the answer takes `len / chunk * pause` and never goes quiet for longer than
/// `pause`, under a declared length.
#[expect(
    clippy::disallowed_methods,
    reason = "the server is the slow peer: it paces its writes on the real clock"
)]
pub(crate) fn serve_paced_writer(len: usize, chunk: usize, pause: Duration) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut head = [0_u8; 4096];
        let _read = stream.read(&mut head);
        let reply =
            format!("HTTP/1.1 200 OK\r\ncontent-length: {len}\r\nconnection: close\r\n\r\n");
        if stream.write_all(reply.as_bytes()).is_err() {
            return;
        }
        let mut left = len;
        while left > 0 {
            let now = left.min(chunk);
            if stream.write_all(&vec![9_u8; now]).is_err() {
                return;
            }
            left = left.saturating_sub(now);
            thread::sleep(pause);
        }
    });
    format!("http://{addr}/key")
}
