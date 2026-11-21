//! The listener on Go's `addr`: `/metrics` ([`crate::metrics`]) and `/healthz`
//! ([`crate::health`]), on a server written by hand over [`TcpListener`] (no new dependency).
//!
//! **The address.** Read as Go reads it (`replicate.go:449-459`): `net.SplitHostPort`, a
//! port required ("must specify port for bind address"), refused at load here rather than
//! after the databases start. The listener binds exactly what the address says, as Go's
//! `ListenAndServe` does: an empty host (`:9090`) is every interface, IPv6 and IPv4 where the
//! system allows, else every IPv4 one; a host name binds the first address it resolves to.
//! Go's log line says `localhost` for an empty host although it binds every interface; this
//! one names the socket actually bound. A port that is taken is logged and the daemon keeps
//! replicating, as Go's goroutine does.
//!
//! **The server.** One accepting thread; each connection gets its own thread, at most
//! [`MAX_CONNECTIONS`] at once. Past that a connection is answered 503 on a thread of its
//! own, at most as many again; past both it is closed unanswered. So at most twice
//! [`MAX_CONNECTIONS`] threads and buffers exist, whatever arrives.
//! One request per connection: the head is read into a fixed [`MAX_REQUEST`]-byte buffer
//! (a longer one is answered 431, so memory per connection never grows), with a
//! [`DEADLINE`] on each read and write; the body, if any, is never read. Only `GET` of
//! `/metrics` and `/healthz` is served, a query string ignored; another method on them is
//! 405, any other path 404, a request line that is not HTTP/1 is 400. Every response closes
//! the connection.

use lsr_replica::stack;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lsr_core::Clock;

use crate::health::{Board, healthz};
use crate::metrics;

/// Connections served at once.
pub const MAX_CONNECTIONS: usize = 8;
/// The longest request head read.
pub const MAX_REQUEST: usize = 8 * 1024;
/// The wait allowed for each read or write on a connection.
pub(crate) const DEADLINE: Duration = Duration::from_secs(5);
/// What is read and dropped after a refused request, so the close is clean.
const DRAIN: usize = 64 * 1024;

/// Why `addr` cannot be bound as written.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AddrError {
    /// Go's own refusal.
    #[error("must specify port for bind address: {addr:?}")]
    NoPort {
        /// The address.
        addr: String,
    },
    /// The port is not a number (Go would fail to listen, after the databases start, and
    /// keep running without the listener).
    #[error("address {addr:?}: the port is not a number from 0 to 65535: {source}")]
    BadPort {
        /// The address.
        addr: String,
        /// Why the port does not parse.
        source: std::num::ParseIntError,
    },
}

/// Where to listen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Target {
    /// Every interface (an empty host).
    Every {
        /// The port.
        port: u16,
    },
    /// One host.
    Host {
        /// The host, without brackets.
        host: String,
        /// The port.
        port: u16,
    },
}

/// Go's `net.SplitHostPort`, then Go's port check, then a numeric port.
///
/// # Errors
/// [`AddrError`].
pub(crate) fn target(addr: &str) -> Result<Target, AddrError> {
    // Go discards SplitHostPort's error and sees an empty port: every malformed address
    // gets Go's one message.
    let no_port = || AddrError::NoPort {
        addr: addr.to_owned(),
    };
    let (host, port) = split_host_port(addr).ok_or_else(no_port)?;
    if port.is_empty() {
        return Err(no_port());
    }
    let port = port.parse().map_err(|source| AddrError::BadPort {
        addr: addr.to_owned(),
        source,
    })?;
    Ok(if host.is_empty() {
        Target::Every { port }
    } else {
        Target::Host {
            host: host.to_owned(),
            port,
        }
    })
}

/// Go's `net.SplitHostPort`: `host:port` or `[host]:port`, one colon outside brackets.
fn split_host_port(addr: &str) -> Option<(&str, &str)> {
    let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        (host, after.strip_prefix(':')?)
    } else {
        let (host, port) = addr.rsplit_once(':')?;
        if host.contains(':') {
            return None;
        }
        (host, port)
    };
    if host.contains(['[', ']']) || port.contains(['[', ']']) {
        return None;
    }
    Some((host, port))
}

/// Binds `target` and serves on a thread of its own; the address actually bound.
///
/// # Errors
/// The bind or the thread failed.
pub(crate) fn serve(
    target: &Target,
    board: Board,
    clock: Arc<dyn Clock>,
) -> io::Result<SocketAddr> {
    let listener = match target {
        Target::Every { port } => {
            TcpListener::bind(("::", *port)).or_else(|_v6| TcpListener::bind(("0.0.0.0", *port)))?
        }
        Target::Host { host, port } => TcpListener::bind((host.as_str(), *port))?,
    };
    let local = listener.local_addr()?;
    stack::spawn("lsr-http", stack::WAIT_STACK, move || {
        accept(&listener, &board, &clock);
    })?;
    Ok(local)
}

/// One connection's place among [`MAX_CONNECTIONS`], given back on drop.
struct Slot(Arc<AtomicUsize>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Slot {
    /// A place among `count`'s [`MAX_CONNECTIONS`], if one is free.
    fn take(count: &Arc<AtomicUsize>) -> Option<Self> {
        if count.fetch_add(1, Ordering::SeqCst) >= MAX_CONNECTIONS {
            count.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(Self(Arc::clone(count)))
    }

    /// Runs `work` on a thread of its own that holds this place until it ends.
    fn run(self, work: impl FnOnce() + Send + 'static) {
        let spawned = stack::spawn("lsr-http-conn", stack::WAIT_STACK, move || {
            let _slot = self;
            work();
        });
        if spawned.is_err() {
            // The thread could not start; the place came back with the closure.
        }
    }
}

/// Serves each connection on its own thread; past [`MAX_CONNECTIONS`], answers 503 on one of
/// at most as many more (an answer drains what the peer sends, which must not hold up the
/// accepting thread); past both, closes the connection unanswered.
fn accept(listener: &TcpListener, board: &Board, clock: &Arc<dyn Clock>) {
    let live = Arc::new(AtomicUsize::new(0));
    let refusing = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        // A failed accept (the peer gave up first) leaves nothing to answer.
        let Ok(mut stream) = stream else { continue };
        if let Some(slot) = Slot::take(&live) {
            let (board, clock) = (board.clone(), Arc::clone(clock));
            slot.run(move || handle(stream, &board, &*clock));
        } else if let Some(slot) = Slot::take(&refusing) {
            slot.run(move || refuse(&mut stream, 503, "too many connections\n"));
        }
    }
}

/// Answers `status` and closes cleanly: what the peer still sends is read (up to [`DRAIN`])
/// and dropped, so the close is not a reset that loses the answer.
fn refuse(stream: &mut TcpStream, status: u16, body: &str) {
    if stream.set_write_timeout(Some(DEADLINE)).is_err()
        || stream.set_read_timeout(Some(DEADLINE)).is_err()
    {
        return;
    }
    respond(stream, status, "text/plain; charset=utf-8", &[], body);
    let mut scratch = [0_u8; 1024];
    let mut left = DRAIN;
    while left > 0 {
        match stream.read(&mut scratch) {
            Ok(0) | Err(_) => break,
            Ok(n) => left = left.saturating_sub(n),
        }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

fn respond(stream: &mut TcpStream, status: u16, kind: &str, extra: &[&str], body: &str) {
    let mut head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n",
        reason(status),
        body.len()
    );
    for line in extra {
        head.push_str(line);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    if stream
        .write_all(head.as_bytes())
        .and_then(|()| stream.write_all(body.as_bytes()))
        .and_then(|()| stream.flush())
        .is_err()
    {
        // The peer went away; there is no one to tell.
    }
    if stream.shutdown(Shutdown::Write).is_err() {
        // Already closed.
    }
}

/// How the request head ended.
enum Head {
    /// The head, up to the blank line.
    Complete(usize),
    /// The buffer filled first.
    TooLarge,
    /// The peer closed, or a read failed or timed out.
    Gone,
}

fn read_head(stream: &mut TcpStream, buf: &mut [u8; MAX_REQUEST]) -> Head {
    let mut len = 0;
    loop {
        let Some(free) = buf.get_mut(len..).filter(|free| !free.is_empty()) else {
            return Head::TooLarge;
        };
        match stream.read(free) {
            Ok(0) | Err(_) => return Head::Gone,
            Ok(n) => len = len.saturating_add(n),
        }
        let read = buf.get(..len).unwrap_or_default();
        if let Some(end) = read.windows(4).position(|w| w == b"\r\n\r\n") {
            return Head::Complete(end);
        }
    }
}

fn handle(mut stream: TcpStream, board: &Board, clock: &dyn Clock) {
    if stream.set_read_timeout(Some(DEADLINE)).is_err()
        || stream.set_write_timeout(Some(DEADLINE)).is_err()
    {
        return;
    }
    let mut buf = [0_u8; MAX_REQUEST];
    let end = match read_head(&mut stream, &mut buf) {
        Head::Complete(end) => end,
        Head::TooLarge => {
            return refuse(&mut stream, 431, "request head too large\n");
        }
        Head::Gone => return,
    };
    let head = buf.get(..end).unwrap_or_default();
    let line = head.split(|b| *b == b'\n').next().unwrap_or_default();
    let line = String::from_utf8_lossy(line);
    let mut parts = line.trim_end_matches('\r').split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return refuse(&mut stream, 400, "bad request line\n");
    };
    if !version.starts_with("HTTP/1.") {
        return refuse(&mut stream, 400, "only HTTP/1 is served\n");
    }
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    if path != "/metrics" && path != "/healthz" {
        return refuse(
            &mut stream,
            404,
            "not found: /metrics and /healthz are served\n",
        );
    }
    if method != "GET" {
        respond(
            &mut stream,
            405,
            "text/plain; charset=utf-8",
            &["Allow: GET"],
            "GET only\n",
        );
        return;
    }
    let views = board.views();
    let now = clock.now();
    if path == "/metrics" {
        let body = metrics::render(&views, now);
        respond(
            &mut stream,
            200,
            "text/plain; version=0.0.4; charset=utf-8",
            &[],
            &body,
        );
    } else {
        let (up, body) = healthz(&views, now);
        let status = if up { 200 } else { 503 };
        respond(&mut stream, status, "application/json", &[], &body);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `net.SplitHostPort` and its port check, as `replicate.go:449-452` applies them.
    #[test]
    fn the_address_is_read_as_go_reads_it() {
        assert_eq!(target(":9090"), Ok(Target::Every { port: 9090 }));
        assert_eq!(
            target("127.0.0.1:0"),
            Ok(Target::Host {
                host: "127.0.0.1".to_owned(),
                port: 0
            })
        );
        assert_eq!(
            target("[::1]:9090"),
            Ok(Target::Host {
                host: "::1".to_owned(),
                port: 9090
            })
        );
        for bad in [
            "localhost:",
            ":",
            "localhost",
            "::1:9090",
            "[::1]9090",
            "[a]:[b]",
        ] {
            assert!(
                matches!(target(bad), Err(AddrError::NoPort { .. })),
                "{bad}"
            );
        }
        for bad in ["a:b", "host:99999"] {
            assert!(
                matches!(target(bad), Err(AddrError::BadPort { .. })),
                "{bad}"
            );
        }
    }
}
