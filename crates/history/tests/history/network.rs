//! The real replicator through a faulty network, judged by restores: Toxiproxy between the
//! engine and MinIO (`just test-s3` starts both; the tests fail without either, unless
//! `LSR_SKIP` names it).
//!
//! The simulator injects store faults above the HTTP client, so it cannot see what a real
//! client does with a connection reset half way through a body, a response that never comes
//! after the request went out, a body sliced into small writes, or a slow link. Here the engine
//! writes through a Toxiproxy proxy to MinIO, and each test adds one kind of toxic while the
//! application commits, then takes it away. Restores read MinIO directly, so they are judged
//! while the fault still holds, and again once it has cleared; the history judge
//! (`lsr_history::Judge`) reads only restored content and the commit log, so a truncated body
//! that the client took as whole, or a retried upload that landed twice with other bytes, shows
//! as a state no commit made or a `Latest` that went back.
//!
//! Toxiproxy is <https://github.com/Shopify/toxiproxy>, MIT licence, built at a pinned
//! version by `scripts/toxiproxy.sh`; it is a test-only tool, never linked into anything.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use lsr_store_s3::{
    BucketName, Endpoint, Prefix, Region, S3Config, S3Credentials, S3Store, SecretString,
};
use tempfile::TempDir;

use crate::replicator::{POLL, Run, wait};

/// One HTTP/1.1 request to the Toxiproxy API; its status and body.
fn api(base: &str, method: &str, path: &str, body: &str) -> (u16, String) {
    let authority = base.trim_start_matches("http://").trim_end_matches('/');
    let mut stream = TcpStream::connect(authority).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = response.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    (status, body.to_owned())
}

/// A proxy to MinIO that lives as long as the value.
struct Proxy {
    api: String,
    name: String,
    listen: String,
}

impl Proxy {
    fn new(base: &str, name: &str, upstream: &str) -> Self {
        let upstream = upstream.trim_start_matches("http://");
        let _ = api(base, "DELETE", &format!("/proxies/{name}"), "");
        let body = format!(
            r#"{{"name":"{name}","listen":"127.0.0.1:0","upstream":"{upstream}","enabled":true}}"#
        );
        let (status, created) = api(base, "POST", "/proxies", &body);
        assert_eq!(status, 201, "creating the proxy: {created}");
        let listen = created
            .split(r#""listen":""#)
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .unwrap()
            .to_owned();
        Self {
            api: base.to_owned(),
            name: name.to_owned(),
            listen,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.listen)
    }

    /// Adds a toxic of `kind` on `stream` (`upstream`: toward MinIO, `downstream`: back to the
    /// engine) with `attributes` (a JSON object's members).
    fn add(&self, toxic: &str, kind: &str, stream: &str, attributes: &str) {
        let body = format!(
            r#"{{"name":"{toxic}","type":"{kind}","stream":"{stream}","toxicity":1.0,"attributes":{{{attributes}}}}}"#
        );
        let (status, out) = api(
            &self.api,
            "POST",
            &format!("/proxies/{}/toxics", self.name),
            &body,
        );
        assert_eq!(status, 200, "adding {toxic}: {out}");
    }

    fn remove(&self, toxic: &str) {
        let (status, out) = api(
            &self.api,
            "DELETE",
            &format!("/proxies/{}/toxics/{toxic}", self.name),
            "",
        );
        assert_eq!(status, 204, "removing {toxic}: {out}");
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = api(&self.api, "DELETE", &format!("/proxies/{}", self.name), "");
    }
}

/// A store on MinIO at `endpoint` under a prefix of its own.
fn store_at(endpoint: &str, minio: &lsr_core::needs::Minio, prefix: &str) -> S3Store {
    let config = S3Config::new(
        Endpoint::new(endpoint).unwrap(),
        BucketName::new("lsr-test").unwrap(),
        Prefix::new(prefix).unwrap(),
    )
    .with_region(Region::new("us-east-1").unwrap());
    let credentials = S3Credentials::new(
        minio.access.clone(),
        SecretString::new(minio.secret.clone()),
    );
    S3Store::new(config, credentials).unwrap()
}

/// One fault: what to add, and on which stream.
struct Toxic {
    kind: &'static str,
    stream: &'static str,
    attributes: &'static str,
}

/// What a run must show of its faults, so a toxic that never bit cannot pass as tested.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Felt {
    /// Some round's uploads failed and were retried.
    Retried,
    /// Every round's uploads went through the toxic and completed.
    Completed,
}

/// Rounds of: commit with the toxic in place, let the engine try until it retries or catches
/// up, judge restores read straight from MinIO while the toxic holds, take it away, catch up,
/// judge again. The run ends with a drained stop, after which `Latest` is the head.
fn through(name: &str, seed: u64, toxics: &[Toxic], felt: Felt) {
    let (Some(minio), Some(toxiproxy)) = (lsr_core::needs::minio(), lsr_core::needs::toxiproxy())
    else {
        return;
    };
    let proxy = Proxy::new(
        &toxiproxy.api,
        &format!("lsr-{name}-{}", std::process::id()),
        &minio.endpoint,
    );
    let dir = TempDir::new().unwrap();
    let prefix = format!(
        "network-{name}-{}-{}",
        std::process::id(),
        dir.path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .trim_start_matches('.')
    );
    let engine = Arc::new(store_at(&proxy.endpoint(), &minio, &prefix));
    let direct = store_at(&minio.endpoint, &minio, &prefix);
    let db = dir.path().join("app.db");
    let mut run = Run::new(&engine, &direct, dir.path(), &db, seed);
    run.start();
    for _ in 0..3 {
        run.commit();
        run.poll();
    }
    let (mut retried, mut completed) = (0_u32, 0_u32);
    for (round, toxic) in toxics.iter().cycle().take(6).enumerate() {
        let id = format!("t{round}");
        proxy.add(&id, toxic.kind, toxic.stream, toxic.attributes);
        let before = run.replicator().health().counters;
        for _ in 0..3 {
            run.commit();
        }
        run.clock.advance(POLL);
        // The engine tries an upload through the toxic: it completes, or it fails and waits.
        let after = wait(run.replicator(), |h| {
            h.counters.retries > before.retries || h.counters.uploads > before.uploads
        })
        .counters;
        retried = retried.saturating_add(u32::from(after.retries > before.retries));
        completed = completed.saturating_add(u32::from(after.uploads > before.uploads));
        // Survived: restores judged while the fault holds.
        run.judged(true, None);
        proxy.remove(&id);
        run.poll();
        // Recovered: and once it has cleared.
        run.judged(round % 2 == 0, None);
    }
    run.commit();
    run.stop();
    let counts = run.judge.counts();
    eprintln!("{name}: {counts:?}, {retried} rounds retried, {completed} completed");
    assert!(
        counts.latest >= 12 && counts.points > 0 && counts.lag > 0,
        "{counts:?}"
    );
    match felt {
        Felt::Retried => assert!(retried > 0, "no round's upload failed: the toxic never bit"),
        Felt::Completed => assert_eq!(completed, 6, "{retried} rounds retried"),
    }
}

/// The connection is reset part way through: the request's body (`limit_data` upstream cuts
/// it after some bytes), or the response's (`limit_data` downstream), or after a delay with
/// a TCP reset (`reset_peer`).
#[test]
#[ignore = "needs MinIO and Toxiproxy; just test-s3"]
fn resets_mid_body_leave_only_committed_states() {
    through(
        "reset",
        11,
        &[
            Toxic {
                kind: "limit_data",
                stream: "upstream",
                attributes: r#""bytes":700"#,
            },
            Toxic {
                kind: "limit_data",
                stream: "downstream",
                attributes: r#""bytes":300"#,
            },
            Toxic {
                kind: "reset_peer",
                stream: "upstream",
                attributes: r#""timeout":50"#,
            },
        ],
        Felt::Retried,
    );
}

/// The request goes out and MinIO acts on it, and the response never comes back: the
/// connection closes after a while with nothing returned, so an upload the store committed
/// looks failed to the engine.
#[test]
#[ignore = "needs MinIO and Toxiproxy; just test-s3"]
fn a_timeout_after_the_request_was_sent_leaves_only_committed_states() {
    through(
        "timeout",
        12,
        &[Toxic {
            kind: "timeout",
            stream: "downstream",
            attributes: r#""timeout":1500"#,
        }],
        Felt::Retried,
    );
}

/// Every byte arrives, in small slices with delays between them, both ways.
#[test]
#[ignore = "needs MinIO and Toxiproxy; just test-s3"]
fn sliced_bodies_leave_only_committed_states() {
    through(
        "slicer",
        13,
        &[
            Toxic {
                kind: "slicer",
                stream: "downstream",
                attributes: r#""average_size":7,"size_variation":5,"delay":20"#,
            },
            Toxic {
                kind: "slicer",
                stream: "upstream",
                attributes: r#""average_size":11,"size_variation":9,"delay":20"#,
            },
        ],
        Felt::Completed,
    );
}

/// A slow link both ways, with latency: every request completes, late.
#[test]
#[ignore = "needs MinIO and Toxiproxy; just test-s3"]
fn a_slow_link_leaves_only_committed_states() {
    through(
        "bandwidth",
        14,
        &[
            Toxic {
                kind: "bandwidth",
                stream: "upstream",
                attributes: r#""rate":32"#,
            },
            Toxic {
                kind: "bandwidth",
                stream: "downstream",
                attributes: r#""rate":32"#,
            },
            Toxic {
                kind: "latency",
                stream: "downstream",
                attributes: r#""latency":300,"jitter":200"#,
            },
        ],
        Felt::Completed,
    );
}
