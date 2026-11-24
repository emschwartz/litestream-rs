//! What a replica with no path inside the bucket puts on the wire. Go's keys then start with a
//! slash (`/0000/<file>.ltx`), so the request target has an empty segment: `/bucket//0000/<file>`
//! in path style, `//0000/<file>` in virtual-host style. S3 does not normalise paths and the
//! signature covers the path as sent, so no layer between the request builder and the socket
//! (the `url` type, the `http` crate, a client) may collapse it. Each transport's tests send
//! every request form to a real socket with [`sent_by`] and compare what arrived.

#![cfg(any(feature = "ureq", feature = "reqwest"))]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::thread;

use lsr_core::{FileKey, Level, PageLimit, PartNumber, ReadLen};
use lsr_ltx::{Timestamp, Txid, TxidRange};
use url::Url;

use crate::config::{BucketName, Endpoint, Prefix, Provider, S3Config, UrlStyle};
use crate::credentials::{S3Credentials, SecretString};
use crate::request::{HttpRequest, RequestBuilder, UploadId};
use crate::transport::Transport;

/// The one file every object request names.
pub(crate) const FILE: &str = "0000000000000001-0000000000000001.ltx";

/// A request as the server read it: its form, the path and the decoded query of the target.
#[derive(Debug)]
pub(crate) struct Arrived {
    pub form: &'static str,
    pub path: String,
    pub query: Vec<(String, String)>,
}

impl Arrived {
    pub(crate) fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Serves one request: reads it whole, answers 200 and closes. Returns the request target.
fn serve_one() -> (u16, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let (target, seen) = mpsc::channel::<String>();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut head = Vec::new();
        let mut buf = [0_u8; 4096];
        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
            let read = stream.read(&mut buf).expect("read");
            assert!(read > 0, "the client closed before the head ended");
            head.extend_from_slice(buf.get(..read).expect("in range"));
        }
        let text = String::from_utf8_lossy(&head).into_owned();
        let _sent = target.send(text.split_whitespace().nth(1).unwrap_or("").to_owned());
        // A body follows the head; read what the client declared so it never sees a reset.
        let declared: usize = text
            .to_ascii_lowercase()
            .lines()
            .find_map(|line| line.strip_prefix("content-length:")?.trim().parse().ok())
            .unwrap_or(0);
        let start = text.find("\r\n\r\n").map_or(0, |at| at.saturating_add(4));
        let mut have = head.len().saturating_sub(start);
        while have < declared {
            let read = stream.read(&mut buf).expect("read body");
            assert!(read > 0, "the client closed before the body ended");
            have = have.saturating_add(read);
        }
        let _wrote =
            stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
    });
    (port, seen)
}

fn bare_builder(style: UrlStyle) -> RequestBuilder {
    let config = S3Config::new(
        Endpoint::new("http://localhost:9000").expect("endpoint"),
        BucketName::new("bucket").expect("bucket"),
        Prefix::new("").expect("a bare bucket is a prefix"),
    )
    .with_style(style)
    .with_provider(Provider::Other);
    let credentials = S3Credentials::new(
        "AKIAIOSFODNN7EXAMPLE".to_owned(),
        SecretString::new("secret".to_owned()),
    );
    RequestBuilder::new(&config, &credentials).expect("builder")
}

/// One request of every form the store sends, for the file [`FILE`] and `/` and `/0000/` listings.
fn every_form(style: UrlStyle) -> Vec<(&'static str, HttpRequest)> {
    let b = bare_builder(style);
    let now = Timestamp::from_unix_millis(1_369_353_600_000);
    let id = Txid::new(1).expect("txid");
    let key = FileKey::new(Level::ZERO, TxidRange::new(id, id).expect("range"));
    let upload = UploadId::new("abc".to_owned());
    let etags = vec!["\"d41d8cd98f00b204e9800998ecf8427e\"".to_owned()];
    let one = ReadLen::new(1).expect("len");
    let part = PartNumber::new(1).expect("part");
    vec![
        ("head", b.head(now, &key).expect("head")),
        ("ranged get", b.get_range(now, &key, 0, one).expect("get")),
        (
            "put",
            b.put_object(now, &key, b"x".to_vec(), now)
                .expect("put")
                .into(),
        ),
        (
            "create multipart",
            b.create_multipart(now, &key, now).expect("create"),
        ),
        (
            "upload part",
            b.upload_part(now, &key, &upload, part, vec![1, 2, 3])
                .expect("part")
                .into(),
        ),
        (
            "complete multipart",
            b.complete_multipart(now, &key, &upload, &etags)
                .expect("complete"),
        ),
        (
            "abort multipart",
            b.abort_multipart(now, &key, &upload).expect("abort"),
        ),
        (
            "batch delete",
            b.delete_objects(now, &[key]).expect("delete"),
        ),
        (
            "list a level",
            b.list_page(now, Level::ZERO, None, PageLimit::MAX)
                .expect("list"),
        ),
        (
            "list after a cursor",
            b.list_page(
                now,
                Level::ZERO,
                Some(TxidRange::new(id, id).expect("range")),
                PageLimit::MAX,
            )
            .expect("list"),
        ),
        (
            "list next page",
            b.list_continue(now, Level::ZERO, "t", PageLimit::MAX)
                .expect("list"),
        ),
        ("list uploads", b.list_uploads(now, None).expect("uploads")),
    ]
}

/// Sends every request form through `transport` to a socket of this process, the URL unchanged
/// but for its scheme, host and port, and returns what each one arrived as.
pub(crate) fn sent_by(transport: &impl Transport, style: UrlStyle) -> Vec<Arrived> {
    every_form(style)
        .into_iter()
        .map(|(form, request)| {
            let (port, seen) = serve_one();
            let mut url: Url = request.url().clone();
            url.set_scheme("http").expect("scheme");
            url.set_host(Some("127.0.0.1")).expect("host");
            url.set_port(Some(port)).expect("port");
            let redirected = request.with_url(url);
            let response = transport.send(redirected).expect("send");
            assert_eq!(response.status(), 200, "{form}");
            let target = seen.recv().expect("the server saw a request");
            let (path, query) = target.split_once('?').unwrap_or((&target, ""));
            Arrived {
                form,
                path: path.to_owned(),
                query: url::form_urlencoded::parse(query.as_bytes())
                    .map(|(k, v)| (k.into_owned(), v.into_owned()))
                    .collect(),
            }
        })
        .collect()
}

/// What must have arrived for a bare bucket in `style`: the empty segment kept in every
/// object path, and listings that ask for `/0000/` and `/`.
pub(crate) fn assert_bare_bucket_arrived(arrived: &[Arrived], style: UrlStyle) {
    let object_path = match style {
        UrlStyle::Path => format!("/bucket//0000/{FILE}"),
        UrlStyle::VirtualHost => format!("//0000/{FILE}"),
    };
    assert_eq!(arrived.len(), 12);
    for sent in arrived {
        match sent.form {
            "list a level" | "list after a cursor" | "list next page" => {
                assert_eq!(sent.param("prefix"), Some("/0000/"), "{sent:?}");
            }
            "list uploads" => assert_eq!(sent.param("prefix"), Some("/"), "{sent:?}"),
            "batch delete" => assert_eq!(sent.path, bucket_path(style), "{sent:?}"),
            _ => assert_eq!(sent.path, object_path, "{style:?} {sent:?}"),
        }
    }
    let cursor = arrived
        .iter()
        .find(|s| s.form == "list after a cursor")
        .expect("cursor");
    assert_eq!(
        cursor.param("start-after"),
        Some(format!("/0000/{FILE}").as_str())
    );
}

fn bucket_path(style: UrlStyle) -> &'static str {
    match style {
        UrlStyle::Path => "/bucket/",
        UrlStyle::VirtualHost => "/",
    }
}
