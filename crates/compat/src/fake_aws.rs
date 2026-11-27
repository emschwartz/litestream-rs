//! A fake of the three AWS services the credential chains call (STS, container
//! credentials and the EC2 instance metadata service) on one loopback port, so Go's SDK and
//! litestream-rs can be pointed at the same answers through the SDK's endpoint variables
//! (`AWS_ENDPOINT_URL_STS`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`,
//! `AWS_EC2_METADATA_SERVICE_ENDPOINT`).
//!
//! Each service answers as [`Answers`] says, every time, so a client that retries sees the
//! same answer again. Every key it hands out is fake and names its service (`sts-id`,
//! `container-id`, `imds-id`), so a disagreement shows which source each side took. The
//! server reads one request per connection and closes it; it understands only what the SDK
//! and litestream-rs send.

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;

use crate::error::CompatError;

/// What one service answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// Its fake credentials, valid until 2099.
    Credentials,
    /// This status with an error body in the service's format.
    Status(u16),
    /// A 200 whose body is not the service's format.
    Malformed,
    /// A 200 in the service's format without credentials (for the instance metadata
    /// service, a `Code` other than `Success`).
    Empty,
}

/// What the instance metadata service's token endpoint answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenAnswer {
    /// A token, which the other endpoints then require.
    Token,
    /// A token without the TTL header, which the SDK does not accept; the other endpoints
    /// answer without one.
    NoTtl,
    /// This status: 403, 404 and 405 send the SDK on without a token, 400 stops it.
    Status(u16),
}

/// How many roles the instance metadata service lists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Roles {
    /// None: an empty list.
    None,
    /// One, `role-a`.
    One,
    /// Two; the SDK uses the first.
    Two,
}

/// What each service answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Answers {
    /// STS `AssumeRoleWithWebIdentity`.
    pub sts: Answer,
    /// The container credentials endpoint.
    pub container: Answer,
    /// The `Authorization` the container endpoint requires, if any.
    pub container_token: Option<&'static str>,
    /// The instance metadata token.
    pub imds_token: TokenAnswer,
    /// The instance metadata role list.
    pub imds_roles: Roles,
    /// The instance metadata credentials document.
    pub imds: Answer,
}

const IMDS_TOKEN: &str = "fake-imds-token";
/// A request larger than this is not one the chains send.
const MAX_REQUEST: u64 = 64 * 1024;
const CREDS_PATH: &str = "/latest/meta-data/iam/security-credentials/";

/// The running fake; it stops when dropped.
#[derive(Debug)]
pub struct FakeAws {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FakeAws {
    /// Starts the fake on a free loopback port.
    ///
    /// # Errors
    /// The port could not be bound.
    pub fn start(answers: Answers) -> Result<Self, CompatError> {
        let listener = TcpListener::bind("127.0.0.1:0").map_err(CompatError::io("127.0.0.1:0"))?;
        let addr = listener
            .local_addr()
            .map_err(CompatError::io("127.0.0.1:0"))?;
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            for stream in listener.incoming() {
                if stopping.load(Ordering::SeqCst) {
                    break;
                }
                // A connection that fails mid-request is the client's to report.
                if let Ok(stream) = stream {
                    drop(serve(stream, &answers));
                }
            }
        });
        Ok(Self {
            addr,
            stop,
            thread: Some(thread),
        })
    }

    /// The base URL, `http://127.0.0.1:<port>`.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The container credentials URL.
    #[must_use]
    pub fn container_url(&self) -> String {
        format!("{}/container-credentials", self.url())
    }
}

impl Drop for FakeAws {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wakes the accept loop so it sees the flag; it may already be gone.
        drop(TcpStream::connect(self.addr));
        if let Some(thread) = self.thread.take() {
            drop(thread.join());
        }
    }
}

struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(have, _)| have.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

fn read_request(stream: &TcpStream) -> std::io::Result<Request> {
    let mut reader = BufReader::new(stream.take(MAX_REQUEST));
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_owned();
    let path = parts.next().unwrap_or_default().to_owned();
    let mut headers = Vec::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 || header.trim().is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.push((name.trim().to_owned(), value.trim().to_owned()));
        }
    }
    let length: u64 = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse().ok())
        .unwrap_or(0);
    let mut body = Vec::new();
    reader.take(length).read_to_end(&mut body)?;
    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

fn serve(mut stream: TcpStream, answers: &Answers) -> std::io::Result<()> {
    let request = read_request(&stream)?;
    let (status, kind, body) = route(&request, answers);
    let ttl = if request.path == "/latest/api/token" && answers.imds_token == TokenAnswer::Token {
        "X-Aws-Ec2-Metadata-Token-Ttl-Seconds: 300\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n{ttl}Connection: close\r\n\r\n",
        reason(status),
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    stream.flush()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Error",
    }
}

type Reply = (u16, &'static str, String);

fn route(request: &Request, answers: &Answers) -> Reply {
    let path = request.path.split('?').next().unwrap_or_default();
    match (request.method.as_str(), path) {
        ("POST", "/") => sts(action(&request.body), answers.sts),
        ("GET", "/container-credentials") => container(request, answers),
        ("PUT", "/latest/api/token") => match answers.imds_token {
            TokenAnswer::Token | TokenAnswer::NoTtl => (200, "text/plain", IMDS_TOKEN.to_owned()),
            TokenAnswer::Status(status) => (status, "text/plain", String::new()),
        },
        ("GET", path) if path.starts_with(CREDS_PATH) => imds(request, path, answers),
        _ => (404, "text/plain", String::new()),
    }
}

const EXPIRATION: &str = "2099-01-01T00:00:00Z";

/// The STS action a form body names: `AssumeRoleWithWebIdentity`, `AssumeRole`, ...
fn action(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .split('&')
        .find_map(|pair| pair.strip_prefix("Action="))
        .unwrap_or("AssumeRoleWithWebIdentity")
        .to_owned()
}

fn sts(action: String, answer: Answer) -> Reply {
    let xml = "text/xml";
    match answer {
        Answer::Credentials => (
            200,
            xml,
            format!(
                "<{action}Response xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
                 <{action}Result><Credentials>\
                 <AccessKeyId>sts-id</AccessKeyId><SecretAccessKey>sts-secret</SecretAccessKey>\
                 <SessionToken>sts-token</SessionToken><Expiration>{EXPIRATION}</Expiration>\
                 </Credentials></{action}Result>\
                 <ResponseMetadata><RequestId>fake</RequestId></ResponseMetadata>\
                 </{action}Response>"
            ),
        ),
        Answer::Status(status) => (
            status,
            xml,
            "<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code>\
             <Message>fake refusal</Message></Error><RequestId>fake</RequestId></ErrorResponse>"
                .to_owned(),
        ),
        Answer::Malformed => (200, xml, "not xml".to_owned()),
        Answer::Empty => (
            200,
            xml,
            format!("<{action}Response><{action}Result></{action}Result></{action}Response>"),
        ),
    }
}

fn document(prefix: &str, token_key: &str) -> String {
    serde_json::json!({
        "Code": "Success",
        "Type": "AWS-HMAC",
        "AccessKeyId": format!("{prefix}-id"),
        "SecretAccessKey": format!("{prefix}-secret"),
        token_key: format!("{prefix}-token"),
        "Expiration": EXPIRATION,
    })
    .to_string()
}

fn json_answer(answer: Answer, prefix: &str, token_key: &str, empty: &str) -> Reply {
    let json = "application/json";
    match answer {
        Answer::Credentials => (200, json, document(prefix, token_key)),
        Answer::Status(status) => (
            status,
            json,
            "{\"code\":\"Refused\",\"message\":\"fake refusal\"}".to_owned(),
        ),
        Answer::Malformed => (200, json, "not json".to_owned()),
        Answer::Empty => (200, json, empty.to_owned()),
    }
}

fn container(request: &Request, answers: &Answers) -> Reply {
    if let Some(token) = answers.container_token
        && request.header("authorization") != Some(token)
    {
        return (
            401,
            "application/json",
            "{\"code\":\"Unauthorized\"}".to_owned(),
        );
    }
    json_answer(answers.container, "container", "Token", "{}")
}

fn imds(request: &Request, path: &str, answers: &Answers) -> Reply {
    if answers.imds_token == TokenAnswer::Token
        && request.header("x-aws-ec2-metadata-token") != Some(IMDS_TOKEN)
    {
        return (401, "text/plain", String::new());
    }
    let role = path.trim_start_matches(CREDS_PATH);
    if role.is_empty() {
        let list = match answers.imds_roles {
            Roles::None => "",
            Roles::One => "role-a",
            Roles::Two => "role-a\nrole-b",
        };
        return (200, "text/plain", list.to_owned());
    }
    if role != "role-a" {
        return (404, "text/plain", String::new());
    }
    json_answer(
        answers.imds,
        "imds",
        "Token",
        "{\"Code\":\"AssumeRoleUnauthorizedAccess\",\"Message\":\"fake\"}",
    )
}
