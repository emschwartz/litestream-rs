//! The credential sources that need a network call: web identity (STS
//! `AssumeRoleWithWebIdentity`), container credentials (ECS and EKS pod identity) and the EC2
//! instance metadata service, as the AWS SDK for Go v2 (`credentials` v1.19.6,
//! `feature/ec2/imds` v1.18.16) calls them.
//!
//! - **Web identity.** The token file is read whole on every fetch and posted, unsigned, with
//!   the role ARN and a session name (the time in nanoseconds when none is configured), to
//!   `AWS_ENDPOINT_URL_STS`, else `AWS_ENDPOINT_URL`, else `https://sts.<region>.amazonaws.com`.
//! - **Container.** `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` under `http://169.254.170.2`, else
//!   `AWS_CONTAINER_CREDENTIALS_FULL_URI`, which over plain HTTP must name a loopback, ECS or EKS
//!   host. The `Authorization` header is the token file's contents
//!   (`AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`), else `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
//! - **Instance metadata.** `AWS_EC2_METADATA_SERVICE_ENDPOINT`, else the IPv4 or IPv6 address
//!   by `AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE`. A 5-minute session token first; any failure
//!   to get one (a status, no answer, an answer without a TTL) falls back to asking without
//!   one unless `AWS_EC2_METADATA_V1_DISABLED` is true, except a 400, which stops. The first role listed is used,
//!   its `Code` must be `Success`, and the expiry is capped at an hour from now.
//!
//! JSON keys are matched as Go's `encoding/json` matches them: exactly, else ignoring case.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs as _};
use std::sync::Arc;
use std::time::Duration;

use lsr_core::Clock;

use lsr_ltx::Timestamp;
use url::Url;

use super::{CredentialError, SourceError};
use crate::credentials::{S3Credentials, SecretString};
use crate::request::{HttpRequest, Method};
use crate::response::xml_text;
use crate::source::{CredentialSource, Expiring};
use crate::transport::{HttpResponse, Transport};

/// The ECS container credentials host.
const ECS_HOST: &str = "http://169.254.170.2";
/// The instance metadata service, by address family.
const IMDS_V4: &str = "http://169.254.169.254";
const IMDS_V6: &str = "http://[fd00:ec2::254]";
/// The session token's lifetime the SDK asks for.
const IMDS_TOKEN_TTL: &str = "300";
const IMDS_CREDS: &str = "/latest/meta-data/iam/security-credentials/";
/// The longest the SDK keeps instance-metadata credentials.
const IMDS_MAX_LIFE: Duration = Duration::from_secs(3600);

/// The HTTP client and clock the sources share; its `Debug` names nothing about them.
#[derive(Clone)]
pub(crate) struct Http {
    pub(crate) transport: Arc<dyn Transport>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for Http {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Http")
    }
}

impl Http {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, crate::transport::TransportError> {
        self.transport.send(request)
    }

    fn now(&self) -> Timestamp {
        self.clock.now()
    }
}

fn send(
    http: &Http,
    source: &'static str,
    request: HttpRequest,
) -> Result<HttpResponse, CredentialError> {
    http.send(request).map_err(|err| {
        CredentialError::Source(SourceError::Unreachable {
            provider: source,
            why: err.to_string(),
        })
    })
}

fn source_error(error: SourceError) -> CredentialError {
    CredentialError::Source(error)
}

fn parse_url(source: &'static str, text: &str) -> Result<Url, CredentialError> {
    Url::parse(text).map_err(|_unparsed| {
        source_error(SourceError::Config {
            provider: source,
            why: format!("{text:?} is not a URL"),
        })
    })
}

/// STS `AssumeRoleWithWebIdentity` with a token file.
#[derive(Debug)]
pub(crate) struct WebIdentity {
    pub(crate) http: Http,
    pub(crate) endpoint: String,
    pub(crate) token_file: String,
    pub(crate) role_arn: String,
    pub(crate) session_name: Option<String>,
}

const WEB_IDENTITY: &str = "web identity";

impl CredentialSource for WebIdentity {
    fn fetch(&self) -> Result<Expiring, CredentialError> {
        let token = std::fs::read(&self.token_file).map_err(|err| {
            source_error(SourceError::Config {
                provider: WEB_IDENTITY,
                why: format!(
                    "failed to read token file {}: {}",
                    self.token_file,
                    err.kind()
                ),
            })
        })?;
        let token = String::from_utf8_lossy(&token);
        let session = self
            .session_name
            .clone()
            .unwrap_or_else(|| nanos(self.http.now()));
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("Action", "AssumeRoleWithWebIdentity")
            .append_pair("RoleArn", &self.role_arn)
            .append_pair("RoleSessionName", &session)
            .append_pair("Version", "2011-06-15")
            .append_pair("WebIdentityToken", &token)
            .finish();
        let url = parse_url(WEB_IDENTITY, &self.endpoint)?;
        let headers = vec![(
            "content-type",
            "application/x-www-form-urlencoded; charset=utf-8".to_owned(),
        )];
        let request = HttpRequest::new(Method::Post, url, headers, body.into_bytes());
        let response = send(&self.http, WEB_IDENTITY, request)?;
        sts_credentials(WEB_IDENTITY, &response)
    }

    fn name(&self) -> &'static str {
        WEB_IDENTITY
    }
}

/// The credentials in an STS answer, or its error code.
pub(crate) fn sts_credentials(
    source: &'static str,
    response: &HttpResponse,
) -> Result<Expiring, CredentialError> {
    let xml = String::from_utf8_lossy(response.body());
    let one = |tag: &str| xml_text(&xml, tag).into_iter().next();
    if response.status() != 200 {
        return Err(status_error(source, response.status(), one("Code")));
    }
    let (Some(id), Some(secret)) = (one("AccessKeyId"), one("SecretAccessKey")) else {
        return Err(source_error(SourceError::Malformed { provider: source }));
    };
    let mut credentials = S3Credentials::new(id, SecretString::new(secret));
    if let Some(token) = one("SessionToken").filter(|t| !t.is_empty()) {
        credentials = credentials.with_session_token(SecretString::new(token));
    }
    match one("Expiration").and_then(|text| parse_time(&text)) {
        Some(expires) => Ok(Expiring::until(credentials, expires)),
        None => Err(source_error(SourceError::Malformed { provider: source })),
    }
}

fn status_error(source: &'static str, status: u16, code: Option<String>) -> CredentialError {
    source_error(SourceError::Status {
        provider: source,
        status,
        code: code.unwrap_or_default(),
    })
}

/// The time in nanoseconds, the SDK's session name when none is configured (to the
/// millisecond here; the name only labels the session).
fn nanos(at: Timestamp) -> String {
    i128::from(at.unix_millis())
        .saturating_mul(1_000_000)
        .to_string()
}

/// An RFC 3339 time, as Go's `time.Time` reads one from JSON or XML.
pub(crate) fn parse_time(text: &str) -> Option<Timestamp> {
    let at: jiff::Timestamp = text.trim().parse().ok()?;
    Some(Timestamp::from_unix_millis(at.as_millisecond()))
}

/// ECS and EKS container credentials.
#[derive(Debug)]
pub(crate) struct Container {
    pub(crate) http: Http,
    pub(crate) url: String,
    pub(crate) token: Option<String>,
    pub(crate) token_file: Option<String>,
}

const CONTAINER: &str = "container";

impl Container {
    /// The URI the environment names; `None` when it names none.
    pub(crate) fn uri(
        relative: Option<String>,
        full: Option<String>,
    ) -> Option<Result<String, CredentialError>> {
        if let Some(path) = relative {
            return Some(Ok(format!("{ECS_HOST}{path}")));
        }
        full.map(|text| allowed(&text).map(|()| text))
    }

    fn authorization(&self) -> Result<Option<String>, CredentialError> {
        match &self.token_file {
            Some(path) => std::fs::read(path)
                .map(|bytes| Some(String::from_utf8_lossy(&bytes).into_owned()))
                .map_err(|err| {
                    source_error(SourceError::Config {
                        provider: CONTAINER,
                        why: format!(
                            "failed to read authorization token from {path}: {}",
                            err.kind()
                        ),
                    })
                }),
            None => Ok(self.token.clone()),
        }
    }
}

impl CredentialSource for Container {
    fn fetch(&self) -> Result<Expiring, CredentialError> {
        let url = parse_url(CONTAINER, &self.url)?;
        let mut headers = vec![("accept", "application/json".to_owned())];
        if let Some(token) = self.authorization()?.filter(|t| !t.is_empty()) {
            headers.push(("authorization", token));
        }
        let response = send(
            &self.http,
            CONTAINER,
            HttpRequest::new(Method::Get, url, headers, Vec::new()),
        )?;
        if !(200..300).contains(&response.status()) {
            let code = json(&response).and_then(|v| field(&v, "code"));
            return Err(status_error(CONTAINER, response.status(), code));
        }
        json_credentials(CONTAINER, &response, "Token", None)
    }

    fn name(&self) -> &'static str {
        CONTAINER
    }
}

/// Go's `resolveLocalHTTPCredProvider` host check: over plain HTTP, every address of the host
/// must be loopback or a container credentials address.
fn allowed(text: &str) -> Result<(), CredentialError> {
    let refuse = |why: String| {
        source_error(SourceError::Config {
            provider: CONTAINER,
            why,
        })
    };
    let url = Url::parse(text).map_err(|_unparsed| refuse(format!("invalid URL {text:?}")))?;
    let host = url.host_str().filter(|h| !h.is_empty()).ok_or_else(|| {
        refuse("unable to parse host from local HTTP cred provider URL".to_owned())
    })?;
    if url.scheme() != "http" {
        return Ok(());
    }
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    let addresses: Vec<IpAddr> = match bare.parse::<IpAddr>() {
        Ok(ip) => vec![ip],
        Err(_) => (bare, 0)
            .to_socket_addrs()
            .map_err(|err| refuse(format!("failed to resolve host {host:?}, {err}")))?
            .map(|addr| addr.ip())
            .collect(),
    };
    if !addresses.is_empty() && addresses.iter().all(|ip| ip_allowed(*ip)) {
        Ok(())
    } else {
        Err(refuse(format!(
            "invalid endpoint host, {host:?}, only loopback/ecs/eks hosts are allowed"
        )))
    }
}

fn ip_allowed(ip: IpAddr) -> bool {
    const ECS: Ipv4Addr = Ipv4Addr::new(169, 254, 170, 2);
    const EKS: Ipv4Addr = Ipv4Addr::new(169, 254, 170, 23);
    const EKS_V6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x23);
    ip.is_loopback() || ip == IpAddr::V4(ECS) || ip == IpAddr::V4(EKS) || ip == IpAddr::V6(EKS_V6)
}

/// The EC2 instance metadata service.
#[derive(Debug)]
pub(crate) struct InstanceMetadata {
    pub(crate) http: Http,
    pub(crate) endpoint: String,
    pub(crate) v1_disabled: bool,
}

const IMDS: &str = "instance metadata";

impl InstanceMetadata {
    /// The endpoint the environment names.
    pub(crate) fn endpoint(explicit: Option<String>, mode: Option<String>) -> String {
        explicit.unwrap_or_else(|| {
            if mode.is_some_and(|m| m.eq_ignore_ascii_case("ipv6")) {
                IMDS_V6.to_owned()
            } else {
                IMDS_V4.to_owned()
            }
        })
    }

    fn url(&self, path: &str) -> Result<Url, CredentialError> {
        parse_url(
            IMDS,
            &format!("{}{path}", self.endpoint.trim_end_matches('/')),
        )
    }

    /// A session token; `None` to ask without one (the SDK's fallback).
    fn token(&self) -> Result<Option<String>, CredentialError> {
        let headers = vec![(
            "x-aws-ec2-metadata-token-ttl-seconds",
            IMDS_TOKEN_TTL.to_owned(),
        )];
        let request = HttpRequest::new(
            Method::Put,
            self.url("/latest/api/token")?,
            headers,
            Vec::new(),
        );
        let fallback = |error: CredentialError| {
            if self.v1_disabled {
                Err(error)
            } else {
                Ok(None)
            }
        };
        match self.http.send(request) {
            // Go's `getToken` refuses a token whose answer has no TTL it can parse, and that
            // failure falls back like any other.
            Ok(response)
                if response.status() == 200
                    && response
                        .header("x-aws-ec2-metadata-token-ttl-seconds")
                        .is_some_and(|ttl| ttl.trim().parse::<i64>().is_ok()) =>
            {
                Ok(Some(String::from_utf8_lossy(response.body()).into_owned()))
            }
            Ok(response) if response.status() == 400 => {
                Err(status_error(IMDS, response.status(), None))
            }
            Ok(response) => fallback(status_error(IMDS, response.status(), None)),
            Err(err) => fallback(source_error(SourceError::Unreachable {
                provider: IMDS,
                why: err.to_string(),
            })),
        }
    }

    fn get(&self, path: &str, token: Option<&str>) -> Result<HttpResponse, CredentialError> {
        let headers = token
            .map(|t| vec![("x-aws-ec2-metadata-token", t.to_owned())])
            .unwrap_or_default();
        let response = send(
            &self.http,
            IMDS,
            HttpRequest::new(Method::Get, self.url(path)?, headers, Vec::new()),
        )?;
        if response.status() == 200 {
            Ok(response)
        } else {
            Err(status_error(IMDS, response.status(), None))
        }
    }
}

impl CredentialSource for InstanceMetadata {
    fn fetch(&self) -> Result<Expiring, CredentialError> {
        let token = self.token()?;
        let list = self.get(IMDS_CREDS, token.as_deref())?;
        let roles = String::from_utf8_lossy(list.body()).into_owned();
        let Some(role) = roles.lines().next() else {
            return Err(source_error(SourceError::Config {
                provider: IMDS,
                why: "unexpected empty EC2 IMDS role list".to_owned(),
            }));
        };
        let creds = self.get(&format!("{IMDS_CREDS}{role}"), token.as_deref())?;
        let document =
            json(&creds).ok_or(source_error(SourceError::Malformed { provider: IMDS }))?;
        let code = field(&document, "Code").unwrap_or_default();
        if !code.eq_ignore_ascii_case("Success") {
            return Err(source_error(SourceError::Refused {
                provider: IMDS,
                code,
            }));
        }
        let cap = self.http.now().saturating_add(IMDS_MAX_LIFE);
        json_credentials(IMDS, &creds, "Token", Some(cap))
    }

    fn name(&self) -> &'static str {
        IMDS
    }
}

pub(crate) fn json(response: &HttpResponse) -> Option<serde_json::Value> {
    serde_json::from_slice(response.body()).ok()
}

/// A string field, matched as Go's `encoding/json` matches it: exactly, else ignoring case.
pub(crate) fn field(value: &serde_json::Value, name: &str) -> Option<String> {
    let object = value.as_object()?;
    let found = object.get(name).or_else(|| {
        object
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })?;
    found.as_str().map(str::to_owned)
}

/// Credentials from a JSON document with `AccessKeyId`, `SecretAccessKey`, the session token
/// under `token_key`, and an optional `Expiration`, capped at `cap`.
pub(crate) fn json_credentials(
    source: &'static str,
    response: &HttpResponse,
    token_key: &str,
    cap: Option<Timestamp>,
) -> Result<Expiring, CredentialError> {
    let malformed = || source_error(SourceError::Malformed { provider: source });
    let value = json(response).ok_or_else(malformed)?;
    document_credentials(source, &value, token_key, cap).ok_or_else(malformed)
}

pub(crate) fn document_credentials(
    _source: &'static str,
    value: &serde_json::Value,
    token_key: &str,
    cap: Option<Timestamp>,
) -> Option<Expiring> {
    let id = field(value, "AccessKeyId")?;
    let secret = field(value, "SecretAccessKey")?;
    let mut credentials = S3Credentials::new(id, SecretString::new(secret));
    if let Some(token) = field(value, token_key).filter(|t| !t.is_empty()) {
        credentials = credentials.with_session_token(SecretString::new(token));
    }
    let expires = match field(value, "Expiration") {
        Some(text) => Some(parse_time(&text)?),
        None => None,
    };
    Some(match (expires, cap) {
        (Some(at), Some(cap)) => Expiring::until(credentials, at.min(cap)),
        (Some(at), None) => Expiring::until(credentials, at),
        (None, _) => Expiring::forever(credentials),
    })
}
