//! The fetching sources and the profile checks, over a scripted transport and a fake
//! clock. The differential against Go's SDK is `lsr-compat`'s `dropin_roles`; these tests
//! pin each refusal and each fallback without Go.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lsr_core::FakeClock;
use lsr_ltx::Timestamp;
use tempfile::TempDir;

use super::{CredentialChain, CredentialError, Resolved, Source, SourceError};
use crate::request::{HttpRequest, Method};
use crate::transport::{HttpResponse, Transport, TransportError};

type Sent = (Method, String, Vec<(&'static str, String)>);
type Script = dyn Fn(&HttpRequest) -> Result<HttpResponse, TransportError> + Send + Sync;

/// A transport that answers by a function and records what it was sent.
struct Scripted {
    answer: Box<Script>,
    sent: Mutex<Vec<Sent>>,
}

impl Transport for Scripted {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        self.sent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((
                request.method(),
                request.url().as_str().to_owned(),
                request.headers().to_vec(),
            ));
        (self.answer)(&request)
    }
}

fn scripted(
    answer: impl Fn(&HttpRequest) -> Result<HttpResponse, TransportError> + Send + Sync + 'static,
) -> Arc<Scripted> {
    Arc::new(Scripted {
        answer: Box::new(answer),
        sent: Mutex::new(Vec::new()),
    })
}

fn ok(body: &str) -> Result<HttpResponse, TransportError> {
    Ok(HttpResponse::new(
        200,
        Vec::new(),
        body.as_bytes().to_vec(),
        true,
    ))
}

fn status(code: u16) -> Result<HttpResponse, TransportError> {
    Ok(HttpResponse::new(code, Vec::new(), Vec::new(), true))
}

fn token(ttl: bool) -> Result<HttpResponse, TransportError> {
    let headers = if ttl {
        vec![(
            "X-Aws-Ec2-Metadata-Token-Ttl-Seconds".to_owned(),
            "300".to_owned(),
        )]
    } else {
        Vec::new()
    };
    Ok(HttpResponse::new(200, headers, b"tok".to_vec(), true))
}

const NOW_MS: i64 = 1_700_000_000_000;
const DOC: &str = r#"{"Code":"Success","AccessKeyId":"id","SecretAccessKey":"secret","Token":"tok","Expiration":"2099-01-01T00:00:00Z"}"#;

fn chain_over(vars: &[(&str, &str)], transport: Arc<Scripted>) -> CredentialChain {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect();
    let clock = Arc::new(FakeClock::new(Timestamp::from_unix_millis(NOW_MS)));
    CredentialChain::with_env(move |name| vars.get(name).cloned())
        .with_transport(transport, clock)
        .with_region("us-east-1")
}

fn imds_vars() -> Vec<(&'static str, &'static str)> {
    vec![
        ("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:9"),
        ("AWS_CONFIG_FILE", "/nonexistent/config"),
        ("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials"),
    ]
}

fn key_id(resolved: &Resolved) -> String {
    resolved.credentials().to_signer().key().to_owned()
}

fn source_error(err: CredentialError) -> SourceError {
    match err {
        CredentialError::Source(source) => source,
        other @ (CredentialError::Missing(_)
        | CredentialError::ProfileNotFound(_)
        | CredentialError::PartialProfile { .. }
        | CredentialError::MalformedLine { .. }
        | CredentialError::NotBuilt { .. }
        | CredentialError::ProfileInvalid { .. }
        | CredentialError::ReadFailed { .. }
        | CredentialError::FileTooLarge { .. }) => panic!("expected a source error, got {other:?}"),
    }
}

/// The instance metadata service answers by path, with a token required when `v2`.
fn imds_service(token_answer: fn() -> Result<HttpResponse, TransportError>) -> Arc<Scripted> {
    scripted(move |request| match request.url().path() {
        "/latest/api/token" => token_answer(),
        "/latest/meta-data/iam/security-credentials/" => ok("role-a\nrole-b"),
        "/latest/meta-data/iam/security-credentials/role-a" => ok(DOC),
        _ => status(404),
    })
}

#[test]
fn instance_metadata_uses_a_token_the_first_role_and_caps_the_expiry() {
    let http = imds_service(|| token(true));
    let resolved = chain_over(&imds_vars(), Arc::clone(&http))
        .resolve()
        .unwrap();
    assert_eq!(key_id(&resolved), "id");
    assert_eq!(resolved.source(), &Source::Remote("instance metadata"));
    let cap = Timestamp::from_unix_millis(NOW_MS).saturating_add(Duration::from_secs(3600));
    assert_eq!(resolved.expiring().expires(), Some(cap));
    assert!(resolved.refresh().is_some());
    let sent = http.sent.lock().unwrap();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[0].0, Method::Put);
    assert!(
        sent[0]
            .2
            .contains(&("x-aws-ec2-metadata-token-ttl-seconds", "300".to_owned()))
    );
    assert!(sent[2].1.ends_with("/role-a"));
    assert!(
        sent[2]
            .2
            .contains(&("x-aws-ec2-metadata-token", "tok".to_owned()))
    );
}

#[test]
fn instance_metadata_falls_back_without_a_token() {
    let answers: [fn() -> Result<HttpResponse, TransportError>; 6] = [
        || status(403),
        || status(404),
        || status(405),
        || status(401),
        || token(false),
        || Err(TransportError::Connect),
    ];
    for answer in answers {
        let http = imds_service(answer);
        let resolved = chain_over(&imds_vars(), Arc::clone(&http))
            .resolve()
            .unwrap();
        assert_eq!(key_id(&resolved), "id");
        let sent = http.sent.lock().unwrap();
        assert!(sent[1].2.is_empty(), "asked without a token: {:?}", sent[1]);
    }
}

#[test]
fn instance_metadata_stops_on_400_and_when_v1_is_disabled() {
    let err = chain_over(&imds_vars(), imds_service(|| status(400)))
        .resolve()
        .unwrap_err();
    assert!(matches!(
        source_error(err),
        SourceError::Status { status: 400, .. }
    ));
    let mut vars = imds_vars();
    vars.push(("AWS_EC2_METADATA_V1_DISABLED", "true"));
    let err = chain_over(&vars, imds_service(|| status(403)))
        .resolve()
        .unwrap_err();
    assert!(matches!(
        source_error(err),
        SourceError::Status { status: 403, .. }
    ));
}

#[test]
fn instance_metadata_refuses_an_empty_role_list_and_a_failed_code() {
    let empty = scripted(|request| match request.url().path() {
        "/latest/api/token" => token(true),
        _ => ok(""),
    });
    let err = chain_over(&imds_vars(), empty).resolve().unwrap_err();
    assert!(matches!(source_error(err), SourceError::Config { .. }));
    let failed = scripted(|request| match request.url().path() {
        "/latest/api/token" => token(true),
        "/latest/meta-data/iam/security-credentials/" => ok("role-a"),
        _ => ok(r#"{"Code":"AssumeRoleUnauthorizedAccess"}"#),
    });
    let err = chain_over(&imds_vars(), failed).resolve().unwrap_err();
    assert!(matches!(source_error(err), SourceError::Refused { .. }));
}

#[test]
fn instance_metadata_off_or_no_client_ends_the_chain() {
    let mut vars = imds_vars();
    vars.push(("AWS_EC2_METADATA_DISABLED", "TRUE"));
    let http = imds_service(|| token(true));
    let err = chain_over(&vars, Arc::clone(&http)).resolve().unwrap_err();
    assert!(matches!(err, CredentialError::Missing(_)));
    assert!(http.sent.lock().unwrap().is_empty());
    let vars: HashMap<String, String> = imds_vars()
        .into_iter()
        .map(|(n, v)| (n.to_owned(), v.to_owned()))
        .collect();
    let err = CredentialChain::with_env(move |name| vars.get(name).cloned())
        .resolve()
        .unwrap_err();
    assert!(matches!(err, CredentialError::Missing(_)));
}

#[test]
fn a_malformed_answer_is_refused() {
    let http = scripted(|request| match request.url().path() {
        "/latest/api/token" => token(true),
        "/latest/meta-data/iam/security-credentials/" => ok("role-a"),
        _ => ok("not json"),
    });
    let err = chain_over(&imds_vars(), http).resolve().unwrap_err();
    assert!(matches!(source_error(err), SourceError::Malformed { .. }));
}

fn container_vars(extra: &[(&'static str, &'static str)]) -> Vec<(&'static str, &'static str)> {
    let mut vars = imds_vars();
    vars.push(("AWS_EC2_METADATA_DISABLED", "true"));
    vars.extend_from_slice(extra);
    vars
}

#[test]
fn container_credentials_send_the_token_file_over_the_token() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("token");
    fs::write(&file, "from-file").unwrap();
    let file = file.to_str().unwrap().to_owned();
    let http = scripted(|request| {
        if request.header("authorization") == Some("from-file") {
            ok(DOC)
        } else {
            status(401)
        }
    });
    let file: &'static str = Box::leak(file.into_boxed_str());
    let vars = container_vars(&[
        (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://127.0.0.1:9/creds",
        ),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "from-env"),
        ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", file),
    ]);
    let resolved = chain_over(&vars, http).resolve().unwrap();
    assert_eq!(resolved.source(), &Source::Remote("container"));
    assert_eq!(key_id(&resolved), "id");
}

#[test]
fn container_relative_uri_is_under_the_ecs_host() {
    let http = scripted(|_| ok(DOC));
    let vars = container_vars(&[("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds")]);
    chain_over(&vars, Arc::clone(&http)).resolve().unwrap();
    assert_eq!(
        http.sent.lock().unwrap()[0].1,
        "http://169.254.170.2/v2/creds"
    );
}

#[test]
fn container_host_must_be_loopback_or_a_container_address() {
    for (uri, allowed) in [
        ("http://192.0.2.1/creds", false),
        ("http://169.254.170.23/creds", true),
        ("http://[::1]:9/creds", true),
        ("https://192.0.2.1/creds", true),
        ("not a url", false),
    ] {
        let http = scripted(|_| ok(DOC));
        let vars = container_vars(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", uri)]);
        let result = chain_over(&vars, Arc::clone(&http)).resolve();
        assert_eq!(result.is_ok(), allowed, "{uri}: {result:?}");
        if !allowed {
            assert!(
                http.sent.lock().unwrap().is_empty(),
                "{uri} must not be asked"
            );
        }
    }
}

#[test]
fn container_status_carries_its_code() {
    let http = scripted(|_| {
        Ok(HttpResponse::new(
            403,
            Vec::new(),
            br#"{"code":"AccessDenied"}"#.to_vec(),
            true,
        ))
    });
    let vars = container_vars(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:9/c")]);
    let err = source_error(chain_over(&vars, http).resolve().unwrap_err());
    assert!(
        matches!(&err, SourceError::Status { status: 403, code, .. } if code == "AccessDenied"),
        "{err:?}"
    );
    assert!(!err.transient());
}

const STS_OK: &str = "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
    <AccessKeyId>sts-id</AccessKeyId><SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
    <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult>\
    </AssumeRoleWithWebIdentityResponse>";

fn token_file(dir: &TempDir) -> &'static str {
    let path = dir.path().join("web-token");
    fs::write(&path, "jwt").unwrap();
    Box::leak(path.to_str().unwrap().to_owned().into_boxed_str())
}

#[test]
fn web_identity_posts_the_token_to_the_sts_endpoint() {
    let dir = TempDir::new().unwrap();
    let file = token_file(&dir);
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&bodies);
    let http = scripted(move |request| {
        seen.lock()
            .unwrap()
            .push(String::from_utf8_lossy(request.body()).into_owned());
        ok(STS_OK)
    });
    let vars = container_vars(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", file),
        ("AWS_ROLE_ARN", "arn:aws:iam::0:role/r"),
        ("AWS_ENDPOINT_URL", "http://127.0.0.1:9"),
    ]);
    let resolved = chain_over(&vars, Arc::clone(&http)).resolve().unwrap();
    assert_eq!(key_id(&resolved), "sts-id");
    assert_eq!(http.sent.lock().unwrap()[0].1, "http://127.0.0.1:9/");
    let body = bodies.lock().unwrap()[0].clone();
    assert!(body.contains("Action=AssumeRoleWithWebIdentity"), "{body}");
    assert!(body.contains("WebIdentityToken=jwt"), "{body}");
    assert!(
        body.contains(&format!(
            "RoleSessionName={}",
            i128::from(NOW_MS) * 1_000_000
        )),
        "{body}"
    );
}

#[test]
fn web_identity_without_a_role_or_a_file_is_refused() {
    let dir = TempDir::new().unwrap();
    let file = token_file(&dir);
    let http = scripted(|_| ok(STS_OK));
    let vars = container_vars(&[("AWS_WEB_IDENTITY_TOKEN_FILE", file)]);
    let err = source_error(chain_over(&vars, Arc::clone(&http)).resolve().unwrap_err());
    assert!(matches!(&err, SourceError::Config { why, .. } if why == "role ARN is not set"));
    let vars = container_vars(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", "/nonexistent/token"),
        ("AWS_ROLE_ARN", "arn:aws:iam::0:role/r"),
    ]);
    let err = source_error(chain_over(&vars, Arc::clone(&http)).resolve().unwrap_err());
    assert!(matches!(err, SourceError::Config { .. }));
    assert!(http.sent.lock().unwrap().is_empty());
}

#[test]
fn sts_refusal_carries_its_code() {
    let dir = TempDir::new().unwrap();
    let file = token_file(&dir);
    let http = scripted(|_| {
        Ok(HttpResponse::new(
            400,
            Vec::new(),
            b"<ErrorResponse><Error><Code>InvalidIdentityToken</Code></Error></ErrorResponse>"
                .to_vec(),
            true,
        ))
    });
    let vars = container_vars(&[
        ("AWS_WEB_IDENTITY_TOKEN_FILE", file),
        ("AWS_ROLE_ARN", "arn:aws:iam::0:role/r"),
    ]);
    let err = source_error(chain_over(&vars, http).resolve().unwrap_err());
    assert!(
        matches!(&err, SourceError::Status { status: 400, code, .. } if code == "InvalidIdentityToken"),
        "{err:?}"
    );
}

fn config(dir: &TempDir, text: &str) -> PathBuf {
    let path = dir.path().join("config");
    fs::write(&path, text).unwrap();
    path
}

fn resolve_profile(path: &Path) -> Result<Resolved, CredentialError> {
    let path: &'static str = Box::leak(path.to_str().unwrap().to_owned().into_boxed_str());
    let vars = vec![
        ("AWS_CONFIG_FILE", path),
        ("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/credentials"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ];
    chain_over(&vars, scripted(|_| status(500))).resolve()
}

#[test]
fn profiles_failing_the_sdk_load_checks_are_refused() {
    let dir = TempDir::new().unwrap();
    for text in [
        "[default]\nrole_arn = r\ncredential_process = true\nweb_identity_token_file = /t\n",
        "[default]\nweb_identity_token_file = /t\n",
        "[default]\nsource_profile = base\n",
        "[default]\ncredential_source = Environment\n",
    ] {
        let err = resolve_profile(&config(&dir, text)).unwrap_err();
        assert!(
            matches!(err, CredentialError::ProfileInvalid { .. }),
            "{text}: {err:?}"
        );
    }
}

#[test]
fn profiles_litestream_rs_does_not_build_are_named() {
    let dir = TempDir::new().unwrap();
    for text in [
        "[default]\nrole_arn = r\nsource_profile = base\n",
        "[default]\nrole_arn = r\ncredential_source = Ec2InstanceMetadata\n",
        "[default]\nsso_session = s\n",
        "[default]\nsso_start_url = https://x.invalid\n",
        "[default]\nlogin_session = s\n",
    ] {
        let err = resolve_profile(&config(&dir, text)).unwrap_err();
        assert!(
            matches!(err, CredentialError::NotBuilt { .. }),
            "{text}: {err:?}"
        );
    }
}

#[test]
fn static_keys_in_the_profile_come_before_a_process() {
    let dir = TempDir::new().unwrap();
    let text = "[default]\naws_access_key_id = file-id\naws_secret_access_key = s\ncredential_process = exit 1\n";
    let resolved = resolve_profile(&config(&dir, text)).unwrap();
    assert_eq!(key_id(&resolved), "file-id");
    assert!(resolved.refresh().is_none());
}

#[test]
fn credential_process_output_is_checked() {
    let dir = TempDir::new().unwrap();
    let keys = r#"printf '{"Version":1,"AccessKeyId":"proc-id","SecretAccessKey":"s","SessionToken":"t","Expiration":"2099-01-01T00:00:00Z"}'"#;
    let resolved = resolve_profile(&config(
        &dir,
        &format!("[default]\ncredential_process = {keys}\n"),
    ))
    .unwrap();
    assert_eq!(key_id(&resolved), "proc-id");
    assert_eq!(resolved.source(), &Source::Remote("credential_process"));
    assert!(resolved.expiring().expires().is_some());
    for command in [
        r#"printf '{"Version":2,"AccessKeyId":"a","SecretAccessKey":"s"}'"#,
        r#"printf '{"Version":1}'"#,
        "printf 'not json'",
        "exit 3",
    ] {
        let err = resolve_profile(&config(
            &dir,
            &format!("[default]\ncredential_process = {command}\n"),
        ))
        .unwrap_err();
        assert!(
            matches!(source_error(err), SourceError::Config { .. }),
            "{command}"
        );
    }
}

#[test]
fn only_outages_and_server_errors_are_transient() {
    let p = "test";
    assert!(
        SourceError::Unreachable {
            provider: p,
            why: String::new()
        }
        .transient()
    );
    for (status, transient) in [
        (500, true),
        (503, true),
        (429, true),
        (403, false),
        (400, false),
    ] {
        let err = SourceError::Status {
            provider: p,
            status,
            code: String::new(),
        };
        assert_eq!(err.transient(), transient, "{status}");
    }
    assert!(!SourceError::Malformed { provider: p }.transient());
    assert!(
        !SourceError::Refused {
            provider: p,
            code: String::new()
        }
        .transient()
    );
    assert!(
        !SourceError::Config {
            provider: p,
            why: String::new()
        }
        .transient()
    );
}

#[test]
fn environment_keys_come_before_web_identity_and_web_identity_before_the_host() {
    let dir = TempDir::new().unwrap();
    let file = token_file(&dir);
    let http = scripted(|request| match request.url().path() {
        "/" => ok(STS_OK),
        _ => ok(DOC),
    });
    let web = [
        ("AWS_WEB_IDENTITY_TOKEN_FILE", file),
        ("AWS_ROLE_ARN", "arn:aws:iam::0:role/r"),
        ("AWS_ENDPOINT_URL_STS", "http://127.0.0.1:9"),
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:9/c"),
    ];
    let mut vars = imds_vars();
    vars.extend_from_slice(&web);
    let resolved = chain_over(&vars, Arc::clone(&http)).resolve().unwrap();
    assert_eq!(key_id(&resolved), "sts-id");
    vars.push(("AWS_ACCESS_KEY_ID", "env-id"));
    vars.push(("AWS_SECRET_ACCESS_KEY", "env-secret"));
    let resolved = chain_over(&vars, Arc::clone(&http)).resolve().unwrap();
    assert_eq!(key_id(&resolved), "env-id");
    assert_eq!(resolved.source(), &Source::Environment);
    assert!(resolved.refresh().is_none());
}
