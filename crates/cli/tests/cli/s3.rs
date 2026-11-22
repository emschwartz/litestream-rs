//! The built `lsr` against S3 replicas: what needs no server (credentials,
//! an unreachable endpoint, a server that refuses) runs in `just check`; the
//! restore through a real MinIO is `#[ignore]`d out of it and run by `just test-s3`, where a
//! missing or unreachable server fails it (`lsr_core::needs`) unless the runner opted out with
//! `LSR_SKIP=minio`.

use std::fs;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use lsr_compat::{Manifest, all_fixtures, stage};
use lsr_core::WriteMode;
use lsr_core::conformance::block_on;
use lsr_core::{FileInfo, Inventory, ObjectStore, Upload};
use lsr_store_fs::FsStore;
use lsr_store_s3::{S3Credentials, S3Store, SecretString};

const CANARY_SECRET: &str = "CANARY-secret-0123456789";
const CANARY_KEY: &str = "CANARYKEYID0123";
const CANARY_TOKEN: &str = "CANARY-token-0123456789";

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

/// Every variable the credential chain reads, so the developer's own
/// keys and `~/.aws` files never reach a test.
const CHAIN_VARS: [&str; 12] = [
    "AWS_ACCESS_KEY_ID",
    "LITESTREAM_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "LITESTREAM_SECRET_ACCESS_KEY",
    "AWS_SECRET_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "HOME",
];

fn lsr_with(args: &[&str], env: &[(&str, &str)]) -> Run {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lsr"));
    command.args(args);
    for var in CHAIN_VARS {
        command.env_remove(var);
    }
    // An empty directory stands in for the home directory.
    let home = tempfile::TempDir::new().expect("home");
    command
        .env("HOME", home.path())
        .env("USERPROFILE", home.path());
    for (name, value) in env {
        command.env(name, value);
    }
    let out = command.output().expect("run lsr");
    Run {
        code: out.status.code().expect("exited with a code"),
        stdout: String::from_utf8(out.stdout).expect("stdout is UTF-8"),
        stderr: String::from_utf8(out.stderr).expect("stderr is UTF-8"),
    }
}

fn canary_env() -> [(&'static str, &'static str); 3] {
    [
        ("AWS_ACCESS_KEY_ID", CANARY_KEY),
        ("AWS_SECRET_ACCESS_KEY", CANARY_SECRET),
        ("AWS_SESSION_TOKEN", CANARY_TOKEN),
    ]
}

fn assert_no_canary(run: &Run) {
    for text in [&run.stdout, &run.stderr] {
        assert!(!text.contains("CANARY"), "{text}");
    }
}

#[test]
fn missing_credentials_exit_3_and_name_the_variable() {
    for (env, missing) in [
        (&[][..], "AWS_ACCESS_KEY_ID"),
        (&[("AWS_ACCESS_KEY_ID", "id")][..], "AWS_SECRET_ACCESS_KEY"),
    ] {
        let run = lsr_with(
            &[
                "files",
                "--replica",
                "s3://bucket/path",
                "--endpoint",
                "http://127.0.0.1:1",
            ],
            env,
        );
        assert_eq!((run.code, run.stdout.as_str()), (3, ""), "{}", run.stderr);
        assert_eq!(run.stderr.lines().count(), 1, "{}", run.stderr);
        assert!(run.stderr.contains(missing), "{}", run.stderr);
    }
}

#[test]
fn credentials_come_from_a_shared_credentials_file() {
    let dir = tempfile::TempDir::new().expect("dir");
    let path = dir.path().join("credentials");
    fs::write(
        &path,
        format!(
            "[ci]\naws_access_key_id = {CANARY_KEY}\naws_secret_access_key = {CANARY_SECRET}\n"
        ),
    )
    .expect("write");
    let path = path.to_str().expect("UTF-8 path");
    // Past the credentials, the unreachable endpoint is what fails.
    let run = lsr_with(
        &[
            "files",
            "--replica",
            "s3://bucket/path",
            "--endpoint",
            "http://127.0.0.1:1",
        ],
        &[("AWS_SHARED_CREDENTIALS_FILE", path), ("AWS_PROFILE", "ci")],
    );
    assert_eq!((run.code, run.stdout.as_str()), (3, ""), "{}", run.stderr);
    assert!(!run.stderr.contains("credentials"), "{}", run.stderr);
    assert_no_canary(&run);
}

#[test]
fn a_bad_replica_url_is_a_usage_error_before_credentials_are_read() {
    let run = lsr_with(&["files", "--replica", "s3://bucket"], &[]);
    assert_eq!((run.code, run.stdout.as_str()), (64, ""));
    assert!(run.stderr.contains("prefix"), "{}", run.stderr);
    let run = lsr_with(&["files", "--replica", "dir", "--region", "eu-west-1"], &[]);
    assert_eq!(run.code, 64);
}

#[test]
fn secret_never_on_stderr_against_an_unreachable_endpoint() {
    // Port 1 on loopback refuses the connection at once.
    let run = lsr_with(
        &[
            "files",
            "--replica",
            "s3://bucket/path",
            "--endpoint",
            "http://127.0.0.1:1",
        ],
        &canary_env(),
    );
    assert_eq!((run.code, run.stdout.as_str()), (3, ""), "{}", run.stderr);
    assert_eq!(run.stderr.lines().count(), 1, "{}", run.stderr);
    assert_no_canary(&run);
    assert!(!run.stderr.contains("127.0.0.1"), "{}", run.stderr);
}

/// Answers every request with 403 and a body that repeats the request back,
/// the worst a server could do with a signed request in hand.
struct Refuser {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: thread::JoinHandle<()>,
}

impl Refuser {
    fn start() -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind");
        let addr = listener.local_addr().expect("addr");
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            for stream in listener.incoming() {
                if flag.load(Ordering::SeqCst) {
                    return;
                }
                if let Ok(stream) = stream {
                    refuse(stream);
                }
            }
        });
        Self { addr, stop, thread }
    }

    fn finish(self) {
        self.stop.store(true, Ordering::SeqCst);
        TcpStream::connect(self.addr).expect("wake the listener");
        self.thread.join().expect("server thread");
    }
}

fn refuse(mut stream: TcpStream) {
    let mut request = Vec::new();
    let mut buf = [0_u8; 4096];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
        }
    }
    let echoed = String::from_utf8_lossy(&request);
    let body = format!(
        "<?xml version=\"1.0\"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message><Echo>{}</Echo></Error>",
        echoed.replace('&', "&amp;").replace('<', "&lt;")
    );
    let response = format!(
        "HTTP/1.1 403 Forbidden\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    // The client may hang up first; there is nothing to do about that here.
    drop(stream.write_all(response.as_bytes()));
}

#[test]
fn secret_never_on_stderr_against_a_403() {
    let server = Refuser::start();
    let endpoint = format!("http://{}", server.addr);
    for command in ["files", "verify"] {
        let run = lsr_with(
            &[
                command,
                "--replica",
                "s3://bucket/path",
                "--endpoint",
                &endpoint,
            ],
            &canary_env(),
        );
        assert_eq!((run.code, run.stdout.as_str()), (3, ""), "{}", run.stderr);
        assert_eq!(run.stderr.lines().count(), 1, "{}", run.stderr);
        assert!(run.stderr.contains("not authorized"), "{}", run.stderr);
        assert_no_canary(&run);
        assert!(
            !run.stderr.contains(&server.addr.to_string()),
            "{}",
            run.stderr
        );
    }
    server.finish();
}

#[expect(
    clippy::disallowed_methods,
    reason = "a run prefix unique across runs against a real bucket"
)]
fn unique_prefix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("cli{nanos}-{}", std::process::id())
}

fn upload(store: &S3Store, info: &FileInfo, body: &[u8]) {
    block_on(async {
        let mut up = store
            .begin_upload(
                &info.key,
                info.created_at().unwrap().at(),
                WriteMode::Replace,
            )
            .await
            .expect("begin");
        up.write(body).await.expect("write");
        up.commit().await.expect("commit");
    });
}

/// The MinIO of `lsr_core::needs::minio`; `None` only under `LSR_SKIP=minio`.
struct Minio {
    endpoint: String,
    access: String,
    secret: String,
}

impl Minio {
    fn from_env() -> Option<Self> {
        let minio = lsr_core::needs::minio()?;
        Some(Self {
            endpoint: minio.endpoint,
            access: minio.access,
            secret: minio.secret,
        })
    }

    /// Puts every file of a directory replica under `prefix`.
    fn seed(&self, replica: &Path, prefix: &str) {
        let escaped = self.endpoint.replace(':', "%3A").replace('/', "%2F");
        let url = format!("s3://lsr-test/{prefix}?endpoint={escaped}&forcePathStyle=true");
        let config = lsr_config::s3_config(
            Some(lsr_config::S3Url::parse(&url).expect("url")),
            &lsr_config::S3Overrides::default(),
        )
        .expect("config");
        let credentials =
            S3Credentials::new(self.access.clone(), SecretString::new(self.secret.clone()));
        let store = S3Store::new(config, credentials).expect("store");
        let dir = FsStore::new(replica);
        for info in block_on(Inventory::load(&dir)).expect("inventory").files() {
            let path = replica
                .join("ltx")
                .join(info.key.level().to_string())
                .join(info.key.file_name());
            upload(&store, info, &fs::read(path).expect("read fixture file"));
        }
    }

    fn restore(&self, prefix: &str, output: &Path) -> Run {
        let url = format!("s3://lsr-test/{prefix}");
        let env = [
            ("AWS_ACCESS_KEY_ID", self.access.as_str()),
            ("AWS_SECRET_ACCESS_KEY", self.secret.as_str()),
        ];
        let output = output.to_str().expect("UTF-8 path");
        let args = [
            "restore",
            "--replica",
            &url,
            "--endpoint",
            &self.endpoint,
            "--force-path-style",
            "--output",
            output,
        ];
        lsr_with(&args, &env)
    }
}

#[test]
#[ignore = "needs MinIO; just test-s3"]
fn restore_from_minio_matches_dir() {
    let Some(minio) = Minio::from_env() else {
        return;
    };
    for fixture in all_fixtures().expect("fixtures") {
        let manifest = Manifest::load(&fixture).expect("manifest");
        let staged = stage(&fixture, &manifest).expect("stage");
        let prefix = unique_prefix();
        minio.seed(&staged.replica(), &prefix);
        let (from_dir, from_s3) = (staged.output("dir.db"), staged.output("s3.db"));
        let replica = staged.replica();
        let want = lsr_with(
            &[
                "restore",
                "--replica",
                replica.to_str().expect("UTF-8 path"),
                "--output",
                from_dir.to_str().expect("UTF-8 path"),
            ],
            &[],
        );
        let got = minio.restore(&prefix, &from_s3);
        let name = fixture.display();
        assert_eq!(got.code, want.code, "{name}: {}", got.stderr);
        if want.code == 0 {
            let pages = |path: &Path| fs::read(path).expect("restored database");
            assert_eq!(pages(&from_s3), pages(&from_dir), "{name}");
        }
    }
}
