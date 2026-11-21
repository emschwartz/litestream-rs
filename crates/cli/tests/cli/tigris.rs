//! Task 8t item 10 against the real Tigris bucket (`just tigris-restore`): a
//! replica written by Go v0.5.18 straight to Tigris, then `lsr verify`, and
//! `lsr restore` and Go's `litestream restore` of it at the same targets,
//! compared on restored pages (never on LTX bytes) with `PRAGMA integrity_check`.
//!
//! Go replicates the compat workload with compressed production intervals
//! (levels 1 s, 4 s and 16 s, snapshots every 30 s, level-0 retention 30 s,
//! checkpoints every 15 s) until the replica holds at least [`MIN_FILES`]
//! files on levels 0 to 3 and 9. Every write waits until the newest level-0
//! object in the bucket ends at the live WAL's committed end: the only waiting
//! is polling that condition. Each target is restored three ways: in process
//! over a request-counting transport (the counts and elapsed time of the
//! report), by the built `lsr`, and by Go. An as-of instant that falls inside a
//! compacted file is refused by Rust (`AsOfInsideFile`, by design) and compared
//! through the chain's own end, as `crates/compat/tests/compat/matrix.rs` does.
//!
//! The run's size comes from the environment (see [`Size`]): by default a 4096-byte-page
//! database of about 10 MB; `LSR_TIGRIS_PAGE_SIZE=16384 LSR_TIGRIS_GROW_MIB=4608 just
//! tigris-restore` first grows the database past SQLite's lock page and 4 GiB with
//! compressible blobs, so the replica stays small while restores write gigabytes.
//!
//! The test is ignored: the recipe supplies credentials and one run prefix, and
//! everything is deleted at the end (also when the test fails), then the prefix
//! is listed again and must be empty. Mismatches are collected and asserted only
//! after the cleanup, so a finding never leaves objects behind.
//!
//! Go is stopped while it may be mid-upload, and an incomplete multipart upload
//! is invisible to an object listing but still billed, so the cleanup also lists
//! the prefix's multipart uploads (ListMultipartUploads, which no store call
//! makes), aborts each, and counts any still listed as left. Those requests are
//! signed here with rusty-s3, as the store signs its own, with Tigris's
//! consistency header. A second ignored test leaves an upload open under its own
//! prefix and checks that the same cleanup aborts it.
#![expect(
    clippy::arithmetic_side_effects,
    reason = "counts and indices bounded by the run's few thousand files"
)]

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lsr_compat::{
    GoRestore, GoVersion, PageDigest, Replicator, Step, Target, Workload, go_binary, hash_pages,
    integrity_check, real_now_ms, restore_with_go, rfc3339_millis, wait_until,
};
use lsr_core::conformance::block_on;
use lsr_core::{
    Depth, FileInfo, Inventory, Level, ObjectStore, PageLimit, PlanOutcome, ReadLen, RestoreError,
    Restored, restore, verify,
};
use lsr_local::FileSink;
use lsr_ltx::{HEADER_LEN, Header};
use lsr_store_s3::{
    HttpRequest, HttpResponse, Method, OpenUpload, RequestBuilder, S3Config, S3Credentials,
    S3Store, SecretString, SystemClock, Transport, TransportError, UreqTransport, abandon_upload,
    abort_upload, open_uploads,
};

/// The replica must reach this many files before the targets are restored.
const MIN_FILES: usize = 300;
/// Rows of the initial load, in transactions of 1000.
const BULK_TXNS: u32 = 50;
/// The step after which `scratch` is filled, and the one after which it is
/// dropped and the database vacuumed (it shrinks).
const FILL_AT: u32 = 150;
const VACUUM_AT: u32 = 300;
/// The shape is checked every this many steps (one listing of ten levels).
const SHAPE_EVERY: u32 = 20;
const MAX_STEPS: u32 = 4000;
const CATCH_UP: Duration = Duration::from_secs(60);
/// A grown database's commits can be a whole-file VACUUM; Go needs minutes for one.
const CATCH_UP_GROWN: Duration = Duration::from_secs(900);
/// Bytes per growth commit: one compressible blob, so the replica stays small.
const GROW_STEP: u64 = 64 << 20;

/// How big the run is, from the environment, so the default stays the
/// few-minute run: `LSR_TIGRIS_PAGE_SIZE` (default 4096),
/// `LSR_TIGRIS_GROW_MIB` (grow the database past this many MiB after the
/// initial load, before the steady mix; default 0, no growth) and
/// `LSR_TIGRIS_MIN_FILES` (default [`MIN_FILES`]).
#[derive(Debug, Clone, Copy)]
struct Size {
    page_size: u32,
    grow_bytes: u64,
    min_files: usize,
}

fn number<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(s) => s
            .parse()
            .unwrap_or_else(|_| panic!("{name}={s} is not a number")),
        Err(_) => default,
    }
}

impl Size {
    fn from_env() -> Self {
        Self {
            page_size: number("LSR_TIGRIS_PAGE_SIZE", 4096),
            grow_bytes: number::<u64>("LSR_TIGRIS_GROW_MIB", 0) << 20,
            min_files: number("LSR_TIGRIS_MIN_FILES", MIN_FILES),
        }
    }

    fn catch_up(self) -> Duration {
        if self.grow_bytes == 0 {
            CATCH_UP
        } else {
            CATCH_UP_GROWN
        }
    }

    /// Room for the steps a larger file count needs (about 1.5 commits per file).
    fn max_steps(self) -> u32 {
        MAX_STEPS.max(u32::try_from(self.min_files * 3).unwrap_or(u32::MAX))
    }
}

fn var(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set: run just tigris-restore"))
}

/// The bucket and the replica's prefix under the run prefix.
struct Bucket {
    endpoint: String,
    region: String,
    bucket: String,
    path: String,
    access: String,
    secret: String,
}

/// The bucket with the replica at `leaf` under the run prefix.
fn bucket(leaf: &str) -> Bucket {
    Bucket {
        endpoint: var("AWS_ENDPOINT_URL_S3"),
        region: var("AWS_REGION"),
        bucket: var("LSR_TIGRIS_BUCKET"),
        path: format!("{}/{leaf}", var("LSR_TIGRIS_PREFIX")),
        access: var("AWS_ACCESS_KEY_ID"),
        secret: var("AWS_SECRET_ACCESS_KEY"),
    }
}

/// Requests by kind, counted at the transport.
#[derive(Debug, Default)]
struct Counts {
    list: AtomicU64,
    get: AtomicU64,
    head: AtomicU64,
    other: AtomicU64,
}

impl Counts {
    /// LIST, GET, HEAD, anything else; zeroed.
    fn take(&self) -> [u64; 4] {
        [&self.list, &self.get, &self.head, &self.other].map(|c| c.swap(0, Ordering::Relaxed))
    }
}

struct Counting {
    inner: UreqTransport,
    counts: Arc<Counts>,
}

impl Transport for Counting {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let listing = request
            .url()
            .query()
            .is_some_and(|q| q.contains("list-type"));
        let counter = match request.method() {
            Method::Get if listing => &self.counts.list,
            Method::Get => &self.counts.get,
            Method::Head => &self.counts.head,
            Method::Put | Method::Post | Method::Delete => &self.counts.other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.inner.send(request)
    }
}

impl Bucket {
    fn url(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.path)
    }

    fn config(&self) -> S3Config {
        let escaped = self.endpoint.replace(':', "%3A").replace('/', "%2F");
        // Virtual-host style, as these runs always used; Go's rule would pick path style.
        let url = format!(
            "{}?endpoint={escaped}&region={}&forcePathStyle=false",
            self.url(),
            self.region
        );
        lsr_config::s3_config(
            Some(lsr_config::S3Url::parse(&url).expect("url")),
            &lsr_config::S3Overrides::default(),
        )
        .expect("config")
    }

    fn store(&self) -> (S3Store<Counting>, Arc<Counts>) {
        let config = self.config();
        let counts = Arc::new(Counts::default());
        let transport = Counting {
            inner: UreqTransport::new(config.deadline()),
            counts: Arc::clone(&counts),
        };
        let credentials =
            S3Credentials::new(self.access.clone(), SecretString::new(self.secret.clone()));
        let store = S3Store::over(config, credentials, transport).expect("store");
        (store, counts)
    }

    /// Go's configuration: the compat harness's, with the replica on S3 and
    /// production's intervals compressed. Credentials come from `AWS_*`.
    fn write_go_config(&self, config: &Path, db: &Path) {
        let text = format!(
            "levels:
  - interval: 1s
  - interval: 4s
  - interval: 16s
snapshot:
  interval: 30s
  retention: 1h
l0-retention: 30s
l0-retention-check-interval: 5s
dbs:
  - path: {db}
    monitor-interval: 100ms
    checkpoint-interval: 15s
    replica:
      type: s3
      bucket: {bucket}
      path: {path}
      endpoint: {endpoint}
      region: {region}
      force-path-style: false
      sync-interval: 100ms
",
            db = db.display(),
            bucket = self.bucket,
            path = self.path,
            endpoint = self.endpoint,
            region = self.region,
        );
        fs::write(config, text).expect("write the Go config");
    }

    fn lsr(&self, command: &str, extra: &[&str]) -> (Run, Duration) {
        let url = self.url();
        let mut args = vec![
            command,
            "--replica",
            &url,
            "--endpoint",
            &self.endpoint,
            "--region",
            &self.region,
        ];
        args.extend_from_slice(extra);
        timed(|| {
            let out = Command::new(env!("CARGO_BIN_EXE_lsr"))
                .args(&args)
                .output()
                .expect("run lsr");
            Run {
                code: out.status.code().expect("exited with a code"),
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            }
        })
    }
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

#[expect(
    clippy::disallowed_methods,
    reason = "the report records the wall time a real restore takes"
)]
fn timed<R>(f: impl FnOnce() -> R) -> (R, Duration) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

fn every_file<S: ObjectStore>(store: &S, level: Level) -> Vec<FileInfo> {
    let mut files = Vec::new();
    let mut after = None;
    loop {
        let page = block_on(store.list(level, after, PageLimit::MAX)).expect("list");
        files.extend(page.files);
        match page.next {
            Some(next) => after = Some(next),
            None => return files,
        }
    }
}

/// Whether the newest level-0 object ends where the live WAL's committed
/// frames end, in the same WAL generation (`generate.rs`'s rule, over S3).
fn caught_up<S: ObjectStore>(store: &S, wl: &Workload, db: &Path) -> bool {
    let Some(newest) = every_file(store, Level::ZERO)
        .into_iter()
        .max_by_key(|f| f.key.range().max())
    else {
        return false;
    };
    let len = ReadLen::new(u32::try_from(HEADER_LEN).unwrap()).unwrap();
    // A file deleted by retention between the listing and the read: look again.
    let Ok(bytes) = block_on(store.read(&newest.key, 0, len)) else {
        return false;
    };
    let header = Header::decode(bytes[..HEADER_LEN].try_into().unwrap()).expect("header");
    let mut head = [0u8; 32];
    let wal = db.with_extension("db-wal");
    if fs::File::open(&wal)
        .and_then(|mut f| f.read_exact(&mut head))
        .is_err()
    {
        return false;
    }
    let salts = (
        u32::from_be_bytes(head[16..20].try_into().unwrap()),
        u32::from_be_bytes(head[20..24].try_into().unwrap()),
    );
    let frame = u64::from(wl.page_size()).saturating_add(24);
    let end = wl
        .wal_frames()
        .expect("frames")
        .saturating_mul(frame)
        .saturating_add(32);
    header
        .fields()
        .wal
        .is_some_and(|w| w.salts() == salts && w.offset().saturating_add(w.size()) == end)
}

/// The live side of the run.
struct Live {
    wl: Workload,
    rep: Replicator,
    db: PathBuf,
    /// Wall-clock instants between commits, for the as-of targets.
    instants: Vec<i64>,
    steps: u32,
    catch_up: Duration,
}

impl Live {
    fn commit<S: ObjectStore>(&mut self, store: &S, step: Option<Step>) {
        match step {
            Some(step) => self.wl.apply(step).expect("step"),
            None => self.wl.touch().expect("touch"),
        }
        let Self {
            wl,
            rep,
            db,
            catch_up,
            ..
        } = self;
        wait_until("the bucket to catch up with the WAL", *catch_up, || {
            rep.ensure_running()?;
            Ok(caught_up(store, wl, db))
        })
        .unwrap_or_else(|e| panic!("{e}: {}", self.rep.log_tail()));
        self.steps = self.steps.saturating_add(1);
    }
}

/// The steady mix: inserts, updates, deletes, overflow blobs and small commits.
fn steady(i: u32) -> Vec<Option<Step>> {
    match (i, i % 8) {
        (FILL_AT, _) => vec![Some(Step::FillScratch(500))],
        (VACUUM_AT, _) => vec![Some(Step::DropScratch), Some(Step::Vacuum)],
        (_, 0) => vec![Some(Step::Insert(60))],
        (_, 1) => vec![Some(Step::Update(40))],
        (_, 2) => vec![Some(Step::Delete(12))],
        (_, 3) => vec![Some(Step::Blob(3))],
        _ => vec![None],
    }
}

/// Files per level, and whether the replica has the shape the run wants.
fn shape<S: ObjectStore>(store: &S, min_files: usize) -> ([usize; 10], bool) {
    let inventory = block_on(Inventory::load(store)).expect("inventory");
    let counts = Level::ALL.map(|level| inventory.level(level).len());
    let mut snapshots: Vec<u64> = inventory
        .level(Level::SNAPSHOT)
        .iter()
        .map(|f| f.key.range().max().get())
        .collect();
    snapshots.dedup();
    let total: usize = counts.iter().sum();
    let ok = total >= min_files && counts[..4].iter().all(|&n| n > 0) && snapshots.len() >= 3;
    (counts, ok)
}

/// Runs Go and the workload until the bucket holds the wanted shape; returns
/// the live database's digest after Go stopped and a final checkpoint.
fn generate(b: &Bucket, work: &Path, size: Size) -> (PathBuf, PathBuf, Vec<i64>, PageDigest) {
    let (store, _) = b.store();
    let (db, config) = (work.join("app.db"), work.join("litestream.yml"));
    b.write_go_config(&config, &db);
    let before = real_now_ms().expect("clock");
    let wl = Workload::create(&db, size.page_size, 0x8_7e57).expect("workload");
    let go = go_binary(GoVersion::V0_5_18).expect("just tigris-restore builds Go v0.5.18");
    let rep = Replicator::start(&go, &config, &work.join("go.log")).expect("replicate");
    let mut live = Live {
        wl,
        rep,
        db,
        instants: vec![before],
        steps: 0,
        catch_up: size.catch_up(),
    };
    live.commit(&store, None);
    for _ in 0..BULK_TXNS {
        live.commit(&store, Some(Step::Insert(1000)));
    }
    grow(&mut live, &store, size);
    let mut i = 0;
    loop {
        for step in steady(i) {
            live.commit(&store, step);
        }
        i += 1;
        if i % SHAPE_EVERY == 0 {
            live.instants.push(real_now_ms().expect("clock"));
            let (counts, ok) = shape(&store, size.min_files);
            eprintln!("RUN step {i}: files per level {counts:?}");
            if ok && i > VACUUM_AT {
                break;
            }
        }
        assert!(
            i < size.max_steps(),
            "no shape after {} steps",
            size.max_steps()
        );
    }
    live.wl.settle().expect("settle");
    live.commit(&store, None);
    live.instants.push(real_now_ms().expect("clock"));
    let Live {
        wl,
        rep,
        db,
        instants,
        steps,
        ..
    } = live;
    rep.stop().expect("stop Go");
    drop(wl);
    let conn = rusqlite::Connection::open(&db).expect("open the live database");
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .expect("checkpoint");
    drop(conn);
    let digest = hash_pages(&db).expect("hash the live database");
    eprintln!("RUN {steps} commits; {} pages live", digest.page_count);
    (config, db, instants, digest)
}

/// Commits compressible blobs until the database file passes `size.grow_bytes`.
fn grow<S: ObjectStore>(live: &mut Live, store: &S, size: Size) {
    let page = u64::from(size.page_size);
    let pages = u32::try_from(GROW_STEP / page).expect("pages per step");
    while u64::from(live.wl.page_count().expect("page count")) * page < size.grow_bytes {
        live.commit(store, Some(Step::Blob(pages)));
        if live.steps.is_multiple_of(16) {
            let mib = (u64::from(live.wl.page_count().expect("page count")) * page) >> 20;
            eprintln!("RUN grown to {mib} MiB");
        }
    }
}

/// How one target came out.
#[derive(Debug)]
enum Verdict {
    /// Rust (in process and `lsr`) and Go restored equal pages, integrity ok.
    Same { reached: u64 },
    /// Rust refused `AsOfInsideFile`; the chain's own end equals Go's as-of restore.
    ViaInsideFile { reaches: u64 },
    /// Both refused.
    BothRefuse { rust: String, go: String },
    /// Anything else: a finding.
    Mismatch(String),
}

impl Verdict {
    fn describe(&self) -> String {
        match self {
            Self::Same { reached } => format!("same pages, txid {reached:x}"),
            Self::ViaInsideFile { reaches } => {
                format!("rust AsOfInsideFile; chain end {reaches:x} same pages as go")
            }
            Self::BothRefuse { rust, go } => format!("both refuse: rust {rust}; go {go}"),
            Self::Mismatch(what) => format!("MISMATCH {what}"),
        }
    }
}

struct Row {
    label: String,
    target: Target,
    verdict: Verdict,
    counts: [u64; 4],
    rust: Duration,
    cli: Duration,
    go: Duration,
    files: usize,
    bytes: u64,
}

struct Ctx<'a> {
    b: &'a Bucket,
    go: PathBuf,
    config: &'a Path,
    db: &'a Path,
    work: &'a Path,
}

fn checked(path: &Path) -> Result<PageDigest, String> {
    let digest = hash_pages(path).map_err(|e| e.to_string())?;
    integrity_check(path).map_err(|e| e.to_string())?;
    Ok(digest)
}

fn lsr_target(target: Target) -> Vec<String> {
    match target {
        Target::Latest => vec![],
        Target::Txid { txid } => vec!["--txid".into(), format!("{txid:016x}")],
        Target::AsOf { unix_ms } => {
            vec![
                "--timestamp".into(),
                rfc3339_millis(unix_ms).expect("format"),
            ]
        }
    }
}

impl Ctx<'_> {
    fn out(&self, n: usize, who: &str) -> PathBuf {
        let dir = self.work.join(format!("t{n}-{who}"));
        fs::create_dir_all(&dir).unwrap();
        dir.join("restored.db")
    }

    fn cli(&self, target: Target, out: &Path) -> (Run, Duration) {
        let mut extra = lsr_target(target);
        extra.extend(["--output".into(), out.display().to_string()]);
        let extra: Vec<&str> = extra.iter().map(String::as_str).collect();
        self.b.lsr("restore", &extra)
    }

    fn row(&self, n: usize, label: &str, target: Target) -> Row {
        let (store, counts) = self.b.store();
        let rust_out = self.out(n, "rust");
        let sink = FileSink::create(&rust_out).expect("sink");
        let wanted = target.restore_target().expect("a valid target");
        let (rust, rust_time) = timed(|| block_on(restore(&store, wanted, sink)));
        let counts = counts.take();
        let cli_out = self.out(n, "lsr");
        let (cli, cli_time) = self.cli(target, &cli_out);
        let go_out = self.out(n, "go");
        let (go, go_time) =
            timed(|| restore_with_go(&self.go, self.config, self.db, &go_out, target));
        let go = go.expect("run Go");
        let (files, bytes) = rust
            .as_ref()
            .map_or((0, 0), |r| (r.files(), r.bytes_read()));
        let verdict = self.verdict(n, target, &rust, &cli, &go, [&rust_out, &cli_out, &go_out]);
        // A grown run's outputs are gigabytes each; compared, they are not needed again,
        // except Latest's Rust output, which the caller compares with the live database.
        for who in ["rust", "lsr", "go", "lsr-chain"] {
            if n == 0 && who == "rust" {
                continue;
            }
            let dir = self.work.join(format!("t{n}-{who}"));
            if dir.exists() {
                fs::remove_dir_all(&dir).expect("remove a compared output");
            }
        }
        Row {
            label: label.to_owned(),
            target,
            verdict,
            counts,
            rust: rust_time,
            cli: cli_time,
            go: go_time,
            files,
            bytes,
        }
    }

    fn verdict(
        &self,
        n: usize,
        target: Target,
        rust: &Result<Restored, RestoreError>,
        cli: &Run,
        go: &GoRestore,
        [rust_out, cli_out, go_out]: [&PathBuf; 3],
    ) -> Verdict {
        match (rust, go) {
            (Ok(r), GoRestore::Restored) => {
                let digests = [rust_out, cli_out, go_out].map(|p| checked(p));
                if cli.code != 0 {
                    return Verdict::Mismatch(format!("lsr exit {}: {}", cli.code, cli.stderr));
                }
                match digests {
                    [Ok(a), Ok(c), Ok(g)] if a == c && a == g => Verdict::Same {
                        reached: r.reached().get(),
                    },
                    other => Verdict::Mismatch(format!(
                        "pages differ or fail: {:?}",
                        other.map(|d| d.map(|d| d.page_count))
                    )),
                }
            }
            (Err(RestoreError::NoPlan(no)), GoRestore::Restored)
                if let PlanOutcome::AsOfInsideFile { chain, .. } = no.outcome() =>
            {
                let reaches = chain.reaches().get();
                let chain_out = self.out(n, "lsr-chain");
                let (run, _) = self.cli(Target::Txid { txid: reaches }, &chain_out);
                match (cli.code, run.code, checked(&chain_out), checked(go_out)) {
                    (2, 0, Ok(a), Ok(g)) if a == g && matches!(target, Target::AsOf { .. }) => {
                        Verdict::ViaInsideFile { reaches }
                    }
                    other => Verdict::Mismatch(format!(
                        "inside a file: lsr {} / chain {} / {:?}",
                        other.0,
                        other.1,
                        (other.2.is_ok(), other.3.is_ok())
                    )),
                }
            }
            (Err(e), GoRestore::Refused { detail }) if cli.code == 2 => Verdict::BothRefuse {
                rust: e.to_string(),
                go: detail.clone(),
            },
            (rust, go) => Verdict::Mismatch(format!(
                "rust {:?}, lsr exit {} ({}), go {go:?}",
                rust.as_ref().map(Restored::reached),
                cli.code,
                cli.stderr.trim()
            )),
        }
    }
}

/// Latest, then transactions the listing makes interesting, then instants.
fn targets(inventory: &Inventory, instants: &[i64]) -> Vec<(String, Target)> {
    let ends = |level: Level| -> Vec<u64> {
        inventory
            .level(level)
            .iter()
            .map(|f| f.key.range().max().get())
            .collect()
    };
    let median = |v: Vec<u64>| v.get(v.len() / 2).copied().expect("a file");
    let head = Level::ALL
        .iter()
        .flat_map(|&l| ends(l))
        .max()
        .expect("a file");
    let snaps = ends(Level::SNAPSHOT);
    let mut out = vec![("latest".to_owned(), Target::Latest)];
    for (label, txid) in [
        ("head", head),
        ("head - 1", head - 1),
        ("newest snapshot", *snaps.last().unwrap()),
        ("oldest snapshot", snaps[0]),
        ("median level-3 end", median(ends(Level::new(3).unwrap()))),
        ("median level-2 end", median(ends(Level::new(2).unwrap()))),
        ("median level-1 end", median(ends(Level::new(1).unwrap()))),
    ] {
        out.push((format!("txid {label}"), Target::Txid { txid }));
    }
    let pick = |q: usize| instants[(instants.len() - 1) * q / 4];
    for (label, unix_ms) in [
        ("before Go started", instants[0]),
        ("a quarter in", pick(1)),
        ("halfway", pick(2)),
        ("three quarters in", pick(3)),
        ("after the last commit", *instants.last().unwrap()),
    ] {
        out.push((format!("as-of {label}"), Target::AsOf { unix_ms }));
    }
    out
}

/// `lsr verify --json` and an in-process verify for the request counts.
fn verify_replica(b: &Bucket, head: u64, findings: &mut Vec<String>) {
    let (run, cli_time) = b.lsr("verify", &["--json"]);
    let json: serde_json::Value = serde_json::from_str(&run.stdout)
        .unwrap_or_else(|e| panic!("verify json: {e}: {} {}", run.stdout, run.stderr));
    let reaches = json["latest"]["reaches"]
        .as_str()
        .map(|s| u64::from_str_radix(s, 16));
    eprintln!(
        "VERIFY lsr exit {} in {cli_time:?}: worst {} files_read {} bytes_read {} reaches {reaches:?}",
        run.code, json["worst"], json["files_read"], json["bytes_read"]
    );
    let (store, counts) = b.store();
    let (report, t) = timed(|| block_on(verify(&store, Depth::Contents)).expect("verify"));
    let [list, get, head_n, other] = counts.take();
    eprintln!(
        "VERIFY in process {t:?}: LIST {list} GET {get} HEAD {head_n} other {other}; worst {:?}, {} findings",
        report.worst(),
        report.findings().len()
    );
    let clean = json["worst"].is_null() || json["worst"] == "note";
    if run.code != 0 || !clean || reaches != Some(Ok(head)) {
        findings.push(format!("verify: exit {}, {}", run.code, run.stdout.trim()));
    }
}

/// Deletes every object of the replica's prefix; returns (deleted, left).
fn delete_all(b: &Bucket) -> (usize, usize) {
    let (store, _) = b.store();
    let mut deleted = 0;
    for level in Level::ALL {
        let keys: Vec<_> = every_file(&store, level)
            .into_iter()
            .map(|f| f.key)
            .collect();
        for chunk in keys.chunks(500) {
            block_on(store.delete(chunk)).expect("delete");
        }
        deleted += keys.len();
    }
    let mut left = 0;
    for level in Level::ALL {
        let page = block_on(store.list(level, None, PageLimit::MAX)).expect("list");
        left += page.files.len() + page.skipped;
    }
    (deleted, left)
}

/// Signed requests the store does not make (multipart uploads by listing), through
/// the store's own raw builder and upload listing (lsr-store-s3's `testing` feature).
struct Raw {
    builder: RequestBuilder,
    transport: UreqTransport,
}

impl Raw {
    fn new(b: &Bucket) -> Self {
        let config = b.config();
        let credentials = S3Credentials::new(b.access.clone(), SecretString::new(b.secret.clone()));
        Self {
            builder: RequestBuilder::new(&config, &credentials).expect("builder"),
            transport: UreqTransport::new(config.deadline()),
        }
    }

    /// Every multipart upload open under `prefix`.
    fn uploads(&self, prefix: &str) -> Vec<OpenUpload> {
        open_uploads(&self.transport, &self.builder, &SystemClock, prefix)
            .unwrap_or_else(|e| panic!("listing the uploads under {prefix}: {e}"))
    }

    fn abort(&self, upload: &OpenUpload) {
        abort_upload(&self.transport, &self.builder, &SystemClock, upload)
            .unwrap_or_else(|e| panic!("aborting an upload of {}: {e}", upload.key));
    }

    /// Starts an upload of `key`, sends its first part and leaves it open.
    fn abandon(&self, key: &str) {
        abandon_upload(
            &self.transport,
            &self.builder,
            &SystemClock,
            key,
            vec![7; 1024],
        )
        .unwrap_or_else(|e| panic!("leaving an upload of {key} open: {e}"));
    }
}

/// Aborts every multipart upload under the replica's prefix; returns
/// (aborted, left).
fn abort_all(b: &Bucket) -> (usize, usize) {
    let raw = Raw::new(b);
    let prefix = format!("{}/", b.path);
    let open = raw.uploads(&prefix);
    for upload in &open {
        raw.abort(upload);
    }
    (open.len(), raw.uploads(&prefix).len())
}

/// Deletes the replica's prefix when the test ends, passed or not: its
/// objects, then its open multipart uploads.
struct Cleanup<'a>(&'a Bucket);

impl Cleanup<'_> {
    /// Returns (objects deleted, uploads aborted, objects and uploads left).
    fn run(&self) -> (usize, usize, usize) {
        let (deleted, objects_left) = delete_all(self.0);
        let (aborted, uploads_left) = abort_all(self.0);
        eprintln!(
            "CLEANUP deleted {deleted} objects and aborted {aborted} multipart uploads under {}; {objects_left} objects and {uploads_left} uploads left",
            self.0.path
        );
        (deleted, aborted, objects_left + uploads_left)
    }
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        let (_, _, left) = self.run();
        assert!(
            std::thread::panicking() || left == 0,
            "{left} objects or multipart uploads left"
        );
    }
}

/// An upload left open, as Go leaves one when it is stopped mid-upload, is
/// aborted by the cleanup, and the prefix then lists none. Its own prefix
/// keeps it clear of the Go run's uploads, which are in flight meanwhile.
#[test]
#[ignore = "needs the Tigris bucket (just tigris-restore)"]
fn cleanup_aborts_an_upload_left_open() {
    let b = bucket("abandoned-upload");
    let cleanup = Cleanup(&b);
    let raw = Raw::new(&b);
    let key = format!("{}/0000/0000000000000001-0000000000000001.ltx", b.path);
    raw.abandon(&key);
    let open = raw.uploads(&format!("{}/", b.path));
    assert_eq!(open.len(), 1, "the abandoned upload is listed");
    assert_eq!(cleanup.run(), (0, 1, 0), "deleted, aborted, left");
}

fn print(row: &Row) {
    let [list, get, head, other] = row.counts;
    eprintln!(
        "ROW {} | {:?} | {} | rust {:?} ({} files, {} bytes; LIST {list} GET {get} HEAD {head} other {other}) | lsr {:?} | go {:?}",
        row.label,
        row.target,
        row.verdict.describe(),
        row.rust,
        row.files,
        row.bytes,
        row.cli,
        row.go
    );
}

#[test]
#[ignore = "needs the Tigris bucket and Go v0.5.18 (just tigris-restore)"]
fn go_written_tigris_replica_restores_the_same_in_rust_and_go() {
    let b = bucket("go");
    let work = tempfile::tempdir().expect("tempdir");
    let _cleanup = Cleanup(&b);
    let size = Size::from_env();
    eprintln!("SIZE {size:?}");
    let (config, db, instants, live) = generate(&b, work.path(), size);
    let (store, _) = b.store();
    let inventory = block_on(Inventory::load(&store)).expect("inventory");
    let per_level = Level::ALL.map(|l| inventory.level(l).len());
    let bytes: u64 = inventory.files().iter().map(|f| f.size).sum();
    eprintln!(
        "REPLICA {} files, {bytes} bytes, per level {per_level:?}",
        inventory.files().len()
    );
    let targets = targets(&inventory, &instants);
    let head = match targets[1].1 {
        Target::Txid { txid } => txid,
        Target::Latest | Target::AsOf { .. } => panic!("the second target is the head"),
    };
    let mut findings = Vec::new();
    verify_replica(&b, head, &mut findings);
    let go = go_binary(GoVersion::V0_5_18).expect("go");
    let ctx = Ctx {
        b: &b,
        go,
        config: &config,
        db: &db,
        work: work.path(),
    };
    for (n, (label, target)) in targets.into_iter().enumerate() {
        let row = ctx.row(n, &label, target);
        print(&row);
        match &row.verdict {
            Verdict::Same { reached } if n == 0 => {
                let got = hash_pages(&ctx.out(0, "rust")).expect("hash");
                if got != live || *reached != head {
                    findings.push(format!("latest is not the live database ({reached:x})"));
                }
            }
            Verdict::Mismatch(what) => findings.push(format!("{label}: {what}")),
            Verdict::Same { .. } | Verdict::ViaInsideFile { .. } | Verdict::BothRefuse { .. } => {}
        }
    }
    assert!(findings.is_empty(), "findings: {findings:#?}");
}
