//! The long soak of `lsr replicate` against the real Tigris bucket
//! (`just soak-tigris <hours>`; see docs/plans/phase-3-syncing.md, the soak section).
//!
//! A synthetic writer (the compat workload: inserts, updates, deletes, overflow blobs, a
//! scratch table filled, dropped and VACUUMed, and outside TRUNCATE checkpoints) commits
//! through one `lsr replicate -config` process with a Go-style config (levels 30 s,
//! 5 min and 30 min, snapshots hourly kept 6 h). After every commit the writer waits until the
//! newest level-0 object in the bucket ends at the live WAL's committed end, and records the
//! transaction it reached: that commit is acknowledged, and a digest of its pages is
//! recorded, read from a copy of the database and its WAL taken under a read transaction
//! with the writer idle, then checkpointed, so the copy holds exactly the pages SQLite reads.
//!
//! Every check interval (an hour by default) the writer pauses: `lsr restore` at Latest and
//! at a random transaction a replica promises exactly (the end of a compacted file or a
//! snapshot, inside the retention) are compared with the recorded digests, with `PRAGMA integrity_check`; the process's resident memory, threads
//! and open files are read, and its log is scanned for warnings. A mismatch is a finding and
//! the soak goes on; findings are asserted only after the process stopped and the run
//! prefix was deleted. Zero findings is the pass bar.

// `cfg(test)` is always set here; it marks the helpers as test code, so clippy.toml's
// allow-*-in-tests cover them as they cover #[test] fns.
#[cfg(test)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "counts bounded by the soak's commits and hours"
)]
mod soak {
    use std::fs;
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    use lsr_compat::{PageDigest, SplitMix, Step, Workload, hash_pages, integrity_check};
    use lsr_core::conformance::block_on;
    use lsr_core::needs::{Resource, must};
    use lsr_core::{Level, ObjectStore, PageLimit, ReadLen};
    use lsr_ltx::{HEADER_LEN, Header};
    use lsr_store_s3::{S3Credentials, S3Store, SecretString, UreqTransport};

    /// The longest one commit may take to reach the bucket before the soak gives up.
    const CATCH_UP: Duration = Duration::from_secs(600);
    /// The snapshot retention in the config; a sampled target is picked inside it, an hour
    /// short so retention cannot remove it between the pick and the restore.
    const RETENTION_HOURS: u64 = 6;

    fn var(name: &str) -> String {
        must(Resource::Tigris, || {
            std::env::var(name).map_err(|_unset| format!("{name} is not set"))
        })
    }

    fn number(name: &str, default: f64) -> f64 {
        std::env::var(name).map_or(default, |s| {
            s.parse()
                .unwrap_or_else(|_| panic!("{name}={s} is not a number"))
        })
    }

    struct Bucket {
        endpoint: String,
        region: String,
        bucket: String,
        path: String,
        access: String,
        secret: String,
    }

    impl Bucket {
        fn from_env() -> Self {
            Self {
                endpoint: var("AWS_ENDPOINT_URL_S3"),
                region: var("AWS_REGION"),
                bucket: var("LSR_TIGRIS_BUCKET"),
                path: format!("{}/soak", var("LSR_TIGRIS_PREFIX")),
                access: var("AWS_ACCESS_KEY_ID"),
                secret: var("AWS_SECRET_ACCESS_KEY"),
            }
        }

        fn url(&self) -> String {
            format!("s3://{}/{}", self.bucket, self.path)
        }

        fn store(&self) -> S3Store<UreqTransport> {
            let escaped = self.endpoint.replace(':', "%3A").replace('/', "%2F");
            // Virtual-host style, as these runs always used; Go's rule would pick path style.
            let url = format!(
                "{}?endpoint={escaped}&region={}&forcePathStyle=false",
                self.url(),
                self.region
            );
            let config = lsr_config::s3_config(
                Some(lsr_config::S3Url::parse(&url).expect("url")),
                &lsr_config::S3Overrides::default(),
            )
            .expect("config");
            let transport = UreqTransport::new(config.deadline());
            let credentials =
                S3Credentials::new(self.access.clone(), SecretString::new(self.secret.clone()));
            S3Store::over(config, credentials, transport).expect("store")
        }

        fn write_config(&self, config: &Path, db: &Path) {
            let text = format!(
                "levels:
  - interval: 30s
  - interval: 5m
  - interval: 30m
snapshot:
  interval: 1h
  retention: {RETENTION_HOURS}h
l0-retention: 5m
l0-retention-check-interval: 30s
dbs:
  - path: {db}
    monitor-interval: 1s
    checkpoint-interval: 1m
    replica:
      type: s3
      bucket: {bucket}
      path: {path}
      endpoint: {endpoint}
      region: {region}
      force-path-style: false
      sync-interval: 1s
",
                db = db.display(),
                bucket = self.bucket,
                path = self.path,
                endpoint = self.endpoint,
                region = self.region,
            );
            fs::write(config, text).expect("write the config");
        }

        /// Runs `lsr restore` of this replica into `out`; the exit code and stderr.
        fn restore(&self, out: &Path, txid: Option<u64>) -> (i32, String) {
            let url = self.url();
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_lsr"));
            cmd.args(["restore", "--replica", &url, "--endpoint", &self.endpoint])
                .args(["--region", &self.region, "--output"])
                .arg(out);
            if let Some(txid) = txid {
                cmd.args(["--txid", &format!("{txid:016x}")]);
            }
            let run = cmd.output().expect("run lsr restore");
            let code = run.status.code().expect("exited with a code");
            (code, String::from_utf8_lossy(&run.stderr).into_owned())
        }
    }

    fn newest_l0<S: ObjectStore>(store: &S) -> Option<lsr_core::FileInfo> {
        let mut newest: Option<lsr_core::FileInfo> = None;
        let mut after = None;
        loop {
            let page = block_on(store.list(Level::ZERO, after, PageLimit::MAX)).ok()?;
            for f in page.files {
                if newest
                    .as_ref()
                    .is_none_or(|n| f.key.range().max() > n.key.range().max())
                {
                    newest = Some(f);
                }
            }
            match page.next {
                Some(next) => after = Some(next),
                None => return newest,
            }
        }
    }

    /// The transaction the bucket reached when its newest level-0 object ends at the live
    /// WAL's committed end, in the same WAL generation; `None` while it has not.
    fn caught_up<S: ObjectStore>(store: &S, wl: &Workload, db: &Path) -> Option<u64> {
        let newest = newest_l0(store)?;
        let len = ReadLen::new(u32::try_from(HEADER_LEN).ok()?)?;
        let bytes = block_on(store.read(&newest.key, 0, len)).ok()?;
        let header = Header::decode(bytes[..HEADER_LEN].try_into().unwrap()).ok()?;
        let mut head = [0u8; 32];
        fs::File::open(db.with_extension("db-wal"))
            .and_then(|mut f| f.read_exact(&mut head))
            .ok()?;
        let salts = (
            u32::from_be_bytes(head[16..20].try_into().unwrap()),
            u32::from_be_bytes(head[20..24].try_into().unwrap()),
        );
        let frame = u64::from(wl.page_size()) + 24;
        let end = wl.wal_frames().ok()? * frame + 32;
        let wal = header.fields().wal?;
        (wal.salts() == salts && wal.offset() + wal.size() == end)
            .then(|| newest.key.range().max().get())
    }

    /// The pages SQLite would read now: a copy of the database and its WAL taken under a
    /// read transaction (no checkpoint can reset the WAL meanwhile), checkpointed into the
    /// copy's main file and hashed.
    fn live_pages(db: &Path, scratch: &Path) -> PageDigest {
        let copy = scratch.join("live.db");
        remove(&copy);
        let reader = rusqlite::Connection::open(db).expect("open a reader");
        reader.execute_batch("BEGIN").expect("begin");
        let _n: i64 = reader
            .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
            .expect("read");
        fs::copy(db, &copy).expect("copy the database");
        let wal = db.with_extension("db-wal");
        if wal.exists() {
            fs::copy(&wal, copy.with_extension("db-wal")).expect("copy the WAL");
        }
        reader.execute_batch("COMMIT").expect("end the read");
        drop(reader);
        let conn = rusqlite::Connection::open(&copy).expect("open the copy");
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .expect("checkpoint the copy");
        drop(conn);
        hash_pages(&copy).expect("hash the copy")
    }

    /// The steady mix, with a VACUUM cycle and an outside checkpoint now and then.
    fn steady(i: u64) -> Vec<Option<Step>> {
        match (i % 600, i % 8) {
            (100, _) => vec![Some(Step::FillScratch(500))],
            (400, _) => vec![Some(Step::DropScratch), Some(Step::Vacuum)],
            (_, 0) => vec![Some(Step::Insert(60))],
            (_, 1) => vec![Some(Step::Update(40))],
            (_, 2) => vec![Some(Step::Delete(55))],
            (_, 3) => vec![Some(Step::Blob(3))],
            _ => vec![None],
        }
    }

    /// What one acknowledged commit left: a digest of its pages and when it was acknowledged.
    struct Acked {
        digest: u64,
        pages: u32,
        at: Instant,
    }

    /// A short digest of every page hash; two databases with equal digests have equal pages
    /// (SipHash over the per-page SHA-256 hashes, with fixed keys).
    fn digest(pages: &PageDigest) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        pages.page_size.hash(&mut h);
        pages.page_hashes.hash(&mut h);
        h.finish()
    }

    /// The live WAL's salts and committed frame count: a replicator commit changes them.
    fn wal_mark(wl: &Workload, db: &Path) -> Option<((u32, u32), u64)> {
        let mut head = [0u8; 32];
        fs::File::open(db.with_extension("db-wal"))
            .and_then(|mut f| f.read_exact(&mut head))
            .ok()?;
        let salts = (
            u32::from_be_bytes(head[16..20].try_into().unwrap()),
            u32::from_be_bytes(head[20..24].try_into().unwrap()),
        );
        Some((salts, wl.wal_frames().ok()?))
    }

    /// Resident memory (KiB), threads and open files of one process, from ps and lsof.
    fn resources(pid: u32) -> String {
        let pid = pid.to_string();
        let run = |cmd: &str, args: &[&str]| {
            Command::new(cmd)
                .args(args)
                .output()
                .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
                .unwrap_or_default()
        };
        let rss = run("ps", &["-o", "rss=", "-p", &pid]);
        let threads = if Path::new("/proc").exists() {
            run("sh", &["-c", &format!("ls /proc/{pid}/task | wc -l")])
        } else {
            run(
                "sh",
                &["-c", &format!("ps -M -p {pid} | tail -n +2 | wc -l")],
            )
        };
        let files = run(
            "sh",
            &[
                "-c",
                &format!("lsof -p {pid} 2>/dev/null | tail -n +2 | wc -l"),
            ],
        );
        format!(
            "rss_kib {} threads {} files {}",
            rss.trim(),
            threads.trim(),
            files.trim()
        )
    }

    struct Soak {
        bucket: Bucket,
        store: S3Store<UreqTransport>,
        wl: Workload,
        db: PathBuf,
        scratch: PathBuf,
        log: PathBuf,
        child: Child,
        rng: SplitMix,
        commits: u64,
        last: u64,
        /// Every acknowledged transaction inside the retention window, by TXID.
        acked: std::collections::BTreeMap<u64, Acked>,
        findings: Vec<String>,
        /// Warning lines of the log already printed.
        warned: usize,
    }

    impl Soak {
        /// One commit (or an outside checkpoint followed by a touch), acknowledged.
        fn commit(&mut self, step: Option<Step>) {
            match step {
                Some(step) => self.wl.apply(step).expect("step"),
                None => self.wl.touch().expect("touch"),
            }
            self.record();
            self.commits += 1;
        }

        /// Waits until the bucket holds everything the WAL has committed and records the
        /// transaction it reached. The replicator commits too (its `_litestream_seq` writes
        /// around checkpoints), so this is the head of the WAL, whoever wrote it.
        #[expect(
            clippy::disallowed_methods,
            reason = "a commit's catch-up has a deadline"
        )]
        fn acknowledge(&mut self) {
            let deadline = Instant::now() + CATCH_UP;
            self.last = loop {
                if let Some(txid) = caught_up(&self.store, &self.wl, &self.db) {
                    break txid;
                }
                if let Ok(Some(status)) = self.child.try_wait() {
                    panic!("lsr replicate exited: {status}: {}", tail(&self.log));
                }
                assert!(
                    Instant::now() < deadline,
                    "no catch-up in {CATCH_UP:?}: {}",
                    tail(&self.log)
                );
                // One LIST and one GET a second: the harness should not be the load it measures.
                std::thread::sleep(Duration::from_secs(1));
            };
        }

        /// Acknowledges the head and records its pages. A replicator commit between the
        /// acknowledgement and the copy would label the copy with an older transaction; such a
        /// commit changes the WAL, so the copy is retaken until the WAL did not move across it.
        #[expect(
            clippy::disallowed_methods,
            reason = "an entry's age bounds the window"
        )]
        fn record(&mut self) {
            loop {
                self.acknowledge();
                let before = wal_mark(&self.wl, &self.db);
                let pages = live_pages(&self.db, &self.scratch);
                if before.is_some() && wal_mark(&self.wl, &self.db) == before {
                    self.acked.insert(
                        self.last,
                        Acked {
                            digest: digest(&pages),
                            pages: pages.page_count,
                            at: Instant::now(),
                        },
                    );
                    return;
                }
                eprintln!(
                    "RECORD retaken: the WAL moved during the copy at {:x}",
                    self.last
                );
            }
        }

        /// Creates `scratch` again after a cycle dropped it, so the next cycle can fill it.
        fn recreate_scratch(&mut self) {
            let conn = rusqlite::Connection::open(&self.db).expect("open");
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS scratch(id INTEGER PRIMARY KEY, filler TEXT NOT NULL)",
            )
            .expect("create scratch");
            drop(conn);
            self.acknowledge();
        }

        /// An application checkpoint through its own connection, as an application would run one.
        fn outside_checkpoint(&mut self) {
            let conn = rusqlite::Connection::open(&self.db).expect("open");
            let (busy, _, _): (i64, i64, i64) = conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })
                .expect("checkpoint");
            drop(conn);
            eprintln!("CHECKPOINT outside truncate busy={busy}");
            self.commit(None);
        }

        /// Restores at `txid` (Latest when `None`) and compares it with `want`; returns how long
        /// `lsr restore` took, so the hourly line can show restore speed against the bucket.
        #[expect(
            clippy::disallowed_methods,
            reason = "the soak measures real restore time against a real bucket"
        )]
        fn compare(&mut self, label: &str, txid: Option<u64>, want: (u64, u32)) -> Duration {
            let out = self.scratch.join("restored.db");
            remove(&out);
            let started = Instant::now();
            let (code, stderr) = self.bucket.restore(&out, txid);
            let took = started.elapsed();
            let got = (code == 0)
                .then(|| hash_pages(&out).ok())
                .flatten()
                .filter(|_| integrity_check(&out).is_ok());
            match got {
                Some(got) if digest(&got) == want.0 => {
                    eprintln!(
                        "CHECK {label}: same pages ({} pages) in {:.2} s",
                        got.page_count,
                        took.as_secs_f64()
                    )
                }
                Some(got) => self.findings.push(format!(
                    "{label}: {} pages restored, {} expected, pages differ",
                    got.page_count, want.1
                )),
                None => self.findings.push(format!(
                    "{label}: lsr restore exit {code}: {}",
                    stderr.trim()
                )),
            }
            remove(&out);
            took
        }

        /// TXIDs a replica promises to restore exactly: the ends of the files on the compacted
        /// levels and the snapshots, as listed now.
        fn boundaries(&self) -> Vec<u64> {
            let mut ends = Vec::new();
            for level in Level::ALL.into_iter().filter(|&l| l != Level::ZERO) {
                let mut after = None;
                loop {
                    let page =
                        block_on(self.store.list(level, after, PageLimit::MAX)).expect("list");
                    ends.extend(page.files.iter().map(|f| f.key.range().max().get()));
                    match page.next {
                        Some(next) => after = Some(next),
                        None => break,
                    }
                }
            }
            ends
        }

        fn check(&mut self, hour: u64) {
            self.record();
            let latest = self
                .acked
                .get(&self.last)
                .map(|a| (a.digest, a.pages))
                .expect("the head");
            let latest_took =
                self.compare(&format!("hour {hour} latest {:x}", self.last), None, latest);
            let window = Duration::from_secs((RETENTION_HOURS - 1) * 3600);
            self.acked.retain(|_, a| a.at.elapsed() < window);
            let pool: Vec<u64> = self
                .boundaries()
                .into_iter()
                .filter(|t| self.acked.contains_key(t) && *t != self.last)
                .collect();
            let mut target_took = None;
            if let Some(len) = u64::try_from(pool.len()).ok().filter(|&n| n > 0) {
                let txid = pool[usize::try_from(self.rng.below(len)).unwrap()];
                let want = self
                    .acked
                    .get(&txid)
                    .map(|a| (a.digest, a.pages))
                    .expect("acked");
                target_took = Some(self.compare(
                    &format!("hour {hour} txid {txid:x} of {len}"),
                    Some(txid),
                    want,
                ));
            } else {
                eprintln!("CHECK hour {hour}: no compacted boundary acknowledged yet");
            }
            let log = fs::read_to_string(&self.log).unwrap_or_default();
            let warned: Vec<&str> = log
                .lines()
                .filter(|l| l.contains("WARN") || l.contains("ERROR") || l.contains("degraded"))
                .collect();
            let warnings = warned.len();
            for line in warned.iter().skip(self.warned).take(20) {
                eprintln!("LSR {line}");
            }
            self.warned = warnings;
            eprintln!(
                "HOUR {hour}: commits {} head {:x} acked {} | restore latest {:.2} s target {} | lsr {} | log warnings {warnings} | findings {}",
                self.commits,
                self.last,
                self.acked.len(),
                latest_took.as_secs_f64(),
                target_took.map_or_else(
                    || "none".to_owned(),
                    |t| format!("{:.2} s", t.as_secs_f64())
                ),
                resources(self.child.id()),
                self.findings.len()
            );
        }
    }

    /// Removes a database file the soak made, and the -wal and -shm SQLite leaves beside it.
    fn remove(path: &Path) {
        for suffix in ["", "-wal", "-shm"] {
            let file = PathBuf::from(format!("{}{suffix}", path.display()));
            if file.exists() {
                fs::remove_file(&file).expect("remove a scratch file");
            }
        }
    }

    /// Stops the replicator and deletes the run prefix when the soak ends, passed or not, so
    /// a panic leaves neither a process nor objects behind.
    impl Drop for Soak {
        fn drop(&mut self) {
            if matches!(self.child.try_wait(), Ok(None)) {
                let sent = Command::new("kill")
                    .args(["-INT", &self.child.id().to_string()])
                    .status();
                if sent.is_err() {
                    self.child.kill().expect("kill lsr");
                }
            }
            let status = self.child.wait().expect("wait for lsr");
            eprintln!("STOP lsr replicate: {status}; {}", tail(&self.log));
            let left = delete_all(&self.store);
            eprintln!("CLEANUP {left} objects left under {}", self.bucket.path);
            assert!(std::thread::panicking() || left == 0, "{left} objects left");
        }
    }

    fn tail(log: &Path) -> String {
        let text = fs::read_to_string(log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().rev().take(10).collect();
        lines.into_iter().rev().collect::<Vec<_>>().join(" | ")
    }

    /// Deletes every object under the replica's prefix; returns how many are left.
    fn delete_all(store: &S3Store<UreqTransport>) -> usize {
        for level in Level::ALL {
            let mut after = None;
            loop {
                let page = block_on(store.list(level, after, PageLimit::MAX)).expect("list");
                let keys: Vec<_> = page.files.iter().map(|f| f.key).collect();
                for chunk in keys.chunks(500) {
                    block_on(store.delete(chunk)).expect("delete");
                }
                match page.next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
        }
        Level::ALL
            .iter()
            .map(|&l| {
                let page = block_on(store.list(l, None, PageLimit::MAX)).expect("list");
                page.files.len() + page.skipped
            })
            .sum()
    }

    fn start(bucket: Bucket, work: &Path) -> Soak {
        let db = work.join("app.db");
        let config = work.join("litestream.yml");
        bucket.write_config(&config, &db);
        let wl = Workload::create(&db, 4096, 0x50_a6).expect("workload");
        let log = work.join("lsr.log");
        let out = fs::File::create(&log).expect("log");
        let child = Command::new(env!("CARGO_BIN_EXE_lsr"))
            .args(["replicate", "-config"])
            .arg(&config)
            .stdout(Stdio::from(out.try_clone().expect("log")))
            .stderr(Stdio::from(out))
            .spawn()
            .expect("start lsr replicate");
        let store = bucket.store();
        let scratch = work.join("scratch");
        fs::create_dir_all(&scratch).expect("scratch");
        Soak {
            bucket,
            store,
            wl,
            db,
            scratch,
            log,
            child,
            rng: SplitMix::new(0x50a6_7e57),
            commits: 0,
            last: 0,
            acked: std::collections::BTreeMap::new(),
            findings: Vec::new(),
            warned: 0,
        }
    }

    #[test]
    #[ignore = "needs the Tigris bucket and hours (just soak-tigris)"]
    #[expect(
        clippy::disallowed_methods,
        reason = "a soak runs for a wall-clock length"
    )]
    fn lsr_replicate_soaks_against_tigris() {
        let hours = number("LSR_SOAK_HOURS", 72.0);
        let every = Duration::from_secs_f64(number("LSR_SOAK_CHECK_SECS", 3600.0));
        let total = Duration::from_secs_f64(hours * 3600.0);
        let work = tempfile::tempdir().expect("tempdir");
        let mut soak = start(Bucket::from_env(), work.path());
        eprintln!(
            "SOAK {hours} h, a check every {every:?}, prefix {}",
            soak.bucket.path
        );
        let (began, mut next, mut hour, mut i) = (Instant::now(), Instant::now() + every, 0, 0u64);
        while began.elapsed() < total {
            for step in steady(i) {
                soak.commit(step);
            }
            if i % 600 == 400 {
                soak.recreate_scratch();
            }
            if i % 97 == 50 {
                soak.outside_checkpoint();
            }
            i += 1;
            if Instant::now() >= next {
                hour += 1;
                soak.check(hour);
                next += every;
            }
        }
        soak.check(hour + 1);
        let findings = std::mem::take(&mut soak.findings);
        drop(soak);
        assert!(findings.is_empty(), "findings: {findings:#?}");
    }
}
