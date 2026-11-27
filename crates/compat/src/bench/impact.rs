//! What a full copy does to an application beside it (issue 315): the set [`snapshot-impact`].
//!
//! Mechanism. A database twice the machine's memory is built once ([`hot::build`]) and reused. A
//! run starts the application ([`hot`]: a hot set read at a fixed rate, a little write traffic),
//! lets it run alone for [`plan::IMPACT_PRE`], then (in every arm but A) starts [`lsr replicate`]
//! on an empty replica prefix, which takes the boot snapshot: a full level-9 copy of every page.
//! The copy's window runs from lsr's start to the snapshot's appearance in the bucket; the
//! application's latencies in that window are the primary metric. Arm A has no copy and uses a
//! window as long as the calibration copy. Every run starts with a cold page cache, a warmed hot
//! set, and an empty replica.
//!
//! The arms ([`plan::ARMS`]) are lsr as it is (B), and binaries built from patches kept beside
//! the bench ([`crates/compat/bench-patches`], built by [`scripts/build-bench-arms.sh`]) that read
//! with a pace or read twice (the known-bad
//! control). None of them is in the engine. The experiment's order is: a calibration round (each
//! arm once, verified by restore, which also fixes the copy's duration and rate), the A/A gate (the
//! noise floor, and from it the rounds), then the rounds, one run of every arm each, the order
//! rotated. [`summary`] prints the table.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use lsr_core::RestoreTarget;
use rusqlite::Connection;

use super::clock::{now, pause};
use super::hot::{self, Job, Sample};
use super::observe::Observer;
use super::plan::{self, ARMS, ArmDef};
use super::proc::{Managed, Usage};
use super::runs::{Cell, METRICS, Phase, Run, Store, at};
use super::summary;
use super::sys::{self, Steal, Vm};
use super::tally;
use super::{Program, Rig, mib, preserve, secs, to_f64};
use crate::error::CompatError;
use crate::pages::hash_pages;
use crate::replicator::wait_until;

/// What the command line may change. Anything changed is not the frozen plan, and the report
/// says so.
#[derive(Debug, Clone, Default)]
pub(super) struct Options {
    pub db_mib: Option<u64>,
    pub hot_mib: Option<u64>,
    pub pre_secs: Option<u64>,
    pub post_secs: Option<u64>,
    pub rounds: Option<usize>,
    pub gate_pairs: Option<usize>,
    /// The directory of [`<patch>/lsr`] binaries built from [`crates/compat/bench-patches`].
    pub patched: Option<PathBuf>,
    /// Run on after a failed gate.
    pub force: bool,
    /// Only these arms (A and B always run).
    pub only: Vec<String>,
}

impl Options {
    /// What differs from the frozen plan, for the banner; empty when nothing does.
    fn overrides(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut note = |name: &str, set: bool| {
            if set {
                out.push(name.to_owned());
            }
        };
        note(
            "--db-mib",
            self.db_mib.is_some_and(|v| v != plan::IMPACT_DB_MIB),
        );
        note(
            "--hot-mib",
            self.hot_mib.is_some_and(|v| v != plan::IMPACT_HOT_MIB),
        );
        note("--pre-secs", self.pre_secs.is_some());
        note("--post-secs", self.post_secs.is_some());
        note("--gate-pairs", self.gate_pairs.is_some());
        note("--arms", !self.only.is_empty());
        out
    }
}

/// What calibration and the gate fixed; written to [`frame.env`] so a restart keeps them.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Frame {
    /// The calibration copy's wall time: the window of arm A.
    window_s: f64,
    /// The rate B's calibration copy read the database at, MiB a second.
    base_rate_mib_s: f64,
    /// Rounds the A/A noise asked for.
    rounds: usize,
}

impl Frame {
    fn load(path: &Path) -> Self {
        let text = fs::read_to_string(path).unwrap_or_default();
        let get = |key: &str| -> f64 {
            text.lines()
                .find_map(|l| l.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0.0)
        };
        Self {
            window_s: get("window_s"),
            base_rate_mib_s: get("base_rate_mib_s"),
            rounds: text
                .lines()
                .find_map(|l| l.strip_prefix("rounds="))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0),
        }
    }

    fn save(&self, path: &Path) -> Result<(), CompatError> {
        let text = format!(
            "window_s={}\nbase_rate_mib_s={}\nrounds={}\n",
            self.window_s, self.base_rate_mib_s, self.rounds
        );
        fs::write(path, text).map_err(CompatError::io(path))
    }

    fn window(&self) -> Duration {
        Duration::try_from_secs_f64(self.window_s).unwrap_or(Duration::ZERO)
    }
}

/// Everything a run needs, fixed for the invocation.
struct Ctx<'a> {
    rig: &'a Rig,
    dir: PathBuf,
    db: PathBuf,
    rows: u64,
    hot_pages: u64,
    pre: Duration,
    post: Duration,
    head: Program,
    patched: Vec<(String, Program)>,
    store: Store,
    frame: Frame,
    arms: Vec<ArmDef>,
}

impl Ctx<'_> {
    fn program_for(&self, arm: &ArmDef) -> Result<&Program, CompatError> {
        match arm.patch {
            None => Ok(&self.head),
            Some(name) => self
                .patched
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, p)| p)
                .ok_or_else(|| {
                    CompatError::Fixture(format!(
                        "arm {} needs the binary of patch {name}: run scripts/build-bench-arms.sh and pass --patched DIR",
                        arm.label
                    ))
                }),
        }
    }

    fn pace_for(&self, arm: &ArmDef) -> Option<String> {
        Some(pace_mib_s(self.frame.base_rate_mib_s, arm.pace_divisor?))
    }
}

/// A paced arm's rate in whole MiB a second: the calibrated rate over the divisor, at least 1.
fn pace_mib_s(base_mib_s: f64, divisor: u64) -> String {
    let rate = (base_mib_s / to_f64(divisor)).floor().max(1.0);
    format!("{rate:.0}")
}

/// The application child, killed if dropped.
struct HotApp {
    child: Child,
    job: Job,
}

impl HotApp {
    fn start(ctx: &Ctx<'_>, dir: &Path, tag: &str) -> Result<Self, CompatError> {
        let job = Job {
            db: ctx.db.clone(),
            total_rows: ctx.rows,
            hot_pages: ctx.hot_pages,
            reads_per_s: plan::IMPACT_READS_PER_S,
            readers: plan::IMPACT_READERS,
            writes_per_s: plan::IMPACT_WRITES_PER_S,
            result: dir.join(format!("{tag}.app")),
            ready: dir.join(format!("{tag}.ready")),
            stop: dir.join(format!("{tag}.stop")),
        };
        for stale in [&job.ready, &job.stop] {
            drop(fs::remove_file(stale));
        }
        let log = dir.join(format!("{tag}.app.log"));
        let out = fs::File::create(&log).map_err(CompatError::io(&log))?;
        let err = out.try_clone().map_err(CompatError::io(&log))?;
        let child = Command::new(&ctx.rig.exe)
            .arg(hot::HOT)
            .args(job.args())
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .map_err(CompatError::io(&ctx.rig.exe))?;
        let mut app = Self { child, job };
        let ready = app.job.ready.clone();
        wait_until("the hot set to warm", plan::IMPACT_WARM_LIMIT, || {
            if let Some(status) = app.child.try_wait().map_err(CompatError::io(&log))? {
                return Err(CompatError::Process {
                    command: "hot-app".to_owned(),
                    detail: format!(
                        "exited early with {status}: {}",
                        fs::read_to_string(&log).unwrap_or_default()
                    ),
                });
            }
            Ok(ready.exists())
        })?;
        Ok(app)
    }

    /// Stops the application and returns its reads, its commits and its major faults.
    fn finish(mut self) -> Result<(Vec<Sample>, Vec<Sample>, u64), CompatError> {
        fs::write(&self.job.stop, b"stop").map_err(CompatError::io(&self.job.stop))?;
        let mut status = None;
        wait_until(
            "the hot app to write its results",
            Duration::from_secs(300),
            || {
                status = self
                    .child
                    .try_wait()
                    .map_err(CompatError::io(&self.job.stop))?;
                Ok(status.is_some())
            },
        )?;
        if !status.is_some_and(|s| s.success()) {
            return Err(CompatError::Process {
                command: "hot-app".to_owned(),
                detail: format!("ended with {status:?}"),
            });
        }
        let with = |ext: &str| PathBuf::from(format!("{}.{ext}", self.job.result.display()));
        let reads = hot::read_samples(&with("reads"))?;
        let writes = hot::read_samples(&with("writes"))?;
        let faults = hot::summary_major_faults(&self.job.result)?;
        for ext in ["reads", "writes"] {
            drop(fs::remove_file(with(ext)));
        }
        Ok((reads, writes, faults))
    }
}

impl Drop for HotApp {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            drop(self.child.kill());
            drop(self.child.wait());
        }
    }
}

/// The copy's window and what was seen while it ran.
#[derive(Debug)]
struct Window {
    start_ns: u64,
    end_ns: u64,
    wall: Duration,
    timed_out: bool,
    vm: Vm,
    steal_pct: f64,
    read_mib: f64,
    rchar_mib: f64,
    wal_growth_mib: f64,
    wal_peak_mib: f64,
    current_s: f64,
}

fn wal_len(db: &Path) -> u64 {
    let wal = PathBuf::from(format!("{}-wal", db.display()));
    fs::metadata(wal).map_or(0, |m| m.len())
}

/// A TRUNCATE checkpoint with no other connection: the file holds every page, the WAL is gone.
fn checkpoint_truncate(db: &Path) -> Result<(), CompatError> {
    let conn = Connection::open(db)?;
    let (busy, _log, _done): (i64, i64, i64) =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    if busy != 0 {
        return Err(CompatError::Fixture(
            "the checkpoint between runs was blocked: something still holds the database"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Leaves the database as a fresh start: WAL truncated, the replicator's state of the last run gone.
fn prepare_db(db: &Path) -> Result<(), CompatError> {
    checkpoint_truncate(db)?;
    if let Some(dir) = db.file_name().map(|n| {
        let mut meta = OsString::from(".");
        meta.push(n);
        meta.push("-litestream");
        db.with_file_name(meta)
    }) {
        drop(fs::remove_dir_all(dir));
    }
    Ok(())
}

/// Waits for the snapshot to land, sampling the WAL; returns whether the copy timed out, and the
/// WAL's peak length.
fn watch_copy(
    process: &mut Managed,
    s3: &crate::s3::S3Replica,
    db: &Path,
) -> Result<(bool, u64), CompatError> {
    let began = now();
    let mut peak = 0_u64;
    loop {
        process.ensure_running()?;
        peak = peak.max(wal_len(db));
        if !s3.snapshots()?.is_empty() {
            return Ok((false, peak));
        }
        if now().duration_since(began) > plan::IMPACT_COPY_LIMIT {
            return Ok((true, peak));
        }
        pause(Duration::from_millis(250));
    }
}

/// Seconds from [`began`] until the replica holds every commit made before now; NaN when it
/// does not within the limit.
fn time_to_current(
    process: &mut Managed,
    s3: &crate::s3::S3Replica,
    ctx: &Ctx<'_>,
    began: std::time::Instant,
) -> Result<f64, CompatError> {
    let (target, page) = hot::marker(&ctx.db)?;
    let observer = Observer::new(s3, page);
    let got = wait_until(
        "the replica to hold every commit made before the snapshot landed",
        plan::IMPACT_CURRENT_LIMIT,
        || {
            process.ensure_running()?;
            Ok(observer.tail_marker()?.is_some_and(|m| m >= target))
        },
    );
    match got {
        Ok(()) => Ok(secs(now().duration_since(began))),
        Err(CompatError::Timeout(_)) => Ok(f64::NAN),
        Err(other) => Err(other),
    }
}

/// The copy window of an arm that copies: starts lsr and waits for the snapshot. Returns the
/// still-running replicator with the window.
fn copy_window(
    ctx: &Ctx<'_>,
    arm: &ArmDef,
    dir: &Path,
    tag: &str,
    s3: &crate::s3::S3Replica,
) -> Result<(Managed, Window), CompatError> {
    let program = ctx.program_for(arm)?;
    let config = dir.join(format!("{tag}.yml"));
    let text = format!(
        "dbs:\n  - path: {}\n{}",
        ctx.db.display(),
        s3.yaml_defaults()
    );
    fs::write(&config, text).map_err(CompatError::io(&config))?;
    let args = [
        OsString::from("replicate"),
        OsString::from("-config"),
        config.into(),
    ];
    let args: Vec<&OsStr> = args.iter().map(OsString::as_os_str).collect();
    let env: Vec<(&str, String)> = ctx
        .pace_for(arm)
        .map(|rate| (plan::PACE_ENV, rate))
        .into_iter()
        .collect();
    let (vm0, steal0, wal0) = (Vm::now(), Steal::now(), wal_len(&ctx.db));
    let start_ns = hot::unix_ns();
    let began = now();
    let mut process = Managed::start_with_env(&ctx.rig.exe, &program.path, &args, dir, tag, &env)?;
    process.wait_up(plan::BOOT_LIMIT)?;
    let (timed_out, peak) =
        watch_copy(&mut process, s3, &ctx.db).map_err(|e| ctx.rig.explain(&mut process, e))?;
    let (end_ns, wall) = (hot::unix_ns(), now().duration_since(began));
    let io = sys::proc_read_bytes(process.pid());
    let vm = vm0
        .zip(Vm::now())
        .map(|(a, b)| a.since(b))
        .unwrap_or_default();
    let steal_pct = steal0
        .zip(Steal::now())
        .map_or(f64::NAN, |(a, b)| a.percent_until(b));
    let wal_end = wal_len(&ctx.db);
    let current_s = if timed_out {
        f64::NAN
    } else {
        time_to_current(&mut process, s3, ctx, began)
            .map_err(|e| ctx.rig.explain(&mut process, e))?
    };
    let window = Window {
        start_ns,
        end_ns,
        wall,
        timed_out,
        vm,
        steal_pct,
        read_mib: io.map_or(f64::NAN, |(_, read)| mib(read)),
        rchar_mib: io.map_or(f64::NAN, |(rchar, _)| mib(rchar)),
        wal_growth_mib: mib(wal_end.saturating_sub(wal0)),
        wal_peak_mib: mib(peak),
        current_s,
    };
    Ok((process, window))
}

/// The window of arm A: nothing copies, and the application is watched for as long as the
/// calibration copy lasted.
fn idle_window(ctx: &Ctx<'_>) -> Window {
    let (vm0, steal0, wal0) = (Vm::now(), Steal::now(), wal_len(&ctx.db));
    let start_ns = hot::unix_ns();
    let began = now();
    pause(ctx.frame.window());
    let (end_ns, wall) = (hot::unix_ns(), now().duration_since(began));
    Window {
        start_ns,
        end_ns,
        wall,
        timed_out: false,
        vm: vm0
            .zip(Vm::now())
            .map(|(a, b)| a.since(b))
            .unwrap_or_default(),
        steal_pct: steal0
            .zip(Steal::now())
            .map_or(f64::NAN, |(a, b)| a.percent_until(b)),
        read_mib: f64::NAN,
        rchar_mib: f64::NAN,
        wal_growth_mib: mib(wal_len(&ctx.db).saturating_sub(wal0)),
        wal_peak_mib: mib(wal_len(&ctx.db)),
        current_s: f64::NAN,
    }
}

fn latencies_in(samples: &[Sample], from: u64, to: u64) -> Vec<u64> {
    let mut found: Vec<u64> = samples
        .iter()
        .filter(|(at, _)| *at >= from && *at < to)
        .map(|(_, took)| *took)
        .collect();
    found.sort_unstable();
    found
}

fn ms(ns: u64) -> f64 {
    to_f64(ns) / 1e6
}

/// A run's numbers from its window, its samples and the replicator's usage.
fn measure(
    pre_start: u64,
    window: &Window,
    reads: &[Sample],
    writes: &[Sample],
    app_faults: u64,
    usage: Option<Usage>,
) -> Run {
    let during = latencies_in(reads, window.start_ns, window.end_ns);
    let before = latencies_in(reads, pre_start, window.start_ns);
    let commits = latencies_in(writes, window.start_ns, window.end_ns);
    let kilo = |n: u64| to_f64(n) / 1000.0;
    Run {
        values: vec![
            ms(tally::percentile(&during, 500)),
            ms(tally::percentile(&during, 990)),
            ms(tally::percentile(&during, 999)),
            ms(tally::percentile(&before, 990)),
            ms(tally::percentile(&commits, 990)),
            to_f64(app_faults),
            to_f64(window.vm.major_faults),
            kilo(window.vm.scanned),
            kilo(window.vm.stolen),
            kilo(window.vm.refaults),
            to_f64(window.vm.alloc_stalls),
            secs(window.wall),
            window.read_mib,
            window.rchar_mib,
            window.wal_growth_mib,
            window.wal_peak_mib,
            window.current_s,
            window.steal_pct,
            usage.map_or(f64::NAN, |u| secs(u.cpu)),
            usage.map_or(f64::NAN, |u| mib(u.peak_rss)),
            if window.timed_out { 1.0 } else { 0.0 },
            to_f64(u64::try_from(during.len()).unwrap_or(0)),
            ms(tally::percentile(&commits, 500)),
        ],
    }
}

/// Restores the replica and compares its pages with the database's (never bytes).
fn verify_replica(
    ctx: &Ctx<'_>,
    s3: &crate::s3::S3Replica,
    dir: &Path,
    tag: &str,
) -> Result<(), CompatError> {
    checkpoint_truncate(&ctx.db)?;
    let source = hash_pages(&ctx.db)?;
    let out = dir.join("restored.db");
    s3.restore(RestoreTarget::Latest, &out)?
        .map_err(|e| CompatError::Fixture(format!("restoring the replica: {e}")))?;
    let restored = hash_pages(&out)?;
    drop(fs::remove_file(&out));
    if restored != source {
        return Err(CompatError::Fixture(format!(
            "{tag}: the restored pages differ from the database's"
        )));
    }
    Ok(())
}

/// One run of [`arm`]: cold cache, warm hot set, the application alone, the copy, the
/// application after.
fn run_one(ctx: &Ctx<'_>, arm: &ArmDef, tag: &str, verify: bool) -> Result<Run, CompatError> {
    eprintln!("bench: {tag}");
    let dir = ctx.rig.run_dir(tag)?;
    prepare_db(&ctx.db)?;
    let dropped = sys::drop_page_cache(&ctx.db);
    let s3 = ctx.rig.replica(tag)?;
    let app = HotApp::start(ctx, &dir, tag)?;
    let pre_start = hot::unix_ns();
    pause(ctx.pre);
    let (process, window) = if arm.idle {
        (None, idle_window(ctx))
    } else {
        let (p, w) = copy_window(ctx, arm, &dir, tag, &s3)?;
        (Some(p), w)
    };
    pause(ctx.post);
    let (reads, writes, faults) = app.finish()?;
    let usage = process.map(Managed::stop).transpose()?;
    if verify && !arm.idle {
        verify_replica(ctx, &s3, &dir, tag)?;
    }
    s3.delete_all()?;
    let run = measure(pre_start, &window, &reads, &writes, faults, usage);
    eprintln!(
        "bench:   cache {dropped}; p99 {:.3} ms, copy {:.1} s, {} reads in the window{}",
        run.get(at::P99),
        run.get(at::COPY_S),
        run.get(METRICS.len().saturating_sub(1)),
        if verify && !arm.idle {
            ", replica verified"
        } else {
            ""
        },
    );
    Ok(run)
}

/// The run at [`cell`] from the store, or a fresh one that is stored.
fn run_cell(ctx: &mut Ctx<'_>, cell: Cell, arm: &ArmDef, verify: bool) -> Result<Run, CompatError> {
    if let Some(done) = ctx.store.find(&cell) {
        return Ok(done.clone());
    }
    let tag = format!(
        "{}-{}-{}",
        cell.phase.name(),
        cell.round,
        arm.label.replace('/', "-")
    );
    let run = run_one(ctx, arm, &tag, verify)?;
    ctx.store.push(cell, run.clone())?;
    Ok(run)
}

fn arm_named<'a>(arms: &'a [ArmDef], label: &str) -> Result<&'a ArmDef, CompatError> {
    arms.iter()
        .find(|a| a.label == label)
        .ok_or_else(|| CompatError::Fixture(format!("no arm {label}")))
}

/// Every arm once, B first so the window and the rates exist, each verified by restore.
fn calibrate(ctx: &mut Ctx<'_>) -> Result<(), CompatError> {
    println!(
        "\n#### calibration (every arm once, unmeasured; each replica is restored and compared)"
    );
    let b = *arm_named(&ctx.arms, "B")?;
    let done = run_cell(ctx, cell(Phase::Calibration, 0, "B"), &b, true)?;
    if ctx.frame.window_s <= 0.0 {
        let copy_s = done.get(at::COPY_S);
        let bytes = fs::metadata(&ctx.db)
            .map_err(CompatError::io(&ctx.db))?
            .len();
        if done.get(at::TIMED_OUT) > 0.0 || !copy_s.is_finite() || copy_s <= 0.0 {
            return Err(CompatError::Fixture(
                "the calibration copy did not finish: the rates cannot be fixed".to_owned(),
            ));
        }
        ctx.frame.window_s = copy_s;
        ctx.frame.base_rate_mib_s = to_f64(plan::COPY_PASSES) * mib(bytes) / copy_s;
        ctx.frame.save(&ctx.dir.join("frame.env"))?;
    }
    println!(
        "calibration: B copied in {:.1} s ({:.0} MiB/s); A's window is that long; paced arms read at {}",
        ctx.frame.window_s,
        ctx.frame.base_rate_mib_s,
        ctx.arms
            .iter()
            .filter_map(|a| ctx.pace_for(a).map(|r| format!("{} {r} MiB/s", a.label)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    for arm in ctx.arms.clone().iter().filter(|a| a.label != "B") {
        run_cell(ctx, cell(Phase::Calibration, 0, arm.label), arm, true)?;
    }
    Ok(())
}

fn cell(phase: Phase, round: usize, arm: &str) -> Cell {
    Cell {
        phase,
        round,
        arm: arm.to_owned(),
    }
}

/// A against A: the noise floor of the primary metric, and from it the number of rounds.
/// Returns whether the gate passed.
fn gate(ctx: &mut Ctx<'_>, pairs: usize, forced: bool) -> Result<bool, CompatError> {
    println!("\n#### A/A gate ({pairs} pairs of arm A against itself)");
    let a = *arm_named(&ctx.arms, "A")?;
    for pair in 0..pairs {
        for name in ["A#1", "A#2"] {
            run_cell(ctx, cell(Phase::Gate, pair, name), &a, false)?;
        }
    }
    let series = |index: usize| -> Vec<(f64, f64)> {
        let first = ctx.store.arm(Phase::Gate, "A#1");
        let second = ctx.store.arm(Phase::Gate, "A#2");
        first
            .iter()
            .zip(&second)
            .map(|((_, x), (_, y))| (x.get(index), y.get(index)))
            .collect()
    };
    let p99 = tally::paired(&series(at::P99));
    let p50 = tally::paired(&series(at::P50));
    println!("A/A p50: {}", tally::show(p50));
    println!("A/A p99: {}", tally::show(p99));
    let Some(p99) = p99 else {
        return Err(CompatError::Fixture(
            "the A/A gate has fewer than two pairs".to_owned(),
        ));
    };
    let drift = p99.lo > 0.0 || p99.hi < 0.0;
    let blind = p99.mde_pct() > plan::IMPACT_SESOI;
    let rounds = tally::pairs_needed(
        p99.sd_log,
        plan::IMPACT_SESOI,
        plan::IMPACT_MIN_ROUNDS,
        plan::IMPACT_MAX_ROUNDS,
    );
    println!(
        "gate: interval {} 0, half width {:.1}% against SESOI {}% ({}); sd of the log ratio {:.3}; rounds needed {rounds}",
        if drift { "EXCLUDES" } else { "contains" },
        p99.mde_pct(),
        plan::IMPACT_SESOI,
        if blind { "TOO WIDE" } else { "fine enough" },
        p99.sd_log,
    );
    if ctx.frame.rounds == 0 {
        ctx.frame.rounds = rounds;
        ctx.frame.save(&ctx.dir.join("frame.env"))?;
    }
    let passed = !drift && !blind;
    if !passed && !forced {
        println!(
            "THE GATE FAILED: this box cannot see the SESOI; the arms do not run (--force runs them anyway, unmeasured claims only)"
        );
    }
    Ok(passed)
}

/// The arms in the order of round [`round`]: the list rotated by the round number, reversed in
/// every second lap of that rotation, so every arm meets every position and no two of the first
/// `2 * arms` rounds share an order (an even count of arms would repeat if it reversed on odd
/// rounds).
fn order(arms: &[ArmDef], round: usize) -> Vec<ArmDef> {
    let mut list = arms.to_vec();
    if let Some(len) = NonZeroUsize::new(list.len()) {
        list.rotate_left(round.checked_rem(len.get()).unwrap_or(0));
        if !round.checked_div(len.get()).unwrap_or(0).is_multiple_of(2) {
            return list;
        }
        list.reverse();
    }
    list
}

fn rounds(ctx: &mut Ctx<'_>, total: usize) -> Result<(), CompatError> {
    println!("\n#### {total} rounds of {} arms", ctx.arms.len());
    for round in 0..total {
        for arm in order(&ctx.arms.clone(), round) {
            run_cell(ctx, cell(Phase::Round, round, arm.label), &arm, false)?;
        }
    }
    Ok(())
}

/// The database: built once, reused while it has the size asked for.
fn ensure_db(ctx_dir: &Path, rows: u64) -> Result<(PathBuf, u32), CompatError> {
    let db = ctx_dir.join("app.db");
    let marker = ctx_dir.join("app.db.rows");
    let built = fs::read_to_string(&marker)
        .ok()
        .and_then(|t| t.trim().parse::<u64>().ok());
    if built == Some(rows) && db.exists() {
        let pages = u32::try_from(
            fs::metadata(&db)
                .map_err(CompatError::io(&db))?
                .len()
                .checked_div(u64::from(super::PAGE_SIZE))
                .unwrap_or(0),
        )
        .unwrap_or(u32::MAX);
        return Ok((db, pages));
    }
    for suffix in ["", "-wal", "-shm"] {
        drop(fs::remove_file(format!("{}{suffix}", db.display())));
    }
    println!("building the database: {rows} rows (this takes minutes)");
    let began = now();
    let pages = hot::build(&db, rows)?;
    fs::write(&marker, rows.to_string()).map_err(CompatError::io(&marker))?;
    println!(
        "built {pages} pages ({:.2} GiB) in {:.0} s",
        mib(u64::from(pages).saturating_mul(u64::from(super::PAGE_SIZE))) / 1024.0,
        secs(now().duration_since(began))
    );
    Ok((db, pages))
}

/// The entry of the set: [`just bench-snapshot-impact`].
pub(super) fn run(rig: &Rig, opts: &Options) -> Result<(), CompatError> {
    if !cfg!(target_os = "linux") && std::env::var_os("LSR_BENCH_SHAKEDOWN").is_none() {
        return Err(CompatError::Fixture(
            "snapshot-impact times only on Linux (page cache, /proc); LSR_BENCH_SHAKEDOWN=1 runs it as a check of the harness, and its numbers are not results"
                .to_owned(),
        ));
    }
    let dir = rig.out.join("snapshot-impact");
    fs::create_dir_all(&dir).map_err(CompatError::io(&dir))?;
    let overrides = opts.overrides();
    println!("{}", plan::IMPACT_FRAME);
    if !overrides.is_empty() || !cfg!(target_os = "linux") {
        println!(
            "NOT THE FROZEN PLAN ({}{}): harness check only, none of these numbers is a result",
            overrides.join(", "),
            if cfg!(target_os = "linux") {
                ""
            } else {
                "; not Linux"
            }
        );
    }
    println!("\nmachine record:");
    let mut record = String::new();
    for (name, value) in sys::machine_record(&dir) {
        println!("  {name}: {value}");
        record.push_str(&format!("{name}: {value}\n"));
    }
    let steal_before = Steal::now();
    record.push_str(&format!(
        "steal before: {}\n",
        steal_before.map_or("unknown".to_owned(), |s| format!("{s:?}"))
    ));
    fs::write(dir.join("machine.txt"), record).map_err(CompatError::io(&dir))?;
    fs::write(dir.join("frame.txt"), plan::IMPACT_FRAME).map_err(CompatError::io(&dir))?;
    let db_mib = opts.db_mib.unwrap_or(plan::IMPACT_DB_MIB);
    let rows = hot::rows_for(db_mib);
    let (db, pages) = ensure_db(&dir, rows)?;
    println!("database: {pages} pages, {rows} rows, {}", db.display());
    if let Some(bw) = sys::disk_read_mb_s(&db, db_mib.min(2048)) {
        println!(
            "disk: sequential direct read {bw:.0} MB/s, so the copy's read floor is {:.0} s",
            to_f64(u64::from(pages).saturating_mul(u64::from(super::PAGE_SIZE))) / 1e6 / bw
        );
    }
    let hot_pages = opts
        .hot_mib
        .unwrap_or(plan::IMPACT_HOT_MIB)
        .saturating_mul(256);
    let arms: Vec<ArmDef> = ARMS
        .iter()
        .filter(|a| {
            opts.only.is_empty()
                || a.label == "A"
                || a.label == "B"
                || opts.only.iter().any(|o| o == a.label)
        })
        .copied()
        .collect();
    let head = preserve(&rig.out, "head", &super::lsr_release())?;
    let mut patched = Vec::new();
    for name in plan::PATCHES {
        if arms.iter().any(|a| a.patch == Some(name)) {
            let from = opts
                .patched
                .as_ref()
                .map(|d| d.join(name).join("lsr"))
                .ok_or_else(|| {
                    CompatError::Fixture(
                        "--patched DIR is required for the patched arms".to_owned(),
                    )
                })?;
            patched.push((name.to_owned(), preserve(&rig.out, name, &from)?));
        }
    }
    let frame = Frame::load(&dir.join("frame.env"));
    let store = Store::open(&dir.join("runs.csv"))?;
    let mut ctx = Ctx {
        rig,
        dir: dir.clone(),
        db,
        rows,
        hot_pages,
        pre: opts.pre_secs.map_or(plan::IMPACT_PRE, Duration::from_secs),
        post: opts
            .post_secs
            .map_or(plan::IMPACT_POST, Duration::from_secs),
        head,
        patched,
        store,
        frame,
        arms,
    };
    calibrate(&mut ctx)?;
    let gate_pairs = opts.gate_pairs.unwrap_or(plan::IMPACT_GATE_PAIRS);
    let passed = gate(&mut ctx, gate_pairs, opts.force)?;
    if !passed && !opts.force {
        return Err(CompatError::Fixture(
            "the A/A gate failed: this box cannot see the SESOI in p99 (see above)".to_owned(),
        ));
    }
    let total = opts.rounds.unwrap_or(ctx.frame.rounds).max(1);
    rounds(&mut ctx, total)?;
    summary::print(&ctx.store, &ctx.arms, total, &dir)?;
    if let (Some(a), Some(b)) = (steal_before, Steal::now()) {
        println!("steal over the whole session: {:.2}%", a.percent_until(b));
        let path = dir.join("machine.txt");
        let mut text = fs::read_to_string(&path).unwrap_or_default();
        text.push_str(&format!("steal after: {b:?}\n"));
        fs::write(&path, text).map_err(CompatError::io(&path))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_round_runs_every_arm_once_and_the_orders_differ() {
        let arms = ARMS.to_vec();
        let mut seen = std::collections::BTreeSet::new();
        for round in 0..arms.len().saturating_mul(2) {
            let list = order(&arms, round);
            assert_eq!(list.len(), arms.len());
            let mut labels: Vec<&str> = list.iter().map(|a| a.label).collect();
            seen.insert(labels.clone());
            labels.sort_unstable();
            labels.dedup();
            assert_eq!(labels.len(), arms.len(), "round {round} repeats an arm");
        }
        assert!(seen.len() > arms.len(), "the orders rotate and reverse");
    }

    #[test]
    fn the_frame_survives_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("frame.env");
        let frame = Frame {
            window_s: 41.5,
            base_rate_mib_s: 812.25,
            rounds: 12,
        };
        frame.save(&path).expect("save");
        assert_eq!(Frame::load(&path), frame);
        assert_eq!(Frame::load(&dir.path().join("none")), Frame::default());
        assert_eq!(frame.window(), Duration::from_secs_f64(41.5));
    }

    #[test]
    fn only_a_changed_setting_makes_the_run_not_the_plan() {
        assert!(Options::default().overrides().is_empty());
        let same = Options {
            db_mib: Some(plan::IMPACT_DB_MIB),
            hot_mib: Some(plan::IMPACT_HOT_MIB),
            rounds: Some(3),
            ..Options::default()
        };
        assert!(same.overrides().is_empty(), "rounds only shortens the run");
        let small = Options {
            db_mib: Some(64),
            pre_secs: Some(1),
            ..Options::default()
        };
        assert_eq!(small.overrides(), ["--db-mib", "--pre-secs"]);
    }

    #[test]
    fn paced_arms_read_at_a_fraction_of_the_calibrated_rate_and_at_least_one() {
        assert_eq!(pace_mib_s(100.0, 2), "50");
        assert_eq!(pace_mib_s(100.0, 8), "12");
        assert_eq!(pace_mib_s(3.0, 8), "1");
    }
}
