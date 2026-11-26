//! A benchmark of `lsr` against Go Litestream on one machine and one local MinIO
//! (`just bench-vs-go`, issue 42).
//!
//! Mechanism. Three measurements, each by interleaved pairs (Go, lsr, lsr, Go, ...), one
//! number per run, the pairs of a metric written to a CSV that `perfstats.py compare` reads:
//! [`latency`] (commit to durable, p50 and p99, steady and bursty writers), [`overhead`] (CPU
//! seconds per GiB of WAL, peak memory) and [`restore`] (wall time and peak memory of a 1 GiB
//! restore from one replica). Both programs run at their default settings from one config file
//! that names only the replica. Every number is exploratory: a laptop has no isolation, so
//! each report says so and names the machine, and an A/A run (the same binary as both arms)
//! gives the noise floor. The workloads are frozen in [`plan`].
//!
//! Programs run under a wrapper ([`proc`]) so each one's CPU time and peak memory are its own.
//! The observer ([`observe`]) watches the bucket directly, not the objects' one-second
//! `LastModified`; a replica is restored and compared to the source (page hashes, never
//! bytes) before a run's numbers count where the run can check it.
//!
//! A fourth set, [`impact`] (`just bench-snapshot-impact`, issue 315), is not a comparison with
//! Go: it measures what a full copy of a database larger than memory does to an application
//! reading a hot set beside it, and which remedy (patches kept in `crates/compat/bench-patches`)
//! brings it back. It has its own frame in [`plan`] and needs no Go build.
//!
//! The binary `bench-vs-go` is a thin shell around [`run`]. It needs MinIO through
//! `lsr_core::needs` and is never part of `just check`.

mod app;
mod clock;
mod hot;
mod impact;
mod latency;
mod observe;
mod overhead;
mod plan;
mod proc;
mod restore;
mod runs;
mod summary;
mod sys;
mod tally;

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use lsr_core::needs::{Resource, need, reachable};
use sha2::{Digest, Sha256};

use self::app::PAGE_SIZE;
use self::latency::Shape;
use self::proc::{EXEC, Managed, Usage, exec_wrapper};
use crate::binaries::{GoVersion, go_binary, workspace_root};
use crate::error::CompatError;
use crate::replicator::wait_until;
use crate::s3::{MinioEnv, S3Replica};

/// Bytes in a GiB.
const GIB: u64 = 1 << 30;

/// An integer as a float; exact below 2^53, far above any figure here.
fn to_f64(n: u64) -> f64 {
    let hi = u32::try_from(n.wrapping_shr(32)).unwrap_or(u32::MAX);
    let lo = u32::try_from(n & 0xFFFF_FFFF).unwrap_or(u32::MAX);
    f64::from(hi) * 4_294_967_296.0 + f64::from(lo)
}

/// Bytes as MiB.
fn mib(bytes: u64) -> f64 {
    to_f64(bytes) / 1_048_576.0
}

/// A duration in seconds.
fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

/// A program under test: a label for the tables and a binary.
#[derive(Debug, Clone)]
struct Program {
    pub label: String,
    pub path: PathBuf,
}

/// The harness: the MinIO, this binary (the wrapper), and the output directory.
#[derive(Debug)]
struct Rig {
    env: MinioEnv,
    exe: PathBuf,
    out: PathBuf,
}

impl Rig {
    /// A fresh directory for one run.
    fn run_dir(&self, tag: &str) -> Result<PathBuf, CompatError> {
        let dir = self.out.join("runs").join(tag);
        drop(fs::remove_dir_all(&dir));
        fs::create_dir_all(&dir).map_err(CompatError::io(&dir))?;
        Ok(dir)
    }

    /// The replica of run `tag`: a prefix of its own in the bucket.
    fn replica(&self, tag: &str) -> Result<S3Replica, CompatError> {
        S3Replica::new(&self.env, &format!("bench-{tag}-{}", std::process::id()))
    }

    /// Starts `program replicate -config config` and waits until it runs.
    fn start_replicate(
        &self,
        program: &Program,
        dir: &Path,
        tag: &str,
        config: &Path,
    ) -> Result<Managed, CompatError> {
        let args = [
            OsString::from("replicate"),
            OsString::from("-config"),
            config.into(),
        ];
        let args: Vec<&std::ffi::OsStr> = args.iter().map(OsString::as_os_str).collect();
        let mut process = Managed::start(&self.exe, &program.path, &args, dir, tag)?;
        process.wait_up(plan::BOOT_LIMIT)?;
        Ok(process)
    }

    /// Waits until the replica holds its boot snapshot.
    fn wait_snapshot(&self, process: &mut Managed, s3: &S3Replica) -> Result<(), CompatError> {
        wait_until("the boot snapshot", plan::BOOT_LIMIT, || {
            process.ensure_running()?;
            Ok(!s3.snapshots()?.is_empty())
        })
    }

    /// Writes the default-settings config for `db` and `s3`, starts `program` on it and waits
    /// for its boot snapshot.
    fn boot(
        &self,
        program: &Program,
        dir: &Path,
        tag: &str,
        db: &Path,
        s3: &S3Replica,
    ) -> Result<Managed, CompatError> {
        let config = dir.join(format!("{tag}.yml"));
        let text = format!("dbs:\n  - path: {}\n{}", db.display(), s3.yaml_defaults());
        fs::write(&config, text).map_err(CompatError::io(&config))?;
        let mut process = self.start_replicate(program, dir, tag, &config)?;
        self.wait_snapshot(&mut process, s3)?;
        Ok(process)
    }

    /// `err` with the end of the program's log.
    fn explain(&self, process: &mut Managed, err: CompatError) -> CompatError {
        CompatError::Process {
            command: "the replicator".to_owned(),
            detail: format!("{err}; its log ends: {}", process.tail()),
        }
    }

    /// `program restore -config config -o out db`, to its end.
    fn restore(
        &self,
        program: &Program,
        dir: &Path,
        tag: &str,
        config: &Path,
        out: &Path,
        db: &Path,
    ) -> Result<Usage, CompatError> {
        let args = [
            OsString::from("restore"),
            OsString::from("-config"),
            config.into(),
            OsString::from("-o"),
            out.into(),
            db.into(),
        ];
        let args: Vec<&std::ffi::OsStr> = args.iter().map(OsString::as_os_str).collect();
        Managed::start(&self.exe, &program.path, &args, dir, tag)?.finish(plan::RESTORE_LIMIT)
    }
}

/// The pairs of each metric of one comparison: (arm a, arm b) per run, in pair order.
#[derive(Debug)]
struct Series {
    name: String,
    rows: Vec<(f64, f64)>,
}

/// Runs `pairs` interleaved pairs: Go first in even pairs, `b` first in odd ones. `once`
/// returns one value per metric.
fn interleave(
    label: &str,
    pairs: usize,
    arms: [&Program; 2],
    names: &[&str],
    mut once: impl FnMut(&Program, &str) -> Result<Vec<f64>, CompatError>,
) -> Result<Vec<Series>, CompatError> {
    let mut series: Vec<Series> = names
        .iter()
        .map(|n| Series {
            name: format!("{label}-{n}"),
            rows: Vec::new(),
        })
        .collect();
    let mut cells: Vec<(Vec<f64>, Vec<f64>)> = Vec::new();
    for pair in 0..pairs {
        let order = if pair % 2 == 0 { [0, 1] } else { [1, 0] };
        let mut got: [Vec<f64>; 2] = [Vec::new(), Vec::new()];
        for arm in order {
            let Some(program) = arms.get(arm) else {
                continue;
            };
            let tag = format!("{label}-p{pair}-{}", program.label);
            eprintln!("bench: {tag}");
            let values = once(program, &tag)?;
            eprintln!("bench:   {values:?}");
            if let Some(slot) = got.get_mut(arm) {
                *slot = values;
            }
        }
        let [a, b] = got;
        cells.push((a, b));
    }
    for (i, one) in series.iter_mut().enumerate() {
        for (a, b) in &cells {
            if let (Some(a), Some(b)) = (a.get(i), b.get(i)) {
                one.rows.push((*a, *b));
            }
        }
    }
    Ok(series)
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values.get(values.len() / 2).copied().unwrap_or(f64::NAN)
}

/// Writes each metric's CSV, prints its medians and `perfstats.py compare`'s verdict.
fn report(rig: &Rig, arms: [&Program; 2], series: &[Series]) -> Result<(), CompatError> {
    for one in series {
        let csv = rig.out.join(format!("{}.csv", one.name));
        let mut text = String::from("pair,a,b\n");
        for (i, (a, b)) in one.rows.iter().enumerate() {
            text.push_str(&format!("{i},{a},{b}\n"));
        }
        fs::write(&csv, text).map_err(CompatError::io(&csv))?;
        let a = median(one.rows.iter().map(|r| r.0).collect());
        let b = median(one.rows.iter().map(|r| r.1).collect());
        println!(
            "\n== {} (n={} pairs): a={} median {a:.4}, b={} median {b:.4}, ratio b/a {:.3}",
            one.name,
            one.rows.len(),
            arms[0].label,
            arms[1].label,
            b / a
        );
        match std::env::var_os("LSR_PERFSTATS") {
            Some(script) => {
                let out = Command::new("uv")
                    .arg("run")
                    .arg(&script)
                    .args(["compare"])
                    .arg(&csv)
                    .args(["--sesoi", plan::SESOI])
                    .output()
                    .map_err(CompatError::io("uv"))?;
                println!("{}", String::from_utf8_lossy(&out.stdout));
                eprintln!("{}", String::from_utf8_lossy(&out.stderr));
            }
            None => println!("(LSR_PERFSTATS is unset: no perfstats verdict)"),
        }
    }
    Ok(())
}

fn sha256_of(path: &Path) -> Result<String, CompatError> {
    let bytes = fs::read(path).map_err(CompatError::io(path))?;
    Ok(Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

/// Copies `from` beside the results, so a rebuild cannot swap an arm, and returns the program.
fn preserve(out: &Path, label: &str, from: &Path) -> Result<Program, CompatError> {
    let dir = out.join("bin");
    fs::create_dir_all(&dir).map_err(CompatError::io(&dir))?;
    let path = dir.join(label);
    fs::copy(from, &path).map_err(CompatError::io(from))?;
    println!(
        "binary {label}: {} sha256 {}",
        from.display(),
        sha256_of(&path)?
    );
    Ok(Program {
        label: label.to_owned(),
        path,
    })
}

fn lsr_release() -> PathBuf {
    if let Some(path) = std::env::var_os("LSR_BENCH_LSR") {
        return PathBuf::from(path);
    }
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || workspace_root().join("target"),
        |dir| workspace_root().join(PathBuf::from(dir)),
    );
    target.join("release").join("lsr")
}

fn machine() {
    for (name, cmd, args) in [
        ("cpu", "sysctl", vec!["-n", "machdep.cpu.brand_string"]),
        ("os", "uname", vec!["-a"]),
        ("power", "pmset", vec!["-g", "batt"]),
        ("go", "go", vec!["version"]),
    ] {
        let text = Command::new(cmd)
            .args(args)
            .output()
            .map(|o| {
                String::from_utf8_lossy(&o.stdout)
                    .trim()
                    .replace('\n', " | ")
            })
            .unwrap_or_default();
        println!("machine {name}: {text}");
    }
    println!("tier: exploratory, candidate and unconfirmed (a laptop, no isolation)");
}

/// The set that measures a copy's effect on an application; not part of `all` (it takes hours).
const IMPACT: &str = "snapshot-impact";

/// Everything one invocation runs, by name.
const SETS: [&str; 7] = [
    "aa-restore",
    "aa-latency",
    "aa-overhead",
    "latency-steady",
    "latency-bursty",
    "overhead",
    "restore",
];

struct Args {
    out: PathBuf,
    pairs: Option<usize>,
    sets: Vec<String>,
    impact: impact::Options,
}

fn parse(args: &[OsString]) -> Result<Args, CompatError> {
    let bad = |m: String| CompatError::Fixture(m);
    let mut out = None;
    let mut pairs = None;
    let mut sets = Vec::new();
    let mut impact = impact::Options::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let text = arg.to_string_lossy().into_owned();
        match text.as_str() {
            "--out" => out = it.next().map(PathBuf::from),
            "--pairs" => {
                pairs = it.next().and_then(|v| v.to_string_lossy().parse().ok());
                if pairs.is_none() {
                    return Err(bad("--pairs needs a number".to_owned()));
                }
            }
            "--force" => impact.force = true,
            "--patched" => impact.patched = it.next().map(PathBuf::from),
            "--arms" => {
                impact.only = it
                    .next()
                    .map(|v| v.to_string_lossy().split(',').map(str::to_owned).collect())
                    .unwrap_or_default();
            }
            "--db-mib" | "--hot-mib" | "--pre-secs" | "--post-secs" | "--rounds"
            | "--gate-pairs" => {
                let value: Option<u64> = it.next().and_then(|v| v.to_string_lossy().parse().ok());
                let Some(value) = value else {
                    return Err(bad(format!("{text} needs a number")));
                };
                match text.as_str() {
                    "--db-mib" => impact.db_mib = Some(value),
                    "--hot-mib" => impact.hot_mib = Some(value),
                    "--pre-secs" => impact.pre_secs = Some(value),
                    "--post-secs" => impact.post_secs = Some(value),
                    "--rounds" => impact.rounds = usize::try_from(value).ok(),
                    _ => impact.gate_pairs = usize::try_from(value).ok(),
                }
            }
            "all" => sets.extend(SETS.iter().map(|s| (*s).to_owned())),
            name if SETS.contains(&name) || name == IMPACT => sets.push(name.to_owned()),
            other => {
                return Err(bad(format!(
                    "unknown argument {other:?}; sets: all {SETS:?}"
                )));
            }
        }
    }
    let out = out.ok_or_else(|| bad("--out DIR is required".to_owned()))?;
    if sets.is_empty() {
        return Err(bad(format!("name a set: all {SETS:?}")));
    }
    Ok(Args {
        out,
        pairs,
        sets,
        impact,
    })
}

/// The entry of the `bench-vs-go` binary.
///
/// # Errors
/// [`CompatError`] when a run fails or its replica does not restore to the source's pages.
pub fn run(args: &[OsString]) -> Result<(), CompatError> {
    if args.first().is_some_and(|a| a == EXEC) {
        return exec_wrapper(args.get(1..).unwrap_or_default());
    }
    if args.first().is_some_and(|a| a == hot::HOT) {
        return hot::run_child(args.get(1..).unwrap_or_default());
    }
    let mut args = parse(args)?;
    let Some(env) = need(Resource::Minio, || {
        let env = MinioEnv::from_env().map_err(|e| e.to_string())?;
        reachable(env.endpoint())?;
        Ok(env)
    }) else {
        return Ok(());
    };
    fs::create_dir_all(&args.out).map_err(CompatError::io(&args.out))?;
    let rig = Rig {
        env,
        exe: std::env::current_exe().map_err(CompatError::io("current_exe"))?,
        out: args.out.clone(),
    };
    if args.sets.iter().any(|s| s == IMPACT) {
        println!("minio: {}, {}-byte pages", rig.env.endpoint(), PAGE_SIZE);
        impact::run(&rig, &args.impact)?;
    }
    args.sets.retain(|s| s != IMPACT);
    if args.sets.is_empty() {
        return Ok(());
    }
    machine();
    println!("minio: {}, {}-byte pages", rig.env.endpoint(), PAGE_SIZE);
    println!("{}", plan::NAPKIN);
    let go = preserve(&rig.out, "go-3e0fdc0", &go_binary(GoVersion::Head)?)?;
    let lsr = preserve(&rig.out, "lsr", &lsr_release())?;
    let lsr_copy = preserve(&rig.out, "lsr-copy", &lsr_release())?;
    let mut generated = None;
    for set in &args.sets {
        run_set(
            &rig,
            set,
            args.pairs,
            [&go, &lsr],
            &lsr_copy,
            &mut generated,
        )?;
    }
    if let Some(generated) = &generated {
        generated.delete()?;
    }
    Ok(())
}

fn run_set(
    rig: &Rig,
    set: &str,
    pairs: Option<usize>,
    [go, lsr]: [&Program; 2],
    lsr_copy: &Program,
    generated: &mut Option<restore::Generated>,
) -> Result<(), CompatError> {
    println!("\n#### {set}");
    let arms = if set.starts_with("aa-") {
        [lsr, lsr_copy]
    } else {
        [go, lsr]
    };
    match set {
        "latency-steady" | "latency-bursty" | "aa-latency" => {
            let shape = if set == "latency-bursty" {
                Shape::Bursty
            } else {
                Shape::Steady
            };
            let n = pairs.unwrap_or(plan::LATENCY_PAIRS);
            let label = set.to_owned();
            let series = interleave(
                &label,
                n,
                arms,
                &["p50_s", "p99_s", "resolution_s"],
                |p, tag| {
                    let l = latency::run(rig, p, shape, tag)?;
                    Ok(vec![secs(l.p50), secs(l.p99), secs(l.resolution)])
                },
            )?;
            report(rig, arms, &series)
        }
        "overhead" | "aa-overhead" => {
            let wal = overhead::wal_bytes(rig)?;
            println!(
                "overhead workload writes {} bytes of WAL ({:.3} GiB)",
                wal,
                to_f64(wal) / to_f64(GIB)
            );
            let n = pairs.unwrap_or(plan::OVERHEAD_PAIRS);
            let series = interleave(
                set,
                n,
                arms,
                &["cpu_s_per_gib", "peak_rss_mib"],
                |p, tag| {
                    let o = overhead::run(rig, p, tag, wal)?;
                    Ok(vec![o.cpu_per_gib, o.peak_rss_mib])
                },
            )?;
            report(rig, arms, &series)
        }
        "restore" | "aa-restore" => {
            if generated.is_none() {
                let g = restore::generate(rig, go)?;
                println!("restore replica (written by {}): {}", go.label, g.shape);
                *generated = Some(g);
            }
            let Some(g) = generated.as_ref() else {
                return Ok(());
            };
            let n = pairs.unwrap_or(plan::RESTORE_PAIRS);
            let series = interleave(set, n, arms, &["wall_s", "peak_rss_mib"], |p, tag| {
                let r = restore::run(rig, p, g, tag)?;
                Ok(vec![r.wall, r.peak_rss_mib])
            })?;
            report(rig, arms, &series)
        }
        other => Err(CompatError::Fixture(format!("no set {other:?}"))),
    }
}
