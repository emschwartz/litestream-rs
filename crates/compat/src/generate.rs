//! Generating one fixture: a seeded workload against a real `litestream replicate`.
//!
//! Catching up is observed, never slept for: after each step the newest level-0 file of the
//! replica must carry the live WAL's salts and end exactly at the WAL's committed end
//! (`wal_checkpoint(NOOP)` frames times the frame size). Polling a condition with a deadline is
//! the only waiting.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use lsr_core::{FileKey, Inventory, Level, PlanOutcome, RestoreTarget, plan_reference};

use crate::binaries::{GoVersion, go_binary};
use crate::error::CompatError;
use crate::golive::{GoRestore, restore_with_go, write_config};
use crate::manifest::{FIXTURE_FORMAT, FileEntry, Manifest, Outcome, Target, TargetEntry};
use crate::pages::{hash_pages, integrity_check};
use crate::replica::{self, Listing, ReplicaFile, info_of};
use crate::replicator::{Replicator, wait_until};
use crate::scenario::Scenario;
use crate::workload::Workload;

const CATCH_UP: Duration = Duration::from_secs(30);
const SHAPE_CHANGE: Duration = Duration::from_secs(20);
const MAX_TAIL: u32 = 400;
const ATTEMPTS: u32 = 4;
const GAP_TOUCHES: u32 = 6;

/// The two salts of a WAL file header.
fn wal_salts(wal: &Path) -> Option<(u32, u32)> {
    let mut head = [0u8; 32];
    fs::File::open(wal).ok()?.read_exact(&mut head).ok()?;
    let s1 = u32::from_be_bytes(head.get(16..20)?.try_into().ok()?);
    let s2 = u32::from_be_bytes(head.get(20..24)?.try_into().ok()?);
    Some((s1, s2))
}

/// Whether the replica's newest level-0 file ends exactly where the live WAL's committed
/// frames end, in the same WAL generation.
fn caught_up(wl: &Workload, db: &Path, replica: &Path) -> Result<bool, CompatError> {
    let Some(header) = replica::newest_l0(replica)? else {
        return Ok(false);
    };
    let Some(salts) = wal_salts(&db.with_extension("db-wal")) else {
        return Ok(false);
    };
    let frame_len = u64::from(wl.page_size()).saturating_add(24);
    let end = wl
        .wal_frames()?
        .saturating_mul(frame_len)
        .saturating_add(32);
    let Some(wal) = header.fields().wal else {
        return Ok(false);
    };
    Ok(wal.salts() == salts && wal.offset().saturating_add(wal.size()) == end)
}

/// Everything one run needs while it is in flight.
#[derive(Debug)]
struct Run {
    wl: Workload,
    rep: Replicator,
    db: PathBuf,
    replica: PathBuf,
}

impl Run {
    fn settle_replica(&mut self) -> Result<(), CompatError> {
        let Self {
            wl,
            rep,
            db,
            replica,
        } = self;
        wait_until("the replica to catch up with the WAL", CATCH_UP, || {
            rep.ensure_running()?;
            caught_up(wl, db, replica)
        })
        .map_err(|e| with_log(e, rep))
    }

    fn higher_keys(&self) -> Result<Vec<FileKey>, CompatError> {
        self_keys(&self.replica)
    }

    /// Writes and waits until the replica shows more than level-0 files: snapshots at two
    /// distinct TXIDs, levels 1 and 2, and enough distinct file ends to pick targets from.
    fn tail(&mut self) -> Result<(), CompatError> {
        for _ in 0..MAX_TAIL {
            if shape_ok(&self.higher_keys()?) {
                return Ok(());
            }
            let before = self.higher_keys()?;
            self.wl.touch()?;
            self.settle_replica()?;
            let Self { rep, .. } = self;
            wait_until("a compaction or snapshot", SHAPE_CHANGE, || {
                rep.ensure_running()?;
                Ok(self_keys(&self.replica)? != before)
            })?;
        }
        Err(CompatError::Timeout(
            "the replica to hold two snapshots and levels 1 and 2".to_owned(),
        ))
    }
}

/// The keys above level 0.
fn self_keys(replica: &Path) -> Result<Vec<FileKey>, CompatError> {
    Ok(replica::list(replica)?
        .files
        .into_iter()
        .map(|f| f.key)
        .filter(|k| k.level() > Level::ZERO)
        .collect())
}

fn last_txid(key: &FileKey) -> u64 {
    key.range().max().get()
}

/// The planner's verdict for a replica given as files, as a hole when it names one:
/// `(have_through, next_starts)`.
fn gap_of(files: &[ReplicaFile], target: RestoreTarget) -> Result<Option<(u64, u64)>, CompatError> {
    let inventory = Inventory::from_files(files.iter().map(info_of))
        .map_err(|e| CompatError::Fixture(format!("inventory: {e}")))?;
    Ok(match plan_reference(&inventory, target) {
        PlanOutcome::Gap {
            have_through,
            next_starts,
        } => Some((have_through.get(), next_starts.get())),
        PlanOutcome::Plan(_)
        | PlanOutcome::EmptyReplica
        | PlanOutcome::NoSnapshot { .. }
        | PlanOutcome::BeyondNewest { .. }
        | PlanOutcome::AsOfInsideFile { .. }
        | PlanOutcome::InsideFile { .. }
        | PlanOutcome::MalformedSnapshot { .. } => None,
    })
}

/// A TXID whose covering files can be deleted to leave a hole strictly inside the chain of the
/// newest snapshot: some progress before the hole, at least two TXIDs of history after it.
fn find_hole(files: &[ReplicaFile]) -> Result<Option<u64>, CompatError> {
    let newest = files
        .iter()
        .filter(|f| f.key.level().is_snapshot())
        .map(|f| last_txid(&f.key))
        .max();
    let head = files.iter().map(|f| last_txid(&f.key)).max();
    let (Some(newest), Some(head)) = (newest, head) else {
        return Ok(None);
    };
    for hole in newest.saturating_add(2)..head {
        let remaining = without(files, hole);
        if let Some((have, next)) = gap_of(&remaining, RestoreTarget::Latest)?
            && have > newest
            && head.saturating_sub(next) >= 2
        {
            return Ok(Some(hole));
        }
    }
    Ok(None)
}

/// The files with every non-snapshot file covering `txid` removed.
fn without(files: &[ReplicaFile], txid: u64) -> Vec<ReplicaFile> {
    files
        .iter()
        .filter(|f| {
            let range = f.key.range();
            f.key.level().is_snapshot() || !(range.min().get() <= txid && txid <= range.max().get())
        })
        .cloned()
        .collect()
}

fn with_log(err: CompatError, rep: &Replicator) -> CompatError {
    match err {
        CompatError::Timeout(what) => {
            CompatError::Timeout(format!("{what} (go log: {})", rep.log_tail()))
        }
        other @ (CompatError::Io { .. }
        | CompatError::Sqlite(_)
        | CompatError::Json { .. }
        | CompatError::BadKey(_)
        | CompatError::MissingBinary { .. }
        | CompatError::Process { .. }
        | CompatError::Fixture(_)
        | CompatError::Integrity { .. }
        | CompatError::NotPages { .. }) => other,
    }
}

/// Two snapshots at distinct TXIDs, files on levels 1 and 2.
fn shape_ok(higher: &[FileKey]) -> bool {
    let snapshots = higher.iter().filter(|k| k.level().is_snapshot()).count();
    let at = |level: u8| higher.iter().any(|k| k.level().get() == level);
    snapshots >= 2 && at(1) && at(2)
}

/// Generates the fixture of `scenario` for `version` under `fixtures` (replacing it) and
/// returns its manifest. A gap scenario retries a fresh run when timing left no mid-chain hole.
pub fn generate(
    version: GoVersion,
    scenario: Scenario,
    fixtures: &Path,
) -> Result<Manifest, CompatError> {
    let binary = go_binary(version)?;
    let dir = fixtures.join(version.name()).join(scenario.name());
    for _ in 0..ATTEMPTS {
        if let Some(manifest) = attempt(version, scenario, &binary, &dir)? {
            return Ok(manifest);
        }
    }
    Err(CompatError::Fixture(format!(
        "{} {}: no mid-chain hole in {ATTEMPTS} runs",
        version.name(),
        scenario.name()
    )))
}

/// One run; `None` when a gap scenario found no hole to cut.
fn attempt(
    version: GoVersion,
    scenario: Scenario,
    binary: &Path,
    dir: &Path,
) -> Result<Option<Manifest>, CompatError> {
    let work = tempfile::tempdir().map_err(CompatError::io(std::env::temp_dir()))?;
    let (db, replica, config) = (
        work.path().join("app.db"),
        work.path().join("replica"),
        work.path().join("litestream.yml"),
    );
    write_config(&config, &db, &replica)?;
    let wl = Workload::create(&db, scenario.page_size(), scenario.seed())?;
    let rep = Replicator::start(binary, &config, &work.path().join("go.log"))?;
    let mut run = Run {
        wl,
        rep,
        db,
        replica,
    };
    run.settle_replica()?;
    let mut peak = run.wl.page_count()?;
    for step in scenario.steps() {
        run.wl.apply(step)?;
        peak = peak.max(run.wl.page_count()?);
        run.settle_replica()?;
    }
    run.tail()?;
    if scenario.has_gap() {
        wait_for_new_snapshot(&mut run)?;
        for _ in 0..GAP_TOUCHES {
            run.wl.touch()?;
            run.settle_replica()?;
        }
    }
    run.wl.settle()?;
    run.settle_replica()?;
    let final_pages = run.wl.page_count()?;
    let Run { rep, replica, .. } = run;
    rep.stop()?;
    let Some(files) = publish_replica(&replica, dir, scenario)? else {
        return Ok(None);
    };
    let targets = record_targets(version, scenario, binary, dir, &files, work.path())?;
    let manifest = Manifest {
        format: FIXTURE_FORMAT,
        go_version: version.name().to_owned(),
        scenario: scenario.name().to_owned(),
        seed: scenario.seed(),
        page_size: scenario.page_size(),
        max_page_count: peak,
        final_page_count: final_pages,
        files: files
            .iter()
            .map(|f| FileEntry {
                key: f.rel.clone(),
                size: f.size,
                created_at_ms: f.mtime_ms,
            })
            .collect(),
        targets,
    };
    if scenario.must_shrink() && manifest.final_page_count >= manifest.max_page_count {
        return Err(CompatError::Fixture(
            "the vacuum scenario did not shrink the database".to_owned(),
        ));
    }
    manifest.save(dir)?;
    Ok(Some(manifest))
}

fn wait_for_new_snapshot(run: &mut Run) -> Result<(), CompatError> {
    let count = |replica: &Path| -> Result<usize, CompatError> {
        Ok(replica::list(replica)?
            .files
            .iter()
            .filter(|f| f.key.level().is_snapshot())
            .map(|f| last_txid(&f.key))
            .max()
            .map_or(0, |m| usize::try_from(m).unwrap_or(usize::MAX)))
    };
    let newest = count(&run.replica)?;
    let Run {
        rep, replica, wl, ..
    } = run;
    wait_until("a snapshot at a newer TXID", SHAPE_CHANGE, || {
        rep.ensure_running()?;
        if count(replica)? == newest {
            wl.touch()?;
        }
        Ok(count(replica)? != newest)
    })
}

/// Copies the replica into the fixture directory with millisecond mtimes (temp files skipped),
/// cutting the hole for a gap scenario. Returns the fixture's files, or `None` for no hole.
fn publish_replica(
    src: &Path,
    dir: &Path,
    scenario: Scenario,
) -> Result<Option<Vec<ReplicaFile>>, CompatError> {
    if dir.exists() {
        fs::remove_dir_all(dir).map_err(CompatError::io(dir))?;
    }
    let dst = Manifest::replica_dir(dir);
    let listing = replica::list(src)?;
    for file in &listing.files {
        let to = dst.join(&file.rel);
        let parent = to.parent().unwrap_or(&dst);
        fs::create_dir_all(parent).map_err(CompatError::io(parent))?;
        fs::copy(src.join(&file.rel), &to).map_err(CompatError::io(&to))?;
        let millis = u64::try_from(file.mtime_ms).unwrap_or(0);
        let when = std::time::UNIX_EPOCH
            .checked_add(Duration::from_millis(millis))
            .unwrap_or(std::time::UNIX_EPOCH);
        fs::File::options()
            .write(true)
            .open(&to)
            .and_then(|f| f.set_modified(when))
            .map_err(CompatError::io(&to))?;
    }
    if scenario.has_gap() {
        let Some(hole) = find_hole(&listing.files)? else {
            return Ok(None);
        };
        let kept = without(&listing.files, hole);
        for file in listing
            .files
            .iter()
            .filter(|f| !kept.iter().any(|k| k.key == f.key))
        {
            let path = dst.join(&file.rel);
            fs::remove_file(&path).map_err(CompatError::io(&path))?;
        }
    }
    let Listing { files, .. } = replica::list(&dst)?;
    Ok(Some(files))
}

/// Restores every chosen target with the same Go binary from the fixture copy and records it.
fn record_targets(
    version: GoVersion,
    scenario: Scenario,
    binary: &Path,
    dir: &Path,
    files: &[ReplicaFile],
    work: &Path,
) -> Result<Vec<TargetEntry>, CompatError> {
    let config = work.join("restore.yml");
    let db_path = work.join("restored-from.db");
    let replica = Manifest::replica_dir(dir);
    write_config(&config, &db_path, &replica)?;
    let ctx = RestoreCtx {
        binary,
        config: &config,
        db_path: &db_path,
        work,
        counter: std::cell::Cell::new(0),
    };
    let label = format!("{} {}", version.name(), scenario.name());
    if scenario.has_gap() {
        gap_targets(&ctx, files, &label)
    } else {
        restorable_targets(&ctx, files, &label)
    }
}

struct RestoreCtx<'a> {
    binary: &'a Path,
    config: &'a Path,
    db_path: &'a Path,
    work: &'a Path,
    counter: std::cell::Cell<u32>,
}

impl RestoreCtx<'_> {
    /// Restores into a fresh directory; the digest is `None` when Go refused.
    fn run(&self, target: Target) -> Result<(GoRestore, Option<crate::PageDigest>), CompatError> {
        let n = self.counter.get().saturating_add(1);
        self.counter.set(n);
        let out_dir = self.work.join(format!("restore-{n}"));
        fs::create_dir_all(&out_dir).map_err(CompatError::io(&out_dir))?;
        let out = out_dir.join("restored.db");
        let result = restore_with_go(self.binary, self.config, self.db_path, &out, target)?;
        match result {
            GoRestore::Restored => {
                let digest = hash_pages(&out)?;
                integrity_check(&out)?;
                Ok((result, Some(digest)))
            }
            GoRestore::Refused { .. } => Ok((result, None)),
        }
    }
}

/// Indices of `len` items in the order a spread of targets prefers: the quartiles first.
fn spread_order(len: usize) -> Vec<usize> {
    let q = |num: usize| len.saturating_mul(num).checked_div(4).unwrap_or(0);
    let mut order: Vec<usize> = Vec::new();
    for i in [q(1), q(2), q(3)].into_iter().chain(0..len) {
        if i < len && !order.contains(&i) {
            order.push(i);
        }
    }
    order
}

fn restorable_targets(
    ctx: &RestoreCtx<'_>,
    files: &[ReplicaFile],
    label: &str,
) -> Result<Vec<TargetEntry>, CompatError> {
    let oldest_snapshot = files
        .iter()
        .filter(|f| f.key.level().is_snapshot())
        .min_by_key(|f| last_txid(&f.key));
    let Some(oldest) = oldest_snapshot else {
        return Err(CompatError::Fixture(format!("{label}: no snapshot")));
    };
    let head = files.iter().map(|f| last_txid(&f.key)).max().unwrap_or(0);
    let mut txids: Vec<u64> = files
        .iter()
        .map(|f| last_txid(&f.key))
        .filter(|&m| m >= last_txid(&oldest.key) && m < head)
        .collect();
    txids.sort_unstable();
    txids.dedup();
    let mut times: Vec<i64> = files.iter().map(|f| f.mtime_ms).collect();
    times.sort_unstable();
    times.dedup();
    let mids: Vec<i64> = times
        .windows(2)
        .filter_map(|w| match w {
            [a, b] if b.saturating_sub(*a) >= 2 && *a >= oldest.mtime_ms => {
                Some(a.saturating_add(b.saturating_sub(*a).saturating_div(2)))
            }
            _ => None,
        })
        .collect();
    let mut out = Vec::new();
    collect(ctx, Target::Latest, &mut out, 1)?;
    let txid_targets = spread_order(txids.len())
        .into_iter()
        .filter_map(|i| txids.get(i))
        .map(|&txid| Target::Txid { txid });
    collect_first(ctx, txid_targets, &mut out, 3, label, "txid")?;
    let time_targets = spread_order(mids.len())
        .into_iter()
        .filter_map(|i| mids.get(i))
        .map(|&unix_ms| Target::AsOf { unix_ms });
    collect_first(ctx, time_targets, &mut out, 2, label, "as-of")?;
    Ok(out)
}

/// Restores the target; pushes it when Go succeeded. Fails when it must succeed and did not.
fn collect(
    ctx: &RestoreCtx<'_>,
    target: Target,
    out: &mut Vec<TargetEntry>,
    _need: usize,
) -> Result<bool, CompatError> {
    match ctx.run(target)? {
        (_, Some(digest)) => {
            out.push(TargetEntry {
                target,
                outcome: Outcome::restored(digest),
            });
            Ok(true)
        }
        (GoRestore::Refused { detail }, None) => Err(CompatError::Fixture(format!(
            "Go refused {target:?}: {detail}"
        ))),
        (GoRestore::Restored, None) => Ok(false),
    }
}

/// Tries candidates in order, keeping the first `need` that Go restores.
fn collect_first(
    ctx: &RestoreCtx<'_>,
    candidates: impl Iterator<Item = Target>,
    out: &mut Vec<TargetEntry>,
    need: usize,
    label: &str,
    kind: &str,
) -> Result<(), CompatError> {
    let mut got = 0usize;
    for target in candidates {
        if got >= need {
            break;
        }
        if let (GoRestore::Restored, Some(digest)) = ctx.run(target)? {
            out.push(TargetEntry {
                target,
                outcome: Outcome::restored(digest),
            });
            got = got.saturating_add(1);
        }
    }
    if got < need {
        return Err(CompatError::Fixture(format!(
            "{label}: only {got} of {need} {kind} targets restore"
        )));
    }
    Ok(())
}

fn gap_targets(
    ctx: &RestoreCtx<'_>,
    files: &[ReplicaFile],
    label: &str,
) -> Result<Vec<TargetEntry>, CompatError> {
    let Some((have_through, next_starts)) = gap_of(files, RestoreTarget::Latest)? else {
        return Err(CompatError::Fixture(format!(
            "{label}: the planner sees no gap"
        )));
    };
    let head = files.iter().map(|f| last_txid(&f.key)).max().unwrap_or(0);
    let mid = next_starts.saturating_add(head.saturating_sub(next_starts).saturating_div(2));
    let mut txids = vec![next_starts, mid, head];
    txids.dedup();
    let newest = files.iter().map(|f| f.mtime_ms).max().unwrap_or(0);
    let mut targets = vec![Target::Latest];
    targets.extend(txids.into_iter().map(|txid| Target::Txid { txid }));
    targets.push(Target::AsOf {
        unix_ms: newest.saturating_add(1000),
    });
    targets.push(Target::AsOf {
        unix_ms: newest.saturating_add(3_600_000),
    });
    let mut out = Vec::new();
    for target in targets {
        if let Some(wanted) = target.restore_target()
            && matches!(target, Target::Txid { .. })
            && gap_of(files, wanted)? != Some((have_through, next_starts))
        {
            return Err(CompatError::Fixture(format!(
                "{label}: {target:?} is not the same gap in the planner"
            )));
        }
        let (result, _) = ctx.run(target)?;
        let go_refused = matches!(result, GoRestore::Refused { .. });
        out.push(TargetEntry {
            target,
            outcome: Outcome::Gap {
                have_through,
                next_starts,
                go_refused,
            },
        });
    }
    Ok(out)
}
