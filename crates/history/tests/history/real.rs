//! What the standalone runs share: a seeded random source, and a real restore of a replica into
//! a file, judged by the history judge through SQLite alone.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use lsr_core::conformance::block_on;
use lsr_core::{Inventory, ObjectStore, PlanOutcome, RestoreError as CoreRestoreError};
use lsr_history::{Anomaly, Digest, Judge, Log, Target, chain};
use lsr_replica::{
    AsOfInside, DynStore, RestoreError, RestoreOptions, RestoreTarget, Timestamp, Txid,
    restore_with,
};

/// splitmix64: a run is named by its seed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next().checked_rem(n.max(1)).unwrap_or(0)
    }

    pub fn payload(&mut self) -> Vec<u8> {
        let len = self.below(6000).saturating_add(1);
        (0..len).map(|_| self.next().to_le_bytes()[0]).collect()
    }

    /// `n` instants from `start` to just past `now`.
    pub fn instants(&mut self, n: usize, start: i64, now: Timestamp) -> Vec<i64> {
        let span = u64::try_from(now.unix_millis().saturating_sub(start)).unwrap_or(0);
        (0..n)
            .map(|_| {
                let offset = i64::try_from(self.below(span.saturating_add(2))).unwrap_or(0);
                start.saturating_add(offset)
            })
            .collect()
    }
}

/// Where restores land: a fresh path each time.
pub struct Scratch {
    dir: PathBuf,
    next: AtomicU32,
}

impl Scratch {
    pub fn new(dir: &Path) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        Self {
            dir: dir.to_owned(),
            next: AtomicU32::new(0),
        }
    }

    fn path(&self) -> PathBuf {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        self.dir.join(format!("restored-{n}.db"))
    }
}

/// A real restore of `store` at `target`, read back with SQLite alone: `None` when the planner
/// refuses (no file reaches the target), else the content or why the file is broken.
pub fn restored(
    store: &dyn DynStore,
    target: RestoreTarget,
    scratch: &Scratch,
) -> Option<Result<Digest, String>> {
    let to = scratch.path();
    let options = RestoreOptions {
        as_of_inside: AsOfInside::TakeEarlier,
    };
    match restore_with(store, target, options, &to) {
        Ok(_) => {}
        Err(RestoreError::Restore(CoreRestoreError::NoPlan(no))) => {
            assert!(
                matches!(
                    no.outcome(),
                    PlanOutcome::EmptyReplica | PlanOutcome::NoSnapshot { .. }
                ) || matches!(target, RestoreTarget::Txid(_)),
                "restoring {target:?} was refused: {:?}",
                no.outcome()
            );
            return None;
        }
        Err(other) => panic!("restoring {target:?}: {other}"),
    }
    let read = chain::read(&to);
    std::fs::remove_file(&to).unwrap();
    Some(read)
}

/// What one judgment of a replica found.
#[derive(Debug, Default)]
pub struct Seen {
    pub latest: bool,
    pub as_of: u32,
    pub points: u32,
}

/// Judges the replica now: `Latest`, `AsOf` at `instants`, every restore point when `sweep`,
/// and with `lag`, that `Latest` is the local head within that many ms of `now`.
#[expect(
    clippy::too_many_arguments,
    reason = "a test helper; each is one input of the judgment"
)]
pub fn judge<S: ObjectStore<Upload: 'static>>(
    store: &S,
    scratch: &Scratch,
    log: &Log,
    judge: &mut Judge,
    now: Timestamp,
    instants: &[i64],
    sweep: bool,
    lag: Option<i64>,
) -> Result<Seen, Anomaly> {
    let mut seen = Seen::default();
    let broken = |target, why| Anomaly::Broken { target, why };
    if sweep {
        let newest = block_on(Inventory::load(store))
            .map_err(|err| broken(Target::Latest, format!("listing the replica: {err}")))?
            .newest();
        let mut points = Vec::new();
        for txid in (1..=newest.map_or(0, Txid::get)).filter_map(Txid::new) {
            let target = Target::Point(txid.get());
            if let Some(read) = restored(store, RestoreTarget::Txid(txid), scratch) {
                points.push((txid.get(), read.map_err(|why| broken(target, why))?, false));
            }
        }
        seen.points = u32::try_from(points.len()).unwrap_or(u32::MAX);
        judge.sweep(log, &points)?;
    }
    if let Some(read) = restored(store, RestoreTarget::Latest, scratch) {
        let digest = read.map_err(|why| broken(Target::Latest, why))?;
        judge.latest(log, digest)?;
        if let Some(bound) = lag {
            judge.lag(log, digest, now.unix_millis(), bound)?;
        }
        seen.latest = true;
    }
    for instant in instants {
        let target = RestoreTarget::AsOf(Timestamp::from_unix_millis(*instant));
        if let Some(read) = restored(store, target, scratch) {
            let digest = read.map_err(|why| broken(Target::AsOf(*instant), why))?;
            judge.as_of(log, *instant, digest, None)?;
            seen.as_of = seen.as_of.saturating_add(1);
        }
    }
    Ok(seen)
}
