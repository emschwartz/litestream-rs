//! Commit to durable: the time from a commit returning to the level-0 file that holds it
//! being listed in the bucket, under a steady writer and a bursty one.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use super::app::App;
use super::clock::{now, pause_until, slot};
use super::observe::{Observer, Seen};
use super::{Program, Rig, plan};
use crate::error::CompatError;
use crate::s3::S3Replica;

/// The shape of the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    /// One commit every [`plan::STEADY_PERIOD`].
    Steady,
    /// [`plan::BURSTS`] bursts of [`plan::BURST_COMMITS`] back-to-back commits, one every
    /// [`plan::BURST_GAP`].
    Bursty,
}

impl Shape {
    const fn commits(self) -> u64 {
        match self {
            Self::Steady => plan::STEADY_COMMITS,
            Self::Bursty => plan::BURSTS.saturating_mul(plan::BURST_COMMITS),
        }
    }
}

/// What one run measured.
#[derive(Debug, Clone, Copy)]
pub(super) struct Latency {
    /// Median commit-to-durable time.
    pub p50: Duration,
    /// 99th percentile (nearest rank).
    pub p99: Duration,
    /// The observer's median listing round trip.
    pub resolution: Duration,
}

fn drive(app: &mut App, shape: Shape, times: &Mutex<Vec<Instant>>) -> Result<(), CompatError> {
    let start = now();
    let commit = |app: &mut App| -> Result<(), CompatError> {
        app.insert_rows(1)?;
        times
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(now());
        Ok(())
    };
    match shape {
        Shape::Steady => {
            for k in 0..plan::STEADY_COMMITS {
                pause_until(slot(
                    start,
                    plan::STEADY_PERIOD,
                    u32::try_from(k).unwrap_or(u32::MAX),
                ));
                commit(app)?;
            }
        }
        Shape::Bursty => {
            for burst in 0..plan::BURSTS {
                pause_until(slot(
                    start,
                    plan::BURST_GAP,
                    u32::try_from(burst).unwrap_or(u32::MAX),
                ));
                for _ in 0..plan::BURST_COMMITS {
                    commit(app)?;
                }
            }
        }
    }
    Ok(())
}

/// Nearest-rank percentile of a sorted list.
fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percent).div_ceil(100).max(1);
    sorted
        .get(rank.saturating_sub(1))
        .copied()
        .unwrap_or_default()
}

/// Each measured commit's time to durable: from the commit's return to the first listing
/// that showed a file holding it.
fn latencies(first: u64, commits: &[Instant], seen: &[Seen]) -> Result<Vec<Duration>, CompatError> {
    let mut out = Vec::with_capacity(commits.len());
    for (k, committed) in (first.saturating_add(1)..).zip(commits) {
        let file = seen
            .iter()
            .find(|s| s.marker >= k)
            .ok_or_else(|| CompatError::Fixture(format!("commit {k} never reached the bucket")))?;
        let took = file.at.checked_duration_since(*committed).ok_or_else(|| {
            CompatError::Fixture(format!("commit {k} was durable before it returned"))
        })?;
        out.push(took);
    }
    Ok(out)
}

fn writer_and_observer(
    app: &mut App,
    s3: &S3Replica,
    shape: Shape,
    first: u64,
) -> Result<(Vec<Duration>, Duration), CompatError> {
    let times = Mutex::new(Vec::new());
    let cancel = AtomicBool::new(false);
    let mut observer = Observer::new(s3, app.marker_page());
    let (seen, written) = std::thread::scope(|scope| {
        let watcher = scope.spawn(|| {
            let mut seen = Vec::new();
            let deadline = now()
                .checked_add(plan::LATENCY_RUN_LIMIT)
                .unwrap_or_else(now);
            observer
                .until(
                    "the commits to reach the bucket",
                    first.saturating_add(shape.commits()),
                    deadline,
                    &cancel,
                    &mut seen,
                )
                .map(|()| seen)
        });
        let written = drive(app, shape, &times);
        if written.is_err() {
            cancel.store(true, Ordering::Relaxed);
        }
        (watcher.join(), written)
    });
    written?;
    let seen =
        seen.map_err(|_panic| CompatError::Fixture("the observer panicked".to_owned()))??;
    let commits = times
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut took = latencies(first, &commits, &seen)?;
    took.sort();
    Ok((took, observer.resolution()))
}

/// One run: boots `program` on a fresh database and prefix, waits for its first commit to
/// be durable, runs the writer, stops the program and reads the numbers.
pub(super) fn run(
    rig: &Rig,
    program: &Program,
    shape: Shape,
    tag: &str,
) -> Result<Latency, CompatError> {
    let dir = rig.run_dir(tag)?;
    let db = dir.join("app.db");
    let mut app = App::create(&db, plan::SEED, 0)?;
    let s3 = rig.replica(tag)?;
    let mut process = rig.boot(program, &dir, tag, &db, &s3)?;
    let mut sink = Vec::new();
    let mut observer = Observer::new(&s3, app.marker_page());
    for _ in 0..plan::WARMUP_COMMITS {
        app.insert_rows(1)?;
    }
    let deadline = now().checked_add(plan::BOOT_LIMIT).unwrap_or_else(now);
    observer
        .until(
            "the warm-up commits",
            app.commits(),
            deadline,
            &AtomicBool::new(false),
            &mut sink,
        )
        .map_err(|e| rig.explain(&mut process, e))?;
    let first = app.commits();
    let (took, resolution) = writer_and_observer(&mut app, &s3, shape, first)
        .map_err(|e| rig.explain(&mut process, e))?;
    process.stop()?;
    s3.delete_all()?;
    Ok(Latency {
        p50: percentile(&took, 50),
        p99: percentile(&took, 99),
        resolution,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let ms: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        assert_eq!(percentile(&ms, 50), Duration::from_millis(50));
        assert_eq!(percentile(&ms, 99), Duration::from_millis(99));
        assert_eq!(percentile(&ms[..3], 99), Duration::from_millis(3));
        assert_eq!(percentile(&[], 50), Duration::ZERO);
    }

    #[test]
    fn a_commit_is_durable_at_the_first_file_that_holds_its_marker() {
        let t0 = now();
        let at = |ms| t0.checked_add(Duration::from_millis(ms)).expect("instant");
        let commits = [at(10), at(20), at(30)];
        let seen = [
            Seen {
                at: at(500),
                marker: 6,
            },
            Seen {
                at: at(900),
                marker: 8,
            },
        ];
        // The first measured commit is marker 6 (five before it).
        let took = latencies(5, &commits, &seen).expect("latencies");
        assert_eq!(
            took,
            [
                Duration::from_millis(490),
                Duration::from_millis(880),
                Duration::from_millis(870)
            ]
        );
    }

    #[test]
    fn a_commit_no_file_holds_is_an_error() {
        let t0 = now();
        let seen = [Seen { at: t0, marker: 1 }];
        latencies(0, &[t0, t0], &seen).unwrap_err();
    }
}
