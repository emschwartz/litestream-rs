//! Host overhead: CPU time and peak resident memory of the replicator while it replicates a
//! known volume of WAL.

use std::fs;
use std::path::PathBuf;

use lsr_core::RestoreTarget;

use super::app::App;
use super::clock::{now, pause_until, slot};
use super::observe::Observer;
use super::{GIB, Program, Rig, mib, plan, secs, to_f64};
use crate::error::CompatError;
use crate::pages::hash_pages;
use crate::replicator::wait_until;

/// What one run measured.
#[derive(Debug, Clone, Copy)]
pub(super) struct Overhead {
    /// User plus system CPU seconds per GiB of WAL.
    pub cpu_per_gib: f64,
    /// Peak resident memory in MiB, for the run's whole volume.
    pub peak_rss_mib: f64,
}

fn drive(app: &mut App) -> Result<(), CompatError> {
    let start = now();
    for k in 0..plan::OVERHEAD_COMMITS {
        pause_until(slot(start, plan::OVERHEAD_PERIOD, k));
        app.update_rows(plan::OVERHEAD_UPDATES)?;
    }
    Ok(())
}

/// The bytes of WAL the workload writes: the workload runs once with no replicator and
/// checkpoints off, and the WAL file's length is read. Frames per commit do not depend on
/// who checkpoints, so every run writes the same volume.
pub(super) fn wal_bytes(rig: &Rig) -> Result<u64, CompatError> {
    let dir = rig.run_dir("calibrate")?;
    let db = dir.join("app.db");
    let mut app = App::create(&db, plan::SEED, plan::OVERHEAD_PRELOAD_ROWS)?;
    for _ in 0..plan::OVERHEAD_COMMITS {
        app.update_rows(plan::OVERHEAD_UPDATES)?;
    }
    let wal = PathBuf::from(format!("{}-wal", db.display()));
    let len = fs::metadata(&wal).map_err(CompatError::io(&wal))?.len();
    drop(app);
    Ok(len)
}

/// One run of `program` replicating the workload, with the replica restored and compared to
/// the source before the numbers count.
pub(super) fn run(
    rig: &Rig,
    program: &Program,
    tag: &str,
    wal: u64,
) -> Result<Overhead, CompatError> {
    let dir = rig.run_dir(tag)?;
    let db = dir.join("app.db");
    let mut app = App::create(&db, plan::SEED, plan::OVERHEAD_PRELOAD_ROWS)?;
    let s3 = rig.replica(tag)?;
    let mut process = rig.boot(program, &dir, tag, &db, &s3)?;
    drive(&mut app).map_err(|e| rig.explain(&mut process, e))?;
    let observer = Observer::new(&s3, app.marker_page());
    wait_until(
        "the last commit to reach the bucket",
        plan::DRAIN_LIMIT,
        || {
            process.ensure_running()?;
            Ok(observer.tail_marker()? == Some(app.commits()))
        },
    )
    .map_err(|e| rig.explain(&mut process, e))?;
    let usage = process.stop()?;
    app.checkpoint()?;
    let source = hash_pages(&db)?;
    let out = dir.join("restored.db");
    s3.restore(RestoreTarget::Latest, &out)?
        .map_err(|e| CompatError::Fixture(format!("restoring the replica: {e}")))?;
    if hash_pages(&out)? != source {
        return Err(CompatError::Fixture(format!(
            "{tag}: the restored pages differ from the source"
        )));
    }
    s3.delete_all()?;
    drop(app);
    drop(fs::remove_file(&out));
    Ok(Overhead {
        cpu_per_gib: secs(usage.cpu) * to_f64(GIB) / to_f64(wal),
        peak_rss_mib: mib(usage.peak_rss),
    })
}
