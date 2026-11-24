//! Restore: wall time and peak resident memory to restore a 1 GiB database from one replica
//! with a deep history, by each program, from the same replica.

use std::fs;
use std::path::PathBuf;

use lsr_core::Level;

use super::app::App;
use super::clock::{now, pause_until, slot};
use super::{Program, Rig, mib, plan, secs};
use crate::golive::{Schedule, write_config_for};
use crate::pages::{PageDigest, hash_pages};
use crate::remote::Remote;
use crate::replicator::wait_until;
use crate::s3::S3Replica;

use super::observe::Observer;
use crate::error::CompatError;

/// The replica every restore reads, and what a correct restore of it looks like.
#[derive(Debug)]
pub(super) struct Generated {
    s3: S3Replica,
    config: PathBuf,
    db: PathBuf,
    dir: PathBuf,
    digest: PageDigest,
    /// Files and bytes per level, for the report.
    pub shape: String,
}

impl Generated {
    /// Removes the replica from the bucket.
    pub(super) fn delete(&self) -> Result<(), CompatError> {
        self.s3.delete_all()
    }
}

fn shape_of(s3: &S3Replica) -> Result<String, CompatError> {
    let files = s3.files_paged(1000)?;
    let mut parts = Vec::new();
    for level in Level::ALL {
        let of: Vec<_> = files.iter().filter(|f| f.key.level() == level).collect();
        if !of.is_empty() {
            let bytes: u64 = of.iter().map(|f| f.size).sum();
            parts.push(format!(
                "level {level}: {} files, {:.0} MiB",
                of.len(),
                mib(bytes)
            ));
        }
    }
    Ok(parts.join("; "))
}

/// Writes the replica once, with Go: a database grown to 1 GiB and then churned, replicated
/// with short compaction intervals and no scheduled snapshot, so the replica holds the boot
/// snapshot, many level-0 files and files at every compaction level.
pub(super) fn generate(rig: &Rig, go: &Program) -> Result<Generated, CompatError> {
    let tag = "restore-replica";
    let dir = rig.run_dir(tag)?;
    let db = dir.join("app.db");
    let mut app = App::create(&db, plan::SEED, 0)?;
    let s3 = rig.replica(tag)?;
    let config = dir.join("generate.yml");
    write_config_for(
        &config,
        &db,
        &Remote::S3(Box::new(rig.replica(tag)?)),
        Schedule::NO_SCHEDULED_SNAPSHOT,
    )?;
    let mut process = rig.start_replicate(go, &dir, tag, &config)?;
    rig.wait_snapshot(&mut process, &s3)?;
    let start = now();
    let mut slot_n = 0_u32;
    while app.page_count()? < plan::TARGET_PAGES {
        pause_until(slot(start, plan::GROW_PERIOD, slot_n));
        app.insert_rows(plan::GROW_BATCH_ROWS)?;
        slot_n = slot_n.saturating_add(1);
        process.ensure_running()?;
    }
    for _ in 0..plan::CHURN_BATCHES {
        pause_until(slot(start, plan::GROW_PERIOD, slot_n));
        app.update_rows(plan::CHURN_ROWS)?;
        slot_n = slot_n.saturating_add(1);
        process.ensure_running()?;
    }
    let observer = Observer::new(&s3, app.marker_page());
    wait_until(
        "the replica to hold the last commit",
        plan::DRAIN_LIMIT,
        || {
            process.ensure_running()?;
            Ok(observer.tail_marker()? == Some(app.commits()))
        },
    )?;
    process.stop()?;
    app.checkpoint()?;
    let digest = hash_pages(&db)?;
    let shape = format!(
        "{} pages ({:.2} GiB), {} commits; {}",
        digest.page_count,
        mib(u64::from(digest.page_count).saturating_mul(4096)) / 1024.0,
        app.commits(),
        shape_of(&s3)?
    );
    drop(app);
    let config = dir.join("restore.yml");
    fs::write(
        &config,
        format!("dbs:\n  - path: {}\n{}", db.display(), s3.yaml_defaults()),
    )
    .map_err(CompatError::io(&config))?;
    Ok(Generated {
        s3,
        config,
        db,
        dir,
        digest,
        shape,
    })
}

/// What one restore cost.
#[derive(Debug, Clone, Copy)]
pub(super) struct Restore {
    /// Wall seconds.
    pub wall: f64,
    /// Peak resident memory in MiB.
    pub peak_rss_mib: f64,
}

/// One restore of `generated` by `program`; the pages must equal the source's.
pub(super) fn run(
    rig: &Rig,
    program: &Program,
    generated: &Generated,
    tag: &str,
) -> Result<Restore, CompatError> {
    let out = generated.dir.join(format!("out-{tag}.db"));
    let usage = rig.restore(
        program,
        &generated.dir,
        tag,
        &generated.config,
        &out,
        &generated.db,
    )?;
    let pages = hash_pages(&out)?;
    for suffix in ["", "-wal", "-shm"] {
        drop(fs::remove_file(format!("{}{suffix}", out.display())));
    }
    if pages != generated.digest {
        return Err(CompatError::Fixture(format!(
            "{tag}: {} restored pages that differ from the source",
            program.label
        )));
    }
    Ok(Restore {
        wall: secs(usage.wall),
        peak_rss_mib: mib(usage.peak_rss),
    })
}
