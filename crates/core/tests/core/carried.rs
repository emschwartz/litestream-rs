//! The heap the engine carries between ticks while one long job runs in slices.
//!
//! `mem.rs` measures the paths that run in one call. The engine's level-9 snapshot of the
//! database and its merges run across many ticks (`UploadConfig::bytes_per_tick`), so what the
//! `Actor` holds from one tick to the next is state of its own: a sliced job that kept every page
//! it had read would hold the database in RAM and pass every one-call gate.
//!
//! The rig drives the public `Actor` with the test doubles of `lsr_core`'s `testing` feature over
//! a database whose pages are computed on read, with a staging directory and a replica that are
//! files on disk, so no page is held anywhere but by the engine. After each tick the live heap is
//! read; the carried heap is that minus what was live before the job began.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::num::{NonZeroU32, NonZeroU64};
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::heap::{live_now, peak_of};
use crate::mem::{FLAT_SLACK, GeneratedDb, KIB, MIB, SNAPSHOT_BYTES_PER_PAGE, page_size};
use lsr_core::conformance::block_on;
use lsr_core::{
    Actor, ActorConfig, CheckpointConfig, CreatedAt, Env, Event, FakeCheckpointer, FakeClock,
    FakeWal, FileInfo, FileKey, Level, ListedTime, Listing, MaintainedEvent, Maintenance,
    MaxUnreplicatedAge, ObjectStore, PageLimit, PageSource, ReadLen, Responses, SourceError,
    StoreError, StoreOp, Tick, Upload, UploadConfig, WriteMode, build_listing, file_name,
};
use lsr_ltx::{DbPages, PageNo, Timestamp, TxidRange};
use lsr_wal::Salts;

/// A store that keeps its objects as files in a temporary directory and holds only their keys
/// and sizes: the stand-in for both the staging directory and the replica, which a database's
/// worth of bytes must not sit in.
#[derive(Clone)]
struct Disk {
    dir: Arc<tempfile::TempDir>,
    files: Arc<Mutex<BTreeMap<FileKey, FileInfo>>>,
    /// While set, every read fails as a dropped connection would.
    cut_reads: Arc<AtomicBool>,
    read_bytes: Arc<AtomicU64>,
}

impl Disk {
    fn new() -> Self {
        Self {
            dir: Arc::new(tempfile::tempdir().unwrap()),
            files: Arc::default(),
            cut_reads: Arc::default(),
            read_bytes: Arc::default(),
        }
    }

    fn path(&self, key: &FileKey) -> PathBuf {
        self.dir
            .path()
            .join(format!("{}-{}", key.level().get(), file_name(key.range())))
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<FileKey, FileInfo>> {
        // Nothing panics while holding the lock, so poison carries no half-written state.
        self.files.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn files(&self) -> Vec<FileInfo> {
        self.lock().values().copied().collect()
    }

    fn info(&self, key: &FileKey) -> Result<FileInfo, StoreError> {
        let files = self.lock();
        files
            .get(key)
            .copied()
            .ok_or(StoreError::NotFound { key: *key })
    }
}

fn io_fault(op: StoreOp, err: &io::Error) -> StoreError {
    StoreError::Transient {
        op,
        kind: err.kind(),
    }
}

struct DiskUpload {
    disk: Disk,
    key: FileKey,
    created_at: Timestamp,
    mode: WriteMode,
    file: File,
    written: u64,
}

impl Upload for DiskUpload {
    async fn write(&mut self, chunk: &[u8]) -> Result<(), StoreError> {
        self.file
            .write_all(chunk)
            .map_err(|err| io_fault(StoreOp::Upload, &err))?;
        self.written += u64::try_from(chunk.len()).unwrap();
        Ok(())
    }

    async fn commit(self) -> Result<FileInfo, StoreError> {
        let mut files = self.disk.lock();
        if self.mode == WriteMode::New && files.contains_key(&self.key) {
            return Err(StoreError::AlreadyExists { key: self.key });
        }
        let (tmp, dest) = (
            self.disk.path(&self.key).with_extension("tmp"),
            self.disk.path(&self.key),
        );
        fs::rename(tmp, dest).map_err(|err| io_fault(StoreOp::Upload, &err))?;
        let info = FileInfo {
            key: self.key,
            size: self.written,
            listed: ListedTime::Created(CreatedAt::mtime(self.created_at)),
        };
        files.insert(self.key, info);
        Ok(info)
    }

    async fn abort(self) -> Result<(), StoreError> {
        drop(fs::remove_file(
            self.disk.path(&self.key).with_extension("tmp"),
        ));
        Ok(())
    }
}

impl ObjectStore for Disk {
    type Upload = DiskUpload;

    async fn remove_scratch(&self) -> Result<u64, StoreError> {
        Ok(0)
    }

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        let level_files = self
            .files()
            .into_iter()
            .filter(|info| info.key.level() == level)
            .collect();
        Ok(build_listing(level_files, &[], after, limit))
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        if self.cut_reads.load(Ordering::Relaxed) {
            return Err(StoreError::Transient {
                op: StoreOp::Read,
                kind: io::ErrorKind::ConnectionReset,
            });
        }
        let info = self.info(key)?;
        let take = usize::try_from(info.size.saturating_sub(offset).min(len.get())).unwrap();
        let mut buf = vec![0_u8; take];
        File::open(self.path(key))
            .and_then(|file| file.read_exact_at(&mut buf, offset))
            .map_err(|err| io_fault(StoreOp::Read, &err))?;
        self.read_bytes
            .fetch_add(u64::try_from(take).unwrap(), Ordering::Relaxed);
        Ok(buf)
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        match self.info(&file.key)?.listed {
            ListedTime::Created(at) => Ok(at),
            ListedTime::LastModified(_) => Err(StoreError::NotFound { key: file.key }),
        }
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        Ok((
            self.read(&file.key, 0, len).await?,
            self.created_at(file).await?,
        ))
    }

    async fn begin_sized_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: WriteMode,
        _len: Option<u64>,
    ) -> Result<DiskUpload, StoreError> {
        let file = File::create(self.path(key).with_extension("tmp"))
            .map_err(|err| io_fault(StoreOp::Upload, &err))?;
        Ok(DiskUpload {
            disk: self.clone(),
            key: *key,
            created_at,
            mode,
            file,
            written: 0,
        })
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        let mut files = self.lock();
        for key in keys {
            files.remove(key);
            drop(fs::remove_file(self.path(key)));
        }
        Ok(())
    }
}

const T0: i64 = 1_700_000_000_000;

fn at(millis: i64) -> Timestamp {
    Timestamp::from_unix_millis(millis)
}

struct Rig {
    wal: FakeWal,
    db: Counted,
    reads: Arc<AtomicU64>,
    ck: FakeCheckpointer,
    staging: Disk,
    remote: Disk,
    clock: FakeClock,
    actor: Actor,
}

impl Rig {
    /// A database of `pages` pages whose first page is in the WAL, an actor that sends `slice`
    /// bytes a tick, and nothing yet in staging or on the replica.
    fn new(pages: u32, slice: u64) -> Self {
        let size = page_size();
        let wal = FakeWal::new(
            size,
            Salts {
                salt1: 10,
                salt2: 20,
            },
        );
        wal.append_tx(
            &[(PageNo::new(1, size).unwrap(), vec![7; size.bytes()])],
            std::num::NonZeroU32::new(pages).unwrap(),
        );
        let config = ActorConfig {
            sync: lsr_core::SyncConfig::default(),
            sync_interval: Duration::ZERO,
            checkpoint: CheckpointConfig {
                interval: None,
                min_pages: None,
                truncate_pages: None,
            },
            upload: UploadConfig {
                files_per_tick: None,
                bytes_per_tick: NonZeroU64::new(slice).unwrap(),
            },
            maintenance: Maintenance::go_default(),
            retry_interval: Duration::from_secs(1),
            shutdown_grace: Duration::from_secs(10),
            responses: Responses::GO_LIKE,
            max_unreplicated_age: MaxUnreplicatedAge::DEFAULT,
        };
        let reads = Arc::new(AtomicU64::new(0));
        Self {
            ck: FakeCheckpointer::new(wal.clone()),
            wal,
            db: Counted {
                inner: GeneratedDb { pages },
                reads: Arc::clone(&reads),
            },
            reads,
            staging: Disk::new(),
            remote: Disk::new(),
            clock: FakeClock::new(at(T0)),
            actor: Actor::new(config),
        }
    }

    fn tick(&mut self, now: i64) -> Tick {
        let env = Env {
            wal: &mut self.wal,
            db: &mut self.db,
            ck: &mut self.ck,
            staging: &self.staging,
            remote: &self.remote,
            clock: &self.clock,
            notice: &mut |_| {},
        };
        block_on(self.actor.tick(env, at(now)))
    }
}

/// A page source that counts the pages read from it.
struct Counted {
    inner: GeneratedDb,
    reads: Arc<AtomicU64>,
}

impl PageSource for Counted {
    fn page_count(&mut self) -> Result<DbPages, SourceError> {
        self.inner.page_count()
    }

    fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_page(pgno, buf)
    }
}

/// What the engine carried through one job, read after each of its ticks.
#[derive(Clone, Copy, Debug)]
struct JobRun {
    /// The level the job wrote: 9 for a snapshot, else the compaction's output level.
    level: u8,
    /// The most live heap after any tick of the job, above what was live before it began.
    carried: usize,
    /// The most heap any tick of the job held at once, above the same mark.
    peak: usize,
    /// Pages the job read from the database file.
    db_reads: u64,
}

/// The level of the job a tick ended, if it ended one.
fn ended(tick: &Tick) -> Option<u8> {
    let Tick::Ran {
        event: Event::Maintained { event, .. },
        ..
    } = tick
    else {
        return None;
    };
    match event {
        MaintainedEvent::Compacted(info) => Some(info.key.level().get()),
        MaintainedEvent::Snapshot(_) => Some(9),
        MaintainedEvent::Deleted(_)
        | MaintainedEvent::Replan
        | MaintainedEvent::Repaired(_)
        | MaintainedEvent::ReadBack
        | MaintainedEvent::Waiting { .. } => None,
    }
}

/// What a run of the rig does to the engine before its jobs are measured.
#[derive(Clone, Copy)]
struct Shape {
    /// Transactions the application commits while the first file is on its way to the replica
    /// and the pages each rewrites, so level 0 holds more than the one snapshot file.
    commits: u32,
    commit_pages: u32,
    /// Whether the snapshot built from level 0 fails once, as a replica whose files cannot be
    /// read does, so the next snapshot is a read of the database file.
    refuse_seed: bool,
}

impl Rig {
    fn cut(&self, on: bool) {
        self.staging.cut_reads.store(on, Ordering::Relaxed);
        self.remote.cut_reads.store(on, Ordering::Relaxed);
    }

    /// Ticks until the first level-0 file is on the replica, then until the first snapshot is,
    /// and returns every job that ended on the way, each measured from the heap that was live
    /// when the job before it ended.
    fn jobs(&mut self, shape: Shape) -> Vec<JobRun> {
        let size = page_size();
        let mut now = T0;
        let mut made = 0;
        for i in 0_u32.. {
            now += 1;
            if i > 4 && i % 3 == 0 && made < shape.commits {
                made += 1;
                let pages: Vec<_> = (0..shape.commit_pages)
                    .map(|n| (PageNo::new(2 + n, size).unwrap(), vec![9; size.bytes()]))
                    .collect();
                let commit = NonZeroU32::new(self.db.inner.pages).unwrap();
                self.wal.append_tx(&pages, commit);
            }
            if matches!(
                self.tick(now),
                Tick::Ran {
                    event: Event::Uploaded(_),
                    ..
                }
            ) {
                break;
            }
            assert!(i < 100_000, "the first file never reached the replica");
        }
        self.cut(shape.refuse_seed);
        let mut refused = !shape.refuse_seed;
        let mut jobs = Vec::new();
        let mut mark = live_now();
        let (mut carried, mut peak) = (0, 0);
        let mut db_from = self.reads.load(Ordering::Relaxed);
        for _ in 0..200_000 {
            now += 1;
            let before = live_now();
            let (tick, tick_peak) = peak_of(|| self.tick(now));
            let live = live_now();
            carried = carried.max(live - mark);
            peak = peak.max(before - mark + isize::try_from(tick_peak.extra).unwrap());
            if !refused
                && matches!(
                    &tick,
                    Tick::Ran {
                        event: Event::Retrying { .. } | Event::Degraded { .. },
                        ..
                    }
                )
            {
                self.cut(false);
                refused = true;
                // Past the snapshot's backoff.
                now += 5_000;
            }
            if let Some(level) = ended(&tick) {
                let db_reads = self.reads.load(Ordering::Relaxed);
                jobs.push(JobRun {
                    level,
                    carried: usize::try_from(carried).unwrap_or(0),
                    peak: usize::try_from(peak).unwrap_or(0),
                    db_reads: db_reads - db_from,
                });
                (mark, carried, peak, db_from) = (live, 0, 0, db_reads);
                if level == 9 {
                    return jobs;
                }
            }
        }
        panic!("the first snapshot never ended");
    }
}

/// Pages the smaller run's database has; the larger has ten times as many.
const SMALL: usize = 4096;

/// Bytes bound by no page count that a level-9 snapshot of the database carries between
/// ticks: its plan, its encoder's buffers and bookkeeping (measured: about 500 KiB).
pub const SNAPSHOT_CARRIED_FIXED: usize = MIB;

/// The same for a merge of replica or staged files: the window it reads each input in
/// (the 16 MiB budget over the inputs, which is 512 KiB for each at the most) and its output
/// buffers (measured: about 1.35 MiB).
pub const MERGE_CARRIED_FIXED: usize = 2 * MIB;

/// The most one tick holds beyond what the job carries into it: a slice of the file, a page
/// read, the listing it asks for (measured: about 30 KiB).
pub const TICK_EXTRA: usize = 128 * KIB;

/// Checks one job at two database sizes: carried and peak heap grow by at most
/// [SNAPSHOT_BYTES_PER_PAGE] bytes for each page more, a fixed part is under
/// `fixed`, and no tick holds more than [TICK_EXTRA] beyond what the job carries.
fn assert_job_law(what: &str, small: JobRun, big: JobRun, fixed: usize) {
    let (extra, more) = (SMALL * 9, SMALL * 9);
    for (name, a, b) in [
        ("carried", small.carried, big.carried),
        ("peak", small.peak, big.peak),
    ] {
        let slope = b.saturating_sub(a) / more;
        let held = a.saturating_sub(slope * SMALL);
        eprintln!(
            "{what} {name}: {SMALL} pages {} KiB, {} pages {} KiB, {slope} B/page, fixed {} KiB",
            a >> 10,
            SMALL * 10,
            b >> 10,
            held >> 10
        );
        assert!(
            b <= a + SNAPSHOT_BYTES_PER_PAGE * extra + FLAT_SLACK,
            "{what}: the engine's {name} heap grew {} bytes for {extra} more pages, over {} a page ({a} bytes at {SMALL} pages, {b} at {})",
            b.saturating_sub(a),
            SNAPSHOT_BYTES_PER_PAGE,
            SMALL * 10
        );
        assert!(
            held <= fixed,
            "{what}: {held} bytes {name} whatever the size, ceiling {fixed}"
        );
    }
    for run in [small, big] {
        assert!(
            run.peak <= run.carried + TICK_EXTRA,
            "{what}: a tick held {} bytes beyond the {} the job carried",
            run.peak - run.carried,
            run.carried
        );
    }
}

/// The jobs of a run over a database of `pages` pages.
fn run(pages: usize, shape: Shape) -> Vec<JobRun> {
    Rig::new(u32::try_from(pages).unwrap(), 64 * 1024).jobs(shape)
}

const NO_COMMITS: Shape = Shape {
    commits: 0,
    commit_pages: 0,
    refuse_seed: false,
};

/// The level-9 snapshot read from the database file, sliced over thousands of ticks, carries
/// the checksum table it pins, the plan and the encoder's index: linear in pages at the
/// pinned coefficient of a one-call snapshot, with a fixed part of its own. A copy of each
/// page it read, which is 4096 bytes a page, breaks the first law.
#[test]
fn a_sliced_job_carries_heap_linear_in_pages_at_the_pinned_coefficient() {
    let shape = Shape {
        refuse_seed: true,
        ..NO_COMMITS
    };
    let (small, big) = (run(SMALL, shape), run(SMALL * 10, shape));
    let last = |jobs: &[JobRun]| *jobs.last().unwrap();
    for (jobs, pages) in [(&small, SMALL), (&big, SMALL * 10)] {
        let snapshot = last(jobs);
        assert_eq!(snapshot.level, 9);
        assert!(
            snapshot.db_reads >= u64::try_from(pages).unwrap() - 1,
            "the snapshot read {} pages of {pages}: it was built from level 0, not from the database",
            snapshot.db_reads
        );
    }
    assert_job_law(
        "database snapshot",
        last(&small),
        last(&big),
        SNAPSHOT_CARRIED_FIXED,
    );
    // The merges the same run makes first (levels 1 to 3 of the one big file) carry the index
    // of the file they write and the window of each input they read, and nothing of its pages.
    let merges = |jobs: &[JobRun]| {
        jobs.iter()
            .filter(|job| job.level < 9)
            .copied()
            .collect::<Vec<_>>()
    };
    let (small, big) = (merges(&small), merges(&big));
    assert!(!small.is_empty() && small.len() == big.len());
    for (a, b) in small.iter().zip(&big) {
        assert_eq!((a.level, a.db_reads, b.db_reads), (b.level, 0, 0));
        assert_job_law(
            &format!("level {} merge", a.level),
            *a,
            *b,
            MERGE_CARRIED_FIXED,
        );
    }
}

/// The first snapshot built from level 0 (no read of the database) over several files,
/// not one, carries what it does over one.
#[test]
fn a_merge_of_many_level_zero_files_carries_what_a_merge_of_one_does() {
    let many = Shape {
        commits: 12,
        commit_pages: 64,
        refuse_seed: false,
    };
    let (one, several) = (run(SMALL, NO_COMMITS), run(SMALL, many));
    let (one, several) = (*one.last().unwrap(), *several.last().unwrap());
    eprintln!(
        "level-0 merge: one file {} KiB, several {} KiB",
        one.carried >> 10,
        several.carried >> 10
    );
    assert_eq!((one.level, several.level, several.db_reads), (9, 9, 0));
    assert!(
        several.carried <= one.carried + FLAT_SLACK,
        "a merge of several files carried {} bytes, of one {}",
        several.carried,
        one.carried
    );
}
