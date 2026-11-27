//! The application the snapshot-impact set protects: a database larger than memory, a hot
//! working set read at a fixed rate, and a little write traffic.
//!
//! Mechanism. The database ([`build`]) is the benchmark's [`App`] table filled to a size. The
//! hot set is [`hot_pages`] rows, one per leaf page (every [`stride`]-th row id, at least four
//! apart, so no two share a page), so its size in cache is its row count times a page. Reads
//! are an open loop: read k of a thread is due at start plus k periods, and its latency is the
//! time from when it was due to when it finished, so a stall shows up as queueing in every read
//! behind it instead of vanishing (coordinated omission). 90% of reads go to the hottest tenth
//! of the hot set, 10% to all of it, so the hottest pages are re-referenced every few seconds
//! and the rest every minute or so, the shape of a real application's hot set. The application
//! opens SQLite with a 1 MiB page cache of its own, so a read that misses it is a [`pread`] on
//! the kernel's page cache, where the replicator's copy competes with it.
//!
//! The harness starts this as a child ([`hot-app`], [`run_child`]) so its page faults are its own.
//! It reads its schedule, warms the hot set, writes the ready file, runs until the stop file
//! appears, then writes every read's (due time, latency) and every commit's, and a summary.

use std::fs;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags};

use super::app::App;
use super::clock::{now, pause, pause_until, slot};
use super::sys;
use crate::error::CompatError;
use crate::workload::SplitMix;

/// The argument that makes the benchmark binary run as the application.
pub(super) const HOT: &str = "hot-app";
/// Rows on one leaf page of the benchmark table (1000-byte payloads on 4 KiB pages).
const ROWS_PER_PAGE: u64 = 4;
/// Rows a page of the hot set stands for: the gap between two hot row ids.
const MIN_STRIDE: u64 = ROWS_PER_PAGE;
const STOP_CHECK_EVERY: u64 = 256;
/// The bytes of one sample on disk: due time (unix nanoseconds) and latency (nanoseconds).
pub(super) const SAMPLE_BYTES: usize = 16;

/// The rows of a database of [`mib`] MiB: four to a page.
pub(super) const fn rows_for(mib: u64) -> u64 {
    mib.saturating_mul(1 << 20)
        .saturating_div(4096)
        .saturating_mul(ROWS_PER_PAGE)
}

/// Builds the database at [`path`] with [`rows`] rows, truncating the WAL as it goes so the file
/// is the only large thing on disk. Returns the page count.
pub(super) fn build(path: &Path, rows: u64) -> Result<u32, CompatError> {
    let rows = u32::try_from(rows)
        .map_err(|_big| CompatError::Fixture("the database has more than 2^32 rows".to_owned()))?;
    let app = App::create_large(path, super::plan::SEED, rows)?;
    app.page_count()
}

/// The hot set: the id of its first row, the gap between rows, and how many rows it holds.
#[derive(Debug, Clone, Copy)]
struct HotSet {
    stride: u64,
    pages: u64,
}

impl HotSet {
    fn new(total_rows: u64, wanted_pages: u64) -> Self {
        let stride = total_rows
            .checked_div(wanted_pages)
            .unwrap_or(MIN_STRIDE)
            .max(MIN_STRIDE);
        let pages = total_rows
            .checked_div(stride)
            .unwrap_or(0)
            .min(wanted_pages);
        Self { stride, pages }
    }

    fn id(self, k: u64) -> i64 {
        i64::try_from(k.saturating_mul(self.stride).saturating_add(1)).unwrap_or(i64::MAX)
    }

    /// A hot row number: nine in ten from the hottest tenth, the rest from the whole set.
    fn pick(self, rng: &mut SplitMix) -> u64 {
        if rng.below(10) < 9 {
            rng.below(self.pages.div_ceil(10).max(1))
        } else {
            rng.below(self.pages.max(1))
        }
    }
}

/// What the harness tells the child.
#[derive(Debug, Clone)]
pub(super) struct Job {
    pub db: PathBuf,
    pub total_rows: u64,
    pub hot_pages: u64,
    pub reads_per_s: u32,
    pub readers: u32,
    pub writes_per_s: u32,
    /// Prefix of the three output files: [`.reads`], [`.writes`], [`.summary`].
    pub result: PathBuf,
    /// Created once the hot set is warm.
    pub ready: PathBuf,
    /// The run ends when this file exists.
    pub stop: PathBuf,
}

impl Job {
    /// The command-line arguments of the child, after [`hot-app`].
    pub(super) fn args(&self) -> Vec<String> {
        vec![
            self.db.display().to_string(),
            self.total_rows.to_string(),
            self.hot_pages.to_string(),
            self.reads_per_s.to_string(),
            self.readers.to_string(),
            self.writes_per_s.to_string(),
            self.result.display().to_string(),
            self.ready.display().to_string(),
            self.stop.display().to_string(),
        ]
    }

    fn parse(args: &[std::ffi::OsString]) -> Option<Self> {
        let text = |i: usize| args.get(i).map(|a| a.to_string_lossy().into_owned());
        let num = |i: usize| text(i)?.parse::<u64>().ok();
        Some(Self {
            db: PathBuf::from(text(0)?),
            total_rows: num(1)?,
            hot_pages: num(2)?,
            reads_per_s: u32::try_from(num(3)?).ok()?,
            readers: u32::try_from(num(4)?).ok()?,
            writes_per_s: u32::try_from(num(5)?).ok()?,
            result: PathBuf::from(text(6)?),
            ready: PathBuf::from(text(7)?),
            stop: PathBuf::from(text(8)?),
        })
    }
}

/// One timed operation: when it was due and how long it took past that, in nanoseconds.
pub(super) type Sample = (u64, u64);

/// The wall clock as unix nanoseconds, the one timeline the harness and the child share.
#[expect(
    clippy::disallowed_methods,
    reason = "the harness and the child agree on windows in wall-clock time, across processes"
)]
pub(super) fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

fn open(db: &Path, readonly: bool) -> Result<Connection, CompatError> {
    let flags = if readonly {
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX
    };
    let conn = Connection::open_with_flags(db, flags)?;
    conn.busy_timeout(Duration::from_secs(30))?;
    // A page cache of 1 MiB: reads past it go to the kernel's page cache, where the copy competes.
    conn.pragma_update(None, "cache_size", -1024)?;
    if !readonly {
        // The replicator owns checkpoints, as in every benchmark of this module.
        conn.pragma_update(None, "wal_autocheckpoint", 0)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
    }
    Ok(conn)
}

/// The marker's value now and the page it sits on.
pub(super) fn marker(db: &Path) -> Result<(u64, u32), CompatError> {
    let conn = open(db, true)?;
    let page: u32 = conn.query_row(
        "SELECT rootpage FROM sqlite_master WHERE name = 'marker'",
        [],
        |r| r.get(0),
    )?;
    let text: String = conn.query_row("SELECT v FROM marker WHERE id = 1", [], |r| r.get(0))?;
    let value = text
        .parse()
        .map_err(|_bad| CompatError::Fixture(format!("marker value {text:?} is not a number")))?;
    Ok((value, page))
}

fn touch(conn: &Connection, hot: HotSet, from: u64, to: u64) -> Result<(), CompatError> {
    let mut stmt = conn.prepare("SELECT length(data) FROM items WHERE id = ?1")?;
    for k in from..to {
        let _len: i64 = stmt.query_row([hot.id(k)], |r| r.get(0))?;
    }
    Ok(())
}

/// Reads the whole hot set twice, split over [`threads`] connections, so every hot page has been
/// referenced twice, as an application's hot pages have.
fn warm(job: &Job, hot: HotSet) -> Result<(), CompatError> {
    let threads = u64::from(job.readers.max(1));
    let share = hot.pages.div_ceil(threads);
    for _pass in 0..2 {
        let failed = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..threads)
                .map(|t| {
                    scope.spawn(move || {
                        let conn = open(&job.db, true)?;
                        let from = t.saturating_mul(share);
                        touch(&conn, hot, from, from.saturating_add(share).min(hot.pages))
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|w| w.join().unwrap_or_else(|_p| Err(panic_error())))
                .find_map(Result::err)
        });
        if let Some(err) = failed {
            return Err(err);
        }
    }
    Ok(())
}

fn panic_error() -> CompatError {
    CompatError::Fixture("a hot-app thread panicked".to_owned())
}

/// One reader thread: an open loop of point reads until the stop file appears.
fn reader(job: &Job, hot: HotSet, index: u32) -> Result<Vec<Sample>, CompatError> {
    let conn = open(&job.db, true)?;
    let mut stmt = conn.prepare("SELECT length(data) FROM items WHERE id = ?1")?;
    let mut rng = SplitMix::new(super::plan::SEED ^ (u64::from(index).wrapping_add(1) << 32));
    let readers = u64::from(job.readers.max(1));
    let period = Duration::from_nanos(
        1_000_000_000_u64
            .saturating_mul(readers)
            .checked_div(u64::from(job.reads_per_s.max(1)))
            .unwrap_or(1_000_000),
    );
    let base_instant = now();
    let base_unix = unix_ns();
    let mut samples = Vec::new();
    let mut k = 0_u64;
    loop {
        if k.is_multiple_of(STOP_CHECK_EVERY) && job.stop.exists() {
            return Ok(samples);
        }
        let due = slot(base_instant, period, u32::try_from(k).unwrap_or(u32::MAX));
        pause_until(due);
        let _len: i64 = stmt.query_row([hot.id(hot.pick(&mut rng))], |r| r.get(0))?;
        let late = now().saturating_duration_since(due);
        let due_ns = base_unix.saturating_add(
            u64::try_from(due.saturating_duration_since(base_instant).as_nanos())
                .unwrap_or(u64::MAX),
        );
        samples.push((due_ns, u64::try_from(late.as_nanos()).unwrap_or(u64::MAX)));
        k = k.saturating_add(1);
    }
}

/// The writer thread: [`writes_per_s`] commits a second, each rewriting one hot row and bumping
/// the marker; every commit's duration is a sample.
fn writer(job: &Job, hot: HotSet) -> Result<Vec<Sample>, CompatError> {
    let (mut commits, _page) = marker(&job.db)?;
    let mut conn = open(&job.db, false)?;
    let mut rng = SplitMix::new(super::plan::SEED ^ 0x77);
    let period = Duration::from_nanos(
        1_000_000_000_u64
            .checked_div(u64::from(job.writes_per_s.max(1)))
            .unwrap_or(100_000_000),
    );
    let base_instant = now();
    let mut samples = Vec::new();
    let mut k = 0_u64;
    loop {
        if job.stop.exists() {
            return Ok(samples);
        }
        let due = slot(base_instant, period, u32::try_from(k).unwrap_or(u32::MAX));
        pause_until(due);
        let began = now();
        let started_unix = unix_ns();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE items SET data = ?2 WHERE id = ?1",
            (hot.id(hot.pick(&mut rng)), vec![b'x'; 1000]),
        )?;
        commits = commits.saturating_add(1);
        tx.execute(
            "UPDATE marker SET v = ?1 WHERE id = 1",
            [format!("{commits:08}")],
        )?;
        tx.commit()?;
        let took = now().saturating_duration_since(began);
        samples.push((
            started_unix,
            u64::try_from(took.as_nanos()).unwrap_or(u64::MAX),
        ));
        k = k.saturating_add(1);
    }
}

fn write_samples(path: &Path, samples: &[Sample]) -> Result<(), CompatError> {
    let file = fs::File::create(path).map_err(CompatError::io(path))?;
    let mut out = BufWriter::new(file);
    for (at, took) in samples {
        out.write_all(&at.to_le_bytes())
            .and_then(|()| out.write_all(&took.to_le_bytes()))
            .map_err(CompatError::io(path))?;
    }
    out.flush().map_err(CompatError::io(path))
}

/// Reads a samples file back.
pub(super) fn read_samples(path: &Path) -> Result<Vec<Sample>, CompatError> {
    let bytes = fs::read(path).map_err(CompatError::io(path))?;
    let word = |chunk: &[u8]| -> Option<u64> { Some(u64::from_le_bytes(chunk.try_into().ok()?)) };
    Ok(bytes
        .as_chunks::<SAMPLE_BYTES>()
        .0
        .iter()
        .filter_map(|c| Some((word(c.get(..8)?)?, word(c.get(8..)?)?)))
        .collect())
}

/// The child's entry: [`hot-app JOB-ARGS...`].
pub(super) fn run_child(args: &[std::ffi::OsString]) -> Result<(), CompatError> {
    let job = Job::parse(args)
        .ok_or_else(|| CompatError::Fixture("hot-app: bad arguments".to_owned()))?;
    let hot = HotSet::new(job.total_rows, job.hot_pages);
    warm(&job, hot)?;
    let faults_start = sys::self_major_faults();
    fs::write(&job.ready, b"ready").map_err(CompatError::io(&job.ready))?;
    let (reads, writes) = std::thread::scope(|scope| {
        let readers: Vec<_> = (0..job.readers.max(1))
            .map(|i| {
                let job = &job;
                scope.spawn(move || reader(job, hot, i))
            })
            .collect();
        let writer = scope.spawn(|| writer(&job, hot));
        let mut reads = Vec::new();
        let mut failed = None;
        for r in readers {
            match r.join().unwrap_or_else(|_p| Err(panic_error())) {
                Ok(mut s) => reads.append(&mut s),
                Err(e) => failed = Some(e),
            }
        }
        let writes = writer.join().unwrap_or_else(|_p| Err(panic_error()));
        match (failed, writes) {
            (Some(e), _) | (None, Err(e)) => Err(e),
            (None, Ok(w)) => Ok((reads, w)),
        }
    })?;
    let faults_end = sys::self_major_faults();
    let with = |ext: &str| PathBuf::from(format!("{}.{ext}", job.result.display()));
    write_samples(&with("reads"), &reads)?;
    write_samples(&with("writes"), &writes)?;
    let faults = faults_start
        .zip(faults_end)
        .map_or(0, |(a, b)| b.saturating_sub(a));
    let summary = format!(
        "hot_pages {}\nstride {}\nreads {}\nwrites {}\nmajor_faults {faults}\n",
        hot.pages,
        hot.stride,
        reads.len(),
        writes.len()
    );
    let path = with("summary");
    fs::write(&path, summary).map_err(CompatError::io(&path))?;
    // A moment for the harness to see the files before this process's exit.
    pause(Duration::from_millis(10));
    Ok(())
}

/// The summary's major faults, the app's own.
pub(super) fn summary_major_faults(result: &Path) -> Result<u64, CompatError> {
    let path = PathBuf::from(format!("{}.summary", result.display()));
    let text = fs::read_to_string(&path).map_err(CompatError::io(&path))?;
    text.lines()
        .find_map(|l| l.strip_prefix("major_faults "))
        .and_then(|v| v.trim().parse().ok())
        .ok_or_else(|| CompatError::Fixture(format!("{}: no major_faults line", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_rows_sit_on_different_pages_and_never_past_the_table() {
        let hot = HotSet::new(1_000_000, 100_000);
        assert_eq!(hot.stride, 10);
        assert_eq!(hot.pages, 100_000);
        assert_eq!(hot.id(0), 1);
        assert!(hot.id(hot.pages - 1) <= 1_000_000);
        // A hot set as big as the table cannot have one row a page: it shrinks.
        let crowded = HotSet::new(1_000, 1_000);
        assert_eq!(crowded.stride, MIN_STRIDE);
        assert_eq!(crowded.pages, 250);
    }

    #[test]
    fn nine_reads_in_ten_go_to_the_hottest_tenth() {
        let hot = HotSet::new(4_000_000, 1_000_000);
        let mut rng = SplitMix::new(7);
        let tenth = hot.pages.div_ceil(10);
        let inside = (0..100_000).filter(|_| hot.pick(&mut rng) < tenth).count();
        // 90% by design plus the tenth of the uniform draws that land there too.
        assert!((90_000..=100_000).contains(&inside), "{inside}");
    }

    #[test]
    fn samples_round_trip_through_a_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("s");
        let samples = vec![(1, 2), (u64::MAX, 7)];
        write_samples(&path, &samples).expect("write");
        assert_eq!(read_samples(&path).expect("read"), samples);
    }

    #[test]
    fn a_database_of_n_mib_has_four_rows_a_page() {
        assert_eq!(rows_for(1), 1024);
    }
}
