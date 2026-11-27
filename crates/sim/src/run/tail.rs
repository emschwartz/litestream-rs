//! The cost of each tick, and the healthy tail every run ends with.
//!
//! Every tick is priced at [`Profile::DEFAULT`] from what the recorders counted
//! ([`crate::cost`]) and judged against the bound configuration fixes; with [`Opts::cost`]
//! set, the clock also moves on by the price, so a tick that does minutes of I/O takes minutes
//! of the engine's time.
//!
//! The tail is the liveness check. With every fault healed and the run's own judges passed,
//! the application commits [`TAIL_COMMITS`] times, [`TAIL_GAP`] apart, for long enough that
//! level-1 and level-2 compactions come due in it, and the clock is charged whatever the run
//! set. No file staged or on the replica before a commit holds it, so the replica holding a
//! transaction newer than the newest of those means the commit reached it: read from the bucket
//! and the staging directory themselves, never from the engine. A backlog the run left staged
//! goes up first, and its files say nothing of the commit. A staged file the engine could not
//! trust (its fsync failed) is no transaction: the engine writes it again under the same name
//! from the WAL as it stands, the commit among it, as Go keeps a transaction's number until its
//! sync succeeds. Once staging or the bucket holds other bytes at its name it stops counting
//! as before the commit ([`Before`]). Each must within [`Runner::lag_bound`] of simulated time.
//! And the tail may hand no more bytes to uploads the store never takes than one file in
//! flight: a job that restarts from scratch with nothing failing wastes a slice each time.
//!
//! The WAL stays small through the tail ([`wal_bound`]): with no reader holding a snapshot, Go
//! checkpoints PASSIVE after a sync that leaves the minimum of synced frames, so the next commit
//! restarts the WAL from its start. Once a commit has reached the replica, the WAL holds at most
//! the minimum, one commit's frames and the engine's own bump frame. An engine that stops
//! checkpointing after a sync lets the WAL grow until the interval or the truncate threshold.
//!
//! [`Opts::cost`]: crate::Opts::cost

use std::time::Duration;

use lsr_core::{Entropy, Event, FileKey};
use lsr_ltx::Txid;

use super::{Runner, Step};
use crate::cost::{Allowance, Link, Profile, Spare};
use crate::oracle::{Bound, Violation, within};
use crate::resources::{SLICES_AHEAD, TickStats};
use crate::store::{RecorderCounts, RecorderEvent};

/// The commits of the healthy tail.
pub const TAIL_COMMITS: u32 = 40;

/// Simulated time between two of the tail's commits: forty of them span over six minutes, so
/// a level-1 compaction (every 30 s) and a level-2 one (every 5 minutes) come due inside it.
pub const TAIL_GAP: Duration = Duration::from_secs(10);

/// Pages each of the tail's commits writes.
const TAIL_COMMIT_PAGES: u8 = 2;

/// The most frames the WAL may hold once one of the tail's commits reached the replica, with
/// no reader holding a snapshot (module doc): Go's PASSIVE minimum, the commit's own frames, and
/// the bump frame the engine writes after a checkpoint.
#[must_use]
pub fn wal_bound() -> u32 {
    super::PASSIVE_PAGES
        .saturating_add(u32::from(TAIL_COMMIT_PAGES))
        .saturating_add(1)
}

/// Ticks one of the tail's commits may take to reach the replica before the run gives up on
/// it, whatever the clock says: a backstop for an engine that ticks without time passing.
const TAIL_TICKS: u32 = 2_000;

/// Files put in the recorder's events from `from` on.
fn puts_since(events: &[RecorderEvent], from: usize) -> u64 {
    let puts = events
        .iter()
        .skip(from)
        .filter(|event| matches!(event, RecorderEvent::Put { .. }))
        .count();
    u64::try_from(puts).unwrap_or(u64::MAX)
}

/// What staging and the replica held before one of the tail's commits: no transaction in it is
/// the commit's (module doc).
#[derive(Debug)]
pub(super) struct Before {
    /// The replica's newest transaction then.
    frontier: Option<Txid>,
    /// Each file staged then, with the digest of its bytes.
    staged: Vec<(FileKey, u64)>,
}

impl Before {
    /// From the replica's newest transaction and the staged files with their digests.
    pub(super) const fn new(frontier: Option<Txid>, staged: Vec<(FileKey, u64)>) -> Self {
        Self { frontier, staged }
    }

    /// The newest transaction the commit is not in, given the digests staging and the bucket
    /// hold at a name now (`held`). A staged file whose name holds other bytes now was written
    /// again, the commit with it, and from then on says nothing of what came before.
    pub(super) fn newest(&mut self, held: impl Fn(&FileKey) -> Vec<u64>) -> Option<Txid> {
        self.staged
            .retain(|(key, digest)| held(key).iter().all(|now| now == digest));
        self.staged
            .iter()
            .map(|(key, _)| key.range().max())
            .max()
            .max(self.frontier)
    }
}

impl Runner {
    /// The prices the clock moves by: the run's, or none. A run with a latency tail is
    /// charged at the default prices.
    fn prices(&self) -> Option<Profile> {
        self.cost
            .or((self.tail || self.latency.is_some()).then_some(Profile::DEFAULT))
    }

    /// The prices in force for the tick under way: the run's, with staging slowed while a
    /// slow disk holds (`Hold::SlowStaging`), which charges an uncharged run for its staging
    /// calls alone. The healthy tail is never slowed.
    pub(super) fn profile_in_force(&self) -> Option<Profile> {
        let base = self.prices();
        match self.slow_staging.filter(|_| !self.tail) {
            Some(factor) => Some(Profile {
                remote: base.map_or(Link::FREE, |base| base.remote),
                staging: base
                    .unwrap_or(Profile::DEFAULT)
                    .staging
                    .slowed(u64::from(factor)),
            }),
            None => base,
        }
    }

    /// Moves the clock on by what the tick that just ran cost, when the run is charged: its
    /// calls at the prices in force, and with a latency tail, each replica request's draw from
    /// the tail past the profile's latency (`crate::cost::tail_factor`).
    pub(super) fn charge(&mut self, remote0: RecorderCounts, staging0: RecorderCounts) {
        let Some(profile) = self.profile_in_force() else {
            return;
        };
        let remote = crate::resources::delta(remote0, self.w.remote_recorder.counts());
        let staging = crate::resources::delta(staging0, self.w.staging_recorder.counts());
        let mut tenfold = 0_u64;
        let tail = match self.latency.as_ref().filter(|_| !self.tail) {
            Some(stream) => (0..crate::cost::requests(&remote)).fold(0_u64, |sum, _| {
                let factor = crate::cost::tail_factor(stream.below(crate::cost::TAIL_DRAWS));
                tenfold = tenfold.saturating_add(u64::from(factor >= 10));
                sum.saturating_add(
                    factor
                        .saturating_sub(1)
                        .saturating_mul(profile.remote.latency_us),
                )
            }),
            None => 0,
        };
        if tenfold > 0 {
            self.report.reach.add("gray.latency.tenfold", tenfold);
        }
        self.w.clock.advance(
            profile
                .tick(&remote, &staging)
                .saturating_add(Duration::from_micros(tail)),
        );
    }

    /// What one tick may cost, from the run's configuration.
    fn allowance(&self) -> Allowance {
        let limits = self.limits();
        Allowance {
            slice: limits.slice_bytes,
            staged: limits.staged_bytes,
            part: self.part_bytes,
        }
    }

    /// Judges the tick just taken in against its bound ([`crate::cost::judge`]).
    pub(super) fn judge_tick_cost(
        &mut self,
        stats: &TickStats,
        (remote_at, staging_at): (usize, usize),
    ) -> Result<(), Violation> {
        let remote_puts = puts_since(&self.w.remote_recorder.events(), remote_at);
        let staging_puts = puts_since(&self.w.staging_recorder.events(), staging_at);
        let allowance = self.allowance();
        crate::cost::judge(
            &Profile::DEFAULT,
            allowance,
            (&stats.remote, remote_puts),
            (&stats.staging, staging_puts),
            &mut self.spare,
        )?;
        self.report.reach.bump("tick_cost.judged");
        Ok(())
    }

    /// Judges what the tick held the write barrier across (`crate::barrier`): no replica
    /// request, a wait within one staged file's price for each barrier, and, when the tick ended without its
    /// process dying, no barrier still held.
    pub(super) fn judge_barrier(&mut self, done: bool) -> Result<(), Violation> {
        let held = self.barrier.take();
        if held.holds > 0 {
            self.report.reach.bump("barrier.judged");
            within(Bound::BarrierRemoteRequests, held.remote_requests, 0)?;
            // Each barrier may stage one file, the WAL's tail since the sync, of at most what one
            // sync may stage; nothing else.
            let limit = Profile::DEFAULT.staging.micros(
                held.holds.saturating_mul(crate::cost::REQUESTS_PER_PUT),
                held.holds
                    .saturating_mul(self.allowance().written(crate::cost::Store::Staging)),
            );
            within(Bound::BarrierWaitMicros, held.wait_micros, limit)?;
            // Each barrier's PASSIVE may copy twice the run's passive threshold, and never under
            // 64 frames (docs/decisions/D303.md), plus one commit of the application's made
            // inside the tick; never the backlog of a snapshot that held checkpoints back.
            let per_barrier = u64::from(super::PASSIVE_PAGES)
                .saturating_mul(2)
                .max(64)
                .saturating_add(self.max_tx_pages);
            within(
                Bound::BarrierCopiedFrames,
                held.copied_frames,
                held.holds.saturating_mul(per_barrier),
            )?;
        }
        if done {
            within(Bound::BarrierHeldAfterTick, u64::from(held.open), 0)?;
        }
        Ok(())
    }

    /// A boot's spare for its whole-file pass over staging ([`Spare`]), from staging's own
    /// sizes.
    pub(super) fn boot_spare(&self) -> Spare {
        let staged = self.w.staged_files().unwrap_or_default();
        let bytes = staged
            .iter()
            .fold(0_u64, |sum, info| sum.saturating_add(info.size));
        let files = u64::try_from(staged.len()).unwrap_or(u64::MAX);
        Spare::boot(&Profile::DEFAULT, (files, bytes))
    }

    /// The newest transaction the bucket holds at any level.
    fn replica_frontier(&self) -> Option<Txid> {
        self.w
            .remote_raw
            .stored()
            .iter()
            .map(|info| info.key.range().max())
            .max()
    }

    /// Every staged file at any level, with the digest of its bytes.
    fn staged_digests(&self) -> Vec<(FileKey, u64)> {
        self.w
            .staged_files()
            .unwrap_or_default()
            .iter()
            .filter_map(|info| Some((info.key, self.w.staged_digest(&info.key)?)))
            .collect()
    }

    /// The digests staging and the bucket hold at `key`, each where it holds a file.
    fn held_at(&self, key: &FileKey) -> Vec<u64> {
        [self.w.staged_digest(key), (self.w.remote_raw.landed())(key)]
            .into_iter()
            .flatten()
            .collect()
    }

    /// The bytes of staged level-0 files newer than `frontier`: what must reach the replica
    /// before a commit staged after them can.
    fn pending(&self, frontier: Option<Txid>) -> u64 {
        self.w
            .staged_files()
            .unwrap_or_default()
            .iter()
            .filter(|info| info.key.level() == lsr_core::Level::ZERO)
            .filter(|info| frontier.is_none_or(|f| info.key.range().max() > f))
            .fold(0_u64, |sum, info| sum.saturating_add(info.size))
    }

    /// Simulated time one of the tail's commits may take to reach the replica with `pending`
    /// bytes staged ahead of it, the commit's own file among them: the ticks it may wait
    /// behind a job's slices ([`SLICES_AHEAD`]) and the two that sync and send it, and for
    /// each slice of what is pending the nine ticks a job leaves the send (eight of its slices
    /// and the send's own), each at a slice's price, and one second of the engine's waits.
    /// A job that holds the tick for longer than its slice is what this catches.
    pub(super) fn lag_bound(&self, pending: u64) -> Duration {
        let slice = self.limits().slice_bytes.max(1);
        let profile = Profile::DEFAULT;
        let bytes = slice
            .saturating_mul(2)
            .saturating_add(crate::budget::PER_FILE);
        let per_slice = profile
            .remote
            .micros(crate::cost::REQUESTS_PER_PUT, bytes)
            .saturating_add(profile.staging.micros(crate::cost::REQUESTS_PER_PUT, bytes));
        let slices = pending.div_ceil(slice);
        let ticks = SLICES_AHEAD
            .saturating_add(2)
            .saturating_add(slices.saturating_mul(SLICES_AHEAD.saturating_add(1)));
        Duration::from_micros(per_slice.saturating_mul(ticks))
            .saturating_add(Duration::from_secs(1))
    }

    /// The healthy tail (module doc).
    pub(super) fn healthy_tail(&mut self) -> Result<(), Violation> {
        self.tail = true;
        // A level-9 snapshot goes up while the tail commits: every run's tail meets a job
        // many slices long, and its commits must get the turns they are owed.
        if self.cost.is_some() {
            self.forcings.note_op(&crate::Op::RequestSnapshot);
            self.actor.request_snapshot();
        }
        let millis = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
        let remote0 = self.w.remote_recorder.counts();
        for _ in 0..TAIL_COMMITS {
            let frontier = self.replica_frontier();
            let mut before = Before::new(frontier, self.staged_digests());
            self.commit(TAIL_COMMIT_PAGES, false);
            // Staged files older than the commit upload before its own: the first file past
            // the frontier is then theirs, and the WAL has had no sync to checkpoint after.
            let backlog = self.pending(frontier) > 0;
            let mut began = self.now();
            let mut excused = false;
            let mut reached = false;
            let mut pending = 0;
            for _ in 0..TAIL_TICKS {
                pending = pending.max(self.pending(frontier));
                let bound = self.lag_bound(pending);
                let tick_began = self.now();
                let faults0 = self.faults_fired();
                let (step, stats) = self.step(None)?;
                if self.faults_fired() != faults0 {
                    // Something the world still imposes fired: the window starts again after
                    // it, and after the retry wait the engine announced for it, if any (the
                    // fault is the world's record; the wait is the engine's own signal).
                    self.report.reach.bump("liveness.excused_by_fault");
                    excused = true;
                    began = match stats.event {
                        Some(Event::Retrying { until, .. } | Event::Degraded { until, .. }) => {
                            until.max(self.now())
                        }
                        _ => self.now(),
                    };
                    continue;
                }
                if self.tail_ends(&step, stats.event.as_ref())? {
                    self.report.reach.bump("liveness.unarmed");
                    return Ok(());
                }
                // To the end of this tick's I/O, not past the wait the engine asked for after it.
                let waited = Duration::from_millis(
                    u64::try_from(tick_began.unix_millis().saturating_sub(began.unix_millis()))
                        .unwrap_or(0),
                )
                .saturating_add(Profile::DEFAULT.tick(&stats.remote, &stats.staging));
                if self.replica_frontier() > before.newest(|key| self.held_at(key)) {
                    reached = true;
                    self.report.reach.bump("liveness.judged");
                    within(Bound::CommitLagMillis, millis(waited), millis(bound))?;
                    // A fault in the window may have failed a checkpoint, a reader still open
                    // blocks the WAL's restart, and a backlog ahead of the commit means the file
                    // that arrived is not its own: the bound is for a healthy WAL.
                    if excused || backlog || !self.readers.is_empty() {
                        self.report.reach.bump("wal_bound.excused");
                    } else {
                        self.report.reach.bump("wal_bound.judged");
                        within(
                            Bound::WalFrames,
                            u64::from(self.w.sqlite.mx_frame()),
                            u64::from(wal_bound()),
                        )?;
                    }
                    break;
                }
                within(Bound::CommitLagMillis, millis(waited), millis(bound))?;
            }
            if !reached {
                within(
                    Bound::DrainTicks,
                    u64::from(TAIL_TICKS).saturating_add(1),
                    u64::from(TAIL_TICKS),
                )?;
            }
            self.w.clock.advance(TAIL_GAP);
        }
        let spent = crate::resources::delta(remote0, self.w.remote_recorder.counts());
        // An upload still open (the snapshot the tail asked for, part-way) wasted nothing yet,
        // and one the store refused for a key it holds from an earlier commit of the same
        // upload (one that landed and answered with an error) is how the engine finds out.
        let wasted = spent
            .written_bytes
            .saturating_sub(spent.committed_bytes)
            .saturating_sub(spent.refused_held_bytes)
            .saturating_sub(self.w.remote_recorder.open_bytes());
        let open = self
            .limits()
            .slice_bytes
            .saturating_add(crate::budget::PER_FILE);
        within(Bound::WastedBytes, wasted, open)
    }

    /// Every injected fault that has fired so far, counted from the fault layers and the
    /// SQLite side, never from the engine's reports.
    fn faults_fired(&self) -> (u64, u64, u64, usize) {
        let (staging, remote) = self.fault_hits();
        let wire = self.remote_fired().all;
        let source = self.w.sqlite.taken_injections().len();
        (staging, remote, wire, source)
    }

    /// Whether a step ends the tail unjudged: a stop the run expects, or a task a persistent
    /// cause the world still imposes keeps degraded.
    fn tail_ends(&self, step: &Step, _event: Option<&Event>) -> Result<bool, Violation> {
        self.ends_idle(step)
    }
}
