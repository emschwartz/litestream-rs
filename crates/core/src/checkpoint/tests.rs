use std::io::Cursor;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{Event, Header, PageNo, PageSize, ReadDecoder, Timestamp};
use lsr_wal::{Salts, WalReader};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::fault::SnapshotCause;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::position::Continuity;
use crate::source::{
    CheckpointCall, CheckpointEffect, CheckpointScript, CommittedRead, FakeCheckpointer, FakeDb,
    FakeWal, SourceError, SourceOp,
};
use crate::store::{ObjectStore, PageLimit, ReadLen};

const PS: PageSize = PageSize::MIN;

use std::num::NonZeroU64;

fn pg(n: u32) -> PageNo {
    PageNo::new(n, PS).unwrap()
}

fn nz(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

fn ts(n: i64) -> Timestamp {
    Timestamp::from_unix_millis(n)
}

// The pure decision.
fn cfg(interval: Option<u64>, min: u32, truncate: u32) -> CheckpointConfig {
    CheckpointConfig {
        interval: interval.map(Duration::from_millis),
        min_pages: NonZeroU32::new(min),
        truncate_pages: NonZeroU32::new(truncate),
    }
}

fn obs(now: i64, frames: u32, through: bool) -> Observed {
    Observed {
        now: ts(now),
        synced_frames: frames,
        through,
    }
}

fn state_of(kind: u8, last: i64) -> CheckpointState {
    match kind % 4 {
        0 => CheckpointState::Idle {
            last: ts(last),
            expect_bump: false,
        },
        1 => CheckpointState::Idle {
            last: ts(last),
            expect_bump: true,
        },
        2 => CheckpointState::Pending { last: ts(last) },
        _ => CheckpointState::PassiveFailedAtTruncate { last: ts(last) },
    }
}

/// Go's ladder (db.go:1507-1594), flags mapped from the state; Idle is the one state the
/// minimum and the interval skip, and only Pending waits out the interval.
fn go_ladder(state: &CheckpointState, o: &Observed, c: &CheckpointConfig) -> Decision {
    let (failed, armed) = match state {
        CheckpointState::Idle { .. } => (false, false),
        CheckpointState::Pending { .. } => (false, true),
        CheckpointState::PassiveFailedAtTruncate { .. } => (true, true),
    };
    let pending = matches!(state, CheckpointState::Pending { .. });
    if let Some(limit) = c.truncate_pages
        && o.synced_frames >= limit.get()
    {
        return if failed {
            Decision::Truncate
        } else {
            Decision::Passive(CheckpointTrigger::TruncateThreshold)
        };
    }
    if !o.through {
        return Decision::Nothing;
    }
    if armed
        && let Some(min) = c.min_pages
        && o.synced_frames >= min.get()
    {
        return Decision::Passive(CheckpointTrigger::MinPages);
    }
    let waited = o
        .now
        .unix_millis()
        .saturating_sub(state.last().unix_millis());
    if pending
        && let Some(interval) = c.interval
        && waited >= i64::try_from(interval.as_millis()).unwrap()
        && o.synced_frames > 1
    {
        return Decision::Passive(CheckpointTrigger::Interval);
    }
    Decision::Nothing
}

proptest! {
    #[test]
    fn decide_follows_the_ladder(
        kind in 0_u8..4,
        last in 0_i64..10_000,
        now in 0_i64..20_000,
        frames in 0_u32..300,
        through in any::<bool>(),
        interval in proptest::option::of(1_u64..5_000),
        min in 0_u32..50,
        truncate in 0_u32..200,
    ) {
        let state = state_of(kind, last);
        let c = cfg(interval, min, truncate);
        let o = obs(now, frames, through);
        prop_assert_eq!(decide(&state, &o, &c), go_ladder(&state, &o, &c));
    }

    #[test]
    fn an_idle_state_never_checkpoints_below_the_truncate_threshold(
        expect_bump in any::<bool>(),
        now in 0_i64..1_000_000,
        frames in 0_u32..100,
    ) {
        let state = CheckpointState::Idle { last: ts(0), expect_bump };
        let c = cfg(Some(1), 1, 1000);
        prop_assert_eq!(decide(&state, &obs(now, frames, true), &c), Decision::Nothing);
    }
}

#[test]
fn the_interval_counts_from_last_and_is_inclusive() {
    let c = cfg(Some(1000), 0, 0);
    let state = CheckpointState::Pending { last: ts(5000) };
    assert_eq!(decide(&state, &obs(5999, 5, true), &c), Decision::Nothing);
    assert_eq!(
        decide(&state, &obs(6000, 5, true), &c),
        Decision::Passive(CheckpointTrigger::Interval)
    );
    assert_eq!(next_wake(&state, &c), Some(ts(6000)));
}

#[test]
fn one_frame_is_not_worth_an_interval_checkpoint() {
    let c = cfg(Some(10), 0, 0);
    let state = CheckpointState::Pending { last: ts(0) };
    assert_eq!(decide(&state, &obs(100, 1, true), &c), Decision::Nothing);
}

#[test]
fn a_capped_step_only_triggers_the_truncate_threshold() {
    let c = cfg(Some(1), 1, 10);
    let state = CheckpointState::Pending { last: ts(0) };
    assert_eq!(decide(&state, &obs(100, 5, false), &c), Decision::Nothing);
    assert_eq!(
        decide(&state, &obs(100, 10, false), &c),
        Decision::Passive(CheckpointTrigger::TruncateThreshold)
    );
}

#[test]
fn a_failed_passive_at_the_threshold_goes_to_truncate() {
    let c = cfg(None, 0, 10);
    let failed = CheckpointState::PassiveFailedAtTruncate { last: ts(0) };
    assert_eq!(decide(&failed, &obs(1, 10, true), &c), Decision::Truncate);
}

#[test]
fn the_passive_before_a_truncate_hands_back_now() {
    let c = cfg(Some(1000), 0, 10);
    let o = obs(50, 10, true);
    let (state, next) = after_checkpoint(
        CheckpointState::Pending { last: ts(0) },
        Decision::Passive(CheckpointTrigger::TruncateThreshold),
        &CheckpointOutcome::NotRestarted,
        &o,
        &c,
    );
    assert_eq!(
        state,
        CheckpointState::PassiveFailedAtTruncate { last: ts(50) }
    );
    assert_eq!(next, Next::Now);
}

#[test]
fn a_truncate_that_could_not_restart_waits_an_interval() {
    let c = cfg(Some(1000), 0, 10);
    let (state, next) = after_checkpoint(
        CheckpointState::PassiveFailedAtTruncate { last: ts(0) },
        Decision::Truncate,
        &CheckpointOutcome::Busy,
        &obs(50, 10, true),
        &c,
    );
    assert_eq!(
        state,
        CheckpointState::PassiveFailedAtTruncate { last: ts(50) }
    );
    assert_eq!(next, Next::At(ts(1050)));
}

#[test]
fn the_failed_state_relaxes_below_the_threshold() {
    let c = cfg(Some(10), 0, 100);
    let state = after_sync(
        CheckpointState::PassiveFailedAtTruncate { last: ts(3) },
        2,
        4,
        &c,
    );
    assert_eq!(state, CheckpointState::Pending { last: ts(3) });
}

#[test]
fn the_failed_state_stays_at_the_threshold() {
    let c = cfg(Some(10), 0, 100);
    let failed = CheckpointState::PassiveFailedAtTruncate { last: ts(3) };
    assert_eq!(after_sync(failed, 2, 100, &c), failed);
    assert_eq!(after_sync(failed, 2, 250, &c), failed);
}

#[test]
fn our_own_bump_frame_does_not_arm_the_time_trigger() {
    let c = cfg(Some(10), 0, 0);
    let idle = CheckpointState::Idle {
        last: ts(0),
        expect_bump: true,
    };
    let once = after_sync(idle, 1, 1, &c);
    assert_eq!(
        once,
        CheckpointState::Idle {
            last: ts(0),
            expect_bump: false
        }
    );
    let again = after_sync(once, 1, 2, &c);
    assert_eq!(again, CheckpointState::Pending { last: ts(0) });
    let two = after_sync(idle, 2, 2, &c);
    assert_eq!(two, CheckpointState::Pending { last: ts(0) });
}

#[test]
fn a_busy_interval_passive_keeps_the_trigger_for_one_more_interval() {
    let c = cfg(Some(1000), 0, 0);
    let (state, next) = after_checkpoint(
        CheckpointState::Pending { last: ts(0) },
        Decision::Passive(CheckpointTrigger::Interval),
        &CheckpointOutcome::Busy,
        &obs(1000, 5, true),
        &c,
    );
    assert_eq!(state, CheckpointState::Pending { last: ts(1000) });
    assert_eq!(next, Next::At(ts(2000)));
}

/// One engine tick, as the caller will drive it: sync, decide, at most one checkpoint.
fn tick_passives(
    state: &mut CheckpointState,
    wrote: u32,
    now: i64,
    frames: u32,
    c: &CheckpointConfig,
) -> bool {
    let o = obs(now, frames, true);
    *state = after_sync(*state, wrote, frames, c);
    let decision = decide(state, &o, c);
    if let Decision::Passive(_) = decision {
        let outcome = CheckpointOutcome::NotRestarted;
        (*state, _) = after_checkpoint(*state, decision, &outcome, &o, c);
        return true;
    }
    false
}

#[test]
fn an_idle_database_never_checkpoints_again() {
    let c = cfg(Some(10), 5, 1000);
    let mut state = CheckpointState::Pending { last: ts(0) };
    assert!(tick_passives(&mut state, 3, 100, 6, &c));
    // The bump frame arrives as one sync of one transaction, then nothing.
    let mut count = 0;
    let mut frames = 7;
    for now in (200..1200).step_by(10) {
        let wrote = u32::from(now == 200);
        frames += wrote;
        count += u32::from(tick_passives(&mut state, wrote, now, frames, &c));
    }
    assert_eq!(count, 0);
}

// The protocol.
struct Rig {
    wal: FakeWal,
    db: FakeDb,
    ck: FakeCheckpointer,
    store: MemStore,
}

fn db_fill(n: u32) -> u8 {
    0xD0_u8.saturating_add(u8::try_from(n).unwrap())
}

impl Rig {
    fn new() -> Self {
        let wal = FakeWal::new(
            PS,
            Salts {
                salt1: 10,
                salt2: 20,
            },
        );
        let mut db = FakeDb::new(PS);
        db.set_page_count(3);
        for n in 1..=3 {
            db.write_page(pg(n), &[db_fill(n); 512]);
        }
        Self {
            ck: FakeCheckpointer::new(wal.clone()),
            wal,
            db,
            store: MemStore::new(),
        }
    }

    fn commit(&self, pages: &[u32]) {
        let pages: Vec<(PageNo, Vec<u8>)> = pages
            .iter()
            .map(|n| (pg(*n), vec![u8::try_from(*n).unwrap(); PS.bytes()]))
            .collect();
        self.wal.append_tx(&pages, nz(3));
    }

    fn step(&mut self, step: Step, now: i64) -> Synced {
        let mut wal = WalReader::open(Cursor::new(self.wal.file_bytes())).unwrap();
        let committed = CommittedRead::new(
            self.wal.committed_end(),
            self.wal.backfilled(),
            self.wal.epoch(),
        )
        .unwrap();
        block_on(sync_step(
            step,
            &mut wal,
            committed,
            &mut self.db,
            &mut PageChecksumTable::new(),
            &self.store,
            &SyncConfig::default(),
            ts(now),
        ))
        .unwrap()
    }

    /// A first snapshot of one commit: the proof every protocol test starts from.
    fn proof(&mut self) -> SyncedThrough {
        self.commit(&[2]);
        let step = Step::Snapshot {
            after: None,
            prev_timestamp: None,
            cause: SnapshotCause::FirstSync,
        };
        match self.step(step, 1000) {
            Synced::Wrote {
                end: StepEnd::Through(reached),
                ..
            } => reached.confirm(&self.wal).unwrap(),
            other @ (Synced::Idle(_) | Synced::IndexReset | Synced::Wrote { .. }) => {
                panic!("expected a snapshot through the end, got {other:?}")
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "the protocol's own result, returned as is"
    )]
    fn run(
        &mut self,
        proof: SyncedThrough,
        mode: CheckpointMode,
    ) -> Result<Checkpointed, CheckpointFailed> {
        self.run_with(proof, mode, &SyncConfig::default())
    }

    #[expect(
        clippy::result_large_err,
        reason = "the protocol's own result, returned as is"
    )]
    fn run_with(
        &mut self,
        proof: SyncedThrough,
        mode: CheckpointMode,
        cfg: &SyncConfig,
    ) -> Result<Checkpointed, CheckpointFailed> {
        block_on(checkpoint(
            proof,
            mode,
            &mut self.wal,
            &mut self.db,
            &mut PageChecksumTable::new(),
            &mut self.ck,
            &self.store,
            cfg,
            &crate::env::FakeClock::new(ts(5000)),
        ))
    }

    fn files(&self) -> Vec<StagedFile> {
        block_on(async {
            let listing = self
                .store
                .list(Level::ZERO, None, PageLimit::MAX)
                .await
                .unwrap();
            let mut out = Vec::new();
            for info in listing.files {
                let len = ReadLen::new(ReadLen::MAX_RAW).unwrap();
                let bytes = self.store.read(&info.key, 0, len).await.unwrap();
                let mut decoder = ReadDecoder::new(bytes.as_slice());
                let mut header = None;
                let mut pages = Vec::new();
                while let Some(event) = decoder.next_event().unwrap() {
                    match event {
                        Event::Header(h) => header = Some(h),
                        Event::Page(page) => pages.push((page.pgno(), page.data().to_vec())),
                    }
                }
                decoder.finish().unwrap();
                out.push((info, header.unwrap(), pages));
            }
            out
        })
    }
}

type StagedFile = (FileInfo, Header, Vec<(PageNo, Vec<u8>)>);

fn script(busy: bool, effect: CheckpointEffect) -> CheckpointScript {
    CheckpointScript { busy, effect }
}

#[test]
fn a_passive_with_nothing_in_the_way_seals_then_follows_the_writer_restart() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    // A fully backfilled WAL is restarted by the bump frame's writer, as SQLite does.
    assert_eq!(done.outcome, CheckpointOutcome::Restarted);
    assert_eq!(done.staged.files().len(), 2);
    assert_eq!(
        rig.ck.calls(),
        [
            CheckpointCall::BeginBarrier,
            CheckpointCall::Checkpoint(CheckpointMode::Passive),
            CheckpointCall::EndBarrier,
            CheckpointCall::BumpSeq,
        ]
    );
    assert_eq!(
        done.staged.proof().unwrap().position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
}

#[test]
fn a_passive_held_back_by_a_reader_is_not_restarted() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.ck.script(script(false, CheckpointEffect::Backfill(1)));
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::NotRestarted);
    // The barrier kept writers out until the read transaction was retaken,
    // so the position is pinned again, at the epoch the checkpoint retook.
    assert_eq!(
        done.staged.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
    // Our own bump frame is past it: no proof that it is the committed end.
    assert_eq!(done.staged.proof(), None);
}

#[test]
fn a_busy_passive_reports_busy() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.ck.script(script(true, CheckpointEffect::Nothing));
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Busy);
}

/// A file the seal stages that changes no page the table holds is reported quiet, so
/// fail-stop does not count it as a commit; a file that changes a page is not.
#[test]
fn a_seal_that_changes_no_page_is_reported_quiet() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    // The same bytes again on page 2: a write that changes nothing.
    rig.commit(&[2]);
    let mut table = PageChecksumTable::new();
    table.fit(PS);
    table.record(pg(2), lsr_ltx::PageChecksum::of(pg(2), &[2; 512]));
    let done = block_on(checkpoint(
        proof,
        CheckpointMode::Passive,
        &mut rig.wal,
        &mut rig.db,
        &mut table,
        &mut rig.ck,
        &rig.store,
        &SyncConfig::default(),
        &crate::env::FakeClock::new(ts(5000)),
    ))
    .unwrap();
    let files: Vec<FileKey> = done.staged.files().iter().map(|f| f.key).collect();
    assert_eq!(files.len(), 2, "the seal and the new generation");
    assert_eq!(
        done.staged.quiet(),
        &files[..1],
        "only the seal: the bump writes a page the table does not hold"
    );
}

#[test]
fn a_passive_that_restarts_the_wal_starts_a_new_generation_file() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    let before = rig.wal.salts();
    rig.ck.script(script(false, CheckpointEffect::Restart));
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Restarted);
    assert_ne!(rig.wal.salts(), before);
    assert_eq!(done.staged.files().len(), 2);
    let through = done.staged.into_proof().unwrap();
    let position = through.position();
    assert_eq!(position.continuity(), Continuity::Held(rig.wal.epoch()));
    assert_eq!(position.txid().get(), 3);
}

/// No file a TRUNCATE stages begins at the first transaction: none is a copy of the database.
fn no_snapshot(rig: &Rig, from: usize) {
    for (info, header, _) in rig.files().iter().skip(from) {
        assert!(header.fields().txids.min().get() > 1, "{:?}", info.key);
    }
}

#[test]
fn a_truncate_follows_the_new_generation_without_a_snapshot() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    let before = rig.wal.salts();
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Truncated);
    assert_eq!(rig.wal.salts().salt1, before.salt1.wrapping_add(1));
    assert_eq!(done.staged.files().len(), 1, "the new generation's frame");
    no_snapshot(&rig, 1);
    assert_eq!(
        rig.ck.calls(),
        [
            CheckpointCall::BeginBarrier,
            CheckpointCall::Checkpoint(CheckpointMode::Passive),
            CheckpointCall::EndBarrier,
            CheckpointCall::Checkpoint(CheckpointMode::Truncate),
            CheckpointCall::BumpSeq,
        ]
    );
    let through = done.staged.into_proof().unwrap();
    assert_eq!(
        through.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
}

/// A TRUNCATE whose result the engine cannot take as proof (another copy of SQLite in the
/// process, or a reset nobody saw) follows nothing: the position is not pinned, so the next
/// sync snapshots the new generation.
#[test]
fn an_unproven_truncate_leaves_the_next_sync_to_snapshot() {
    for effect in [CheckpointEffect::SecondSqlite, CheckpointEffect::ResetTwice] {
        let mut rig = Rig::new();
        let proof = rig.proof();
        rig.ck.script(script(false, CheckpointEffect::Natural));
        rig.ck.script(script(false, effect));
        let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
        assert_eq!(done.outcome, CheckpointOutcome::Truncated, "{effect:?}");
        assert_eq!(done.staged.files().len(), 0, "{effect:?}: nothing followed");
        assert_eq!(
            done.staged.position().continuity(),
            Continuity::Broken,
            "{effect:?}"
        );
    }
}

/// A commit before the barrier is sealed and staged; the TRUNCATE then truncates only what
/// the seal staged.
#[test]
fn a_commit_before_the_truncate_is_sealed_not_snapshotted() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[2]);
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Truncated);
    assert_eq!(
        done.staged.files().len(),
        2,
        "the seal and the new generation"
    );
    no_snapshot(&rig, 1);
}

/// A TRUNCATE that a commit since the barrier's release made busy leaves the WAL as it was:
/// no snapshot, and a busy outcome.
#[test]
fn a_busy_truncate_takes_no_snapshot() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.ck.script(script(false, CheckpointEffect::Natural));
    rig.ck.script(script(true, CheckpointEffect::Nothing));
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    // The bump's writer restarts the WAL the TRUNCATE could not.
    assert_eq!(done.outcome, CheckpointOutcome::Restarted);
    no_snapshot(&rig, 1);
}

/// A TRUNCATE refused because another connection holds the checkpoint lock is busy, not a fault.
#[test]
fn a_truncate_refused_the_checkpoint_lock_is_busy() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.ck.script(script(false, CheckpointEffect::Natural));
    rig.ck.script(script(false, CheckpointEffect::Refused));
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Restarted);
    assert!(rig.ck.calls().contains(&CheckpointCall::BumpSeq));
}

/// A PASSIVE whose read retake left the engine on a mark above 0 makes its own TRUNCATE busy:
/// the engine's slot blocks the restart.
#[test]
fn a_truncate_behind_the_engines_own_mark_is_busy() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.ck.script(script(false, CheckpointEffect::Nothing));
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Busy);
    assert_eq!(rig.wal.restarts(), 0);
}

/// A backend whose TRUNCATE ended the read transaction voids the proof: a fault, and the
/// position is not pinned.
#[test]
fn a_truncate_that_ended_the_read_is_a_lost_read() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.ck.script(script(false, CheckpointEffect::Natural));
    rig.ck.script(script(false, CheckpointEffect::LostRead));
    let failed = rig.run(proof, CheckpointMode::Truncate).unwrap_err();
    assert_eq!(failed.fault, Fault::ReadTxLost);
    assert_eq!(failed.staged.position().continuity(), Continuity::Broken);
}

/// A seal whose read lapsed proves nothing at the hold the PASSIVE retook, so no TRUNCATE runs.
#[test]
fn no_truncate_runs_on_a_position_the_passive_did_not_pin() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.wal.lapse_at_next_sync();
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert!(
        !rig.ck
            .calls()
            .contains(&CheckpointCall::Checkpoint(CheckpointMode::Truncate)),
        "{:?}",
        rig.ck.calls()
    );
    assert_ne!(done.outcome, CheckpointOutcome::Truncated);
}

// Failures at each step of the protocol.
fn fail_at(op: SourceOp, mode: CheckpointMode) -> (Rig, Result<Checkpointed, CheckpointFailed>) {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.wal
        .inject(op, SourceError::Io(std::io::ErrorKind::Other));
    let result = rig.run(proof, mode);
    (rig, result)
}

#[test]
fn every_passive_step_failure_releases_the_barrier_and_reports_the_fault() {
    for op in [
        SourceOp::Open,
        SourceOp::Committed,
        SourceOp::PageCount,
        SourceOp::BeginBarrier,
        SourceOp::Checkpoint,
        SourceOp::EndBarrier,
        SourceOp::BumpSeq,
    ] {
        let (mut rig, result) = fail_at(op, CheckpointMode::Passive);
        let Err(failed) = result else {
            // PageCount is not read by an incremental step: nothing to fail.
            assert_eq!(op, SourceOp::PageCount);
            continue;
        };
        assert_eq!(failed.fault, Fault::LocalIo, "{op:?}");
        if op == SourceOp::EndBarrier {
            // The fake keeps a barrier whose release failed; the call is what counts.
            assert!(rig.ck.calls().contains(&CheckpointCall::EndBarrier));
        } else {
            assert!(
                rig.ck.begin_barrier().is_ok(),
                "{op:?} left the barrier held"
            );
        }
    }
}

#[test]
fn a_position_after_a_checkpoint_is_pinned_at_the_epoch_it_retook() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    let before = rig.wal.epoch();
    rig.commit(&[3]);
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    let after = rig.wal.epoch();
    assert_ne!(
        before, after,
        "the checkpoint released the read transaction"
    );
    assert_eq!(done.staged.position().continuity(), Continuity::Held(after));
}

/// The protocol's own steps ignore the sync cap: a seal that stopped at the
/// cap would checkpoint frames it has not staged.
#[test]
fn the_seal_takes_every_committed_transaction_whatever_the_cap() {
    let run = |cfg: &SyncConfig| {
        let mut rig = Rig::new();
        let proof = rig.proof();
        rig.commit(&[3]);
        rig.commit(&[2]);
        rig.commit(&[1, 3]);
        let done = rig.run_with(proof, CheckpointMode::Passive, cfg).unwrap();
        let files: Vec<_> = done.staged.files().iter().map(|f| f.key).collect();
        (files, done.staged.position().txid(), done.outcome)
    };
    let capped = SyncConfig {
        cap: NonZeroU64::new(1),
        ..SyncConfig::default()
    };
    assert_eq!(run(&capped), run(&SyncConfig::default()));
}

/// The read transaction a PASSIVE retakes under its barrier pins the position from the moment
/// the call returns: a fault after it (here the bump) leaves the position pinned, so a writer
/// that restarts the WAL next is followed and costs no snapshot.
#[test]
fn a_fault_after_the_passive_ran_leaves_the_position_pinned() {
    let (rig, result) = fail_at(SourceOp::BumpSeq, CheckpointMode::Passive);
    let failed = result.unwrap_err();
    assert_eq!(
        failed.staged.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
    assert_eq!(failed.staged.position().txid().get(), 2);
    assert_eq!(
        failed.staged.proof(),
        None,
        "pinned, but the proof was spent"
    );
}

/// A checkpoint call that failed some other way than by losing the retake still holds a read
/// transaction begun under the barrier: the position is pinned to the hold the WAL names.
#[test]
fn a_passive_call_that_failed_leaves_the_position_pinned() {
    let (rig, result) = fail_at(SourceOp::Checkpoint, CheckpointMode::Passive);
    let failed = result.unwrap_err();
    assert_eq!(failed.fault, Fault::LocalIo);
    assert_eq!(
        failed.staged.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
    assert_eq!(failed.run, None);
}

/// A retake that was lost is the one answer that proves nothing: no read transaction holds
/// the WAL, so the position is unpinned.
#[test]
fn a_lost_retake_leaves_the_position_unpinned() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.wal
        .inject(SourceOp::Checkpoint, SourceError::ReadTxLost);
    let failed = rig.run(proof, CheckpointMode::Passive).unwrap_err();
    assert_eq!(failed.fault, Fault::ReadTxLost);
    assert_eq!(failed.staged.position().continuity(), Continuity::Broken);
    assert_eq!(failed.staged.position().txid().get(), 2);
}

/// A TRUNCATE runs after the barrier's PASSIVE and keeps the read it retook: a fault after it
/// leaves the position pinned to that hold.
#[test]
fn a_fault_after_a_truncate_keeps_the_position_pinned() {
    let (rig, result) = fail_at(SourceOp::BumpSeq, CheckpointMode::Truncate);
    let failed = result.unwrap_err();
    assert_eq!(
        failed.staged.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
}

/// Another connection holds the write lock when the PASSIVE asks for its barrier: SQLite is
/// not asked to checkpoint, nothing is staged, and the proof the caller gave comes back.
#[test]
fn a_write_lock_busy_at_the_barrier_is_an_outcome_not_a_fault() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.wal.inject(SourceOp::BeginBarrier, SourceError::Busy);
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::LockBusy);
    assert_eq!(rig.ck.calls(), [CheckpointCall::BeginBarrier]);
    assert_eq!(done.run, None);
    assert!(done.staged.files().is_empty());
    assert!(done.staged.proof().is_some(), "nothing moved");
}

/// Another connection holds the write lock when the PASSIVE's bump is due: its commit moves
/// the WAL on. The checkpoint ran, the seal is staged, and the position is pinned to the read
/// transaction retaken under the barrier.
#[test]
fn a_passive_overtaken_at_its_bump_is_pinned_and_not_a_fault() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.wal.inject(SourceOp::BumpSeq, SourceError::Busy);
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Overtaken);
    assert_eq!(done.staged.files().len(), 1, "the seal");
    assert_eq!(
        done.staged.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
    assert_eq!(done.staged.position().txid().get(), 2);
    let run = done.run.unwrap();
    assert_eq!((run.busy, run.log, run.checkpointed), (false, 2, 2));
}

/// A TRUNCATE that emptied the WAL and found the write lock held at its bump was overtaken:
/// the position stays pinned, and the writer's commit starts the generation the next sync
/// follows.
#[test]
fn a_truncate_overtaken_at_its_bump_stays_pinned() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.wal.inject(SourceOp::BumpSeq, SourceError::Busy);
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Overtaken);
    assert!(done.staged.files().is_empty(), "no snapshot");
    assert_eq!(
        done.staged.position().continuity(),
        Continuity::Held(rig.wal.epoch())
    );
}

#[test]
fn the_wait_for_a_busy_write_lock_doubles_to_the_cap() {
    let cap = Duration::from_secs(1);
    let waits: Vec<u128> = (1..=9)
        .map(|skips| lock_busy_wait(nz(skips), cap).as_millis())
        .collect();
    assert_eq!(waits, [10, 20, 40, 80, 160, 320, 640, 1000, 1000]);
    assert_eq!(lock_busy_wait(nz(u32::MAX), cap), cap);
    assert_eq!(
        lock_busy_wait(nz(1), Duration::from_millis(3)),
        Duration::from_millis(3),
        "never past the cap"
    );
    assert_eq!(LOCK_BUSY_WAIT, Duration::from_millis(10));
}

/// A write lock that was busy at the barrier changes nothing in the policy: the same state,
/// asked again.
#[test]
fn a_busy_write_lock_leaves_the_policy_where_it_was() {
    let c = cfg(Some(1000), 5, 10);
    for kind in 0..4 {
        let state = state_of(kind, 7);
        for decision in [
            Decision::Passive(CheckpointTrigger::TruncateThreshold),
            Decision::Passive(CheckpointTrigger::MinPages),
            Decision::Passive(CheckpointTrigger::Interval),
            Decision::Truncate,
        ] {
            let after = after_checkpoint(
                state,
                decision,
                &CheckpointOutcome::LockBusy,
                &obs(50, 10, true),
                &c,
            );
            assert_eq!(after, (state, Next::Now), "{state:?} {decision:?}");
        }
    }
}

/// An overtaken PASSIVE wrote no frame of ours. At the truncate threshold it did not restart
/// the WAL as far as the policy knows, so the TRUNCATE is next unless the following sync
/// shows the writer's restart; below it the checkpoint is done and expects no bump frame.
#[test]
fn an_overtaken_passive_is_finished_and_expects_no_bump_frame() {
    let c = cfg(Some(1000), 5, 10);
    let o = obs(50, 10, true);
    let pending = CheckpointState::Pending { last: ts(0) };
    let overtaken = CheckpointOutcome::Overtaken;
    assert_eq!(
        after_checkpoint(
            pending,
            Decision::Passive(CheckpointTrigger::TruncateThreshold),
            &overtaken,
            &o,
            &c
        ),
        (
            CheckpointState::PassiveFailedAtTruncate { last: ts(50) },
            Next::Now
        )
    );
    let idle = CheckpointState::Idle {
        last: ts(50),
        expect_bump: false,
    };
    for trigger in [CheckpointTrigger::MinPages, CheckpointTrigger::Interval] {
        assert_eq!(
            after_checkpoint(pending, Decision::Passive(trigger), &overtaken, &o, &c),
            (idle, Next::OnWalChange),
            "{trigger:?}"
        );
    }
    assert_eq!(
        after_checkpoint(pending, Decision::Truncate, &overtaken, &o, &c),
        (
            CheckpointState::PassiveFailedAtTruncate { last: ts(50) },
            Next::At(ts(1050))
        )
    );
    // The same writer's commit is one sync of one transaction: it arms the triggers again,
    // where a bump frame of ours would have been absorbed.
    assert_eq!(
        after_sync(idle, 1, 1, &c),
        CheckpointState::Pending { last: ts(50) }
    );
}

#[test]
fn a_failure_before_the_checkpoint_keeps_the_pinned_position() {
    let (_, result) = fail_at(SourceOp::BeginBarrier, CheckpointMode::Passive);
    let failed = result.unwrap_err();
    assert_ne!(failed.staged.position().continuity(), Continuity::Broken);
    assert_eq!(failed.staged.position().txid().get(), 1);
}

#[test]
fn a_failed_release_never_replaces_the_first_fault() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.wal.inject(SourceOp::Checkpoint, SourceError::DiskFull);
    rig.wal.inject(
        SourceOp::EndBarrier,
        SourceError::Io(std::io::ErrorKind::Other),
    );
    let failed = rig.run(proof, CheckpointMode::Passive).unwrap_err();
    assert_eq!(failed.fault, Fault::from_source(SourceError::DiskFull));
}

#[test]
fn every_truncate_step_failure_reports_the_fault_and_holds_no_lock() {
    for op in [
        SourceOp::Open,
        SourceOp::BeginBarrier,
        SourceOp::Checkpoint,
        SourceOp::BumpSeq,
        SourceOp::Committed,
    ] {
        let (mut rig, result) = fail_at(op, CheckpointMode::Truncate);
        let failed = result.unwrap_err();
        assert_eq!(failed.fault, Fault::LocalIo, "{op:?}");
        let barrier = rig.ck.begin_barrier();
        assert!(barrier.is_ok(), "{op:?} left the writer held");
    }
}

/// A step that fails after a step staged a file hands that file back with
/// the position it leaves, so the caller can upload it.
#[test]
fn a_failure_after_a_staging_step_hands_back_the_file_it_staged() {
    for (op, mode) in [
        (SourceOp::Checkpoint, CheckpointMode::Passive),
        (SourceOp::EndBarrier, CheckpointMode::Passive),
        (SourceOp::BumpSeq, CheckpointMode::Passive),
    ] {
        let (rig, result) = fail_at(op, mode);
        let failed = result.unwrap_err();
        let position = failed.staged.position();
        let newest = failed
            .staged
            .files()
            .iter()
            .map(|f| f.key.range().max())
            .max();
        assert_eq!(newest, Some(position.txid()), "{op:?}");
        let on_disk = rig.files().last().map(|(info, _, _)| info.key);
        assert_eq!(
            on_disk,
            failed.staged.files().last().map(|f| f.key),
            "{op:?}"
        );
    }
}

/// The proof comes back only when the protocol's last step reached the
/// committed end: a restart followed, or a busy TRUNCATE's catch-up.
#[test]
fn a_checkpoint_returns_a_proof_only_after_a_step_through_the_end() {
    let run = |effect: Option<CheckpointEffect>, mode| {
        let mut rig = Rig::new();
        let proof = rig.proof();
        rig.commit(&[3]);
        if let Some(effect) = effect {
            rig.ck.script(script(false, effect));
        }
        let done = rig.run(proof, mode).unwrap();
        (done.outcome, done.staged.proof().is_some())
    };
    assert_eq!(
        run(None, CheckpointMode::Passive),
        (CheckpointOutcome::Restarted, true)
    );
    assert_eq!(
        run(Some(CheckpointEffect::Backfill(1)), CheckpointMode::Passive),
        (CheckpointOutcome::NotRestarted, false)
    );
    assert_eq!(
        run(None, CheckpointMode::Truncate),
        (CheckpointOutcome::Truncated, true)
    );
    assert_eq!(
        run(Some(CheckpointEffect::Nothing), CheckpointMode::Truncate),
        (CheckpointOutcome::Busy, true)
    );
}
