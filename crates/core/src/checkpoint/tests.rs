use std::io::Cursor;
use std::num::NonZeroU32;
use std::time::Duration;

use lsr_ltx::{Event, Header, PageNo, PageSize, ReadDecoder, Timestamp};
use lsr_wal::{Salts, WalReader};
use proptest::prelude::*;

use super::*;
use crate::conformance::block_on;
use crate::layout::Level;
use crate::mem::MemStore;
use crate::position::Continuity;
use crate::source::{
    CheckpointCall, CheckpointEffect, CheckpointScript, CommittedRead, FakeCheckpointer, FakeDb,
    FakeWal, SourceError, SourceOp,
};
use crate::store::{ObjectStore, PageLimit, ReadLen};

const PS: PageSize = PageSize::MIN;

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
            Decision::Passive(Why::TruncateThreshold)
        };
    }
    if !o.through {
        return Decision::Nothing;
    }
    if armed
        && let Some(min) = c.min_pages
        && o.synced_frames >= min.get()
    {
        return Decision::Passive(Why::MinPages);
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
        return Decision::Passive(Why::Interval);
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
        Decision::Passive(Why::Interval)
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
        Decision::Passive(Why::TruncateThreshold)
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
        Decision::Passive(Why::TruncateThreshold),
        &CheckpointOutcome::NotRestarted {
            fully_backfilled: false,
        },
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
        &obs(5, 4, true),
        &c,
    );
    assert_eq!(state, CheckpointState::Pending { last: ts(3) });
}

#[test]
fn our_own_bump_frame_does_not_arm_the_time_trigger() {
    let c = cfg(Some(10), 0, 0);
    let idle = CheckpointState::Idle {
        last: ts(0),
        expect_bump: true,
    };
    let once = after_sync(idle, 1, &obs(1, 1, true), &c);
    assert_eq!(
        once,
        CheckpointState::Idle {
            last: ts(0),
            expect_bump: false
        }
    );
    let again = after_sync(once, 1, &obs(2, 2, true), &c);
    assert_eq!(again, CheckpointState::Pending { last: ts(0) });
    let two = after_sync(idle, 2, &obs(2, 2, true), &c);
    assert_eq!(two, CheckpointState::Pending { last: ts(0) });
}

#[test]
fn a_busy_interval_passive_keeps_the_trigger_for_one_more_interval() {
    let c = cfg(Some(1000), 0, 0);
    let (state, next) = after_checkpoint(
        CheckpointState::Pending { last: ts(0) },
        Decision::Passive(Why::Interval),
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
    *state = after_sync(*state, wrote, &o, c);
    let decision = decide(state, &o, c);
    if let Decision::Passive(_) = decision {
        let outcome = CheckpointOutcome::NotRestarted {
            fully_backfilled: true,
        };
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
        let committed =
            CommittedRead::new(self.wal.committed_end(), self.wal.backfilled()).unwrap();
        block_on(sync_step(
            step,
            &mut wal,
            committed,
            self.wal.epoch(),
            &mut self.db,
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
                end: StepEnd::Through(through),
                ..
            } => through,
            other @ (Synced::Idle(_) | Synced::Wrote { .. }) => {
                panic!("expected a snapshot through the end, got {other:?}")
            }
        }
    }

    fn run(
        &mut self,
        proof: SyncedThrough,
        mode: CheckpointMode,
    ) -> Result<Checkpointed, CheckpointFailed> {
        block_on(checkpoint(
            proof,
            mode,
            &mut self.wal,
            &mut self.db,
            &mut self.ck,
            &self.store,
            &SyncConfig::default(),
            ts(5000),
        ))
    }

    fn files(&self) -> Vec<Staged> {
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
                        Event::Page { pgno, data } => pages.push((pgno, data.to_vec())),
                    }
                }
                decoder.finish().unwrap();
                out.push((info, header.unwrap(), pages));
            }
            out
        })
    }
}

type Staged = (FileInfo, Header, Vec<(PageNo, Vec<u8>)>);

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
    assert_eq!(done.files.len(), 2);
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
        done.through.position().continuity(),
        Continuity::Pinned(rig.wal.epoch())
    );
}

#[test]
fn a_passive_held_back_by_a_reader_is_not_fully_backfilled() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[3]);
    rig.ck.script(script(false, CheckpointEffect::Backfill(1)));
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(
        done.outcome,
        CheckpointOutcome::NotRestarted {
            fully_backfilled: false
        }
    );
}

#[test]
fn a_busy_passive_reports_busy() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.ck.script(script(true, CheckpointEffect::Nothing));
    let done = rig.run(proof, CheckpointMode::Passive).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Busy);
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
    assert_eq!(done.files.len(), 2);
    let position = done.through.position();
    assert_eq!(position.continuity(), Continuity::Pinned(rig.wal.epoch()));
    assert_eq!(position.txid().get(), 3);
}

#[test]
fn a_truncate_snapshots_the_new_generation_once_under_the_write_lock() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Truncated);
    assert_eq!(done.files.len(), 1);
    let files = rig.files();
    let (_, header, _) = files.last().unwrap();
    assert_eq!(header.fields().txids.max().get(), 2);
    assert_eq!(
        rig.ck.calls(),
        [
            CheckpointCall::Checkpoint(CheckpointMode::Truncate),
            CheckpointCall::BumpSeq,
            CheckpointCall::BeginWriteLock,
            CheckpointCall::EndWriteLock,
        ]
    );
    let barrier = rig.ck.begin_barrier().unwrap();
    rig.ck.end_barrier(barrier).unwrap();
}

#[test]
fn a_commit_before_the_truncate_is_in_the_snapshot() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.commit(&[2]);
    // The checkpoint backfills it; the fake WAL's backfill is a database write.
    rig.db.write_page(pg(2), &[2; 512]);
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Truncated);
    let files = rig.files();
    let (_, _, pages) = files.last().unwrap();
    let page2 = pages.iter().find(|(pgno, _)| pgno.get() == 2).unwrap();
    assert_eq!(page2.1.first(), Some(&2));
}

#[test]
fn a_busy_truncate_that_changed_nothing_takes_no_snapshot() {
    let mut rig = Rig::new();
    let proof = rig.proof();
    rig.ck.script(script(true, CheckpointEffect::Nothing));
    let done = rig.run(proof, CheckpointMode::Truncate).unwrap();
    assert_eq!(done.outcome, CheckpointOutcome::Busy);
    assert!(done.files.len() <= 1);
    assert!(!rig.ck.calls().contains(&CheckpointCall::BeginWriteLock));
    assert_eq!(rig.files().len(), 1 + done.files.len());
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
fn a_position_after_a_checkpoint_fault_is_unpinned() {
    let (_, result) = fail_at(SourceOp::BumpSeq, CheckpointMode::Passive);
    let failed = result.unwrap_err();
    assert_eq!(failed.position.continuity(), Continuity::Unpinned);
    assert_eq!(failed.position.txid().get(), 2);
}

#[test]
fn a_failure_before_the_checkpoint_keeps_the_pinned_position() {
    let (_, result) = fail_at(SourceOp::BeginBarrier, CheckpointMode::Passive);
    let failed = result.unwrap_err();
    assert_ne!(failed.position.continuity(), Continuity::Unpinned);
    assert_eq!(failed.position.txid().get(), 1);
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
fn every_truncate_step_failure_releases_the_write_lock() {
    for op in [
        SourceOp::Open,
        SourceOp::Checkpoint,
        SourceOp::BumpSeq,
        SourceOp::Len,
        SourceOp::BeginWriteLock,
        SourceOp::EndWriteLock,
    ] {
        let (mut rig, result) = fail_at(op, CheckpointMode::Truncate);
        let failed = result.unwrap_err();
        assert_eq!(failed.fault, Fault::LocalIo, "{op:?}");
        if op == SourceOp::EndWriteLock {
            assert!(rig.ck.calls().contains(&CheckpointCall::EndWriteLock));
        } else if op != SourceOp::BeginWriteLock {
            assert!(
                rig.ck.begin_write_lock().is_ok(),
                "{op:?} left the lock held"
            );
        }
    }
}
