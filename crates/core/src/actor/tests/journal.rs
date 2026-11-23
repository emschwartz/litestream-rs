//! Boot reads the journal and reports it; whatever it finds, the engine does what it does with
//! no journal at all ("the worst journal is main", docs/journal.md).

use lsr_journal::{FILE, Journal, MAGIC, SLOT, Seq, Slot, Why, encode};

use super::*;
use crate::journal::JournalWriter;
use crate::journal::{JournalFile, JournalSignal};

/// A journal file in memory: its bytes (`None` when absent), a read that fails, and writes
/// that fail. Nothing is lost between processes: these tests are about what the engine does
/// with the journal; the crash law is the model's and the simulator's.
#[derive(Clone, Debug, Default)]
struct MemJournal {
    bytes: Option<Vec<u8>>,
    open_fails: bool,
    slot_fails: Option<Slot>,
    writes_fail: bool,
    writes: usize,
}

impl JournalFile for MemJournal {
    fn open(&mut self) -> Result<(), ErrorKind> {
        match (&self.bytes, self.open_fails) {
            (None, _) => Err(ErrorKind::NotFound),
            (Some(_), true) => Err(ErrorKind::Other),
            (Some(_), false) => Ok(()),
        }
    }

    fn read_slot(&mut self, slot: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind> {
        if self.slot_fails == Some(slot) {
            return Err(ErrorKind::Other);
        }
        let from = usize::try_from(slot.offset()).unwrap();
        let bytes = self.bytes.as_ref().ok_or(ErrorKind::NotFound)?;
        Ok(bytes
            .get(from..from + SLOT)
            .map(|slot| slot.try_into().unwrap()))
    }

    fn create(&mut self) -> Result<(), ErrorKind> {
        if self.writes_fail {
            return Err(ErrorKind::StorageFull);
        }
        self.bytes.get_or_insert_with(Vec::new);
        Ok(())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ErrorKind> {
        if self.writes_fail {
            return Err(ErrorKind::StorageFull);
        }
        self.writes += 1;
        let file = self.bytes.as_mut().ok_or(ErrorKind::NotFound)?;
        let from = usize::try_from(offset).unwrap();
        if file.len() < from + bytes.len() {
            file.resize(from + bytes.len(), 0);
        }
        file[from..from + bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), ErrorKind> {
        Ok(())
    }

    fn sync_dir(&mut self) -> Result<(), ErrorKind> {
        Ok(())
    }
}

fn valid() -> MemJournal {
    let mut bytes = encode(&Journal::ABSENT, Seq::FIRST).to_vec();
    bytes.extend(encode(&Journal::ABSENT, Seq::new(2).unwrap()));
    MemJournal {
        bytes: Some(bytes),
        ..MemJournal::default()
    }
}

fn newer() -> MemJournal {
    let mut record = MAGIC.to_vec();
    record.extend(9_u16.to_le_bytes());
    record.extend(0_u16.to_le_bytes());
    record.extend(5_u64.to_le_bytes());
    let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(&record);
    record.extend(crc.to_le_bytes());
    record.resize(SLOT, 0);
    record.extend(vec![0; SLOT]);
    MemJournal {
        bytes: Some(record),
        ..MemJournal::default()
    }
}

/// Every journal a boot can find, with what it must report on a replica that holds files.
fn cases() -> Vec<(&'static str, Option<MemJournal>, Option<JournalSignal>)> {
    vec![
        ("no journal", None, None),
        (
            "missing",
            Some(MemJournal::default()),
            Some(JournalSignal::Missing),
        ),
        (
            "garbage",
            Some(MemJournal {
                bytes: Some(vec![0xee; FILE]),
                ..MemJournal::default()
            }),
            Some(JournalSignal::Unreadable(Why::Invalid)),
        ),
        (
            "open fails",
            Some(MemJournal {
                open_fails: true,
                ..valid()
            }),
            Some(JournalSignal::Unreadable(Why::Io)),
        ),
        (
            "newer",
            Some(newer()),
            Some(JournalSignal::Newer { version: 9 }),
        ),
        (
            "slot fails",
            Some(MemJournal {
                slot_fails: Some(Slot::S1),
                ..valid()
            }),
            Some(JournalSignal::SlotUnreadable),
        ),
        ("valid", Some(valid()), None),
    ]
}

fn tick(rig: &mut Rig, actor: &mut Actor, now: i64, journal: Option<&mut MemJournal>) -> Tick {
    let env = Env {
        wal: &mut rig.wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &rig.staging,
        remote: &rig.remote,
        clock: &FakeClock::new(ts(now)),
        journal: journal.map(|file| -> &mut (dyn JournalFile + Send) { file }),
    };
    block_on(actor.tick(env, ts(now)))
}

/// Ticks at `now` while the actor has work, lending it `journal` each tick.
fn settle(
    rig: &mut Rig,
    actor: &mut Actor,
    now: i64,
    mut journal: Option<&mut MemJournal>,
) -> Vec<Tick> {
    let mut seen = Vec::new();
    for _ in 0..100 {
        let tick = tick(rig, actor, now, journal.as_deref_mut());
        let more = matches!(
            tick,
            Tick::Ran {
                next: Next::Now,
                ..
            }
        );
        seen.push(tick);
        if !more {
            return seen;
        }
    }
    panic!("never settled: {seen:?}");
}

/// A first run, then a second process booting with `journal` after more commits: the ticks of
/// the second process, what it reported, and both stores afterwards.
fn run(
    journal: Option<MemJournal>,
) -> (
    Vec<Tick>,
    Option<JournalSignal>,
    Vec<FileInfo>,
    Vec<FileInfo>,
) {
    let mut rig = Rig::new();
    settle(&mut rig, &mut Actor::new(cfg()), T0, None);
    rig.commit(2);
    rig.commit(3);
    let mut actor = Actor::new(cfg());
    let mut journal = journal;
    let ticks = settle(&mut rig, &mut actor, T0 + 10, journal.as_mut());
    let staged = Rig::files(&rig.staging, Level::ZERO);
    let remote = Rig::files(&rig.remote, Level::ZERO);
    (ticks, actor.journal(), staged, remote)
}

#[test]
fn boot_reports_what_it_read_of_the_journal() {
    for (name, journal, want) in cases() {
        let (_, got, _, _) = run(journal);
        assert_eq!(got, want, "{name}");
    }
}

#[test]
fn a_missing_journal_on_a_first_run_reports_nothing() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(cfg());
    settle(&mut rig, &mut actor, T0, Some(&mut MemJournal::default()));
    assert_eq!(actor.journal(), None);
}

/// "A missing journal costs nothing beyond main's behaviour": every journal a boot can find,
/// the worst included, gives the same ticks and the same files as no journal at all.
#[test]
fn no_journal_a_boot_can_find_changes_what_the_engine_does() {
    let (ticks, _, staged, remote) = run(None);
    assert!(!remote.is_empty(), "the run uploaded nothing: {ticks:?}");
    for (name, journal, _) in cases() {
        let (got_ticks, _, got_staged, got_remote) = run(journal);
        assert_eq!(got_ticks, ticks, "{name}");
        assert_eq!(
            (got_staged, got_remote),
            (staged.clone(), remote.clone()),
            "{name}"
        );
    }
}

/// A restart after a run that owed nothing reads the journal that run left: the first process
/// creates it, so `JournalMissing` means the journal was lost, not that nothing went wrong.
#[test]
fn a_restart_after_a_run_with_no_debt_reports_nothing() {
    // No destination, so no floor: the run holds no fact at all, and still writes a record.
    let nothing = ActorConfig {
        destination: None,
        ..cfg()
    };
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    settle(
        &mut rig,
        &mut Actor::new(nothing.clone()),
        T0,
        Some(&mut journal),
    );
    rig.commit(2);
    let mut actor = Actor::new(nothing);
    settle(&mut rig, &mut actor, T0 + 10, Some(&mut journal));
    assert_eq!(actor.journal(), None);
    assert_eq!(recorded(&mut journal), Some(lsr_journal::Unproven::No));
}

/// A meta directory wiped while the replica keeps its files: staging is empty, the replica is
/// not, so a missing journal is not a first run and is reported.
#[test]
fn a_missing_journal_beside_a_replica_with_files_is_reported() {
    let mut rig = Rig::new();
    settle(&mut rig, &mut Actor::new(cfg()), T0, None);
    rig.staging = Counting::default();
    let mut actor = Actor::new(cfg());
    settle(
        &mut rig,
        &mut actor,
        T0 + 10,
        Some(&mut MemJournal::default()),
    );
    assert_eq!(actor.journal(), Some(JournalSignal::Missing));
}

/// Staging holds files and the replica is empty (a new or emptied bucket): not a first run, so
/// a missing journal is reported. Either store holding files is enough.
#[test]
fn a_missing_journal_beside_staged_files_and_an_empty_replica_is_reported() {
    let mut rig = Rig::new();
    settle(&mut rig, &mut Actor::new(cfg()), T0, None);
    rig.remote = Counting::default();
    let mut actor = Actor::new(cfg());
    settle(
        &mut rig,
        &mut actor,
        T0 + 10,
        Some(&mut MemJournal::default()),
    );
    assert_eq!(actor.journal(), Some(JournalSignal::Missing));
}

/// How a tick's `Env` reads in a log line, with a journal lent or not.
fn shown(rig: &mut Rig, lend: bool) -> String {
    let (staging, remote) = (MemStore::default(), MemStore::default());
    let clock = FakeClock::new(ts(T0));
    let mut file = MemJournal::default();
    let env = Env {
        wal: &mut rig.wal,
        db: &mut rig.db,
        ck: &mut rig.ck,
        staging: &staging,
        remote: &remote,
        clock: &clock,
        journal: lend
            .then_some(&mut file)
            .map(|file| -> &mut (dyn JournalFile + Send) { file }),
    };
    format!("{env:?}")
}

/// A tick's `Env` in a log line says whether a journal was lent.
#[test]
fn an_env_shows_whether_a_journal_is_lent() {
    let mut rig = Rig::new();
    assert!(shown(&mut rig, false).contains("journal: false"));
    assert!(shown(&mut rig, true).contains("journal: true"));
}

// ---- The unproven-WAL debt (`actor::unproven`), held in the journal across restarts. ----

/// What the journal records of the debt, read as a new process would.
fn recorded(journal: &mut MemJournal) -> Option<lsr_journal::Unproven> {
    match JournalWriter::open(journal).1.verdict {
        lsr_journal::Verdict::Read { journal, .. } => Some(journal.unproven),
        lsr_journal::Verdict::Newer { .. }
        | lsr_journal::Verdict::Missing
        | lsr_journal::Verdict::Unreadable(_) => None,
    }
}

fn truncates(rig: &Rig) -> usize {
    rig.ck
        .calls()
        .iter()
        .filter(|c| **c == CheckpointCall::Checkpoint(CheckpointMode::Truncate))
        .count()
}

/// A caught-up engine lent `journal`, whose next sync of the WAL fails with one more commit
/// waiting; the failing tick, and when its retry is due.
fn failed_sync(journal: &mut MemJournal) -> (Rig, Actor, i64) {
    failed_sync_with(journal, go_like())
}

/// [`failed_sync`] under `config`.
fn failed_sync_with(journal: &mut MemJournal, config: ActorConfig) -> (Rig, Actor, i64) {
    let mut rig = Rig::new();
    let mut actor = Actor::new(config);
    settle(&mut rig, &mut actor, T0, Some(journal));
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(2);
    let failed = tick(&mut rig, &mut actor, T0 + 1, Some(journal));
    let Some(Event::Degraded { until, .. }) = event(&failed) else {
        panic!("the sync should fail: {failed:?}");
    };
    (rig, actor, until.unix_millis())
}

/// The failing tick itself records the debt, before any later tick can read the WAL; a run
/// with no debt writes one record, that nothing is owed, and then nothing more.
#[test]
fn a_failed_wal_sync_is_recorded_in_the_tick_that_met_it() {
    // No destination, so no floor: the first run holds no fact, and still writes a record.
    let nothing = ActorConfig {
        destination: None,
        ..go_like()
    };
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    settle(
        &mut rig,
        &mut Actor::new(nothing.clone()),
        T0,
        Some(&mut journal),
    );
    assert_eq!(recorded(&mut journal), Some(lsr_journal::Unproven::No));
    let created = journal.writes;
    settle(
        &mut rig,
        &mut Actor::new(nothing.clone()),
        T0 + 10,
        Some(&mut journal),
    );
    assert_eq!(
        journal.writes, created,
        "a boot that read the record writes nothing"
    );
    let (_, _, _) = failed_sync_with(&mut journal, nothing);
    assert_eq!(
        recorded(&mut journal),
        Some(lsr_journal::Unproven::Truncate)
    );
}

/// A restart between the failed sync and the TRUNCATE still owes the TRUNCATE: the new process
/// checkpoints before it stages anything, and its next file is the snapshot.
#[test]
fn a_restart_after_a_failed_wal_sync_still_owes_the_truncate() {
    let mut journal = MemJournal::default();
    let (mut rig, _, until) = failed_sync(&mut journal);
    let mut restarted = Actor::new(go_like());
    let seen = settle(&mut rig, &mut restarted, until, Some(&mut journal));
    assert_eq!(truncates(&rig), 1, "{seen:?}");
    assert!(
        seen.iter()
            .any(|t| event(t) == Some(&Event::Resnapshot(SnapshotCause::WalSyncFailed))),
        "{seen:?}"
    );
    let resnapshot = seen
        .iter()
        .position(|t| event(t) == Some(&Event::Resnapshot(SnapshotCause::WalSyncFailed)));
    let staged = seen
        .iter()
        .position(|t| matches!(event(t), Some(Event::Synced { files: 1.. })));
    assert!(
        staged > resnapshot,
        "nothing is staged before the snapshot: {seen:?}"
    );
    assert_eq!(recorded(&mut journal), Some(lsr_journal::Unproven::No));
}

/// Without a journal the restart forgets the debt and trusts the WAL again: the defect the
/// journal closes, kept as the contrast.
#[test]
fn without_a_journal_a_restart_forgets_the_debt() {
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    settle(&mut rig, &mut actor, T0, None);
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.commit(2);
    tick(&mut rig, &mut actor, T0 + 1, None);
    let seen = settle(&mut rig, &mut Actor::new(go_like()), T0 + 10_000, None);
    assert_eq!(truncates(&rig), 0, "{seen:?}");
}

/// After the TRUNCATE the journal records that it ran, and the snapshot that follows clears the
/// debt: a restart in between owes no second TRUNCATE.
#[test]
fn a_recorded_truncate_already_run_is_not_run_again() {
    let mut journal = valid();
    let snapshot = Journal {
        unproven: lsr_journal::Unproven::Snapshot,
        ..Journal::ABSENT
    };
    let mut bytes = encode(&snapshot, Seq::new(3).unwrap()).to_vec();
    bytes.extend(encode(&Journal::ABSENT, Seq::new(2).unwrap()));
    journal.bytes = Some(bytes);
    let mut rig = Rig::new();
    let seen = settle(&mut rig, &mut Actor::new(go_like()), T0, Some(&mut journal));
    assert_eq!(truncates(&rig), 0, "{seen:?}");
    assert_eq!(recorded(&mut journal), Some(lsr_journal::Unproven::No));
}

/// A journal that cannot be written is reported while it lags the engine, and the engine pays
/// the debt from memory as it does with no journal. The report stands while the journal holds
/// nothing this process knows, and clears at the first write that succeeds.
#[test]
fn an_unwritable_journal_is_reported_and_the_debt_is_paid_from_memory() {
    let mut journal = MemJournal {
        writes_fail: true,
        ..MemJournal::default()
    };
    let (mut rig, mut actor, until) = failed_sync(&mut journal);
    assert_eq!(
        actor.journal(),
        Some(JournalSignal::Unwritable(ErrorKind::StorageFull))
    );
    let seen = settle(&mut rig, &mut actor, until, Some(&mut journal));
    assert_eq!(truncates(&rig), 1, "{seen:?}");
    assert_eq!(
        actor.journal(),
        Some(JournalSignal::Unwritable(ErrorKind::StorageFull)),
        "the journal still holds nothing"
    );
    journal.writes_fail = false;
    tick(&mut rig, &mut actor, until + 1, Some(&mut journal));
    assert_eq!(actor.journal(), None);
    assert_eq!(recorded(&mut journal), Some(lsr_journal::Unproven::No));
}

/// A journal read at boot that cannot be written lags only while the engine owes what it does
/// not hold: once the debt is paid from memory, the record read is again all there is to know,
/// and the report clears without a write.
#[test]
fn an_unwritable_journal_stops_lagging_once_the_debt_is_paid() {
    let mut journal = MemJournal {
        writes_fail: true,
        ..valid()
    };
    // No destination, so no floor: the debt is the one fact this test owes.
    let no_floor = ActorConfig {
        destination: None,
        ..go_like()
    };
    let (mut rig, mut actor, until) = failed_sync_with(&mut journal, no_floor);
    assert_eq!(
        actor.journal(),
        Some(JournalSignal::Unwritable(ErrorKind::StorageFull))
    );
    let seen = settle(&mut rig, &mut actor, until, Some(&mut journal));
    assert_eq!(truncates(&rig), 1, "{seen:?}");
    assert_eq!(actor.journal(), None, "nothing is owed, so nothing lags");
}

/// What a page holds in the commit after the failed sync.
fn page_after_failure() -> Vec<u8> {
    vec![9; PS.bytes()]
}

/// A caught-up engine with `journal`; its WAL sync fails on a commit that writes page 2;
/// `backfill` copies that page into the database file, as a checkpoint copies a frame the
/// failed sync covered; a power loss then cuts the WAL back to the frames before it. Returns
/// the rig and the frame count the WAL kept.
fn lost_after_backfill(journal: &mut MemJournal, config: &ActorConfig, backfill: bool) -> Rig {
    let mut rig = Rig::new();
    // Page 1 in the WAL differs from the file, which no checkpoint has written yet: a page the
    // WAL covers is not the file's to answer for.
    rig.commit(3);
    let mut actor = Actor::new(config.clone());
    settle(&mut rig, &mut actor, T0, Some(journal));
    let kept = rig.wal.committed_end().frame_count();
    rig.wal.inject(SourceOp::Sync, SourceError::FsyncFailed);
    rig.wal.append_tx(&[(pg(2), page_after_failure())], nz(3));
    let failed = tick(&mut rig, &mut actor, T0 + 1, Some(journal));
    assert!(
        matches!(event(&failed), Some(Event::Degraded { .. })),
        "{failed:?}"
    );
    assert_eq!(recorded(journal), Some(lsr_journal::Unproven::Truncate));
    if backfill {
        rig.db.write_page(pg(2), &page_after_failure());
    }
    rig.wal.lose_frames_from(kept);
    rig
}

/// The verdict the journal holds.
fn ahead_recorded(journal: &mut MemJournal) -> Option<lsr_journal::Ahead> {
    match JournalWriter::open(journal).1.verdict {
        lsr_journal::Verdict::Read { journal, .. } => journal.ahead,
        lsr_journal::Verdict::Newer { .. }
        | lsr_journal::Verdict::Missing
        | lsr_journal::Verdict::Unreadable(_) => None,
    }
}

/// Simulator seed 0x725's shape: a failed WAL sync, a checkpoint that copies the page the failed
/// sync covered, a power loss that cuts the WAL short of it. The restart finds the database file
/// ahead of its WAL before it runs the TRUNCATE, records the verdict, degrades with its own cause,
/// and then cures the debt as before, keeping the state before the damage restorable.
#[test]
fn a_boot_with_a_debt_reports_pages_from_commits_the_wal_lost() {
    let mut journal = MemJournal::default();
    let mut rig = lost_after_backfill(&mut journal, &go_like(), true);
    let mut restarted = Actor::new(go_like());
    let first = tick(&mut rig, &mut restarted, T0 + 10_000, Some(&mut journal));
    assert!(
        matches!(event(&first), Some(Event::Booted { .. })),
        "{first:?}"
    );
    let checked = tick(&mut rig, &mut restarted, T0 + 10_000, Some(&mut journal));
    let ahead = StopCause::DatabaseAheadOfWal {
        pages: nz(1),
        first: nz(2),
    };
    assert!(
        matches!(event(&checked), Some(Event::Degraded { cause, .. })
            if PersistentCause::stop(*cause) == Some(ahead)),
        "{checked:?}"
    );
    assert_eq!(truncates(&rig), 0, "the check comes before the TRUNCATE");
    let verdict = ahead_recorded(&mut journal).expect("the verdict is journalled");
    assert_eq!((verdict.pages, verdict.first), (nz(1), nz(2)));
    assert_eq!(restarted.ahead(), Some(verdict));
    // An owed TRUNCATE waits for the WAL to move, as it does with no verdict.
    rig.commit(5);
    let seen = settle(&mut rig, &mut restarted, T0 + 20_000, Some(&mut journal));
    assert_eq!(truncates(&rig), 1, "{seen:?}");
    assert_eq!(restarted.degraded(Task::Sync), None, "{seen:?}");
    assert_eq!(
        restarted.ahead(),
        Some(verdict),
        "the notice outlasts the degrade"
    );
    assert_eq!(
        restarted.kept.kept_restore_point(),
        Some(verdict.restore_point)
    );
}

/// The same restart without the backfill: every page the WAL no longer covers is its image at
/// the newest staged transaction, so the boot cures the debt and reports nothing.
#[test]
fn a_boot_with_a_debt_whose_file_kept_its_pages_comes_up_clean() {
    let mut journal = MemJournal::default();
    let mut rig = lost_after_backfill(&mut journal, &go_like(), false);
    let mut restarted = Actor::new(go_like());
    rig.commit(5);
    let seen = settle(&mut rig, &mut restarted, T0 + 10_000, Some(&mut journal));
    assert!(
        !seen
            .iter()
            .any(|t| matches!(event(t), Some(Event::Degraded { .. }))),
        "{seen:?}"
    );
    assert_eq!(truncates(&rig), 1, "{seen:?}");
    assert_eq!(
        (restarted.ahead(), ahead_recorded(&mut journal)),
        (None, None)
    );
}

/// Under `STRICT` the verdict stops the engine once; the next boot finds it recorded, does not
/// raise it again, and cures the debt with the notice standing.
#[test]
fn a_recorded_verdict_stops_once_under_strict_responses() {
    let strict = ActorConfig {
        responses: Responses::STRICT,
        ..go_like()
    };
    let mut journal = MemJournal::default();
    let mut rig = lost_after_backfill(&mut journal, &go_like(), true);
    let mut stopped = Actor::new(strict.clone());
    let seen = settle(&mut rig, &mut stopped, T0 + 10_000, Some(&mut journal));
    assert!(
        matches!(
            seen.last(),
            Some(Tick::Stopped(StopCause::DatabaseAheadOfWal { .. }))
        ),
        "{seen:?}"
    );
    let verdict = ahead_recorded(&mut journal).expect("journalled before the stop");
    let mut again = Actor::new(strict);
    rig.commit(5);
    let seen = settle(&mut rig, &mut again, T0 + 20_000, Some(&mut journal));
    assert!(
        !seen.iter().any(|t| matches!(t, Tick::Stopped(_))),
        "{seen:?}"
    );
    assert_eq!(truncates(&rig), 1, "{seen:?}");
    assert_eq!(again.ahead(), Some(verdict));
}

/// The restore point is kept for the window from the verdict, across a restart, and then goes
/// back to normal retention with the notice gone and the journal's fact cleared.
#[test]
fn the_restore_point_is_kept_for_the_window_across_a_restart() {
    let mut journal = MemJournal::default();
    let mut rig = lost_after_backfill(&mut journal, &go_like(), true);
    let mut restarted = Actor::new(go_like());
    settle(&mut rig, &mut restarted, T0 + 10_000, Some(&mut journal));
    let verdict = restarted.ahead().expect("a verdict");
    // Seven days, stated here rather than read from the code under test.
    let until = verdict.made.unix_millis() + 7 * 24 * 60 * 60 * 1000;
    let mut later = Actor::new(go_like());
    let seen = settle(&mut rig, &mut later, until - 1, Some(&mut journal));
    let reminded = Event::AheadOfWal(AheadNotice::Kept {
        restore_point: verdict.restore_point,
        until: ts(until),
    });
    assert!(
        seen.iter().any(|t| event(t) == Some(&reminded)),
        "a boot inside the window says how long the restore point is kept: {seen:?}"
    );
    assert_eq!(
        later.ahead(),
        Some(verdict),
        "the window survives the restart"
    );
    assert_eq!(later.kept.kept_restore_point(), Some(verdict.restore_point));
    let ended = tick(&mut rig, &mut later, until, Some(&mut journal));
    assert_eq!(
        event(&ended),
        Some(&Event::AheadOfWal(AheadNotice::Released {
            restore_point: verdict.restore_point
        }))
    );
    assert_eq!(later.ahead(), None);
    assert_eq!(later.kept.kept_restore_point(), None);
    assert_eq!(ahead_recorded(&mut journal), None);
}

/// The floor the journal holds.
fn floor_recorded(journal: &mut MemJournal) -> Option<lsr_journal::Floor> {
    match JournalWriter::open(journal).1.verdict {
        lsr_journal::Verdict::Read { journal, .. } => journal.floor,
        lsr_journal::Verdict::Newer { .. }
        | lsr_journal::Verdict::Missing
        | lsr_journal::Verdict::Unreadable(_) => None,
    }
}

/// The replica's level-0 file ending at `txid`: its length and the FNV-1a 64 digest of its
/// bytes, computed here rather than by the code under test.
fn replica_bytes_at(rig: &Rig, txid: Txid) -> (u64, u64) {
    let inventory = block_on(Inventory::load(&rig.remote.inner)).unwrap();
    let file = inventory
        .level(Level::ZERO)
        .iter()
        .find(|file| file.key.range().max() == txid)
        .copied()
        .expect("the replica holds a level-0 file ending at the floor");
    let bytes = block_on(rig.remote.inner.read(&file.key, 0, ReadLen::CHUNK)).unwrap();
    let digest = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
    });
    (u64::try_from(bytes.len()).unwrap(), digest)
}

/// Deletes every replica file ending at the newest transaction, as a bucket put back to an
/// older version does; returns the newest it removed.
fn roll_back_replica(rig: &Rig) -> Txid {
    let all = block_on(Inventory::load(&rig.remote.inner)).unwrap();
    let newest = all.newest().unwrap();
    let gone: Vec<FileKey> = all
        .files()
        .iter()
        .map(|info| info.key)
        .filter(|key| key.range().max() == newest)
        .collect();
    block_on(rig.remote.inner.delete(&gone)).unwrap();
    newest
}

fn destination() -> lsr_journal::ReplicaId {
    lsr_journal::ReplicaId::of(b"test")
}

/// Each confirmed upload raises the floor to its transaction, anchored on the bytes it put, and
/// the end of the tick journals it (issue #171, item 4).
#[test]
fn a_confirmed_upload_raises_the_floor_anchored_on_the_bytes_it_put() {
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    let floor = actor.floor().expect("a confirmed upload sets the floor");
    let newest = block_on(Inventory::load(&rig.remote.inner))
        .unwrap()
        .newest()
        .unwrap();
    assert_eq!((floor.replica(), floor.bound()), (destination(), newest));
    let anchor = floor
        .anchor()
        .expect("this process put the file, so it knows its bytes");
    assert_eq!((anchor.len, anchor.digest), replica_bytes_at(&rig, newest));
    assert_eq!(floor_recorded(&mut journal), Some(floor));
}

/// A restart reads the floor back for its destination, and a replica rolled back before the
/// boot lowers neither the floor held nor the one recorded: the ids stay taken.
#[test]
fn the_floor_is_read_back_and_a_rollback_before_the_boot_does_not_lower_it() {
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    let floor = actor.floor().expect("a floor");
    let lost = roll_back_replica(&rig);
    assert_eq!(lost, floor.bound());
    let mut restarted = Actor::new(go_like());
    tick(&mut rig, &mut restarted, T0 + 10, Some(&mut journal));
    assert_eq!(restarted.floor(), Some(floor), "read back, not lowered");
    assert_eq!(floor_recorded(&mut journal), Some(floor));
}

/// A floor recorded for another destination is not this engine's: it is not adopted, and it
/// stays in the record until this engine has a floor of its own, since none is ever dropped.
#[test]
fn another_destination_s_floor_is_kept_in_the_record_but_not_adopted() {
    let theirs = lsr_journal::Floor::new(
        lsr_journal::ReplicaId::of(b"elsewhere"),
        Txid::new(99).unwrap(),
        None,
    );
    let mut bytes = encode(
        &Journal {
            floor: Some(theirs),
            ..Journal::ABSENT
        },
        Seq::FIRST,
    )
    .to_vec();
    bytes.extend([0; SLOT]);
    let mut journal = MemJournal {
        bytes: Some(bytes),
        ..MemJournal::default()
    };
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    tick(&mut rig, &mut actor, T0, Some(&mut journal));
    assert_eq!(
        actor.floor(),
        None,
        "another destination's floor is not adopted"
    );
    assert_eq!(floor_recorded(&mut journal), Some(theirs));
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    let ours = actor.floor().expect("this destination's own floor");
    assert_eq!(ours.replica(), destination());
    assert_eq!(floor_recorded(&mut journal), Some(ours));
}

/// With no destination named the engine keeps no floor and leaves a recorded one as it is.
#[test]
fn no_destination_keeps_no_floor_and_leaves_the_record_s() {
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    let recorded = floor_recorded(&mut journal).expect("a floor");
    let no_floor = ActorConfig {
        destination: None,
        ..go_like()
    };
    let mut restarted = Actor::new(no_floor);
    rig.commit(3);
    settle(&mut rig, &mut restarted, T0 + 10, Some(&mut journal));
    assert_eq!(restarted.floor(), None);
    assert_eq!(
        floor_recorded(&mut journal),
        Some(recorded),
        "passed through untouched"
    );
}

/// A rollback the running engine sees withdraws the floor's anchor, since the replica no longer
/// holds what it names, and keeps its bound: the ids stay taken.
#[test]
fn a_rollback_seen_while_running_withdraws_the_anchor_and_keeps_the_bound() {
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    rig.commit(3);
    settle(&mut rig, &mut actor, T0 + 10, Some(&mut journal));
    let lost = roll_back_replica(&rig);
    rig.commit(4);
    let mut now = T0 + 20;
    let mut regressed = false;
    for _ in 0..200 {
        let seen = tick(&mut rig, &mut actor, now, Some(&mut journal));
        regressed |= matches!(
            event(&seen),
            Some(Event::Degraded {
                cause: PersistentCause::RemoteRegressed,
                ..
            })
        );
        if regressed {
            break;
        }
        now += 1_000;
    }
    assert!(regressed, "the rollback is seen");
    let floor = actor.floor().expect("a floor");
    assert!(floor.bound() >= lost, "{floor:?} fell below {lost}");
    assert_eq!(floor.anchor(), None);
    assert_eq!(floor_recorded(&mut journal), Some(floor));
}

/// A floor the journal cannot record is owed: an unwritable journal keeps reporting while the
/// floor is held in this process only, and the report clears once a write takes it.
#[test]
fn an_unwritable_journal_reports_a_floor_it_does_not_hold() {
    let mut journal = MemJournal {
        writes_fail: true,
        ..valid()
    };
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    assert!(actor.floor().is_some());
    assert_eq!(
        actor.journal(),
        Some(JournalSignal::Unwritable(ErrorKind::StorageFull))
    );
    journal.writes_fail = false;
    tick(&mut rig, &mut actor, T0 + 1, Some(&mut journal));
    assert_eq!(actor.journal(), None);
    assert_eq!(floor_recorded(&mut journal), actor.floor());
}

/// A rebuilt engine boots beside the floor it held, which a journal that cannot be written lags:
/// the boot keeps the floor it held, never the lower one recorded.
#[test]
fn a_rebuild_keeps_the_floor_a_lagging_journal_does_not_hold() {
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
    rig.commit(2);
    settle(&mut rig, &mut actor, T0, Some(&mut journal));
    let recorded = floor_recorded(&mut journal).expect("a floor");
    journal.writes_fail = true;
    rig.commit(3);
    settle(&mut rig, &mut actor, T0 + 10, Some(&mut journal));
    let held = actor.floor().expect("a floor");
    assert!(held.bound() > recorded.bound(), "{held:?} {recorded:?}");
    assert_eq!(
        floor_recorded(&mut journal),
        Some(recorded),
        "the journal lags"
    );
    actor.fault(
        Task::Upload,
        Fault::Bug(crate::fault::BugSite::PageMap),
        ts(T0 + 20),
    );
    let mut now = T0 + 20;
    let mut booted = false;
    for _ in 0..50 {
        let seen = tick(&mut rig, &mut actor, now, Some(&mut journal));
        booted |= matches!(event(&seen), Some(Event::Booted { .. }));
        if booted {
            break;
        }
        now += 1_000;
    }
    assert!(booted, "the rebuilt engine boots");
    assert_eq!(actor.floor(), Some(held), "the held floor stands");
}
