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
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    settle(&mut rig, &mut Actor::new(cfg()), T0, Some(&mut journal));
    rig.commit(2);
    let mut actor = Actor::new(cfg());
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
    let mut rig = Rig::new();
    let mut actor = Actor::new(go_like());
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
    let mut journal = MemJournal::default();
    let mut rig = Rig::new();
    settle(&mut rig, &mut Actor::new(go_like()), T0, Some(&mut journal));
    assert_eq!(recorded(&mut journal), Some(lsr_journal::Unproven::No));
    let created = journal.writes;
    settle(&mut rig, &mut Actor::new(go_like()), T0 + 10, Some(&mut journal));
    assert_eq!(journal.writes, created, "a boot that read the record writes nothing");
    let (_, _, _) = failed_sync(&mut journal);
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
