//! Boot reads the journal and reports it; whatever it finds, the engine does what it does with
//! no journal at all ("the worst journal is main", docs/journal.md).

use lsr_journal::{FILE, Journal, MAGIC, SLOT, Seq, Slot, Why, encode};

use super::*;
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

/// A brand-new meta directory is one with nothing staged and no takeover record: a journal file
/// beside it, whatever it holds, changes nothing, so the equality check runs and the boot
/// snapshots (`docs/decisions/D170.md`, `docs/decisions/D176.md`).
#[test]
fn a_journal_file_does_not_make_a_new_meta_directory_old() {
    for journal in [valid(), MemJournal::default()] {
        let mut journal = journal;
        let mut rig = super::vouch::redeployed_with_a_new_meta_directory();
        let mut actor = Actor::new(go_like());
        let seen = settle(&mut rig, &mut actor, T0 + 2, Some(&mut journal));
        no_stop(&seen);
        let booted = seen.iter().any(|tick| {
            matches!(
                event(tick),
                Some(Event::Booted {
                    cause: Some(SnapshotCause::GenerationChanged)
                })
            )
        });
        assert!(booted, "{seen:?}");
    }
}
