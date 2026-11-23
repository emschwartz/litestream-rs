//! The journal's writer against its exhaustive model (`lsr_spec::journal`, docs/journal.md).
//!
//! The walk visits every state of the model within a budget. Beside each state it keeps the
//! real [`JournalWriter`] over a byte-level file that applies, in bytes, exactly the outcome the
//! model drew for each system call: a write, a failed fsync that left each block old, new or
//! torn, a power loss, an eviction. Each write the model makes is replayed on the writer from the
//! state before it, with the model's outcomes scripted in order; the writer must make the same
//! calls in the same order, and afterwards the file, read as records, and the writer's position
//! must equal the model's. Each boot must reach the model's verdict. The model and the writer
//! share no code: the model knows records only by their place in its log, the writer only by
//! their bytes.

use std::collections::{HashSet, VecDeque};
use std::io::ErrorKind;

use lsr_core::{JournalFile, JournalWriter};
use lsr_journal::{
    Decoded, FILE, Floor, Journal, MAGIC, Proof, ReplicaId, SLOT, Slot, StoredBudget, Triple,
    Unproven, Verdict, WalChecksum, Why, decode,
};
use lsr_ltx::{FileChecksum, Timestamp, Txid};
use lsr_spec::journal::fs::{self as model_fs, Block, Content, Device, Len, Reach};
use lsr_spec::journal::world::{
    self as model, Budget, Call, Config, Label, Outcome, ReadFault, State,
};

// ---- Facts: the model's small values as the writer's records. ----

fn journal_of(facts: &model::Facts) -> Journal {
    let checksum = FileChecksum::from_raw(1 << 63 | 7).unwrap();
    Journal {
        floor: facts.floor.map(|floor| {
            let bound = Txid::new(u64::from(floor.bound) + 1).unwrap();
            Floor::new(ReplicaId([floor.replica; 8]), bound, None)
        }),
        unproven: match facts.unproven {
            model::Unproven::No => Unproven::No,
            model::Unproven::Truncate => Unproven::Truncate,
            model::Unproven::Snapshot => Unproven::Snapshot,
        },
        budget: Some(StoredBudget {
            tokens: facts.tokens,
            as_of: Timestamp::from_unix_millis(0),
        }),
        pending: facts.pending.then_some(Proof {
            txid: Txid::MIN,
            triple: Triple {
                len: 1,
                digest: 2,
                checksum,
            },
            wal: WalChecksum(3, 4),
        }),
        // The model's facts carry no verdict: its storage properties do not depend on what a
        // record holds, and the verdict's one rule is checked by the codec's own tests.
        ahead: None,
        unchecked: None,
    }
}

fn slot_of(slot: model_fs::Slot) -> Slot {
    match slot {
        model_fs::Slot::S0 => Slot::S0,
        model_fs::Slot::S1 => Slot::S1,
    }
}

fn index(slot: Slot) -> usize {
    match slot {
        Slot::S0 => 0,
        Slot::S1 => 1,
    }
}

// ---- The byte-level file. ----

#[derive(Clone, PartialEq, Eq, Debug)]
struct Page {
    cache: Option<[u8; SLOT]>,
    disk: [u8; SLOT],
    dirty: bool,
    bad: bool,
}

const EMPTY: Page = Page {
    cache: None,
    disk: [0; SLOT],
    dirty: false,
    bad: false,
};

/// What a torn sector reads as, were it readable: its checksum fails.
const TORN: [u8; SLOT] = [0xa5; SLOT];

#[derive(Clone, Debug)]
struct Disk {
    entry_cache: bool,
    entry_disk: bool,
    len_cache: usize,
    len_disk: usize,
    pages: [Page; 2],
    block: Block,
    full: bool,
}

fn class(len: usize) -> Len {
    match len {
        0..4096 => Len::Short0,
        4096..8192 => Len::Short1,
        _ => Len::Full,
    }
}

impl Disk {
    fn new(block: Block) -> Self {
        Self {
            entry_cache: false,
            entry_disk: false,
            len_cache: 0,
            len_disk: 0,
            pages: [EMPTY, EMPTY],
            block,
            full: false,
        }
    }

    fn current(&self, slot: Slot) -> [u8; SLOT] {
        let page = &self.pages[index(slot)];
        page.cache.unwrap_or(page.disk)
    }

    fn covers(&self, slot: Slot) -> bool {
        self.len_cache >= usize::try_from(slot.offset()).unwrap() + SLOT
    }

    fn allocates(&self, slot: Slot) -> bool {
        !self.covers(slot)
    }

    fn write(&mut self, slot: Slot, bytes: [u8; SLOT]) {
        let page = &mut self.pages[index(slot)];
        page.cache = Some(bytes);
        page.dirty = true;
        if self.block == Block::Large {
            self.pages[index(slot.other())].dirty = true;
        }
        let end = usize::try_from(slot.offset()).unwrap() + SLOT;
        self.len_cache = self.len_cache.max(end);
        self.normalize();
    }

    fn normalize(&mut self) {
        for page in &mut self.pages {
            if !page.dirty && !page.bad && page.cache == Some(page.disk) {
                page.cache = None;
            }
        }
    }

    fn synced(&mut self) {
        for page in &mut self.pages {
            if page.dirty {
                if let Some(bytes) = page.cache.take() {
                    page.disk = bytes;
                    page.bad = false;
                }
                page.dirty = false;
            }
        }
        self.len_disk = self.len_cache;
    }

    /// Each dirty page lands as `reaches` says; every page is then clean.
    fn land(&mut self, reaches: &model_fs::Reaches, keep_cache: bool) {
        for slot in [Slot::S0, Slot::S1] {
            let current = self.current(slot);
            let reach = *reaches.get(match slot {
                Slot::S0 => model_fs::Slot::S0,
                Slot::S1 => model_fs::Slot::S1,
            });
            let page = &mut self.pages[index(slot)];
            let written = page.cache.is_some();
            match reach {
                None | Some(Reach::Old) => {}
                Some(Reach::New) => {
                    page.disk = current;
                    page.bad &= !written;
                }
                Some(Reach::Torn) => {
                    page.disk = TORN;
                    page.bad = true;
                }
            }
            page.dirty = false;
            page.cache = (keep_cache && (current != page.disk || page.bad)).then_some(current);
        }
        self.normalize();
    }

    fn length_for(&self, target: Len) -> usize {
        if class(self.len_cache) == target {
            self.len_cache
        } else if class(self.len_disk) == target {
            self.len_disk
        } else {
            match target {
                Len::Short0 => 0,
                Len::Short1 => 4096,
                Len::Full => FILE,
            }
        }
    }

    fn power(&mut self, reaches: &model_fs::Reaches, entry: bool, len: Len) {
        if !entry {
            *self = Self::new(self.block);
            return;
        }
        self.land(reaches, false);
        let len = self.length_for(len);
        self.len_cache = len;
        self.len_disk = len;
        self.entry_disk = true;
        for slot in [Slot::S0, Slot::S1] {
            if !self.covers(slot) {
                self.pages[index(slot)] = EMPTY;
            }
        }
    }

    fn evict(&mut self) {
        for page in &mut self.pages {
            if !page.dirty {
                page.cache = None;
            }
        }
    }

    /// A newer binary's record over the slot that does not hold the winner, synced.
    fn newer_write(&mut self) {
        let seq = |slot: Slot| match decode(&self.current(slot)) {
            Ok(Decoded::Ours { seq, .. } | Decoded::Newer { seq, .. }) => Some(seq.get()),
            Err(_) => None,
        };
        let (s0, s1) = (seq(Slot::S0), seq(Slot::S1));
        let (winner, seq) = if s1 > s0 {
            (Slot::S1, s1)
        } else {
            (Slot::S0, s0)
        };
        let mut bytes = MAGIC.to_vec();
        bytes.extend((lsr_journal::VERSION + 1).to_le_bytes());
        bytes.extend(0_u16.to_le_bytes());
        bytes.extend((seq.unwrap() + 1).to_le_bytes());
        let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(&bytes);
        bytes.extend(crc.to_le_bytes());
        let mut slot = [0; SLOT];
        slot[..bytes.len()].copy_from_slice(&bytes);
        self.write(winner.other(), slot);
        self.synced();
    }
}

// ---- The scripted file the writer runs on. ----

#[derive(Clone, Debug)]
enum Expect {
    Sys(Call, Outcome),
    Env(Label),
    Crash,
}

#[derive(Clone, Debug)]
struct Fake {
    disk: Disk,
    script: VecDeque<Expect>,
    read_fault: Option<ReadFault>,
}

fn apply_env(disk: &mut Disk, label: &Label) {
    match label {
        Label::Evict => disk.evict(),
        Label::Latent(slot) => disk.pages[index(slot_of(*slot))].bad = true,
        Label::Fill => disk.full = true,
        Label::NewerWrite => disk.newer_write(),
        other => panic!("not an environment event: {other:?}"),
    }
}

impl Fake {
    /// The next scripted outcome for `call`, after any environment events queued before it.
    fn next(&mut self, call: Call) -> Option<Outcome> {
        loop {
            match self.script.pop_front() {
                Some(Expect::Env(label)) => apply_env(&mut self.disk, &label),
                Some(Expect::Crash) => return None,
                Some(Expect::Sys(want, outcome)) => {
                    assert_eq!(call, want, "the writer made another call than the model");
                    return Some(outcome);
                }
                None => panic!("the writer made {call:?}, which the model did not"),
            }
        }
    }

    fn fail(outcome: Outcome) -> Result<(), ErrorKind> {
        match outcome {
            Outcome::Ok => Ok(()),
            Outcome::Enospc => Err(ErrorKind::StorageFull),
            Outcome::Failed(_) | Outcome::SyncFailed(..) | Outcome::DirFailed(_) => {
                Err(ErrorKind::Other)
            }
        }
    }
}

impl JournalFile for Fake {
    fn open(&mut self) -> Result<(), ErrorKind> {
        if !self.disk.entry_cache {
            return Err(ErrorKind::NotFound);
        }
        match self.read_fault {
            Some(ReadFault::Open) => Err(ErrorKind::Other),
            Some(ReadFault::Slot(_)) | None => Ok(()),
        }
    }

    fn read_slot(&mut self, slot: Slot) -> Result<Option<[u8; SLOT]>, ErrorKind> {
        if self.read_fault
            == Some(ReadFault::Slot(match slot {
                Slot::S0 => model_fs::Slot::S0,
                Slot::S1 => model_fs::Slot::S1,
            }))
        {
            return Err(ErrorKind::Other);
        }
        if !self.disk.covers(slot) {
            return Ok(None);
        }
        let page = &self.disk.pages[index(slot)];
        match page.cache {
            Some(bytes) => Ok(Some(bytes)),
            None if page.bad => Err(ErrorKind::Other),
            None => Ok(Some(page.disk)),
        }
    }

    fn create(&mut self) -> Result<(), ErrorKind> {
        let outcome = self.next(Call::Create).ok_or(ErrorKind::Other)?;
        if outcome == Outcome::Ok && !self.disk.entry_cache {
            self.disk = Disk::new(self.disk.block);
            self.disk.full = false;
            self.disk.entry_cache = true;
        }
        Self::fail(outcome)
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), ErrorKind> {
        let whole = bytes.len() == FILE;
        let call = match (whole, offset) {
            (true, _) if bytes.iter().all(|b| *b == 0) => Call::WriteZeros,
            (true, _) => Call::WriteWhole,
            (false, 0) => Call::WriteSlot(model_fs::Slot::S0),
            (false, _) => Call::WriteSlot(model_fs::Slot::S1),
        };
        let outcome = self.next(call).ok_or(ErrorKind::Other)?;
        let first: [u8; SLOT] = bytes[..SLOT].try_into().unwrap();
        let slot = if offset == 0 { Slot::S0 } else { Slot::S1 };
        match (outcome, whole) {
            (Outcome::Ok, true) => {
                self.disk.write(Slot::S0, first);
                self.disk.write(Slot::S1, bytes[SLOT..].try_into().unwrap());
            }
            (Outcome::Ok, false) => self.disk.write(slot, first),
            (Outcome::Failed(model_fs::Partial::FirstSlot), true) => {
                self.disk.write(Slot::S0, first)
            }
            (Outcome::Enospc, true) if !self.disk.allocates(Slot::S0) => {
                self.disk.write(Slot::S0, first);
            }
            _ => {}
        }
        Self::fail(outcome)
    }

    fn sync(&mut self) -> Result<(), ErrorKind> {
        let outcome = self.next(Call::Fsync).ok_or(ErrorKind::Other)?;
        match outcome {
            Outcome::Ok => self.disk.synced(),
            Outcome::SyncFailed(reaches, len) => {
                self.disk.land(&reaches, true);
                self.disk.len_disk = self.disk.length_for(len);
            }
            other => panic!("an fsync cannot end {other:?}"),
        }
        Self::fail(outcome)
    }

    fn sync_dir(&mut self) -> Result<(), ErrorKind> {
        let outcome = self.next(Call::DirFsync).ok_or(ErrorKind::Other)?;
        match outcome {
            Outcome::Ok | Outcome::DirFailed(true) => self.disk.entry_disk = self.disk.entry_cache,
            _ => {}
        }
        Self::fail(outcome)
    }
}

// ---- Comparing the two. ----

/// Whether `bytes` hold what the model says the slot holds.
fn same_content(state: &State, content: Content, bytes: &[u8; SLOT]) -> bool {
    match (content, decode(bytes)) {
        (Content::Zero, _) => bytes.iter().all(|b| *b == 0),
        (Content::Garbage, Err(_)) => !bytes.iter().all(|b| *b == 0),
        (Content::Garbage, Ok(_)) => false,
        (Content::Rec(rid), decoded) => {
            let rec = state.rec(rid).unwrap();
            match decoded {
                Ok(Decoded::Ours { seq, journal }) => {
                    !rec.newer
                        && seq.get() == u64::from(rec.seq)
                        && journal == journal_of(&rec.facts)
                }
                Ok(Decoded::Newer { seq, .. }) => rec.newer && seq.get() == u64::from(rec.seq),
                Err(_) => false,
            }
        }
    }
}

fn compare_disk(state: &State, disk: &Disk, at: &[Label]) {
    let fs = state.fs();
    let ctx = || format!("{at:#?}\n{disk:#?}\n{state:#?}");
    assert_eq!(fs.exists(), disk.entry_cache, "{}", ctx());
    if !fs.exists() {
        return;
    }
    assert_eq!(fs.entry_durable(), disk.entry_disk, "{}", ctx());
    assert_eq!(fs.len(), class(disk.len_cache), "{}", ctx());
    assert_eq!(fs.len_durable(), class(disk.len_disk), "{}", ctx());
    for slot in model_fs::Slot::BOTH {
        let sector = fs.sector(slot);
        let page = &disk.pages[index(slot_of(slot))];
        if !fs.len().covers(slot) {
            continue;
        }
        assert_eq!(sector.dirty(), page.dirty, "{slot:?} dirty\n{}", ctx());
        assert_eq!(sector.bad(), page.bad, "{slot:?} bad\n{}", ctx());
        assert!(
            same_content(state, sector.disk(), &page.disk),
            "{slot:?} disk\n{}",
            ctx()
        );
        match (sector.cache(), page.cache) {
            (None, None) => {}
            (Some(content), Some(bytes)) => {
                assert!(
                    same_content(state, content, &bytes),
                    "{slot:?} cache\n{}",
                    ctx()
                );
            }
            (model, real) => panic!(
                "{slot:?} cache {model:?} against {}\n{}",
                real.is_some(),
                ctx()
            ),
        }
    }
}

fn compare_writer(state: &State, writer: &JournalWriter, at: &[Label]) {
    let view = state.writer().expect("a running process");
    let (establish, winner, created, synced) = writer.position();
    assert_eq!(view.establish, establish, "{at:#?}");
    if !establish {
        let want = (slot_of(view.winner), u64::from(view.seq));
        assert_eq!(winner, Some(want), "{at:#?}");
        assert_eq!((view.created, view.synced), (created, synced), "{at:#?}");
    }
}

fn same_verdict(state: &State, model: model::Verdict, real: Verdict) -> bool {
    let rec = |rid| state.rec(rid).copied().unwrap();
    match (model, real) {
        (model::Verdict::Missing, Verdict::Missing) => true,
        (model::Verdict::Unreadable(want), Verdict::Unreadable(got)) => matches!(
            (want, got),
            (model::Why::Io, Why::Io)
                | (model::Why::Invalid, Why::Invalid)
                | (model::Why::Tie, Why::Tie)
        ),
        (model::Verdict::Read(rid), Verdict::Read { seq, journal, .. }) => {
            let rec = rec(rid);
            !rec.newer && seq.get() == u64::from(rec.seq) && journal == journal_of(&rec.facts)
        }
        (model::Verdict::Newer(rid), Verdict::Newer { seq, .. }) => {
            let rec = rec(rid);
            rec.newer && seq.get() == u64::from(rec.seq)
        }
        _ => false,
    }
}

// ---- The walk. ----

/// A running writer and the file lent to it.
type Lent = (JournalWriter, Fake);

#[derive(Clone, Debug)]
struct Inflight {
    start: Lent,
    facts: Journal,
    steps: Vec<Expect>,
}

#[derive(Clone, Debug)]
enum Real {
    Between(Fake),
    Running(Lent, Option<Inflight>),
}

/// Replays an in-flight write from its start, with `steps` and then `end` scripted.
fn replay(flight: &Inflight, end: Option<Expect>) -> (Lent, bool) {
    let (mut writer, mut fake) = flight.start.clone();
    fake.script = flight.steps.iter().cloned().chain(end).collect();
    let ok = writer.write(&mut fake, &flight.facts).is_ok();
    let left = &fake.script;
    assert!(
        left.is_empty(),
        "the model made calls the writer did not: {left:?}"
    );
    ((writer, fake), ok)
}

fn disk_of(real: &mut Real) -> &mut Disk {
    match real {
        Real::Between(fake) => &mut fake.disk,
        Real::Running((_, fake), _) => &mut fake.disk,
    }
}

/// The real side after the model took `label` from `before` to `after`.
fn step(real: Real, label: &Label, before: &State, after: &State) -> Real {
    match (real, label) {
        (Real::Between(mut fake), Label::Boot(fault, verdict)) => {
            fake.read_fault = *fault;
            let (writer, parsed) = JournalWriter::open(&mut fake);
            fake.read_fault = None;
            assert!(
                same_verdict(before, *verdict, parsed.verdict),
                "{verdict:?} against {:?}",
                parsed.verdict
            );
            Real::Running((writer, fake), None)
        }
        (Real::Running(lent, None), Label::Engine(_)) => {
            let rec = after
                .writer()
                .and_then(|view| view.writing)
                .expect("a write in flight");
            let flight = Inflight {
                start: lent.clone(),
                facts: journal_of(&rec.facts),
                steps: Vec::new(),
            };
            Real::Running(lent, Some(flight))
        }
        (Real::Running(lent, Some(mut flight)), Label::Sys(call, outcome)) => {
            flight.steps.push(Expect::Sys(*call, *outcome));
            if after.writer().and_then(|view| view.writing).is_some() {
                return Real::Running(lent, Some(flight));
            }
            let (lent, ok) = replay(&flight, None);
            assert_eq!(ok, *outcome == Outcome::Ok, "the write's result differs");
            Real::Running(lent, None)
        }
        (Real::Running(_, Some(flight)), Label::Kill | Label::Power { .. }) => {
            let ((_, fake), ok) = replay(&flight, Some(Expect::Crash));
            assert!(!ok, "a write the model crashed completed");
            crash(fake, label)
        }
        (Real::Running((_, fake), None), Label::Kill | Label::Power { .. }) => crash(fake, label),
        (Real::Running(lent, Some(mut flight)), env) => {
            flight.steps.push(Expect::Env(*env));
            Real::Running(lent, Some(flight))
        }
        (mut real, env @ (Label::Evict | Label::Latent(_) | Label::Fill | Label::NewerWrite)) => {
            apply_env(disk_of(&mut real), env);
            real
        }
        (real, other) => panic!("the model took {other:?} where the writer is {real:?}"),
    }
}

fn crash(mut fake: Fake, label: &Label) -> Real {
    fake.script.clear();
    if let Label::Power {
        reaches,
        entry,
        len,
    } = label
    {
        fake.disk.power(reaches, *entry, *len);
    }
    Real::Between(fake)
}

struct Walk<'a> {
    config: &'a Config,
    seen: HashSet<State>,
    path: Vec<Label>,
}

impl Walk<'_> {
    fn visit(&mut self, state: &State, real: &Real) {
        if !self.seen.insert(state.clone()) {
            return;
        }
        for (label, next) in state.successors(self.config) {
            let next = next.unwrap_or_else(|property| panic!("the model broke {property:?}"));
            self.path.push(label);
            let mut real = step(real.clone(), &label, state, &next);
            match &mut real {
                Real::Between(fake) => compare_disk(&next, &fake.disk, &self.path),
                Real::Running((writer, fake), None) => {
                    compare_writer(&next, writer, &self.path);
                    compare_disk(&next, &fake.disk, &self.path);
                }
                Real::Running(_, Some(_)) => {}
            }
            self.visit(&next, &real);
            self.path.pop();
        }
    }
}

/// Walks `config` on a thread whose stack holds the recursion: each frame carries the real
/// side's pages.
fn walk(config: Config) -> usize {
    let run = move || {
        let mut walk = Walk {
            config: &config,
            seen: HashSet::new(),
            path: Vec::new(),
        };
        let fake = Fake {
            disk: Disk::new(config.block),
            script: VecDeque::new(),
            read_fault: None,
        };
        walk.visit(&State::initial(&config), &Real::Between(fake));
        walk.seen.len()
    };
    let thread = std::thread::Builder::new()
        .stack_size(1 << 30)
        .spawn(run)
        .unwrap();
    thread
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn budget() -> Budget {
    Budget {
        writes: 4,
        crashes: 2,
        failures: 1,
        evictions: 1,
        resets: 1,
        newer: 1,
        fills: 1,
        ..Budget::default()
    }
}

#[test]
fn the_writer_matches_the_model_under_the_block_law() {
    for block in [Block::B512, Block::B4096, Block::Large] {
        let states = walk(Config::new(block, Device::Atomic, budget()));
        assert!(states > 1000, "{block:?}: {states} states");
    }
}

#[test]
fn the_writer_matches_the_model_on_a_tearing_device() {
    let tears = Device::Tears {
        unchanged_safe: true,
    };
    for block in [Block::B512, Block::B4096, Block::Large] {
        let states = walk(Config::new(block, tears, budget()));
        assert!(states > 1000, "{block:?}: {states} states");
    }
}

#[test]
fn the_writer_matches_the_model_with_unreadable_sectors() {
    let media = Budget {
        latent: 1,
        read_faults: 1,
        newer: 0,
        resets: 0,
        fills: 0,
        ..budget()
    };
    let states = walk(Config::new(Block::B4096, Device::Atomic, media));
    assert!(states > 1000, "{states} states");
}

#[test]
fn a_non_monotonic_record_is_refused_as_a_bug() {
    let fake = Fake {
        disk: Disk::new(Block::B4096),
        script: VecDeque::new(),
        read_fault: None,
    };
    let mut fake = fake;
    let (mut writer, _) = JournalWriter::open(&mut fake);
    let high = model::Facts {
        floor: Some(model::Floor {
            replica: 0,
            bound: 3,
        }),
        ..model::Facts::ABSENT
    };
    let low = model::Facts {
        floor: Some(model::Floor {
            replica: 0,
            bound: 1,
        }),
        ..model::Facts::ABSENT
    };
    let script = [
        Expect::Sys(Call::Create, Outcome::Ok),
        Expect::Sys(Call::WriteWhole, Outcome::Ok),
        Expect::Sys(Call::Fsync, Outcome::Ok),
        Expect::Sys(Call::DirFsync, Outcome::Ok),
    ];
    fake.script = script.into_iter().collect();
    writer.write(&mut fake, &journal_of(&high)).unwrap();
    let refused = lsr_core::reporting_bugs(|| writer.write(&mut fake, &journal_of(&low)));
    assert!(
        matches!(refused, Err(lsr_core::JournalWriteError::Bug(_))),
        "{refused:?}"
    );
    assert_eq!(writer.durable(), Some(&journal_of(&high)));
}
