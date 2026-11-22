//! The world the checker explores: the file ([`Fs`]), at most one engine process running the
//! reference writer, the recorder's log of every record issued, and the recorder's own memory of
//! what was promised (ghost state the process cannot see).
//!
//! Each transition is one system call with one of its outcomes, one engine decision, one boot,
//! one crash image, or one environment event. Properties are checked on the transition that can
//! break them: a boot's verdict, a write's target, an acknowledgement.

use super::fs::{Block, Content, Device, Fs, Len, Partial, Reaches, Rid, Slot};

mod boot;
mod job;

/// The unproven-WAL debt: a WAL fsync failed and the cure is at this step.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Unproven {
    /// No debt.
    No,
    /// A TRUNCATE checkpoint is owed.
    Truncate,
    /// The TRUNCATE ran; a snapshot is owed.
    Snapshot,
}

impl Unproven {
    /// The next step of the cure.
    const fn next(self) -> Self {
        match self {
            Self::No => Self::Truncate,
            Self::Truncate => Self::Snapshot,
            Self::Snapshot => Self::No,
        }
    }

    /// Whether `self` may follow `old` in consecutive records: the debt only moves forward
    /// through its cure (review 5, J5).
    const fn may_follow(self, old: Self) -> bool {
        matches!(
            (old, self),
            (Self::No, Self::No | Self::Truncate)
                | (Self::Truncate, Self::Truncate | Self::Snapshot)
                | (Self::Snapshot, Self::Snapshot | Self::No)
        )
    }
}

/// The floor: a destination and a bound under which every transaction id is taken.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Floor {
    /// Which destination the bound is for.
    pub replica: u8,
    /// The bound.
    pub bound: u8,
}

/// The four facts a record holds, at the grain the storage checks need: the floor, the debt, the
/// budget's token count and whether a pending proof is held.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Facts {
    /// The floor, if any.
    pub floor: Option<Floor>,
    /// The unproven-WAL debt.
    pub unproven: Unproven,
    /// The snapshot budget's tokens.
    pub tokens: u8,
    /// Whether a pending proof is held.
    pub pending: bool,
}

impl Facts {
    /// What an absent journal means: no floor, no debt known, a full budget, no proof.
    pub const ABSENT: Self = Self {
        floor: None,
        unproven: Unproven::No,
        tokens: 1,
        pending: false,
    };

    /// Whether `self`'s floor is below `other`'s for the same destination.
    #[must_use]
    pub fn floor_below(&self, other: &Self) -> bool {
        match (self.floor, other.floor) {
            (Some(a), Some(b)) => a.replica == b.replica && a.bound < b.bound,
            (None, Some(_)) => true,
            (Some(_) | None, None) => false,
        }
    }

    /// The record after `old` the writer accepts (review 5, J5): the same destination's bound
    /// does not fall, a floor is not dropped, and the debt moves only through its cure.
    #[must_use]
    pub fn monotonic(old: Option<&Self>, new: &Self) -> bool {
        let Some(old) = old else {
            return true;
        };
        let floor = match (old.floor, new.floor) {
            (Some(o), Some(n)) => o.replica != n.replica || n.bound >= o.bound,
            (Some(_), None) => false,
            (None, _) => true,
        };
        floor && new.unproven.may_follow(old.unproven)
    }
}

/// A record some writer issued.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Rec {
    /// Its sequence number.
    pub seq: u8,
    /// What it says.
    pub facts: Facts,
    /// Whether a newer binary wrote it (its version is above this binary's).
    pub newer: bool,
}

/// What the engine asks the journal to record.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Op {
    /// Every fact moves: the bound rises, the debt steps, the budget and proof change.
    Advance,
    /// The destination changed: a logged reset, after which the bound starts again.
    Reset,
    /// An engine bug: the bound falls with no reset. The writer must refuse it.
    Lower,
}

/// A deliberate defect in the reference writer, for showing the checker catches it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Plant {
    /// Creation skips the directory fsync.
    NoDirFsync,
    /// A write overwrites the slot holding the newest record instead of the other one.
    WrongSlot,
    /// A read error is reported as a missing journal.
    EioIsMissing,
    /// Both slots are read with one call, so an error in one loses the other (review 5, J2).
    OneRead,
    /// A winner with `seq = 1` is taken as a finished creation (review 5, J1).
    Seq1Created,
    /// Boot takes the winner as durable without having synced it.
    ClaimWinner,
    /// A failed fdatasync is treated as a success.
    FailedSyncIsSuccess,
    /// After a failed fdatasync the retry is a bare fdatasync, not a rewrite.
    BareRetry,
    /// A record is acknowledged without an fdatasync.
    NoSync,
    /// The reader takes the lower sequence number.
    LowerSeqWins,
    /// Creation sets the length with `ftruncate` instead of writing zeros: slot 1 is a hole.
    SetLenCreate,
    /// The writer does not check that a record is monotonic over the one before it.
    NoMonotonic,
    /// The first write of a lifetime does not first sync what an earlier process left dirty.
    NoBootSync,
    /// Recreating an unreadable file writes the new record without first zeroing the file.
    RecreateWithoutZeroing,
    /// A boot that could not read a slot continues from the other slot's record.
    ContinueAfterSlotError,
}

/// How much of each kind of event a run may draw.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct Budget {
    /// Engine decisions that ask for a write.
    pub writes: u8,
    /// Process kills and power losses.
    pub crashes: u8,
    /// Failed system calls.
    pub failures: u8,
    /// Evictions of clean pages.
    pub evictions: u8,
    /// Latent sector errors.
    pub latent: u8,
    /// Transient read errors at a boot.
    pub read_faults: u8,
    /// Records written by a newer binary.
    pub newer: u8,
    /// Destination changes.
    pub resets: u8,
    /// Times the disk fills up.
    pub fills: u8,
}

/// One exploration: the disk, the faults it may draw, the engine's decisions, and the plant.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Config {
    /// The filesystem block.
    pub block: Block,
    /// The device under it.
    pub device: Device,
    /// How many of each event.
    pub budget: Budget,
    /// Whether the engine may issue [`Op::Lower`].
    pub lower: bool,
    /// Whether a failed write may leave part of a page changed in the cache. A write of one
    /// aligned 4096-byte slot from a resident buffer is page-atomic on Linux (the copy into the
    /// page cache stops only on a fault in the source buffer, and space is reserved before the
    /// copy), so this is off except in the scenario that shows why the writer needs it off.
    pub torn_writes: bool,
    /// The defect under test, if any.
    pub plant: Option<Plant>,
}

/// Why a journal that exists cannot be read.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Why {
    /// An I/O error, and no slot valid.
    Io,
    /// Every slot short or failing its checksum.
    Invalid,
    /// Two valid slots with one sequence number, which no writer produces.
    Tie,
}

/// What boot read.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Verdict {
    /// No file.
    Missing,
    /// A file, and no record in it.
    Unreadable(Why),
    /// The winning record.
    Read(Rid),
    /// The winning record is from a newer binary.
    Newer(Rid),
}

/// A transient read error injected at one boot.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ReadFault {
    /// `open` fails.
    Open,
    /// The read of this slot fails.
    Slot(Slot),
}

/// A system call the writer makes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Call {
    /// `open(O_CREAT)`.
    Create,
    /// One write of all 8192 bytes: a record in slot 0, zeros in slot 1.
    WriteWhole,
    /// One write of 8192 zeros.
    WriteZeros,
    /// One write of all of a slot.
    WriteSlot(Slot),
    /// `fsync` or `fdatasync` of the file.
    Fsync,
    /// `fsync` of the directory.
    DirFsync,
}

/// How a system call returned.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Outcome {
    /// Success.
    Ok,
    /// An error, having done part of a write (for a whole-file write, `Head` stops after slot 0).
    Failed(Partial),
    /// The disk is full.
    Enospc,
    /// An fsync failed, leaving each dirty slot so.
    SyncFailed(Reaches),
    /// A directory fsync failed; whether the entry is on disk.
    DirFailed(bool),
}

/// One transition.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Label {
    /// A process started and read the journal.
    Boot(Option<ReadFault>, Verdict),
    /// The engine asked for a record.
    Engine(Op),
    /// The writer refused a record that is not monotonic (a `Fault::Bug` in the engine).
    Refused(Op),
    /// A system call returned.
    Sys(Call, Outcome),
    /// The process was killed.
    Kill,
    /// The power went; the dirty slots landed so, and the file is as `entry` and `len` say.
    Power {
        /// Each dirty slot's fate.
        reaches: Reaches,
        /// Whether the file survived.
        entry: bool,
        /// Its length.
        len: Len,
    },
    /// The kernel dropped clean pages.
    Evict,
    /// A sector went bad.
    Latent(Slot),
    /// A newer binary wrote a record.
    NewerWrite,
    /// The disk filled up.
    Fill,
}

impl Label {
    /// The label as text with record ids elided: what a scenario step names, and what a walk
    /// tallies, since ids differ between histories that are otherwise alike.
    #[must_use]
    pub fn kind(&self) -> String {
        let text = format!("{self:?}");
        let mut out = String::with_capacity(text.len());
        let mut rest = text.as_str();
        while let Some(at) = rest.find("Rid(") {
            out.push_str(rest.get(..at).unwrap_or_default());
            out.push_str("Rid");
            let after = rest.get(at..).unwrap_or_default();
            rest = after
                .find(')')
                .and_then(|end| after.get(end.saturating_add(1)..))
                .unwrap_or_default();
        }
        out.push_str(rest);
        out
    }
}

/// A stated property of the journal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Property {
    /// No boot reads a record older than the newest one acknowledged or claimed durable, and
    /// none reads nothing once one was (where the device and faults allow the strict form).
    Recovered,
    /// A boot finds no file only when no record was ever acknowledged.
    MissingAfterAck,
    /// A read error never turns into `Missing`.
    EioNotMissing,
    /// A slot that reads valid on its own is never lost to its neighbour (review 5, J2).
    SlotSurvives,
    /// The floor read or written never falls below the acknowledged floor without a logged reset.
    FloorMonotone,
    /// A record is acknowledged only once the file's entry and full length are durable (J1).
    CreateFinished,
    /// No write of a record after the first needs a new block, so ENOSPC cannot refuse it.
    NoAllocation,
    /// Two valid slots never share a sequence number.
    NoTie,
}

/// Where the writer is inside one write.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Pc {
    EstCreate,
    EstZero,
    EstZeroSync,
    EstWrite,
    EstSync,
    EstDir,
    FinWrite,
    FinSync,
    FinDir,
    BootSync,
    RecWrite,
    RecSync,
}

/// What this process found at boot.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Base {
    /// No file, or no record: the next write creates the file in place.
    Establish,
    /// A record to continue from.
    Winner,
}

/// The engine process and its writer.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Proc {
    base: Base,
    seq: u8,
    winner: Slot,
    next: Slot,
    created: bool,
    synced: bool,
    durable: Option<Rid>,
    last: Option<Facts>,
    facts: Facts,
    job: Option<(Rid, Pc)>,
    retry: Option<Rid>,
}

/// What the recorder remembers that no process can see.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct Ghost {
    /// The newest record acknowledged or claimed durable.
    floor: Option<Rid>,
    /// A logged reset happened since `floor` was set.
    reset: bool,
    /// An fsync failed and none has succeeded since.
    outstanding: bool,
    /// The strict form of [`Property::Recovered`] is out of scope until the next acknowledgement
    /// (a tearing device after a failed fsync crossed a lifetime; module doc of `journal`).
    relaxed: bool,
    /// A latent sector error was injected: only the weak properties hold from here.
    media: bool,
}

/// One state of the world.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct State {
    fs: Fs,
    proc: Option<Proc>,
    log: Vec<Rec>,
    ghost: Ghost,
    left: Budget,
    dest: u8,
    full: bool,
}

/// A transition's result: the next state, or the property it broke.
pub type Step = (Label, Result<State, Property>);

impl State {
    /// No file, no process, nothing issued.
    #[must_use]
    pub fn initial(config: &Config) -> Self {
        Self {
            fs: Fs::ABSENT,
            proc: None,
            log: Vec::new(),
            ghost: Ghost::default(),
            left: config.budget,
            dest: 0,
            full: false,
        }
    }

    /// The file.
    #[must_use]
    pub const fn fs(&self) -> &Fs {
        &self.fs
    }

    /// The record `rid` names.
    #[must_use]
    pub fn rec(&self, rid: Rid) -> Option<&Rec> {
        self.log.get(usize::from(rid.0))
    }

    /// The record `slot` reads now, if it reads one.
    #[must_use]
    pub fn reads(&self, slot: Slot) -> Option<Rec> {
        match self.fs.read(slot) {
            super::fs::SlotRead::Got(Content::Rec(rid)) => self.rec(rid).copied(),
            super::fs::SlotRead::Got(Content::Zero | Content::Garbage)
            | super::fs::SlotRead::Short
            | super::fs::SlotRead::Eio => None,
        }
    }

    /// Every transition from here.
    #[must_use]
    pub fn successors(&self, config: &Config) -> Vec<Step> {
        let mut out = Vec::new();
        match self.proc {
            None => out.extend(self.boots(config)),
            Some(proc) => {
                match proc.job {
                    None => out.extend(self.decisions(config, proc)),
                    Some(job) => out.extend(self.syscall(config, proc, job)),
                }
                out.extend(self.crashes(config));
            }
        }
        out.extend(self.environment(config));
        out.into_iter()
            .map(|(label, next)| (label, next.map(Self::canonical)))
            .collect()
    }

    /// Kills and power losses of the running process.
    fn crashes(&self, config: &Config) -> Vec<Step> {
        let Some(left) = self.left.crashes.checked_sub(1) else {
            return Vec::new();
        };
        let mut base = self.clone();
        base.left.crashes = left;
        base.proc = None;
        let mut kill = base.clone();
        kill.ghost.relaxed |= kill.ghost.outstanding;
        let mut out = vec![(Label::Kill, Ok(kill))];
        for (fs, reaches) in self.fs.power_losses(config.block, config.device) {
            let mut next = base.clone();
            next.fs = fs;
            next.ghost.outstanding = false;
            let label = Label::Power {
                reaches,
                entry: fs.exists(),
                len: fs.len(),
            };
            out.push((label, Ok(next)));
        }
        out
    }

    /// Events no process causes.
    fn environment(&self, _config: &Config) -> Vec<Step> {
        let mut out = Vec::new();
        if let Some(left) = self.left.evictions.checked_sub(1)
            && let Some(fs) = self.fs.evicted()
        {
            let mut next = self.clone();
            next.left.evictions = left;
            next.fs = fs;
            out.push((Label::Evict, Ok(next)));
        }
        if let Some(left) = self.left.latent.checked_sub(1) {
            for slot in Slot::BOTH {
                if self.fs.exists() && self.fs.len().covers(slot) && !self.fs.sector(slot).bad() {
                    let mut next = self.clone();
                    next.left.latent = left;
                    next.fs = self.fs.latent(slot);
                    next.ghost.media = true;
                    out.push((Label::Latent(slot), Ok(next)));
                }
            }
        }
        if let Some(left) = self.left.fills.checked_sub(1) {
            let mut next = self.clone();
            next.left.fills = left;
            next.full = true;
            out.push((Label::Fill, Ok(next)));
        }
        if self.proc.is_none()
            && let Some(next) = self.newer_write()
        {
            out.push((Label::NewerWrite, Ok(next)));
        }
        out
    }

    /// A newer binary boots, reads the journal correctly, and writes one record to the slot
    /// that does not hold the winner, synced.
    fn newer_write(&self) -> Option<Self> {
        let left = self.left.newer.checked_sub(1)?;
        let (slot, seq) = self.winner_on_disk()?;
        // A newer binary keeps the rule that `seq >= 2` proves creation returned: it finishes
        // an unfinished creation before writing, so it writes only over a finished one.
        let finished = self.fs.entry_durable() && self.fs.len_durable() == Len::Full;
        if seq < 2 || !finished {
            return None;
        }
        let mut next = self.clone();
        next.left.newer = left;
        let rid = next.issue(Rec {
            seq: seq.checked_add(1)?,
            facts: Facts::ABSENT,
            newer: true,
        })?;
        next.fs = self
            .fs
            .write(slot.other(), Content::Rec(rid), Block::B4096, None)
            .synced();
        next.ghost.floor = Some(rid);
        next.ghost.reset = false;
        Some(next)
    }

    /// The valid slot with the highest sequence number, as a correct reader sees it.
    fn winner_on_disk(&self) -> Option<(Slot, u8)> {
        Slot::BOTH
            .into_iter()
            .filter_map(|slot| match self.fs.read(slot) {
                super::fs::SlotRead::Got(Content::Rec(rid)) => Some((slot, self.rec(rid)?.seq)),
                super::fs::SlotRead::Got(Content::Zero | Content::Garbage)
                | super::fs::SlotRead::Short
                | super::fs::SlotRead::Eio => None,
            })
            .max_by_key(|(_, seq)| *seq)
    }

    /// Appends `rec` to the log.
    fn issue(&mut self, rec: Rec) -> Option<Rid> {
        let rid = Rid(u8::try_from(self.log.len()).ok()?);
        self.log.push(rec);
        Some(rid)
    }

    /// The same state with every record nothing refers to dropped and the rest renumbered in
    /// order, so that runs differing only in forgotten history meet.
    fn canonical(mut self) -> Self {
        let mut used = vec![false; self.log.len()];
        let mut mark = |rid: Rid| {
            if let Some(slot) = used.get_mut(usize::from(rid.0)) {
                *slot = true;
            }
        };
        self.fs.contents().for_each(|content| {
            if let Content::Rec(rid) = content {
                mark(rid);
            }
        });
        let proc_rids = self
            .proc
            .iter()
            .flat_map(|p| [p.durable, p.retry, p.job.map(|j| j.0)]);
        self.ghost
            .floor
            .into_iter()
            .chain(proc_rids.flatten())
            .for_each(&mut mark);
        let mut map = Vec::with_capacity(used.len());
        let mut log = Vec::new();
        for (rec, keep) in self.log.iter().zip(&used) {
            map.push(Rid(u8::try_from(log.len()).unwrap_or(u8::MAX)));
            if *keep {
                log.push(*rec);
            }
        }
        let rename = |rid: Rid| map.get(usize::from(rid.0)).copied().unwrap_or(rid);
        self.log = log;
        self.fs = self.fs.renamed(rename);
        self.ghost.floor = self.ghost.floor.map(rename);
        if let Some(proc) = self.proc.as_mut() {
            proc.durable = proc.durable.map(rename);
            proc.retry = proc.retry.map(rename);
            proc.job = proc.job.map(|(rid, pc)| (rename(rid), pc));
        }
        self
    }
}

impl Config {
    /// The block law alone: no defect, `block`, the atomic device, the given budget.
    #[must_use]
    pub const fn new(block: Block, device: Device, budget: Budget) -> Self {
        Self {
            block,
            device,
            budget,
            lower: false,
            torn_writes: false,
            plant: None,
        }
    }

    /// This configuration with `plant` in the writer.
    #[must_use]
    pub const fn planted(self, plant: Plant) -> Self {
        Self {
            plant: Some(plant),
            ..self
        }
    }

    /// Whether `plant` is in the writer.
    fn has(&self, plant: Plant) -> bool {
        self.plant == Some(plant)
    }
}
