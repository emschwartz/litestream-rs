//! Coverage marks for the soak: per fault class, counts a nightly gate can fail on.
//!
//! [`crate::reach`] counts states by name and gates the 64 fixed seeds. This
//! module adds the keying the soak is accountable for:
//!
//! - **Fault classes.** Every variant of every injector (`Op`, `MetaOp`,
//!   `RemoteOp`, `WalOp`, `EnvFault`, `FaultShape`, `Hold`) maps, through a
//!   match with no wildcard, to one [`Class`]; a new variant does not build
//!   until it has one. For each class [`FaultMarks`] counts, under
//!   `fault.<class>.<count>` (or `op.<class>.<count>` for the app's and the
//!   operator's own steps):
//!   - `drawn`: a history held it;
//!   - `applied`: it had something to act on (a drawn step with nothing to
//!     act on is skipped, and counted `skipped`);
//!   - `hit`: the engine met it. For a store fault, the faulted call happened;
//!     for a failure injected into our SQLite handles, a call took it; for
//!     every other class, an engine tick ran while it was in place or, for a
//!     one-time edit, after it. The last is the weakest of the three: a tick
//!     that ran is not proof the engine read what was damaged;
//!   - `judged_during`: a restore was judged while a lasting fault held;
//!   - `judged_after`: a restore was judged after it was applied (one-time
//!     faults) or lifted (lasting ones).
//!
//! [`gate`] reads a soak's summed reach and names every required count left
//! at zero. What is required follows from a class's [`Kind`]; a count a class
//! cannot earn says why in
//! [`EXEMPT`]. The key [`CANARY`] is required and nothing bumps it, so a gate
//! that reports zeros at all reports it: a soak passes when the canary is its
//! only zero ([`Gate::passed`]).

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use lsr_core::{SourceError, SourceOp, StoreOp};

use crate::generate::{EnvFault, Hold, MetaOp, Op, RemoteOp, Target, WalOp};
use crate::reach::Reach;
use crate::sqlite::CrashKind;
use crate::store::FaultShape;

/// A key no code bumps. The gate requires it, so it must be the one zero a
/// passing soak reports; a gate that stops seeing zeros fails on its absence.
pub const CANARY: &str = "canary.never";

/// What a class is, which fixes the counts the gate requires of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A step of the app or the operator: drawn and applied.
    Input,
    /// A fault that acts once: drawn, applied, hit, judged after.
    Once,
    /// A fault that lasts: as [`Kind::Once`], and judged while it holds.
    Lasting,
    /// The soak's mixes never draw it: a scenario writes it, or its weight is
    /// zero in every mix. Each says why in the list; the gate requires it to
    /// stay undrawn, so the reason cannot go stale quietly.
    Undrawn,
}

/// One of the counts kept per class.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Count {
    /// A history held it.
    Drawn,
    /// It had something to act on.
    Applied,
    /// It had nothing to act on and was skipped.
    Skipped,
    /// The engine met it.
    Hit,
    /// A restore was judged while it held.
    JudgedDuring,
    /// A restore was judged after it.
    JudgedAfter,
}

impl Count {
    const fn name(self) -> &'static str {
        match self {
            Self::Drawn => "drawn",
            Self::Applied => "applied",
            Self::Skipped => "skipped",
            Self::Hit => "hit",
            Self::JudgedDuring => "judged_during",
            Self::JudgedAfter => "judged_after",
        }
    }
}

/// How a class's `hit` is observed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Probe {
    /// A scheduled store fault's own hit count.
    Store,
    /// A failure injected into our SQLite handles, taken by a call.
    Source,
    /// An engine tick ran with it in place or after it.
    Tick,
}

/// Defines [`Class`] with its names and kinds, in one list.
macro_rules! classes {
    ($($variant:ident = $name:literal, $kind:ident;)*) => {
        /// A fault class (or an input step) the generator can draw.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
        pub enum Class {
            $(
                #[doc = concat!("`", $name, "`.")]
                $variant,
            )*
        }

        impl Class {
            /// Every class.
            pub const ALL: &'static [Self] = &[$(Self::$variant),*];

            /// The class's name in reach keys.
            #[must_use]
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)*
                }
            }

            /// What the class is.
            #[must_use]
            pub const fn kind(self) -> Kind {
                match self {
                    $(Self::$variant => Kind::$kind,)*
                }
            }
        }
    };
}

classes! {
    Commit = "Commit", Input;
    CommitInTick = "CommitInTick", Input;
    AppInTick = "AppInTick", Input;
    OtherConnectionCommit = "OtherConnectionCommit", Input;
    Spill = "Spill", Input;
    Rollback = "Rollback", Input;
    Vacuum = "Vacuum", Input;
    Shrink = "Shrink", Input;
    OtherProcessReader = "OtherProcessReader", Input;
    OtherProcessCheckpoint = "OtherProcessCheckpoint", Input;
    Tick = "Tick", Input;
    Shutdown = "Shutdown", Input;
    RequestSnapshot = "RequestSnapshot", Input;
    Restore = "Restore", Input;
    Heal = "Heal", Input;
    Verify = "Verify", Input;
    // Undrawn: the generator draws Crash instead; Restart, PowerLoss, LyingWalSync (a
    // per-run setting in the mixes instead), OurReader, AppAutocheckpoint and TakeOver are
    // scenario steps.
    Restart = "Restart", Undrawn;
    TakeOver = "TakeOver", Undrawn;
    PowerLoss = "PowerLoss", Undrawn;
    LyingWalSync = "LyingWalSync", Undrawn;
    OurReader = "OurReader", Undrawn;
    AppAutocheckpoint = "AppAutocheckpoint", Undrawn;
    CrashProcess = "Crash.Process", Once;
    CrashPower = "Crash.Power", Once;
    ClockJump = "ClockJump", Once;
    SecondWriter = "SecondWriter", Lasting;
    StoreError = "StoreFault.Error", Once;
    StoreErrorAfterPublish = "StoreFault.ErrorAfterPublish", Once;
    StoreSizeMismatch = "StoreFault.SizeMismatch", Once;
    StoreChecksumMismatch = "StoreFault.ChecksumMismatch", Once;
    StoreCorruptPublish = "StoreFault.CorruptPublish", Once;
    StoreFlipByteOnRead = "StoreFault.FlipByteOnRead", Once;
    StoreRate = "StoreFault.Rate", Lasting;
    EnvDeleteWal = "EnvFault.DeleteWal", Once;
    EnvWipeStaging = "EnvFault.WipeStaging", Once;
    EnvSource = "EnvFault.Source", Once;
    HttpFault = "HttpFault", Once;
    // Undrawn: these EnvFault variants are scenario steps (see their docs in generate.rs).
    // A finding of issue #127 row 42 and others: they never meet another fault by chance.
    EnvShmMidUpdate = "EnvFault.ShmMidUpdate", Undrawn;
    EnvShmEndInsideTx = "EnvFault.ShmEndInsideTx", Undrawn;
    EnvOverwriteLastFrame = "EnvFault.OverwriteLastFrame", Undrawn;
    EnvUnlinkWal = "EnvFault.UnlinkWal", Undrawn;
    EnvEmptyDatabase = "EnvFault.EmptyDatabase", Undrawn;
    EnvTruncateFails = "EnvFault.TruncateFails", Undrawn;
    EnvFlipDatabasePage = "EnvFault.FlipDatabasePage", Undrawn;
    EnvDriverWritesNothing = "EnvFault.DriverWritesNothing", Undrawn;
    // Undrawn: a copy put back is an operator's act (docs/decisions/D173.md): scenarios only.
    MetaSave = "Meta.Save", Undrawn;
    MetaRestore = "Meta.Restore", Undrawn;
    // Undrawn: the generator draws DeleteNewest, TruncateNewest and StripNewestOrigin only
    // with the engine down (StagingDamage), never under a running engine.
    MetaDeleteNewest = "Meta.DeleteNewest", Undrawn;
    MetaFlipNewestByte = "Meta.FlipNewestByte", Once;
    MetaTruncateNewest = "Meta.TruncateNewest", Undrawn;
    MetaStripNewestOrigin = "Meta.StripNewestOrigin", Undrawn;
    MetaDeleteAll = "Meta.DeleteAll", Once;
    // Undrawn: a copy put back is an operator's act (docs/decisions/D173.md): scenarios only.
    DamageSave = "StagingDamage.Save", Undrawn;
    DamageRestore = "StagingDamage.Restore", Undrawn;
    DamageDeleteNewest = "StagingDamage.DeleteNewest", Once;
    DamageFlipNewestByte = "StagingDamage.FlipNewestByte", Once;
    DamageTruncateNewest = "StagingDamage.TruncateNewest", Once;
    DamageStripNewestOrigin = "StagingDamage.StripNewestOrigin", Once;
    // Undrawn: EnvFault.WipeStaging is the drawn form of this.
    DamageDeleteAll = "StagingDamage.DeleteAll", Undrawn;
    // Undrawn: SecondWriter is the drawn form of these two.
    RemotePutUnrecognized = "Remote.PutUnrecognized", Undrawn;
    RemoteRemoveUnrecognized = "Remote.RemoveUnrecognized", Undrawn;
    // Undrawn: scenarios and recorded histories only; the generators do not draw it.
    RemoteRollBack = "Remote.RollBack", Undrawn;
    // Undrawn: weight 0 in the default and swarm mixes; `dropped_created_at` draws it.
    RemoteDropCreatedAt = "Remote.DropCreatedAt", Undrawn;
    // Undrawn as an op: the swarm draws it as Weights::no_create_only, for the whole run.
    RemoteRefuseCreateOnly = "Remote.RefuseCreateOnly", Undrawn;
    // Undrawn: WalDamage is the drawn form of WAL damage; Wal is the scenarios' step.
    WalTearHeader = "Wal.TearHeader", Undrawn;
    WalTruncateAtFrame = "Wal.TruncateAtFrame", Undrawn;
    WalRewriteSameLength = "Wal.RewriteSameLength", Undrawn;
    WalRegrowSameSalts = "Wal.RegrowSameSalts", Undrawn;
    WalRegrowWithoutCommitMarks = "Wal.RegrowWithoutCommitMarks", Undrawn;
    WalFlipLastFrameBit = "Wal.FlipLastFrameBit", Undrawn;
    WalDamageTearHeader = "WalDamage.TearHeader", Lasting;
    WalDamageTruncateAtFrame = "WalDamage.TruncateAtFrame", Lasting;
    // Undrawn: left out of the generator (see `wal_damage` in generate.rs).
    WalDamageRewriteSameLength = "WalDamage.RewriteSameLength", Undrawn;
    WalDamageRegrowSameSalts = "WalDamage.RegrowSameSalts", Lasting;
    WalDamageRegrowWithoutCommitMarks = "WalDamage.RegrowWithoutCommitMarks", Lasting;
    WalDamageFlipLastFrameBit = "WalDamage.FlipLastFrameBit", Lasting;
    HoldStoreDown = "Hold.StoreDown", Lasting;
    HoldCredentialsRevoked = "Hold.CredentialsRevoked", Lasting;
    HoldDiskFull = "Hold.DiskFull", Lasting;
    HoldLyingReads = "Hold.LyingReads", Lasting;
    HoldReadersHeld = "Hold.ReadersHeld", Lasting;
    HoldWal = "Hold.Wal", Lasting;
    HoldLyingDisk = "Hold.LyingDisk", Lasting;
    HoldStoreDownAll = "Hold.StoreDownAll", Lasting;
    HoldSecondWriter = "Hold.SecondWriter", Lasting;
}

const WRITE_LOCK_RARE: &str = "the engine takes the write lock only after a TRUNCATE that moved the WAL, and an app \
     step arms for one tick: ran 0 to 2 times in 6000 seeds (the one required, \
     BeginWriteLock.Commit, has a coverage scenario); a finding";

const PAGE_COUNT_RARE: &str = "the engine reads the page count only for a snapshot that finds nothing in the WAL (a \
     torn header at boot; the repair D173 removed read it too), and an app step arms for one \
     tick: ran 0 or 1 times in 6000 seeds (the one required, PageCount.DeleteWal, has a \
     coverage scenario); a finding";

/// A count the gate would require that a class cannot earn, with the reason. Each is a known gap in what the soak
/// exercises, not a pass.
pub const EXEMPT: &[(&str, &str)] = &[
    // Lasting faults no restore is judged during.
    (
        "fault.SecondWriter.judged_during",
        "the other writer's ticks run with no restore judged; a finding",
    ),
    (
        "fault.WalDamage.TearHeader.judged_during",
        "drawn WAL damage is not judged while in place; Hold.Wal judges this shape",
    ),
    (
        "fault.WalDamage.TruncateAtFrame.judged_during",
        "drawn WAL damage is not judged while in place; Hold.Wal judges this shape",
    ),
    (
        "fault.WalDamage.RegrowSameSalts.judged_during",
        "never judged while in place, by WalDamage or Hold.Wal (which draws no regrow); a finding",
    ),
    (
        "fault.WalDamage.RegrowWithoutCommitMarks.judged_during",
        "never judged while in place, by WalDamage or Hold.Wal (which draws no regrow); a finding",
    ),
    (
        "fault.WalDamage.FlipLastFrameBit.judged_during",
        "drawn WAL damage is not judged while in place; Hold.Wal judges this shape",
    ),
    // Error-domain cells and app action points the soak rarely or never reaches (measured over
    // 6000 seeds with the nightly's TRUNCATE law, docs/decisions.md D112). The engine takes the
    // write lock only for the snapshot after a TRUNCATE that moved the WAL; an injection queued
    // for the lock waits for one, an app step arms for one tick. Marks that rare and required
    // anyway are reached by `lsr_sim::scenario::coverage`.
    ("cell.source.EndWriteLock.Io.hit", WRITE_LOCK_RARE),
    ("point.BeginWriteLock.ReaderBegin.ran", WRITE_LOCK_RARE),
    ("point.BeginWriteLock.ReaderEnd.ran", WRITE_LOCK_RARE),
    (
        "point.BeginWriteLock.Checkpoint.Passive.ran",
        WRITE_LOCK_RARE,
    ),
    (
        "point.BeginWriteLock.Checkpoint.Restart.ran",
        WRITE_LOCK_RARE,
    ),
    (
        "point.BeginWriteLock.Checkpoint.Truncate.ran",
        WRITE_LOCK_RARE,
    ),
    ("point.BeginWriteLock.DeleteWal.ran", WRITE_LOCK_RARE),
    ("point.EndWriteLock.Commit.ran", WRITE_LOCK_RARE),
    ("point.EndWriteLock.ReaderBegin.ran", WRITE_LOCK_RARE),
    ("point.EndWriteLock.ReaderEnd.ran", WRITE_LOCK_RARE),
    ("point.EndWriteLock.Checkpoint.Passive.ran", WRITE_LOCK_RARE),
    ("point.EndWriteLock.Checkpoint.Restart.ran", WRITE_LOCK_RARE),
    (
        "point.EndWriteLock.Checkpoint.Truncate.ran",
        WRITE_LOCK_RARE,
    ),
    ("point.EndWriteLock.DeleteWal.ran", WRITE_LOCK_RARE),
    // The engine reads the page count only for a snapshot that scans nothing, and an app step
    // arms for one tick: Commit ran once in 6000 seeds and the others in none.
    ("point.PageCount.Commit.ran", PAGE_COUNT_RARE),
    ("point.PageCount.ReaderBegin.ran", PAGE_COUNT_RARE),
    ("point.PageCount.ReaderEnd.ran", PAGE_COUNT_RARE),
    ("point.PageCount.Checkpoint.Passive.ran", PAGE_COUNT_RARE),
    ("point.PageCount.Checkpoint.Restart.ran", PAGE_COUNT_RARE),
    ("point.PageCount.Checkpoint.Truncate.ran", PAGE_COUNT_RARE),
    // Ran 3 and 6 times in 6000 seeds, and in some nights not at all.
    (
        "point.ReadPage.ReaderEnd.ran",
        "rare: an app reader ends right before a page read 3 times in 6000 seeds",
    ),
    (
        "point.EndBarrier.Checkpoint.Passive.ran",
        "rare: a PASSIVE right before the barrier ends ran 6 times in 6000 seeds",
    ),
];

impl Class {
    /// The class of a step.
    #[must_use]
    pub const fn of(op: &Op) -> Self {
        match op {
            Op::Commit { .. } => Self::Commit,
            Op::CommitInTick { .. } => Self::CommitInTick,
            Op::AppInTick { .. } => Self::AppInTick,
            Op::OtherConnectionCommit { .. } => Self::OtherConnectionCommit,
            Op::Spill => Self::Spill,
            Op::Rollback => Self::Rollback,
            Op::Vacuum { .. } => Self::Vacuum,
            Op::Shrink { .. } => Self::Shrink,
            Op::OtherProcessReader { .. } => Self::OtherProcessReader,
            Op::OtherProcessCheckpoint(_) => Self::OtherProcessCheckpoint,
            Op::Tick => Self::Tick,
            Op::Shutdown | Op::ShutdownWithSnapshot | Op::ShutdownWithRetention => Self::Shutdown,
            Op::RequestSnapshot => Self::RequestSnapshot,
            Op::Restore => Self::Restore,
            Op::Heal => Self::Heal,
            Op::Verify => Self::Verify,
            Op::Restart => Self::Restart,
            Op::TakeOver => Self::TakeOver,
            Op::PowerLoss => Self::PowerLoss,
            Op::LyingWalSync => Self::LyingWalSync,
            Op::OurReader { .. } => Self::OurReader,
            Op::AppAutocheckpoint(_) => Self::AppAutocheckpoint,
            Op::HttpFault(_) => Self::HttpFault,
            Op::Crash(CrashKind::Process) => Self::CrashProcess,
            Op::Crash(CrashKind::Power) => Self::CrashPower,
            Op::ClockJump(_) => Self::ClockJump,
            Op::SecondWriter { .. } => Self::SecondWriter,
            Op::StoreFault { shape, .. } => Self::of_shape(shape),
            Op::EnvFault(fault) => Self::of_env(*fault),
            Op::Meta(op) => Self::of_meta(*op),
            Op::StagingDamage(op) => Self::of_damage(*op),
            Op::Remote(op) => Self::of_remote(*op),
            Op::Wal(op) => Self::of_wal(*op),
            Op::WalDamage { op, .. } => Self::of_wal_damage(*op),
            Op::Hold { hold, .. } => Self::of_hold(*hold),
        }
    }

    /// The class of a scheduled store fault.
    #[must_use]
    pub const fn of_shape(shape: &FaultShape) -> Self {
        match shape {
            FaultShape::Error(_) => Self::StoreError,
            FaultShape::ErrorAfterPublish(_) => Self::StoreErrorAfterPublish,
            FaultShape::SizeMismatch => Self::StoreSizeMismatch,
            FaultShape::ChecksumMismatch => Self::StoreChecksumMismatch,
            FaultShape::CorruptPublish => Self::StoreCorruptPublish,
            FaultShape::FlipByteOnRead { .. } => Self::StoreFlipByteOnRead,
            FaultShape::Rate { .. } => Self::StoreRate,
        }
    }

    const fn of_env(fault: EnvFault) -> Self {
        match fault {
            EnvFault::DeleteWal => Self::EnvDeleteWal,
            EnvFault::WipeStaging => Self::EnvWipeStaging,
            EnvFault::Source(..) => Self::EnvSource,
            EnvFault::ShmMidUpdate => Self::EnvShmMidUpdate,
            EnvFault::ShmEndInsideTx => Self::EnvShmEndInsideTx,
            EnvFault::OverwriteLastFrame => Self::EnvOverwriteLastFrame,
            EnvFault::UnlinkWal => Self::EnvUnlinkWal,
            EnvFault::EmptyDatabase => Self::EnvEmptyDatabase,
            EnvFault::TruncateFails => Self::EnvTruncateFails,
            EnvFault::FlipDatabasePage(_) => Self::EnvFlipDatabasePage,
            EnvFault::DriverWritesNothing => Self::EnvDriverWritesNothing,
        }
    }

    const fn of_meta(op: MetaOp) -> Self {
        match op {
            MetaOp::Save => Self::MetaSave,
            MetaOp::Restore => Self::MetaRestore,
            MetaOp::DeleteNewest => Self::MetaDeleteNewest,
            MetaOp::FlipNewestByte => Self::MetaFlipNewestByte,
            MetaOp::TruncateNewest => Self::MetaTruncateNewest,
            MetaOp::StripNewestOrigin => Self::MetaStripNewestOrigin,
            MetaOp::DeleteAll => Self::MetaDeleteAll,
        }
    }

    const fn of_damage(op: MetaOp) -> Self {
        match op {
            MetaOp::Save => Self::DamageSave,
            MetaOp::Restore => Self::DamageRestore,
            MetaOp::DeleteNewest => Self::DamageDeleteNewest,
            MetaOp::FlipNewestByte => Self::DamageFlipNewestByte,
            MetaOp::TruncateNewest => Self::DamageTruncateNewest,
            MetaOp::StripNewestOrigin => Self::DamageStripNewestOrigin,
            MetaOp::DeleteAll => Self::DamageDeleteAll,
        }
    }

    const fn of_remote(op: RemoteOp) -> Self {
        match op {
            RemoteOp::PutUnrecognized => Self::RemotePutUnrecognized,
            RemoteOp::RemoveUnrecognized => Self::RemoteRemoveUnrecognized,
            RemoteOp::RollBack => Self::RemoteRollBack,
            RemoteOp::DropCreatedAt => Self::RemoteDropCreatedAt,
            RemoteOp::RefuseCreateOnly => Self::RemoteRefuseCreateOnly,
        }
    }

    const fn of_wal(op: WalOp) -> Self {
        match op {
            WalOp::TearHeader => Self::WalTearHeader,
            WalOp::TruncateAtFrame(_) => Self::WalTruncateAtFrame,
            WalOp::RewriteSameLength(_) => Self::WalRewriteSameLength,
            WalOp::RegrowSameSalts(_) => Self::WalRegrowSameSalts,
            WalOp::RegrowWithoutCommitMarks(_) => Self::WalRegrowWithoutCommitMarks,
            WalOp::FlipLastFrameBit => Self::WalFlipLastFrameBit,
        }
    }

    const fn of_wal_damage(op: WalOp) -> Self {
        match op {
            WalOp::TearHeader => Self::WalDamageTearHeader,
            WalOp::TruncateAtFrame(_) => Self::WalDamageTruncateAtFrame,
            WalOp::RewriteSameLength(_) => Self::WalDamageRewriteSameLength,
            WalOp::RegrowSameSalts(_) => Self::WalDamageRegrowSameSalts,
            WalOp::RegrowWithoutCommitMarks(_) => Self::WalDamageRegrowWithoutCommitMarks,
            WalOp::FlipLastFrameBit => Self::WalDamageFlipLastFrameBit,
        }
    }

    const fn of_hold(hold: Hold) -> Self {
        match hold {
            Hold::StoreDown(_) => Self::HoldStoreDown,
            Hold::CredentialsRevoked => Self::HoldCredentialsRevoked,
            Hold::DiskFull => Self::HoldDiskFull,
            Hold::LyingReads => Self::HoldLyingReads,
            Hold::ReadersHeld => Self::HoldReadersHeld,
            Hold::Wal(_) => Self::HoldWal,
            Hold::LyingDisk => Self::HoldLyingDisk,
            Hold::StoreDownAll => Self::HoldStoreDownAll,
            Hold::SecondWriter => Self::HoldSecondWriter,
        }
    }

    /// A store fault's class is named for its [`FaultShape`] ([`Class::of_shape`]).
    fn probe(self) -> Probe {
        if self == Self::EnvSource {
            Probe::Source
        } else if self.name().starts_with("StoreFault.") {
            Probe::Store
        } else {
            Probe::Tick
        }
    }

    /// The reach key of one of the class's counts.
    #[must_use]
    pub fn key(self, count: Count) -> String {
        let prefix = match self.kind() {
            Kind::Input => "op",
            Kind::Once | Kind::Lasting | Kind::Undrawn => "fault",
        };
        format!("{prefix}.{}.{}", self.name(), count.name())
    }

    /// The counts the gate requires to be above zero.
    #[must_use]
    pub const fn required(self) -> &'static [Count] {
        match self.kind() {
            Kind::Input => &[Count::Drawn, Count::Applied],
            Kind::Once => &[Count::Drawn, Count::Applied, Count::Hit, Count::JudgedAfter],
            Kind::Lasting => &[
                Count::Drawn,
                Count::Applied,
                Count::Hit,
                Count::JudgedDuring,
                Count::JudgedAfter,
            ],
            Kind::Undrawn => &[],
        }
    }
}

/// One run's fault-class counts, kept as the runner applies its steps.
#[derive(Debug)]
pub struct FaultMarks {
    counts: BTreeMap<(Class, Count), u64>,
    /// Lasting classes in place now.
    active: BTreeSet<Class>,
    /// Classes applied or lifted since the last judged restore.
    since: BTreeSet<Class>,
    /// One-time classes waiting for the engine's next tick.
    awaiting_tick: BTreeSet<Class>,
    /// The step being applied, with the ticks run before it.
    current: Option<(Class, u64)>,
    ticks: u64,
    skipped: Cell<bool>,
    /// The error domain's cells, counted apart from their classes.
    cells: crate::domain::marks::CellMarks,
}

impl FaultMarks {
    /// Starts a run's counts.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counts: BTreeMap::new(),
            active: BTreeSet::new(),
            since: BTreeSet::new(),
            awaiting_tick: BTreeSet::new(),
            current: None,
            ticks: 0,
            skipped: Cell::new(false),
            cells: crate::domain::marks::CellMarks::default(),
        }
    }

    fn add(&mut self, class: Class, count: Count, n: u64) {
        let slot = self.counts.entry((class, count)).or_insert(0);
        *slot = slot.saturating_add(n);
    }

    /// The runner begins applying `op`.
    pub fn begin(&mut self, op: &Op) {
        let class = Self::class(op);
        self.add(class, Count::Drawn, 1);
        self.cells.begin(op);
        self.skipped.set(false);
        self.current = Some((class, self.ticks));
        if matches!(class.kind(), Kind::Lasting) {
            self.active.insert(class);
        }
    }

    /// The step being applied had nothing to act on.
    pub fn skip(&self) {
        self.skipped.set(true);
    }

    /// The runner finished applying `op`.
    pub fn end(&mut self, op: &Op) {
        let class = Self::class(op);
        let Some((_, ticks0)) = self.current.take() else {
            return;
        };
        if self.skipped.replace(false) {
            self.add(class, Count::Skipped, 1);
            self.active.remove(&class);
            return;
        }
        self.add(class, Count::Applied, 1);
        if class == Class::Heal {
            self.lift_rates();
        }
        let lasting = matches!(class.kind(), Kind::Lasting);
        if lasting && class.probe() == Probe::Tick && self.ticks > ticks0 {
            self.add(class, Count::Hit, 1);
        }
        if !lasting && class.probe() == Probe::Tick {
            self.awaiting_tick.insert(class);
        }
        // A store fault at a rate lasts until a heal; every other lasting
        // fault is lifted by the step that put it in place.
        if lasting && class != Class::StoreRate {
            self.active.remove(&class);
        }
        if class.kind() != Kind::Input && (!lasting || class != Class::StoreRate) {
            self.since.insert(class);
        }
    }

    fn lift_rates(&mut self) {
        if self.active.remove(&Class::StoreRate) {
            self.since.insert(Class::StoreRate);
        }
    }

    const fn class(op: &Op) -> Class {
        Class::of(op)
    }

    /// The engine ran a tick.
    pub fn ticked(&mut self) {
        self.ticks = self.ticks.saturating_add(1);
        for class in std::mem::take(&mut self.awaiting_tick) {
            self.add(class, Count::Hit, 1);
        }
    }

    /// A restore was judged on content.
    pub fn judged(&mut self) {
        for class in self.active.clone() {
            self.add(class, Count::JudgedDuring, 1);
        }
        for class in std::mem::take(&mut self.since) {
            self.add(class, Count::JudgedAfter, 1);
        }
    }

    /// Adds the run's counts to `reach`, with the calls each store fault
    /// hit (`store_hits`, with the store it was scheduled on) and the failures
    /// injected into our SQLite handles that a call took (`taken`).
    pub fn tally(
        &mut self,
        reach: &mut Reach,
        store_hits: &[(Target, StoreOp, FaultShape, u32)],
        taken: &[(SourceOp, SourceError)],
    ) {
        for (_, _, shape, hits) in store_hits {
            self.add(Class::of_shape(shape), Count::Hit, u64::from(*hits));
        }
        let taken_n = u64::try_from(taken.len()).unwrap_or(u64::MAX);
        self.add(Class::EnvSource, Count::Hit, taken_n);
        for ((class, count), n) in &self.counts {
            reach.add(class.key(*count), *n);
        }
        self.cells.tally(reach, store_hits, taken);
    }
}

impl Default for FaultMarks {
    fn default() -> Self {
        Self::new()
    }
}

/// What the gate found in a soak's summed reach.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gate {
    /// Required keys at zero, the canary among them when the gate works.
    pub zeros: Vec<String>,
    /// Exempt keys that were reached after all: the exemption is stale.
    pub stale: Vec<String>,
    /// Classes written by scenarios only that a history drew, and error-domain cells marked
    /// impossible or unmodelled that a history drew.
    pub drawn_undrawn: Vec<String>,
}

impl Gate {
    /// Whether the soak reached everything it must: the canary is the only
    /// zero, and no scenario-only class was drawn.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.zeros.iter().map(String::as_str).eq([CANARY]) && self.drawn_undrawn.is_empty()
    }
}

/// Every key the gate requires above zero: the canary and each class's required
/// counts, less [`EXEMPT`].
#[must_use]
pub fn required_keys() -> Vec<String> {
    let mut keys = vec![CANARY.to_owned()];
    for class in Class::ALL {
        keys.extend(class.required().iter().map(|count| class.key(*count)));
    }
    keys.extend(crate::domain::marks::required_keys());
    keys.extend(crate::domain::points::required_keys());
    keys.retain(|key| !EXEMPT.iter().any(|(exempt, _)| exempt == key));
    keys
}

/// Judges a soak's summed reach.
#[must_use]
pub fn gate(reach: &Reach) -> Gate {
    let zeros = required_keys()
        .into_iter()
        .filter(|key| reach.get(key) == 0)
        .collect();
    let stale = EXEMPT
        .iter()
        .filter(|(key, _)| reach.get(key) > 0)
        .map(|(key, _)| (*key).to_owned())
        .collect();
    let drawn_undrawn = Class::ALL
        .iter()
        .filter(|class| class.kind() == Kind::Undrawn)
        .map(|class| class.key(Count::Drawn))
        .chain(crate::domain::marks::undrawn_keys())
        .filter(|key| reach.get(key) > 0)
        .collect();
    Gate {
        zeros,
        stale,
        drawn_undrawn,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_distinct() {
        let mut names: Vec<&str> = Class::ALL.iter().map(|class| class.name()).collect();
        let before = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), before);
    }

    #[test]
    fn the_canary_is_the_only_zero_of_a_reach_that_has_everything_else() {
        let mut reach = Reach::default();
        for key in required_keys().into_iter().filter(|key| key != CANARY) {
            reach.bump(key);
        }
        let judged = gate(&reach);
        assert!(judged.passed(), "{judged:?}");
        reach.bump(CANARY);
        assert!(!gate(&reach).passed(), "a gate that sees no zero must fail");
        let empty = gate(&Reach::default());
        assert!(!empty.passed());
        assert_eq!(empty.zeros.len(), required_keys().len());
    }

    #[test]
    fn every_exemption_names_a_required_count() {
        let mut all = vec![CANARY.to_owned()];
        for class in Class::ALL {
            all.extend(class.required().iter().map(|count| class.key(*count)));
        }
        all.extend(crate::domain::marks::required_keys());
        all.extend(crate::domain::points::required_keys());
        for (key, why) in EXEMPT {
            assert!(all.iter().any(|k| k == key), "{key} is not a gated key");
            assert!(!why.is_empty() && *key != CANARY, "{key}");
        }
    }
}
