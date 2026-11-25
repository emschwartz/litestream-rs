//! The history generator: a seed and a length become a list of operations.
//!
//! A history is a pure function of its seed: it draws from one sequential
//! stream and looks at no state, so the first `k` operations of a history of
//! `n` are the history of `k` (what [`crate::shrink`] relies on). Whatever
//! needs the world's state (which pages a commit touches, which other process ends)
//! is decided when the runner applies the operation, from the step number.
//!
//! A regression must keep reproducing its bug when the live generator gains a
//! draw, so it never replays through [`history`]: it is kept as its list of
//! operations, recorded by [`crate::record`] or written by hand, and replayed
//! through `run_ops`.

use std::io::ErrorKind;
use std::time::Duration;

use lsr_core::{Entropy, SeededEntropy, SourceError, SourceOp, StoreError, StoreOp};
use lsr_ltx::PageSize;

use crate::clock::ClockLaw;
use crate::seed::Seed;
use crate::sqlite::{CrashKind, OtherProcessMode};
use crate::store::FaultShape;

/// Which store a fault is scheduled on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The staging directory.
    Staging,
    /// The replica.
    Remote,
}

/// A fault in the app's or the machine's world rather than a store's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvFault {
    /// The last connection closes and the WAL file is deleted (when no
    /// connection holds a transaction).
    DeleteWal,
    /// The staging directory is wiped and the process dies with it.
    WipeStaging,
    /// The next call of an operation on our SQLite handles fails.
    Source(SourceOp, SourceError),
    /// The next wal-index read finds its two header copies unequal, as when a
    /// writer is mid-update. Scenarios only.
    ShmMidUpdate,
    /// The next wal-index read reports an end inside a transaction. Scenarios
    /// only.
    ShmEndInsideTx,
    /// The last committed frame is overwritten between the scan that verified
    /// it and the read of its page. Scenarios only.
    OverwriteLastFrame,
    /// The WAL file is unlinked while SQLite keeps writing it. Scenarios only.
    UnlinkWal,
    /// The next page count of the database reads zero: the file exists and
    /// holds no page yet. Scenarios only.
    EmptyDatabase,
    /// The next TRUNCATE checkpoint that finishes resets the wal-index and
    /// fails to truncate the file, which keeps its old header. Scenarios only.
    TruncateFails,
    /// One byte of this page of the database file flips, outside SQLite; the
    /// database the application serves has it from then on. Scenarios only.
    FlipDatabasePage(u32),
    /// The next driver start writes no WAL header, as the real driver's open
    /// does not once `_litestream_seq` exists. Scenarios only.
    DriverWritesNothing,
}

/// The request an [`HttpFault`] waits for: the shapes the S3 store sends for the simulator's
/// files, all of them small enough for a single PUT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpShape {
    /// ListObjectsV2.
    List,
    /// HEAD of one object (a created-at time).
    Head,
    /// GET of one object, or a range of it.
    Get,
    /// A single PUT.
    Put,
    /// DeleteObjects.
    DeleteObjects,
    /// ListMultipartUploads (the boot's sweep of abandoned uploads).
    ListUploads,
}

/// A provider behaviour the S3 store's fault layer answers the next request of a shape with
/// (`lsr_store_s3::Fault`). Applies only with the replica on S3 ([`Weights::remote_s3`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpFault {
    /// 503 `SlowDown`: a throttle.
    SlowDown(HttpShape),
    /// 429 with `Retry-After: 1`.
    TooManyRequests(HttpShape),
    /// 500 `InternalError`, the request never reaching the bucket.
    Internal(HttpShape),
    /// 408 `RequestTimeout`: Tigris shedding load.
    RequestTimeout(HttpShape),
    /// The request lands, and the caller sees 500: a lost acknowledgement.
    AfterPublish(HttpShape),
    /// The response body ends after 16 bytes, before its `Content-Length`.
    TruncateBody(HttpShape),
    /// The ETag answered belongs to other bytes: a provider that accepted a digest it did not
    /// verify.
    WrongEtag(HttpShape),
    /// No response: the connection is reset.
    ConnectionReset(HttpShape),
    /// The provider refuses the request with `code`'s status and S3 error code.
    Denied(HttpShape, DenialCode),
    /// A bucket policy refuses deleting each object the replica holds now, key by key inside a
    /// DeleteObjects that otherwise succeeds, until the next heal.
    DenyDeletes,
}

/// An S3 refusal, as a provider answers it: AWS's error codes, and a bare 401.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenialCode {
    /// 403 `AccessDenied`.
    AccessDenied,
    /// 403 `AllAccessDisabled`.
    AllAccessDisabled,
    /// 403 `InvalidAccessKeyId`.
    InvalidAccessKeyId,
    /// 403 `SignatureDoesNotMatch`.
    SignatureDoesNotMatch,
    /// 400 `ExpiredToken`.
    ExpiredToken,
    /// 401 with no code.
    Unauthorized,
    /// 403 `RequestTimeTooSkewed`.
    RequestTimeTooSkewed,
    /// 404 `NoSuchBucket`.
    NoSuchBucket,
    /// 400 `KMS.DisabledException`: the object's KMS key is disabled.
    KmsDisabled,
    /// 400 `KMS.NotFoundException`: the object's KMS key is gone.
    KmsNotFound,
}

/// One step of a history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// The app commits `pages` random pages (more when it grows).
    Commit {
        /// How many pages it writes besides those it adds.
        pages: u8,
        /// The database grows by a few pages.
        grow: bool,
    },
    /// The actor runs one tick, and the app commits `pages` random pages
    /// inside it: after the tick's sync read the committed end, right before a
    /// checkpoint takes its write barrier (nothing, if the tick takes none).
    /// The checkpoint's seal must stage that commit, and keep it staged when a
    /// later step fails.
    CommitInTick {
        /// How many pages it writes; it never grows the database.
        pages: u8,
    },
    /// The app writes frames of a transaction and does not commit.
    Spill,
    /// Another application connection, which stays open (another process: a second writer, a
    /// replicator writing its own table), commits `pages` random pages. It keeps its own
    /// checkpoint count, so the WAL header it writes after a restart whose writer rolled back
    /// carries the same salts and another seed.
    OtherConnectionCommit {
        /// How many pages it writes; it never grows the database.
        pages: u8,
    },
    /// The app rolls its open transaction back.
    Rollback,
    /// The app vacuums: into pages of `page_size`, or (`None`) at the same size,
    /// which is one large transaction that rewrites every page.
    Vacuum {
        /// The new page size.
        page_size: Option<PageSize>,
    },
    /// Another process begins a read transaction, or the oldest one ends.
    OtherProcessReader {
        /// Begin (otherwise end).
        begin: bool,
    },
    /// A checkpoint from outside the engine.
    OtherProcessCheckpoint(OtherProcessMode),
    /// The actor runs one tick, and the app does `op` inside it, right before one of
    /// the tick's calls into SQLite (`sqlite/in_tick.rs`), or not at all when the tick
    /// makes no such call.
    AppInTick {
        /// The call it comes before.
        at: AppAt,
        /// What the app does.
        op: AppOp,
    },
    /// The next tick is cut short, or the machine fails right after it.
    Crash(CrashKind),
    /// A store call fails `ahead` calls from now.
    StoreFault {
        /// Which store.
        target: Target,
        /// Which operation.
        op: StoreOp,
        /// How many calls of it pass first.
        ahead: u16,
        /// What the failure looks like.
        shape: FaultShape,
    },
    /// A fault outside the stores.
    EnvFault(EnvFault),
    /// The clock moves by this many milliseconds, either way.
    ClockJump(i64),
    /// The actor runs one tick.
    Tick,
    /// The operator stops the engine: it drains, then a fresh one boots later.
    Shutdown,
    /// The operator stops the engine as `replicate -once -force-snapshot` does: it drains and
    /// ends with a level-9 snapshot, then a fresh one boots later. Scenarios only.
    ShutdownWithSnapshot,
    /// The operator stops the engine as `replicate -once -enforce-retention` does: it drains
    /// and ends with a retention pass, then a fresh one boots later. Scenarios only.
    ShutdownWithRetention,
    /// The operator asks for a snapshot.
    RequestSnapshot,
    /// The oracle restores every transaction and `Latest` and judges them.
    Restore,
    /// The process dies between ticks and a new one starts. Scenarios only: a
    /// history never draws this.
    Restart,
    /// The operator runs `lsr trust-local`: a takeover record through the
    /// replica's newest transaction, read by the next process. Scenarios
    /// only.
    TrustLocal,
    /// The machine loses power between ticks. Scenarios only.
    PowerLoss,
    /// From now on the disk lies about the engine's own fsync of the WAL: it
    /// succeeds and keeps nothing. Scenarios only.
    LyingWalSync,
    /// A step on the staging directory, with the engine as it is: scenarios
    /// stop it first where they mean it down.
    Meta(MetaOp),
    /// Damage to the WAL file that SQLite would not do itself.
    Wal(WalOp),
    /// Our own read transaction ends or begins, as at a checkpoint's lapse.
    /// Scenarios only.
    OurReader {
        /// Begin (otherwise end).
        begin: bool,
    },
    /// A step on the replica from outside the engine. Scenarios only.
    Remote(RemoteOp),
    /// The app's `wal_autocheckpoint` becomes this many frames (`None`: 0, the model's
    /// default), and the engine is told so. Scenarios only.
    AppAutocheckpoint(Option<u32>),
    /// The S3 store's fault layer answers a request with a provider fault. Skipped with the
    /// replica on `MemStore`.
    HttpFault(HttpFault),
    /// The app shrinks the database by up to this many pages (it keeps at
    /// least one): a commit of page 1 with a smaller size.
    Shrink {
        /// How many pages at most it drops.
        pages: u8,
    },
    /// Another writer puts a level-0 file one transaction past the replica's
    /// newest, the engine runs this many ticks with it there, then the
    /// operator removes it (if it still holds the other writer's bytes) and
    /// restarts the engine.
    SecondWriter {
        /// Ticks the engine runs while the other writer's file is there.
        ticks: u8,
    },
    /// The process dies, this is done to the staging directory, and a new
    /// process starts: a staging step while the engine is down.
    StagingDamage(MetaOp),
    /// Every store fault that fails at a rate stops failing.
    Heal,
    /// The WAL is damaged, the engine runs this many ticks, and then the app
    /// heals it as an application would: its connections let go, another
    /// process truncates the WAL, and it commits again (a flipped bit is
    /// flipped back first).
    WalDamage {
        /// The damage.
        op: WalOp,
        /// Ticks the engine runs with the damage in place.
        ticks: u8,
    },
    /// A fault that lasts: it is put in place, the app commits and the engine
    /// runs this many ticks under it, the replica is restored and judged in the
    /// middle of them and at their end while the fault still holds, and then
    /// the fault is lifted.
    Hold {
        /// The fault.
        hold: Hold,
        /// Ticks the engine runs under it.
        ticks: u8,
    },
    /// The scheduled verify runs once: the engine reads the live pages in a
    /// sync tick, the replica is checked at names depth and the live
    /// transaction restored and compared with them, and the verdict is judged
    /// against what the database really was.
    Verify,
}

/// A fault that lasts across ticks, for [`Op::Hold`]. A restore judged while
/// one holds is the evidence that the replica survives it, not merely
/// recovers from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hold {
    /// Every call of the operation on the replica fails as transient: the
    /// store is down for it. Restores made meanwhile go through the same fault.
    StoreDown(StoreOp),
    /// Every call on the replica is refused as unauthorized: credentials
    /// revoked. Restores made meanwhile are refused too.
    CredentialsRevoked,
    /// Every staging upload fails as the disk is full (`ENOSPC`, which the
    /// directory store reports as a transient `StorageFull`).
    DiskFull,
    /// Every read of the replica returns one byte flipped (a middlebox, or a
    /// store that returns bytes it never stored): a restore's, in the middle of
    /// what it read, and the engine's next reads, in the LTX header.
    LyingReads,
    /// Other processes hold read transactions on the WAL, so no checkpoint
    /// can pass them.
    ReadersHeld,
    /// The WAL is damaged.
    Wal(WalOp),
    /// The disk lies about the engine's fsync of the WAL, and the power fails
    /// while it lies, in the middle of the hold.
    LyingDisk,
    /// Every operation on the replica fails as transient: the store is down
    /// for all of them at once. Restores made meanwhile go through the same
    /// fault. Drawn only with [`Weights::overlap`].
    StoreDownAll,
    /// Another writer's file stands on the replica past the engine's
    /// position, and the replica is judged while it stands. Drawn only with
    /// [`Weights::overlap`].
    SecondWriter,
}

/// Where inside a tick the app acts ([`Op::AppInTick`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppAt {
    /// Before the call with this many calls before it, modulo the calls the tick before
    /// made, so that it falls inside a tick of that length.
    Call(u8),
    /// Before the tick's first call of this kind.
    Before(SourceOp),
}

/// What the app does inside a tick ([`Op::AppInTick`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppOp {
    /// It commits `pages` random pages, and the pages a growth adds.
    Commit {
        /// How many pages it writes, besides those it adds.
        pages: u8,
        /// The database grows by a few pages.
        grow: bool,
    },
    /// Another process begins a read transaction.
    ReaderBegin,
    /// The oldest other process's read transaction ends.
    ReaderEnd,
    /// A checkpoint from outside the engine.
    Checkpoint(OtherProcessMode),
    /// The last connection closes and the WAL is deleted, when no connection, ours
    /// included, holds a transaction.
    DeleteWal,
}

/// What a scenario does to the staging directory, the engine's own memory of
/// where it stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetaOp {
    /// Copies every staged file aside.
    Save,
    /// Replaces the staging directory with the last copy: a backup restored.
    Restore,
    /// Deletes the newest level-0 file.
    DeleteNewest,
    /// Flips one byte in the middle of the newest level-0 file.
    FlipNewestByte,
    /// Cuts the newest level-0 file to half its length.
    TruncateNewest,
    /// Rewrites the newest level-0 file with no WAL origin in its header.
    StripNewestOrigin,
    /// Deletes every staged file while the engine runs: the staging directory
    /// removed under it, with no restart.
    DeleteAll,
}

/// A step on the replica from outside the engine. Scenarios only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RemoteOp {
    /// The replica's store turns out to have no conditional writes, as an S3 provider that
    /// answers `If-None-Match` with `NotImplemented`: from now on every create-only commit is
    /// refused with nothing published (`docs/decisions.md` D79).
    RefuseCreateOnly,
    /// Another writer puts a level-0 file one transaction past the newest:
    /// the newest file's pages under the next transaction.
    PutUnrecognized,
    /// The operator removes the newest level-0 file, which is what
    /// [`RemoteOp::PutUnrecognized`] put when it ran last.
    RemoveUnrecognized,
    /// The bucket rolls back to an earlier version: every file, at every level,
    /// that ends at the newest transaction is gone, and the replica
    /// legitimately loses what the engine had confirmed. Only scenarios and recorded
    /// histories use it: the generators do not draw it (`docs/decisions/D173.md`).
    RollBack,
    /// A bucket copy that keeps no metadata (or a writer before Go v0.5.1)
    /// leaves the newest-ending level 0-8 file without its stored created-at
    /// time: the store now reports the object's last-modified time, the
    /// moment of the copy, which is after the transaction committed.
    DropCreatedAt,
}

/// Damage to the WAL. Each one leaves the database as SQLite's rules now read
/// it, which the harness takes as the database's new truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalOp {
    /// Zeroes the header's page size, salts and checksum.
    TearHeader,
    /// Cuts the file just after this many frames.
    TruncateAtFrame(u32),
    /// A valid WAL of the same length in another generation.
    RewriteSameLength(u64),
    /// The same generation's frames with other bytes, and one more commit.
    RegrowSameSalts(u64),
    /// As [`WalOp::RegrowSameSalts`], with no commit mark left on the old frames.
    RegrowWithoutCommitMarks(u64),
    /// Flips one bit in the page of the last committed frame.
    FlipLastFrameBit,
}

/// How often each kind of operation is drawn, in relative weights.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Weights {
    /// [`Op::Tick`].
    pub tick: u32,
    /// [`Op::Commit`].
    pub commit: u32,
    /// [`Op::CommitInTick`].
    pub commit_in_tick: u32,
    /// [`Op::Spill`].
    pub spill: u32,
    /// [`Op::Rollback`].
    pub rollback: u32,
    /// [`Op::Vacuum`].
    pub vacuum: u32,
    /// [`Op::OtherProcessReader`].
    pub other_process_reader: u32,
    /// [`Op::OtherProcessCheckpoint`].
    pub other_process_checkpoint: u32,
    /// [`Op::Crash`].
    pub crash: u32,
    /// [`Op::StoreFault`].
    pub store_fault: u32,
    /// [`Op::EnvFault`].
    pub env_fault: u32,
    /// [`Op::ClockJump`].
    pub clock_jump: u32,
    /// [`Op::Shutdown`].
    pub shutdown: u32,
    /// [`Op::RequestSnapshot`].
    pub request_snapshot: u32,
    /// [`Op::Restore`].
    pub restore: u32,
    /// [`Op::Shrink`].
    pub shrink: u32,
    /// [`Op::SecondWriter`].
    pub second_writer: u32,
    /// [`Op::OtherConnectionCommit`].
    pub other_connection_commit: u32,
    /// [`Op::Remote`] with [`RemoteOp::DropCreatedAt`].
    pub drop_created_at: u32,
    /// [`Op::WalDamage`].
    pub wal_damage: u32,
    /// [`Op::StagingDamage`], and [`Op::Meta`] with the steps that need no
    /// restart.
    pub staging_damage: u32,
    /// [`Op::Heal`].
    pub heal: u32,
    /// The chance in a thousand that a step is an [`Op::Hold`] instead of
    /// what the history drew there. Not a weight: holds come from a stream of
    /// their own and replace the drawn step, so a run that draws no hold is
    /// the run it was, and every other step of one that does is unchanged.
    pub hold: u32,
    /// Whether a vacuum may change the page size. A replica prefix holds one
    /// page size, so after a resize the engine's next boot stops, or keeps
    /// retrying in a degraded state, on `PageSizeChanged`, and the states
    /// acknowledged before it still restore; the runner expects either.
    pub vacuum_resizes: bool,
    /// Whether a power loss keeps the block law's draw over the WAL writes
    /// since the last fsync (any subset of the dirty blocks, at 512 or 4096
    /// bytes, and a length of its own; [`crate::crash_law`]) instead of a
    /// drawn prefix of them. Not an operation: it changes what every power
    /// loss of the run leaves, so the history's draws are the same either way.
    pub block_crash_law: bool,
    /// Whether the disk lies about the engine's own fsync of the WAL (its
    /// barrier before staging) for the whole run: the sync succeeds and
    /// keeps nothing, while SQLite's own syncs hold. A power loss can then
    /// take frames the engine staged and published. Not an operation, for
    /// the same reason as `block_crash_law`.
    pub lying_wal_sync: bool,
    /// What the app does right after every restart (a crash, a power loss, a
    /// stop), before the new engine's first tick. Not an operation, for the
    /// same reason as `block_crash_law`.
    pub after_restart: AfterRestart,
    /// How the clock moves between reads inside a tick ([`ClockLaw`]): under
    /// [`ClockLaw::EachRead`] every read is later than the one before, and an
    /// app's commit inside a tick lands either right before the sync reads
    /// the committed end or right before a checkpoint's barrier. Not an
    /// operation, for the same reason as `block_crash_law`.
    pub clock_law: ClockLaw,
    /// Whether staging is the real directory store (`lsr-store-fs`) on the in-memory
    /// filesystem ([`crate::staging`]) instead of `MemStore`, so a power loss acts on the
    /// store's own syncs and renames. Not an operation, for the same reason as
    /// `block_crash_law`.
    pub staging_fs: bool,
    /// Whether the replica is the real S3 store (`lsr-store-s3`) over its in-memory bucket and
    /// recorded-fault layer ([`crate::remote`]) instead of `MemStore`. Not an operation, for
    /// the same reason as `block_crash_law`; drawn last in the swarm mix.
    pub remote_s3: bool,
    /// Whether the replica's store has no conditional writes for the whole run, as an S3
    /// provider that answers `If-None-Match` with `NotImplemented`: every create-only commit is
    /// refused (`docs/decisions.md` D79). Not an operation, for the same reason as
    /// `block_crash_law`.
    pub no_create_only: bool,
    /// Store and SQLite-side faults draw their error from the whole error domain
    /// ([`crate::domain`]): every operation, every error an implementation can return from
    /// it. When unset they draw from the older subset (two store errors, two SQLite errors on
    /// nine operations), so recorded histories keep their draws. Not an operation, for the
    /// same reason as `block_crash_law`.
    pub error_domain: bool,
    /// Faults overlap: a hold runs a drawn sub-history (commits and ticks, a process crash, a
    /// power loss, a shutdown, one-shot store faults) instead of committing and ticking, so it
    /// spans restarts and other faults; holds last up to 200 ticks; and the hold kinds that
    /// need it are drawn too (every store operation down at once, a second writer judged while
    /// it stands, the WAL regrowths). Not an operation, for the same reason as
    /// `block_crash_law`; drawn last in the swarm mix.
    pub overlap: bool,
    /// The process dies right after every tick in which an injected fault fired: a store fault,
    /// or a failure of a SQLite-side call. Whatever the fault left the engine owing must then
    /// be found again by the restarted engine, so an obligation held only in memory fails the
    /// run. Not an operation, for the same reason as `block_crash_law`; drawn last in the swarm
    /// mix.
    pub kill_after_fault: bool,
    /// [`Op::AppInTick`].
    pub app_in_tick: u32,
    /// [`Op::Verify`].
    pub verify: u32,
    /// The engine checkpoints TRUNCATE at this many synced frames instead of the simulator's
    /// usual 32, so its TRUNCATE and the snapshot it takes after one (CKPT-4) are reached in
    /// short runs. Never drawn by a mix: the nightly sets it.
    pub truncate_pages: Option<std::num::NonZeroU32>,
    /// [`Op::HttpFault`]: a step that does anything only with the replica on S3.
    pub http_fault: u32,
    /// The bytes of staged files the engine sends in one tick, when not its own 8 MiB (which
    /// no file here reaches): a level-0 file larger than this goes up a slice a tick, and
    /// whatever the history does next lands between two slices (`docs/decisions/D217.md`).
    /// Not an operation, for the same reason as `block_crash_law`; drawn last in the swarm
    /// mix.
    pub slice_bytes: Option<std::num::NonZeroU64>,
}

/// What the application does when it comes back after a restart, before the
/// engine has looked at the WAL. Real applications do not sit still: a job
/// runner replays the work it cannot prove finished, an idempotent migration
/// runs again, a busy service goes straight back to committing. Replayed
/// work writes the same page images again, which is the one way a rewound
/// WAL can regrow frames identical to the ones it lost (see the anchor
/// scenario in the correctness review of the boot proof).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AfterRestart {
    /// It waits for its next drawn operation.
    #[default]
    Nothing,
    /// It replays its last `K` commits, verbatim (the same pages with the
    /// same bytes, at the database size each left), oldest first. `K` is
    /// drawn per restart from 1 to [`REDO_MAX`], so it often reaches back
    /// past the commits the restart lost.
    Redo,
    /// It commits new work until the WAL's committed end passes the end it
    /// had before the restart, so the regrown WAL climbs over every frame
    /// the restart left behind (at most [`BUSY_MAX`] commits).
    Busy,
    /// It replays its last `K` commits as [`AfterRestart::Redo`] does, but
    /// only some of its jobs are idempotent: each replayed commit, drawn
    /// apiece, writes the same pages at the same database size either
    /// verbatim or with a fresh stamp in every page (a job that records when
    /// it ran). The one law here that regrows a lost commit's frames with
    /// other bytes and the next lost commit's frames byte for byte.
    Retry,
}

/// The most commits a [`AfterRestart::Redo`] replays, and the length of the
/// app's own log of commits.
pub const REDO_MAX: u64 = 16;

/// The most commits a [`AfterRestart::Busy`] makes after one restart.
pub const BUSY_MAX: u32 = 64;

impl Default for Weights {
    /// Ticks and commits dominate and faults are rare, so a history makes
    /// progress between them.
    fn default() -> Self {
        Self {
            tick: 40,
            commit: 25,
            commit_in_tick: 3,
            spill: 3,
            rollback: 2,
            vacuum: 1,
            other_process_reader: 4,
            other_process_checkpoint: 3,
            crash: 4,
            store_fault: 3,
            env_fault: 1,
            clock_jump: 2,
            shutdown: 1,
            request_snapshot: 1,
            restore: 2,
            shrink: 2,
            second_writer: 1,
            other_connection_commit: 0,
            // Off in the shared mixes so their recorded histories keep their
            // shape; `tests/sim/dropped_created_at.rs` turns it on.
            drop_created_at: 0,
            wal_damage: 1,
            staging_damage: 1,
            heal: 1,
            // Off in the default mix so its recorded histories keep their
            // shape; swarm mixes draw it.
            // Off in the default mix so its recorded histories keep their
            // shape; swarm mixes draw it, and `tests/sim/survival.rs` turns
            // it on.
            hold: 0,
            vacuum_resizes: true,
            block_crash_law: false,
            lying_wal_sync: false,
            after_restart: AfterRestart::Nothing,
            // One reading per tick in the default mix, so its recorded runs
            // keep their shape; swarm mixes draw the law.
            clock_law: ClockLaw::PerTick,
            staging_fs: false,
            remote_s3: false,
            no_create_only: false,
            error_domain: false,
            overlap: false,
            // Off in the default mix so its recorded histories keep their
            // shape; swarm mixes draw it.
            kill_after_fault: false,
            // Off in the default mix so its recorded histories keep their
            // shape; swarm mixes draw it.
            app_in_tick: 0,
            // Off in the default mix so its recorded histories keep their
            // shape; swarm mixes draw it.
            verify: 0,
            truncate_pages: None,
            http_fault: 0,
            // Off in the default mix so its recorded histories keep their
            // shape; swarm mixes draw it.
            slice_bytes: None,
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Tick,
    Commit,
    CommitInTick,
    Spill,
    Rollback,
    Vacuum,
    OtherProcessReader,
    OtherProcessCheckpoint,
    Crash,
    StoreFault,
    EnvFault,
    ClockJump,
    Shutdown,
    RequestSnapshot,
    Restore,
    Shrink,
    SecondWriter,
    DropCreatedAt,
    WalDamage,
    StagingDamage,
    Heal,
    AppInTick,
    Verify,
    OtherConnectionCommit,
    HttpFault,
}

impl Weights {
    fn table(&self) -> [(Kind, u32); 25] {
        [
            (Kind::Tick, self.tick),
            (Kind::Commit, self.commit),
            (Kind::CommitInTick, self.commit_in_tick),
            (Kind::Spill, self.spill),
            (Kind::Rollback, self.rollback),
            (Kind::Vacuum, self.vacuum),
            (Kind::OtherProcessReader, self.other_process_reader),
            (Kind::OtherProcessCheckpoint, self.other_process_checkpoint),
            (Kind::Crash, self.crash),
            (Kind::StoreFault, self.store_fault),
            (Kind::EnvFault, self.env_fault),
            (Kind::ClockJump, self.clock_jump),
            (Kind::Shutdown, self.shutdown),
            (Kind::RequestSnapshot, self.request_snapshot),
            (Kind::Restore, self.restore),
            (Kind::Shrink, self.shrink),
            (Kind::SecondWriter, self.second_writer),
            (Kind::DropCreatedAt, self.drop_created_at),
            (Kind::WalDamage, self.wal_damage),
            (Kind::StagingDamage, self.staging_damage),
            (Kind::Heal, self.heal),
            (Kind::AppInTick, self.app_in_tick),
            (Kind::Verify, self.verify),
            (Kind::OtherConnectionCommit, self.other_connection_commit),
            (Kind::HttpFault, self.http_fault),
        ]
    }

    fn total(&self) -> u64 {
        self.table()
            .iter()
            .map(|(_, w)| u64::from(*w))
            .sum::<u64>()
            .max(1)
    }

    fn pick(&self, draw: u64) -> Kind {
        let mut left = draw;
        for (kind, weight) in self.table() {
            let weight = u64::from(weight);
            if left < weight {
                return kind;
            }
            left = left.saturating_sub(weight);
        }
        Kind::Tick
    }
}

impl Weights {
    /// A mix of its own for `seed` (swarm testing): about a third of the
    /// kinds besides ticks and commits are left out, the rest weigh 1 to
    /// 100, and a vacuum may or may not resize. One fixed mix suppresses
    /// states: interleaved crashes keep compaction shallow, a page-size
    /// change leaves the second half of most histories degraded, faults
    /// rarely overlap. Drawn from the seed's own stream, so the history's
    /// stream is untouched.
    #[must_use]
    pub fn swarm(seed: Seed) -> Self {
        let entropy = SeededEntropy::new(seed.stream(12));
        let weight = |keep_always: bool| {
            let left_out = !keep_always && below(&entropy, 3) == 0;
            let drawn = u32::try_from(below(&entropy, 100).saturating_add(1)).unwrap_or(1);
            if left_out { 0 } else { drawn }
        };
        let mut mix = Self {
            tick: weight(true),
            commit: weight(true),
            commit_in_tick: weight(false),
            spill: weight(false),
            rollback: weight(false),
            vacuum: weight(false),
            other_process_reader: weight(false),
            other_process_checkpoint: weight(false),
            crash: weight(false),
            store_fault: weight(false),
            env_fault: weight(false),
            clock_jump: weight(false),
            shutdown: weight(false),
            request_snapshot: weight(false),
            restore: weight(false),
            shrink: weight(false),
            second_writer: 0,
            other_connection_commit: 0,
            drop_created_at: 0,
            wal_damage: weight(false),
            staging_damage: weight(false),
            heal: weight(false),
            hold: 0,
            vacuum_resizes: below(&entropy, 2) == 0,
            block_crash_law: false,
            lying_wal_sync: false,
            after_restart: AfterRestart::Nothing,
            clock_law: ClockLaw::PerTick,
            staging_fs: false,
            remote_s3: false,
            no_create_only: false,
            error_domain: false,
            overlap: false,
            kill_after_fault: false,
            app_in_tick: 0,
            verify: 0,
            truncate_pages: None,
            http_fault: 0,
            slice_bytes: None,
        };
        // Drawn last, so every other kind keeps the weight it had before
        // second writers were drawn.
        mix.second_writer = weight(false);
        // Drawn after that, for the same reason.
        mix.block_crash_law = below(&entropy, 2) == 0;
        // And after that, for the same reason.
        // Drawn after that, for the same reason: a third each.
        mix.after_restart = match below(&entropy, 3) {
            0 => AfterRestart::Nothing,
            1 => AfterRestart::Redo,
            _ => AfterRestart::Busy,
        };
        // Drawn after that, for the same reason: half the redoing apps retry
        // jobs that are not all idempotent.
        if mix.after_restart == AfterRestart::Redo && below(&entropy, 2) == 0 {
            mix.after_restart = AfterRestart::Retry;
        }
        // Drawn after that, for the same reason: a disk in four lies.
        mix.lying_wal_sync = below(&entropy, 4) == 0;
        // And after that, for the same reason: a rate of 1 to 30 in a
        // thousand steps, left out a third of the time.
        mix.hold = if below(&entropy, 3) == 0 {
            0
        } else {
            u32::try_from(below(&entropy, 30).saturating_add(1)).unwrap_or(1)
        };
        // Drawn after that, for the same reason: half each.
        mix.clock_law = if below(&entropy, 2) == 0 {
            ClockLaw::PerTick
        } else {
            ClockLaw::EachRead
        };
        // And after that, for the same reason: staging on the real directory store in half the
        // swarm.
        mix.staging_fs = below(&entropy, 2) == 0;
        // Drawn after that, for the same reason: half each.
        mix.error_domain = below(&entropy, 2) == 0;
        // Drawn after that, for the same reason: half each.
        mix.overlap = below(&entropy, 2) == 0;
        // And after that, for the same reason.
        mix.app_in_tick = weight(false);
        // Drawn after that, for the same reason.
        mix.verify = weight(false);
        // And after that, for the same reason: a second application connection that commits,
        // with its own checkpoint count (a WAL header written again with the same salts).
        mix.other_connection_commit = weight(false);
        // Drawn after that, for the same reason: half each.
        mix.kill_after_fault = below(&entropy, 2) == 0;
        // And after that, for the same reason: the replica on the real S3 store in half the
        // swarm, and provider faults on its requests.
        mix.remote_s3 = below(&entropy, 2) == 0;
        mix.http_fault = weight(false);
        // And after that: a store without conditional writes in a quarter of the swarm.
        mix.no_create_only = below(&entropy, 4) == 0;
        // And after that: half the swarm sends staged files a slice a tick, of 1 KiB or 4 KiB
        // with even odds (a sync step stages up to 16 KiB and a transaction).
        mix.slice_bytes = (below(&entropy, 2) == 0)
            .then(|| if below(&entropy, 2) == 0 { 1024 } else { 4096 })
            .and_then(std::num::NonZeroU64::new);
        mix
    }

    /// The nightly's settings on top of this mix: the laws no mix draws, from the seed's own
    /// stream (so a replay line with `LSR_SIM_NIGHTLY_LAWS` set gets them too): with even odds, a TRUNCATE threshold of 2 to 8 frames, with other processes'
    /// readers drawn at least at weight 10. The engine truncates only after a PASSIVE at the
    /// threshold that a reader held off, so the readers are what make the low threshold reach
    /// its TRUNCATE: over seeds 1 to 300 of the mixed set, the engine's TRUNCATE ran in 8 seeds
    /// as drawn, 25 at a threshold of 4, and 36 at a threshold of 2 with the readers.
    #[must_use]
    pub fn with_nightly_laws(mut self, seed: Seed) -> Self {
        let entropy = SeededEntropy::new(seed.stream(25));
        let frames = u32::try_from(below(&entropy, 7).saturating_add(2)).unwrap_or(2);
        self.truncate_pages = (below(&entropy, 2) == 0)
            .then_some(frames)
            .and_then(std::num::NonZeroU32::new);
        if self.truncate_pages.is_some() {
            self.other_process_reader = self.other_process_reader.max(10);
        }
        self
    }

    /// What the rotating seed sets run: the default mix for half the seeds
    /// and a swarm mix for the other half, chosen by the seed.
    #[must_use]
    pub fn mixed(seed: Seed) -> Self {
        if seed.stream(13).is_multiple_of(2) {
            Self::default()
        } else {
            Self::swarm(seed)
        }
    }
}

fn below(entropy: &SeededEntropy, upper: u64) -> u64 {
    entropy.below(upper.max(1))
}

fn small(entropy: &SeededEntropy, upper: u64) -> u8 {
    u8::try_from(below(entropy, upper)).unwrap_or(u8::MAX)
}

fn http_fault(entropy: &SeededEntropy) -> HttpFault {
    let shape = choose(
        entropy,
        &[
            HttpShape::List,
            HttpShape::Head,
            HttpShape::Get,
            HttpShape::Put,
            HttpShape::DeleteObjects,
            HttpShape::ListUploads,
        ],
    )
    .unwrap_or(HttpShape::Put);
    let make = choose(
        entropy,
        &[
            HttpFault::SlowDown,
            HttpFault::TooManyRequests,
            HttpFault::Internal,
            HttpFault::RequestTimeout,
            HttpFault::AfterPublish,
            HttpFault::TruncateBody,
            HttpFault::WrongEtag,
            HttpFault::ConnectionReset,
        ],
    )
    .unwrap_or(HttpFault::Internal);
    match below(entropy, 4) {
        // A refused DeleteObjects comes from `DenyDeletes`: a whole one refused may be the
        // store's own best-effort cleanup, whose failure it rightly does not report.
        0 if shape != HttpShape::DeleteObjects => HttpFault::Denied(shape, denial(entropy)),
        1 if below(entropy, 4) == 0 => HttpFault::DenyDeletes,
        _ => make(shape),
    }
}

fn denial(entropy: &SeededEntropy) -> DenialCode {
    choose(
        entropy,
        &[
            DenialCode::AccessDenied,
            DenialCode::AllAccessDisabled,
            DenialCode::InvalidAccessKeyId,
            DenialCode::SignatureDoesNotMatch,
            DenialCode::ExpiredToken,
            DenialCode::Unauthorized,
            DenialCode::RequestTimeTooSkewed,
            DenialCode::NoSuchBucket,
            DenialCode::KmsDisabled,
            DenialCode::KmsNotFound,
        ],
    )
    .unwrap_or(DenialCode::AccessDenied)
}

fn choose<T: Copy>(entropy: &SeededEntropy, from: &[T]) -> Option<T> {
    let at = usize::try_from(below(entropy, u64::try_from(from.len()).unwrap_or(1))).ok()?;
    from.get(at).copied()
}

fn store_error(entropy: &SeededEntropy, op: StoreOp) -> StoreError {
    if below(entropy, 3) == 0 {
        StoreError::Throttled {
            op,
            retry_after: (below(entropy, 2) == 0).then_some(Duration::from_secs(2)),
        }
    } else {
        StoreError::Transient {
            op,
            kind: ErrorKind::ConnectionReset,
        }
    }
}

fn store_fault(entropy: &SeededEntropy, weights: &Weights) -> Op {
    let target = if below(entropy, 2) == 0 {
        Target::Staging
    } else {
        Target::Remote
    };
    let op = choose(
        entropy,
        &[
            StoreOp::List,
            StoreOp::Read,
            StoreOp::Upload,
            StoreOp::Upload,
            StoreOp::Delete,
        ],
    )
    .unwrap_or(StoreOp::Upload);
    let error = if weights.error_domain {
        crate::domain::draw_store(entropy, target, op)
    } else {
        crate::domain::possible_store(target, op, store_error(entropy, op))
    };
    Op::StoreFault {
        target,
        op,
        ahead: u16::from(small(entropy, 4)),
        shape: shape(entropy, op, error),
    }
}

/// A fault shape that means something for `op`: the commit shapes for an
/// upload, a flipped byte for a read, and a rate (between 1% and 10%, until
/// an [`Op::Heal`]) or a plain error for any.
fn shape(entropy: &SeededEntropy, op: StoreOp, error: StoreError) -> FaultShape {
    let rate = |entropy: &SeededEntropy, err| FaultShape::Rate {
        per_mille: u16::try_from(below(entropy, 91).saturating_add(10)).unwrap_or(10),
        err,
    };
    match (op, below(entropy, 8)) {
        (StoreOp::Upload, 0) if !crate::domain::refused_before_acting(&error) => {
            FaultShape::ErrorAfterPublish(error)
        }
        (StoreOp::Upload, 1) => FaultShape::SizeMismatch,
        (StoreOp::Upload, 2) => FaultShape::ChecksumMismatch,
        (StoreOp::Upload, 3) => FaultShape::CorruptPublish,
        (StoreOp::Read, 0 | 1) => FaultShape::FlipByteOnRead {
            at: below(entropy, 4096),
        },
        (_, 7) => rate(entropy, error),
        (StoreOp::List | StoreOp::Read | StoreOp::Upload | StoreOp::Delete, _) => {
            FaultShape::Error(error)
        }
    }
}

/// Damage to the WAL. [`WalOp::RewriteSameLength`] is left out: it leaves the
/// wal-index naming a generation the file no longer holds, which SQLite's
/// next open would recover and the rules never do, so the engine would wait
/// for ever on a state no real database stays in.
fn wal_damage(entropy: &SeededEntropy) -> Op {
    let seed = below(entropy, u64::MAX);
    let op = match below(entropy, 5) {
        0 => WalOp::TearHeader,
        1 => WalOp::TruncateAtFrame(u32::try_from(below(entropy, 40)).unwrap_or(0)),
        2 => WalOp::RegrowSameSalts(seed),
        3 => WalOp::RegrowWithoutCommitMarks(seed),
        _ => WalOp::FlipLastFrameBit,
    };
    Op::WalDamage {
        op,
        ticks: small(entropy, 8),
    }
}

/// Damage to staged files. A copy saved and put back later is an operator's act
/// (`docs/decisions/D173.md`) and is drawn by no generator; the scenarios have it.
fn staging_damage(entropy: &SeededEntropy) -> Op {
    match below(entropy, 6) {
        0 => Op::Meta(MetaOp::DeleteAll),
        1 => Op::StagingDamage(MetaOp::DeleteNewest),
        2 => Op::StagingDamage(MetaOp::FlipNewestByte),
        3 => Op::StagingDamage(MetaOp::TruncateNewest),
        4 => Op::StagingDamage(MetaOp::StripNewestOrigin),
        _ => Op::Meta(MetaOp::FlipNewestByte),
    }
}

/// A lasting fault, held for up to 11 ticks, or up to 200 with `overlap`, when a third of the
/// holds are of the kinds only overlapping faults draw.
fn hold(entropy: &SeededEntropy, overlap: bool) -> Op {
    let hold = match below(entropy, 7) {
        0 => Hold::StoreDown(
            choose(
                entropy,
                &[
                    StoreOp::List,
                    StoreOp::Read,
                    StoreOp::Upload,
                    StoreOp::Delete,
                ],
            )
            .unwrap_or(StoreOp::Upload),
        ),
        1 => Hold::CredentialsRevoked,
        2 => Hold::DiskFull,
        3 => Hold::LyingReads,
        4 => Hold::ReadersHeld,
        5 => match below(entropy, 3) {
            0 => Hold::Wal(WalOp::TearHeader),
            1 => Hold::Wal(WalOp::TruncateAtFrame(
                u32::try_from(below(entropy, 4)).unwrap_or(0),
            )),
            _ => Hold::Wal(WalOp::FlipLastFrameBit),
        },
        _ => Hold::LyingDisk,
    };
    let hold = if overlap && below(entropy, 3) == 0 {
        let seed = below(entropy, u64::MAX);
        match below(entropy, 4) {
            0 => Hold::StoreDownAll,
            1 => Hold::SecondWriter,
            2 => Hold::Wal(WalOp::RegrowSameSalts(seed)),
            _ => Hold::Wal(WalOp::RegrowWithoutCommitMarks(seed)),
        }
    } else {
        hold
    };
    // A lying disk loses power in the middle of its hold, so it keeps the short length; the
    // draw is made either way, so the history keeps its shape.
    let long = overlap
        .then(|| u8::try_from(below(entropy, 199).saturating_add(2)).unwrap_or(u8::MAX))
        .filter(|_| hold != Hold::LyingDisk);
    let ticks = long.unwrap_or_else(|| small(entropy, 10).saturating_add(2));
    Op::Hold { hold, ticks }
}

fn env_fault(entropy: &SeededEntropy, weights: &Weights) -> Op {
    let fault = match below(entropy, 4) {
        0 => EnvFault::DeleteWal,
        1 => EnvFault::WipeStaging,
        _ if weights.error_domain => match crate::domain::draw_source(entropy) {
            crate::domain::SourceDraw::Inject(op, err) => EnvFault::Source(op, err),
            crate::domain::SourceDraw::World(fault) => fault,
        },
        _ => {
            let op = choose(
                entropy,
                &[
                    SourceOp::Open,
                    SourceOp::Len,
                    SourceOp::Committed,
                    SourceOp::PageCount,
                    SourceOp::ReadPage,
                    SourceOp::BeginBarrier,
                    SourceOp::Checkpoint,
                    SourceOp::BumpSeq,
                ],
            )
            .unwrap_or(SourceOp::Len);
            let err = if below(entropy, 2) == 0 {
                SourceError::Busy
            } else {
                SourceError::Io(ErrorKind::Other)
            };
            EnvFault::Source(op, crate::domain::possible_source(op, err))
        }
    };
    Op::EnvFault(fault)
}

fn clock_jump(entropy: &SeededEntropy) -> Op {
    let millis = match below(entropy, 10) {
        0 => 0_i64
            .saturating_sub(i64::try_from(below(entropy, 60_000).saturating_add(1)).unwrap_or(1)),
        1 => 93_600_000,
        _ => i64::try_from(below(entropy, 120_000).saturating_add(1)).unwrap_or(1),
    };
    Op::ClockJump(millis)
}

fn vacuum(entropy: &SeededEntropy, resizes: bool) -> Op {
    // The draw happens either way so a history keeps its shape across the flag.
    let page_size = match below(entropy, 3) {
        0 if resizes => PageSize::new(1024),
        1 if resizes => PageSize::new(2048),
        _ => None,
    };
    Op::Vacuum { page_size }
}

fn draw(weights: &Weights, entropy: &SeededEntropy) -> Op {
    match weights.pick(below(entropy, weights.total())) {
        Kind::Tick => Op::Tick,
        Kind::Commit => Op::Commit {
            pages: small(entropy, 4).saturating_add(1),
            grow: below(entropy, 6) == 0,
        },
        Kind::CommitInTick => Op::CommitInTick {
            pages: small(entropy, 4).saturating_add(1),
        },
        Kind::Spill => Op::Spill,
        Kind::Rollback => Op::Rollback,
        Kind::OtherConnectionCommit => Op::OtherConnectionCommit {
            pages: small(entropy, 4).saturating_add(1),
        },
        Kind::HttpFault => Op::HttpFault(http_fault(entropy)),
        Kind::Vacuum => vacuum(entropy, weights.vacuum_resizes),
        Kind::OtherProcessReader => Op::OtherProcessReader {
            begin: below(entropy, 2) == 0,
        },
        Kind::OtherProcessCheckpoint => Op::OtherProcessCheckpoint(
            choose(
                entropy,
                &[
                    OtherProcessMode::Passive,
                    OtherProcessMode::Restart,
                    OtherProcessMode::Truncate,
                ],
            )
            .unwrap_or(OtherProcessMode::Passive),
        ),
        Kind::Crash => Op::Crash(if below(entropy, 4) == 0 {
            CrashKind::Power
        } else {
            CrashKind::Process
        }),
        Kind::StoreFault => store_fault(entropy, weights),
        Kind::EnvFault => env_fault(entropy, weights),
        Kind::ClockJump => clock_jump(entropy),
        Kind::Shutdown => Op::Shutdown,
        Kind::RequestSnapshot => Op::RequestSnapshot,
        Kind::Restore => Op::Restore,
        Kind::Shrink => Op::Shrink {
            pages: small(entropy, 3).saturating_add(1),
        },
        Kind::SecondWriter => Op::SecondWriter {
            ticks: small(entropy, 8),
        },
        Kind::DropCreatedAt => Op::Remote(RemoteOp::DropCreatedAt),
        Kind::WalDamage => wal_damage(entropy),
        Kind::StagingDamage => staging_damage(entropy),
        Kind::Heal => Op::Heal,
        Kind::AppInTick => app_in_tick(entropy),
        Kind::Verify => Op::Verify,
    }
}

/// An [`Op::AppInTick`]: at one of the tick's first 24 calls into SQLite or before its
/// first call of a kind, half each, a commit (a sixth of them growing), a reader
/// beginning or ending, a checkpoint of each mode, or a deleted WAL.
fn app_in_tick(entropy: &SeededEntropy) -> Op {
    let at = if below(entropy, 2) == 0 {
        AppAt::Call(small(entropy, 24))
    } else {
        AppAt::Before(choose(entropy, &SOURCE_OPS).unwrap_or(SourceOp::Committed))
    };
    let op = match below(entropy, 7) {
        0 | 1 => AppOp::Commit {
            pages: small(entropy, 4).saturating_add(1),
            grow: below(entropy, 6) == 0,
        },
        2 => AppOp::ReaderBegin,
        3 => AppOp::ReaderEnd,
        4 => AppOp::Checkpoint(
            choose(
                entropy,
                &[
                    OtherProcessMode::Passive,
                    OtherProcessMode::Restart,
                    OtherProcessMode::Truncate,
                ],
            )
            .unwrap_or(OtherProcessMode::Passive),
        ),
        5 => AppOp::Checkpoint(OtherProcessMode::Truncate),
        _ => AppOp::DeleteWal,
    };
    Op::AppInTick { at, op }
}

/// Every call the engine makes into SQLite, by kind.
const SOURCE_OPS: [SourceOp; 10] = [
    SourceOp::Open,
    SourceOp::Len,
    SourceOp::Committed,
    SourceOp::Sync,
    SourceOp::PageCount,
    SourceOp::ReadPage,
    SourceOp::BeginBarrier,
    SourceOp::EndBarrier,
    SourceOp::Checkpoint,
    SourceOp::BumpSeq,
];

/// The history of `steps` operations that `seed` names.
///
/// Each step draws its operation from the history's stream, and then, from a
/// stream of its own, whether a hold replaces it ([`Weights::hold`]). Both
/// draws happen at every step, so the steps a hold does not replace are the
/// steps a mix with no holds draws.
#[must_use]
pub fn history(seed: Seed, steps: u32, weights: &Weights) -> Vec<Op> {
    let entropy = SeededEntropy::new(seed.stream(10));
    let holds = SeededEntropy::new(seed.stream(41));
    (0..steps)
        .map(|_| {
            let drawn = draw(weights, &entropy);
            if weights.hold > 0 && below(&holds, 1000) < u64::from(weights.hold) {
                hold(&holds, weights.overlap)
            } else {
                drawn
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;
