//! The local course: sync steps and checkpoints from local state while the replica cannot
//! be listed (`docs/decisions/D220.md`).
//!
//! A boot needs the replica's listing before it decides anything, so a process that starts
//! during a store outage had no position: it staged nothing and checkpointed nothing, and the
//! WAL grew for as long as the outage lasted. Go's database sync never asks the store, so Go
//! stages and checkpoints through the same outage.
//!
//! Once boot attempts have failed for [`super::lag::BOOT_UNDECIDED_BOUND`] without the listing
//! answering (the moment the application's writes are released, `docs/decisions/D207.md`), the
//! engine reads every staged file once and the WAL. When they agree ([`Agreed`]: every staged
//! file reads whole, each starts where the one before it ended, and the WAL continues from the
//! newest), it takes the position they prove and runs the WAL side of a booted engine from
//! it: sync steps stage files, checkpoints run on their proofs. It is still not booted:
//!
//! - Nothing is uploaded, pruned or compacted. Those units run only in a booted engine
//!   ([`Course::booted`]), and the upload state stays unknown.
//! - Boot attempts go on, paced by the upload task, whose state reports the store's fault;
//!   the sync task is the local steps'. The boot that lists the replica judges it exactly as
//!   a boot from nothing does and assigns a whole new engine with its own witness tokens, so
//!   no token of this course outlives it.
//! - A step that says the WAL no longer continues staging ends the course: only a boot
//!   numbers a snapshot. So does a boot attempt that deleted staged files before it failed.
//!
//! The try is made once per process ([`LocalTry`]): it reads every staged file.
//!
//! # Where the course's files are kept
//!
//! Every level-0 file a boot finds in staging is taken to be above the replica's newest
//! file, or to be this engine's own copy of it: files are staged only after a boot listed the
//! replica. An upload leans on that when the replica's newest file does not read: it takes
//! the object for its own, cut short, and writes the staged file over it. A course stages
//! with no listing. When staging was behind the replica as the course began (its newest files
//! deleted, an old copy of the directory put back), its files number transactions the
//! replica already holds, with other content, and nothing in the files says so.
//!
//! So where a file is kept says what made it. A course writes its files in a slot of their
//! own ([`Level::LOCAL_COURSE`], which staging uses for nothing else) and sees staging
//! through [`Apart`]: level 0 through the newest file there as it began (its base), and
//! above that the slot. No boot, upload or prune reads the slot, in this process or a later
//! one. The boot that lists the replica settles it first (`Actor::boot_listed`): with the
//! replica's newest transaction at or below the newest level-0 file, every file in the slot is
//! above the replica, and is moved up to level 0 to be judged as any pending file is; with
//! the replica past it, the slot's files are deleted, and the boot decides as if no course
//! had run. On disk the slot is the directory `ltx/8` in the meta directory, beside the
//! `ltx/0` Go and every build of this engine stage in. Neither Go nor an older build reads
//! it, so one that runs after a course stages over the same transactions at level 0; the
//! settling boot sees level 0 past the slot's oldest file and deletes the slot.

use lsr_ltx::{Timestamp, Txid, TxidRange};

use super::apart::{Apart, raise_course};
use super::parts::{Course, Engine, Stop, Tail};
use super::{
    Actor, BootStep, Env, Listed, Takeover, Task, Tick, boot_listing, boot_wal_of, open_reader,
    wal_view,
};
use crate::checkpoint::CheckpointState;
use crate::fault::role::Role;
use crate::fault::{Fault, Next};
use crate::inventory::Inventory;
use crate::layout::{FileKey, Level};
use crate::position::Position;
use crate::recover::{
    NewestStagedFile, PositionSource, Recovered, newest_level_zero, recover_traced,
};
use crate::source::{Checkpointer, PageSource, WalSource};
use crate::store::{PageLimit, exists};
use crate::upload::{Unsent, drop_staged_from, inventory_fault};
use crate::witness::{Base, Witnessed, scan};

/// Whether this process may still try the local course.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LocalTry {
    /// Not judged yet.
    Open,
    /// Staging and the WAL were judged once, whatever the answer.
    Spent,
}

/// `env` with staging as a local course sees it.
fn lend<'e, W, P, C, L, R>(
    env: &'e mut Env<'_, W, P, C, L, R>,
    staging: &'e Apart<'e, L>,
) -> Env<'e, W, P, C, Apart<'e, L>, R> {
    Env {
        wal: &mut *env.wal,
        db: &mut *env.db,
        ck: &mut *env.ck,
        staging,
        remote: env.remote,
        clock: env.clock,
        notice: &mut *env.notice,
    }
}

/// Proof that staging and the WAL agree, as of the reads that made it: every staged level-0
/// file read whole, each after the oldest starts where the one before it ended (or at a
/// generation's first frame), and the WAL holds the newest file's range with the same pages
/// and continues from its end. Only [`agree`] makes one.
struct Agreed {
    position: Position,
    /// A token for every staged file after the oldest: they count toward the cap on staged
    /// files, and none of them is ever shown to an upload.
    tokens: Vec<Witnessed>,
}

/// Where a local course stands in the WAL. Made only from an [`Agreed`], and moved only by
/// its own steps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LocalAt {
    position: Position,
    tail: Tail,
    /// The newest level-0 file under the course's own ([`Apart`]).
    base: Option<Txid>,
}

impl LocalAt {
    pub(super) const fn position(&self) -> Position {
        self.position
    }

    pub(super) const fn tail(&self) -> Tail {
        self.tail
    }

    /// The course after one of its steps left it at `position`.
    pub(super) const fn moved(self, position: Position, tail: Tail) -> Self {
        Self {
            position,
            tail,
            base: self.base,
        }
    }

    /// The course after a boot moved its files up to level 0: the file it stands at is the
    /// newest there.
    const fn raised(self) -> Self {
        Self {
            base: Some(self.position.txid()),
            ..self
        }
    }
}

impl Course {
    /// The local course `agreed` proves, over level 0 through `base`.
    const fn local(agreed: &Agreed, base: Option<Txid>) -> Self {
        Self::Local(LocalAt {
            position: agreed.position,
            tail: Tail::Synced,
            base,
        })
    }
}

/// Reads every staged level-0 file once, and the WAL, and says whether they agree. `None`
/// when they do not, or staging is empty.
///
/// # Errors
/// A read of staging or of the WAL that failed, which judges nothing.
async fn agree<W: WalSource, L: Role, R: Role>(
    wal: &mut W,
    staging: &L,
) -> Result<Option<Agreed>, Fault> {
    // The length first, as the boot reads it (`boot_judge`).
    let len = wal.len().map_err(Fault::from_source)?;
    let committed = wal.committed().map_err(Fault::from_source)?;
    let mut reader = open_reader(wal)?;
    let end = committed.end();
    let found = scan(
        staging,
        Base::<R>::Oldest,
        boot_wal_of(reader.as_mut(), end),
    )
    .await?;
    if found.bad.is_some() {
        return Ok(None);
    }
    let within = boot_wal_of(reader.as_mut(), end);
    let newest = NewestStagedFile::load_in(staging, within, &found.scanned).await?;
    if !matches!(newest, Some(NewestStagedFile::Readable { .. })) {
        return Ok(None);
    }
    let view = wal_view(len, reader.as_ref(), committed);
    // No replica: the verdict rests on the staged file and the WAL alone.
    let verdict = recover_traced(newest.as_ref().map(PositionSource::Boot), &view, None);
    Ok(match verdict.recovered {
        Recovered::Continue(position) => Some(Agreed {
            position,
            tokens: found.tokens,
        }),
        Recovered::WalRestarted(_) | Recovered::Snapshot { .. } | Recovered::Stop(_) => None,
    })
}

impl Actor {
    /// The task whose wait paces boot attempts and whose state reports their failures: the
    /// sync task, or the upload task while a local course keeps the sync task for its steps.
    pub(super) const fn boot_task(&self) -> Task {
        match self.engine.course {
            Course::Local(_) => Task::Upload,
            Course::Unbooted | Course::Snapshot { .. } | Course::At { .. } => Task::Sync,
        }
    }

    /// Whether checkpoints may run: the engine syncs (it booted, or is on a local course)
    /// and no drain is on.
    pub(super) fn checkpoints(&self) -> bool {
        let syncs = match self.engine.course {
            Course::Unbooted => false,
            Course::Local(_) | Course::Snapshot { .. } | Course::At { .. } => true,
        };
        syncs && self.kept.stop == Stop::No
    }

    /// What a local course does on a tick that is not a boot attempt's: a sync step and the
    /// checkpoint it allows. `None` when there is no local course, or it has nothing to do.
    pub(super) async fn local_unit<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        now: Timestamp,
    ) -> Option<Tick>
    where
        W: WalSource,
        P: PageSource,
        C: Checkpointer,
        L: Role,
        R: Role,
    {
        match self.engine.course {
            Course::Local(at) => {
                let staging = Apart::over(env.staging, at.base);
                let mut env = lend(env, &staging);
                self.wal_unit(&mut env, now, self.checkpoints()).await
            }
            Course::Unbooted | Course::Snapshot { .. } | Course::At { .. } => None,
        }
    }

    /// `tick`, the end of a boot attempt that failed before the replica's listing answered.
    /// When such failures have lasted the bound, and this process has not tried before, the
    /// engine begins the local course if staging and the WAL agree: boot attempts move to
    /// the upload task with their run of failures, and the sync task starts clean.
    pub(super) async fn or_local<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        tick: Tick,
        now: Timestamp,
    ) -> Tick
    where
        W: WalSource,
        L: Role,
        R: Role,
    {
        let due = matches!(self.engine.course, Course::Unbooted)
            && self.kept.local == LocalTry::Open
            && self.kept.stop == Stop::No
            && self.boot_overdue_at().is_some_and(|at| now >= at);
        let (event, next) = match tick {
            Tick::Ran { event, next } if due => (event, next),
            other @ (Tick::Ran { .. }
            | Tick::Stopped(_)
            | Tick::Drained { .. }
            | Tick::DeadlineReached(_)) => return other,
        };
        // A read that failed judges nothing: the next failed attempt asks again.
        let Ok(base) = newest_level_zero(env.staging).await else {
            return Tick::Ran { event, next };
        };
        let base = base.map(|info| info.key.range().max());
        // With the slot's files: a course an earlier process began goes on from them.
        let staging = Apart::over(env.staging, base);
        let Ok(agreed) = agree::<W, _, R>(env.wal, &staging).await else {
            return Tick::Ran { event, next };
        };
        self.kept.local = LocalTry::Spent;
        let Some(agreed) = agreed else {
            return Tick::Ran { event, next };
        };
        self.engine = Engine {
            course: Course::local(&agreed, base),
            checkpoint: CheckpointState::boot(now),
            witnesses: agreed.tokens.into_iter().collect(),
            ..Engine::fresh()
        };
        self.kept.tasks.hand(Task::Sync, Task::Upload);
        // The WAL may hold frames past the position already.
        Tick::Ran {
            event,
            next: Next::Now,
        }
    }

    /// The boot's listing ([`boot_listing`]), with the files a local course left in its slot
    /// settled before anything is made of it (the module docs say why they are kept apart).
    /// The slot is looked at on every boot whose listing answers, whatever this process
    /// knows: the course may have been an earlier process's. The listing stands whatever
    /// became of the slot: what it decided is about keys the replica holds, and files that
    /// moved up are all above those.
    pub(super) async fn boot_listed<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        takeover: Takeover,
        unsent: &Unsent<'_>,
    ) -> Result<Listed, BootStep>
    where
        L: Role,
        R: Role,
    {
        let denied = &mut self.kept.scratch_sweep_denied;
        let listed = boot_listing(env.staging, env.remote, denied, takeover).await?;
        self.settle_course(env.staging, env.remote, unsent).await?;
        Ok(listed)
    }

    /// Settles the files in the course's slot, if it holds any, against the replica as
    /// listed now. The replica's newest transaction, at any level, at or below the newest
    /// level-0 staged file: every file in the slot is above the replica, as a file staged
    /// after a boot is, and moves up to level 0. Past it: the slot's files number
    /// transactions the replica holds, so they are deleted and a course in progress ends.
    ///
    /// They are deleted too when level 0 ends past the slot's oldest file. A course writes
    /// above level 0 and a move copies the oldest first, so neither leaves that: a build that
    /// does not know the slot (an older one, or Go) staged from where the course began, and
    /// moving the slot up would put the course's files over its files.
    ///
    /// Called once the boot's own listing answered, so a boot that cannot reach the replica
    /// leaves staging as it found it.
    pub(super) async fn settle_course<L: Role, R: Role>(
        &mut self,
        staging: &L,
        remote: &R,
        unsent: &Unsent<'_>,
    ) -> Result<(), Fault> {
        let slot = staging
            .list(Level::LOCAL_COURSE, None, PageLimit::ONE)
            .await
            .map_err(|err| L::fault(&err))?;
        let Some(oldest) = slot.files.first().map(|info| info.key.range().max()) else {
            return Ok(());
        };
        let base = newest_level_zero(staging)
            .await?
            .map(|info| info.key.range().max());
        let held = Inventory::load(remote)
            .await
            .map_err(|err| inventory_fault::<R>(&err))?
            .newest();
        if held > base || base > Some(oldest) {
            drop_staged_from(&Apart::over(staging, None), Txid::MIN, unsent).await?;
            if matches!(self.engine.course, Course::Local(_)) {
                self.leave_local();
            }
            return Ok(());
        }
        raise_course(staging, unsent).await?;
        if let Course::Local(at) = self.engine.course {
            self.engine.course = Course::Local(at.raised());
        }
        Ok(())
    }

    /// `tick`, the end of a boot attempt, which leaves a local course as it was. One that
    /// deleted staged files before it failed (a
    /// drop takes the newest first) left a local course at a position whose file is gone:
    /// the course ends, where carrying on would stage the next file across the hole.
    pub(super) async fn keep_local<W, P, C, L, R>(
        &mut self,
        env: &mut Env<'_, W, P, C, L, R>,
        tick: Tick,
        now: Timestamp,
    ) -> Tick
    where
        L: Role,
    {
        let Course::Local(at) = self.engine.course else {
            return tick;
        };
        let key = FileKey::new(Level::ZERO, TxidRange::single(at.position().txid()));
        let staging = Apart::over(env.staging, at.base);
        if matches!(exists(&staging, &key).await, Ok(true)) {
            // The attempt had its turn ahead of the course's own steps.
            self.engine.syncs_ahead = 0;
            return tick;
        }
        self.leave_local();
        match tick {
            Tick::Ran { event, .. } => self.ran(event, now),
            other @ (Tick::Stopped(_) | Tick::Drained { .. } | Tick::DeadlineReached(_)) => other,
        }
    }

    /// Ends a local course: the engine is one that has not booted, and boot attempts are the
    /// sync task's again, with their run of failures.
    pub(super) fn leave_local(&mut self) {
        self.engine = Engine::fresh();
        self.kept.tasks.hand(Task::Upload, Task::Sync);
    }
}
