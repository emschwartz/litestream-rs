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

use lsr_ltx::{PageSize, Timestamp, TxidRange};

use super::parts::{Course, Engine, Stop, Tail};
use super::{Actor, Env, Task, Tick, boot_wal_of, open_reader, wal_view};
use crate::checkpoint::CheckpointState;
use crate::fault::role::Role;
use crate::fault::{Fault, Next};
use crate::layout::{FileKey, Level};
use crate::position::Position;
use crate::recover::{NewestStagedFile, PositionSource, Recovered, recover_traced};
use crate::source::{Checkpointer, PageSource, WalSource};
use crate::witness::{Base, Witnessed, scan};

/// Whether this process may still try the local course.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LocalTry {
    /// Not judged yet.
    Open,
    /// Staging and the WAL were judged once, whatever the answer.
    Spent,
}

/// Proof that staging and the WAL agree, as of the reads that made it: every staged level-0
/// file read whole, each after the oldest starts where the one before it ended (or at a
/// generation's first frame), and the WAL holds the newest file's range with the same pages
/// and continues from its end. Only [`agree`] makes one.
struct Agreed {
    position: Position,
    page_size: PageSize,
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
        Self { position, tail }
    }
}

impl Course {
    /// The local course `agreed` proves.
    const fn local(agreed: &Agreed) -> Self {
        Self::Local(LocalAt {
            position: agreed.position,
            tail: Tail::Synced,
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
    let Some(NewestStagedFile::Readable { page_size, .. }) = newest else {
        return Ok(None);
    };
    let view = wal_view(len, reader.as_ref(), committed);
    // No replica: the verdict rests on the staged file and the WAL alone.
    let verdict = recover_traced(newest.as_ref().map(PositionSource::Boot), &view, None);
    Ok(match verdict.recovered {
        Recovered::Continue(position) => Some(Agreed {
            position,
            page_size,
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
            Course::Local(_) => self.wal_unit(env, now, self.checkpoints()).await,
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
        let Ok(agreed) = agree::<W, L, R>(env.wal, env.staging).await else {
            return Tick::Ran { event, next };
        };
        self.kept.local = LocalTry::Spent;
        let Some(agreed) = agreed else {
            return Tick::Ran { event, next };
        };
        self.engine = Engine {
            course: Course::local(&agreed),
            page_size: Some(agreed.page_size),
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
        if matches!(crate::store::exists(env.staging, &key).await, Ok(true)) {
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
