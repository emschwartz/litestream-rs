//! Faults that last ([`Op::Hold`](crate::Op::Hold)), and restores judged while they hold.
//!
//! A hold puts its fault in place, then the app commits and the engine ticks under it. In the
//! middle of those ticks and again at their end, with the fault still in place, the replica is
//! judged twice:
//!
//! - by both judges over the raw replica, as every [`Op::Restore`](crate::Op::Restore) judges
//!   it: the files the engine left while the fault held must restore committed states, in
//!   order, and the newest must not be behind what the engine acknowledged;
//! - for the faults a restore itself meets (a store that is down or refuses, a listing that
//!   lags, reads that return other bytes), by restores made through that fault ([`View`]).
//!   Each must publish exactly the database the raw replica gives for the same target, or fail
//!   with an error the fault explains. Another database, or an error the fault does not
//!   explain, is [`Violation::HeldRestore`].
//!
//! Then the fault is lifted, and the run goes on to judge recovery as it always has.

use std::collections::BTreeSet;
use std::io::ErrorKind;

use lsr_core::conformance::block_on;
use lsr_core::{
    CreatedAt, Entropy, FileInfo, FileKey, Inventory, Level, Listing, MemStore, ObjectStore,
    PageLimit, ReadLen, RestoreError, RestoreTarget, StopCause, StoreError, StoreOp,
};
use lsr_ltx::{Timestamp, Txid, TxidRange};

use crate::generate::{Hold, WalOp};
use crate::history::restored_digest;
use crate::oracle::{Violation, bug_fault};
use crate::reach::variant;
use crate::sqlite::{CrashKind, ReaderId};
use crate::store::HeldFault;

use super::{Runner, small};

/// Transaction ids a judgment restores through the fault besides `Latest`, at most.
const SAMPLED_TXIDS: u64 = 4;

/// Reads of the replica the engine makes under [`Hold::LyingReads`] that return a flipped
/// byte, at most: one flip scheduled for each of the next this many reads.
const LYING_ENGINE_READS: u32 = 64;

/// What a restore through the fault meets.
#[derive(Clone, Debug)]
enum Lens {
    /// Every call fails with the error its operation maps to.
    Down { ops: Vec<StoreOp>, auth: bool },
    /// Listings show only these keys: the bucket as it stood when the hold began.
    Lagging(BTreeSet<FileKey>),
    /// Every read returns one byte flipped.
    Lying,
}

/// The raw replica as a restore made during the hold sees it.
#[derive(Clone, Debug)]
pub(super) struct View {
    raw: MemStore,
    lens: Lens,
}

impl View {
    fn fail(&self, op: StoreOp) -> Result<(), StoreError> {
        match &self.lens {
            Lens::Down { ops, auth } if ops.contains(&op) => Err(held_error(op, *auth)),
            Lens::Down { .. } | Lens::Lagging(_) | Lens::Lying => Ok(()),
        }
    }

    fn lie(&self, bytes: &mut [u8]) {
        if matches!(self.lens, Lens::Lying) {
            let middle = bytes.len() / 2;
            if let Some(byte) = bytes.get_mut(middle) {
                *byte ^= 0x10;
            }
        }
    }
}

/// The error a held store fault returns for `op`.
fn held_error(op: StoreOp, auth: bool) -> StoreError {
    if auth {
        StoreError::Auth { op }
    } else {
        StoreError::Transient {
            op,
            kind: ErrorKind::ConnectionReset,
        }
    }
}

impl ObjectStore for View {
    type Upload = <MemStore as ObjectStore>::Upload;

    async fn list(
        &self,
        level: Level,
        after: Option<TxidRange>,
        limit: PageLimit,
    ) -> Result<Listing, StoreError> {
        self.fail(StoreOp::List)?;
        let mut listing = self.raw.list(level, after, limit).await?;
        if let Lens::Lagging(seen) = &self.lens {
            listing.files.retain(|file| seen.contains(&file.key));
        }
        Ok(listing)
    }

    async fn read(&self, key: &FileKey, offset: u64, len: ReadLen) -> Result<Vec<u8>, StoreError> {
        self.fail(StoreOp::Read)?;
        let mut bytes = self.raw.read(key, offset, len).await?;
        self.lie(&mut bytes);
        Ok(bytes)
    }

    async fn created_at(&self, file: &FileInfo) -> Result<CreatedAt, StoreError> {
        self.fail(StoreOp::Read)?;
        self.raw.created_at(file).await
    }

    async fn read_with_created_at(
        &self,
        file: &FileInfo,
        len: ReadLen,
    ) -> Result<(Vec<u8>, CreatedAt), StoreError> {
        self.fail(StoreOp::Read)?;
        let (mut bytes, created_at) = self.raw.read_with_created_at(file, len).await?;
        self.lie(&mut bytes);
        Ok((bytes, created_at))
    }

    async fn begin_upload(
        &self,
        key: &FileKey,
        created_at: Timestamp,
        mode: lsr_core::WriteMode,
    ) -> Result<Self::Upload, StoreError> {
        self.fail(StoreOp::Upload)?;
        self.raw.begin_upload(key, created_at, mode).await
    }

    async fn delete(&self, keys: &[FileKey]) -> Result<(), StoreError> {
        self.fail(StoreOp::Delete)?;
        self.raw.delete(keys).await
    }
}

/// What a hold put in place, so it can be lifted.
enum Placed {
    Remote(Vec<HeldFault>),
    Staging(HeldFault),
    Readers(Vec<ReaderId>),
    LyingDisk,
}

/// The fault's name in reach and in violations.
fn name(hold: Hold) -> String {
    match hold {
        Hold::StoreDown(op) => format!("store_down.{op:?}"),
        Hold::Wal(op) => format!("wal.{}", variant(&op)),
        Hold::CredentialsRevoked
        | Hold::DiskFull
        | Hold::LaggingListing
        | Hold::LyingReads
        | Hold::ReadersHeld
        | Hold::LyingDisk => variant(&hold),
    }
}

/// Whether `err`, from a restore made through `hold`, is an error that fault explains.
fn explained(hold: Hold, err: &RestoreError) -> bool {
    match hold {
        Hold::StoreDown(_) | Hold::CredentialsRevoked => {
            matches!(err, RestoreError::Inventory(_) | RestoreError::Store(_))
        }
        // A lagging listing hides files: a gap, or files that vanish under it.
        Hold::LaggingListing => matches!(
            err,
            RestoreError::NoPlan(_)
                | RestoreError::Unstable { .. }
                | RestoreError::ListingBehind { .. }
        ),
        Hold::LyingReads => matches!(err, RestoreError::File { .. }),
        Hold::DiskFull | Hold::ReadersHeld | Hold::Wal(_) | Hold::LyingDisk => false,
    }
}

impl Runner {
    /// Runs `ticks` ticks under `hold`, judging restores in the middle and at the end while it
    /// holds, then lifts it.
    pub(super) fn hold(&mut self, hold: Hold, ticks: u8) -> Result<(), Violation> {
        self.report.reach.bump(format!("hold.{}", name(hold)));
        if let Hold::Wal(op) = hold {
            return self.wal_damage_held(op, ticks);
        }
        let view = self.view(hold)?;
        let flipped = self.w.remote_faults.flipped().len();
        let placed = self.place(hold);
        let middle = ticks / 2;
        for at in 0..ticks {
            // A commit before every tick keeps the sync unit busy each tick and no upload ever
            // runs, so the engine would never meet a fault on the upload path.
            if at % 2 == 0 {
                let pages = u8::try_from(small(&self.rng, 3).saturating_add(1)).unwrap_or(1);
                self.commit(pages, false);
            }
            if hold == Hold::LyingDisk && at == middle {
                self.restart(CrashKind::Power);
                self.report.reach.bump("hold.lying_disk.power_lost");
            }
            self.tick()?;
            if at == middle {
                self.judge_held(hold, view.as_ref())?;
            }
        }
        self.judge_held(hold, view.as_ref())?;
        self.lift(placed);
        let lied = self.w.remote_faults.flipped().len().saturating_sub(flipped);
        self.report.reach.add(
            format!("hold.{}.engine_reads_flipped", name(hold)),
            u64::try_from(lied).unwrap_or(u64::MAX),
        );
        Ok(())
    }

    /// The view a restore made during `hold` has of the replica, if the fault reaches it.
    fn view(&self, hold: Hold) -> Result<Option<View>, Violation> {
        let lens = match hold {
            Hold::StoreDown(op) => Lens::Down {
                ops: vec![op],
                auth: false,
            },
            Hold::CredentialsRevoked => Lens::Down {
                ops: vec![
                    StoreOp::List,
                    StoreOp::Read,
                    StoreOp::Upload,
                    StoreOp::Delete,
                ],
                auth: true,
            },
            Hold::LaggingListing => {
                let inventory = block_on(Inventory::load(&self.w.remote_raw))
                    .map_err(|err| bug_fault(format!("listing the replica: {err}")))?;
                Lens::Lagging(inventory.files().iter().map(|f| f.key).collect())
            }
            Hold::LyingReads => Lens::Lying,
            Hold::DiskFull | Hold::ReadersHeld | Hold::Wal(_) | Hold::LyingDisk => {
                return Ok(None);
            }
        };
        Ok(Some(View {
            raw: self.w.remote_raw.clone(),
            lens,
        }))
    }

    /// Puts the fault of `hold` in place for the engine.
    fn place(&mut self, hold: Hold) -> Option<Placed> {
        match hold {
            Hold::StoreDown(op) => Some(Placed::Remote(vec![
                self.w.remote_faults.hold(op, held_error(op, false)),
            ])),
            Hold::CredentialsRevoked => {
                self.expect(StopCause::Credentials);
                let ops = [
                    StoreOp::List,
                    StoreOp::Read,
                    StoreOp::Upload,
                    StoreOp::Delete,
                ];
                Some(Placed::Remote(
                    ops.into_iter()
                        .map(|op| self.w.remote_faults.hold(op, held_error(op, true)))
                        .collect(),
                ))
            }
            Hold::DiskFull => {
                let full = StoreError::Transient {
                    op: StoreOp::Upload,
                    kind: ErrorKind::StorageFull,
                };
                Some(Placed::Staging(
                    self.w.staging_faults.hold(StoreOp::Upload, full),
                ))
            }
            Hold::ReadersHeld => {
                let count = small(&self.rng, 2).saturating_add(1);
                let readers = (0..count)
                    .map(|_| self.w.sqlite.other_process_reader_begin())
                    .collect();
                Some(Placed::Readers(readers))
            }
            Hold::LyingDisk => {
                self.w.sqlite.lie_on_our_sync();
                Some(Placed::LyingDisk)
            }
            Hold::LyingReads => {
                // The engine's reads lie too: a read that covers the header gets one byte of it
                // flipped, so it fails decode, as a proxy's wrong body does. Two such reads of
                // one sound object are what the engine takes as rot.
                self.expect(StopCause::RemoteCorrupt);
                Some(Placed::Remote(
                    (0..LYING_ENGINE_READS)
                        .map(|ahead| {
                            self.w.remote_faults.held(
                                StoreOp::Read,
                                ahead,
                                crate::store::FaultShape::FlipByteOnRead { at: 8 },
                            )
                        })
                        .collect(),
                ))
            }
            // The engine never lists through a restore's lens.
            Hold::LaggingListing | Hold::Wal(_) => None,
        }
    }

    fn lift(&mut self, placed: Option<Placed>) {
        match placed {
            None => {}
            Some(Placed::Remote(held)) => {
                for one in held {
                    self.w.remote_faults.release(one);
                }
            }
            Some(Placed::Staging(held)) => self.w.staging_faults.release(held),
            Some(Placed::Readers(readers)) => {
                // A power loss inside the hold ended them already.
                for id in readers {
                    self.w.sqlite.other_process_reader_end(id);
                }
            }
            Some(Placed::LyingDisk) => self.w.sqlite.honest_on_our_sync(),
        }
    }

    /// The judgments made while `hold` holds: both judges over the raw replica, and restores
    /// through `view` when the fault reaches restores.
    pub(super) fn judge_held(&mut self, hold: Hold, view: Option<&View>) -> Result<(), Violation> {
        self.restore_all()?;
        self.report
            .reach
            .bump(format!("hold.{}.judged", name(hold)));
        let Some(view) = view else {
            return Ok(());
        };
        let newest = block_on(Inventory::load(&self.w.remote_raw))
            .map_err(|err| bug_fault(format!("listing the replica: {err}")))?
            .newest();
        let mut targets = vec![RestoreTarget::Latest, RestoreTarget::AsOf(self.now())];
        if let Some(newest) = newest {
            for _ in 0..SAMPLED_TXIDS {
                let at = self.rng.below(newest.get()).saturating_add(1);
                targets.extend(Txid::new(at).map(RestoreTarget::Txid));
            }
        }
        for target in targets {
            self.judge_through(hold, view, target)?;
        }
        Ok(())
    }

    /// One restore through `view`, held to the raw replica's answer for `target`.
    fn judge_through(
        &mut self,
        hold: Hold,
        view: &View,
        target: RestoreTarget,
    ) -> Result<(), Violation> {
        let held = restored_digest(view, target)?;
        let raw = restored_digest(&self.w.remote_raw, target)?;
        let wrong = |got: String| Violation::HeldRestore {
            target,
            hold: name(hold),
            got,
        };
        match (held, raw) {
            (Ok((got, _)), Ok((want, _))) if got == want => {
                self.report.reach.bump(format!("hold.{}.same", name(hold)));
                Ok(())
            }
            (Ok(_), Ok(_)) => Err(wrong("another database than the replica holds".into())),
            (Ok((got, reached)), Err(err)) => {
                if hold == Hold::LaggingListing && self.behind_a_hole(&err, got, reached)? {
                    self.report.reach.bump("hold.LaggingListing.past_hole");
                    return Ok(());
                }
                // The raw replica stops on an object the store damaged as it published it, so
                // it has no answer to hold this one to; the most a restore can promise then is
                // a state the application committed.
                if let RestoreError::File { key, .. } = &err
                    && self.w.remote_faults.damaged().contains(key)
                {
                    if self.w.history.log().find(&got).is_empty() {
                        return Err(wrong(format!(
                            "a database no commit made, where the raw replica stops on {key}, \
                             which the store damaged"
                        )));
                    }
                    self.report
                        .reach
                        .bump(format!("hold.{}.raw_damaged", name(hold)));
                    return Ok(());
                }
                Err(wrong(format!(
                    "a database, where the raw replica gives {err}"
                )))
            }
            // The replica itself has no answer: no fault can give one.
            (Err(_), Err(_)) => {
                self.report
                    .reach
                    .bump(format!("hold.{}.refused", name(hold)));
                Ok(())
            }
            (Err(err), Ok(_)) if explained(hold, &err) => {
                self.report
                    .reach
                    .bump(format!("hold.{}.refused", name(hold)));
                Ok(())
            }
            (Err(err), Ok(_)) => Err(wrong(format!("an error the fault does not explain: {err}"))),
        }
    }

    /// Whether a restore through a lagging listing that reached `reached` with `got` stopped
    /// short of a hole the raw replica has (`err` is its gap): the raw replica's own answer
    /// for `reached` is `got`, and the hole is past it. The files past the hole are known only
    /// to a listing, and no read can name them, so the lag hides them as it hides any file; a
    /// listing whole up to the hole gives the same chain.
    fn behind_a_hole(
        &self,
        err: &RestoreError,
        got: lsr_history::Digest,
        reached: Txid,
    ) -> Result<bool, Violation> {
        let RestoreError::NoPlan(no) = err else {
            return Ok(false);
        };
        let lsr_core::PlanOutcome::Gap { have_through, .. } = no.outcome() else {
            return Ok(false);
        };
        if reached > *have_through {
            return Ok(false);
        }
        let raw = restored_digest(&self.w.remote_raw, RestoreTarget::Txid(reached))?;
        Ok(matches!(raw, Ok((want, _)) if want == got))
    }

    /// WAL damage held for `ticks` ticks, judged in the middle and at the end, then healed.
    fn wal_damage_held(&mut self, op: WalOp, ticks: u8) -> Result<(), Violation> {
        self.wal_damage_judged(op, ticks, true)
    }
}
