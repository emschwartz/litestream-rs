//! What the database really was, commit by commit.
//!
//! The oracle compares the replica against this, never against the code under
//! test. A commit's image shares every page it did not write with its parent
//! through `Rc`, so memory is pages written, not pages times commits; a chain
//! is flattened every [`FLATTEN_DEPTH`] commits so a lookup stays short.
//!
//! The model keeps every commit ever made, on every branch. A power loss ends
//! the local branch at the last durable commit and starts a new one from it; the
//! lost commits stay, because a file that was uploaded before the loss still
//! covers one of them.

use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use lsr_ltx::{PageNo, PageSize, Txid};
use lsr_wal::{Salts, WalOffset};

use crate::sqlite::AppCommit;

/// A chain of images this long is flattened into one.
const FLATTEN_DEPTH: u32 = 32;

/// Which commit, in the order the app made them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitId(u32);

impl CommitId {
    /// The position in [`Model::commits`].
    #[must_use]
    pub fn index(self) -> usize {
        usize::try_from(self.0).unwrap_or(usize::MAX)
    }
}

impl fmt::Display for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "commit {}", self.0)
    }
}

/// The database's pages at one commit.
#[derive(Debug)]
pub struct PageImage {
    page_size: PageSize,
    db_size: u32,
    parent: Option<Rc<PageImage>>,
    own: BTreeMap<u32, Rc<Vec<u8>>>,
    depth: u32,
}

impl PageImage {
    /// An image of `bytes`, a whole number of pages.
    #[must_use]
    pub fn from_bytes(page_size: PageSize, bytes: &[u8]) -> Self {
        let own: BTreeMap<u32, Rc<Vec<u8>>> = bytes
            .chunks_exact(page_size.bytes())
            .enumerate()
            .filter_map(|(i, page)| {
                let pgno = u32::try_from(i).ok()?.checked_add(1)?;
                Some((pgno, Rc::new(page.to_vec())))
            })
            .collect();
        Self {
            page_size,
            db_size: u32::try_from(own.len()).unwrap_or(u32::MAX),
            parent: None,
            own,
            depth: 0,
        }
    }

    /// The page size.
    #[must_use]
    pub const fn page_size(&self) -> PageSize {
        self.page_size
    }

    /// The length in pages.
    #[must_use]
    pub const fn db_size(&self) -> u32 {
        self.db_size
    }

    /// A page the image holds explicitly, found along the chain.
    fn explicit(&self, pgno: u32) -> Option<Rc<Vec<u8>>> {
        let mut node = self;
        loop {
            if let Some(page) = node.own.get(&pgno) {
                return Some(Rc::clone(page));
            }
            node = node.parent.as_deref()?;
        }
    }

    /// One page, zeros when nothing wrote it; `None` outside 1 to the length.
    #[must_use]
    pub fn page(&self, pgno: u32) -> Option<Rc<Vec<u8>>> {
        if pgno == 0 || pgno > self.db_size {
            return None;
        }
        Some(
            self.explicit(pgno)
                .unwrap_or_else(|| Rc::new(vec![0; self.page_size.bytes()])),
        )
    }

    /// The image after the pages in `writes` are written and the database is
    /// `db_size` pages long. Pages past the new length are dropped, and a
    /// shrink flattens, so growing again never brings an old page back.
    #[must_use]
    pub fn child(self: &Rc<Self>, writes: &[(PageNo, Vec<u8>)], db_size: u32) -> Rc<Self> {
        let written = writes
            .iter()
            .filter(|(pgno, _)| pgno.get() <= db_size)
            .map(|(pgno, bytes)| (pgno.get(), Rc::new(bytes.clone())));
        let flatten = db_size < self.db_size || self.depth.saturating_add(1) >= FLATTEN_DEPTH;
        if !flatten {
            return Rc::new(Self {
                page_size: self.page_size,
                db_size,
                parent: Some(Rc::clone(self)),
                own: written.collect(),
                depth: self.depth.saturating_add(1),
            });
        }
        let mut own: BTreeMap<u32, Rc<Vec<u8>>> = (1..=db_size.min(self.db_size))
            .filter_map(|pgno| Some((pgno, self.explicit(pgno)?)))
            .collect();
        own.extend(written);
        Rc::new(Self {
            page_size: self.page_size,
            db_size,
            parent: None,
            own,
            depth: 0,
        })
    }
}

/// One commit the app made.
#[derive(Clone, Debug)]
pub struct CommitRec {
    /// Its position.
    pub id: CommitId,
    /// The database as of the commit.
    pub image: Rc<PageImage>,
    /// The WAL generation it was written in.
    pub salts: Salts,
    /// The WAL offset just past its commit frame.
    pub end: WalOffset,
    /// It has survived a power loss or a checkpoint's sync, so none can undo it.
    pub durable: bool,
    /// The local branch it started on: 0 until the first power loss.
    pub branch: u32,
    /// How many recorder events existed when it was made, so a file put earlier
    /// cannot be taken to cover it.
    pub at: usize,
}

/// Where a staged file's WAL range ends: its generation and the byte offset.
pub type WalEnd = (Salts, u64);

/// A snapshot file's cover, noted from the actor's events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotNote {
    /// The last transaction the snapshot holds.
    pub txid: Txid,
    /// The commit it was built from.
    pub commit: CommitId,
    /// A `LocalRewound` snapshot, which may re-issue a transaction id.
    pub rewound: bool,
    /// Recorder events at the time.
    pub at: usize,
    /// Its place among every snapshot and stage note.
    pub seq: u64,
}

/// A staged level-0 file's commit, noted when the engine staged it: a power
/// loss lets a later commit sit at the same WAL position, so the position alone
/// does not say which one the file holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StageNote {
    /// The file's transaction id.
    pub txid: Txid,
    /// The WAL generation its range ends in.
    pub salts: Salts,
    /// Where its range ends.
    pub end: u64,
    /// The commit it holds.
    pub commit: CommitId,
    /// Replica recorder events at the time.
    pub at: usize,
    /// Its place among every snapshot and stage note.
    pub seq: u64,
}

/// Every commit, the local branch they sit on, and the snapshot notes.
#[derive(Debug)]
pub struct Model {
    initial: Rc<PageImage>,
    commits: Vec<CommitRec>,
    lineage: Vec<CommitId>,
    branch: u32,
    notes: Vec<SnapshotNote>,
    stages: Vec<StageNote>,
    seq: u64,
}

impl Model {
    /// A model whose database starts as `initial`, before any commit.
    #[must_use]
    pub fn new(initial: PageImage) -> Self {
        let initial = Rc::new(initial);
        // Commit 0 is the database before the app wrote anything: after a power
        // loss that kept nothing, a snapshot still holds it.
        let genesis = CommitRec {
            id: CommitId(0),
            image: Rc::clone(&initial),
            salts: Salts { salt1: 0, salt2: 0 },
            end: WalOffset::start(initial.page_size()),
            durable: true,
            branch: 0,
            at: 0,
        };
        Self {
            initial,
            commits: vec![genesis],
            lineage: vec![CommitId(0)],
            branch: 0,
            notes: Vec::new(),
            stages: Vec::new(),
            seq: 0,
        }
    }

    /// The image the next commit builds on: the local branch's newest commit.
    #[must_use]
    pub fn newest_image(&self) -> Rc<PageImage> {
        self.lineage
            .last()
            .and_then(|id| self.commits.get(id.index()))
            .map_or_else(|| Rc::clone(&self.initial), |rec| Rc::clone(&rec.image))
    }

    /// The local branch's newest commit.
    #[must_use]
    pub fn newest_commit(&self) -> Option<CommitId> {
        self.lineage.last().copied()
    }

    fn push(&mut self, image: Rc<PageImage>, app: AppCommit, at: usize, durable: bool) -> CommitId {
        let id = CommitId(u32::try_from(self.commits.len()).unwrap_or(u32::MAX));
        self.commits.push(CommitRec {
            id,
            image,
            salts: app.salts,
            end: app.end,
            durable,
            branch: self.branch,
            at,
        });
        self.lineage.push(id);
        id
    }

    /// Records the app's commit of `pages` leaving `db_size` pages. `at` is the
    /// recorder's length now. A shrink is a commit with no pages.
    pub fn commit(
        &mut self,
        pages: &[(PageNo, Vec<u8>)],
        db_size: u32,
        app: AppCommit,
        at: usize,
    ) -> CommitId {
        let image = self.newest_image().child(pages, db_size);
        self.push(image, app, at, false)
    }

    /// Records a VACUUM: a whole new image, written to the database file by a
    /// full backfill, so it is durable from the start.
    pub fn vacuum(&mut self, image: PageImage, app: AppCommit, at: usize) -> CommitId {
        self.push(Rc::new(image), app, at, true)
    }

    /// A checkpoint syncs the WAL: every commit so far is durable.
    pub fn on_checkpoint(&mut self) {
        for id in &self.lineage {
            if let Some(rec) = self.commits.get_mut(id.index()) {
                rec.durable = true;
            }
        }
    }

    /// A power loss. Commits of the current generation (`current`) past
    /// `kept_frames` that no checkpoint made durable are gone from the local
    /// branch; a new branch starts at what is left.
    pub fn on_power_loss(&mut self, current: Salts, kept_frames: u32) {
        let commits = &mut self.commits;
        self.lineage.retain(|id| {
            let Some(rec) = commits.get_mut(id.index()) else {
                return false;
            };
            let survives =
                rec.durable || (rec.salts == current && rec.end.frame_index() <= kept_frames);
            rec.durable = survives;
            survives
        });
        self.branch = self.branch.saturating_add(1);
    }

    /// Whether a power loss has rewound the local branch.
    #[must_use]
    pub const fn rewound(&self) -> bool {
        self.branch > 0
    }

    /// The commit written in generation `salts` that ends at byte `end`, as of
    /// recorder length `at`. After a power loss the WAL regrows over the same offsets
    /// with other commits, so several can match; the newest one made by `at`
    /// wins.
    #[must_use]
    pub fn find(&self, salts: Salts, end: u64, at: usize) -> Option<CommitId> {
        self.commits
            .iter()
            .filter(|rec| rec.salts == salts && rec.end.get() == end && rec.at <= at)
            .max_by_key(|rec| (rec.at, rec.id))
            .map(|rec| rec.id)
    }

    /// One commit.
    #[must_use]
    pub fn rec(&self, id: CommitId) -> Option<&CommitRec> {
        self.commits.get(id.index())
    }

    /// Every commit ever made, on every branch.
    #[must_use]
    pub fn commits(&self) -> &[CommitRec] {
        &self.commits
    }

    /// Notes that the file for `txid`, whose WAL range ends at `end` in `salts`,
    /// was staged from whichever commit sits there now.
    pub fn note_stage(&mut self, txid: Txid, salts: Salts, end: u64, at: usize) {
        if let Some(commit) = self.find(salts, end, usize::MAX) {
            self.seq = self.seq.saturating_add(1);
            self.stages.push(StageNote {
                seq: self.seq,
                txid,
                salts,
                end,
                commit,
                at,
            });
        }
    }

    /// The commit the file for `txid` held, when it was staged by `at` or before.
    #[must_use]
    pub fn staged_from(&self, txid: Txid, salts: Salts, end: u64, at: usize) -> Option<CommitId> {
        self.stages
            .iter()
            .rev()
            .find(|n| n.txid == txid && n.salts == salts && n.end == end && n.at <= at)
            .map(|n| n.commit)
    }

    /// The commit the newest file staged for `txid` by `at` held, whatever
    /// its WAL range, snapshot or not: for a file whose header names no WAL
    /// range (a snapshot of an empty WAL, or a file that lost its origin).
    #[must_use]
    pub fn staged_as(&self, txid: Txid, at: usize) -> Option<CommitId> {
        let stage = self
            .stages
            .iter()
            .rev()
            .find(|n| n.txid == txid && n.at <= at)
            .map(|n| (n.seq, n.commit));
        let snapshot = self
            .notes
            .iter()
            .rev()
            .find(|n| n.txid == txid && n.at <= at)
            .map(|n| (n.seq, n.commit));
        stage.max(snapshot).map(|(_, commit)| commit)
    }

    /// The commit a staged file for `txid` holds now, as the oracle would map
    /// it: by its WAL range (`origin`, generation and end) when it names one,
    /// otherwise the newest note for `txid`. Taken when a backup copies the file.
    #[must_use]
    pub fn staged_commit(&self, txid: Txid, origin: Option<WalEnd>) -> Option<CommitId> {
        match origin {
            Some((salts, end)) => self
                .staged_from(txid, salts, end, usize::MAX)
                .or_else(|| self.find(salts, end, usize::MAX)),
            None => self.staged_as(txid, usize::MAX),
        }
    }

    /// Notes again that the staged file for `txid` holds `commit`: a backup put
    /// it back over whatever was staged for that id since, so the newest note
    /// for the id is this one again.
    pub fn note_restored(
        &mut self,
        txid: Txid,
        origin: Option<WalEnd>,
        commit: CommitId,
        at: usize,
    ) {
        self.seq = self.seq.saturating_add(1);
        match origin {
            Some((salts, end)) => self.stages.push(StageNote {
                seq: self.seq,
                txid,
                salts,
                end,
                commit,
                at,
            }),
            None => self.notes.push(SnapshotNote {
                seq: self.seq,
                txid,
                commit,
                rewound: false,
                at,
            }),
        }
    }

    /// Notes that a snapshot ending at `txid` was built from `commit`.
    pub fn note_snapshot(&mut self, txid: Txid, commit: CommitId, rewound: bool, at: usize) {
        self.seq = self.seq.saturating_add(1);
        self.notes.push(SnapshotNote {
            seq: self.seq,
            txid,
            commit,
            rewound,
            at,
        });
    }

    /// The snapshot notes, oldest first.
    #[must_use]
    pub fn snapshot_notes(&self) -> &[SnapshotNote] {
        &self.notes
    }
}

#[cfg(test)]
mod tests;
