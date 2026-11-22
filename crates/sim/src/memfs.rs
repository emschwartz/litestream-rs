//! An in-memory filesystem under the crash law, so the real directory store (`lsr-store-fs`) can
//! be the simulator's staging store and a power loss acts on the store's own durability steps.
//!
//! What it keeps per file and per directory, and what a power loss leaves:
//!
//! - A file (an inode) holds its current bytes and mtime, and the bytes and mtime as of its last
//!   `sync_all`, with the byte ranges written since. A power loss keeps the synced image under
//!   [`CrashLaw::AllOrNothing`], and the block law's draw over the dirty ranges under
//!   [`CrashLaw::Blocks`] ([`crate::crash_law::crash_image`]); the mtime is kept or lost with a
//!   coin under the block law. A file sync makes the bytes and the mtime durable, not the name.
//! - A directory holds its entries and, for each name changed since its last `sync_dir`, what
//!   the name durably held. A power loss keeps or undoes each such change by the one
//!   directory-entry rule of the simulated disk ([`crate::crash_law::entry_kept`]): under the
//!   block law each name on its own coin, so a rename's two names may part; under
//!   all-or-nothing a deletion stays done and anything else is undone. A directory whose own
//!   entry is undone goes with everything in it.
//! - After a power loss the files no name reaches are gone, and every handle opened before it
//!   fails: the process that held it is gone too.
//!
//! What it leaves out, on purpose: symlinks, cross-directory renames (the store makes none; it
//! refuses one), permissions, and space. Errors carry the `io::ErrorKind` the real call gives on
//! Linux, which the store matches on (`NotFound`, `AlreadyExists`); `differential` tests hold it
//! to the host's filesystem through the store.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use lsr_store_fs::{DirEntryInfo, Fs, FsFile, Meta};

use crate::crash_law::{CrashLaw, Rng, crash_image, entry_kept};

type Ino = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Node {
    File(Ino),
    Dir,
}

#[derive(Debug, Default)]
struct Dir {
    entries: BTreeMap<OsString, Node>,
    /// Each name changed since the last sync, with what it durably held.
    durable: BTreeMap<OsString, Option<Node>>,
}

#[derive(Debug)]
struct Inode {
    data: Vec<u8>,
    synced: Vec<u8>,
    dirty: Vec<(u64, u64)>,
    mtime: SystemTime,
    synced_mtime: SystemTime,
}

impl Inode {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            synced: Vec::new(),
            dirty: Vec::new(),
            mtime: SystemTime::UNIX_EPOCH,
            synced_mtime: SystemTime::UNIX_EPOCH,
        }
    }

    fn meta(&self) -> Meta {
        Meta::new(true, wide(self.data.len()), self.mtime)
    }
}

/// What one power loss did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PowerLoss {
    /// Directory entries put back because no sync made their change durable.
    pub undone: u64,
    /// Files that came back with other bytes or another mtime than they had.
    pub changed: u64,
}

#[derive(Debug)]
struct State {
    dirs: BTreeMap<PathBuf, Dir>,
    inodes: BTreeMap<Ino, Inode>,
    next_ino: Ino,
    /// Bumped by every power loss; a handle from an earlier epoch is dead.
    epoch: u64,
    law: CrashLaw,
    rng: Rng,
}

/// The filesystem; clones share it.
#[derive(Clone, Debug)]
pub struct MemFs {
    state: Arc<Mutex<State>>,
}

fn wide(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

fn at(n: u64) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

fn err(kind: io::ErrorKind) -> io::Error {
    io::Error::from(kind)
}

/// The parent directory and the name of `path`; the root has neither.
fn split(path: &Path) -> io::Result<(&Path, OsString)> {
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) => Ok((parent, name.to_owned())),
        _ => Err(err(io::ErrorKind::InvalidInput)),
    }
}

impl State {
    /// The directory at `path`, or the error the real call gives for a missing one.
    fn dir_mut(&mut self, path: &Path) -> io::Result<&mut Dir> {
        if !self.dirs.contains_key(path) {
            return Err(self.missing(path));
        }
        self.dirs
            .get_mut(path)
            .ok_or_else(|| err(io::ErrorKind::NotFound))
    }

    fn dir(&self, path: &Path) -> io::Result<&Dir> {
        self.dirs.get(path).ok_or_else(|| self.missing(path))
    }

    /// `NotADirectory` when a component names a file, else `NotFound`.
    fn missing(&self, path: &Path) -> io::Error {
        let file = split(path).ok().and_then(|(parent, name)| {
            self.dirs
                .get(parent)
                .and_then(|dir| dir.entries.get(&name))
                .filter(|node| matches!(node, Node::File(_)))
        });
        err(if file.is_some() {
            io::ErrorKind::NotADirectory
        } else {
            io::ErrorKind::NotFound
        })
    }

    fn lookup(&self, path: &Path) -> io::Result<Node> {
        if self.dirs.contains_key(path) {
            return Ok(Node::Dir);
        }
        let (parent, name) = split(path)?;
        self.dir(parent)?
            .entries
            .get(&name)
            .copied()
            .ok_or_else(|| err(io::ErrorKind::NotFound))
    }

    fn inode(&self, ino: Ino) -> io::Result<&Inode> {
        self.inodes
            .get(&ino)
            .ok_or_else(|| err(io::ErrorKind::NotFound))
    }

    fn inode_mut(&mut self, ino: Ino) -> io::Result<&mut Inode> {
        self.inodes
            .get_mut(&ino)
            .ok_or_else(|| err(io::ErrorKind::NotFound))
    }

    /// Sets names in one directory, keeping what each durably held before its first change
    /// since the last sync.
    fn change(&mut self, dir: &Path, names: Vec<(OsString, Option<Node>)>) -> io::Result<()> {
        let dir = self.dir_mut(dir)?;
        for (name, node) in names {
            let old = match node {
                Some(node) => dir.entries.insert(name.clone(), node),
                None => dir.entries.remove(&name),
            };
            dir.durable.entry(name).or_insert(old);
        }
        Ok(())
    }

    fn new_file(&mut self, path: &Path) -> io::Result<Ino> {
        let (parent, name) = split(path)?;
        if self.dir(parent)?.entries.contains_key(&name) {
            return Err(err(io::ErrorKind::AlreadyExists));
        }
        let ino = self.next_ino;
        self.next_ino = self.next_ino.saturating_add(1);
        self.inodes.insert(ino, Inode::new());
        self.change(parent, vec![(name, Some(Node::File(ino)))])?;
        Ok(ino)
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        let (from_dir, from_name) = split(from)?;
        let (to_dir, to_name) = split(to)?;
        let node = self.lookup(from)?;
        let Node::File(_) = node else {
            return Err(err(io::ErrorKind::Unsupported));
        };
        if from_dir != to_dir {
            return Err(err(io::ErrorKind::Unsupported));
        }
        if self.lookup(to).ok() == Some(Node::Dir) {
            return Err(err(io::ErrorKind::IsADirectory));
        }
        if from_name == to_name {
            return Ok(());
        }
        self.change(from_dir, vec![(from_name, None), (to_name, Some(node))])
    }

    /// A second name for a file, in the same directory, as `rename` allows; a taken name fails.
    fn hard_link(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        let (from_dir, _) = split(from)?;
        let (to_dir, to_name) = split(to)?;
        let node = self.lookup(from)?;
        let Node::File(_) = node else {
            return Err(err(io::ErrorKind::Unsupported));
        };
        if from_dir != to_dir {
            return Err(err(io::ErrorKind::Unsupported));
        }
        if self.dir(to_dir)?.entries.contains_key(&to_name) {
            return Err(err(io::ErrorKind::AlreadyExists));
        }
        self.change(to_dir, vec![(to_name, Some(node))])
    }
}

impl MemFs {
    /// An empty filesystem (the root directory alone) whose power losses follow `law`.
    #[must_use]
    pub fn new(law: CrashLaw) -> Self {
        let rng = match law {
            CrashLaw::AllOrNothing => Rng(0),
            CrashLaw::Blocks { seed, .. } => Rng(seed),
        };
        let mut dirs = BTreeMap::new();
        dirs.insert(PathBuf::from("/"), Dir::default());
        Self {
            state: Arc::new(Mutex::new(State {
                dirs,
                inodes: BTreeMap::new(),
                next_ino: 1,
                epoch: 0,
                law,
                rng,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // No guarded section panics, so poison carries no half-written state.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Loses power: every unsynced directory entry keeps or loses its change and every file
    /// what the law keeps of its unsynced writes (module doc). Handles opened before are dead.
    pub fn power_loss(&self) -> PowerLoss {
        let mut state = self.lock();
        let mut report = PowerLoss::default();
        undo_unsynced_entries(&mut state, &mut report);
        crash_files(&mut state, &mut report);
        collect_unreachable(&mut state);
        state.epoch = state.epoch.saturating_add(1);
        report
    }

    /// Whether every directory change and every file write so far is durable: nothing a power
    /// loss could take.
    #[must_use]
    pub fn all_durable(&self) -> bool {
        let state = self.lock();
        state.dirs.values().all(|dir| dir.durable.is_empty())
            && state.inodes.values().all(|inode| {
                inode.dirty.is_empty()
                    && inode.data == inode.synced
                    && inode.mtime == inode.synced_mtime
            })
    }

    fn handle(&self, ino: Ino, epoch: u64) -> MemFile {
        MemFile {
            fs: self.clone(),
            ino,
            epoch,
            pos: 0,
        }
    }
}

/// Keeps or undoes each directory entry changed since its directory's last sync, and drops
/// the directories an undone change unnamed.
fn undo_unsynced_entries(state: &mut State, report: &mut PowerLoss) {
    let paths: Vec<PathBuf> = state.dirs.keys().cloned().collect();
    for path in paths {
        let State { dirs, rng, law, .. } = &mut *state;
        let Some(dir) = dirs.get_mut(&path) else {
            continue;
        };
        let mut gone = Vec::new();
        for (name, before) in std::mem::take(&mut dir.durable) {
            let now = dir.entries.get(&name).copied();
            if now == before || entry_kept(*law, now.is_none(), rng) {
                continue;
            }
            report.undone = report.undone.saturating_add(1);
            match before {
                Some(node) => dir.entries.insert(name.clone(), node),
                None => dir.entries.remove(&name),
            };
            if now == Some(Node::Dir) && before != Some(Node::Dir) {
                gone.push(path.join(&name));
            }
        }
        for root in gone {
            dirs.retain(|p, _| !p.starts_with(&root));
        }
    }
}

/// Each file keeps what the law keeps of its writes since its last sync.
fn crash_files(state: &mut State, report: &mut PowerLoss) {
    let law = state.law;
    let State { inodes, rng, .. } = state;
    for inode in inodes.values_mut() {
        let (data, mtime) = match law {
            CrashLaw::AllOrNothing => (inode.synced.clone(), inode.synced_mtime),
            CrashLaw::Blocks { block, .. } => {
                let data = crash_image(&inode.synced, &inode.data, &inode.dirty, &[], block, rng);
                let mtime = if rng.coin() {
                    inode.mtime
                } else {
                    inode.synced_mtime
                };
                (data, mtime)
            }
        };
        if data != inode.data || mtime != inode.mtime {
            report.changed = report.changed.saturating_add(1);
        }
        inode.data = data.clone();
        inode.synced = data;
        inode.dirty.clear();
        inode.mtime = mtime;
        inode.synced_mtime = mtime;
    }
}

/// Drops the files no directory names.
fn collect_unreachable(state: &mut State) {
    let named: Vec<Ino> = state
        .dirs
        .values()
        .flat_map(|dir| dir.entries.values())
        .filter_map(|node| match node {
            Node::File(ino) => Some(*ino),
            Node::Dir => None,
        })
        .collect();
    state.inodes.retain(|ino, _| named.contains(ino));
}

impl Fs for MemFs {
    type File = MemFile;

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntryInfo>> {
        let state = self.lock();
        Ok(state
            .dir(dir)?
            .entries
            .keys()
            .map(|name| DirEntryInfo::new(name.clone(), false))
            .collect())
    }

    fn metadata(&self, path: &Path) -> io::Result<Meta> {
        let state = self.lock();
        match state.lookup(path)? {
            Node::Dir => Ok(Meta::new(false, 0, SystemTime::UNIX_EPOCH)),
            Node::File(ino) => Ok(state.inode(ino)?.meta()),
        }
    }

    fn try_exists(&self, path: &Path) -> io::Result<bool> {
        match self.lock().lookup(path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        let mut state = self.lock();
        let (parent, name) = split(path)?;
        if state.dir(parent)?.entries.contains_key(&name) {
            return Err(err(io::ErrorKind::AlreadyExists));
        }
        state.change(parent, vec![(name, Some(Node::Dir))])?;
        state.dirs.insert(path.to_owned(), Dir::default());
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.lock().dir_mut(dir)?.durable.clear();
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.lock().rename(from, to)
    }

    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.lock().hard_link(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut state = self.lock();
        match state.lookup(path)? {
            Node::Dir => Err(err(io::ErrorKind::IsADirectory)),
            Node::File(_) => {
                let (parent, name) = split(path)?;
                state.change(parent, vec![(name, None)])
            }
        }
    }

    fn create_new(&self, path: &Path) -> io::Result<MemFile> {
        let mut state = self.lock();
        let ino = state.new_file(path)?;
        let epoch = state.epoch;
        drop(state);
        Ok(self.handle(ino, epoch))
    }

    fn open(&self, path: &Path) -> io::Result<MemFile> {
        let state = self.lock();
        match state.lookup(path)? {
            Node::Dir => Err(err(io::ErrorKind::IsADirectory)),
            Node::File(ino) => {
                let epoch = state.epoch;
                drop(state);
                Ok(self.handle(ino, epoch))
            }
        }
    }
}

/// An open [`MemFs`] file. It names the inode, not the path, as a file descriptor does.
#[derive(Debug)]
pub struct MemFile {
    fs: MemFs,
    ino: Ino,
    epoch: u64,
    pos: u64,
}

impl MemFile {
    /// Runs `f` on the inode while the handle's process is alive.
    fn with<T>(&self, f: impl FnOnce(&mut Inode) -> io::Result<T>) -> io::Result<T> {
        let mut state = self.fs.lock();
        if state.epoch != self.epoch {
            return Err(io::Error::other("the handle did not survive a power loss"));
        }
        f(state.inode_mut(self.ino)?)
    }
}

impl FsFile for MemFile {
    fn metadata(&self) -> io::Result<Meta> {
        self.with(|inode| Ok(inode.meta()))
    }

    fn read_at(&mut self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        self.with(|inode| {
            let from = at(offset).min(inode.data.len());
            let to = at(offset.saturating_add(len)).min(inode.data.len());
            Ok(inode.data.get(from..to).unwrap_or_default().to_vec())
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let pos = self.pos;
        let end = pos.saturating_add(wide(bytes.len()));
        self.with(|inode| {
            if inode.data.len() < at(end) {
                inode.data.resize(at(end), 0);
            }
            let slot = inode
                .data
                .get_mut(at(pos)..at(end))
                .ok_or_else(|| err(io::ErrorKind::InvalidInput))?;
            slot.copy_from_slice(bytes);
            inode.dirty.push((pos, end));
            Ok(())
        })?;
        self.pos = end;
        Ok(())
    }

    fn set_modified(&self, time: SystemTime) -> io::Result<()> {
        self.with(|inode| {
            inode.mtime = time;
            Ok(())
        })
    }

    fn sync_all(&self) -> io::Result<()> {
        self.with(|inode| {
            inode.synced = inode.data.clone();
            inode.synced_mtime = inode.mtime;
            inode.dirty.clear();
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
