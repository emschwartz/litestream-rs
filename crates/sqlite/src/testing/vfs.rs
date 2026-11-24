//! `FaultVfs`: a SQLite VFS that delegates to `unix` and fails calls on a schedule.
//!
//! Mechanism. [`FaultVfs::register`] leaks one `sqlite3_vfs` and its shared state for the life
//! of the process (SQLite keeps the pointer in its VFS list). Every file it opens is a
//! `FaultFile` header followed by the `unix` VFS's own `sqlite3_file`, in the one allocation of
//! `szOsFile` bytes SQLite makes; each io method checks the schedule, then calls the `unix`
//! method on the inner file. The io methods are version 2 (shared memory, no `xFetch`), so
//! SQLite never maps the database file and every write goes through `xWrite`. The `-shm` file
//! is the `unix` VFS's own business: it is opened inside `xShmMap`, not through `xOpen`.
//!
//! The schedule is a list of [`Fault`]s, each firing once, on the `nth` call of its operation on
//! its kind of file counted from when it was armed, and only while the counting window is open
//! ([`FaultVfs::set_window`]): the suite opens it around the engine's ticks, so an application
//! write between them never moves a count.
//!
//! Power loss ([`Effect::PowerLoss`], [`FaultVfs::cut_power`]). While
//! [`FaultVfs::track_durability`] is on, every write and truncate of the database or its WAL
//! first records the bytes it is about to replace and the range it touches, per path
//! (connections share a file's durability, as they share its inode), and a sync forgets them.
//! Journals and temporary files are not tracked. The loss applies the [`CrashLaw`]
//! (module `law`): under [`CrashLaw::AllOrNothing`], the default, it undoes the records newest
//! first and cuts each file back to its length at its last sync, and deletions stay done; under
//! [`CrashLaw::Blocks`] each dirty block, the length and each unsynced directory entry survive
//! or not independently. It then fails every later read, write, sync, truncate and delete until
//! [`FaultVfs::restore_power`]: the connections still open are the dead machine's, and nothing
//! they do may land. Lock calls still go through, so those connections close cleanly. Files that
//! were never written since tracking began are left as they are, which is what a sync at the
//! start would have made of them.
//!
//! Directory entries, under [`CrashLaw::Blocks`] only. A delete SQLite asks for without a
//! directory sync (`sqlite3WalClose` deletes the WAL with `syncDir` 0) and a database or WAL file
//! created since tracking began change the directory without syncing it; until the next sync of
//! the directory the change may be lost, and the loss then puts back what the entry durably held:
//! the deleted file as the law leaves it, or no file. The `unix` VFS syncs the directory on the
//! first sync of a WAL it created (`UNIXFILE_DIRSYNC`), so a WAL's sync makes its entry durable.
//! A directory sync on behalf of another file in the same directory is not seen, so the law may
//! put back an entry a real disk would have kept; the database's directory holds only the
//! database, its WAL and `-shm`, so nothing else syncs it. The `-shm` file is not tracked: the
//! first connection after the loss resets it and SQLite recovers the wal-index from the WAL.
//!
//! Disks that lie, while durability is tracked. [`FaultVfs::lie_on_syncs`] makes every sync
//! succeed without making anything durable: a drive that acknowledges a flush it never did.
//! A fault fired on a sync of the WAL loses that sync's writeback for good, as Linux does when
//! `fsync` fails: the kernel marks the dirty pages clean, so a later sync that succeeds does not
//! write them and a power loss puts back what the disk held before them. The lost ranges stay
//! lost until SQLite starts a new WAL generation (a header rewrite at offset 0, or a truncate to
//! 0), which overwrites them; a failed sync of the database file is not modelled this way, since
//! a checkpoint whose sync fails does not advance its backfill mark, so the next one copies the
//! same frames again.
//!
//! The suite's `SQLITE_TESTCTRL_PRNG_SEED` ([`seed_prng`]) pins SQLite's random numbers, so WAL
//! salts and `randomblob` repeat from run to run and a failing schedule replays.
//!
//! Test support only (the crate's `testing` feature). `unsafe` here is the C ABI: every callback
//! is one reasoned site, and the suite reaches each of them.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CString, c_char, c_int, c_void};
use std::io;
use std::num::{NonZeroI32, NonZeroU32};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::ptr::{self, NonNull};
use std::sync::{Mutex, MutexGuard, PoisonError};

use rusqlite::ffi;

mod law;

pub use law::{BlockSize, CrashLaw};
use law::{Rng, Undo, crash_image, write_at};

/// What a [`Effect::Hang`] prints on stdout before it blocks, for the parent process to wait on.
pub const HANG_LINE: &str = "lsr-fault-vfs: hung";

/// Which file a call is on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileKind {
    /// The main database file (its `-shm` locks count here too).
    Main,
    /// The write-ahead log.
    Wal,
    /// Anything else SQLite opens: journals, temporary files.
    Other,
}

/// Which call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Op {
    /// `xRead`.
    Read,
    /// `xWrite`.
    Write,
    /// `xSync`.
    Sync,
    /// `xTruncate`.
    Truncate,
    /// `xLock`, the database file's own locks.
    Lock,
    /// `xShmLock` taking a wal-index lock (a read mark, the write or checkpoint lock); unlocks
    /// never fail.
    ShmLock,
}

impl Op {
    /// The result code a plain failure of this call returns.
    #[must_use]
    pub const fn error(self) -> c_int {
        match self {
            Self::Read => ffi::SQLITE_IOERR_READ,
            Self::Write => ffi::SQLITE_IOERR_WRITE,
            Self::Sync => ffi::SQLITE_IOERR_FSYNC,
            Self::Truncate => ffi::SQLITE_IOERR_TRUNCATE,
            Self::Lock => ffi::SQLITE_IOERR_LOCK,
            // Shared-memory locks fail only by being held elsewhere.
            Self::ShmLock => ffi::SQLITE_BUSY,
        }
    }
}

/// What a fault does when it fires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    /// The call does nothing and returns this result code.
    Error(c_int),
    /// On a write: the first half of the buffer reaches the file and the call returns
    /// `SQLITE_FULL`, which is how the `unix` VFS answers a short `write(2)`. Elsewhere, the
    /// call's plain error.
    ShortWrite,
    /// On a sync, write or truncate: the power fails instead, before the call lands (module
    /// doc). Elsewhere, the call's plain error.
    PowerLoss,
    /// On a sync: the sync runs, then the calling thread prints [`HANG_LINE`] and blocks for
    /// ever, so a parent process can SIGKILL it at exactly that point. Elsewhere, the call's
    /// plain error.
    Hang,
}

/// One scheduled fault: the `nth` call of `op` on a `kind` file after it was armed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fault {
    /// The file.
    pub kind: FileKind,
    /// The call.
    pub op: Op,
    /// Which call, counted from one inside the window.
    pub nth: NonZeroU32,
    /// What happens.
    pub effect: Effect,
}

/// A fault that fired, and the file it fired on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fired {
    /// The fault.
    pub fault: Fault,
    /// The file's path, when it has one.
    pub path: Option<PathBuf>,
}

/// Why [`FaultVfs::register`] gave no VFS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum VfsError {
    /// The name holds a NUL byte.
    #[error("a VFS name cannot hold a NUL byte")]
    NameHasNul,
    /// This SQLite has no `unix` VFS to delegate to.
    #[error("SQLite has no unix VFS")]
    NoUnixVfs,
    /// `sqlite3_vfs_register` failed.
    #[error("sqlite3_vfs_register failed with {0}")]
    Register(c_int),
    /// A power loss could not undo a file's writes, so the files are not what a loss leaves.
    #[error("the power loss could not undo the writes: {0:?}")]
    Undo(io::ErrorKind),
    /// A query named a file no connection through this VFS ever opened, so there is no record
    /// to answer from (a path spelled differently resolves to the same record, see `known`).
    #[error("the VFS never saw this file")]
    UnknownFile,
}

#[derive(Debug)]
struct Pending {
    fault: Fault,
    seen: u32,
}

#[derive(Debug)]
struct State {
    pending: Vec<Pending>,
    fired: Vec<Fired>,
    window: bool,
    calls: BTreeMap<(FileKind, Op), u32>,
    syncs: BTreeMap<FileKind, u64>,
    paths: Vec<PathBuf>,
    tracking: bool,
    undo: BTreeMap<usize, Undo>,
    /// Paths of the database and WAL kinds, the ones durability tracks.
    tracked: BTreeSet<usize>,
    /// The WAL paths among them.
    wals: BTreeSet<usize>,
    /// Losses under the block law that kept some unsynced WAL block, and those that kept one
    /// after a block they lost.
    wal_losses: (u32, u32),
    law: CrashLaw,
    rng: Rng,
    /// Per path, the bytes it last held at each offset before an overwrite, truncate or delete:
    /// what a block the law keeps past the data written may show.
    ghost: BTreeMap<usize, Vec<u8>>,
    /// Paths whose directory entry changed since the directory's last sync, and what the entry
    /// durably holds: a file with these bytes, or none.
    entries: BTreeMap<usize, Option<Vec<u8>>>,
    powered: bool,
    undo_failed: Option<io::ErrorKind>,
    /// Syncs succeed and make nothing durable while this is set ([`FaultVfs::lie_on_syncs`]).
    lying_sync: bool,
    /// Per WAL path, the writes a failed sync covered: the kernel called those pages clean,
    /// so no later sync writes them, until the WAL starts a new generation.
    lost: BTreeMap<usize, Undo>,
    /// Per open database file (its `sqlite3_file` address), its path and where the unix VFS
    /// mapped region 0 of its wal-index: live from `xShmMap` until that file's `xShmUnmap`.
    shm: BTreeMap<usize, (Option<usize>, usize)>,
    /// What [`FaultVfs::after_next_wal_truncate`] armed.
    after_truncate: Option<AfterTruncate>,
}

/// A closure armed to run once, in the first wal-index lock request after the next truncate of a
/// WAL to nothing.
struct AfterTruncate {
    run: Box<dyn FnOnce() + Send>,
    /// Whether that truncate has happened.
    truncated: bool,
}

impl std::fmt::Debug for AfterTruncate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AfterTruncate")
            .field("truncated", &self.truncated)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct Shared {
    unix: NonNull<ffi::sqlite3_vfs>,
    name: CString,
    state: Mutex<State>,
}

/// What a call does after the schedule is consulted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Proceed,
    Act(Effect),
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn intern(&self, path: PathBuf, kind: FileKind) -> usize {
        let mut state = self.lock();
        let at = match state.paths.iter().position(|p| *p == path) {
            Some(at) => at,
            None => {
                state.paths.push(path);
                state.paths.len().saturating_sub(1)
            }
        };
        if kind != FileKind::Other {
            state.tracked.insert(at);
        }
        if kind == FileKind::Wal {
            state.wals.insert(at);
        }
        at
    }

    /// The record for `path`, spelled as the caller likes: SQLite hands the VFS fully resolved
    /// paths (symlinks followed, so `/var/...` is `/private/var/...` on macOS), so a caller's
    /// spelling is resolved the same way before the lookup. The directory is resolved and the
    /// file name kept, so a file SQLite has since deleted (a WAL after a clean close) is still
    /// found; a path whose directory is gone is looked up as given.
    fn known(&self, path: &std::path::Path) -> Option<usize> {
        let resolved = path
            .parent()
            .zip(path.file_name())
            .and_then(|(dir, name)| Some(std::fs::canonicalize(dir).ok()?.join(name)))
            .unwrap_or_else(|| path.to_path_buf());
        self.lock().paths.iter().position(|p| *p == resolved)
    }

    fn decide(&self, kind: FileKind, op: Op, path: Option<usize>) -> Decision {
        let mut state = self.lock();
        if !state.powered && !matches!(op, Op::Lock | Op::ShmLock) {
            return Decision::Act(Effect::Error(op.error()));
        }
        if !state.window {
            return Decision::Proceed;
        }
        let count = state.calls.entry((kind, op)).or_insert(0);
        *count = count.saturating_add(1);
        let mut hit = None;
        for (at, pending) in state.pending.iter_mut().enumerate() {
            if pending.fault.kind == kind && pending.fault.op == op {
                pending.seen = pending.seen.saturating_add(1);
                if hit.is_none() && pending.seen == pending.fault.nth.get() {
                    hit = Some(at);
                }
            }
        }
        let Some(at) = hit else {
            return Decision::Proceed;
        };
        let fault = state.pending.remove(at).fault;
        let path = path.and_then(|id| state.paths.get(id).cloned());
        state.fired.push(Fired { fault, path });
        Decision::Act(fault.effect)
    }

    fn tracking(&self, path: Option<usize>) -> Option<usize> {
        path.filter(|_| self.lock().tracking)
    }

    /// Records the bytes a write or truncate over `[offset, end)` replaces; `len_before` is the
    /// file's length then.
    fn record(&self, path: usize, len_before: u64, offset: u64, end: u64, old: Vec<u8>) {
        let mut state = self.lock();
        if !old.is_empty() {
            write_at(state.ghost.entry(path).or_default(), offset, &old);
        }
        let undo = state.undo.entry(path).or_insert_with(|| Undo {
            synced_len: len_before,
            ..Undo::default()
        });
        undo.dirty.push((offset, end));
        if !old.is_empty() {
            undo.replaced.push((offset, old));
        }
    }

    fn synced(&self, kind: FileKind, path: Option<usize>, len: Option<u64>) {
        let mut state = self.lock();
        let count = state.syncs.entry(kind).or_insert(0);
        *count = count.saturating_add(1);
        if let (Some(path), Some(len)) = (path, len) {
            state.undo.remove(&path);
            // The unix VFS syncs the directory on the first sync of a WAL it created.
            if kind == FileKind::Wal {
                state.entries.remove(&path);
            }
            if state.tracking {
                // Writes a failed sync lost stay undoable: this sync does not write them.
                let (replaced, dirty) = state
                    .lost
                    .get(&path)
                    .map(|lost| (lost.replaced.clone(), lost.dirty.clone()))
                    .unwrap_or_default();
                state.undo.insert(
                    path,
                    Undo {
                        synced_len: len,
                        replaced,
                        dirty,
                    },
                );
            }
        }
    }

    /// A sync of WAL `path` failed: the writes it covered are lost for good (`lost`). Bytes it
    /// covered past the synced length are lost as zeros, since nothing was there before.
    fn lose_writeback(&self, path: Option<usize>) {
        let mut state = self.lock();
        let Some(path) = path.filter(|path| state.tracking && state.wals.contains(path)) else {
            return;
        };
        let Some(undo) = state.undo.get(&path) else {
            return;
        };
        let mut replaced = undo.replaced.clone();
        for (from, to) in &undo.dirty {
            let from = (*from).max(undo.synced_len);
            if *to > from {
                let zeros = vec![0; usize::try_from(to.saturating_sub(from)).unwrap_or(0)];
                replaced.push((from, zeros));
            }
        }
        let dirty = undo.dirty.clone();
        let lost = state.lost.entry(path).or_default();
        lost.replaced.extend(replaced);
        lost.dirty.extend(dirty);
    }

    /// WAL `path` starts a new generation (its header is written again, or it is cut to
    /// nothing): what a failed sync lost belongs to the old one.
    fn new_generation(&self, path: Option<usize>) {
        if let Some(path) = path {
            self.lock().lost.remove(&path);
        }
    }

    /// Whether the block law's directory entries are tracked for `path`.
    fn tracks_entries(state: &State, path: usize) -> bool {
        state.tracking
            && state.tracked.contains(&path)
            && matches!(state.law, CrashLaw::Blocks { .. })
    }

    /// `path` was created by an open: until a sync of the directory, it may vanish.
    fn created(&self, path: usize) {
        let mut state = self.lock();
        if Self::tracks_entries(&state, path) {
            state.entries.entry(path).or_insert(None);
            state.undo.insert(path, Undo::default());
        }
    }

    /// `path` is about to be deleted, with the directory synced after or not.
    fn deleting(&self, path: usize, sync_dir: bool) {
        let mut state = self.lock();
        if !Self::tracks_entries(&state, path) {
            return;
        }
        let undo = state.undo.remove(&path).unwrap_or_default();
        let Some(file) = state.paths.get(path).cloned() else {
            return;
        };
        let now = match read_whole(&file) {
            Ok(now) => now,
            Err(err) => {
                state.undo_failed.get_or_insert(err.kind());
                return;
            }
        };
        if let Some(now) = &now {
            write_at(state.ghost.entry(path).or_default(), 0, now);
        }
        if sync_dir {
            state.entries.remove(&path);
        } else if !state.entries.contains_key(&path) {
            let left = now.map(|now| state.leave(path, &now, &undo));
            state.entries.insert(path, left);
        }
    }

    /// The power fails: the law decides what each file keeps (module doc).
    fn cut(&self) {
        let mut state = self.lock();
        if !state.powered {
            return;
        }
        state.powered = false;
        let undo = std::mem::take(&mut state.undo);
        let entries = std::mem::take(&mut state.entries);
        let all_or_nothing = state.law == CrashLaw::AllOrNothing;
        let paths: BTreeSet<usize> = undo.keys().chain(entries.keys()).copied().collect();
        let mut undo = undo;
        for id in paths {
            let Some(path) = state.paths.get(id).cloned() else {
                continue;
            };
            let done = if all_or_nothing {
                undo.remove(&id).map_or(Ok(()), |undo| apply(&path, &undo))
            } else {
                let file_undo = undo.remove(&id).unwrap_or_default();
                state.cut_blocks(id, &path, &file_undo, entries.get(&id))
            };
            match done {
                // Deleted since: a deletion is not something the all-or-nothing law undoes.
                Err(err) if err.kind() != io::ErrorKind::NotFound => {
                    state.undo_failed.get_or_insert(err.kind());
                }
                Ok(()) | Err(_) => {}
            }
        }
    }
}

impl State {
    /// What the block law leaves of a file that holds `now`.
    fn leave(&mut self, path: usize, now: &[u8], undo: &Undo) -> Vec<u8> {
        let CrashLaw::Blocks { block, .. } = self.law else {
            return law::synced_image(now, undo);
        };
        let ghost = self.ghost.get(&path).cloned().unwrap_or_default();
        crash_image(now, undo, &ghost, block, &mut self.rng)
    }

    /// The block law on one path: its entry (when unsynced) and then its blocks.
    fn cut_blocks(
        &mut self,
        id: usize,
        path: &std::path::Path,
        undo: &Undo,
        entry: Option<&Option<Vec<u8>>>,
    ) -> io::Result<()> {
        if let Some(durable) = entry
            && self.rng.coin()
        {
            return put(path, durable.as_deref());
        }
        match read_whole(path)? {
            Some(now) => {
                let left = self.leave(id, &now, undo);
                if let CrashLaw::Blocks { block, .. } = self.law
                    && self.wals.contains(&id)
                {
                    let old = law::synced_image(&now, undo);
                    let (kept, hole) = lsr_sim::crash_law::kept_and_holes(&old, &now, &left, block);
                    self.wal_losses.0 = self.wal_losses.0.saturating_add(u32::from(kept));
                    self.wal_losses.1 = self.wal_losses.1.saturating_add(u32::from(hole));
                }
                put(path, Some(&left))
            }
            None => Ok(()),
        }
    }
}

/// The whole file, or `None` when there is none.
fn read_whole(path: &std::path::Path) -> io::Result<Option<Vec<u8>>> {
    #[expect(
        clippy::disallowed_methods,
        reason = "only at a power loss or a delete: every connection on the file is dead, or the file is the WAL, which carries no POSIX lock"
    )]
    let read = std::fs::read(path);
    match read {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

/// Makes `path` hold `bytes`, or removes it.
fn put(path: &std::path::Path, bytes: Option<&[u8]>) -> io::Result<()> {
    match bytes {
        Some(bytes) => std::fs::write(path, bytes),
        None => match std::fs::remove_file(path) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            done => done,
        },
    }
}

fn apply(path: &std::path::Path, undo: &Undo) -> io::Result<()> {
    #[expect(
        clippy::disallowed_methods,
        reason = "power loss: every connection on this file is dead and about to be dropped, so the locks this close drops are already gone"
    )]
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    for (offset, bytes) in undo.replaced.iter().rev() {
        file.write_all_at(bytes, *offset)?;
    }
    file.set_len(undo.synced_len)
}

/// A registered fault-injecting VFS. Cheap to copy: every copy is the same VFS.
#[derive(Clone, Copy, Debug)]
pub struct FaultVfs {
    shared: &'static Shared,
}

/// The header of every file this VFS opens; the `unix` file follows it at [`REAL_OFFSET`].
#[repr(C)]
struct FaultFile {
    base: ffi::sqlite3_file,
    shared: *const Shared,
    kind: FileKind,
    path: Option<usize>,
}

/// Where the `unix` VFS's file starts. `FaultFile`'s size is a multiple of its 8-byte alignment,
/// which is all `unixFile` needs.
const REAL_OFFSET: usize = size_of::<FaultFile>();

fn real_of(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    file.cast::<u8>().wrapping_add(REAL_OFFSET).cast()
}

const fn kind_of(flags: c_int) -> FileKind {
    if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        FileKind::Main
    } else if flags & ffi::SQLITE_OPEN_WAL != 0 {
        FileKind::Wal
    } else {
        FileKind::Other
    }
}

impl FaultVfs {
    /// Registers a VFS named `name` that delegates to `unix`. It is never unregistered; a
    /// second registration of one name makes a second VFS, and SQLite finds the newer one.
    ///
    /// # Errors
    /// [`VfsError`].
    pub fn register(name: &str) -> Result<Self, VfsError> {
        let name = CString::new(name).map_err(|_nul| VfsError::NameHasNul)?;
        let unix = find_unix().ok_or(VfsError::NoUnixVfs)?;
        let shared: &'static Shared = Box::leak(Box::new(Shared {
            unix,
            name,
            state: Mutex::new(State {
                pending: Vec::new(),
                fired: Vec::new(),
                window: true,
                calls: BTreeMap::new(),
                syncs: BTreeMap::new(),
                paths: Vec::new(),
                tracking: false,
                undo: BTreeMap::new(),
                tracked: BTreeSet::new(),
                wals: BTreeSet::new(),
                wal_losses: (0, 0),
                law: CrashLaw::AllOrNothing,
                rng: Rng(0),
                ghost: BTreeMap::new(),
                entries: BTreeMap::new(),
                powered: true,
                undo_failed: None,
                lying_sync: false,
                lost: BTreeMap::new(),
                shm: BTreeMap::new(),
                after_truncate: None,
            }),
        }));
        let vfs = Self { shared };
        register(vfs.build()?)?;
        Ok(vfs)
    }

    fn build(self) -> Result<&'static mut ffi::sqlite3_vfs, VfsError> {
        let unix = unix_vfs(self.shared);
        let size = usize::try_from(unix.szOsFile)
            .ok()
            .and_then(|size| size.checked_add(REAL_OFFSET))
            .and_then(|size| c_int::try_from(size).ok())
            .ok_or(VfsError::NoUnixVfs)?;
        Ok(Box::leak(Box::new(ffi::sqlite3_vfs {
            // Version 2: xCurrentTimeInt64, and no system-call overrides.
            iVersion: 2,
            szOsFile: size,
            mxPathname: unix.mxPathname,
            pNext: ptr::null_mut(),
            zName: self.shared.name.as_ptr(),
            pAppData: ptr::from_ref(self.shared).cast_mut().cast::<c_void>(),
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            // SQLite loads no extension here; it draws randomness from the default VFS only
            // (`sqlite3_randomness`), reads the time through xCurrentTimeInt64 when a version 2
            // VFS has it, and checks xGetLastError for null before calling it.
            xDlOpen: None,
            xDlError: None,
            xDlSym: None,
            xDlClose: None,
            xRandomness: None,
            xSleep: Some(x_sleep),
            xCurrentTime: None,
            xGetLastError: None,
            xCurrentTimeInt64: Some(x_current_time),
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        })))
    }

    /// The name to open connections with.
    #[must_use]
    pub fn name(&self) -> &str {
        self.shared.name.to_str().unwrap_or_default()
    }

    /// Schedules `fault`; its count starts now.
    pub fn arm(&self, fault: Fault) {
        self.shared.lock().pending.push(Pending { fault, seen: 0 });
    }

    /// Runs `run` once, at the start of the first wal-index lock request (by any connection)
    /// after a WAL is next truncated to nothing. After a TRUNCATE checkpoint that is the moment
    /// its connection has released every lock and not yet begun its next read: a commit made
    /// there lands between a checkpoint and the read transaction retaken after it, inside one
    /// call into SQLite. Nothing is held while `run` runs, so it may use other connections
    /// through this VFS; their own lock requests do not run it again.
    pub fn after_next_wal_truncate(&self, run: impl FnOnce() + Send + 'static) {
        self.shared.lock().after_truncate = Some(AfterTruncate {
            run: Box::new(run),
            truncated: false,
        });
    }

    /// Drops every fault not fired yet.
    pub fn disarm(&self) {
        self.shared.lock().pending.clear();
    }

    /// Opens or closes the counting window (open when registered): calls outside it neither
    /// count nor fire.
    pub fn set_window(&self, open: bool) {
        self.shared.lock().window = open;
    }

    /// Calls of `op` on `kind` files inside the window since registration or [`Self::reset_calls`].
    #[must_use]
    pub fn calls(&self, kind: FileKind, op: Op) -> u32 {
        self.shared
            .lock()
            .calls
            .get(&(kind, op))
            .copied()
            .unwrap_or(0)
    }

    /// Every count [`Self::calls`] would give.
    #[must_use]
    pub fn all_calls(&self) -> BTreeMap<(FileKind, Op), u32> {
        self.shared.lock().calls.clone()
    }

    /// Zeroes [`Self::calls`].
    pub fn reset_calls(&self) {
        self.shared.lock().calls.clear();
    }

    /// Syncs of `kind` files that succeeded, inside the window or not.
    #[must_use]
    pub fn syncs(&self, kind: FileKind) -> u64 {
        self.shared.lock().syncs.get(&kind).copied().unwrap_or(0)
    }

    /// Every fault that fired, oldest first.
    #[must_use]
    pub fn fired(&self) -> Vec<Fired> {
        self.shared.lock().fired.clone()
    }

    /// Starts recording what each write replaces, so a power loss can undo it. What the files
    /// hold now counts as synced.
    pub fn track_durability(&self) {
        let mut state = self.shared.lock();
        state.tracking = true;
        state.undo.clear();
        state.entries.clear();
    }

    /// Sets what a power loss keeps ([`CrashLaw`]); [`CrashLaw::AllOrNothing`] until then.
    pub fn set_crash_law(&self, law: CrashLaw) {
        let mut state = self.shared.lock();
        state.law = law;
        state.rng = Rng(match law {
            CrashLaw::AllOrNothing => 0,
            CrashLaw::Blocks { seed, .. } => seed,
        });
    }

    /// While `on`, every sync returns success and makes nothing durable, as on a disk that lies
    /// about fsync. The simulator's backend turns it on around the engine's own sync of the WAL
    /// only, so SQLite's own syncs still hold.
    pub fn lie_on_syncs(&self, on: bool) {
        self.shared.lock().lying_sync = on;
    }

    /// Whether bytes `[start, end)` of the file at `path` survive a power loss now under
    /// [`CrashLaw::AllOrNothing`]: no write since its last sync touched them, counting the
    /// writes a failed sync lost. Durability not tracked at all loses nothing.
    ///
    /// # Errors
    /// [`VfsError::UnknownFile`] for a file no connection through this VFS opened: with no
    /// record, "durable" would be a guess that passes whatever is asked.
    pub fn durable(&self, path: &std::path::Path, start: u64, end: u64) -> Result<bool, VfsError> {
        let at = self.shared.known(path).ok_or(VfsError::UnknownFile)?;
        let state = self.shared.lock();
        Ok(state.undo.get(&at).is_none_or(|undo| {
            undo.dirty
                .iter()
                .all(|(from, to)| *to <= start || *from >= end)
        }))
    }

    /// The first `N` bytes of the wal-index of the database at `db`, read from the mapping the
    /// unix VFS made for a connection that has it open: SQLite's shared memory as every
    /// connection sees it. `None` while no connection through this VFS has it mapped.
    #[must_use]
    #[expect(
        unsafe_code,
        reason = "reads the unix VFS's live mapping of wal-index region 0"
    )]
    pub fn shm_head<const N: usize>(&self, db: &std::path::Path) -> Option<[u8; N]> {
        let at = self.shared.known(db)?;
        let state = self.shared.lock();
        let (_path, ptr) = state.shm.values().find(|(path, _)| *path == Some(at))?;
        let mut out = [0; N];
        if N > 32 * 1024 {
            return None;
        }
        let src = std::ptr::with_exposed_provenance::<u8>(*ptr);
        // SAFETY: region 0 is 32 KiB and stays mapped until the file's `xShmUnmap`, which
        // removes the entry under the same lock held here; `N` fits in it.
        unsafe { std::ptr::copy_nonoverlapping(src, out.as_mut_ptr(), N) };
        Some(out)
    }

    /// Whether a failed sync of a WAL lost writeback that its next generation has not yet
    /// overwritten.
    #[must_use]
    pub fn writeback_lost(&self) -> bool {
        !self.shared.lock().lost.is_empty()
    }

    /// The power fails now, between calls (module doc): what an [`Effect::PowerLoss`] does
    /// when its call comes.
    pub fn cut_power(&self) {
        self.shared.cut();
    }

    /// Power losses under the block law that kept some unsynced WAL block, and those among
    /// them that kept one after a block they lost.
    #[must_use]
    pub fn wal_block_losses(&self) -> (u32, u32) {
        self.shared.lock().wal_losses
    }

    /// Whether the power is on.
    #[must_use]
    pub fn powered(&self) -> bool {
        self.shared.lock().powered
    }

    /// Turns the power back on after an [`Effect::PowerLoss`]: the files are what the loss left,
    /// and all of it counts as synced. Open new connections after this; the old ones are dead.
    ///
    /// # Errors
    /// [`VfsError::Undo`] when the loss could not undo a file's writes.
    pub fn restore_power(&self) -> Result<(), VfsError> {
        let mut state = self.shared.lock();
        state.powered = true;
        state.undo.clear();
        state.entries.clear();
        state.lost.clear();
        state
            .undo_failed
            .take()
            .map_or(Ok(()), |kind| Err(VfsError::Undo(kind)))
    }
}

/// Pins SQLite's random numbers to `seed` (`SQLITE_TESTCTRL_PRNG_SEED`), so the WAL salts and
/// `randomblob` repeat. The generator is process-wide.
#[expect(unsafe_code, reason = "sqlite3_test_control is a C variadic function")]
pub fn seed_prng(seed: NonZeroI32) {
    // SAFETY: PRNG_SEED reads an `int` and a `sqlite3*`, which may be null (`sqlite3.c:192676`).
    let _rc = unsafe {
        ffi::sqlite3_test_control(
            ffi::SQLITE_TESTCTRL_PRNG_SEED,
            seed.get(),
            ptr::null_mut::<ffi::sqlite3>(),
        )
    };
}

#[expect(
    unsafe_code,
    reason = "sqlite3_vfs_find is C; the unix VFS it returns is static"
)]
fn find_unix() -> Option<NonNull<ffi::sqlite3_vfs>> {
    // SAFETY: a NUL-terminated name; the call initialises SQLite first and returns a VFS
    // that lives as long as the process, or null.
    NonNull::new(unsafe { ffi::sqlite3_vfs_find(c"unix".as_ptr()) })
}

#[expect(unsafe_code, reason = "sqlite3_vfs_register keeps the pointer")]
fn register(vfs: &'static mut ffi::sqlite3_vfs) -> Result<(), VfsError> {
    // SAFETY: the VFS and everything it points to are leaked, so they outlive SQLite's list.
    let rc = unsafe { ffi::sqlite3_vfs_register(vfs, 0) };
    if rc == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(VfsError::Register(rc))
    }
}

#[expect(unsafe_code, reason = "the unix VFS is C memory")]
fn unix_vfs(shared: &Shared) -> &'static ffi::sqlite3_vfs {
    // SAFETY: `sqlite3_vfs_find` returned it; SQLite's built-in VFS objects are static and
    // never written after initialisation.
    unsafe { shared.unix.as_ref() }
}

#[expect(unsafe_code, reason = "a VFS callback's own pointer")]
fn shared_of_vfs(vfs: *mut ffi::sqlite3_vfs) -> &'static Shared {
    // SAFETY: SQLite passes the VFS it called, which `build` made with `pAppData` set to a
    // leaked `Shared`.
    let data = unsafe { (*vfs).pAppData };
    // SAFETY: as above; the `Shared` is never freed.
    unsafe { &*data.cast::<Shared>() }
}

/// The header of a file this VFS opened.
#[expect(
    unsafe_code,
    reason = "an io method's file is C memory this VFS laid out"
)]
fn header(file: *mut ffi::sqlite3_file) -> (&'static Shared, FileKind, Option<usize>) {
    // SAFETY: SQLite calls these methods only on files `x_open` opened, which start with a
    // `FaultFile` it wrote; SQLite serialises calls on one file.
    let head = unsafe { &*file.cast::<FaultFile>() };
    // SAFETY: `x_open` stored a leaked `Shared`.
    let shared = unsafe { &*head.shared };
    (shared, head.kind, head.path)
}

fn head_path(file: *mut ffi::sqlite3_file) -> Option<usize> {
    header(file).2
}

#[expect(unsafe_code, reason = "the inner unix file's method table is C memory")]
fn methods(file: *mut ffi::sqlite3_file) -> Option<&'static ffi::sqlite3_io_methods> {
    let real = real_of(file);
    // SAFETY: the unix file follows the header inside SQLite's allocation; `x_open` succeeded,
    // so its `pMethods` points to one of the unix VFS's static tables.
    let table = unsafe { (*real).pMethods };
    // SAFETY: as above: null or a static table.
    unsafe { table.as_ref() }
}

#[expect(unsafe_code, reason = "a call through the unix file's xRead")]
fn real_read(file: *mut ffi::sqlite3_file, buf: &mut [u8], offset: u64) -> c_int {
    let (Some(read), Ok(amount), Ok(offset)) = (
        methods(file).and_then(|m| m.xRead),
        c_int::try_from(buf.len()),
        i64::try_from(offset),
    ) else {
        return ffi::SQLITE_IOERR_READ;
    };
    // SAFETY: the method belongs to the live unix file; `buf` holds `amount` bytes.
    unsafe {
        read(
            real_of(file),
            buf.as_mut_ptr().cast::<c_void>(),
            amount,
            offset,
        )
    }
}

#[expect(unsafe_code, reason = "a call through the unix file's xWrite")]
fn real_write(file: *mut ffi::sqlite3_file, buf: &[u8], offset: u64) -> c_int {
    let (Some(write), Ok(amount), Ok(offset)) = (
        methods(file).and_then(|m| m.xWrite),
        c_int::try_from(buf.len()),
        i64::try_from(offset),
    ) else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    // SAFETY: the method belongs to the live unix file; `buf` holds `amount` bytes it only reads.
    unsafe { write(real_of(file), buf.as_ptr().cast::<c_void>(), amount, offset) }
}

#[expect(unsafe_code, reason = "a call through the unix file's xFileSize")]
fn real_size(file: *mut ffi::sqlite3_file) -> Option<u64> {
    let size = methods(file)?.xFileSize?;
    let mut out: i64 = 0;
    // SAFETY: the method belongs to the live unix file and writes one `i64`.
    let rc = unsafe { size(real_of(file), &raw mut out) };
    (rc == ffi::SQLITE_OK).then_some(())?;
    u64::try_from(out).ok()
}

/// Records what a write or truncate over `[from, to)` replaces, while durability is tracked; a
/// truncate passes `u64::MAX` as `to`.
fn remember(file: *mut ffi::sqlite3_file, from: u64, to: u64) {
    let (shared, kind, path) = header(file);
    let Some(path) = shared.tracking(path).filter(|_| kind != FileKind::Other) else {
        return;
    };
    let Some(len) = real_size(file) else {
        return;
    };
    let end = to.min(len);
    let mut old = vec![0_u8; usize::try_from(end.saturating_sub(from)).unwrap_or(0)];
    if !old.is_empty() && real_read(file, &mut old, from) != ffi::SQLITE_OK {
        old.clear();
    }
    // A truncate (`to` unbounded) touches what it cuts; a write, what it covers.
    let touched = if to == u64::MAX { len } else { to };
    shared.record(path, len, from, touched, old);
}

fn hang() -> ! {
    use std::io::Write;
    let mut out = io::stdout().lock();
    drop(writeln!(out, "{HANG_LINE}"));
    drop(out.flush());
    drop(out);
    loop {
        std::thread::park();
    }
}

#[expect(unsafe_code, reason = "the VFS's xOpen callback")]
extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let shared = shared_of_vfs(vfs);
    let path = (!name.is_null()).then(|| {
        // SAFETY: a non-null name SQLite passes is NUL-terminated.
        let bytes = unsafe { std::ffi::CStr::from_ptr(name) }.to_bytes();
        PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
    });
    let kind = kind_of(flags);
    let creates = flags & ffi::SQLITE_OPEN_CREATE != 0
        && path
            .as_deref()
            .is_some_and(|path| std::fs::symlink_metadata(path).is_err());
    let head = FaultFile {
        base: ffi::sqlite3_file {
            pMethods: ptr::null(),
        },
        shared: ptr::from_ref(shared),
        kind,
        path: path.map(|path| shared.intern(path, kind)),
    };
    // SAFETY: SQLite hands `szOsFile` writable bytes, which `build` sized for the header.
    unsafe { file.cast::<FaultFile>().write(head) };
    let Some(open) = unix_vfs(shared).xOpen else {
        return ffi::SQLITE_CANTOPEN;
    };
    // SAFETY: the unix VFS opens its own file in the bytes after the header, which `build`
    // sized as its `szOsFile`; the other arguments are SQLite's own.
    let rc = unsafe { open(shared.unix.as_ptr(), name, real_of(file), flags, out_flags) };
    if rc == ffi::SQLITE_OK {
        // SAFETY: the header was written above; SQLite calls xClose once this is set.
        unsafe { (*file).pMethods = &raw const METHODS };
        if creates && let Some(id) = head_path(file) {
            shared.created(id);
        }
    }
    rc
}

#[expect(unsafe_code, reason = "the VFS's xDelete callback")]
extern "C" fn x_delete(vfs: *mut ffi::sqlite3_vfs, name: *const c_char, sync_dir: c_int) -> c_int {
    let shared = shared_of_vfs(vfs);
    if !shared.lock().powered {
        return ffi::SQLITE_IOERR_DELETE;
    }
    let Some(delete) = unix_vfs(shared).xDelete else {
        return ffi::SQLITE_IOERR_DELETE;
    };
    if !name.is_null() {
        // SAFETY: a non-null name SQLite passes is NUL-terminated.
        let bytes = unsafe { std::ffi::CStr::from_ptr(name) }.to_bytes();
        let path = std::path::Path::new(std::ffi::OsStr::from_bytes(bytes));
        if let Some(id) = shared.known(path) {
            shared.deleting(id, sync_dir != 0);
        }
    }
    // SAFETY: SQLite's own arguments, passed on to the VFS it would have called.
    unsafe { delete(shared.unix.as_ptr(), name, sync_dir) }
}

#[expect(unsafe_code, reason = "the VFS's xAccess callback")]
extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let shared = shared_of_vfs(vfs);
    let Some(access) = unix_vfs(shared).xAccess else {
        return ffi::SQLITE_IOERR_ACCESS;
    };
    // SAFETY: SQLite's own arguments, passed on to the VFS it would have called.
    unsafe { access(shared.unix.as_ptr(), name, flags, out) }
}

#[expect(unsafe_code, reason = "the VFS's xFullPathname callback")]
extern "C" fn x_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    let shared = shared_of_vfs(vfs);
    let Some(full) = unix_vfs(shared).xFullPathname else {
        return ffi::SQLITE_CANTOPEN;
    };
    // SAFETY: SQLite's own arguments, passed on to the VFS it would have called.
    unsafe { full(shared.unix.as_ptr(), name, size, out) }
}

#[expect(unsafe_code, reason = "the VFS's xSleep callback")]
extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    let shared = shared_of_vfs(vfs);
    let Some(sleep) = unix_vfs(shared).xSleep else {
        return 0;
    };
    // SAFETY: SQLite's own argument, passed on to the VFS it would have called.
    unsafe { sleep(shared.unix.as_ptr(), micros) }
}

#[expect(unsafe_code, reason = "the VFS's xCurrentTimeInt64 callback")]
extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, out: *mut i64) -> c_int {
    let shared = shared_of_vfs(vfs);
    let Some(now) = unix_vfs(shared).xCurrentTimeInt64 else {
        return ffi::SQLITE_ERROR;
    };
    // SAFETY: SQLite's own argument, passed on to the VFS it would have called.
    unsafe { now(shared.unix.as_ptr(), out) }
}

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

#[expect(unsafe_code, reason = "the io method xClose")]
extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let Some(close) = methods(file).and_then(|m| m.xClose) else {
        return ffi::SQLITE_OK;
    };
    // SAFETY: the unix file is live until this call, which is its last.
    unsafe { close(real_of(file)) }
}

#[expect(unsafe_code, reason = "the io method xRead")]
extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    let (shared, kind, path) = header(file);
    if let Decision::Act(effect) = shared.decide(kind, Op::Read, path) {
        return plain(effect, Op::Read);
    }
    let Some(read) = methods(file).and_then(|m| m.xRead) else {
        return ffi::SQLITE_IOERR_READ;
    };
    // SAFETY: SQLite's own arguments, passed on to the unix file under the header.
    unsafe { read(real_of(file), buf, amount, offset) }
}

/// The result of an effect that has no special meaning for `op`.
const fn plain(effect: Effect, op: Op) -> c_int {
    match effect {
        Effect::Error(rc) => rc,
        Effect::ShortWrite | Effect::PowerLoss | Effect::Hang => op.error(),
    }
}

#[expect(unsafe_code, reason = "the io method xWrite")]
extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    let (shared, kind, path) = header(file);
    let (Ok(len), Ok(at)) = (usize::try_from(amount), u64::try_from(offset)) else {
        return ffi::SQLITE_IOERR_WRITE;
    };
    // SAFETY: SQLite passes `amount` readable bytes at `buf`.
    let bytes = unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), len) };
    let end = at.saturating_add(u64::try_from(len).unwrap_or(u64::MAX));
    match shared.decide(kind, Op::Write, path) {
        Decision::Act(Effect::ShortWrite) => {
            let half = bytes
                .get(..len.checked_div(2).unwrap_or(0))
                .unwrap_or_default();
            remember(file, at, end);
            let rc = real_write(file, half, at);
            if rc == ffi::SQLITE_OK {
                ffi::SQLITE_FULL
            } else {
                rc
            }
        }
        Decision::Act(Effect::PowerLoss) => {
            shared.cut();
            ffi::SQLITE_IOERR_WRITE
        }
        Decision::Act(effect) => plain(effect, Op::Write),
        Decision::Proceed => {
            if at == 0 && kind == FileKind::Wal {
                shared.new_generation(path);
            }
            remember(file, at, end);
            real_write(file, bytes, at)
        }
    }
}

#[expect(unsafe_code, reason = "the io method xTruncate")]
extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    let (shared, kind, path) = header(file);
    match shared.decide(kind, Op::Truncate, path) {
        Decision::Act(Effect::PowerLoss) => {
            shared.cut();
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        Decision::Act(effect) => return plain(effect, Op::Truncate),
        Decision::Proceed => {}
    }
    let Some(truncate) = methods(file).and_then(|m| m.xTruncate) else {
        return ffi::SQLITE_IOERR_TRUNCATE;
    };
    if size == 0 && kind == FileKind::Wal {
        shared.new_generation(path);
    }
    remember(file, u64::try_from(size).unwrap_or(0), u64::MAX);
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    let rc = unsafe { truncate(real_of(file), size) };
    if rc == ffi::SQLITE_OK
        && size == 0
        && kind == FileKind::Wal
        && let Some(armed) = shared.lock().after_truncate.as_mut()
    {
        armed.truncated = true;
    }
    rc
}

#[expect(unsafe_code, reason = "the io method xSync")]
extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    let (shared, kind, path) = header(file);
    let effect = match shared.decide(kind, Op::Sync, path) {
        Decision::Act(Effect::PowerLoss) => {
            shared.cut();
            return ffi::SQLITE_IOERR_FSYNC;
        }
        Decision::Act(Effect::Hang) => Some(Effect::Hang),
        Decision::Act(effect) => {
            if kind == FileKind::Wal {
                shared.lose_writeback(path);
            }
            return plain(effect, Op::Sync);
        }
        Decision::Proceed => None,
    };
    if shared.lock().lying_sync {
        return ffi::SQLITE_OK;
    }
    let Some(sync) = methods(file).and_then(|m| m.xSync) else {
        return ffi::SQLITE_IOERR_FSYNC;
    };
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    let rc = unsafe { sync(real_of(file), flags) };
    if rc == ffi::SQLITE_OK {
        shared.synced(kind, path, real_size(file));
    }
    if effect == Some(Effect::Hang) {
        hang();
    }
    rc
}

#[expect(unsafe_code, reason = "the io method xFileSize")]
extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, out: *mut i64) -> c_int {
    let Some(size) = methods(file).and_then(|m| m.xFileSize) else {
        return ffi::SQLITE_IOERR_FSTAT;
    };
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    unsafe { size(real_of(file), out) }
}

#[expect(unsafe_code, reason = "the io method xLock")]
extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let (shared, kind, path) = header(file);
    if let Decision::Act(effect) = shared.decide(kind, Op::Lock, path) {
        return plain(effect, Op::Lock);
    }
    let Some(lock) = methods(file).and_then(|m| m.xLock) else {
        return ffi::SQLITE_IOERR_LOCK;
    };
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    unsafe { lock(real_of(file), level) }
}

#[expect(unsafe_code, reason = "the io method xUnlock")]
extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    let Some(unlock) = methods(file).and_then(|m| m.xUnlock) else {
        return ffi::SQLITE_IOERR_UNLOCK;
    };
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    unsafe { unlock(real_of(file), level) }
}

#[expect(unsafe_code, reason = "the io method xCheckReservedLock")]
extern "C" fn x_check_reserved_lock(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    let Some(check) = methods(file).and_then(|m| m.xCheckReservedLock) else {
        return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK;
    };
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    unsafe { check(real_of(file), out) }
}

#[expect(unsafe_code, reason = "the io method xFileControl")]
extern "C" fn x_file_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    let Some(control) = methods(file).and_then(|m| m.xFileControl) else {
        return ffi::SQLITE_NOTFOUND;
    };
    // SAFETY: SQLite's own arguments, passed on to the unix file under the header.
    unsafe { control(real_of(file), op, arg) }
}

#[expect(unsafe_code, reason = "the io method xSectorSize")]
extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    let Some(sector) = methods(file).and_then(|m| m.xSectorSize) else {
        return 4096;
    };
    // SAFETY: the unix file under the header is live.
    unsafe { sector(real_of(file)) }
}

#[expect(unsafe_code, reason = "the io method xDeviceCharacteristics")]
extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    let Some(device) = methods(file).and_then(|m| m.xDeviceCharacteristics) else {
        return 0;
    };
    // SAFETY: the unix file under the header is live.
    unsafe { device(real_of(file)) }
}

#[expect(unsafe_code, reason = "the io method xShmMap")]
extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    let Some(map) = methods(file).and_then(|m| m.xShmMap) else {
        return ffi::SQLITE_IOERR_SHMMAP;
    };
    // SAFETY: SQLite's own arguments, passed on to the unix file under the header.
    let rc = unsafe { map(real_of(file), region, size, extend, out) };
    if rc == ffi::SQLITE_OK && region == 0 && !out.is_null() {
        // SAFETY: `out` is SQLite's own out-pointer, which the unix method just wrote.
        let at = unsafe { *out };
        if !at.is_null() {
            let (shared, _kind, path) = header(file);
            shared
                .lock()
                .shm
                .insert(file.addr(), (path, at.expose_provenance()));
        }
    }
    rc
}

#[expect(unsafe_code, reason = "the io method xShmLock")]
extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    let (shared, kind, path) = header(file);
    if flags & ffi::SQLITE_SHM_UNLOCK == 0 {
        let armed = {
            let mut state = shared.lock();
            if state.after_truncate.as_ref().is_some_and(|a| a.truncated) {
                state.after_truncate.take()
            } else {
                None
            }
        };
        if let Some(armed) = armed {
            (armed.run)();
        }
    }
    // Only a request can fail: SQLite ignores what an unlock returns, so a failed unlock would
    // leave a lock held that no connection knows it holds.
    if flags & ffi::SQLITE_SHM_UNLOCK == 0
        && let Decision::Act(effect) = shared.decide(kind, Op::ShmLock, path)
    {
        return plain(effect, Op::ShmLock);
    }
    let Some(lock) = methods(file).and_then(|m| m.xShmLock) else {
        return ffi::SQLITE_IOERR_SHMLOCK;
    };
    // SAFETY: SQLite's own arguments, passed on to the unix file under the header.
    unsafe { lock(real_of(file), offset, n, flags) }
}

#[expect(unsafe_code, reason = "the io method xShmBarrier")]
extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    if let Some(barrier) = methods(file).and_then(|m| m.xShmBarrier) {
        // SAFETY: the unix file under the header is live.
        unsafe { barrier(real_of(file)) };
    }
}

#[expect(unsafe_code, reason = "the io method xShmUnmap")]
extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    header(file).0.lock().shm.remove(&file.addr());
    let Some(unmap) = methods(file).and_then(|m| m.xShmUnmap) else {
        return ffi::SQLITE_OK;
    };
    // SAFETY: SQLite's own argument, passed on to the unix file under the header.
    unsafe { unmap(real_of(file), delete) }
}

#[cfg(test)]
mod tests;
