# Pure-Rust pin and checkpoint for litestream-rs: what Turso shows, what C SQLite needs

Tags: [M] = measured (I read the code or ran it), [I] = inferred.
Turso clone: scratch/turso @ 936484848db2 (2026-11-20). C SQLite: libsqlite3-sys-0.40.0 bundled amalgamation, SQLITE_VERSION "3.54.1" (sqlite3.c:470) [M].

## Verdict
- The guess ("not that much") holds for the PIN in deployments A (Linux and macOS, via OFD locks) and B: about 300-450 lines of Rust plus tests [I, sized from the C below]. It does NOT hold for the CHECKPOINT: about 1,200-1,800 lines of Rust that must match a protocol C SQLite was still fixing races in this year (2026-03), and a bug there corrupts the live database [M for the history, I for the size].
- Turso is no reference for interop. It never reads or writes the -shm file or its lock bytes [M]. Its line counts measure a different problem.

## 1. Turso's WAL code [M]
Sizes: core/storage/wal.rs 10,403 lines, core/storage/shared_wal_coordination.rs 4,728, core/storage/pager.rs 7,562, core/io/unix.rs 624, core/multiprocess_tests.rs 1,186.
(a) Read-mark / read-lock acquisition
  - TursoRwLock, an in-process atomic RwLock with a value slot: wal.rs:280-458 (~180)
  - InProcessWalCoordination::try_begin_read_tx: wal.rs:1036-1110 (75). It mirrors SQLite's walTryBeginRead: slot 0 when fully backfilled, otherwise the highest mark <= max_frame, else take a slot exclusively and set mark=max_frame, then re-validate the snapshot.
  - ShmWalCoordination::try_begin_read_tx: wal.rs:2101-2167 (67). Same algorithm plus publish_shared_reader to .tshm.
  - WalFile::try_begin_read_tx / begin_read_tx: wal.rs:3407-3529 (~120)
(b) Checkpoint
  - CheckpointMode (Passive/Full/Restart/Truncate, with upper bounds): wal.rs:160-191
  - checkpoint_inner state machine: wal.rs:4990-5345 (356)
  - restart / truncate log: wal.rs:5357-5465 (~110)
  - acquire_checkpoint_guard: in-process wal.rs:1127-1205, shm wal.rs:2191-2296
  - determine_max_safe_checkpoint_frame: wal.rs:1206-1231, 2297-2329
  - OngoingCheckpoint batching: wal.rs:2699-2838
  - Total about 850 lines, async state machines included.
(c) Wal-index
  - Single-process: an in-memory frame cache in WalFileShared (wal.rs:3050), shared through a process registry.
  - Multi-process: its own .tshm file, "TSHMWAL\0" magic, own layout, own lock bytes 0..3+N (shared_wal_coordination.rs:1-24, 38-71). Path is "{db}-tshm" (wal.rs:120-127).
(d) OS locking
  - unix_shared_wal_lock_byte / unlock: unix.rs:124-215 (~90). Raw libc::fcntl in unsafe blocks; F_OFD_SETLK on Linux, F_SETLK ("ProcessScopedFcntl") elsewhere (shared_wal_coordination.rs:618-626, 763-767).
  - Plus mmap via libc (unix.rs:220-270, unsafe).
  - The main db file takes an EXCLUSIVE whole-file fcntl lock at open (unix.rs:67-71, 277-303) unless NoLock, which multiprocess mode sets (database.rs ~1084).

## 2. Interop with live C SQLite [M]
None.
- COMPAT.md:71: "We don't support mixed SQLite and Turso in multi-process scenarios."
- No code path in core/ opens "-shm". The grep hits are test names only.
- Single-process Turso holds an exclusive lock on the db file, so C SQLite cannot share it at all.
- Multi-process Turso uses .tshm, which C SQLite never looks at. Multi-process mode is labelled "experimental multiprocess WAL" (database.rs:1063-1110).

Missing for our problem:
- the -shm layout: two 48-byte header copies plus checksum, WalCkptInfo, aReadMark[5], lock bytes 120..127, DMS at 128, 32 KiB index pages with hash tables
- the POSIX/OFD lock protocol against another library in the same process
- recovery hand-off
- the db-file lock interplay

What Turso IS good for as a reference:
- the read-mark selection algorithm
- the checkpoint state machine shape: modes, upper bounds, max-safe-frame, restart
- a ~90-line OFD/fcntl byte-lock wrapper

## 3. C SQLite: what pin and checkpoint need [M line ranges in sqlite3.c]
Locks and constants:
- WAL_WRITE_LOCK 0, CKPT 1, RECOVER 2, READ(I)=3+I, WAL_NREADER=5 (67166-67171)
- Lock bytes start at offset 120 (UNIX_SHM_BASE); DMS at 128
- WAL_MAX_VERSION = WALINDEX_MAX_VERSION = 3007000 (67139-67150)
- WalIndexHdr at 67193; WalCkptInfo at 67266, with the read-mark rules in comments 67220-67264
Functions:
| fn | lines | len |
|---|---|---|
| walIndexTryHdr | 69544-69594 | 51 |
| walIndexReadHdr | 69614-69717 | 104 |
| walTryBeginRead | 69974-70226 | 253 |
| walBeginReadTransaction | 70328-70431 | 104 |
| sqlite3WalEndReadTransaction | 70460-70469 | 10 |
| walBusyLock | 69061-69079 | 19 |
| walRestartHdr | 69106-69120 | 15 |
| walCheckpoint | 69153-69349 | 197 |
| walLimitSize | 69355-69367 | 13 |
| sqlite3WalCheckpoint | 71259-71390 | 132 |
| walIteratorInit | 68872-68971 | 100 |
| walMerge+walMergesort+walIteratorNext | 68735-68850, 67467-67504 | ~136 |
| walHashGet/walFramePage/walIndexPage(Realloc) | 67660-68117 | ~80 |
| walIndexRecover (NOT needed if we refuse to recover) | 68294-68518 | 225 |
| walRestartLog (writer side, informs pin) | 70833-70873 | 41 |
| unixShmLock | 45938-46130 | 193 |
| unixShmMap | 45760-45893 | 134 |
| unixLockSharedMemory (DMS) | 45504-45569 | 66 |

Pin
- C path: TryHdr + ReadHdr (no recovery) + the TryBeginRead core, about 250 C lines of logic once SETLK_TIMEOUT, snapshot and heap-memory branches are dropped.
- Rust estimate: about 300-450 lines [I]:
  - open -shm (never truncate, never close)
  - shared OFD lock on DMS
  - pread both header copies, compare, checksum, isInit, iVersion==3007000
  - read nBackfill and marks; pick or claim a slot >= 1 (exclusive lock, pwrite the mark at 100+4n, downgrade); take the shared lock; re-read the header and confirm unchanged
  - on an invalid header, return "not ready" and let the host's connection run recovery

Checkpoint PASSIVE+TRUNCATE
- C path: ReadHdr + walCheckpoint + sqlite3WalCheckpoint + iterator/merge + hash lookup + restart/truncate + busy locks, about 800-950 C lines.
- Rust estimate: about 1,200-1,800 lines plus a crash/interleaving test suite [I].
- It must also cover:
  - fsync order: WAL before the db writes, db before publishing nBackfill
  - nBackfillAttempted
  - db-size truncate (nTruncate)
  - the db-file side
  - the Checkpointer trait also needs WAL_WRITE_LOCK (crates/core/src/source.rs:241) and bump_seq

## 4. Deployments
### A: same process as the host's C SQLite
- How SQLite locks in-process [M]: unixShmLock (46040-46110) keeps per-process aLock[] counts. Only the first shared locker of a slot calls fcntl. An exclusive lock calls fcntl whenever no sibling holds the slot. Traditional F_SETLK from our own fd never conflicts with the host (same process) and silently pins nothing.
- OFD locks work, measured on this Mac (Darwin 25.6.0, scratch/ofdtest.py):
  - F_OFD_SETLK (cmd 90, in libc for apple: libc-0.2.168 apple/mod.rs:3756) took a shared lock
  - a same-process traditional F_SETLK F_WRLCK on another fd was then refused (EAGAIN)
  - the OFD lock survived close() of other fds
  - closing ANY fd on the file dropped the process's traditional locks
  - Linux OFD semantics are documented the same way [I]
- Pin [I, from the code read]:
  - Sound with OFD locks, given two conditions: (1) our -shm and db fds are never closed during process life, because closing them drops the HOST's POSIX locks (howtocorrupt §2.2); (2) we never run recovery or truncate -shm.
  - Writing a read-mark is safe if done exactly like SQLite: hold an exclusive OFD lock on READ(n), pwrite aReadMark[n], downgrade. The host's attempt to change that slot calls fcntl, gets BUSY and retries (walTryBeginRead handles WAL_RETRY). The host never caches marks across a lock.
  - Primitives: libc::fcntl with F_OFD_SETLK, which needs unsafe. rustix's fcntl_lock is whole-file only, and nix exposes OFD on linux only [I]. So: one small crate with unsafe allowed, ~40 lines. pread/pwrite on -shm (no mmap needed) relies on the unified page cache, coherent on Linux and macOS [I].
  - Side effect since 3.51.0: unixIsSharingShmNode (45333) sees our DMS lock, so the host's last connection will not delete -wal/-shm or switch journal_mode out of WAL (harmless, documented as such).
- Checkpoint: possible in principle with the same primitives [I]. Blast radius:
  - an ordering or fsync bug, a wrong nBackfill, or a truncate race = live db corruption
  - closing a db fd = all the host's db locks gone
  - any future change in SQLite's checkpoint protocol must be tracked by hand
  - Not recommended.
### B: separate process
- Pin: sound with plain F_SETLK or OFD, since POSIX locks conflict across processes [I]. Same code; the close() hazard is gone.
- Checkpoint: same correctness burden as A minus the close() hazard [I]. SQLite itself is the reference for multi-process access, so the protocol is designed for this case.
### C: host uses no C SQLite (Turso)
- Pure-Rust -shm code is irrelevant [M]:
  - Turso's wal-index lives in process memory (InProcessWalCoordination, wal.rs:873) or in .tshm.
  - It locks the db file exclusively in single-process mode (unix.rs:70).
- Pin and checkpoint must go through Turso's API (begin_read_tx; pager.checkpoint(mode) as in multiprocess_tests.rs:31-37). Or: speak .tshm, an unstable v1 format (SHARED_WAL_COORDINATION_VERSION=1).

## 5. Middle option: pure-Rust pin, host does every checkpoint
- Guarantee: yes, "no frame vanishes before it is copied", if the pin holds a slot n >= 1 [I from walformat + walRestartLog]:
  - The WAL is only overwritten by a writer restart (walRestartLog) or a RESTART/TRUNCATE checkpoint (walRestartHdr).
  - Both need exclusive READ(1..4) (walBusyLock), which a shared lock on any slot >= 1 blocks.
  - Holding READ(0) does NOT block a reset (walformat.html). So even when fully backfilled the pin must use slot >= 1, unlike a normal SQLite reader.
  - Checkpoints still backfill up to our mark, which is harmless.
- Size: ~300-450 lines + tests [I].
- Catches:
  - the host's TRUNCATE returns BUSY while pinned
  - the WAL grows while pinned, so pins must be short
  - an invalid header (no connection yet, or a crash) needs the host to recover first
  - Linux and macOS need OFD for A
- Recommended if anything is done.

## 6. Stability of what SQLite promises
### Format [M quotes]
- fileformat2.html §4.6: "The wal-index is transient. After a crash, the wal-index is reconstructed from the original WAL file." "Because the wal-index is transient, it can use an architecture-specific format". Native byte order. "no further information about the format of the wal-index will be provided here."
- walformat.html documents the layout and lock bytes but says the details "are left to discretion of individual VFS implementations". It has a "TBD: More information about the header" and an inconsistent nBackfill offset. It does not say what a client does with an unknown iVersion.
- wal.c (sqlite3.c:67139-67163):
  - A newer WAL version means recovery fails with SQLITE_CANTOPEN.
  - A wal-index header that passes the checksum but carries a version other than 3007000 means "no read-transaction is opened and SQLite returns SQLITE_CANTOPEN".
  - "Technically, the various VFSes are free to implement these locks however they see fit. However, compatibility is encouraged so that VFSes can interoperate."
- So the version is a tripwire: a future layout change makes an old reader refuse, not misread. A Rust pin must check iVersion==3007000 and refuse otherwise.
- WAL file format and checksum: version 3007000 since 3.7.0 (2010) [M constant; date I].
### History
From gh api commits on sqlite/sqlite, saved in scratch/wal_c_log.txt and scratch/os_unix_log.txt [M].
Format, bytes on disk and lock offsets:
- No layout change seen. WALINDEX_MAX_VERSION is still 3007000 in 3.54.1 [M].
- Additions since 2010, from memory [I]: mxFrame/nBackfillAttempted for SNAPSHOT (3.10, 2016) and TRUNCATE mode (3.8.8, 2015).
Protocol behaviour, within the last 5 years [M]:
- 2026-10-23: recovery copy and read-only -shm races
- 2026-03-03: "Avoid an obscure race condition between a checkpointer and a writer wrapping around to the start of the wal file". Release notes 3.51.3 / 3.53.0: "Fix the WAL-reset database corruption bug."
- 2025-11 (3.51.0): broken-posix-lock defenses (unixIsSharingShmNode, /proc lock analysis)
- 2025-09: CHECKPOINT_NOOP
- 2025-05 / 2025-01: SETLK_TIMEOUT recovery and busy handler, plus sqlite3_setlk_timeout
- 2024-09: snapshot_get avoids READ(0)
- 2023-11: blocking read-lock slots, no blocking in PASSIVE
- 2020-08: unixShmLock in-process fixes
- 2020-07-30: "guaranteed-safe" wal-index overwrite
Branches wal2 and begin-concurrent (not on trunk) would change the layout and locks: two WAL files, page-level locking [I, not read].
### Other live -shm implementations
Open (not verified). See the Open list.
### Conclusion
- (a) Reading the WAL and the -shm header: YES. The bytes are unchanged since 2010, and there is a version tripwire.
- (b) Pin: YES. The lock offsets and read-mark semantics have not changed since 2010. The last change touching an external locker is 3.51.0 (2025-11), and it is harmless.
- (c) Checkpoint: NO. The checkpoint/restart behaviour had a corruption race fixed 2026-03 (3.51.3), and a fork would not get such fixes.

## Open
- Other implementations speaking live -shm against C SQLite: LiteFS (FUSE, sees all lock calls), mvsqlite, rqlite (uses C SQLite), libSQL (a C fork), sqlite-wasm OPFS (no shm), modernc (transpiled C, so the same code). Unverified; no breakage reports searched.
- Exact release dates for TRUNCATE (3.8.8) and SNAPSHOT (3.10): not checked; the changes.html fetch was truncated.
- Linux OFD vs same-process POSIX conflict: not run on Linux, only documented behaviour.
- Whether libsqlite3-sys builds with SQLITE_ENABLE_SETLK_TIMEOUT (its F_SETLKW against our OFD lock would block, not BUSY): not checked.
- Whether nix or rustix expose F_OFD_SETLK on macOS: not checked.
