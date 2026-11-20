# Phase 3 (syncing on real SQLite): implementation plan

**Goal:** run the phase 2 engine against a live SQLite database in the host's process, through a public API, with every belief about SQLite checked by a real-SQLite fault suite and every handover between Go and Rust a plain restart.

**Spec:** `docs/PLAN.md` (phase 3; section 3 types; section 6 host integration). Rules: `AGENTS.md`. Research: `docs/notes/pure-rust-pin.md` (the pin and the checkpoint against C SQLite). Upstream (read-only): `.upstream/litestream` (`db.go`, `wal_reader.go`, `replica.go`, `store.go`), and SQLite 3.54.1's `sqlite3.c` from the bundled `libsqlite3-sys` (line numbers below are from that file, as in the research note).

Status: draft. Phase 2's exit items (handoff issue #1: the level 1 crash window, the level 9 scenario, the nightly seeds, `core` mutants) land first. Section "Open decisions" is the owner's; no task that depends on one starts before it is ruled. Decisions already ruled are listed under "Rulings".

## Global constraints
The constraints of the phase 1 and phase 2 plans apply unchanged. In addition:
- **No descriptor on the database file or its `-shm` is ever opened and closed by this library.** Closing any descriptor on a file drops every POSIX lock the process holds on it, the host's SQLite locks included (howtocorrupt §2.2). Database pages and the wal-index are read through SQLite's own file handle (`SQLITE_FCNTL_FILE_POINTER`, then the handle's `xRead` and `xShmMap`), or through descriptors opened once and never closed until the process ends. The `-wal` file may be opened with `std::fs`: SQLite takes no POSIX lock on it. `crates/sqlite/clippy.toml` disallows `std::fs::File::open`, `std::fs::OpenOptions::open` and `std::fs::read` in that crate; the WAL open is the one `#[expect(clippy::disallowed_methods, reason = …)]`.
- `unsafe` stays inside `lsr-sqlite`, one reasoned `#[expect(unsafe_code)]` per site, each with a test that reaches it.
- The checkpoint stays with SQLite (`sqlite3_wal_checkpoint_v2`), whatever backend holds the pin. The research note sizes a pure-Rust checkpoint at 1,200 to 1,800 lines against a protocol SQLite fixed a corruption race in during 2026; it is out of scope.
- Task 4's harness stays generic over the backend that holds the pin, so a later pure-Rust pin (#9) runs the same suite unchanged.
- No async runtime enters the workspace. The actor thread drives `Actor::tick` with the same small executor the CLI uses.

## Review focus
1. A descriptor on the database or `-shm` closed anywhere in the process: the host's locks are gone and nothing reports it.
2. The pin lapsing while a frame past the synced end is still unread (a checkpoint by the host, a restart, a read transaction that ended).
3. A database-file page shipped without a check (walgrowth pages, snapshot pages, the page after a VACUUM).
4. A shutdown that reports success with a committed transaction not on the replica.
5. A handover (Go to Rust or Rust to Go) that snapshots when the WAL was intact, or continues when it was not.

## Task order
0 (spikes) first; its answers can change task 1. Then 1 (the SQLite backend) and 3 (the page checksum table, pure core) in parallel. 4 (the real-SQLite fault suite) after 1. 5 (recovery from the bucket) after 3. 6 (actor thread and public API) after 1 and 5. 7 (compat in all four directions, the alternation test) after 6. 8 (the upstream-test ledger) can start any time and closes last. 9 (the 72-hour Tigris soak) runs on the machine with the credentials, after 7.

## Open decisions (the owner's)

- **O2, where the page checksum table lives across a restart.** In memory it is lost at every boot (task 3).
  - **Rebuild at boot** by reading every database page once. Simple; costs one full read of the database per boot (task 3 measures it); a page corrupted while the process was down is trusted from then on.
  - **Persist** it in the meta directory (a new file beside Go's layout, with its own checksum), updated at each staged commit. Boot is cheap, and corruption while down is caught. Costs: one more file to keep consistent with the staging directory, and spike S5 must show Go ignores the file.
  - Recommendation: rebuild at boot for v1. PLAN section 7 already makes scheduled verify-restores a v1 requirement, and they catch what a rebuild trusts.

## Rulings

- **P0 (O1, ruled by the owner 2026-11-20): a SQLite read transaction holds the pin**, on the library's own connection, as Go and `LiveSqlite` do. SQLite manages its own locks, so there is no lock protocol to copy, and the checkpoint needs that connection anyway. The known cost: a reader on a fully backfilled WAL takes read mark 0, which does not block a WAL restart (walformat.html), so ruling F1's continuity proof and its tripwire carry correctness in that case. The alternative, a pure-Rust pin on a read-mark slot of 1 or more (`docs/notes/pure-rust-pin.md`, sections 4A and 5), is deferred to #9, to be built if task 4 or the simulator finds a mark-0 case F1 does not cover.
- **P1, a database page that does not match the table: halt.** `Fault::DatabasePageMismatch { pgno }`, policy `Halt(HaltCause::DatabaseCorrupt { pgno })`. A resnapshot would read the same page, and uploading it would put corruption into every later restore. The event names the page; nothing is uploaded from the read that found it.
- **P2, an unexplained WAL rewind: F2 stands.** Same salts with the committed end below the synced end is `Snapshot { cause: LocalRewound }` plus the event, as phase 2 ruled. A different-salts rewind is F1's case. No new outcome.
- **P3, TRUNCATE needs the host's word.** `decide()` takes `Readers { Idle, Pinned, Unknown }` from the host's signal (task 6); TRUNCATE runs only on `Idle`. `Unknown` (no signal given) behaves as `Pinned`, so a host that says nothing never sees a TRUNCATE. PASSIVE is unchanged.
- **P4, polling is the change detector.** One `committed()` read per second when idle (PLAN phase 3). The wal hook is a later optimisation and needs its own measurement.

---

### Task 0: spikes (branch `sqlite-spikes`)

**Files:** `docs/spikes.md` (one section per spike), scratch code under `crates/sqlite/examples/` deleted before landing unless a test keeps it.

Each spike answers one question with a measurement on Linux at SQLite 3.54.1, the way the phase 0 spikes did:
- **S1, the wal-index through SQLite's own mapping.** `SQLITE_FCNTL_FILE_POINTER` on the reader connection, then `pMethods->xShmMap(file, 0, 32768, 0, &p)`: does it return the region the host's connections use, without a new descriptor, and can both header copies plus `nBackfill` be read from it under the read lock with the same results as `LiveSqlite`'s file reads? If not, the fallback is one `-shm` descriptor opened at start and never closed.
- **S4, `PRAGMA wal_checkpoint(NOOP)`.** Does it return mxFrame and nBackfill from one header read, and can the salts be had from the same read? If not, S1's mapping is the committed-end source.
- **S5, Go and an extra meta-directory file.** Does Go v0.5.16, v0.5.18 and Head start, sync and restart with an unknown file in `<meta>/`? Needed only if O2 is ruled "persist". (The pure-Rust pin's spikes, OFD on Linux and `SETLK_TIMEOUT`, are listed in #9.)

### Task 1: `lsr-sqlite`, the rusqlite backend (branch `sqlite-backend`)

**Files:** create `crates/sqlite/` (package `lsr-sqlite`; `src/{lib,conn,shm,pages,checkpoint,error}.rs`, `clippy.toml`, `tests/`). Switch `crates/compat` from `LiveSqlite` to `lsr_sqlite::Sqlite` and delete `crates/compat/src/live_source.rs`; `crates/compat/tests/live_source.rs` stays and must pass unchanged apart from its import line (phase 2 task 7a, rule 5). Dependencies: `rusqlite` 0.43 bundled (already in the workspace), `libsqlite3-sys` for the raw handle; no new crate.

**Produces:**
- `Sqlite`: a handle over two connections (the reader that holds the pin, the writer for the barrier, the write lock and the sequence bump), implementing `WalSource`, `PageSource` and `Checkpointer`. `Sqlite::open(path, SqliteConfig) -> Result<Self, OpenError>` returns once the read transaction is held, so nothing can checkpoint past it.
- The committed end from S1's mapping (or S4's pragma), both header copies compared, `CommittedRead::new(end, backfilled)` from one read.
- Pages through the reader's own file handle (`SQLITE_FCNTL_FILE_POINTER` then `xRead`), checked arithmetic on the offset.
- `checkpoint(mode)` through `sqlite3_wal_checkpoint_v2` for typed busy results, ending and retaking the read transaction around it (that is what moves `ReadEpoch`).
- `app_connection(conn: &rusqlite::Connection) -> Result<(), SetupError>`: `SQLITE_FCNTL_PERSIST_WAL` on and `wal_autocheckpoint = 0`, for every host connection. `check_app_connection` reads both back.
- `OpenError`, `SetupError`; failures inside the trait methods map to `SourceError` variants, one test each.

**Semantics:**
1. No `std::fs` open of the database or `-shm` (global constraint). The clippy config is the check; a test also lists the process's open descriptors (`/proc/self/fd`) before and after a full sync and checkpoint cycle and finds no new or closed descriptor on either file.
2. Busy timeouts are zero on both connections, as in `LiveSqlite`; the actor's policy decides what waits.
3. The read transaction is `BEGIN; SELECT COUNT(1) FROM _litestream_seq` (`db.go:1288`), the tables created first (`db.go:1174`, `1180`), so Go and Rust leave the same schema behind.

**Tests:** `crates/compat/tests/live_source.rs` unchanged (the outside-checkpoint cases, the pin cases, `live_sync_chain_restores`); one test per error variant; the descriptor test of semantics 1; the host-connection helpers on a pooled connection (four connections, each checked).

### Task 2: deferred

The pure-Rust pin is not part of phase 3 (ruling P0); it is tracked in #9. The number is kept so the task references below stay stable.

### Task 3: the page checksum table (branch `page-table`)

**Files:** `crates/core/src/pages.rs` and its tests; call sites in `sync.rs` (walgrowth reads), `snapshot.rs` (snapshot reads), `actor.rs` (boot); `fault.rs` for the new variants; a scenario in `crates/sim/src/scenario/faults.rs`.

**Produces:**
- `PageTable`: page number to `Checksum` (the CRC-64 of the page, as `Checksum::of_page`), bounded by the database's page count. `record(pgno, digest)` from verified WAL frames (`FrameRef::digest`, already computed on the checked read); `check(pgno, page) -> Result<(), Mismatch>` before any database-file page is used; `truncate(commit)` when the database shrinks.
- Seeding: the first snapshot records every page it reads; until a page is recorded, a read of it records instead of checking (the rebuild of O2's recommendation). The boot path rebuilds by reading every page once, timed in the PR (that number goes back into O2).
- `Fault::DatabasePageMismatch { pgno }` and `HaltCause::DatabaseCorrupt { pgno }` (ruling P1), in every exhaustive match.

**Semantics:** a page read from the database file reaches an LTX file only after `check` passed on the same bytes. A page the WAL holds newer than the database never needs the database copy (unchanged from phase 2). Memory: 8 bytes per page plus the map's overhead, stated in the module doc for 1 GiB at 4 KiB pages.

**Tests:** a property over the simulator's SQLite model (every walgrowth and snapshot read is checked, never only recorded, once seeded); a flipped byte in a database page between two syncs halts with the page number and stages nothing; VACUUM and a shrink keep the table consistent; the new fault's simulator scenario (`db_page_flipped_under_sync`), so `every_fault_has_a_scenario_or_a_reason` stays green.

### Task 4: the real-SQLite fault suite (branch `sqlite-faults`)

**Files:** `crates/sqlite/src/testing/{mod,vfs}.rs` behind the crate's `testing` feature; `crates/sqlite/tests/{faults,kill,power_loss,big}.rs`.

**Produces:**
- `FaultVfs`: a VFS registered under its own name that delegates to `unix` and injects, on a schedule: an error on read, write, sync, truncate or lock; a short write; and "power loss", which drops every write since the last `xSync` of that file. `SQLITE_TESTCTRL_PRNG_SEED` pins WAL salts so runs replay.
- A harness generic over the backend (task 1's today; #9's if it is built): the actor over the backend, an `FsStore` replica, the simulator's oracle (`lsr_sim::oracle::check` over restored page hashes at every acknowledged txid).

**Tests** (each case names the upstream fix or SQLite belief it checks, where there is one):
- Each injected fault at each step of a sync, a PASSIVE and a TRUNCATE cycle: the outcome is the fault's policy, and the oracle holds afterwards.
- Power loss at each `xSync` of the WAL under `synchronous=NORMAL`: the next boot snapshots (`LocalRewound`) or continues, never loses an acknowledged txid that reached the replica.
- A writer process SIGKILLed mid-transaction (a child process, not a thread): its pages never appear in any file.
- VACUUM, with and without a page-size change; a WAL deleted by a second process while pinned (`WalUnlinked`); an outside PASSIVE, RESTART and TRUNCATE while pinned and while not.
- The pin under read mark 0 with a fully backfilled WAL: F1's tripwire fires when frames were appended after the pin and abandoned.
- `big.rs`, `#[ignore = "large; just sqlite-big"]`: a 16 KiB-page database over 1 GiB (the lock page), a database over 4 GiB (32-bit offsets), a WAL larger than the sync cap. A `just sqlite-big` recipe runs them; CI runs them nightly.

### Task 5: position recovery from the bucket (branch `bucket-recover`)

**Files:** `crates/core/src/recover.rs` and `actor.rs` (boot), `crates/sim/src/scenario.rs` (`meta_wiped_remote_kept` changes its expectation).

**Semantics:** when the staging directory has no level 0 file, boot reads the header of the replica's newest level 0 file (WAL origin: offset, size, salts; commit; last page through the file's index) and builds the same `Tip` a staged file would give. `recover` then decides as today: a WAL that still matches continues with no snapshot. The read is one ranged GET of the header and index; the PR states its byte count. A replica whose newest file is above level 0 (the level 0 files were retained away) gives no tip, as today: `FirstSync`.

**Tests:** `meta_wiped_remote_kept` expects `Booted { cause: None }` and the oracle holds; the same with a WAL that moved on while down expects the snapshot cause phase 2 gives a staged tip; a remote tip whose header does not match its key halts as `RemoteCorrupt`; the GET count asserted.

### Task 6: the actor thread and the public API (branch `replica-api`)

**Files:** create `crates/replica/` (package `lsr-replica`; `src/{lib,thread,health,config,error}.rs`, `tests/`). `crates/cli` gains `lsr replicate <db> <replica-url>` over it (for task 7's process tests and the alternation test).

**Produces:**
- `Replicator::start(ReplicatorConfig) -> Result<Replicator, StartError>`: opens the backend, boots the actor on its own OS thread, and returns once the read transaction is held. Commands reach the thread over a bounded channel.
- `final_sync(&self, deadline: Instant) -> FinalSync`, a future with no runtime behind it (a waker set by the thread): `FinalSync { Drained { txid }, DeadlineReached { synced, committed }, Halted(HaltCause) }`. Every committed transaction is on the replica exactly when it returns `Drained`.
- `health(&self) -> Health`: a copy of one `Arc` snapshot, no lock across I/O: last synced txid, its age, the actor's phase, the halt cause.
- `set_readers(&self, Readers)`: the host's signal for ruling P3.
- `restore` and `verify`: the phase 1 functions with the replica's config.
- Drop sends a stop command and does no I/O on the caller's thread; the thread drains for at most `shutdown_grace`, then stops.

**Semantics:** the thread sleeps until the tick's `next` or a command, and wakes each second to poll (P4). No panic crosses `start`, `final_sync` or `health`: the thread catches its own and halts with `HaltCause::Bug`.

**Tests:** start returns with the pin held (an outside TRUNCATE then reports busy); `final_sync` after N commits is `Drained` at the newest txid and a restore equals the database; a deadline of zero with work pending is `DeadlineReached` with both offsets; a halted replicator reports the cause in `health` and in `final_sync`; dropping a replicator mid-upload leaves a replica the next start continues; the CLI's `replicate` stops cleanly on SIGTERM.

### Task 7: compatibility in all four directions (branch `compat-phase3`)

**Files:** `crates/compat/tests/{vfs,alternation}.rs`, `crates/compat/tests/handover.rs` (the process cases), `crates/compat/src/replicator.rs` (a Rust replicator process beside the Go one). Every test that runs Go is `#[ignore]` and runs under `just compat`.

**Tests:**
- The Go VFS reads a Rust replica (the direction phase 2 did not cover): `scripts/build-go-litestream.sh` also builds upstream's `cmd/litestream-vfs` for each pinned version; open the replica through it, read every table, page hashes equal the database at the replica's newest txid.
- `rust_resumes_from_go_meta` as a process: Go replicates, stops; `lsr replicate` starts on the same database, meta directory and replica; the next file is `tip + 1`, incremental, no snapshot; and the reverse, already in `go_resumes_from_rust_meta`, kept.
- The alternation test: Go and Rust take turns on one prefix, at least 20 switches per run, each ended by SIGKILL at a random point (mid-upload, mid-compaction, with staged files not yet uploaded) from a seed the test prints; every switch boots with no snapshot cause, and a restore at every acknowledged txid matches. Run for v0.5.18 and Head; the nightly `compat-releases` workflow adds every release from v0.5.16.

### Task 8: the upstream-test ledger (branch `ledger`)

**Files:** `docs/ledger.toml` (one entry per upstream test), `scripts/upstream-tests.sh` (lists every `func Test…` in `.upstream/litestream` at the pinned commit), `crates/compat/tests/ledger.rs`.

**Semantics:** one entry per top-level `func Test…` and per `t.Run` name that is a string literal (a recent upstream checkout has about 360 top-level test functions; PLAN's "about 900" counts subtests). Each entry is `counterpart = "<crate>::<test path>"` or `reason = "<why it does not apply>"` (the VFS read replica, v0.3 layout, the control socket: section 1's "not in v1"). The committed list of upstream test names is the input, so the test needs no clone.

**Tests:** `every_upstream_test_is_in_the_ledger` (a name on the list with no entry fails), `every_counterpart_exists` (each named Rust test is in `cargo nextest list` output, read from a file `just ledger` writes), and no entry without one of the two keys. The PR states the entry count and how many are counterparts.

### Task 9: the 72-hour Tigris soak (run, not a branch; needs the credentials machine)

A synthetic writer (the compat workload at a steady rate, with VACUUMs and outside checkpoints) through `lsr replicate` against the task 8t bucket under a run prefix, for 72 hours. Every hour, a restore at a random acknowledged txid and at Latest is compared with the recorded page hashes. The resource properties (PLAN section 4) are read from `health` and the store's counters. A `just soak-tigris <hours>` recipe; results go into `docs/spikes.md` under task 8t's section, with the open items listed there (`encoding-type=url` listings, the incomplete-upload lifecycle rule, load shedding).

---

## Exit, mapped to tasks

| PLAN phase 3 exit item | Task |
| --- | --- |
| The fault suite is green | 4 |
| The upstream-test ledger is green | 8 |
| Handover both ways without a snapshot when the WAL is intact | 5, 7 |
| The alternation test with SIGKILL at a random point in each handover | 7 |
| 72 hours against real Tigris: zero oracle failures, resource properties holding | 9 |
