# Phase 3 (syncing on real SQLite): implementation plan

**Goal:** run the phase 2 engine against a live SQLite database in the host's process, through a public API, with every belief about SQLite checked by a real-SQLite fault suite and every handover between Go and Rust a plain restart.

**Spec:** `docs/PLAN.md` (phase 3; section 3 types; section 6 host integration). Rules: `AGENTS.md`. Research: `docs/notes/pure-rust-pin.md` (the pin and the checkpoint against C SQLite). Upstream (read-only): `.upstream/litestream` (`db.go`, `wal_reader.go`, `replica.go`, `store.go`), and SQLite 3.54.1's `sqlite3.c` from the bundled `libsqlite3-sys` (line numbers below are from that file, as in the research note).

Status: draft. Folds in the parts of the parallel draft (#8) that this one lacked: what phase 2 hands over, the shared suite's cases, and the position file (O3, since ruled out for v1). Phase 2's exit items (handoff issue #1: the level 1 crash window, the level 9 scenario, the nightly seeds, `core` mutants) land first. Section "Open decisions" is the owner's; no task that depends on one starts before it is ruled. Decisions already ruled are listed under "Rulings".

## What phase 2 hands over

- The engine: `lsr_core::Actor`, sans-IO, one `tick` at a time over an `Env` of `WalSource`, `PageSource`, `Checkpointer` and two `ObjectStore`s. It already decides recovery, sync, checkpoints, uploads and maintenance; phase 3 gives it real doors and a thread.
- The doors (`crates/core/src/source.rs`): `WalSource` (`open`, `len`, `committed`, `epoch`), `PageSource` (`page_count`, `read_page`), `Checkpointer` (`begin_barrier`, `end_barrier`, `checkpoint`, `bump_seq`, `begin_write_lock`, `end_write_lock`).
- `LiveSqlite` (`crates/compat/src/live_source.rs`), test support over two rusqlite connections. Its tests, `crates/compat/tests/live_source.rs` and `live_matches_sim.rs`, are the contract the production backend must pass.
- The simulator's model of SQLite (`crates/sim/src/sqlite.rs`), pinned against real SQLite by `crates/sim/tests/sqlite_rules.rs`.
- Go handover in process: `crates/compat/tests/handover.rs`.

Two places where `docs/PLAN.md`'s phase 3 text and phase 2 as built differ. The built behaviour stands; this plan follows it:
- The committed end comes from one read of the wal-index header (frame count, salts and `nBackfill` together), not from `PRAGMA wal_checkpoint(NOOP)` followed by a second read. `WalSource::committed` is that one read.
- A page-size change is a halt at boot (`HaltCause::PageSizeChanged`), not a snapshot.

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
0 (spikes) first; its answers can change task 1. Then 1 (the SQLite backend), 3 (the page checksum table, pure core) and 10 (degraded states, pure core, after `core`'s mutation triage closes) in parallel. 4 (the real-SQLite fault suite) after 1. 5 (recovery from the bucket) after 3. 6 (actor thread and public API) after 1, 5 and 10. 7 (compat in all four directions, the alternation test) after 6. 8 (the upstream-test ledger) can start any time and closes last. 9 (the 72-hour Tigris soak) runs on the machine with the credentials, after 7.

## Open decisions (the owner's)

None open. O1 to O3 are ruled below (P0, P5, P6). v1 copies Go unless a better behaviour is obvious and adds no file or format Go does not speak (`docs/PLAN.md` D6).

## Rulings

- **P0 (O1, ruled by the owner 2026-11-20): a SQLite read transaction holds the pin**, on the library's own connection, as Go and `LiveSqlite` do. SQLite manages its own locks, so there is no lock protocol to copy, and the checkpoint needs that connection anyway. The known cost: a reader on a fully backfilled WAL takes read mark 0, which does not block a WAL restart (walformat.html), so ruling F1's continuity proof and its tripwire carry correctness in that case. The alternative, a pure-Rust pin on a read-mark slot of 1 or more (`docs/notes/pure-rust-pin.md`, sections 4A and 5), is deferred to #9, to be built if task 4 or the simulator finds a mark-0 case F1 does not cover.
- **P1, a database page that does not match the table: keep backing up, loudly** (ruled by the owner 2026-11-20: a backup system should not stop backing up). `Fault::DatabasePageMismatch { pgno }` puts the engine in a degraded state: an event names the page, `health()` reports it, and retention keeps the last restore point from before the mismatch and its chain (it pins, it does not freeze: files after that point are retained normally). WAL-only steps never read the database file and continue unchanged. A step that needs the page ships what is on disk, since that is the database the host serves. Halting would leave every later write unprotected and protect nothing. The degraded state clears when a later verified WAL frame rewrites the page, or by an operator. Which other halts become degraded states is a separate proposal (see "Halts" below).
- **P2, an unexplained WAL rewind: F2 stands.** Same salts with the committed end below the synced end is `Snapshot { cause: LocalRewound }` plus the event, as phase 2 ruled. A different-salts rewind is F1's case. No new outcome.
- **P3, TRUNCATE needs the host's word.** `decide()` takes `Readers { Idle, Pinned, Unknown }` from the host's signal (task 6); TRUNCATE runs only on `Idle`. `Unknown` (no signal given) behaves as `Pinned`, so a host that says nothing never sees a TRUNCATE. PASSIVE is unchanged.
- **P5 (O2, ruled by the owner 2026-11-20): trust on first use, seeded from the staged level 0 files**, the option closest to Go (which checks no database page at all, `db.go:1562`). After a boot that continues (`Continue` or `WalRestarted`), the pages of the staged files are recorded newest-wins; boot already verifies those files and computes each page's checksum, so the seed costs no GET and works the same on Go-written files after a handover. Any other page is recorded on its first read and checked on every read after. The table is cleared mid-run on every snapshot cause except `CheckpointBoundary` and `OperatorRequested`, and on a page-size change (after `LocalRewound` it holds digests of frames a power loss removed). With P1 this ships what Go ships, plus an alert when a recorded page changes. Not chosen, tracked in #10: seeding from the replica (a verify at boot), and persisting the table in the meta directory. Reading every page at boot is wrong, not just slow: after a mid-WAL `Continue` it records the old database copy of a page whose newer copy is still in the WAL, and the next checkpoint's boundary snapshot then mismatches.
- **P6 (O3, ruled by the owner 2026-11-20): no local position file in v1.** It would be a file Go does not speak (D6). Boot keeps phase 2's whole-tip compare (V3), which already checks more than Go; the residue it misses (a rewind of two or more transactions that regrows a byte-identical tip) is gap G1 of `docs/proposals/beyond-go.md`, whose single local state file would close it.
- **P7 (ruled by the owner 2026-11-20, D7): keep trying; stop only for a second writer; every response configurable.** Phase 2 stops every lane on eleven causes. They become degraded states that retry with Go's capped backoff (5 minutes), scoped to the lane that failed, except a second writer on the prefix (`ForeignWriter`), which stops uploads and maintenance by default. The responses are config (task 10). P1's degraded state keeps its retention pin. A second review (Fable, recorded with the ruling) found two things the default must not do: write into a fresh prefix (restore and the Go VFS read the configured one, so Latest there would be stale) and snapshot from the database file alone after WAL corruption (a state that never existed). Both are refused in task 10.
- **P4, polling is the change detector.** One `committed()` read per second when idle (PLAN phase 3). The wal hook is a later optimisation and needs its own measurement.

---

### Task 0: spikes (branch `sqlite-spikes`)

**Files:** `docs/spikes.md` (one section per spike), scratch code under `crates/sqlite/examples/` deleted before landing unless a test keeps it.

Each spike answers one question with a measurement on Linux at SQLite 3.54.1, the way the phase 0 spikes did:
- **S1, the wal-index through SQLite's own mapping.** `SQLITE_FCNTL_FILE_POINTER` on the reader connection, then `pMethods->xShmMap(file, 0, 32768, 0, &p)`: does it return the region the host's connections use, without a new descriptor, and can both header copies plus `nBackfill` be read from it under the read lock with the same results as `LiveSqlite`'s file reads? If not, the fallback is one `-shm` descriptor opened at start and never closed. **Answered 2026-11-20: yes** (`docs/spikes.md`, "Phase 3 spikes", S1; probe `crates/compat/tests/shm_mapping.rs`). Task 1 maps through `xShmMap` after a guard (read transaction, WAL, normal locking), checks `aCksum` as well as the copies, and needs no fallback descriptor.
- **S5, Go and an extra meta-directory file.** Does Go v0.5.16, v0.5.18 and Head start, sync and restart with an unknown file in `<meta>/`? Not needed while P5 stands (#10 and the beyond-Go proposal would need it). (The pure-Rust pin's spikes, OFD on Linux and `SETLK_TIMEOUT`, are listed in #9.)

### Task 1: `lsr-sqlite`, the rusqlite backend (branch `sqlite-backend`)

**Files:** create `crates/sqlite/` (package `lsr-sqlite`; `src/{lib,conn,shm,pages,checkpoint,error}.rs`, `clippy.toml`, `tests/`). Switch `crates/compat` from `LiveSqlite` to `lsr_sqlite::Sqlite` and delete `crates/compat/src/live_source.rs`; `crates/compat/tests/live_source.rs` stays and must pass unchanged apart from its import line (phase 2 task 7a, rule 5). Dependencies: `rusqlite` 0.43 bundled (already in the workspace), `libsqlite3-sys` for the raw handle; no new crate.

**Produces:**
- `Sqlite`: a handle over two connections (the reader that holds the pin, the writer for the barrier, the write lock and the sequence bump), implementing `WalSource`, `PageSource` and `Checkpointer`. `Sqlite::open(path, SqliteConfig) -> Result<Self, OpenError>` returns once the read transaction is held, so nothing can checkpoint past it.
- The committed end from one wal-index header read through S1's mapping, both header copies compared, `CommittedRead::new(end, backfilled)` from one read.
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
- Seeding as ruling P5 says. Until a page is recorded, a read of it records instead of checking. `clear()` on every snapshot cause P5 lists.
- What the table cannot check, stated in the module doc: a database page above the previous commit that no scanned frame holds (`plan_pages` in `sync.rs` reads it from the file), and `page_count()`. Review focus 3 is met only for pages a frame has shown.
- `Fault::DatabasePageMismatch { pgno }` and its degraded state (ruling P1), in every exhaustive match.

**Semantics:** a page read from the database file reaches an LTX file only after `check` passed on the same bytes. A page the WAL holds newer than the database never needs the database copy (unchanged from phase 2). Memory: 8 bytes per page plus the map's overhead, stated in the module doc for 1 GiB at 4 KiB pages.

**Tests:** a property over the simulator's SQLite model (every walgrowth and snapshot read is checked, never only recorded, once seeded); a flipped byte in a database page between two syncs enters the degraded state with the page number, WAL syncs continue, and retention keeps the restore point before it; a `LocalRewound` clears the table and raises no mismatch; VACUUM and a shrink keep the table consistent; the new fault's simulator scenario (`db_page_flipped_under_sync`), so `every_fault_has_a_scenario_or_a_reason` stays green.

### Task 4: the real-SQLite fault suite (branch `sqlite-faults`)

**Files:** `crates/sqlite/src/testing/{mod,vfs}.rs` behind the crate's `testing` feature; `crates/sqlite/tests/{faults,kill,power_loss,big}.rs`.

**Produces:**
- `FaultVfs`: a VFS registered under its own name that delegates to `unix` and injects, on a schedule: an error on read, write, sync, truncate or lock; a short write; and "power loss", which drops every write since the last `xSync` of that file. `SQLITE_TESTCTRL_PRNG_SEED` pins WAL salts so runs replay.
- A harness generic over the backend (task 1's today; #9's if it is built): the actor over the backend, an `FsStore` replica, the simulator's oracle (`lsr_sim::oracle::check` over restored page hashes at every acknowledged txid).

The suite is the contract for any backend: it holds every case of `crates/compat/tests/live_source.rs`, written once and run per backend. A case whose expected answer differs by backend (the mark-0 restart is the known one) states both answers in one table in the suite, never in a skip.

**Tests** (each case names the upstream fix or SQLite belief it checks, where there is one):
- `no_synced_frame_is_overwritten_while_pinned`: a second connection commits, checkpoints in every mode and commits again; every frame at or below the engine's position still verifies.
- `a_stranger_truncate_reports_busy_and_the_wal_keeps_its_length`; `the_pin_survives_the_hosts_last_connection_closing`; `a_wal_index_with_an_unknown_version_is_refused`; `an_invalid_wal_index_is_not_ready_never_recovered_by_us`.
- Each injected fault at each step of a sync, a PASSIVE and a TRUNCATE cycle: the outcome is the fault's policy, and the oracle holds afterwards.
- Power loss at each `xSync` of the WAL under `synchronous=NORMAL`: the next boot snapshots (`LocalRewound`) or continues, never loses an acknowledged txid that reached the replica.
- A writer process SIGKILLed mid-transaction (a child process, not a thread): its pages never appear in any file.
- VACUUM, with and without a page-size change; a WAL deleted by a second process while pinned (`WalUnlinked`); an outside PASSIVE, RESTART and TRUNCATE while pinned and while not.
- The pin under read mark 0 with a fully backfilled WAL: F1's tripwire fires when frames were appended after the pin and abandoned.
- `big.rs`, `#[ignore = "large; just sqlite-big"]`: a 16 KiB-page database over 1 GiB (the lock page), a database over 4 GiB (32-bit offsets), a WAL larger than the sync cap. A `just sqlite-big` recipe runs them; CI runs them nightly.

### Task 5: position recovery from the bucket (branch `bucket-recover`)

**Files:** `crates/core/src/recover.rs` and `actor.rs` (boot), `crates/sim/src/scenario.rs` (`meta_wiped_remote_kept` changes its expectation).

**Semantics:** when the staging directory has no level 0 file, boot reads the header of the replica's newest level 0 file (WAL origin: offset, size, salts; commit; last page through the file's index) and builds the same `Tip` a staged file would give. `recover` then decides as today: a WAL that still matches continues with no snapshot. The read is one ranged GET of the header and index; the PR states its byte count. A replica whose newest file is above level 0 (the level 0 files were retained away) gives no tip, as today: `FirstSync`.

**Tests:** the V3 residue case (two transactions dropped, byte-identical tip regrown) as an ignored `review_` test that names gap G1, so the gap stays visible; `meta_wiped_remote_kept` expects `Booted { cause: None }` and the oracle holds; the same with a WAL that moved on while down expects the snapshot cause phase 2 gives a staged tip; a remote tip whose header does not match its key halts as `RemoteCorrupt`; the GET count asserted.

### Task 6: the actor thread and the public API (branch `replica-api`)

**Signatures:** `docs/plans/phase-3-api.md`, which supersedes this block wherever they differ (counted reader guards instead of `set_readers`, a type-erased store, `stop` with a per-call deadline, `Runtime` with an interruptible park, and the engine changes listed there).

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

### Task 10: degraded states and configurable responses (branch `degraded`; before task 6)

**Files:** `crates/core/src/fault.rs` (`HaltCause` becomes the stop cause; a `Degraded` cause beside it), `crates/core/src/actor.rs` and `actor/state.rs` (lanes), `crates/core/src/retry.rs` (the cap), the simulator's fault scenarios and resource properties, `crates/core/src/maintain/retain.rs` (the pin).

**Produces:**
- `Responses`, a config struct with one named field per persistent cause (never a map, so a new cause breaks the build): `credentials`, `store_refused`, `remote_corrupt`, `remote_regressed`, `wal_corrupt`, `wal_truncated`, `wal_unlinked`, `page_size_changed`, `database_page_mismatch`, `bug`, `foreign_writer`. Each is `Response { KeepTrying, Stop }`. `Responses::GO_LIKE` is the default: every field `KeepTrying` except `foreign_writer: Stop`. `Responses::STRICT` stops on all of them (phase 2's behaviour).
- Lanes: a persistent failure in one lane (sync, upload, prune, maintenance) degrades that lane only. A failed retention delete no longer stops syncing. `Tick` reports `Degraded { cause, lane, since }`, and the actor keeps ticking.
- Backoff for a degraded lane: doubling up to `MAX_BACKOFF`, 5 minutes as Go's `DefaultSyncBackoffMax`, never less than the store's `Retry-After`.
- Retention while any lane is degraded: pinned at the last restore point before the degradation and its chain; files after it are retained normally.
- What `KeepTrying` never does, whatever the config: write to a different prefix; ship a frame that failed its checksum; snapshot from the database file while the WAL is corrupt below the committed end.

**Tests:** the simulator's `every_fault_has_a_scenario_or_a_reason` gains each response: under `GO_LIKE` every scenario ends with the oracle holding and the lane degraded, then recovered once the fault clears; under `STRICT` it ends stopped with the cause. A failed delete leaves sync and upload running. `foreign_writer` under `GO_LIKE` stops uploads and maintenance and keeps the health cause. The backoff never exceeds the cap. Retention never deletes the pinned restore point while degraded.

---

## Exit, mapped to tasks

| PLAN phase 3 exit item | Task |
| --- | --- |
| The fault suite is green | 4 |
| The upstream-test ledger is green | 8 |
| Handover both ways without a snapshot when the WAL is intact | 5, 7 |
| The alternation test with SIGKILL at a random point in each handover | 7 |
| 72 hours against real Tigris: zero oracle failures, resource properties holding | 9 |
