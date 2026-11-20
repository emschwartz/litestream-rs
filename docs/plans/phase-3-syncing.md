# Phase 3 (syncing on real SQLite): implementation plan

**Status:** draft. Nothing here is built. Four decisions are open and marked **Open**; the tasks that depend on one say so and do not start before it is ruled.

**Goal:** run the phase 2 engine against a live SQLite database inside the host's process: hold the WAL, sync it, own the checkpoint, and hand a prefix back and forth with Go Litestream as a plain restart.

**Spec:** `docs/PLAN.md` (phase 3, section 3 concurrency and types, section 6 host integration). Rules: `AGENTS.md`. Evidence for the pin: `docs/notes/pure-rust-pin.md`. Upstream (read-only): `.upstream/litestream` (`db.go`, `replica.go`, `store.go`, `vfs.go`).

## What phase 2 hands over

- The engine: `lsr_core::Actor`, sans-IO, one `tick` at a time over an `Env` of `WalSource`, `PageSource`, `Checkpointer` and two `ObjectStore`s. It already decides recovery, sync, checkpoints, uploads and maintenance; phase 3 gives it real doors and a thread.
- The doors (`crates/core/src/source.rs`): `WalSource` (`open`, `len`, `committed`, `epoch`), `PageSource` (`page_count`, `read_page`), `Checkpointer` (`begin_barrier`, `end_barrier`, `checkpoint`, `bump_seq`, `begin_write_lock`, `end_write_lock`).
- A test-support implementation of all three over two rusqlite connections: `LiveSqlite` in `crates/compat/src/live_source.rs`. Its tests, `crates/compat/tests/live_source.rs` and `live_matches_sim.rs`, are the contract the production backend must pass unchanged.
- The simulator's model of SQLite (`crates/sim/src/sqlite.rs`), pinned against real SQLite by `crates/sim/tests/sqlite_rules.rs`.
- Go handover in process: `crates/compat/tests/handover.rs` (Go resumes from Rust's meta directory, and the actor boots from Go's).

Two places where `docs/PLAN.md`'s phase 3 text and phase 2 as built differ. The built behaviour stands; this plan follows it:

- The committed end comes from one read of the wal-index header (frame count, salts and `nBackfill` together), not from `PRAGMA wal_checkpoint(NOOP)` followed by a second read. `WalSource::committed` is that one read.
- A page-size change is a halt at boot (`HaltCause::PageSizeChanged`), not a snapshot.

## Global constraints

The constraints of `docs/plans/phase-1-reading.md` and `docs/plans/phase-2-writing.md` apply unchanged. In addition:

- `unsafe` stays denied workspace-wide. The SQLite crate is the only one that opts in, one reasoned `#[expect(unsafe_code)]` per site. If the pure-Rust pin is chosen, its `fcntl` call is such a site and lives in that crate (see Open O1).
- No panic crosses the library boundary. A data-dependent failure is a typed error and a `Halted` engine; the application keeps serving.
- The write lock is never held while waiting on a reader.
- No descriptor on the database, the WAL or the `-shm` file is ever closed while the process lives: closing any descriptor on a file drops every POSIX lock the process holds on it, the host's included.
- Real-SQLite tests are synthetic databases in a temporary directory, each under a second where possible, and wait on conditions, never on the clock. Anything that needs a Go binary stays behind `just compat`; anything that needs credentials stays out of `just check` and CI.
- Every backend runs the same real-SQLite suite (Task 2). A test that only one backend can pass is a finding against the suite or the backend, not a reason to fork the suite.
- New dependencies are named with licence and reason. Expected: `rusqlite` 0.43 (bundled, already a compat dependency) and `libsqlite3-sys` for the raw calls; `libc` only if the pure-Rust pin is chosen.

## Review focus

1. A frame that fails its checksum or salt below the committed end: `Fault::WalCorruptBelowCommitted` and a halt, never end-of-WAL (Go returns EOF, `wal_reader.go:188`).
2. No frame the engine has not synced can be overwritten: the pin holds across every tick, and the only place it is released is inside `Checkpointer::checkpoint`, where the epoch moves.
3. No second, unchecked read of a page: LTX pages come from the read that verified them, and a database-file page that does not match the checksum table is never uploaded.
4. The application is never blocked by the engine beyond the barrier and the TRUNCATE the policy allows, and never crashed by it.
5. Switching between Go and Rust on one prefix, in either direction, at any moment including mid-upload and mid-compaction, is a restart: no forced snapshot, no restore mismatch.

## Open decisions

These are the owner's. Each lists the options and a recommendation; none is settled by this document.

- **O1, the pin backend. Open.** How the engine holds the WAL between ticks.
  - *A, a SQLite read transaction* on a connection the engine owns (`BEGIN; SELECT COUNT(1) FROM _litestream_seq`, Go `db.go:1288`), as `LiveSqlite` does. SQLite's own code takes the lock; a future SQLite change is followed by upgrading SQLite. It pins on read mark 0 when the WAL is fully backfilled, which does not stop a writer's restart (`pin_on_zero_one_restart` is the cost: one snapshot).
  - *B, a pure-Rust pin* (`docs/notes/pure-rust-pin.md`): open the `-shm` file, take a shared OFD lock on a read-mark slot of 1 or above, write the mark as SQLite does. About 300 to 450 lines plus tests. It can always pin on a slot that blocks a restart, so the mark-0 snapshot goes away, and it needs no SQLite connection for the pin. It must check `iVersion == 3007000` and refuse otherwise, never run recovery, never truncate or close the `-shm` descriptor, and it needs one `unsafe` `fcntl` site. The note's own open items apply: OFD against same-process POSIX locks was measured on macOS and only read about for Linux, and whether the bundled SQLite is built with `SQLITE_ENABLE_SETLK_TIMEOUT` was not checked.
  - In both, the checkpoint stays with SQLite (`sqlite3_wal_checkpoint_v2`). A pure-Rust checkpoint is out: about 1200 to 1800 lines tracking a protocol that had a corruption race fixed in 2026-03, where a bug corrupts the live database.
  - Recommendation: build A first (Task 1), because it is what the phase 2 tests already pin and what Go does; build B behind the same traits (Task 3) only after the two open measurements in the note are made on Linux, and choose by running Task 2's suite over both. Nothing in Tasks 4 to 11 depends on which is chosen.
- **O2, the policy for a database page that does not match the checksum table, and for an unexplained WAL rewind. Open.** `docs/PLAN.md` leaves both to this plan.
  - A mismatched database page: the options are a halt (new fault and halt cause) or a snapshot. A snapshot reads the same page again, so it cannot repair anything and would publish the bad page as the new base. Recommendation: halt, with the page number in the cause, and nothing uploaded from that read.
  - A WAL that is shorter than the synced position: at boot it is what a power loss under `synchronous=NORMAL` leaves, and phase 2 already snapshots (`SnapshotCause::LocalRewound`). While the engine runs with its pin held it cannot happen through SQLite. Recommendation: keep the boot snapshot; mid-run, halt (`HaltCause::WalTruncated` exists for the truncated case), because the only explanations are another writer on the file or a bug.
- **O3, the local position file (phase 2's Task 4e). Open.** V3's whole-tip compare cannot see a rewind that drops two or more transactions and regrows with a byte-identical tip. The recommended design in `docs/plans/phase-2-writing.md` (a position file in the staging directory holding the cumulative checksum pair at the tip; a missing file falls back to V3) was deferred until the actor landed. It is local state, not part of the replica. Recommendation: build it in Task 6, where boot recovery is reworked anyway.
- **O4, how the host tells the engine that no long reader is pinned. Open.** The engine truncates only when the host reports no pinned readers (PLAN section 6). The options are a method the host calls on a handle (`set_readers_idle(bool)`) or a callback the engine polls before a TRUNCATE. Recommendation: the method on the handle, read at decision time, defaulting to "readers may be pinned" so a host that never calls it gets PASSIVE only. This is public API, so it waits for the ruling.

Deferred, not this phase: the sqlx `after_connect` spike (`docs/spikes.md`), which is a host-integration question, and everything under "Later versions" in `docs/PLAN.md`.

## Task order

1 (the SQLite backend) and 2 (the shared suite) first, together: 2 is written against `LiveSqlite` and 1 must pass it. Then 4 (the actor thread and the public API) and 5 (checkpoint policy), which need 1. 6 (recovery from the bucket) and 7 (the page checksum table) are core changes and need only phase 2; they can run beside 1 but touch `recover.rs` and `sync.rs`, so not beside each other. 8 (the fault suite) needs 1, 2 and 7. 9 (the upstream ledger) needs 8's layer to exist. 10 (the `replicate` command) needs 4. 11 (compatibility, all four directions) needs 10. 12 (the Tigris soak) needs 10 and credentials. 3 (the pure-Rust pin) is gated on O1 and can start any time after 2.

---

### Task 1: `lsr-sqlite`, the doors on real SQLite (branch `sqlite-source`)

**Files:** create `crates/sqlite/` (package `lsr-sqlite`): `src/{lib,conn,shm,pages,checkpoint,pragmas,error}.rs`, `tests/`. Move nothing out of `crates/compat` yet.

**Consumes:** `WalSource`, `PageSource`, `Checkpointer`, `CommittedRead`, `CheckpointRun`, `Barrier`, `WriteLock`, `ReadEpoch`, `SourceError` (`lsr-core`); `CommittedEnd`, `Salts` (`lsr-wal`).

**Produces:** `pub struct SqliteDoors` (one object behind three handles, as `LiveSqlite` is) implementing the three traits; `pub fn prepare_connection(&rusqlite::Connection) -> Result<(), SqliteError>` for every application connection; one `SqliteError` enum.

**Semantics:**
1. Two connections the engine owns: a reader that holds the read transaction, and a writer for the barrier, the write lock and the sequence bump. Both have a zero busy timeout: a contended lock is `SourceError::Busy` at once and the engine's policy decides what waits.
2. The read transaction is taken before the application's first write and ends only inside `checkpoint`; each retake moves the `ReadEpoch`.
3. `committed` is one read of the wal-index header: both 48-byte copies equal (else `Busy`), `mxFrame`, the salts and `nBackfill` from that one read. The salts are the WAL header's bytes copied verbatim (big-endian); `mxFrame` and `nBackfill` are native byte order.
4. `checkpoint(mode)` calls `sqlite3_wal_checkpoint_v2` and returns its busy flag and both counts as typed fields. PASSIVE and TRUNCATE only.
5. Database pages are read through one descriptor that is never closed, or through SQLite's own (`SQLITE_FCNTL_FILE_POINTER`). The choice is made by a test that closes a second descriptor and checks the host's lock survives; write the test first and let it decide. Call: record which.
6. `prepare_connection` sets `SQLITE_FCNTL_PERSIST_WAL` and `wal_autocheckpoint = 0`. It is a helper the host calls; this crate opens no application connection.
7. SQLite is pinned to 3.54.1 (bundled). A build with another version fails a test, not a runtime check.

**Tests (write first):**
- `crates/compat/tests/live_source.rs` and `live_matches_sim.rs` run over `SqliteDoors` with no change to the test bodies (Task 2 makes that mechanical).
- `the_read_transaction_is_held_before_the_first_write`, `a_checkpoint_moves_the_epoch_and_nothing_else_does`.
- `committed_is_one_read`: a writer mid-update of the header makes it `Busy`, never a mixed answer.
- `a_closed_second_descriptor_does_not_drop_the_hosts_locks` (Linux and macOS).
- `persist_wal_keeps_the_wal_after_the_last_close`, `autocheckpoint_is_off_on_a_prepared_connection`.
- Every `SqliteError` variant is produced by a named test.

### Task 2: one real-SQLite suite for every backend (branch `source-suite`)

**Files:** create `crates/sqlite/src/suite.rs` behind a `testing` feature (a `Backend` trait: open a database path, return the three doors and a way to count pin releases), `crates/sqlite/tests/suite.rs`. Rewire `crates/compat/tests/live_source.rs` to call the suite with `LiveSqlite` as the first backend.

**Semantics:** the suite is the contract for O1. It holds every case of `live_source.rs` today (`outside_restart_against_pin_above_zero`, `outside_truncate_against_pin_above_zero`, `pin_on_zero_one_restart`, `pin_on_zero_proven_restart_continues_in_the_new_generation`, `live_sync_chain_restores`) plus the cases below, each written once and run per backend. A case whose expected answer differs by backend (the mark-0 restart is the known one) states both answers in one table in the suite, never in a skip.

**Tests added:**
- `no_synced_frame_is_overwritten_while_pinned`: a second connection commits, checkpoints in every mode and commits again; every frame at or below the engine's position still verifies.
- `a_stranger_truncate_reports_busy_and_the_wal_keeps_its_length`.
- `the_pin_survives_the_hosts_last_connection_closing`.
- `a_wal_index_with_an_unknown_version_is_refused` (the backend returns a typed error; it never reads on).
- `an_invalid_wal_index_is_not_ready_never_recovered_by_us`: after a crash that leaves no valid header, the backend reports "not ready" until a host connection has run recovery.

### Task 3: the pure-Rust pin (branch `rust-pin`). Gated on O1

Not started until O1 says to build it. If it is built:

**Files:** `crates/sqlite/src/pin/{mod,shm,lock}.rs` (inside the SQLite crate, where `unsafe` may be opted into), tests beside them.

**Produces:** `pub struct ShmPin` implementing `WalSource`; a `Checkpointer` that releases the pin, calls SQLite's checkpoint and takes the pin again, moving the epoch exactly as backend A does.

**Semantics** (from `docs/notes/pure-rust-pin.md`, sections 3 to 5):
1. Open `-shm` without truncating; never close it. Shared lock on the DMS byte.
2. Read both header copies, compare, verify the checksum, require `isInit` and `iVersion == 3007000`. Anything else is "not ready": the host's connection recovers, this code never does.
3. Pick or claim a read-mark slot of 1 or above: exclusive OFD lock on the slot, write the mark, downgrade to shared, re-read the header and confirm it is unchanged. Slot 0 is never used, because holding it does not block a reset.
4. OFD locks on Linux and macOS (`F_OFD_SETLK`); one `unsafe` call site of about 40 lines, with its invariant in the `#[expect]`.
5. Pins are short where they can be: the WAL grows while pinned, as it does for backend A.

**Tests:** Task 2's suite, unchanged, plus `ofd_lock_conflicts_with_a_same_process_posix_lock` on Linux (the measurement the note lists as open), `setlk_timeout_build_does_not_block_on_our_lock`, and a property test of slot selection against a model of `walTryBeginRead`.

### Task 4: the actor thread and the public API (branch `replica-api`)

**Files:** create `crates/replica/` (package `lsr-replica`): `src/{lib,thread,handle,health,config,error}.rs`, `tests/`.

**Consumes:** `Actor`, `ActorConfig`, `Env`, `Tick`, `Next`, `Event`, `HaltCause` (`lsr-core`); the doors of Task 1; `FsStore` for staging at Go's meta path; the S3 store.

**Produces:**
- `Replicator::start(config) -> Result<Replicator, StartError>`: returns once the read transaction is held, so the host may open its pool afterwards.
- `Replicator::final_sync(deadline) -> FinalSync` (`Drained`, `DeadlineReached { behind }`, `Halted(HaltCause)`): the drain phase 2 already has, driven to its end.
- `Replicator::health() -> Health`: a cheap handle with the last synced transaction, its age, the phase and the halt cause.
- `Replicator::request_snapshot()`.
- Config with `Option<NonZero…>` fields and unknown keys rejected, as PLAN section 3 requires.

**Semantics:**
1. One OS thread per database owns both connections and the actor. Commands arrive over a bounded channel; a full channel is a typed error to the caller, never a block.
2. The thread ticks, then waits for the earlier of `Next::At`, a command, or the 1-second WAL poll (`Next::OnWalChange`). A wal hook is a later optimisation.
3. Dropping a `Replicator` does no I/O: it signals the thread and returns. `final_sync` is the only orderly stop.
4. A panic on the engine thread is caught at the thread boundary and becomes `HaltCause::Bug` in `health()`. It never unwinds into the host.
5. Events go to an `EventSink` the host provides (a trait with one method, called on the engine thread, which must not block); the default sink drops them.
6. The real `Clock` and `Sleeper` live here. Nothing in `lsr-core` changes.

**Tests (write first):**
- `start_returns_only_once_the_read_lock_is_held`: a writer started the instant `start` returns cannot checkpoint past the engine's position.
- `drop_does_no_io` (a store that counts calls), `final_sync_outcomes` (one test per variant).
- `a_full_command_channel_is_an_error_not_a_wait`.
- `a_panic_in_a_door_is_a_halt_the_host_can_read`.
- `idle_database_costs_nothing`: 0 LTX files, 0 checkpoints, 0 LIST calls over a minute of virtual polls.
- `health_is_cheap`: reading it takes no lock the engine thread holds across I/O.

### Task 5: checkpoint policy on a live database (branch `checkpoint-policy`). Needs O4

**Files:** `crates/core/src/checkpoint.rs` and its tests (the decision), `crates/replica/src/handle.rs` (the host's signal).

**Semantics:**
1. PASSIVE by default, at most one attempt per cycle, behind the barrier transaction and the seal sync phase 2 built.
2. TRUNCATE only when the threshold is crossed and the host reports no pinned readers; with the unconditional boundary snapshot under the write lock, as built.
3. The engine never waits on a reader while holding the write lock: a busy TRUNCATE releases the lock, reports `busy` and backs off through the existing gate.
4. A host that never reports readers gets PASSIVE only, and the WAL-length resource property is then the host's to keep.

**Tests:** `truncate_waits_for_the_hosts_signal`, `a_busy_truncate_releases_the_write_lock_before_it_waits`, `passive_only_when_the_host_is_silent`, and the simulator's WAL-length bound with and without the signal.

### Task 6: recovery from the bucket alone (branch `bucket-recovery`). O3 decides its second half

**Files:** `crates/core/src/recover.rs`, `crates/core/src/actor.rs` (boot), their tests; `crates/sim` scenarios.

**Semantics:**
1. With no staged tip, boot reads the header of the replica's newest level-0 file (WAL offset, size, salts, commit) and recovers from it as it would from a staged tip, so a missing local directory does not force a snapshot when the WAL still matches. Today that case is `SnapshotCause::FirstSync` (`meta_wiped_remote_kept`).
2. The whole-tip compare (V3) needs the tip's pages; without a staged copy they are read from the replica's file, verified on that read.
3. If O3 is accepted: the local position file is written atomically after each staged file, and at boot a matching cumulative checksum at its offset proves every earlier frame. A missing or unreadable file falls back to V3 alone, which is also what a directory inherited from Go has.

**Tests:** `meta_wiped_remote_kept` changes its expectation to `Booted { cause: None }` when the WAL matches and keeps `FirstSync` when it does not; `a_replica_tip_that_does_not_verify_is_remote_corrupt`; the residue case from V3 as a failing test first (two transactions dropped, byte-identical tip regrown); a simulator scenario for each.

### Task 7: the page checksum table (branch `page-checksums`). Needs O2

**Files:** create `crates/core/src/pagesums.rs`; `crates/core/src/sync.rs`, `snapshot.rs`, `fault.rs` and their tests; `crates/sim` (one scenario per new fault).

**Produces:** `PageSums`: page number to CRC, bounded by the database's page count, fed by verified WAL frames and seeded by the first snapshot.

**Semantics:**
1. Every page taken from the WAL updates the table from the read that verified it.
2. Every page taken from the database file (walgrowth pages, snapshots) is compared with the table before it is encoded. A page the table has never seen is accepted and recorded only while the table is being seeded by the first snapshot.
3. A mismatch is the fault O2 rules on. Whatever the policy, the page is never uploaded.
4. The table is memory, rebuilt by a snapshot after a restart. Its size is `8 bytes * pages` and is one of the named memory bounds.

**Tests:** `a_db_page_that_changed_outside_the_wal_is_never_uploaded`; `the_first_snapshot_seeds_and_later_ones_check`; `a_restart_reseeds`; a proptest that the table equals a model after any sequence of commits and checkpoints; the simulator's `every_fault_has_a_scenario` covers the new variant by construction.

### Task 8: the real-SQLite fault suite (branch `sqlite-faults`)

**Files:** create `crates/sqlite/src/faultvfs.rs` behind the `testing` feature (a shim VFS that delegates to the unix VFS and injects on read, write, sync, truncate and lock); `crates/sqlite/tests/faults/`.

**Semantics:** each case drives a real database through the engine (Task 4's thread, a file store as the replica), injects one fault, and ends in a restore compared page by page with a copy of the database, plus `PRAGMA integrity_check`. `SQLITE_TESTCTRL_PRNG_SEED` pins the WAL salts so a failure replays.

**Cases:**
- One per injectable VFS operation, at a seeded call count: the engine's outcome is the typed fault the simulator expects for the same event.
- SIGKILL of a writer process mid-transaction; a VACUUM; the WAL deleted by a second process; unsynced writes dropped to model power loss (Linux CI only).
- A 16 KiB-page database above 1 GiB (the lock page), a database above 4 GiB (32-bit offsets), a WAL larger than the sync cap. These are slow: behind their own recipe, not in `just check`.
- The four beliefs upstream got wrong (#946, #1580, #875, #1344), each as a named test that fails if the belief is restored.

**Tests about the suite itself:** every `Fault` variant that a real database can produce has a case here or a stated reason it is simulator-only; a test fails on a variant with neither.

### Task 9: the upstream-test ledger (branch `upstream-ledger`)

**Files:** create `crates/compat/ledger/upstream.toml` (one row per relevant upstream test: file, name, counterpart or reason), `crates/compat/tests/ledger.rs`, a script that lists upstream's tests at the pinned commit.

**Semantics:** about 900 upstream cases are ported by scenario, not by code, into the simulator or the real-SQLite layer. The ledger maps each to its counterpart (a test path that exists) or to a reason from a closed list (v0.3 layout, not in v1, removed by in-process ownership, and so on). `ledger.rs` fails on an upstream test with no row, on a counterpart that does not exist, and on a row for a test upstream no longer has.

### Task 10: `lsr replicate` (branch `cli-replicate`)

**Files:** `crates/cli/src/{args,run,replica}.rs` and tests.

**Semantics:** a thin command over Task 4: a database path, a replica URL, the meta directory at Go's path; runs until SIGTERM, then `final_sync` within the grace period and an exit code per outcome. It exists so a Rust replicator can be killed and restarted like Go's, which the alternation test needs; a host application uses the library, not this command.

**Tests:** argument parsing (unknown keys rejected), SIGTERM drains, a halt prints its cause on stderr and exits non-zero, a second instance on the same meta directory refuses.

### Task 11: compatibility, all four directions (branch `compat-live-handover`)

**Files:** `crates/compat/tests/{handover,alternation,govfs}.rs`; every test ignored behind `just compat`.

**Tests:**
- Go writes, Rust restores, and Rust writes, Go restores: already built; rerun over the Task 1 backend.
- `go_vfs_reads_a_rust_replica`: the Go VFS serves queries from a prefix the Rust engine is writing.
- `rust_resumes_from_go_meta` live, through `lsr replicate` (phase 2 has it in process).
- `go_rust_go_on_one_prefix`: three handovers, each a restart, no level-9 file before the snapshot interval, restore equal to the database after each.
- `alternation`: Go and Rust switch on one prefix repeatedly with a SIGKILL at a seeded point in each handover (mid-upload, mid-compaction, with staged files not yet uploaded). Every switch is a plain restart: no forced snapshot, no restore mismatch. The seed is printed.

### Task 12: the Tigris soak (branch `tigris-soak`). Needs credentials

A 72-hour run of `lsr replicate` with a synthetic writer against a real Tigris bucket: zero oracle failures (scheduled verify-restores compared with the writer's own record) and the resource properties holding. It cannot run in CI or from a session without the credentials file; `just test-tigris` shows the pattern. The items left open in `docs/spikes.md` under Task 8t (listing encoding, the lifecycle rule for incomplete uploads, the larger restore comparison, the load-shedding response) are settled first, on the machine that holds the credentials.

---

## Exit

From `docs/PLAN.md`: the fault suite and the upstream-test ledger are green; handover in both directions happens without a snapshot when the WAL is intact; the alternation test passes with a SIGKILL at a random point in each handover; a 72-hour soak against real Tigris shows zero oracle failures and the resource properties holding. Added by this plan: Task 2's suite passes over every backend that ships, and O1 to O4 are each closed by a ruling recorded here.
