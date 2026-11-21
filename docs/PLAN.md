# Litestream-compatible SQLite replication in Rust: port plan

Master plan for the whole port: reading (LTX decode, restore), writing (LTX encode, compaction, snapshots) and syncing (WAL tracking, checkpoint ownership, upload, retention). Each phase gets its own task-level plan (files, signatures, failing tests first) when it starts; this document fixes the decisions those plans may not reopen.

Evidence base: upstream read at benbjohnson/litestream 041a8c3 (v0.5.18-21) and superfly/ltx ad7202a (v0.5.3); a 110-line Rust spike that decoded Go LTX, restored a byte-identical DB and produced files Go's `Decoder.Verify` accepts; a classification of all 176 real fixes upstream shipped since v0.5.0.

## Status (2026-11-20)

Scope for now is the library itself, phases 0 to 3, then a sidecar (phase 4), built to the quality bar in sections 2 and 4. Integration into a host application (a shadow run, then a cutover) is out of scope for this repository; section 6 lists what a host must provide. Decisions D1 to D5 are taken; see section 8.

## 1. Goal and contract

Build an in-process Rust library that replicates a WAL-mode SQLite database to S3-compatible storage and restores from it, interchangeable with Litestream 0.5.x on the same bucket prefix.

**The contract is the replica, not Litestream's behaviour.** Whatever Go wrote, Rust reads. Whatever Rust writes, Go `restore` and the Go VFS read. Inside that boundary the port is free to be more correct than upstream, and is expected to be.

- Wire format: LTX v3 (100-byte header, LZ4 block page frames, varint page index, 16-byte trailer, CRC-64 GO_ISO).
- Layout: `<path>/<%04x level>/<min16>-<max16>.ltx`, levels 0, 1, 2, 3 and 9 (snapshot); object metadata `litestream-timestamp` (RFC3339Nano).
- Tolerant reader: both page encodings (LZ4 frame, written by v0.5.0 to v0.5.15; LZ4 block, written from v0.5.16), files with and without tracked checksums, non-zero reserved bytes, unknown sibling objects such as `lock.json`.
- Strict writer: LZ4 block with the size-prefix flag, pages strictly ascending and never above commit, lock page never encoded, snapshots contiguous from page 1, reserved bytes zero. v1 writes the `NoChecksum` flag exactly as Go does (decision D2).
- Reader floor for Rust-written files: Go v0.5.16 (older builds reject block-encoded pages; measured).
- Known Go bug for handover to Go: releases through v0.5.18 can replicate WAL frames past the wal-index's committed end that a writer stopped mid-commit left (#38, fixed upstream in bcd69ae, in no release yet; `docs/plans/phase-3-syncing.md`).
- Equality is defined on restored database pages (page hashes plus rolling checksum), never on LTX bytes: `lz4_flex` matched Go's compressor on 14 of 40 frames.

**Not in v1:** the v0.3 `generations/` layout, backends other than S3 and a local directory, a VFS read replica (the Go VFS stays in use), control socket, heartbeat, runtime auto-recover (suspected lineage-fork bug upstream; do not copy), multi-writer leases. A separate-process mode (a sidecar, as Go runs) is phase 4, after the library.

## 2. Design goals, in priority order

1. **Never silently wrong.** Every restore verifies before it renames. Every ambiguous recovery state becomes a typed outcome that ends in a snapshot, a degraded state that keeps backing up, or a stop (D7). A request that cannot be met exactly is an error, never a shorter history (the upstream timestamp-restore gap is the model failure).
2. **Invariants live in types and API shape, not in asserts or comments.** 33 of 95 in-scope upstream fixes were preventable this way. Upstream's checkpoint logic grew six independent state fields, each added by a fix; here that state is one enum and a pure `decide()`.
3. **Every case is handled or the build fails.** Recovery is parsed once into a closed enum and matched exhaustively. No wildcard arms on domain enums (clippy `wildcard_enum_match_arm`, `match_wildcard_for_single_variants` denied).
4. **Uncontrollable faults are a closed enum, each with a named policy**: retry with backoff, re-snapshot, or a degraded state that alerts; stopping is the response only where continuing would damage existing backups, and is configurable per cause (D7). A test fails if a variant has no simulator scenario.
5. **Sans-IO core.** Time, randomness, the object store, the WAL, database pages and checkpoints all sit behind traits. The core never spawns, sleeps or reads a clock (enforced by `clippy.toml` `disallowed-methods`). This is what makes deterministic simulation possible.
6. **Never harm the host application.** No panic crosses the library boundary. Data-dependent failures return typed errors and put the replicator in a degraded state that keeps trying (or stops, where D7 says so); the app keeps serving and the health handle says why. Memory, I/O and queue depths are bounded by named constants. The write lock is never held while waiting on readers.
7. **Real SQLite gets the same test weight as the simulator.** Four severe upstream bugs were wrong beliefs about SQLite (#946, #1580, #875, #1344); a simulator built on the same beliefs cannot find them.
8. **Compatibility is tested continuously against pinned Go binaries**, in four directions across a version matrix. Upstream has no cross-version tests; this project does.
9. **Reversible rollout.** Shadow first, small database first, and a tested path back to the Go sidecar at every step.

## 3. Architecture

A Cargo workspace (decision D1: its own repository). Each crate is `crates/<name>`, package `lsr-<name>` (a working prefix); the README has the same map in short.

| Crate | Owns | Rules |
|---|---|---|
| `ltx` | Header, page frame, index, trailer; checksums; streaming decoder and encoder; both LZ4 read paths | `forbid(unsafe_code)`, no I/O beyond `Read`/`Write`, fuzzed, Kani on checksum math |
| `wal` | The SQLite WAL reader: header and frame checksums, committed transactions only, page maps | No I/O beyond `Read`; fuzzed |
| `core` | Position recovery, sync planning, checkpoint decision, upload sequencing, compaction, retention, restore planning | Sans-IO; depends only on `ltx`, `wal` and the traits |
| `store-s3` | `ObjectStore` on `rusty-s3` behind a `Transport` trait: blocking `ureq` by default (no runtime), `reqwest` as a feature for hosts that already have it: PUT, multipart (an abandoned upload's id goes on a bounded list and is aborted on next use; the bucket's lifecycle rule is the backstop), ranged GET, paged LIST, batch DELETE | Own retry classifier (408, 429, 5xx, connection reset retryable; jittered backoff); payload checksum policy explicit, never an SDK default; a failed LIST page is an error, never a short listing |
| `store-fs` | `ObjectStore` on a directory | Used by tests and by the Go cross-restore harness |
| `local` | The restore sink: a scratch file beside the target, the SQLite check, publish by hard link | Never overwrites or deletes a file it does not own; a failed publish says whether the target is visible |
| `sqlite` | `WalSource`, `PageSource`, `Checkpointer` on `rusqlite` plus raw FFI; the per-database actor thread | The only crate with `unsafe`; each site carries `#[expect(unsafe_code, reason = …)]` |
| `replica` | Public API: `Replicator::start`, `final_sync(deadline)`, `health()`, `restore()`, `verify()` | Async facade over the actor; drop does no I/O |
| `config` | Go's `litestream.yml` read as Go 041a8c3 reads it, mapped onto replicators; the one `s3://` URL parser | A key Go reads either works, is refused, or warns; unknown keys refused |
| `sidecar` | The `replicate` daemon (phase 4): Go's command line and config, many databases in one process, signals, `-exec`, `/metrics`, `/healthz` | Opens no database file itself; refuses what it would run differently from Go |
| `sim` | In-memory store with a fault schedule, synthetic WAL source, model oracle, seeded scenario generator | Dev only |
| `compat` | Go fixture generator, pinned Go binaries, cross-restore tests, golden fixtures | Dev only |
| `cli` | The `lsr` binary: `restore`, `verify`, `files`, `config check`; `replicate` is the sidecar | Stdout carries only the result; one closed exit-code table |
| `fuzzing` | Fuzz targets as plain functions over bytes, replayed over the committed corpus in the test suite (`fuzz/` drives them with cargo-fuzz) | Dev only |

Traits at the core boundary: `Clock`, `Sleeper`, `Entropy`, `ObjectStore` (the replica, and the local L0 staging directory through the same trait), `WalSource`, `PageSource`, `Checkpointer`, `DatabaseSink` (where a restore writes). `EventSink` is the replica crate's, for the host.

**Concurrency.** One OS thread per database owns both SQLite connections (the long-lived read transaction and the writer used for the sequence bump and barrier) and takes commands over a bounded channel; that removes upstream's `execSem` and `chkMu`. Uploads, compaction and retention run in one sequential actor, in priority order; concurrent tasks wait for a measurement that shows the sequential actor falling behind. Snapshots stream page chunks through a bounded channel into a multipart upload. Shutdown is a state with a deadline, not a cancellation race. Every thread gets an explicit stack (512 KiB for one that runs the engine, 64 KiB for one that only waits); `crates/replica/src/stack.rs` says how they were measured and how many threads a database costs. Issue #17 tracks a shared pool for hosts with many databases.

**Local state.** The staging directory uses Go's meta-directory layout (`<meta>/ltx/0/<txid>-<txid>.ltx`), so either implementation can resume from the other's state without a snapshot (decision D4).

### Types that carry the invariants

| Invariant (upstream fix that discovered it) | Mechanism |
|---|---|
| Never read past the committed WAL end (#1581) | `CommittedEnd` is built only from the wal-index mxFrame under the read lock; the WAL reader has no other constructor |
| Pages count only at commit frames; drop pages above commit (#881) | The reader yields whole `CommittedTx` values, never frames |
| Lock page never encoded (page 65537 at 16 KiB) | `PageNo::new(n, page_size)` rejects it; the encoder takes `PageNo` only |
| Logical WAL offset vs physical file size (#997, #946) | Distinct `SyncedOffset` and `WalFileLen` types with no conversion between them |
| Checkpoint only after syncing through the WAL (#896, #1347 chain) | `checkpoint(proof: SyncedThrough, mode, …) -> Result<Checkpointed, CheckpointFailed>`; the proof is the position itself, not `Clone`, built only by `ReachedEnd::confirm`: a sync step that reached the committed end, and a second read of the epoch that finds it unchanged, so a stale proof cannot exist |
| Six syncing flags that drifted apart (`syncedToWALEnd`, `truncatePassiveFailed`, …) | One `CheckpointState` enum plus `Continuity` on the position |
| A WAL restart is safe to follow only under an unbroken read transaction | `ReadEpoch` names one hold of the read transaction (a holder unique in the process, and how many times it has taken the transaction), carried by each `CommittedRead`; `Continuity::Pinned(epoch)` is set only when the synced end equalled the committed end during that epoch; any other salt change is a snapshot |
| Committed end and backfill count read together | `WalSource::committed() -> CommittedRead { end, backfilled }` from one wal-index read |
| Restart recovery (10 of 13 commits to upstream's verify are fixes or reverts) | `fn recover(...) -> Recovered`, matched exhaustively |
| Restore chain has no gaps | `ContiguousChain`, whose only constructor returns `PlanOutcome::Gap { have_through, next_starts }` on a hole; `RestoreTarget { Latest, Txid, AsOf }` replaces zero-valued mode flags |
| Local TXID never below the remote's | At boot `recover` returns `Snapshot { after: remote newest, cause: BehindRemote }`; mid-run it is `Halt(ForeignWriter)` |
| An upload whose commit failed may or may not be visible | `UploadState::ListFirst`: nothing uploads until a listing settles it |
| "Disabled" is not zero | Config fields are `Option<NonZeroU32>` and friends; unknown keys rejected |

```rust
enum Recovered {
    Continue(Position),
    WalRestarted(Position),
    Snapshot { after: Option<Txid>, cause: SnapshotCause },
    Halt(HaltCause),
}
enum SnapshotCause {
    FirstSync, BehindRemote { local }, LocalTipUnreadable, WalMissing, PageSizeChanged,
    GenerationChanged, PrevFrameMismatch, LastPageMismatch, LocalRewound { synced, committed },
    CheckpointBoundary, OperatorRequested,
}
```

Exact signatures live in `docs/plans/phase-2-writing.md`, task 4.

`SnapshotCause` is also the event field that replaces today's log grep for forced re-snapshots.

### Faults

One closed `Fault` enum covers what the library cannot prevent: store transient, throttled, auth, not found, corrupt object; local disk full or I/O error; SQLite busy; WAL changed under a read; external checkpoint; WAL deleted by another process; shutdown deadline reached. `fn policy(&Fault) -> Policy { Retry(Backoff) | Resnapshot(SnapshotCause) | Halt(HaltCause) }` is exhaustive; `Backoff` is `Doubling` or `After(Duration)` so a store's `Retry-After` is honoured. Two things are deliberately not faults: a clock that steps backwards is clamped and reported as a `Clamped` event, and a power-loss WAL rewind is a snapshot with cause `LocalRewound` (it is expected under `synchronous=NORMAL`). Hard `assert!` exists only in the simulator's oracle.

## 4. How correctness is established

| Layer | What it proves | Share of 95 in-scope upstream fixes it would have caught |
|---|---|---|
| Types and exhaustive matches | Illegal states do not compile | 39 |
| Seeded simulator + model oracle | For every reachable history, restore at TXID N equals the database as of commit N | 15 |
| Fault injection on real SQLite and on the store | Crashes, kills, torn writes, external checkpoints, provider errors | 16 |
| Fuzzing | Decoders, WAL parser and restore planner never panic, never accept corrupt input | 2 |
| Differential against Go | The replica contract, both directions | 2 |
| Nothing listed (review, production evidence) | | 19, plus 2 that in-process ownership removes |

- **Simulator.** Seeded operations: commit, VACUUM (two shapes: same page size, changed page size), checkpoint by a stranger, crash at any step, store fault, clock jump, compaction, retention, restore. The model is a list of page maps per commit. Every failure prints its seed. CI runs a fixed seed set per commit and a rotating set nightly.
- **Resource properties**, asserted by the simulator because 18 upstream availability fixes were liveness or resource bugs no data oracle sees: an idle database writes 0 LTX files and runs 0 checkpoints; at most 1 PASSIVE attempt per cycle; L0 bytes per tick bounded by the sync cap; WAL length bounded when no reader pins it; 0 LIST calls in steady state (a listing costs one HEAD per file until phase-1 task 8g); memory at most part size times upload concurrency.
- **Real SQLite.** A shim VFS delegating to the unix VFS injects faults on read, write, sync, truncate and lock. Separate tests SIGKILL a writer mid-transaction, run VACUUM, delete the WAL from a second process, and drop unsynced writes to model power loss (Linux CI). Fixed cases: a 16 KiB-page database larger than 1 GiB (lock page), a database larger than 4 GiB (32-bit offsets), a WAL larger than the sync cap. `SQLITE_TESTCTRL_PRNG_SEED` pins WAL salts.
- **Go compatibility harness.** Pinned Go binaries built from source for v0.5.0, v0.5.15, v0.5.16, the newest release and upstream HEAD. Four directions: Go writes and Rust restores; Rust writes and Go restores; the Go VFS reads a Rust replica; Go, then Rust, then Go replicate one prefix in turn. Each case ends in page-hash equality plus `PRAGMA integrity_check`. A CI job adds each new upstream release to the matrix.
- **Upstream's suite as a regression list.** About 900 relevant upstream test cases are ported by scenario, not by code, into the simulator or the real-SQLite layer. A ledger maps every upstream test to its counterpart or to a stated reason it does not apply; a test fails on an unmapped entry.
- **Also:** cargo-fuzz targets with a persisted corpus, Kani proofs for the rolling-checksum algebra, cargo-mutants on `ltx` and `core` with surviving mutants triaged, a soak against real Tigris (provider quirks are not simulable).
- **Lints.** A strict `[workspace.lints]` set (`unsafe_code = "deny"`, `indexing_slicing`, `unwrap_used`, `panic`, `arithmetic_side_effects`, the `cast_*` family, `undocumented_unsafe_blocks`, `allow_attributes_without_reason`, cargo `unused_dependencies = "deny"`), with `too-many-lines-threshold = 70` (decision D5), plus `deny.toml`, nextest and a pinned toolchain. SQLite is pinned (3.54.1), after upstream's bundled 3.51.2 shipped a WAL-reset bug.

## 5. Phases

Sequenced so that each phase is useful alone and the riskiest beliefs are tested earliest.

### Phase 0: scaffold and three spikes
Workspace, lint set, CI, pinned Go binaries. Settle what the research left unverified: `rusty-s3` signing of `x-amz-meta-*` headers and multipart against Tigris; whether the sqlx fork exposes the raw handle in `after_connect` (needed for `SQLITE_FCNTL_PERSIST_WAL` on every pooled connection); whether prior art (rustyriver) is usable given its unstated licence.
**Exit:** CI green on an empty workspace; each spike answered in the repo's notes.

### Phase 1: reading (LTX codec, restore, verifier, compat harness)
- `ltx`: full decoder and encoder (the encoder is needed for fixtures and round-trip tests now, and by phase 2).
- Stores: `store-fs`, `store-s3`, the in-memory fake.
- Restore planner on `RestoreTarget` and `ContiguousChain`: the furthest-reaching eligible file that begins at transaction 1 (a level-9 snapshot, or a lower-level file when no snapshot is old enough or none exists yet), then extend with the eligible level 0-8 file that touches the chain's end and reaches furthest (level only breaks ties). Eligible means every file for `Latest`, files at or below the target for `Txid`, files created before the instant for `AsOf`. The refusal precedence and payload rules are the contract in the `crates/core/src/plan.rs` module doc. `AsOf` stops at the last file boundary before the instant, as Go's does, because compaction stamps a file with its newest input's time; when a later file straddles the instant the outcome is `PlanOutcome::AsOfInsideFile` (the newest restorable state plus the straddling file), never a plain plan, and a restore refuses it unless `RestoreOptions::as_of_inside` is `AsOfInside::TakeEarlier` (the CLI's `--allow-earlier`), which applies the chain and records the file in `Restored::inside_file`. The plan is computed from one listing snapshot; a file that vanishes mid-restore triggers a bounded re-plan.
- Restore executor: per page the newest file wins, pages above the final commit are dropped, the lock page is zero-filled, length is commit times page size. It reads each file's index and fetches bounded ranges with a deadline per chunk, instead of holding one open stream per file (the cause of a 7-fix chain upstream). Verification (rolling checksum where tracked, per-file checksums always, optional `integrity_check`) runs before the rename; stale `-wal`, `-shm` and `-journal` files are refused (the caller clears them with `clear_sidecars` first; publish never deletes); the directory is fsynced.
- `verify` command: walks a prefix, checks every file's checksum and index, level coverage and chain contiguity, and reports gaps.
- Verify before use: a file's checksum, and its header level and TXID range against its key (level 9 must be a snapshot), are checked before any page from it is applied or merged. Go's follow path writes pages first and verifies after.
- Compat harness, direction Go to Rust, across the matrix; golden fixtures from synthetic databases only.
**Exit:** Rust restore is page-identical to Go restore for every matrix version at Latest, Txid and AsOf targets; a gap is reported as `Gap` in all three modes; decoders survive a 24-hour fuzz run; `verify` and a full Rust restore of a production-sized replica match a Go restore at the same TXID. Function-length checkpoint (decision D5): list every function with exactly one caller; if many exist only to satisfy the 70-line lint, raise the limit.
**Value alone:** an independent verifier for existing Litestream backups (upstream restore defaults to no integrity check and exits 0 on corrupt output).

### Phase 2: writing (sans-IO core, simulator, compaction)
- Core state machine: `recover`, sync planning from a `WalSource` (incremental from a cursor, walgrowth pages from the database file, snapshot from page 1), checkpoint `decide()`, upload sequencing, level compaction, snapshot schedule, retention.
- Compactor: k-way merge with bounded fan-in (upstream #1467 is open on unbounded fan-in); header fields per upstream's merge rule so Go accepts the output.
- Retention deletes only what a higher level covers, keeps contiguity and the newest file, and keeps L0 files longer than the longest expected restore takes (a long restore fails partway if retention deletes files its plan had named).
- Upload integrity: each staged file is verified on the same read that uploads it. Single PUTs and parts send Content-MD5 and compare the returned ETag with a local MD5. Tigris documents verifying only a sha256 checksum and lists a wrong sha256 on PutObject as accepted; whether it checks Content-MD5 is undocumented (task 8t settles it), so the ETag compare is the check that does not depend on the provider.
- A failed fsync of a staged file means discard and re-encode, never a second fsync of the same file. File timestamps are clamped so they never go backwards.
- Simulator, model oracle, resource properties, one scenario per `Fault`.
- Compat direction Rust to Go for files produced from synthetic WALs: Go `Decoder.Verify`, Go restore, Go VFS.
**Exit:** the oracle (a restore at every acknowledged txid returns what was acknowledged, and Latest is at or above the highest acknowledged txid; across a local rewind Latest follows the local database) holds over the fixed seed set and a nightly rotating set with zero open failures; every `Fault` and every `SnapshotCause` has a scenario; mutation survivors in `ltx` and `core` triaged to zero unexplained; Go restores Rust-written chains across the matrix from v0.5.16 up.

### Phase 3: syncing on real SQLite
- `sqlite` crate: actor thread; read transaction held from before the application's first write; `PRAGMA wal_checkpoint(NOOP)` for mxFrame; `sqlite3_wal_checkpoint_v2` for typed busy results; PASSIVE with barrier transaction and seal sync; TRUNCATE with the unconditional boundary snapshot under the write lock; `SQLITE_FCNTL_PERSIST_WAL` and `wal_autocheckpoint = 0` helpers for every application connection; database pages read through one never-closed descriptor or SQLite's own (`SQLITE_FCNTL_FILE_POINTER`).
- Checkpoint policy: PASSIVE by default; TRUNCATE only when the host reports no pinned readers; never wait on readers while holding the write lock.
- Position recovery from the bucket alone (last L0 header: WAL offset, size, salts, commit), so a missing local directory does not force a snapshot when the WAL still matches.
- Public API: `start` returns once the read lock is held; `final_sync(deadline)` returns a typed outcome; `health()` is a cheap handle (last synced TXID, age, state, degraded or stop cause).
- Change detection by 1-second polling (a wal hook is a later optimisation).
- A WAL checksum or salt mismatch below the committed end is `Fault::WalCorruptBelowCommitted` with policy Halt, never end-of-WAL (Go returns EOF there, wal_reader.go:188). LTX pages are taken from the verified read; there is no second, unchecked read of a page.
- Page checksum table: page number to CRC, fed by verified WAL frames and seeded by the first snapshot. A database-file page that does not match is never uploaded. The policy for a mismatch and for an unexplained WAL rewind is fixed in the phase 3 task plan (P1, P2).
- Real-SQLite fault suite and all four compat directions, including Go, Rust, Go handover on one prefix with no forced snapshot.
**Exit:** the fault suite and the upstream-test ledger are green; handover in both directions happens without a snapshot when the WAL is intact; an alternation test switches Go and Rust on one prefix repeatedly, with a SIGKILL at a random point in each handover (mid-upload, mid-compaction, with local files not yet uploaded), and every switch is a plain restart with no forced snapshot and no restore mismatch (one exception, for Go builds without upstream bcd69ae, every release through v0.5.18: Go's boot transaction may hold a WAL tail SQLite never committed, left by a Rust process stopped mid-commit, and the test accepts that one restore when it equals exactly the tail's state; known Go bug #38, `docs/plans/phase-3-syncing.md`. A minimum Go version for handover will be named once upstream tags a release containing bcd69ae); a 72-hour soak against real Tigris with a synthetic writer shows zero oracle failures and the resource properties holding.

### Phase 4: the sidecar (a separate process, as Go runs)
Added 2026-11-20 at the owner's request. A `litestream`-style daemon over the same library, so a host that cannot embed Rust (or runs Go's sidecar today) can switch by replacing a binary.
- `lsr replicate` grown into a daemon: one process, many databases, each its own actor thread; signals for reload and shutdown (`final_sync` on SIGTERM within a deadline); `health()` exposed for the host (a status file or a local HTTP endpoint, decided in the phase 4 task plan).
- Configuration: whether to read Go's `litestream.yml` as is (drop-in) or a stricter subset that refuses unknown keys (design goal: "disabled" is not zero), decided in the phase 4 task plan.
- The pin across processes: a SQLite read transaction in the sidecar works as in phase 3 (ruling P0); POSIX locks conflict across processes, so the in-process descriptor hazard of section 6 is gone, and the pure-Rust pin of #9 is simpler here (`docs/notes/pure-rust-pin.md`, deployment B).
- Checkpoints from outside the host: the host must still run with `wal_autocheckpoint = 0` and `PERSIST_WAL`, which a sidecar cannot set for it; the phase 4 plan says how that is checked and what happens when the host checkpoints anyway (phase 3's outside-checkpoint cases already cover the replication side).
- Compatibility: the phase 3 alternation test, run as Go sidecar to Rust sidecar and back on one prefix.
**Exit:** the alternation test and the phase 3 fault suite pass with the library in a separate process; a Go user's config and meta directory start the Rust sidecar with no snapshot when the WAL is intact.

### Later versions
From the storage-fault review against TigerBeetle's fault model (2026-11-20). Repair from peers, quorum and direct I/O do not transfer to a single node with an asynchronous replica; these do: a low-rate scrubber of the live database against the page checksum table; rolling-checksum tracking (D2), which the table makes cheap; a second bucket as a repair source for restore; a parent-file checksum linking each file to its predecessor; a restore planner that routes around a corrupt file through the lower-level files that cover the same transactions (today a corrupt compacted file fails the restore even when its inputs survive).

## 6. Host integration

Out of scope for this repository. A host that adopts the library provides:

- The replicator started before any connection pool opens, with `PERSIST_WAL` and `wal_autocheckpoint = 0` on every connection, and no other checkpointer on the live file.
- No `std::fs` open of the database files anywhere in the process: closing any descriptor on the file drops every POSIX lock SQLite holds.
- `final_sync` called after its writers close, inside its shutdown budget.
- A signal for long-lived readers, so the library stays PASSIVE while one runs.
- One SQLite in the process, and it is ours: `lsr-sqlite` builds SQLite 3.54.1 through `rusqlite`'s `bundled` feature (`libsqlite3-sys` 0.40), and the `wal-index` and pin code depend on that build. `libsqlite3-sys` declares `links = "sqlite3"`, so Cargo allows one copy per build: the host's own `rusqlite`, `sqlx` or other SQLite crate must resolve to the same `libsqlite3-sys` 0.40 (a compatible `rusqlite` 0.43), or the build fails at resolution, never at run time. Cargo features unify, so the host's connections then run on the same bundled build.

## 7. What this design does not prevent

- A wrong belief about SQLite that both the simulator and the real-SQLite tests share. Mitigation: a shadow run in the host and scheduled verify-restores, permanently.
- A stale page inside a Go-written or v1 Rust-written chain: with `NoChecksum` files there is nothing to check a page against except a restore compared with the live database. Scheduled verify-restores are therefore a v1 requirement, not an extra.
- A stray process (`sqlite3`, a CLI) that checkpoints or deletes the WAL. The library detects it (`SnapshotCause`) but pays a snapshot.
- Shared fate: an OOM kill ends replication until restart; the unsynced tail stays on the volume.
- Ownership: upstream's fixes stop arriving for free (19 releases in about 13 months). Each upstream release is triaged against the invariant table and added to the compat matrix.

## 8. Decisions

| # | Decision | Outcome |
|---|---|---|
| D1 | Own repository or a crate in the host's workspace | **Decided: own repository.** Its own CI matrix (Go binaries, fuzz, Linux power-loss tests), no host dependency, usable by others |
| D2 | Track rolling checksums in Rust-written files | **Decided: not until after cutover.** v1 writes `NoChecksum` as Go does. Go's level compactor forces `NoChecksum` on its output while copying the last input's post-apply checksum (compactor.go:165 at 041a8c3), so a tracked last input fails with "post-apply checksum not allowed" (read in code, not run), and Go restore reads tracked chains only from v0.5.18. Tracking would break rollback to Go. Turn it on once Go is no longer the fallback writer. With the page checksum table, tracking needs no prior page content and no extra scan, so rollback is the only remaining reason to wait |
| D3 | Adopt the existing prefixes or start new ones | **Decided: adopt**, so tooling, the VFS and the retention window carry over |
| D4 | Local staging layout | **Decided: Go's meta-directory layout.** Requirement: switching between Go and Rust in either direction, at any moment, is a restart (phase 3 alternation test) |
| D5 | Function length limit | **Decided: start at 70 lines** (TigerStyle), with an `#[expect(…, reason)]` escape, and a checkpoint at phase 1 exit |
| D6 | How far v1 departs from Go | **Decided (owner, 2026-11-20): v1 copies Go's behaviour unless a better behaviour is obvious, and adds no file, header, metadata or object Go does not speak.** Correctness checks that read more (verify before use, gap errors, the whole-tip compare, the boot read-back) stay, since they change nothing Go sees. Improvements that need new state are parked in `docs/proposals/beyond-go.md` and its issues |
| D7 | When replication stops | **Decided (owner, 2026-11-20): as Go, keep trying.** Every persistent failure retries with Go's capped backoff (5 minutes) and shows as a typed degraded state in `health()` and events, scoped to the lane that failed. The one default stop is a second writer on the prefix (uploads and maintenance stop; continuing would interleave two databases into one history and delete the other writer's files). Each cause's response (keep trying or stop) is a config option, so an operator who prefers stopping can choose it. Phase 3 task 10 |

