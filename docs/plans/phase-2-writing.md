# Phase 2 (writing): implementation plan

**Goal:** turn a SQLite WAL into a Litestream 0.5.x replica that Go restores page for page, with every fault the library cannot prevent handled by a typed policy and proven in a seeded simulator.

**Spec:** `docs/PLAN.md` (phase 2, section 3 types and faults). Rules: `AGENTS.md`. Upstream (read-only): `.upstream/litestream` (`db.go`, `wal_reader.go`, `compactor.go`, `store.go`), `.upstream/ltx` (`compactor.go`).

## Global constraints
The constraints of `docs/plans/phase-1-reading.md` apply unchanged. In addition:
- Nothing in `lsr-core` reads a clock, a random number, a file or a socket except through a trait; the simulator must be able to replay any run from its seed.
- No timer-driven retry loop outside the store wrapper; the core state machine returns what to do next and when.
- Hard `assert!` exists only in the simulator's oracle.

## Review focus
1. A WAL frame that fails its checksum or salt below the committed end: a stop, never "end of WAL".
2. A WAL restarted (new salts) or truncated behind the reader: a typed recovery, never frames from the old generation.
3. A transaction that spilled to the WAL and never committed: none of its pages in any file.
4. An upload that fails after the object became visible: the next sync lists before it writes.
5. Compaction or retention racing a restore: the restore re-plans; retention never deletes what no higher level covers.

## Task order
1 (pure WAL reader) and 2 (compactor) need only phase 1. Then 3 (store wrapper: retry classifier, verify-on-upload), 4 (core state machine: recover, sync plan, checkpoint decision, upload sequencing), 5 (levels, snapshot schedule, retention), 6 (simulator, model oracle, one scenario per `Fault` and `SnapshotCause`), 7 (Rust to Go compat for chains written from synthetic WALs).

---

### Task 1: `lsr-wal`, the SQLite WAL reader (branch `wal-reader`)

**Files:** create `crates/wal/` (package `lsr-wal`; `src/{lib,header,frame,checksum,scan,pagemap,error}.rs`, `tests/`). Pure like `lsr-ltx`: `#![forbid(unsafe_code)]`, no I/O beyond `std::io::{Read, Seek}`, no clock. Depends on `lsr-ltx` for `PageNo`, `PageSize`, `Checksum`. Dev-dependencies: `rusqlite` (bundled), `proptest`, `tempfile`.

**Format** (https://sqlite.org/fileformat2.html#walformat; check every line against it and against `.upstream/litestream/wal_reader.go`):
- Header, 32 bytes, big-endian fields: magic `0x377f0682` or `0x377f0683` (the low bit picks big-endian checksum words), version 3007000, page size, checkpoint sequence, salt-1, salt-2, checksum-1, checksum-2 over the first 24 bytes.
- Frame: 24-byte header (page number, database size in pages after commit or 0, salt-1, salt-2, checksum-1, checksum-2) then one page. The checksum is cumulative: it continues from the previous frame's (or the header's) over this frame's first 8 header bytes and its page.
- A frame is valid when its salts equal the header's and its checksum matches. A transaction is the frames up to and including a frame with a non-zero database size.

**Produces:**
- `Salts { salt1: u32, salt2: u32 }`; `WalChecksum(u32, u32)`; `ByteOrder`; `WalHeader` with `decode(&[u8; 32]) -> Result<Self, WalHeaderError>` (magic, version, page size and its own checksum all checked) and accessors.
- `WalOffset`: a byte offset that is 32 plus a whole number of frames for a given page size (the only constructors check it); `CommittedEnd`: a `WalOffset` and the generation it counts, built by `CommittedEnd::new(mx_frame: u32, salts: Salts, page_size)` (`salts()` accessor), documented as "mxFrame and salts from one wal-index header read under the read lock; a connection on read mark 0 has no committed end to give". `next_tx` under `ScanLimit::Committed` returns `WalError::Restarted` when the end's salts differ from the WAL header's, before reading any frame.
- `FrameCursor { next: WalOffset, checksum: WalChecksum, salts: Salts }`: where the next frame starts and the checksum it continues from. `WalReader::open(reader: R) -> Result<Self, OpenError>` (`R: Read + Seek`); `cursor_at_start()`; `resume(at: WalOffset, expected: Salts) -> Result<FrameCursor, ResumeError>` takes the generation the caller remembered (Go's `NewWALReaderWithOffset` and `db.go` verify do the same): header salts other than `expected` are `ResumeError::Restarted { expected, found }`, also at the start; otherwise the frame before `at` is read whole and verified (salts, checksum chained from the frame before it, non-zero commit field), failing as `ResumeError::PrevFrame { at, fault }` or `ResumeError::NotAtCommit { at }`. Cost: two frame-header reads and one page read. `cursor_at_start` is for a caller with no position.
- `next_tx(&mut self, cursor: &FrameCursor, limit: ScanLimit) -> Result<TxOutcome, WalError>`. `ScanLimit { Committed(CommittedEnd), FirstInvalidFrame }`. `TxOutcome { Tx(CommittedTx), End { at: FrameCursor, tail: Tail } }`, `Tail { Clean, PartialFrame, InvalidFrame(FrameFault), Uncommitted { frames: u32 } }`. Under `ScanLimit::Committed` an invalid or missing frame before the end is `WalError::CorruptBelowCommitted { offset, fault }`, and a committed end that is not right after a commit frame is `WalError::EndNotAtCommit`; the reader never reads a byte at or past the end.
- `CommittedTx { frames: Vec<FrameRef>, commit: u32, end: FrameCursor }`, `FrameRef { pgno: PageNo, offset: WalOffset, digest: Checksum }` where the digest is `Checksum::of_page` computed during the checksum-verified pass.
- `PageMap`: `PageMap::new(page_size)`, `apply(&mut self, tx: &CommittedTx)` (a map is one contiguous run of one generation: the first transaction fixes the start and the salts, each later one must start exactly at the map's end, else `PageMapError::Gap` or `NotAfterEnd`, and carry the same salts, else `PageMapError::Generation`; newest frame per page wins; pages above the transaction's commit are dropped, including ones earlier transactions put there), `salts() -> Option<Salts>`, `pages() -> impl Iterator<Item = &FrameRef>` in ascending page order, `commit()`, `range() -> (WalOffset, WalOffset)`.
- `read_page(&mut self, frame: &FrameRef, buf: &mut [u8]) -> Result<(), ReadPageError>`: the second read of a page is checked against the digest from the first (`ReadPageError::ChangedSinceScan`), so no page reaches an LTX file unverified.
- `FrameFault { SaltMismatch { found: Salts }, ChecksumMismatch, BadPageNumber { raw: u32 } }` (page 0, or the lock page for this page size). Closed enums, no wildcard arms, every variant with a named test.

**Tests:**
- Differential against SQLite itself, as a property: a seeded workload on `rusqlite` (page sizes 512, 4096, 16384, 65536; `wal_autocheckpoint = 0`; inserts, updates, deletes, `VACUUM`-free growth and shrink via `incremental_vacuum`, a rolled-back transaction large enough to spill with `cache_size = 1`) with the database and WAL files copied after each commit; for each copy, the main file plus this crate's `PageMap` applied over it equals, page for page and in length, what SQLite produces from the same copy after `wal_checkpoint(TRUNCATE)`.
- A WAL copied while a spilled transaction is open: `Tail::Uncommitted`, none of its pages in the map.
- A WAL restarted after a checkpoint with old-generation frames left behind the new ones: the scan ends at the first stale frame with `Tail::InvalidFrame(SaltMismatch)`; `resume` with the old generation's salts is `Restarted`, at any offset including the start.
- Big-endian checksum WALs, built in the test by re-checksumming a real one.
- Every single-bit flip and every truncation of a small WAL: under `FirstInvalidFrame` the result is a prefix of the clean result, never a different page; under `Committed` it is `CorruptBelowCommitted` whenever the damage is below the end.
- `read_page` after the frame's bytes change: `ChangedSinceScan`.
- One named test per error variant; no input makes the reader panic, loop or allocate beyond the frames it was asked to scan.

### Task 2: the compactor (branch `compactor`)

**Files:** create `crates/core/src/compact.rs` and `crates/core/src/compact/tests.rs`; export from `crates/core/src/lib.rs`. A new test file `crates/compat/tests/compat/compaction.rs` (touch nothing else in `crates/compat` but its `Cargo.toml` if a dependency is missing). If the encoder needs a way to hand written bytes out between pages, add the smallest accessor to `crates/ltx/src/encoder.rs` with its own test.

**Consumes:** `lsr_core::{ObjectStore, Upload, FileInfo, FileKey, Level, ReadLen, StoreError}`, `lsr_ltx::{Decoder, Event, Verified, Encoder, Header, HeaderFields}`. The read loop is `apply_file`/`feed_chunk` in `crates/core/src/restore.rs`: one `Decoder` per file fed by chunked `store.read`; an empty chunk ends the file and only `decoder.finish()` says it was whole. When both loops need the same code, extract it into one private module both use, as a first commit that changes no behavior.

**Produces:**
- `pub const MAX_INPUTS: usize = 32` and a private read chunk of 1 MiB: memory is bounded by 32 chunks plus 32 pages plus the encoder, whatever the database size. (Go's fan-in is unbounded, upstream #1467. A level with more files than this is compacted into several destination files by the caller, task 5.)
- `CompactionInputs`: built only by `CompactionInputs::new(files: Vec<FileInfo>) -> Result<Self, InputsError>`, which requires 1 to `MAX_INPUTS` files, ascending, each starting at the transaction right after the previous one ends (stricter than Go's `IsContiguous`, which lets inputs overlap). `range() -> TxidRange`, `files()`.
- `async fn compact<S: ObjectStore, D: ObjectStore>(source: &S, dest: &D, inputs: &CompactionInputs, level: Level) -> Result<Compacted, CompactError>`. Source and destination differ because level 0 is read from the local copy.
- `Compacted { info: FileInfo, header: Header, trailer: Trailer, pages: u32, bytes_read: u64 }` with accessors.
- `CompactError { InputVanished { key }, Source(StoreError), Dest(StoreError), Input { key, fault: InputFault }, Encode(EncodeError) }`; `InputFault { Decode(DecodeError), RangeMismatch { header }, PageSizeChanged { expected, found }, ChecksumChain { expected, found }, PageSurvivesShrink { pgno } }`; `InputsError { Empty, TooMany { found }, NotContiguous { after, next } }`. Closed, no wildcard arms, one named test per variant.

**Semantics:**
1. Read every input's header first and check it (range equals key, one page size, and for tracked neighbours the earlier post-apply equals the later pre-apply, which needs the earlier file's trailer and so is checked when that file finishes). Nothing is uploaded before every header passed.
2. Output header, as `.upstream/ltx/compactor.go:137-147`: page size; commit, max TXID and timestamp from the last input; min TXID and pre-apply checksum from the first; no node id; no WAL origin. Tracked only when every input is tracked, then post-apply is the last input's; otherwise untracked with no checksums (decision D2: Go's level compactor writes `NoChecksum`).
3. Pages: a k-way merge in ascending page order; for each page number the newest input that holds it wins; a page above the output's commit is dropped; the lock page never appears.
4. A page whose newest copy comes from an input that a later input shrank below (a later input's commit is under the page number, the output's commit is not, and no later input rewrote it) is `PageSurvivesShrink`: SQLite writes every page it grows into, so such a chain was not written by SQLite, and Go's merge and an in-order apply would disagree on it. Refuse and leave the inputs alone.
5. The upload is committed only after every input's `finish()` returned `Verified` and every chain check passed; on any error the upload is aborted (an abort failure does not replace the original error). `created_at` is the output header's timestamp, as Go's file replica created-at times it.
6. `NotFound` on any input read is `InputVanished` (retention or a rival compactor got there first; the caller re-plans). No retry here.
7. A compaction whose first input is a snapshot (min TXID 1) produces a snapshot: every page 1 to commit except the lock page. If the merge leaves one out, that is an `Encode` or `Input` error, never a published file; check the encoder enforces it and add the check there if it does not.

**Tests** (`MemStore`; failures injected with its hooks):
- Property, the main one: for a generated chain of 1 to 32 files as SQLite would write it (growth always writes the new pages; shrinks; overwrites; page sizes 512, 4096, 65536 with databases crossing the lock page for 512), restoring snapshot plus the chain equals restoring snapshot plus the compacted file (`restore` with `MemSink`), page for page, and the output decodes to `Verified` with the header of semantics 2.
- Compacting the output of two compactions with a third equals compacting all inputs at once (same pages, same header).
- One input: the output holds the same pages and header fields as the input, minus node id and WAL origin.
- Tracked chains: checksums carry through and a restore over the compacted file passes its chain check; a mixed tracked/untracked set yields an untracked output; a broken link is `ChecksumChain` and nothing is published.
- Each fault at each point: a flipped byte in input i (every i for a small chain), a truncated input, an input vanishing at each read, a read error, upload write failure, commit failure after publish (`MemStore::inject_failure_after_publish`): the destination holds no new file unless the commit itself reported it, and no scratch survives.
- `PageSurvivesShrink` on a hand-built chain.
- A memory check: with 32 inputs of 2,000 pages each, peak buffered bytes stay under the stated bound (count bytes held, not the allocator).
- In `crates/compat/tests/compat/compaction.rs` (runs in `just check`, no Go binary): for every Go-written level 1 or 2 file in every fixture whose range is exactly covered by files one level down that are still present (at most 32), compacting those with this code yields Go's commit, TXID range, timestamp, pre-apply checksum and tracked flag, and the same page numbers with the same bytes. Assert the number of files compared is above a floor you measure, so the test cannot pass by comparing nothing.

### Task 3: the environment traits and the retrying store (branch `store-retry`)

**Files:** create `crates/core/src/env.rs`, `crates/core/src/retry.rs`, `crates/core/src/retry/tests.rs`; export from `lib.rs`; one new variant in `crates/core/src/store.rs` (`CorruptCause`). Touch no other file in `crates/core`.

**Produces:**
- `env.rs`, the only doors to time and chance in core: `trait Clock: Send + Sync { fn now(&self) -> Timestamp; }`, `trait Sleeper: Send + Sync { fn sleep(&self, wait: Duration) -> impl Future<Output = ()> + Send; }`, `trait Entropy: Send + Sync { fn below(&self, upper: u64) -> u64; }` (uniform in `0..upper`, `upper > 0`). Behind `testing`: `FakeClock`, `RecordingSleeper` (returns at once, records every wait), `SeededEntropy` (a fixed small PRNG written here, no dependency).
- `RetryPolicy { attempts: NonZeroU8, base: Duration, cap: Duration }` with `RetryPolicy::DEFAULT` (5 attempts, 100 ms, 5 s). Wait before attempt n+1 is full jitter: `entropy.below(min(cap, base * 2^(n-1)))` and never zero.
- `fn classify(err: &StoreError) -> Class`; `Class { Retry, RetryAfter(Duration), Missing, Permanent }`; exhaustive over `StoreError`: `Transient` is `Retry`; `Throttled` is `RetryAfter` when the backend named a wait and `Retry` otherwise; `NotFound` is `Missing`; `Auth`, `ScratchExhausted`, `Corrupt` are `Permanent`.
- `Retrying<S, P, E>` (`S: ObjectStore`, `P: Sleeper`, `E: Entropy`) implementing `ObjectStore`: `list`, `read`, `delete` and `begin_upload` are retried up to `attempts`; `Missing` and `Permanent` return at once; a `RetryAfter` longer than `cap` returns the error at once (long waits belong to the caller's schedule, not to a sleeping task). `Upload::write` and `commit` are never retried: the stream cannot be replayed and a failed commit may have published.
- The wrapper's upload counts the bytes written and, when `commit` returns a `FileInfo` whose `size` differs, returns `StoreError::Corrupt { key, cause: CorruptCause::CommitSizeMismatch { wrote, stored } }`.
- `Retrying::stats() -> RetryStats { calls, retries, gave_up, waited: Duration }` (atomics), for the simulator's resource properties.

**Tests:** one named test per `StoreError` variant's class; with `MemStore::inject_failure` and the fakes: success after k transient failures makes k recorded waits each within its bound; `attempts` failures return the last error with no extra wait; `NotFound` and `Auth` make no wait; `RetryAfter` over the cap makes no wait; upload write and commit failures pass through with one call each; the size mismatch case; property: for any failure schedule and seed, calls per operation never exceed `attempts` and the total waited never exceeds the sum of the bounds; the store conformance suite (`crates/core/src/conformance.rs`) passes over `Retrying<MemStore, _, _>`.

### Task 4: the sync engine (4a types, 4b recover and sync step, 4c checkpoint, 4d upload)

Order: 4a alone; then 4b and 4d in parallel; 4c after 4b. A blind review of the whole state machine follows 4c.

#### Rulings (these override the blocks below wherever they differ)

- **F1, what proves a WAL restart lost nothing: read-transaction continuity, with two conditions.**
  - `Continuity::Held(e)` means "during read-lock hold e, this position's synced end equalled the committed end". It is assigned in exactly three places: `recover` returning `Continue` when the newest staged file's end equals the committed end of the same `WalView`; a sync step ending `Idle` or `Through` whose epoch is the same before the scan and after the staging commit; the checkpoint protocol's step 6.
  - Everything else is `Broken`: a `Continue` whose cursor is below the committed end (this overrides recover rule 10, which pinned every `Continue`), and any step ending `Limited`.
  - Why: a reader pinned on read mark 0 lets the next writer restart the WAL exactly once. If the read lock was taken while frames past the cursor were still unsynced, that restart discards them. A read lock taken at or after "synced equals committed end" cannot lose anything: a mark above 0 blocks every restart, and under read mark 0 the old generation cannot grow and then restart (the pinned reader blocks the backfill a restart needs).
  - Abandoned-frames check: when `recover` would return `WalRestarted` and the new generation is still shorter than the old cursor offset, read the frame at the old cursor. A frame with the old salts whose checksum continues the cursor's chain means frames were appended after the read lock and then abandoned: return `Snapshot { GenerationChanged }` and emit a `ReadLockViolated` event.
  - Verdict logged with the Call: measured on SQLite 3.47.1. Under a mark above 0, `wal_checkpoint(RESTART)` and `TRUNCATE` return busy and salts never change. Under read mark 0 the next write restarts once (salt1 + 1, checkpoint_seq + 1), and the new generation can be neither backfilled nor restarted while the read lock holds. An old generation that grows under a read-mark-0 read lock is never backfilled or restarted.
  - Residual risk, recorded: POSIX advisory locks dropped by another descriptor in this process closing the same file silently break the read lock. Phase 3 owns that (one never-closed descriptor). Fallback if the proof ever fails in the simulator or the real-SQLite suite: always snapshot on a salt change.
  - Call: continuity proof instead of Go's last-page and salt-scan heuristics (why: Go's heuristics miss the app-checkpoint-while-down case; cost if wrong: frames lost across a restart, which the abandoned-frames check and the phase 3 fault suite exist to catch).
- **F2, same salts but the committed end or file is before the newest staged file's end: snapshot, not stop.** This overrides recover rule 5. The result is `Snapshot { after: newest_staged.txid, cause: SnapshotCause::LocalRewound { synced, committed } }` plus a loud `LocalRewound` event carrying both offsets. `StopCause::LocalLostReplicatedTx` and `Fault::LocalLostReplicatedTx` are not created.
  - Why: with `synchronous=NORMAL` a power loss legitimately drops a WAL tail that was already replicated, so this is an expected event. If the WAL regrows past the cursor before the next look, rule 9 (`LastPageMismatch`) already snapshots; stopping only when the rewind is caught early gives one event two outcomes by timing. A stop also leaves every later write unreplicated until a person acts. The replicated transactions the database lost stay restorable by txid or time until retention ages them out.
  - Call: snapshot on a local rewind (why: above; the designer recommended stop, so this disagreement goes to the owner in the report; cost if wrong: one match arm in `recover`).
- **F3, remote ahead of local at boot:** `Snapshot { after: remote newest, cause: BehindRemote { local } }`, as Go does. Mid-run it is `Stop(OtherWriter)`. A `node_id` check joins later.
- **F4:** staging is an `FsStore` at Go's meta path. **F5:** after any failed staged write, encode again; never trust or re-fsync the old file. **F6:** database-file pages are unverified in phase 2; phase 3's page checksum table closes it. **F7:** a busy TRUNCATE that left the header unchanged takes no boundary snapshot (Go does). **F8:** the `bump_seq` frame ships as an ordinary L0 file.
- **4d, what confirms an upload:** after a commit error (including `CommitSizeMismatch`, whether or not the store removed the object) the state is `ListFirst`, and a listing confirms the key only when the listed bytes equal the staged file's (size first, then a streamed comparison; with no staged file the object must decode and its header must name the key; only the newest key is read back). Anything else is deleted and uploaded again (task 5c, R1). This overrides 4d rule 7's "if the listing finds the key".
- **Which store failed:** `Fault::from_remote(&StoreError)` for the replica, `Fault::from_staging(&StoreError)` for the staging directory (they replace `from_store`). A staging error is never a remote fault: missing is `StagedLost`, a size-mismatched write is `LocalFsyncFailed` (encode again), anything else corrupt is `StagedCorrupt`, the rest is `LocalIo` or `LocalDiskFull`. Both matches are exhaustive over `StoreError` and `CorruptCause`.
- **4a as landed** (differences 4b to 4d build on): `Position::new(txid, cursor, commit, last_page, timestamp, continuity)` derives `synced` and the page size from the cursor; `CommittedRead::new` returns `Option`; `Fault::PageSizeChanged` exists; `StoreThrottled` carries `Option<Duration>`; the three `expect(dead_code)` attributes on `Position::new`, `Position::with_continuity` and `SyncedThrough::new` are deleted by the first task that calls them.
- `SnapshotCause` is therefore: `FirstSync`, `BehindRemote { local }`, `NewestStagedFileUnreadable`, `WalMissing`, `PageSizeChanged`, `GenerationChanged`, `PrevFrameMismatch`, `LastPageMismatch`, `LocalRewound { synced, committed }`, `CheckpointBoundary`, `OperatorRequested`. Its simulator scenario is `power_loss_wal_rewind`.

### Tasks 4a to 4d: the core sync state machine

Order: task 3 lands first, because 4a consumes `Clock`, `classify` and `Class`. Then **4a** lands alone. After 4a lands:
- **4b** (recover and the sync step) and **4d** (upload sequencing) run in parallel. They share only 4a's types.
- **4c** (the checkpoint) starts once 4b has landed, because its protocol runs 4b's sync step.

Each task lands alone with its own tests.

lsr-wal signatures are the ones now in `crates/wal/src/error.rs` and `scan.rs`:
- `WalReader::resume(&mut self, at: WalOffset, expected: Salts) -> Result<FrameCursor, ResumeError>`.
- `ResumeError { Io, PageSizeMismatch, Truncated { at }, Restarted { expected, found }, PrevFrame { at, fault }, NotAtCommit { at } }`.
- `WalError { Io, PageSizeMismatch, Restarted { cursor, header }, CursorPastEnd, CorruptBelowCommitted, TruncatedBelowCommitted, EndNotAtCommit, TooManyFrames }`.
- `ReadPageError { Io, BufferSize, PageSizeMismatch, Truncated, ChangedSinceScan }`.
- `PageMapError { PageSizeMismatch, NotAfterEnd, Gap, Generation }`.
- `CommittedEnd::new(mx_frame: u32, salts: Salts, page_size: PageSize)`.

Go citations are `.upstream/litestream/<file>:<line>` unless they name `.upstream/ltx`.

---

### Task 4a: positions, sources and the fault policy (branch `sync-types`)

**Files:**
- Create `crates/core/src/{position,source,fault}.rs`, `crates/core/src/source/fake.rs` (behind the `testing` feature) and `crates/core/src/fault/tests.rs`.
- Export the new modules from `lib.rs`.
- Touch no other file.

**Consumes:**
- From lsr-wal: `WalOffset`, `CommittedEnd`, `FrameCursor`, `FrameRef`, `Salts`, `WalHeader` and the four error enums above.
- From lsr-ltx: `Txid`, `PageNo`, `PageSize`, `Checksum`, `Timestamp`.
- From task 3, in lsr-core: `StoreError`, `classify`, `Class`.

**Produces, `position.rs`:**
- `SyncedOffset(WalOffset)` is the end of the WAL bytes covered by the newest staged L0 file. Its constructor is `pub(crate)`.
- `WalFileLen(u64)` is built only by a `WalSource`.
- The two types have no conversion between them. The only comparison is `WalFileLen::covers(self, end: SyncedOffset) -> bool`. Go compares the two as raw `int64` values (`db.go:1811-1813`), and bug #997 came from mixing them up (`db.go:1459-1461`).
- `ReadLockHold(u64)` counts how many times the read transaction has been taken. `Continuity { Held(ReadLockHold), Broken }`.
- `LastPage { pgno: PageNo, digest: Checksum }` is the page of the last frame the position covers.
- `Position { txid: Txid, cursor: FrameCursor, synced: SyncedOffset, commit: NonZeroU32, page_size: PageSize, last_page: LastPage, timestamp: Timestamp, continuity: Continuity }` has accessors and a `pub(crate)` constructor.
- `SyncedThrough(Position)` is the proof that a completed sync reached the committed end.
  - It is built only by the sync step, in `StepEnd::Through`.
  - It provides `position(&self) -> &Position` and `into_position(self) -> Position`.
  - It has no `Clone`.

**Produces, `source.rs`.** These traits are the only way core reaches SQLite or a file.
- `trait WalSource`, with `type Wal: Read + Seek`:
  - `fn open(&mut self) -> Result<Option<Self::Wal>, SourceError>`. `None` means there is no WAL file.
  - `fn len(&mut self) -> Result<Option<WalFileLen>, SourceError>`.
  - `fn committed(&mut self) -> Result<CommittedRead, SourceError>`.
  - `fn epoch(&self) -> ReadLockHold`.
- `CommittedRead { end: CommittedEnd, backfilled: u32 }`.
  - The frame count, the salts and `nBackfill` all come from one read of the wal-index.
  - `backfilled == end.frame_count()` means the WAL is fully backfilled. A connection on read mark 0 has no committed end of its own, but this read answers anyway.
  - In phase 3, this is the wal-index header at `-shm` offset 0, read under the read lock:
    - The two 48-byte copies must be equal, with a valid `aCksum`.
    - `mxFrame` is at byte 16 and `aSalt` at byte 32.
    - `nBackfill` is at byte 96.
    - If the copies differ, a writer is mid-update, and the read returns `SourceError::Busy`.
  - Go instead takes the count from `PRAGMA wal_checkpoint(NOOP)` and then rereads the WAL header to confirm the generation (`db.go:1927-1960`). That is two reads with a race between them, which this design rules out.
- `trait PageSource`:
  - `fn page_count(&mut self) -> Result<u32, SourceError>` is the database file length over the page size.
  - `fn read_page(&mut self, pgno: PageNo, buf: &mut [u8]) -> Result<(), SourceError>`. A short read is `SourceError::Short { pgno }`. Go ignores short reads (`db.go:2493`).
- `trait Checkpointer`:
  - `fn begin_barrier(&mut self) -> Result<Barrier, SourceError>` runs `BEGIN; INSERT INTO _litestream_lock (id) VALUES (1)` (Go `db.go:2660-2674`).
  - `fn end_barrier(&mut self, b: Barrier) -> Result<(), SourceError>` rolls the barrier back.
  - `fn checkpoint(&mut self, mode: CheckpointMode) -> Result<CheckpointRun, SourceError>` releases the read transaction, runs `PRAGMA wal_checkpoint(mode)`, then takes the read transaction again (Go `db.go:2805-2849`).
  - `fn bump_seq(&mut self) -> Result<(), SourceError>` (Go `db.go:1667`).
  - `fn begin_write_lock(&mut self) -> Result<WriteLock, SourceError>` and `fn end_write_lock(&mut self, l: WriteLock) -> Result<(), SourceError>` (Go `db.go:2759-2771` and `2795`).
  - `Barrier` and `WriteLock` are `#[must_use]` tokens with no `Clone`.
- `CheckpointRun { busy: bool, log: u32, checkpointed: u32, epoch: ReadLockHold }`. Go discards `busy` (`db.go:2838-2848`).
- `CheckpointMode { Passive, Truncate }`. These are the only two modes Go's own policy uses (`db.go:1524`, `1546`, `1555`, `1580`). Go's RESTART and FULL path at `db.go:2743-2751` is dropped.
- `SourceError { Io(io::ErrorKind), DiskFull, FsyncFailed, Busy, Short { pgno: PageNo }, Unlinked, ReadTxLost }`.
- Fakes, behind `testing`:
  - `FakeWal` is an in-memory WAL with a scripted `committed` and `epoch`.
  - `FakeDb` holds its pages in a `BTreeMap`.
  - `FakeCheckpointer` records its calls and applies a scripted `CheckpointRun` to a `FakeWal`. The script can be a restart (new salts with `salt1 + 1`, and `checkpoint_seq + 1`) or a truncate.
  - Each fake has `inject(op, SourceError)`.

**Produces, `fault.rs`:**
- `Fault` is a closed enum, one variant per fault, each with a named policy. The list and the simulator scenario for each are in task4-forks.md, section C.
- `SnapshotCause` and `StopCause` are closed enums.
- `Policy { Retry(Backoff), Resnapshot(SnapshotCause), Stop(StopCause) }` and `Backoff { Doubling, After(Duration) }`.
- `pub fn policy(f: &Fault) -> Policy` is one exhaustive match with no wildcard arm.
- `pub fn retry_wait(attempt: NonZeroU32, interval: Duration) -> Duration` returns `interval * 2^(attempt-1)`, capped at 5 minutes (Go `replica.go:449-457`). It has no jitter, as in Go.
- Exhaustive constructors:
  - `Fault::from_store(op: StoreOp, e: &StoreError)`, which goes through `classify`;
  - `from_wal(e: WalError)`;
  - `from_resume(e: ResumeError)`;
  - `from_read_page(e: ReadPageError)`;
  - `from_page_map(e: PageMapError)`;
  - `from_source(e: SourceError)`.
- `Next { Now, At(Timestamp), OnWalChange }` tells the actor what to do next. Core has no timer loop.
- `Clamped { prev: Timestamp, now: Timestamp }` is an event, not a fault.

**Semantics:**
1. `policy` is total and pure. It never depends on state, so the simulator can assert it per variant.
2. `from_store`:
   - `Transient` becomes `StoreTransient`.
   - `Throttled { retry_after }` becomes `StoreThrottled`. Its policy is `Retry(After)` when the store gave a wait, and `Retry(Doubling)` when it did not.
   - `Auth` becomes `StoreAuth`, a stop.
   - `NotFound` on a remote read becomes `RemoteObjectMissing`, a retry that lists again.
   - `ScratchExhausted` becomes `LocalDiskFull`.
   - `Corrupt { CommitSizeMismatch }` becomes `UploadAmbiguous`.
   - Any other `Corrupt` becomes `RemoteCorrupt`, a stop.
3. `from_wal`:
   - `CorruptBelowCommitted` becomes `WalCorruptBelowCommitted`, a stop.
   - `TruncatedBelowCommitted` becomes `WalTruncatedBelowCommitted`, a stop.
   - `Restarted` and `CursorPastEnd` become `WalMoved`, a retry. The next step reruns recover, which classifies the move.
   - `PageSizeMismatch` becomes `Resnapshot(PageSizeChanged)`.
   - `EndNotAtCommit` becomes `WalIndexInconsistent`, a retry.
   - `TooManyFrames` and `Io` become `LocalIo`.

   Go turns every bad frame into EOF (`wal_reader.go:180-182`, `191-193`). A corrupt committed frame therefore looks to Go like the end of the WAL.
4. `from_resume` is used only inside recover, which maps the variants to its own rules (recover, rules 6 to 8). `Io` becomes `LocalIo`.
5. `from_read_page`:
   - `ChangedSinceScan` becomes `WalChangedUnderRead`, a retry. The page is never encoded, and the step rescans.
   - `Truncated` becomes `WalTruncatedBelowCommitted`.
   - `BufferSize` and `PageSizeMismatch` become `Stop(Bug)`.
6. Every `from_page_map` variant is `Stop(Bug)`, because the step feeds the map only from one scan.

**Tests:**
- One named test per `Fault` variant, asserting its policy.
- One test per source variant for each `from_*` constructor.
- A proptest that `retry_wait` is non-decreasing and capped at 5 minutes.
- A guard test that greps `fault.rs` for `_ =>` and fails on any match.
- A compile-fail doctest showing that `let _: WalFileLen = synced.into();` does not compile.

---

### Task 4b: recover and the sync step (branch `sync-step`)

**Files:**
- Create `crates/core/src/{recover,sync}.rs`, `crates/core/src/recover/tests.rs` and `crates/core/src/sync/tests.rs`.
- Add one method, with its test, to `crates/wal/src/scan.rs`: `pub fn frame_before(&mut self, at: WalOffset, expected: Salts) -> Result<FrameRef, ResumeError>`. It uses the same verified read as `resume` and returns the commit frame's `FrameRef`, which carries its `pgno` and digest.
- Add `crates/compat/tests/compat/handover.rs`.

**Consumes:**
- 4a.
- From lsr-wal: `WalReader`, `ScanLimit`, `TxOutcome`, `PageMap`, `CommittedEnd`, `FrameCursor`.
- From lsr-ltx: `Encoder`, `Header`, `HeaderFields`, `WalOrigin`, `TxidRange`, `Decoder`.
- From lsr-core: `ObjectStore`, `Upload`, `FileKey`, `Level`, `FileInfo`, `Inventory`.

Staging is any `ObjectStore`. In production it is `FsStore` rooted at Go's meta directory, so keys land at `<meta>/ltx/0/<min>-<max>.ltx`, exactly as Go's `LTXPath` places them (fork F4).

**Produces:**
- `NewestStagedFile { Readable { txid: Txid, origin: WalOrigin, commit: NonZeroU32, page_size: PageSize, timestamp: Timestamp, last_page: LastPageCheck }, Unreadable { txid: Txid } }`.
  - It is built by `async fn NewestStagedFile::load<L: ObjectStore, R: Read + Seek>(staging: &L, wal: Option<&mut WalReader<R>>) -> Result<Option<NewestStagedFile>, Fault>`, which takes four steps:
    1. Read the newest level-0 key.
    2. Decode the header.
    3. Call `frame_before(origin end, salts)`.
    4. Find that page number in the file's pages and compare `Checksum::of_page`.
  - Steps 3 and 4 are Go's `lastPageMatch` (`db.go:1963-2007`).
  - `LastPageCheck { Match(LastPage), Mismatch, NotInNewestStagedFile, NoFrame(ResumeError) }`.
- `WalView { header: Option<WalHeader>, len: Option<WalFileLen>, committed: Option<CommittedRead>, epoch: ReadLockHold }` is read once per step through `WalSource`.
- `PositionSource<'a> { Boot(&'a NewestStagedFile), Running(&'a Position, Result<FrameRef, ResumeError>) }`. `Running` carries the result of calling `frame_before` at the cursor on the step's fresh reader.
- `pub fn recover(newest_staged: Option<PositionSource<'_>>, wal: &WalView, remote: Option<Txid>) -> Recovered` is a pure function.
- `Recovered { Continue(Position), WalRestarted(Position), Snapshot { after: Option<Txid>, cause: SnapshotCause }, Stop(StopCause) }`.
  - PLAN's `Fresh` becomes `Snapshot { after: None, cause: FirstSync }`.
  - PLAN's `BehindRemote` becomes `Snapshot { after: Some(remote), cause: BehindRemote { local } }`. Because it is resolved in the same value, no state can exist with it pending.
- `Step { Incremental(Position), NewGeneration(Position), Snapshot { after: Option<Txid>, prev_timestamp: Option<Timestamp>, cause: SnapshotCause } }`. `Step::try_from(Recovered)` fails only on `Stop`.
- `SyncConfig { cap: Option<NonZeroU64>, node_id: u64 }`.
  - The default is a 64 MiB cap and node 0 (Go `db.go:33-40`).
  - `None` means no cap. Zero cannot be expressed, which avoids Go's "zero means default" (#1348).
- `pub async fn sync_step<R: Read + Seek, P: PageSource, L: ObjectStore>(step: Step, wal: &mut WalReader<R>, committed: CommittedRead, epoch: ReadLockHold, db: &mut P, staging: &L, cfg: &SyncConfig, now: Timestamp) -> Result<Synced, StepFailed>`.
- `Synced { Idle(SyncedThrough), Wrote { file: FileInfo, end: StepEnd, clamped: Option<Clamped> } }` and `StepEnd { Through(SyncedThrough), Limited(Position) }`.
- `StepFailed { fault: Fault, retry: Step }`. `retry` is the step to run again, so a failure never loses the position.
- Pure helpers, each with a proptest:
  - `plan_pages(map: &PageMap, prev_commit: Option<u32>, commit: NonZeroU32, page_size: PageSize) -> Vec<PageFrom>`, with `PageFrom { Wal(FrameRef), Db(PageNo) }`;
  - `header_fields(txid: Txid, commit: NonZeroU32, page_size: PageSize, origin: WalOrigin, timestamp: Timestamp, node_id: u64) -> HeaderFields`;
  - `clamp(prev: Option<Timestamp>, now: Timestamp) -> (Timestamp, Option<Clamped>)`.

**Semantics of recover.** The rules are checked in order and the first match wins. Every branch of Go's `verifyWithExecutor` (`db.go:1782-1925`) is accounted for.
1. No newest staged file and no remote: `Snapshot { after: None, FirstSync }` (Go `db.go:1786-1789`).
2. The remote is ahead. That means either no newest staged file, or the newest staged file txid below `remote`. The result is `Snapshot { after: Some(remote), BehindRemote { local } }`.
   - Go deletes the local L0 directory, downloads the remote's last file, and then snapshots because they mismatch (`db.go:1689-1766`).
   - Rust deletes and downloads nothing (fork F3).
   - A remote below the newest staged file is normal: uploads are still pending.
3. `NewestStagedFile::Unreadable`: `Snapshot { after: Some(max(txid, remote)), NewestStagedFileUnreadable }`. Go returns `ErrLTXCorrupted` (`db.go:1800-1804`, `715-717`) and relies on auto-recover (`replica.go:484-494`), which is not copied.
4. No WAL header, meaning no file, under 32 bytes, or a bad header checksum: `Snapshot { WalMissing }`.
   - Go first calls `ensureWALExists` (`db.go:1673`). The phase 3 driver does the same.
   - The snapshot is still needed, because frames the app checkpointed while we were down were never seen.
5. The page size differs from the newest staged file's: `Snapshot { PageSizeChanged }`.
6. The salts differ from the newest staged file's (`Restarted` from `frame_before` lands here too):
   - If all of the following hold, the result is `WalRestarted`: a position at the new generation's start, with the same txid.
     - The newest staged file is `Running` with `Held(e)`.
     - `wal.epoch == e`.
     - `header.salt1 == old.salt1 + 1` (wrapping). `checkpoint_seq` is not checked: real SQLite writes the restarting connection's own checkpoint count into the header, so with several connections it is not the old value plus one. `salt1 + 1` comes from the shared wal-index header and is the proof (ruling V4).
   - Otherwise the result is `Snapshot { GenerationChanged }`.
   - This replaces two Go heuristics with the proof in fork F1:
     - Go checks the last page and scans for unknown salts (`db.go:1901-1920`, `2011-2039`).
     - Go accepts a file truncated after a "synced to end" flag (`db.go:1816-1838`, #927).
7. The salts are the same, but the committed end or the file length is before the newest staged file's end (or `frame_before` returns `Truncated`): `Stop(LocalLostReplicatedTx { synced, committed })`.
   - Frames we already shipped are gone locally, which is a power-loss rewind.
   - When the file is short, Go snapshots instead (`db.go:1840-1841`).
   - When only the index is behind, Go stalls forever: `SetEnd` is placed below the offset (`db.go:2216-2221`), so every read returns EOF (`wal_reader.go:158-160`). See fork F2.
8. `frame_before` returns `PrevFrame` or `NotAtCommit`: `Snapshot { PrevFrameMismatch }`.
   - Go reads that frame unverified (`wal_reader.go:69-77`).
   - On a mismatch, Go silently resets to offset 32 but keeps going incrementally (`db.go:2203-2213`). That path drops frames, so it is not copied.
9. The last page does not match: `Snapshot { LastPageMismatch }` (Go `db.go:1887-1897`, #1547).
   - At `Boot`, a mismatch is `Mismatch` or `NotInNewestStagedFile`.
   - When `Running`, a mismatch is a frame digest other than `position.last_page`.
   - The rule applies at every end past the first frame. Go skips the check when the end is at frame 1 (`db.go:1871-1882`).
10. Otherwise: `Continue(Position)`.
    - The cursor comes from `resume(end, salts)`.
    - `continuity` is `Held(wal.epoch)`.
    - `last_page` is the matched page.
    - An end exactly at offset 32 (`WALSize` 0) continues when the salts match (Go `db.go:1861-1866`).

**Semantics of the sync step:**
1. The txid is the first of `position.txid + 1`, `after + 1` or `Txid::MIN` that applies (Go `db.go:2152`).
2. The step reads a fresh `WalReader`.
   - The salts of `committed.end` must equal the header's. If they do not, the step fails with `WalMoved`.
   - It scans with `ScanLimit::Committed(committed.end)`. An `Incremental` step starts at the cursor; `NewGeneration` and `Snapshot` start at `cursor_at_start()`.
   - Each `CommittedTx` is applied to one `PageMap`.
   - Go likewise never reads past the index's end (`db.go:2216-2221`).
3. The cap applies only at commit boundaries.
   - After each transaction, if `range().1 - start >= cap`, the step stops and returns `Limited` (Go `wal_reader.go:241-244`).
   - A single transaction larger than the cap still makes one file.
   - A snapshot ignores the cap (Go `db.go:2230-2232`).
4. An `Incremental` or `NewGeneration` step with no transactions writes nothing and returns `Idle` (Go `db.go:2259-2263`). An idle database writes 0 files.
5. `commit` is `map.commit()` if non-zero, else `db.page_count()` (Go `db.go:2183-2188`, `2238-2240`). If both are zero, the step fails with `NothingToSnapshot`, which is retried (Go `errReplicaWaitForData`, `replica.go:196-198`).
6. `plan_pages` lists the pages to write, in ascending order.
   - **Snapshot:** every page from 1 to `commit` except the lock page. A page comes from the WAL when the map has it, and from the database otherwise (Go `writeLTXFromDB`, `db.go:2457-2502`).
   - **Incremental:** the map's pages, plus grown pages from the database: the pages from `prev_commit + 1` to `commit` that the map does not have, skipping the lock page (Go `db.go:2504-2561`).
   - WAL pages are read only through `read_page`, which checks them against the scan's digest. Go reads them raw (`db.go:2477`, `2542`).
   - Database pages come through `PageSource` (fork F6).
7. The header is what Go writes (`db.go:2294-2311`):
   - `tracked: false`, which is the `NoChecksum` flag (D2).
   - The page size and `commit`.
   - `txids: single(txid)`.
   - `pre_apply: None` and `finish(None)`.
   - `wal: WalOrigin::new(start, end - start, salts)`, where `start` is the cursor for `Incremental` and 32 for the other two steps.
   - The timestamp from `clamp(prev, now)`. Go uses `time.Now()` without a clamp (`db.go:2294`).
   - `node_id: cfg.node_id`. Go never sets it, so it writes 0.
8. Staging, and what happens when it fails:
   - The file is staged with `staging.begin_upload(key, created_at = timestamp)`, then `write`, then `commit`.
   - FsStore's commit fsyncs the file, renames it and fsyncs the directory, as Go does (`db.go:2373-2409`).
   - On any error the position does not move, and `StepFailed` carries the same step.
   - A retry encodes the file again and overwrites the same key.
   - A file whose fsync failed is never trusted, because the position only moves on `Ok`. The WAL frames are still there, because no checkpoint runs without a `SyncedThrough`.
9. The new position:
   - It records the txid, a cursor at the last transaction's end, `synced`, `commit` and the timestamp.
   - `last_page` is the commit frame of the last transaction, the same page Go records (`db.go:2318-2327`).
   - `continuity` is unchanged when `epoch` equals the read lock. A different epoch fails the step with `ReadTxLost`.
   - The step returns `Through` when the scan reached `committed.end`, and `Limited` otherwise.
10. D4, Go resuming from a Rust-written newest staged file. Go checks these things, so this step guarantees them:
    1. The file is at `ltx/0/<16 hex>-<16 hex>.ltx` with min equal to max (`db.go:1792`).
    2. `Decoder.Verify` passes (`db.go:715`).
       - `Header.Validate` (`.upstream/ltx/ltx.go:218-270`) needs `NoChecksum`, a zero pre-apply checksum, and a non-zero offset whenever salts are present.
       - `Trailer.Validate` needs a post-apply checksum of 0 under `NoChecksum` (`ltx.go:364-368`).
    3. `WALOffset + WALSize` is at most the WAL file's size (`db.go:1810-1813`).
    4. The salts equal the WAL header's (`db.go:1851`).
    5. Past frame 2, the frame one frame before the end carries the same salts, and its bytes equal the file's copy of that page (`db.go:1963-2007`). This holds because the frame is the last commit frame and `PageMap` keeps the newest frame of each page.
    6. `Commit` becomes Go's `prevCommit` (`db.go:1808`, `2511`), and the next txid is `MaxTXID + 1` (`db.go:719`, `2152`).
11. D4, Rust resuming from a Go-written newest staged file. Rust checks these things:
    1. The header decodes, and `wal` is `Some` with an aligned end. Otherwise the newest staged file is `Unreadable`, which leads to a snapshot.
    2. Recover's salt and length rules, 6 and 7.
    3. `frame_before` verifies the checksum chain and that the frame is a commit frame. This is stricter than Go, which loads that frame unverified (`wal_reader.go:72`).
    4. Recover's last-page rule, 9.
    5. The tracked flag, the node id and the timestamp are read without checks. The timestamp seeds the clamp.

**Tests.** These use 4a's fakes, with `MemStore` as staging.
- **Recover, generated:** a proptest over generated `(newest_staged, WalView, remote)` triples. Each triple must give the variant its rule names, and recover never panics.
- **Recover, named:** one test per rule, plus Go-parity tests for:
  - the first sync;
  - a WAL truncated after a "synced to end" flag, which is now a snapshot;
  - an end at offset 32;
  - an end one frame past 32;
  - a salt reset;
  - an unknown salt.
- **Differential sync** (the main property):
  - Run `rusqlite` workloads as in task 1, copying the database after each commit.
  - Call `sync_step` repeatedly with a random cap.
  - The staged chain applied in txid order over an empty database must equal SQLite's own checkpointed copy, page for page.
  - No page of an uncommitted, spilled transaction may appear in any file.
- **Page sources:**
  - grown pages, built by hand;
  - a snapshot crossing the lock page at a page size of 512;
  - a snapshot of an empty WAL (`WALSize` 0, `commit` from the database).
- **Cap:** every limited end falls on a commit boundary, and one oversized transaction makes one file.
- **Faults:**
  - `ChangedSinceScan` gives a retry and no file;
  - a staging commit error gives the same step back, and its retry overwrites the file;
  - an epoch change gives `ReadTxLost`;
  - a corrupt committed frame gives a stop.
- **Clamp:** a clock stepping backwards gives an equal timestamp and one `Clamped`.
- **`handover.rs`:**
  - **Go to Rust:** for every Go meta directory fixture with its WAL, recover returns `Continue`, and the next file passes Go's header rules.
  - **Rust to Go:** a transcription of Go's rules from `db.go:1782-1925`, `go_would_continue(newest_staged_file_header, newest_staged_file_pages, wal_bytes) -> bool`, returns true at every newest staged file of Rust-written chains.
  - The run against the real Go binary belongs to phase 2, task 7.

---

### Task 4c: the checkpoint decision and protocol (branch `checkpoint`)

**Files:** create `crates/core/src/checkpoint.rs` and `crates/core/src/checkpoint/tests.rs`, and export them from `lib.rs`.

**Consumes:** 4a, and 4b's `sync_step`, `Step`, `Synced`, `StepEnd`.

**Produces:**
- `CheckpointConfig { interval: Option<Duration>, min_pages: Option<NonZeroU32>, truncate_pages: Option<NonZeroU32> }`.
  - The defaults are 60 s, 1000 and 121359 (Go `db.go:33-40`).
  - `None` turns a trigger off.
- `CheckpointState` replaces six Go fields with one enum:
  ```
  enum CheckpointState {
      Idle { last: Timestamp, expect_bump: bool },
      Pending { last: Timestamp },
      PassiveFailedAtTruncate { last: Timestamp },
  }
  ```
  Where each Go field went:
  - `truncatePassiveFailed` is the `PassiveFailedAtTruncate` variant.
  - `syncedSinceCheckpoint` is the difference between `Pending` and `Idle`.
  - `lastSyncedWALOffset` is `Position::synced`.
  - `syncedToWALEnd` is replaced by `Continuity` (fork F1).
  - `checkpointAttempted` is gone. It existed only because a snapshot held `chkMu` (`db.go:2626-2633`), and a single actor makes that impossible.
  - The database mtime (`db.go:1573-1579`) is replaced by `last`.

  The first four fields are in `db.go:226-259`.
- `Observed { now: Timestamp, synced_frames: u32, through: bool }`.
- `Decision { Nothing, Passive(Why), Truncate }`, with `Why { TruncateThreshold, MinPages, Interval }`.
- Pure functions:
  - `decide(&CheckpointState, &Observed, &CheckpointConfig) -> Decision`;
  - `after_sync(CheckpointState, wrote_txs: u32, &Observed, &CheckpointConfig) -> CheckpointState`;
  - `after_checkpoint(CheckpointState, Decision, &CheckpointOutcome, &Observed, &CheckpointConfig) -> (CheckpointState, Next)`;
  - `next_wake(&CheckpointState, &CheckpointConfig) -> Option<Timestamp>`.
- `CheckpointOutcome { Busy, NotRestarted, Restarted, Truncated }`.
- `pub async fn checkpoint<W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore>(proof: SyncedThrough, mode: CheckpointMode, wal: &mut W, db: &mut P, ck: &mut C, staging: &L, cfg: &SyncConfig, now: Timestamp) -> Result<Checkpointed, CheckpointFailed>`.
  - `Checkpointed { through: SyncedThrough, outcome: CheckpointOutcome, files: Vec<FileInfo> }`.
  - `CheckpointFailed { fault: Fault, position: Position }`.
  - The proof is consumed and a new one is returned. The proof carries the position, so a stale one cannot be passed.

**Semantics of `decide`** (Go `checkpointIfNeeded`, `db.go:1507-1594`):
1. When `through` is false, only the truncate threshold can trigger (Go `db.go:1410-1414`). The threshold is measured on the synced frames, not on the file size (#997).
2. At `synced_frames >= truncate_pages`, `PassiveFailedAtTruncate` gives `Truncate`, and any other state gives `Passive(TruncateThreshold)` (Go `db.go:1517-1541`).
3. At `synced_frames >= min_pages`, the decision is `Passive(MinPages)` (Go `db.go:1554`).
4. When the state is `Pending`, `now - last >= interval` and `synced_frames > 1`, the decision is `Passive(Interval)` (Go `db.go:1571-1579`).
5. Otherwise, `Nothing`.

**Semantics of `after_sync` and `after_checkpoint`:**
1. Below the truncate threshold, `PassiveFailedAtTruncate` becomes `Pending` (Go `db.go:1511-1513`).
2. A sync that wrote transactions moves `Idle` to `Pending`, with one exception:
   - `Idle { expect_bump: true }` absorbs exactly one one-transaction sync, which is our own `bump_seq` frame, and moves to `Idle { expect_bump: false }`.
   - This keeps PLAN's "idle database: 0 checkpoints", which Go only partly reaches (#896, `db.go:1503-1506`).
3. After `Passive(TruncateThreshold)`:
   - `Busy` or `NotRestarted` gives `(PassiveFailedAtTruncate, Next::Now)`. Go runs the TRUNCATE in the same call (`db.go:1524-1546`).
   - `Restarted` gives `Idle` (Go `db.go:1530-1537`).
4. `Busy` after `MinPages` or `Interval` leaves the state unchanged. Go also ignores busy here (`db.go:1556-1561`, `1583-1586`).
5. Every other completed checkpoint gives `Idle { last: now, expect_bump: true }`.

**Semantics of the protocol** (Go `checkpointWithExecutor`, `db.go:2617-2801`):
1. The proof stands in for Go's copy-before sync (`db.go:2654`).
2. For `Passive`:
   1. Call `begin_barrier`.
   2. Run a seal sync, `sync_step(Incremental)` with the cap removed. It must return `Through` (Go `db.go:2660-2681`).
3. Call `ck.checkpoint(mode)`.
   - If taking the read transaction again fails, the fault is `ReadTxLost` and the position becomes `Broken`.
   - Go ignores this error in a defer (`db.go:2827`).
4. Call `end_barrier` on every path after a successful begin (Go `db.go:2666-2670`, `2701-2706`).
5. Call `bump_seq` (Go `db.go:2708`).
6. Read the WAL header again (Go `db.go:2718-2724`).
   - If it is unchanged and `busy` was set, the outcome is `Busy`.
   - If it is unchanged otherwise, the outcome is `NotRestarted`.
   - In both cases the position is pinned at the new epoch. That is sound only because the barrier held writers out until the read transaction was taken again.
7. If the header changed under `Passive`, run `sync_step(NewGeneration)` from the new start, at the new epoch. The outcome is `Restarted` (Go `db.go:2727-2735`).
8. For `Truncate`:
   - There is no barrier, because TRUNCATE takes the writer lock itself (Go `db.go:2661`).
   - If the header changed or the file became shorter, the protocol runs:
     1. `begin_write_lock`;
     2. `sync_step(Snapshot { cause: CheckpointBoundary })`, from 32 with the new salts;
     3. `end_write_lock`.

     The outcome is `Truncated` (Go `db.go:2737-2800`).
   - If the checkpoint was busy and the header is unchanged, no snapshot is taken: the generation is the same and no frame was lost. Go snapshots in this case too.
9. A fault at any point returns `CheckpointFailed` with the last good position. Any barrier or lock that was taken is released first. A failure to release never replaces the original fault.

**Tests:**
- A proptest of `decide` against a transcription of Go's priority ladder. The two must agree except where this design changes Go, and each change has a named test:
  - the truncate PASSIVE now returns `Next::Now`;
  - the interval uses `last` instead of the mtime.
- At most one PASSIVE per tick.
- An idle database makes 0 further checkpoints and 0 files over 100 ticks.
- With the fakes:
  - A PASSIVE that restarts gives `Restarted`, and the next recover gives `Continue`.
  - A PASSIVE blocked by a reader gives `NotRestarted`.
  - A TRUNCATE gives one boundary snapshot.
  - A writer that commits between the seal and the TRUNCATE has its commit in the snapshot.
- Each `SourceError` injected at each protocol step:
  - the barrier is released;
  - the position moves only for a step that completed;
  - the policy is the documented one.

---

### Task 4d: upload sequencing (branch `uploader`)

**Files:** create `crates/core/src/upload.rs` and `crates/core/src/upload/tests.rs`, and export them from `lib.rs`.

**Consumes:**
- 4a.
- From lsr-core: `ObjectStore`, `Upload`, `Inventory`, `FileKey`, `Level`.
- From lsr-ltx: `Decoder`, `Verified`.

**Produces:**
- `UploadState { Unknown, At(Option<Txid>), ListFirst { confirmed: Option<Txid> } }`.
- `UploadPlan { Idle, List, Upload(FileKey) }`.
- `UploadConfig { files_per_tick: Option<NonZeroU32> }`, which is Go's `MaxSyncLTXFiles` (`replica.go:208-218`).
- `pub fn plan(state: &UploadState, newest_staged: Option<Txid>) -> UploadPlan`, a pure function.
- `pub async fn upload_next<L: ObjectStore, S: ObjectStore>(state: UploadState, staging: &L, remote: &S, newest_staged: Option<Txid>) -> Result<Uploaded, UploadFailed>`.
- `Uploaded { state: UploadState, did: Did }`, with `Did { Nothing, Listed { newest: Option<Txid> }, Put(FileInfo) }`.
- `UploadFailed { state: UploadState, fault: Fault }`.

**Semantics:**
1. **Order.** Files are uploaded one at a time, in ascending txid order. The next upload is always `confirmed + 1`, and uploads stop when `confirmed == newest_staged` (Go `replica.go:206-228`).
2. **Listing.** `Unknown` (at boot) and `ListFirst` both plan `List`. A list finds the newest txid on the remote with `Inventory::newest` over all levels.
   - Go lists level 0 only (`replica.go:274-299`).
   - Listing all levels keeps this correct when task 5's retention deletes old L0 files.
3. **What a listing can show.**
   - A remote newer than `newest_staged`: stop with `RemoteAheadOfLocal`.
   - A remote older than `confirmed`: stop with `RemoteRegressed`.
   - Otherwise: `At(newest)`.

   At boot the order is list, then recover with that remote, then upload. A remote ahead at boot is therefore handled by recover's rule 2, never by this stop.
4. **Verify on upload.** Each staged file is streamed through a `Decoder` while it is uploaded. The upload commits only if `finish()` returns `Verified` and the header's txid range equals the key.
   - Otherwise the upload is aborted with `StagedCorrupt`, a stop.
   - Go uploads the file without reading it (`replica.go:248-256`).
5. **Missing staged file.** `NotFound` on the staged file is `StagedLost`, a stop. Go instead feeds this case to auto-recover (`replica.go:481-494`).
6. **Errors before commit.** A failure in `begin_upload`, in `write` or in a staging read leaves the state unchanged.
   - Go clears its position on any error (`replica.go:170-177`), which costs one LIST per error.
7. **A commit error, including `CommitSizeMismatch`,** gives `ListFirst { confirmed }`, and the next plan is `List`.
   - If the listing finds the key, the state becomes `At(txid)`.
   - If it does not, the same key is uploaded again. Re-uploading the same bytes is safe.
8. **Monotonic position.** `At(t)` is only ever replaced by a value at or above `t`.
   - The remote is never newer than local.
   - Steady state makes 0 LIST calls.
9. **Per tick.** After `files_per_tick` uploads, return `Next::Now`.

**Tests:**
- A proptest of `plan` and the state transitions over random outcome scripts:
  - uploads are strictly ascending;
  - nothing is uploaded while in `ListFirst`;
  - `confirmed` never decreases;
  - nothing is uploaded above `newest_staged`.
- With `MemStore`:
  - `inject_failure_after_publish` on commit gives one LIST and no duplicate upload;
  - a commit failure before publish gives one LIST, then a re-upload;
  - transient write errors give 0 LISTs;
  - every single-byte flip of a small staged file aborts the upload and leaves nothing visible;
  - a missing staged file gives `StagedLost`;
  - the `RemoteAheadOfLocal` and `RemoteRegressed` stops;
  - `files_per_tick` is honored.
- In a steady-state run of 1,000 ticks, there are 0 LIST calls after boot.

---

### Task 5: maintenance (5a schedule and executor, 5b retention)

Order: 5a, then 5b. 5a is independent of task 4. Retention deletes get a blind review after 5b lands.

#### Rulings

- **F1:** an aged L0 file is deleted only when kept files of levels 1 to 8 cover its whole range (Go compares with L1's max only). **F2:** only a hole after the newest snapshot holds deletes. **F3:** ages use `FileInfo.created_at`, measured from `min(now, newest created_at)`. **F4:** `due` returns one job and takes `published: Option<Txid>`. **F5:** level 1 reads local first, the replica once on `InputVanished`. **F6:** config is stricter than Go (increasing intervals, snapshot interval above the top level's, retention at least the snapshot interval).
- A copied file replica whose stored times are out of order holds every delete (`SnapshotCreatedAtBackwards`). That is the intended fail-closed result; the hold's text says the stored times no longer follow the headers. A command that resets stored times from the headers is a later item.
- Call: stricter config than Go (why: each refused shape makes retention or cadence meaningless; cost if wrong: a config Go loads is refused here with the field named).
- Call: header created-at time as the time base instead of Go's S3 upload time (why: one time base for restore and retention; cost if wrong: files kept longer when the writer's clock runs behind).

### Task 5a: the maintenance schedule and executor (branch `maintain-schedule`)

**Files:** create `crates/core/src/maintain.rs` (planner, jobs, executor), `crates/core/src/maintain/config.rs` (`Maintenance` and its errors) and `crates/core/src/maintain/tests.rs`; export from `crates/core/src/lib.rs`. Add one accessor to `crates/core/src/inventory.rs` with its own test: `pub fn level(&self, level: Level) -> &[FileInfo]` (the slice of `files()` at that level, found by `partition_point` on the key order). Touch no other file.

**Consumes:** `lsr_core::{Inventory, FileInfo, FileKey, Level, is_snapshot_start, ObjectStore, StoreError, CompactionInputs, InputsError, MAX_INPUTS, compact, Compacted, CompactError}`, `lsr_ltx::{Timestamp, Txid, TxidRange}`. The planner never reads a clock: `now` is an argument, and task 4 passes `Clock::now()`. Retries come from wrapping both stores in `Retrying` (task 3); the executor never retries.

**Go behavior this task reproduces** (read-only, `.upstream/litestream`; check each line against the file):
- Defaults: level intervals L1 30 s, L2 5 min, L3 1 h (`compaction_level.go:12-17`); snapshot interval 24 h and snapshot retention 24 h (`store.go:80-81`, repeated at `cmd/litestream/main.go:365-366`); L0 retention 5 min (`store.go:88`); L0 retention check interval 15 s (`store.go:92`).
- Validation: levels contiguous from 0, at most level 8, level 0's interval is 0, every other interval above 0 (`compaction_level.go:65-85`); snapshot interval and retention above 0, L0 retention not negative (`cmd/litestream/main.go:395-418`); retention below interval is only a warning (`cmd/litestream/main.go:542-550`). Go does not require increasing intervals.
- Windows: `PrevCompactionAt = now.Truncate(interval)`, next is prev plus interval (`compaction_level.go:32-40`). Go's `Time.Truncate` counts from Go's zero time (0001-01-01 UTC), not the Unix epoch. The two agree for every interval dividing a day; for 7 days they do not.
- A level is too early when the destination's newest file has `CreatedAt` after the previous window start; nothing is done when the source's max TXID is at or below the destination's (`store.go:797-858`, idle fix 703a7aa / upstream #1564). The inputs are every source file whose min TXID is above the destination's max (`compactor.go:104-195`; the seek is `file/replica_client.go:118`), with no fan-in limit (upstream #1467).
- A snapshot is written when the newest snapshot's max TXID is below the database position, and the snapshot level is too early by the same window rule at the snapshot interval (`store.go:584-589`, `store.go:797-858`). The writer is `db.Snapshot`, which is task 4's job here.
- Go reads level 0 from the local copy first and the replica second (`compactor.go:134-140`).

**Produces:**
- In `config.rs`:
  - `pub struct Interval(u64)`, whole milliseconds, built only by `Interval::new(d: Duration) -> Result<Self, ConfigError>` (refuses zero and any sub-millisecond remainder); `as_duration()`, `millis()`.
  - `pub enum L0Retention { Keep, After(Duration) }`. `Keep` is Go's `l0-retention: 0` (fix 63225f1 / upstream #1425): level 0 is never deleted.
  - `pub struct Maintenance`, fields private, built only by `Maintenance::new(levels: Vec<Duration>, snapshot_interval: Duration, snapshot_retention: Duration, l0: L0Retention) -> Result<Self, ConfigError>`. `levels[i]` is level i+1's interval. `Maintenance::go_default() -> Self` builds Go's defaults through `new` (an `Err` there is a test failure, so `go_default` returns the value from a `const` table a test feeds through `new`). Accessors: `top() -> Level`, `interval(level: Level) -> Option<Interval>` (`None` for level 0, levels above `top`, and 9), `snapshot_interval() -> Interval`, `snapshot_retention() -> Duration`, `l0_retention() -> L0Retention`.
  - `pub enum ConfigError { NoLevels, TooManyLevels { found: usize }, ZeroInterval { what: Setting }, SubMillisecond { what: Setting }, NotIncreasing { level: Level, interval: Duration, below: Duration }, SnapshotIntervalNotAboveTop { snapshot: Duration, top: Duration }, RetentionBelowInterval { retention: Duration, interval: Duration } }`, `pub enum Setting { Level(Level), SnapshotInterval, SnapshotRetention, L0Retention }`. `TooManyLevels` above 8.
- In `maintain.rs`:
  - `pub enum Job { Compact(CompactJob), Snapshot(SnapshotJob), Delete(DeleteJob) }`.
  - `pub struct CompactJob` built only by `CompactJob::new(inputs: CompactionInputs, level: Level) -> Result<Self, JobError>`: `level` is the destination, 1 to 8, and every input is at `level - 1`. Accessors `inputs()`, `level()`, `output_key() -> FileKey`.
  - `pub struct SnapshotJob { pub after: Option<Txid>, pub through: Txid }`: write a snapshot when the database position is above `after` (the newest snapshot's max, `None` when there is none). `through` is the newest position the planner saw; task 4 snapshots its own position, which is at least this.
  - `pub const MAX_DELETE: usize = 1000` (S3 `DeleteObjects` takes at most 1,000 keys; `s3/replica_client.go:58`). `pub struct DeleteJob` built only by `DeleteJob::new(keys: Vec<FileKey>) -> Result<Self, JobError>`: 1 to `MAX_DELETE` keys, strictly ascending. `keys()`. This task constructs it only in tests; task 5b's planner produces it.
  - `pub enum JobError { LevelOutOfRange { level: Level }, InputAtWrongLevel { key: FileKey, expected: Level }, NoKeys, TooManyKeys { found: usize }, KeysNotAscending { after: FileKey, next: FileKey } }`.
  - `pub enum Due { Run(Job), Wait { until: Option<Timestamp> } }`. `until: None` means nothing becomes due without a new write.
  - `pub fn due(inventory: &Inventory, now: Timestamp, config: &Maintenance, published: Option<Txid>) -> Due`. `published` is the newest TXID task 4 knows it committed to this replica (`None` before its first confirmed upload). One job per call; the caller runs it, loads a fresh `Inventory`, and calls again.
  - `pub fn go_truncate(now: Timestamp, interval: Interval) -> Timestamp`: Go's `Time.Truncate` on milliseconds, counted from Go's zero time (`-62_135_596_800_000` ms), rounding toward the past for every input, saturating.
  - `pub enum Outcome { Compacted { info: FileInfo, bytes_read: u64 }, Deleted { count: usize }, Replan { key: FileKey }, Failed(MaintainError) }` and `pub enum MaintainError { Compact(CompactError), Delete(StoreError) }` (thiserror, `#[source]` on both).
  - `pub async fn run_compact<L: ObjectStore, R: ObjectStore>(job: &CompactJob, local: &L, replica: &R) -> Outcome` and `pub async fn run_delete<R: ObjectStore>(job: &DeleteJob, replica: &R) -> Outcome`. There is no executor for `SnapshotJob`: task 4's loop matches `Job` exhaustively and writes the snapshot itself, so no arm here pretends to run it.

**Semantics:**
1. `Maintenance::new` checks, in this order: at least one level, at most 8; every interval, the snapshot interval and the snapshot retention are nonzero whole milliseconds (`L0Retention::After(d)` too, and `After(Duration::ZERO)` is `ZeroInterval`: zero is spelled `Keep`); intervals strictly increasing; the snapshot interval above the top level's; the retention at least the snapshot interval. The last three are stricter than Go (see the Go list above) and each gets a doc line saying so.
2. `due` reads nothing but its arguments and keeps nothing between calls. The same arguments give the same answer, so a restarted process, or a second process reading the same listing, plans the same job.
3. Order: compaction of level 1, then 2, up to `top`; then the snapshot; the first job that is ready wins. Lower levels go first so a higher level compacts what the lower one just produced.
4. Compaction of level n (source n-1). Let `dst` be `inventory.level(n)`, `dst_max` the max TXID of its last file (none when empty), and `src` the files of level n-1 whose min TXID is above `dst_max` (all of them when `dst` is empty), in key order. A source file that straddles `dst_max` is skipped, as in Go.
   - `src` empty: idle, no wake time. This is the idle rule: no write, no job.
   - Too early: `dst`'s last file has `created_at` after `go_truncate(now, interval)`. Not a job; wake at that window start plus the interval.
   - Otherwise the inputs are the longest prefix of `src` that is strictly contiguous (each file starting at the previous max plus one), cut at `MAX_INPUTS`. A hole in the source ends the run and the next call starts after it, so a hole one level down becomes the same hole here and never a file claiming transactions nobody wrote. (Go merges across it.)
   - The output's `created_at` is the last input's (task 2), so a backlog keeps passing the too-early test until a run's last input is newer than the window start: 100 source files drain as four jobs in one window, and the last one closes it.
   - `CompactionInputs::new` and `CompactJob::new` cannot fail on a run built this way; if one does, the level is treated as idle and the property test in Tests proves the branch is never taken. No `expect`.
5. Snapshot. Let `pos` be the larger of `inventory.newest()` and `published`; none means no job. Let `snap` be the newest level-9 file for which `is_snapshot_start` holds (malformed ones are ignored here, and `verify` reports them).
   - `snap` absent: `Run(Snapshot { after: None, through: pos })` at once.
   - `pos` at or below `snap`'s max: idle (an idle database writes no snapshot).
   - `snap.created_at` after `go_truncate(now, snapshot_interval)`: wait for the next window.
   - Otherwise `Run(Snapshot { after: Some(snap max), through: pos })`.
6. `Wait.until` is the earliest wake time among the levels and the snapshot that have work pending; `None` when none has.
7. Windows need not align for any reader: `plan` (and Go's restore) need only contiguous ranges, and Go's own output boundaries are whatever source files exist at the tick. Alignment is kept for when a job is due, so Rust and Go write at the same cadence and file count per window.
8. `run_compact`: level-1 jobs read from `local`; when that returns `CompactError::InputVanished`, the same job is run once more with `replica` as the source (the local copy may be gone after a restart; Go falls back the same way). Every other level reads from `replica`. The destination is always `replica`. Map `CompactError` exhaustively: `InputVanished { key }` (from the replica) is `Replan { key }`; `Source`, `Dest`, `Input` and `Encode` are `Failed`. After `Failed(Compact(Dest(_)))` the output may be visible; the next `due` on a fresh listing settles it, because a published output raises `dst_max`.
9. Compaction never deletes its inputs. A crash at any point leaves a replica that restores: the inputs are still there, and the output is either visible or absent.
10. `run_delete` calls `replica.delete(job.keys())` once: `Ok` is `Deleted { count }`, and an error is `Failed(Delete(err))`. A key already gone is not an error (store contract).
11. Go and Rust may take turns on one prefix (never at once). Go reads Rust's files: same names, levels, `NoChecksum` (D2) and the `litestream-timestamp` metadata; Go's next compaction starts from the destination max whatever wrote it, and its 32-capped outputs are just smaller files. Rust reads Go's: unbounded-fan-in outputs are ordinary files, and a hole Go merged across is a file like any other.

**Tests** (`crates/core/src/maintain/tests.rs`; inventories built with `Inventory::from_files`, no store, except the executor tests on `MemStore`):
- One named test per `ConfigError` and `JobError` variant. A test feeds Go's defaults through `new` and asserts each value with the Go file:line in its doc comment.
- `go_truncate` vectors: 30 s, 5 min, 1 h and 24 h agree with Unix-epoch truncation; for 7 days the Unix epoch itself truncates to `-259_200_000` (Monday 1969-12-29, since 0001-01-01 was a Monday); negative instants round toward the past; extremes saturate.
- Property, the planner harness. Generate a write history (syncs at random times, each one L0 file), run `due` in a loop with a model executor that applies jobs to the inventory without bytes (compact: add the output `FileInfo` with the last input's `created_at`; snapshot: add `1..=through` at level 9 recorded as created `now`), with `now` advancing randomly. Assert:
  - every compact job has 1 to 32 inputs, strictly contiguous, starting above the destination max, and its output never overlaps the destination;
  - the `CompactionInputs::new` error branch of semantics 4 is never taken;
  - `plan(Latest)` after every job reaches the newest L0 TXID;
  - idle: after the last write, once `due` returns `Wait`, no later `now` ever yields a compact or snapshot job, and `until` becomes `None`;
  - determinism: `due` on equal arguments returns equal values (this is the restart and second-process property);
  - at most one compaction per level per window, except backlog chunks whose last input is at or before the window start.
- Backlog: 100 contiguous L0 files, empty L1: exactly four L1 jobs in one window, 32, 32, 32 and 4 inputs.
- A hole in L0: L1 gets the same hole and no file spans it.
- A straddling source file is skipped, matching Go's seek.
- A Go-shaped inventory (L1 files of fan-in 500, a 24-hour snapshot) plans the same next job Go would at three chosen instants, each with its Go line cited.
- Executor, `MemStore`: a compact job publishes `output_key()` and `due` on the reloaded listing does not plan it again; level-1 input missing locally reads from the replica; missing from both is `Replan`; a commit failure after publish (`inject_failure_after_publish`) is `Failed` and the next `due` plans nothing for that level; running a job twice (a crash before the reload) leaves one file at the key; `run_delete` counts, tolerates gone keys and maps a store error to `Failed`.

---

### Task 5b: retention (branch `retention`)

**Files:** create `crates/core/src/maintain/retain.rs` and `crates/core/src/maintain/retain/tests.rs`; in `crates/core/src/maintain.rs` add the `held` field, `Hold`, and the call to retention at the end of `due`. Touch no other file.

**Consumes:** task 5a (`due`, `Due`, `DeleteJob`, `MAX_DELETE`, `Maintenance`, `L0Retention`, `Inventory::level`), `lsr_core::{plan, PlanOutcome, RestoreTarget, ContiguousChain, is_snapshot_start, Inventory}`.

**Go behavior this task reproduces, and where it departs** (each departure is a doc line on the rule):
- Snapshot retention (`db.go:3122-3185`): delete snapshots whose `CreatedAt` is before `now - retention`, keep the newest listed one even if old. The floor for the other levels is the max TXID of the snapshot just before the first retained one, so a restore that planned on that snapshot still finds its files (fix 2b34013 / upstream #1325, test `TestStore_EnforceSnapshotRetention_RetainsInFlightRestorePlanFiles`). The VFS twin (`compactor.go:238-290`) lacks that floor; follow the DB path.
- Levels 1 to top (`store.go:863-884`, `compactor.go:293-340`): delete files whose max TXID is below the floor, keeping the last listed file. Runs after every snapshot-level tick (`store.go:626-629`).
- Level 0 (`db.go:3188-3311`, every 15 s and after each L1 compaction, `db.go:3079-3096`): skipped when retention is 0 or L1 is empty; walk L0 ascending, stop at the first file created after `now - l0_retention`, delete files whose max is at or below L1's max, keep the newest L0 when the whole level was walked. Departure: Go checks only L1's max, not that L1 covers each file's range; this port requires real coverage. Departure: a zero `CreatedAt` in Go falls back to the local mtime and then counts as old; here it holds.
- Go on S3 lists `LastModified` for retention (`useMetadata=false`, `s3/replica_client.go:1430, 1473, 1756-1764`); this port uses `FileInfo.created_at`, the header timestamp. Upload time is never earlier, so Go keeps Rust's files at least as long as Rust does.

**Produces** (in `maintain.rs`, the rules in `retain.rs`):
- `Due::Wait { until: Option<Timestamp>, held: Option<Hold> }`: `held` names why deletions are refused right now; compaction and snapshots still run when held.
- `pub enum Hold { UnrecognizedNames { level: Level, count: usize }, MalformedSnapshot { key: FileKey }, ListingBehind { listed: Option<Txid>, published: Txid }, Hole { after: Txid, next: Option<Txid> }, LatestUnplannable { outcome: PlanOutcome }, NoCreatedAt { key: FileKey }, SnapshotCreatedAtBackwards { earlier: FileKey, later: FileKey }, SelfCheck { outcome: PlanOutcome } }`. `LatestUnplannable` and `SelfCheck` never carry `PlanOutcome::Plan`.
- No new public function: retention is reached through `due`. `retain.rs` exposes `pub(super) fn retention(inventory: &Inventory, now: Timestamp, config: &Maintenance, published: Option<Txid>) -> Retention` with `pub(super) enum Retention { Delete(DeleteJob), Wait { until: Option<Timestamp> }, Held(Hold) }`.

**Semantics:**
1. `due` calls retention only when no compaction or snapshot job is ready, so deletes are planned against a listing that already holds the newest coverage. One delete job per call, like every job.
2. Trust gate, checked first; the first failure is the `Hold` and no file is deleted:
   - `inventory.skipped(level) > 0` for any level: `UnrecognizedNames`. A name this code cannot parse may be a file that covers something.
   - any `malformed_snapshots()`: `MalformedSnapshot`.
   - `published` above `inventory.newest()`: `ListingBehind`. The listing misses a file this process knows it wrote, so it is stale or partial.
   - `plan(inventory, Latest)` not a `Plan` reaching `inventory.newest()`: `LatestUnplannable`.
   - a transaction after the newest snapshot's max, up to `newest()`, that no file of levels 0 to 8 covers: `Hole`. Holes further back do not hold: they are history already lost, the rules below never widen them, and holding on them would pin every delete forever after one Go-era loss.
   - any snapshot or L0 file with `created_at` at or below 0: `NoCreatedAt`.
   - snapshots ordered by max TXID whose `created_at` goes backwards: `SnapshotCreatedAtBackwards`. Rewritten or skewed created-at times make age meaningless.
   A listing is trustworthy when `Inventory::load` succeeded (every page read, no stall, no conflicting duplicate) and it passes this gate.
3. Anchor: `anchor = min(now, newest created_at in the inventory)`. Ages are measured from the anchor, never from `now` alone, so a clock that jumps ahead deletes nothing the replica's own newest created-at time would not. A clock behind makes files look young: nothing extra is deleted.
4. Snapshots (`is_snapshot_start`, ascending): the first retained index `i` is the first snapshot not created before `anchor - snapshot_retention`, or the last snapshot when all are older. The created-at times are monotone (gate), so the deleted snapshots are a prefix. `floor` is the max of snapshot `i - 1`; none when `i == 0`, and then levels 1 to 8 delete nothing.
5. Levels 1 to 8: delete the files whose max is below `floor`, never the level's last file. This is always a prefix of the level.
6. Level 0, only for `L0Retention::After(d)`: walk ascending; a file is deleted when it was created at or before `anchor - d` and its whole range lies inside the union of the ranges of level 1 to 8 files kept by rule 5. Stop at the first file that fails; never the last L0 file.
7. The candidate set is sorted by key and cut to the first `MAX_DELETE`. Key order puts levels 0 to 8 before the snapshots, so the floor's snapshot is still listed while the files under it go; the next call computes the same floor and continues. Once the snapshots go, rule 4 finds `i == 0` and nothing more is deleted.
8. Self-check before returning a job: `plan` over the inventory without those keys, target `Latest`, must reach the same TXID; otherwise `Held(SelfCheck)`. Unreachable if rules 4 to 6 are right; it exists so a bug there costs a hold, not data.
9. No candidates: `Wait { until }`, the earliest of the next snapshot aging out (when more than one exists) and the next L0 file aging out (when covered), both in anchor time. Once an idle database has converged (only the newest snapshot and each level's last file left, or nothing aged), `until` is `None`.
10. Faults and their guards:
    - Clock skew or a jumping clock: the anchor (rule 3).
    - A backend that rewrites mtime: created-at times rewritten later delete nothing early; created-at times rewritten earlier are caught by `SnapshotCreatedAtBackwards` or `NoCreatedAt` when they break the order; a uniform shift into the past that keeps the order is a non-goal (the store contract says a store returns the created-at time it was given).
    - An eventually consistent listing: a file missing from the listing is never deleted and covers nothing, so it only shrinks the delete set; a missing tail is `ListingBehind`. A listing that still shows a file already deleted (a ghost) could make L0 look covered: non-goal, stated as "the store lists after a delete consistently", which S3, GCS, Azure Blob, R2 and the file backend all do.
    - Another writer at the same time: non-goal (Litestream's single-writer rule). Taking turns with Go is supported: every rule reads only names, ranges and created-at times, which both write the same way.
    - A snapshot of bad pages: retention still trusts it (Go does too). Non-goal here; the fix is a verified-snapshot floor once task 4 can verify one.
11. A restore that races a delete loses a file and re-plans (`restore`'s `MAX_ATTEMPTS`); the floor of rule 4 keeps the previous snapshot's chain for exactly that window.

**Tests** (`retain/tests.rs`; the 5a planner harness, extended so the model executor applies delete jobs):
- Invariants, each a property over generated histories and maintenance runs, checked after every delete job:
  1. `plan(Latest)` reaches the same TXID as before the job.
  2. Every `t` from the oldest retained snapshot's max to `newest()` that `plan(Txid(t))` restored before still restores, or a kept file `g` at level 1 or above has `g.min <= t < g.max` and `plan(Txid(g.max))` restores (the documented loss to compaction granularity). Same for `AsOf` at each kept file's `created_at` plus 1 ms.
  3. For every kept snapshot `S`: the inventory after the job with every other snapshot removed plans `Latest` to `newest()` (so no delete names a file `S`'s chain needs).
  4. Every level's last file before the job is there after, and each level's deleted files are a prefix of it.
  5. Fail closed: each gate condition of semantics 2, injected into an otherwise deletable inventory, gives `Wait { held: Some(that Hold) }` and never a `Delete`. One named test per `Hold` variant; `SelfCheck` is reached through a test-only hook in `retain.rs` that breaks rule 6.
  6. Clock: for any history, `due` at `now = Timestamp::from_unix_millis(i64::MAX)` deletes exactly what it deletes at `now` equal to the newest created-at time; `now` far behind deletes nothing.
  7. Listing omission: removing any subset of files from the listing (with `published: None`) never deletes a key that the full listing would keep.
  8. Created-at times rewritten later by any monotone amount never delete a key the true created-at times would keep.
- Go's in-flight restore case, ported from `TestStore_EnforceSnapshotRetention_RetainsInFlightRestorePlanFiles`: the files after the previous snapshot survive.
- Go-shaped inventories: after Go's L0 rule deleted an L0 file that L1 did not cover (a hole behind the newest snapshot), the hole does not hold and invariants 1 to 4 hold; a hole after the newest snapshot holds.
- `L0Retention::Keep`: no L0 file is ever deleted.
- 2,500 deletable files: three delete jobs of 1,000, 1,000 and 500, in key order, the snapshots last.

---

### Tasks 6 and 7: the simulator and Go compatibility for written chains

Order and parallelism (each sub-task is one branch, gate-green alone):

| # | Branch | Needs landed | Runs beside |
|---|---|---|---|
| 6a | `wal-builder` | nothing | everything |
| 6d | `sim-sqlite` | 4a, 6a | 4b, 4c, 4d, 5a, 5b |
| 6b | `snapshot-writer` | 4b | 4c, 4d, 6d |
| 6e | `sim-harness` | 6d, 4b | 6b, 6c |
| 6c | `actor` | 4b, 4c, 4d, 5a, 5b, 6b | 6e, 7a |
| 7a | `compat-live-source` | 4b, 4c, 6a | 6c, 6e |
| 6f | `sim-runner` | 6c, 6e | 7b |
| 6g1 | `sim-snapshot-causes` | 6f | 6g2, 7b |
| 6g2 | `sim-faults` | 6f | 6g1, 7b |
| 7b | `compat-go-chains` | 6c, 7a | 6f, 6g1, 6g2 |

Critical path: 4b, 6b, 6c, 6f, 6g1/6g2. 6a can start today. 6d can start the day 4a lands, which removes the simulator's riskiest piece (the SQLite rules) from the critical path.

#### Rulings

- **S1:** the actor is its own core task (6c, `crates/core/src/actor.rs`), sans-IO over the same traits; phase 3's thread wraps it. The simulator owns no loop logic.
- **S2:** the oracle maps txid to commit from ground truth: a `Tap` store wrapper records every committed L0 file's `WalOrigin` and txid range; the model records (salts, end offset) per commit. No content matching.
- **S3:** a crash is the executor dropping the actor's future at its k-th `Pending`; `Yield` makes every store call one await point. MemStore is not changed.
- **S4:** the L9 snapshot writer is a 4b-adjacent core module (6b); the actor runs `Job::Snapshot` only when uploads are idle (`UploadPlan::Idle`), so a snapshot's max is at or below the published txid.
- **S5:** a retried fault never gives up in the actor; attempts saturate and the wait caps at `MAX_BACKOFF`. Stops come only from `Policy::Stop` and the shutdown deadline.
- **S6:** task 7's live SQLite source is test-only code in `crates/compat` on rusqlite (no FFI, no new dep). Phase 3's `lsr-sqlite` replaces it and must pass the same cases.
- **S7:** "acknowledged" (defined in 6e) is what Latest must keep, except across `SnapshotCause::LocalRewound`, where Latest follows the local branch and acknowledged commits survive by txid only. PLAN's wording is amended.
- **S8:** store faults are injected by a `Faults<S>` wrapper in the simulator, not by `MemStore`. `FakeWal` stays for unit tests; the simulator's SQLite is the rule-based `SimSqlite` (6d), checked against rusqlite.
- **S9:** `just check` runs 64 seeds of 300 steps; 6f measures the wall time and moves the number if the gate grows by more than 10 s.
- **S10:** `Fault::from_remote` and `Fault::from_staging` already tell the two stores apart (landed), so every `Fault` is reachable from a scenario.
- **PLAN amended with this block:** the actor is sequential until a measurement says otherwise (PLAN said a `JoinSet`); the simulator's VACUUM has two shapes (same page size, changed page size); the phase 2 exit says what "acknowledged" promises across a local rewind; the memory bound the simulator checks is one buffer at most one part.
- **`crates/compat/tests/compat/handover.rs` has two halves:** 4b's `go_would_continue` and 7b's live Go resume. Each task adds its half and leaves the other alone.

---

### Task 6a: the WAL builder as a testing export (branch `wal-builder`)

**Files:** create `crates/wal/src/testing.rs`; add `[features] testing = []` to `crates/wal/Cargo.toml` and `#[cfg(any(test, feature = "testing"))] pub mod testing;` to `crates/wal/src/lib.rs`. Rewrite `crates/wal/tests/wal/common.rs` to re-export it (the wal crate's own tests turn the feature on through a self dev-dependency, as core does). Touch no other file.

**Consumes:** `lsr_wal::{WalChecksum, WalHeader, HEADER_LEN, FRAME_HEADER_LEN}`, `lsr_ltx::{PageNo, PageSize}`.

**Produces:**
- `pub struct WalBuilder` (the current test `Wal`, renamed): `new(page_size: PageSize, big_endian: bool, salts: Salts) -> Self`, `frame_with(&mut self, pgno: PageNo, commit: u32, salts: Salts, data: &[u8])`, `frame(&mut self, pgno, commit, seed: u64)`, `tx(&mut self, pgnos: &[PageNo], db_size: u32, seed: u64)`, `frame_len() -> usize`, `bytes() -> &[u8]`, `into_bytes() -> Vec<u8>`.
- `pub fn page(seed: u64, pgno: PageNo, size: PageSize) -> Vec<u8>` (the deterministic page filler, splitmix64).

**Semantics:**
1. Byte-identical output to today's test builder: the move is a rename, not a rewrite. The wal tests that use it pass unchanged.
2. No `unwrap` or indexing even though it is test support: it ships under a feature other crates build, so the workspace lints apply.

**Tests:** the existing `crates/wal/tests/wal` suite, unchanged; one new case `builder_matches_sqlite_header_checksum` that decodes a built header with `WalHeader::decode`.

---

### Task 6b: the level-9 snapshot writer (branch `snapshot-writer`)

5a leaves `Job::Snapshot` to "task 4's loop", and no task 4 block writes a level-9 file. This block is that writer.

**Files:** create `crates/core/src/snapshot.rs` and `crates/core/src/snapshot/tests.rs`; export from `crates/core/src/lib.rs`. Touch no other file.

**Consumes:** 4b's `SyncedThrough`, `plan_pages` (the page-map-or-database choice of a snapshot step), `WalSource`, `PageSource`, `CommittedRead`, `ReadLockHold`; `ObjectStore`, `Upload`, `FileInfo`, `FileKey`, `Level`; `lsr_ltx::{Encoder, Header, Txid, TxidRange, Timestamp}`.

**Go behavior this task reproduces:** `db.Snapshot` (`db.go:3102-3117`) writes level 9, range `1..pos.TXID`, through `SnapshotReader`/`snapshotPosition` (`db.go:2877-2960`): every page from the database file, overlaid with WAL frames up to `lastSyncedWALOffset`, while holding `chkMu` so no checkpoint runs under it. Go's snapshot header carries no WAL origin.

**Produces:**
- `pub async fn write_snapshot<W: WalSource, P: PageSource, S: ObjectStore>(proof: &SyncedThrough, wal: &mut W, db: &mut P, remote: &S, now: Timestamp) -> Result<FileInfo, SnapshotFailed>`.
- `pub struct SnapshotFailed { pub fault: Fault, pub key: FileKey }` (thiserror).

**Semantics:**
1. The file is `FileKey { level: 9, range: 1..=proof.through().txid }`, header `min_txid = 1`, `pre_apply_checksum = 0`, no WAL origin, `timestamp = now` clamped to the proof's timestamp.
2. Page sources follow 4b's snapshot step exactly (one helper, not a copy): frames in `[32, synced]` of the current generation win, the database file supplies the rest, page count from the proof's last commit frame.
3. `SyncedThrough` is borrowed, not consumed: the checkpoint that follows needs it. Holding it is the proof that no checkpoint runs (the Go `chkMu` equivalent is the actor's single thread of control).
4. The read transaction pins the WAL at our mark, so another process can backfill frames at or below the mark into the database file while we read it. Those frames are also in the page map (synced is at or above the mark, which `SyncedThrough` guarantees), so the map wins and a page torn by a concurrent backfill is never read. A debug-free check: if `proof.through()` is below `epoch`'s mark the call returns `Fault::Bug`.
5. Streamed: pages are encoded as read, never buffered whole. Memory is one page plus the encoder's block.
6. Uploads through `ObjectStore::put`, so the caller's `Retrying` wrapper applies. Errors map by `Fault::from_remote` (the replica is the only store here), `from_source`, `from_read_page`.
7. No local copy is staged: level 9 lives only in the replica, as in Go.

**Tests:**
- property `snapshot_equals_model`: random histories on `FakeWal`/`FakeDb`, random backfill point at or below our mark; the written file restores to the model's image at `proof.through()`.
- property `snapshot_ignores_concurrent_backfill`: another process backfills up to the mark between pages (injected through `FakeDb`); the output does not change.
- cases `snapshot_header_has_no_wal_origin`, `snapshot_key_is_level_9_from_1`, `snapshot_mark_above_synced_is_bug`, `snapshot_store_error_maps_by_from_remote`.
- case `go_decoder_accepts_snapshot` lives in 7b (needs the Go helper).

---

### Task 6c: the actor (branch `actor`)

The sequential driver phase 3 reuses. Sans-IO: it owns no thread, no timer, no clock; it is told the time and returns when to be called again.

**Files:** create `crates/core/src/actor.rs`, `crates/core/src/actor/state.rs` (phases and transitions), `crates/core/src/actor/tests.rs`; export from `crates/core/src/lib.rs`. Touch no other file.

**Consumes:** `recover`, `Recovered`, `Step`, `sync_step`, `Synced`, `StepFailed`, `SyncConfig` (4b); `checkpoint`, `decide`, `after_sync`, `after_checkpoint`, `next_wake`, `CheckpointState`, `CheckpointConfig` (4c); `upload_next`, `plan` as `UploadPlan`, `UploadState`, `UploadConfig` (4d); `due`, `Job`, `run_compact`, `run_delete`, `Outcome`, `Maintenance` (5a, 5b); `write_snapshot` (6b); `Fault`, `policy`, `Policy`, `retry_wait`, `Next`, `Clamped`, `StopCause` (4a); `Inventory`, `Clock`, `Retrying`.

**Go behavior this task reproduces:** the monitor loop (`db.go:3322-3406`: sync, then the checkpoint decision), the replica loop (`replica.go:392-516`, backoff at `replica.go:440-457`: doubling to `DefaultSyncBackoffMax`, no give-up) and the per-level compaction ticker (`store.go:591-656`). Go runs these as goroutines; the actor runs them in one priority order, which is legal because each is idempotent against the inventory.

**Produces:**
- `pub struct Env<'a, W, P, C, L, R> { pub wal: &'a mut W, pub db: &'a mut P, pub ck: &'a mut C, pub staging: &'a L, pub remote: &'a R }` (`W: WalSource, P: PageSource, C: Checkpointer, L: ObjectStore, R: ObjectStore`).
- `pub struct ActorConfig { pub sync: SyncConfig, pub checkpoint: CheckpointConfig, pub upload: UploadConfig, pub maintenance: Maintenance, pub retry_interval: Duration, pub shutdown_grace: Duration }`.
- `pub struct Actor` (fields private): phase, position, `CheckpointState`, `UploadState`, per-task attempt counters, last timestamp, pending `SnapshotCause`.
- `pub enum Phase { Boot, Running, Draining { deadline: Timestamp }, Stopped(StopCause) }`.
- `impl Actor { pub fn new(cfg: ActorConfig) -> Self; pub fn phase(&self) -> Phase; pub fn request_snapshot(&mut self); pub fn begin_shutdown(&mut self, now: Timestamp); pub async fn tick<..>(&mut self, env: Env<'_, ..>, now: Timestamp) -> Tick }`.
- `pub enum Tick { Ran { event: Event, next: Next }, Stopped(StopCause), Drained }`.
- `pub enum Event { Booted { cause: Option<SnapshotCause> }, Synced { files: u32 }, Checkpointed { mode: CheckpointMode, busy: bool }, Uploaded(Did), Maintained(MaintainedEvent), Idle, Retrying { fault: Fault, attempt: NonZeroU32 }, Resnapshot(SnapshotCause), Clamped(Clamped) }` and `pub enum MaintainedEvent { Compacted(FileInfo), Snapshot(FileInfo), Deleted(usize), Replan, Waiting { held: Option<Hold> } }`.

**Semantics:**
1. Boot (phase `Boot`): list the remote once (`upload_next` from `UploadState::ListFirst`), then `NewestStagedFile::load`, then `recover(Some(PositionSource::Boot(..)), &view, remote_newest)`. The boot order is 4d's: list, recover with that remote, upload. Booting is a tick; a crash mid-boot boots again.
2. One unit of work per tick, chosen in this order, first ready wins:
   a. sync when the WAL's committed end moved or a `Step::Snapshot` is pending;
   b. checkpoint when `decide` says so, consuming the `SyncedThrough` from (a) in the same tick (the proof is not `Clone` and never stored across ticks);
   c. upload while `plan(state, newest_staged)` is not `Idle`, at most `files_per_tick`;
   d. maintenance: `due(Inventory::load(remote), now, cfg, published)` only when (c) is `Idle`. `Job` is matched exhaustively: `Compact` to `run_compact(job, staging, remote)`, `Snapshot` to `write_snapshot` with a fresh sync proof, `Delete` to `run_delete`.
   Sync before checkpoint is a safety rule (F1: no checkpoint without a proof). Upload before maintenance keeps a snapshot's max at or below `published`, so `due`'s `published` argument is truthful.
3. `next`: `Now` when any task has ready work; otherwise the earliest of `next_wake` (4c), `Due::Wait.until`, a pending retry; `OnWalChange` when all are `None`. The caller may wake early on a WAL change; an early tick is always safe.
4. Faults: every failure becomes a `Fault` and goes through `policy` once, here and nowhere else. `Retry(b)`: that task's attempt counter saturating-increments, `next = At(now + wait)` where wait is `b`'s `After` value or `retry_wait(attempt, retry_interval)`; other tasks keep running. A success resets the task. `Resnapshot(cause)`: the next sync is `Step::Snapshot { cause, .. }`. `Stop(c)`: phase `Stopped(c)`, every later tick returns `Stopped(c)` without I/O.
5. Time: `now` earlier than the last timestamp used is clamped and reported once as `Event::Clamped`; it is never a fault.
6. Shutdown: `begin_shutdown` sets `Draining { deadline: now + shutdown_grace }`. Draining runs only (a) and (c). `Drained` when `plan` is `Idle` and the WAL has nothing unsynced; `Stopped(Shutdown)` (`Fault::ShutdownDeadline`) at the deadline with work left. A future dropped by the caller at the deadline is a crash, and is safe by 6e's property `crash_anywhere_recovers`.
7. Idle: with no WAL change and nothing due, a tick does no I/O except none at all: zero list calls, zero puts, zero checkpoints. `Inventory::load` in (d) is called only when `due` could change its answer: after a maintenance job, or when the last `Wait.until` has passed. (As built: an upload brings `Wait.until` forward to the next compaction window or the level-0 retention check interval, whichever is first, rather than listing at once; see the As built of `docs/plans/phase-3-api.md`.)
8. `tick` holds no borrow of `Env` across ticks and no state outside `Actor`, so phase 3 can drop and rebuild the environment (reopen connections) between ticks.

**Tests** (FakeWal, FakeCheckpointer, FakeDb, MemStore; the simulator covers the cross product):
- cases `boot_lists_then_recovers_then_uploads`, `sync_precedes_checkpoint_same_tick`, `maintenance_waits_for_idle_uploads`, `snapshot_job_writes_level_9`, `retry_backs_off_per_task`, `throttle_retry_after_honored`, `stop_is_terminal`, `resnapshot_policy_forces_snapshot_step`, `clock_back_clamped_once`, `drain_finishes_uploads`, `drain_deadline_stops`.
- property `idle_tick_does_no_io`: after any history, once caught up, N ticks with no WAL change issue 0 store calls and 0 checkpoints (counted by a wrapper).
- property `policy_called_once_per_fault`: every injected `StoreError`/`SourceError` yields exactly one `Event::Retrying`, `Resnapshot` or `Stopped`.
- guard `job_match_is_exhaustive`: no `_ =>` arm on `Job`, `Fault`, `Policy` in `actor.rs` (source scan, as the existing exhaustiveness guards do).

---

### Task 6d: the SQLite model (branch `sim-sqlite`)

The 4a fakes are scripted: a test says what a checkpoint does. The simulator needs SQLite's rules, so another process's checkpoint, a writer's restart and a reader's mark interact as they do in 3.47.1.

**Files:** create the crate `crates/sim` (package `lsr-sim`, `publish = false`, used only as a dev-dependency): `Cargo.toml`, `src/lib.rs`, `src/sqlite.rs`, `src/sqlite/tests.rs`, `tests/sqlite_rules.rs` (the differential test). Add it to the workspace members. Touch no other file.

**Dependencies:** `lsr-core` (feature `testing`), `lsr-wal` (feature `testing`), `lsr-ltx`. Dev: `rusqlite` 0.43 bundled (MIT, already a dev-dependency of wal and compat), `proptest` (MIT/Apache-2.0, already in the tree), `tempfile` (MIT/Apache-2.0, already in the tree). No new crate enters the lock file.

**Measured facts it models** (SQLite 3.47.1, `walpin.py`, three cases; reproduced by `tests/sqlite_rules.rs`):
- R1, backfill limit: a checkpoint copies frames up to the smallest read mark above 0 held by any reader; a reader holding read mark 0 while `nBackfill < mxFrame` blocks all backfill (it reads only the database file, so overwriting it is unsafe). PASSIVE returns busy=0 with partial progress; RESTART and TRUNCATE return busy=1 when they could not finish.
- R2, restart: a writer restarts the WAL (salt1+1, salt2 new, checkpoint_seq+1, writes from frame 1) only when `nBackfill == mxFrame` and no reader holds a mark above 0. A read-mark-0 reader does not block it. RESTART and TRUNCATE also restart when they finish under the same condition; TRUNCATE then truncates the file to 0 bytes.
- R3, mark choice: a new read transaction takes read mark 0 when `nBackfill == mxFrame`, else mark `mxFrame`.
- Consequences the model must show: pinned above 0, every checkpoint mode is busy and no restart happens; pinned on 0, exactly one restart (by the next writer), and none after while the read lock holds, because the new generation's frames are not backfilled past the read lock; a read-mark-0 reader plus a mark>0 reader lets the old generation grow, and after the second ends no restart happens until the first ends.

**Produces:**
- `pub struct SimSqlite` (cheap clone, shared state): WAL bytes (built with `WalBuilder`'s checksum rules), wal-index (`mxFrame`, `nBackfill`, salts, header copies), database file pages, readers `BTreeMap<ReaderId, u32>`, `durable_frames: u32` (frames fsynced: set at each checkpoint, `synchronous=NORMAL`), the epoch counter.
- Handles: `fn source(&self) -> SimSource` (`WalSource`), `fn pages(&self) -> SimPages` (`PageSource`), `fn checkpointer(&self) -> SimCheckpointer` (`Checkpointer`). Our own connection is one reader id; its read tx follows R3.
- App operations, each a method returning what it changed: `commit(pages, db_size) -> AppCommit { salts, end: WalOffset }`, `spill(pages)` (uncommitted frames), `rollback()` (drops spilled frames past `mxFrame`), `shrink(db_size)`, `rewrite_page_size(new)` (VACUUM to a new page size: full backfill, truncate, new header), `other_process_reader_begin() -> ReaderId`, `other_process_reader_end(id)`, `other_process_checkpoint(mode) -> (busy, log, ckpt)`.
- Environment faults: `delete_wal()`, `tear_wal_header()`, `truncate_wal_at(bytes)`, `flip_bit(offset)`, `rewrite_same_length(seed)`, `regrow_same_salts(seed)`, `shm_mid_update()`, `shm_end_inside_tx()`, `overwrite_frame_between_scan_and_read(frame)`, `unlink_wal()`, `inject(SourceOp, SourceError)`.
- Crash: `fn crash(&self, kind: CrashKind)`, `pub enum CrashKind { Process, Power }`. Process: our readers and locks vanish, files and wal-index stay. Power: as Process, then WAL frames past `durable_frames` are dropped, the wal-index is rebuilt from the file (SQLite's recovery), database pages written by a checkpoint since the last fsync are kept (checkpoints fsync the database before `nBackfill` moves).

**Semantics:**
1. `SimCheckpointer::checkpoint` releases our read tx, applies R1 and R2 against every reader, retakes our read tx under R3, bumps the epoch, and returns SQLite's three numbers. No script: what happens follows from the readers.
2. `bump_seq` is a commit of page 1 by our connection (`db.go:1668`), going through R2 like any write.
3. The epoch increments every time our read tx ends; `WalSource::epoch` reports it, so `Continuity::Held` holds exactly while the read tx is unbroken.
4. Every method is total: an operation that is impossible in the current state (rollback with nothing spilled) is a no-op returning `false`, so the generator never needs to pre-check.
5. Determinism: no clock, no randomness inside; byte content comes from the caller's seed.

**Tests:**
- differential `rules_match_sqlite` (proptest, 64 cases, real files in a tempdir): random sequences over {commit, other process reader begin/end, our reader begin/end, PASSIVE, RESTART, TRUNCATE}, applied to both rusqlite (`wal_autocheckpoint=0`, `timeout=0`) and `SimSqlite`; after each op the WAL header (salts, seq), file length, the three checkpoint numbers and busy agree.
- cases `pinned_above_zero_blocks_all_modes`, `pinned_on_zero_restarts_once`, `two_readers_grow_old_generation` (the three `walpin.py` cases, asserted on both).
- property `power_crash_keeps_checkpointed_pages`: after any history, a Power crash keeps every page as of the last checkpoint and some prefix of later commits, never a partial transaction.
- property `sources_pass_conformance`: `SimSource`, `SimPages`, `SimCheckpointer` pass 4a's trait conformance suite.

---

### Task 6e: the harness (branch `sim-harness`)

**Files:** in `crates/sim`: create `src/store.rs` (wrappers), `src/exec.rs` (executor and crash points), `src/model.rs`, `src/oracle.rs`, `src/seed.rs` and their `tests.rs` files. Touch no other crate.

**Consumes:** `MemStore`, `ObjectStore`, `Upload`, `StoreError`, `StoreOp`, `Inventory`, `restore`, `MemSink`, `RestoreTarget`, `PlanOutcome`, `SeededEntropy`, `FakeClock`, `RecordingSleeper`, `lsr_ltx::Decoder`; `SimSqlite` (6d).

**Produces:**
- Store wrappers, each `impl<S: ObjectStore> ObjectStore`, stacked `Faults<Yield<Tap<Stage<MemStore>>>>` for staging and `Faults<Yield<Tap<MemStore>>>` for the remote:
  - `Faults<S>`: a seeded schedule `Vec<(StoreOp, nth: u32, FaultShape)>` with `pub enum FaultShape { Error(StoreError), ErrorAfterPublish(StoreError), SizeMismatch, FlipByteOnRead { at: u64 }, Rate { per_mille: u16, err: StoreError } }`. Faults live here, not in `MemStore`, so the raw store under them is clean for the oracle's restores.
  - `Yield<S>`: every call returns `Pending` once before forwarding (wakes itself), giving each store call an await point.
  - `Tap<S>`: records `TapEvent { key, header: Header, origin: Option<WalOrigin>, result: Ok | ErrAfterPublish | Err }` for every committed upload, and every delete; read by the oracle, never by the code under test.
  - `Stage<S>`: power-loss bookkeeping for the staging store. A commit that returns `ErrorAfterPublish` models "rename done, fsync or directory fsync failed": its key is visible and volatile. A commit failing before publish models "file fsync failed": nothing visible. `fn power_loss(&self, rng)` drops each volatile key, or truncates it to a seeded prefix; `fn fsync_all(&self)` clears the volatile set (a later successful commit in the same directory, as a directory fsync covers earlier renames).
- Executor: `pub fn run_until(fut: impl Future, budget: Polls) -> RunEnd`, `pub enum RunEnd<T> { Done(T), Crashed { polls: u32 } }`. Polls with a no-op waker; at the k-th `Pending` it drops the future. Dropping is the crash: every borrow, read tx handle and in-flight `MemUpload` dies with it.
- Model: `pub struct Model`: `commits: Vec<CommitRec { id: CommitId, image: Rc<PageImage>, salts: Salts, end: WalOffset, durable: bool }>`, built from `AppCommit` results; images are persistent maps (each commit shares untouched pages with its parent through `Rc`, so memory is pages written, not pages times commits). `fn on_power_loss(&mut self, kept_frames)` marks the local branch rewound and starts a new branch from the last durable commit.
- Oracle: `pub fn check(model: &Model, tap: &[TapEvent], remote: &MemStore, acked: &Acked) -> Result<Checked, Violation>`, `pub enum Violation { CoversMidTx { key }, TxidReused { txid, first, second }, CoversNotIncreasing { .. }, RestoreMismatch { target: RestoreTarget, first_diff: PageNo }, RestoreRefused { target, outcome: PlanOutcome }, LatestLostAck { acked: Txid, latest: Option<Txid> }, ResourceBound { what: Bound, seen: u64, limit: u64 }, BugFault { context } }`. The only hard asserts in the workspace outside tests live here, as `Violation` values the runner turns into a failure.
- Seeds: `pub struct Seed(u64)`, `Display` as 16 hex digits, `FromStr`; `pub fn replay_line(seed, steps: Option<u32>) -> String` giving `just sim-replay <seed> [steps]`.

**Semantics:**
1. Covers. Each L0 file in the tap with a WAL origin `(salts, offset, size)` covers the model commit whose (salts, end) equals `(salts, offset + size)`; `c(N)` is that commit for the file's max txid N. A file whose end matches no commit is `CoversMidTx`. Two files with the same txid and different covered commits (a reused txid) is `TxidReused`, unless the second is after a `LocalRewound` snapshot that legitimately re-issues it (Go would too; see forks). `c` strictly increases in txid except across a `LocalRewound` snapshot. Snapshot files (no origin) cover the commit of the sync proof that produced them, recorded by the actor's `Event` stream.
2. O1, every txid inside retention: let `W` be the min txid of the oldest snapshot still listed. For every t in `[W, newest]`, either `restore(Txid(t))` equals `image[c(t)]`, or t lies strictly inside a kept compacted file g (`g.min <= t < g.max`), in which case the plan refuses t with the documented outcome and `restore(Txid(g.max))` equals `image[c(g.max)]`. Restores run against the raw `MemStore` under the wrappers (no faults, no yields).
3. O2, latest: `restore(Latest)` returns a txid at or above the highest acknowledged txid, and its content equals `image[c(latest)]`.
4. Acknowledged: the actor's `UploadState` reached `At(t)` through a `Did::Put` whose commit returned `Ok`, or a `Did::Listed { newest }` that found t. An `ErrorAfterPublish` commit is not acknowledged until a later listing finds it.
5. What a crash may lose, and nothing else:
   - Process crash: nothing committed to the remote; staged files not yet uploaded survive (the next boot uploads them).
   - Power crash: the WAL tail past the last checkpoint fsync (the app's own loss under `synchronous=NORMAL`), volatile staged keys (dropped or torn; a torn one must surface as `NewestStagedFileUnreadable` or `StagedCorrupt`, never be uploaded).
   - Meta directory wiped: every unuploaded commit; the next boot is `BehindRemote` or `FirstSync`.
   - Never: an acknowledged txid, except that after `LocalRewound` the txid survives but Latest's content follows the local branch.
6. Crash points: the runner picks a crash poll k uniformly in `[0, polls_of_this_tick)`, measured by one dry run of the same seed prefix; crash kind is Process 3 in 4, Power 1 in 4. After a crash the next tick builds a fresh `Actor` (boot) from what the stores and `SimSqlite` hold.
7. `Fault::Bug` reaching the actor's events is `BugFault`: the code under test never has a reason to emit it.

**Tests:** `yield_makes_every_store_call_a_crash_point`, `faults_wrapper_leaves_raw_store_clean`, `stage_power_loss_drops_or_tears_only_volatile`, `err_before_publish_is_invisible`, `tap_records_err_after_publish`, `oracle_catches_planted_mid_tx_file`, `oracle_catches_planted_wrong_page`, `oracle_catches_lost_ack`, `oracle_accepts_compacted_interior_refusal`, `seed_display_round_trips`. Each planted-defect case proves a `Violation` variant can fire (a mutation check on the oracle itself).

---

### Task 6f: the runner, resource properties and recipes (branch `sim-runner`)

**Files:** in `crates/sim`: `src/gen.rs` (history generator), `src/run.rs` (the loop), `src/resources.rs`, `tests/fixed_seeds.rs`, `tests/replay.rs`, `tests/soak.rs`. Add recipes `sim-replay` and `sim-soak` to the `justfile`. Touch no other file.

**Consumes:** `Actor`, `ActorConfig`, `Tick`, `Event` (6c); everything in 6d and 6e.

**Produces:**
- `pub enum Op { Commit { pages: u8, grow: bool }, Spill, Rollback, Vacuum { page_size: Option<PageSize> }, OtherProcessReader { begin: bool }, OtherProcessCheckpoint(CheckpointMode), Crash(CrashKind), StoreFault(Target, FaultShape), EnvFault(EnvFault), ClockJump(i64), Tick, Shutdown, RequestSnapshot, Restore(RestoreTarget) }` and `pub fn history(seed: Seed, steps: u32, weights: &Weights) -> Vec<Op>`. `Weights::default()` favours `Tick` and `Commit`; faults are rare so histories make progress.
- `pub fn run(seed: Seed, steps: u32, weights: &Weights) -> Result<RunReport, Failure>`; `pub struct Failure { seed: Seed, step: u32, violation: Violation, replay: String }`, whose `Display` starts with the replay line.
- `pub fn shrink(seed, steps) -> u32`: the smallest step count of the same seed that still fails (binary search on the prefix; histories are prefix-stable by construction).

**Semantics:**
1. The loop applies ops in order; `Tick` runs `Actor::tick` through the executor with the crash point chosen by 6e rule 6 when the history says so, and sets `FakeClock` to the `next` it returned (or one second later for `OnWalChange`). Clock jumps move `FakeClock` either way.
2. The oracle runs after every `Restore` op and at the end of every history, after a final drain (crash-free ticks until `Drained` or 10,000 ticks, the latter a `ResourceBound` violation).
3. Resource properties, checked on counters the wrappers keep:
   - idle: 100 ticks with no app op after a drain write 0 LTX files, run 0 checkpoints, issue 0 list calls;
   - at most 1 PASSIVE per tick;
   - bytes staged per tick at most `SyncConfig.cap` plus one transaction (the cap is checked at transaction boundaries);
   - WAL length bounded when no other process reader holds a mark: below `truncate_threshold` pages plus one tick's commits;
   - list calls: only at boot, after an `UploadAmbiguous`/`RemoteObjectMissing`, and in maintenance when `due` can change;
   - memory: the largest live `MemUpload` buffer is at most one part (the in-memory store holds whole objects, so this checks the writer streams; the parts-times-concurrency bound is phase 3's, against a real store).
4. Failure output: the panic message's first line is `seed 0x<16 hex> failed at step <k>: <violation>`, the second `replay: just sim-replay 0x<seed> <shrunk k>`.
5. Budgets. Fixed set: 64 seeds in `tests/fixed_seeds.rs`, 300 steps each, split across 8 test functions of 8 seeds so nextest runs them in parallel; target under 4 s per function in the dev profile, about 30 s of CPU, under 5 s wall on 8 cores. The budget is asserted: a function that takes over 10 s fails with "simulator budget exceeded", so a slowdown is a red gate, not a creeping one. Rotating set: `just sim-soak <seconds>` runs `tests/soak.rs` (ignored by default) with `LSR_SIM_BASE_SEED` from the recipe (`date +%s` unless given) and `LSR_SIM_SECONDS`, printing every seed's line before running it. `just sim-replay <seed> [steps]` runs `tests/replay.rs` with `LSR_SIM_SEED`/`LSR_SIM_STEPS` and prints every op.
6. A failing soak seed is added to the fixed set by the change that fixes it (a one-line table entry with the issue number).

**Tests:** the fixed set; `history_is_prefix_stable`; `shrink_finds_planted_step` (an oracle mutation planted at step 117 shrinks to 117); `failure_prints_replay_line`; `idle_resource_property` on its own as a fast case.

**As built:** `generate.rs` (the plan's `gen` is a reserved word), `run.rs`, `resources.rs`, `tests/{fixed_seeds,replay,soak}.rs`; `just sim-replay <seed> [steps]` and `just sim-soak <seconds> [base]`. Differences from the plan:
- The harness is the driver: `World::start_driver` runs at the start of a run and on every reboot (`ensureWALExists` at `db.go:1673`, then the long read lock). Without it another process checkpoint plus a restart lost unsynced frames. `SimSqlite::wal_synced` gates what another process checkpoint marks durable, and `retire_generation` marks a retired generation's commits durable when the WAL restarts.
- The model has a genesis commit 0, learns the engine's own bump commit (`sync_model_at`), and keeps `StageNote`s for staged positions that fit two branches. `CoversNotIncreasing` is non-decreasing: a requested snapshot is a new id for the same commit. `TapResult` is gone.
- Generated faults are retry-class only (tampering is 6g2's). A stop other than a drain is a finding. 
- Idle ticks that ask to run again at once (`spins`) advance the clock by `SPIN_STEP` (50 ms); the settle loop ignores them. The app lets go (rolls back, closes readers) before the idle bounds, and the WAL-length bound is waived when it had held a spill or reader.
- Gap 4 (staged L0 never pruned) is fixed on main by 6h W3: every idle-files seed passes, no seed list. The final-phase idle checks wait for a tick that is Idle and waits on a WAL change alone (a timer-driven checkpoint is work, not idleness), and an Idle tick may list staging once (the prune scan). A failure in the final phase replays with the whole history length (`Failure::steps`).
- Finding 1 (a resizing vacuum stopped the engine with `RemoteCorrupt`) no longer reproduces on main, so `Weights::vacuum_resizes` is on. Finding 2 stays as the ignored `review_torn_staged_file` (Task 6i): a power loss tears an unuploaded staged L0 file, boot resnapshots one txid higher, and the upload task stops with `StagedCorrupt` (`just sim-replay 0x25 44`, `0x2d 256`, `0x46 252`). Seeds 37 and 45 are swapped for 66 and 65 in the fixed set.
- Not built: the big-database memory case (`resources` covers the bound on counters only).

---

### Task 6g1: one scenario per SnapshotCause (branch `sim-snapshot-causes`)

**Files:** `crates/sim/tests/sim/snapshot_causes.rs`, `crates/sim/src/scenario.rs` (`Scenario`: a fixed op list plus expected events). Touch no other file.

**Produces:** `pub struct Scenario { pub name: &'static str, pub ops: Vec<Op>, pub expect: Vec<Expect> }`, `pub enum Expect { Event(EventMatch), Stop(StopCause), Oracle }`; `pub fn scenario_of(cause: SnapshotCause) -> &'static [fn() -> Scenario]`, an exhaustive match with no `_` arm: a new cause does not compile without a scenario.

**Semantics:** each scenario runs through the same `run` as random histories (oracle and resource properties included), then asserts its expected events in order. Table (from task4-forks section C, `LocalLostReplicatedTx` replaced):

| Cause | Scenarios |
|---|---|
| FirstSync | `empty_meta_boot` |
| BehindRemote | `meta_wiped_remote_kept`, `restored_old_meta` |
| NewestStagedFileUnreadable | `newest_staged_file_byte_flip`, `newest_staged_file_truncated` (Power crash tears a volatile staged key), `newest_staged_file_no_wal_origin` |
| WalMissing | `wal_deleted_while_down`, `wal_header_torn`, `restored_with_no_wal`, `restart_between_steps` |
| PageSizeChanged | `vacuum_page_size_change` |
| GenerationChanged | `app_checkpoint_while_down`, `restart_during_read_lock_lapse` |
| PrevFrameMismatch | `wal_rewritten_same_length`, `cursor_mid_tx` |
| LastPageMismatch | `wal_regrown_same_salts_diff_bytes` |
| CheckpointBoundary | `truncate_threshold_crossed` |
| OperatorRequested | `cli_snapshot` (`Op::RequestSnapshot`) |
| LocalRewound | `power_loss_wal_rewind`: commits acknowledged, Power crash drops the WAL tail, boot snapshots; O1 holds for old txids, O2's content follows the local branch |

Plus `pinned_on_zero_single_restart`: our read tx on read mark 0, one app write restarts the WAL; the next step reports `GenerationChanged` without `ReadLockViolated` firing (the measured R2 case), and `read_lock_violated_abandoned_frames`: a restart forced while pinned above 0 (impossible in SQLite, done with `FakeWal::force_restart`) fires the abandoned-frames check, not a silent snapshot.

**Tests:** every table row; `every_cause_has_a_scenario` iterates a `const ALL: [SnapshotCause; N]` (itself pinned by an exhaustive match) and requires a non-empty slice.

---

### Task 6g2: one scenario per Fault (branch `sim-faults`)

**Files:** `crates/sim/tests/sim/faults.rs`; add `pub fn fault_scenarios(fault: FaultKind) -> &'static [fn() -> Scenario]` to `crates/sim/src/scenario.rs`. If 6g1 has not landed, this branch rebases on it; the file is shared but the two functions are disjoint.

**Semantics:** `FaultKind` is `Fault` without payloads, with an exhaustive `From<&Fault>`. Each scenario asserts the policy outcome (one `Retrying`, `Resnapshot` or `Stopped`) and the oracle afterwards. Table:

| Fault | Scenarios |
|---|---|
| StoreTransient | `store_flaky_5pct` |
| StoreThrottled | `store_429_retry_after` (wait honoured exactly, on `RecordingSleeper`) |
| StoreAuth | `creds_revoked_mid_run` (stop, no further calls) |
| RemoteObjectMissing | `remote_file_deleted_externally` |
| UploadAmbiguous | `commit_fails_after_publish`, `size_mismatch_on_commit` |
| RemoteCorrupt | `remote_bitrot_on_list_read` |
| RemoteAheadOfLocal | `second_writer_mid_run` (`MemStore::plant_unrecognized`) |
| RemoteRegressed | `bucket_rolled_back` |
| StagedCorrupt | `staged_byte_flip_before_upload` |
| StagedLost | `meta_dir_deleted_mid_run` |
| LocalDiskFull | `disk_full_on_staging` |
| LocalIo | `eio_on_wal_read` |
| LocalFsyncFailed | `fsync_fails_once` (reachable through `Fault::from_staging`) |
| SqliteBusy | `busy_on_barrier`, `shm_header_mid_update` |
| ReadersHeld | `readers_hold_the_wal` |
| WalMoved | `wal_header_torn_while_running` |
| WalIndexInconsistent | `shm_end_inside_tx` |
| WalChangedUnderRead | `frame_overwritten_between_scan_and_read` |
| NothingToSnapshot | `db_created_no_pages` |
| ReadTxLost | `reacquire_fails_after_checkpoint` |
| PageSizeChanged | covered by 6g1 `vacuum_page_size_change` (a fault whose policy is a resnapshot) |
| WalCorruptBelowCommitted | `bit_flip_in_committed_frame` |
| WalTruncatedBelowCommitted | `wal_truncated_externally_under_lock` |
| WalUnlinked | `wal_unlinked_mid_run` |
| ShutdownDeadline | `sigterm_during_upload` |
| Bug | none: the oracle's `BugFault` violation asserts it never fires over the fixed set |

Plus `clock_steps_back_10s` (`Clamped`: one event, no fault, no retry).

**Tests:** every row; `every_fault_has_a_scenario_or_a_reason` over `FaultKind::ALL`, where `Bug` and `PageSizeChanged` return a named reason instead of an empty slice.

---

### Task 7a: a live SQLite source and the outside-checkpoint cases (branch `compat-live-source`)

**Files:** create `crates/compat/src/live_source.rs` (test support, behind the crate's existing test-only use), `crates/compat/tests/compat/live_source.rs`. Export from `crates/compat/src/lib.rs`. Touch no other file. No Go binary; runs in `just check`.

**Dependencies:** none new (rusqlite 0.43 bundled is already a compat dependency; `lsr-wal` feature `testing` for the shm decode helpers only if 6a exports them).

**Produces:** `pub struct LiveSqlite` over a database path: two rusqlite connections (reader, writer-for-barrier), implementing `WalSource`, `PageSource`, `Checkpointer`.

**Semantics:**
1. Read tx: `BEGIN; SELECT COUNT(1) FROM _litestream_seq` (Go `db.go:1288`), creating `_litestream_seq` and `_litestream_lock` first (`db.go:1174`, `1180`). The epoch increments whenever that tx ends.
2. Committed end: the `-shm` header read with `std::fs`: two 48-byte copies at 0 and 48 must be equal (else `SqliteBusy`, retried, as SQLite readers do), `mxFrame` at byte 16, salts at 32, `nBackfill` at 96. Native byte order, as SQLite writes it.
3. `checkpoint(mode)` runs `PRAGMA wal_checkpoint(<mode>)` on the reader connection after ending its read tx, returns busy/log/ckpt, retakes the read tx. `bump_seq` is `db.go:1668`'s statement. The barrier is `BEGIN IMMEDIATE` on `_litestream_lock` on the second connection.
4. Pages are read from the database file with `std::fs` at `(pgno - 1) * page_size`, checked arithmetic.
5. It is test support: phase 3's `lsr-sqlite` replaces it and must pass `tests/live_source.rs` unchanged.

**Tests** (real SQLite, tempdir, each under a second):
- `outside_restart_against_read_lock_above_zero`: we hold a read tx above read mark 0; a second app connection writes, then runs `PRAGMA wal_checkpoint(RESTART)`: busy=1, the WAL header's salts and seq unchanged; our next `recover` is `Continue` and the next step is incremental.
- `outside_truncate_against_read_lock_above_zero`: as above with TRUNCATE; the file is not truncated.
- `read_lock_on_zero_one_restart`: our read tx taken when the WAL is fully backfilled (read mark 0); one app write restarts the WAL; our next step reports `GenerationChanged`, no `ReadLockViolated`; a second app write does not restart again.
- `live_matches_sim`: a seeded op list applied to `LiveSqlite` and to `SimSqlite` (6d, as a dev-dependency); after each sync the staged LTX files are byte-equal. As built: `crates/compat/tests/compat/live_matches_sim.rs`, a proptest plus named cases; salts drawn at random by the model go through `SimSqlite::force_random_salts`, and the model's commits mirror the real WAL's pages.
- `live_sync_chain_restores`: 200 rusqlite commits of random size through `sync_step` and `checkpoint`; `restore` at every txid equals `hash_pages` of a copy taken at that commit; `integrity_check` passes.

---

### Task 7b: Go reads and resumes Rust chains (branch `compat-go-chains`)

**Files:** create `crates/compat/tests/compat/rust_chains.rs` and `crates/compat/tests/compat/handover.rs` (4b planned the file with `go_would_continue`; this adds the live case). Every test `#[ignore = "runs the pinned Go binary; just compat"]`. Touch no other file.

**Consumes:** `Actor` (6c), `LiveSqlite` (7a), `FsStore`, `Workload`, `hash_pages`, `integrity_check`, `go_binary`, `GoVersion::ALL`, `write_config`, `restore_with_go`, `build_verify_helper`, `go_verify`, `rfc3339_millis`.

**Semantics:**
1. Chain writer: the actor over `LiveSqlite`, staging `FsStore` at `<meta>/ltx/0` (F4, Go's `db.go:600` layout), remote `FsStore` at the replica path, the real clock. A workload drives 300 commits with page-count growth, a shrink, a VACUUM, and outside PASSIVE/RESTART/TRUNCATE between ticks, and records `hash_pages` and the wall time after every commit. Maintenance runs with intervals small enough (1 s, 2 s, snapshot 3 s) to produce L1, L2 and L9 files within the run.
2. Versions: v0.5.16, v0.5.18 and Head read Rust chains. v0.5.0 and v0.5.15 are listed and expected to refuse (they predate the layout); the test records the refusal rather than skipping, so a change in that answer is visible.

**Tests:**
- `go_verifies_every_rust_file`: `go_verify` (Go `Decoder.Verify`) over every L0, compacted and L9 file the run wrote.
- `go_restores_rust_chain_latest`, `go_restores_rust_chain_each_txid` (every txid inside retention, interior-of-compacted txids at their file's max), `go_restores_rust_chain_as_of` (each recorded time, `rfc3339_millis`): page hashes equal the recorded ones, `integrity_check` passes, for each reading version.
- `go_resumes_from_rust_meta` (D4): the actor syncs and uploads, drains and stops; Go `replicate` starts on the same database, meta directory and replica (config from `write_config`); the app commits three more transactions; Go's next L0 file is `newest_staged + 1`, its header has a WAL origin with offset above 32 (incremental, not a snapshot) and no new L9 file appears before the snapshot interval; then Go restore at Latest equals the database. Run for v0.5.18 and Head.
- `go_resumes_after_rust_checkpoint`: as above with a Rust TRUNCATE checkpoint as the last action before stopping; Go's next file is still `newest_staged + 1` and incremental from the new generation.
- `rust_resumes_from_go_meta` is phase 3's (needs the CLI); listed here so it is not forgotten.

**As built:** the writer is `lsr_compat::{AppFixture, Chain}` (`crates/compat/src/chain.rs`), shared by `tests/rust_chains.rs` and `tests/handover.rs` (module `live_go`); `Replicator` and `wait_until` moved to `replicator.rs` and are public; `write_config_snapshotting` adds a snapshot interval to `write_config`. The clock is virtual (100 ms per commit, ending a second before real now), not the real one, so the 1 s, 2 s and 3 s windows trigger without sleeping; maintenance is levels 1 s and 2 s, snapshot 3 s, L0 retention 2 s. Go resumes are run for v0.5.16, v0.5.18 and Head. The Rust TRUNCATE needs an application reader to make a checkpoint report busy first (the actor truncates only after that), so `grow_until_truncate` holds one. Added: a Go-free smoke test (`rust_restores_its_own_chain_at_every_point`, in `just check`), `rust_chain_covers_every_shape`, `go_compacts_and_prunes_a_rust_replica` and, ahead of phase 3, an in-process `rust_resumes_from_go_meta` (the actor booted from Go's meta: `Booted { cause: None }`, one chain). v0.5.0 and v0.5.15 refuse Rust chains (block-encoded files). Not covered: a database crossing the lock page (about 1 GiB per run).


---

### Task 5c: review fixes for retention, maintenance and upload (branch `maintain-fixes`)

A blind review found six defects; four have failing tests on branch `review-maintain` (`7dd7866`, names start `review_`). Rulings:

- **R1, what confirms an upload (replaces the size rule in "4d, what confirms an upload"):** a listing confirms a key newer than `confirmed` only by content. The listed object is read back and compared with the staged file, streamed in chunks (sizes first, then bytes); equal confirms, anything else means upload again over the same key. With no staged counterpart the object must decode and verify as LTX naming that key; if it does not, the fault is `RemoteCorrupt`. This runs at boot and after an ambiguous commit only, so steady state still makes no list and no read call. After a commit the store rejected (`UploadChecksumMismatch`), the uploader also deletes the key, best effort, so readers do not see the bad newest staged file while the retry is pending.
- **R2, a compaction output the store rejected:** when `run_compact`'s commit fails with a corrupt-destination error (size or checksum mismatch), the executor deletes the output key. If that delete fails, the key is remembered as distrusted in the maintenance state: `due` plans its delete before anything else at that level, and retention holds (`Hold::OutputDistrusted`) until it is gone. A distrusted key never counts as cover.
- **R3, one failing job must not starve the rest:** a failed job's level is skipped until its retry time (doubling, capped at `MAX_BACKOFF`); `due` returns the next ready job at another level. A level-1 compaction whose local input fails to decode falls back to the replica's copy of that input, as it already does when the file is missing.
- **R4, the file store checks before it publishes:** `lsr-store-fs` compares the temp file's size with the bytes written after fsync and before the rename; a mismatch removes the temp file and returns `CommitSizeMismatch { removed: true }`. The check after the rename stays.
- **R5, the two low findings:** write the test for each first. Retention's anchor is not remembered (the first form of this ruling kept the largest anchor seen; withdrawn, because one clock reading far ahead would then keep retention deleting after the clock came back; the rule is not monotone in the anchor, so "a lower anchor deletes less" is not a property: a level-0 file straddling the high anchor's floor stays, and at a lower anchor the level 1 file below that floor is kept, covers it, and the file goes. What holds: every job is restore-safe for the range its anchor promises, any subset of a job is as safe as the whole, and the promised range only shrinks as the anchor rises). A clock that steps back may keep a snapshot whose level-0 chain an earlier job cut; a restore goes through the newer snapshot. One uncovered L0 file must not block deleting L0 files above it that are covered. If either fix needs more than the retention module and its state, document the limit in the module doc and say so in the report.
- **Open, not in this task:** a crash between a corrupt publish and its delete loses the in-memory distrust. For L0 the read-back in R1 closes it. For levels 1 to 9 the candidates are a read-back of the newest file per level at boot (cheap for levels 1 to 8, a whole-snapshot read for level 9) or treating a multipart commit as verified before publish. Task 6g2 gets the scenario; the choice waits for it.

**Files:** `crates/core/src/upload.rs`, `crates/core/src/maintain.rs`, `crates/core/src/maintain/retain.rs`, their tests, `crates/store-fs/src/` (R4 only). Cherry-pick `7dd7866` first; every `review_` test passes at the end and none is weakened.

**Tests added beyond the review's:** `a_listing_confirms_by_content_not_size` (same size, one byte differs: uploaded again), `a_listed_key_with_no_staged_file_must_decode`, `steady_state_makes_no_read_call`, `a_rejected_commit_deletes_the_key`, `a_distrusted_output_holds_retention_until_deleted`, `a_failing_level_backs_off_and_others_run`, `fs_commit_size_mismatch_is_caught_before_rename`, and one test per R5 fix.


### Task 4d: rulings on the blind review of sync, recover, checkpoint and snapshot

A blind review of 4b, 4c and 6b (failing tests on branch `review-sync`, commit 59ab0d8) found five defects. These rulings override the Task 4 text above where they differ.

- **V1, a WAL that changes under a scan is moved, not corrupt.** A read on read mark 0 holds no lock that stops an application `wal_checkpoint(TRUNCATE)` (it cuts the file to 0) or a writer's restart in place (it rewrites the header and frame 1). So a scan, a page fill or a snapshot can meet a short or non-verifying frame below the committed end in a healthy database. Rule: when any WAL read below the committed end fails (`WalTruncatedBelowCommitted`, `WalCorruptBelowCommitted`, a truncated frame read during the fill), the step rereads the WAL header before it classifies. Header absent, short or naming other salts than the scanned generation: `WalMoved` (retried; `recover` classifies what happened, including a read lock violation). Header still names the scanned generation: the stop stands, because then the bytes are wrong inside one generation. The reread comes after the failed read, and SQLite writes the new header before any new frame, so a header that still matches cannot be a restart in progress. No condition on which mark the read held: one place (`recover`) decides whether a move was legal. Applies to `sync_step`, `write_snapshot` and the checkpoint protocol's own steps through one helper. This replaces the table row that says truncation below the committed end needs a lock violation.
- **V2, the abandoned-frames check needs a lost commit, not a lost frame.** the abandoned-frames check fires only when the old generation's residue holds a whole committed transaction chained from the cursor (frames that verify from the cursor's checksum, ending in a commit frame). Frames with no commit after them are a rolled-back spill and were never part of the database. `FakeWal` restarts after a rolled-back spill, as SQLite does (frames past `mxFrame` do not block `walRestartLog`).
- **V3, boot compares the whole newest staged file, not its last page.** Rule 9 at Boot re-derives the newest staged file from the WAL frames in the newest staged file's range (the newest version of each page in `[wal_offset, wal_offset + wal_size)` and the commit size) and requires the same page numbers, the same bytes for every page and the same commit size as the staged newest staged file. Any difference is `Snapshot { cause: LocalRewound }`. Go compares less; the replica is unchanged, so this is behaviour, not format.
- **V3 residue, open (Task 4e).** A rewind that drops two or more transactions and regrows with a transaction boundary at the newest staged file's offset and a byte-identical newest staged file transaction still passes: the page compare cannot see that an earlier transaction differs. Only chain identity closes the class, and the LTX header has no field for the WAL's cumulative frame checksum. Recommended design, not built: a local position file in the staging directory (txid, salts, WAL offset, the cumulative checksum pair at the newest staged file), written atomically after each staged file; at Boot a matching checksum at that offset proves every earlier frame; a file behind the newest staged file is closed by V3's compare over the staged files after it; a missing or unreadable file falls back to V3 alone (which is also what a directory inherited from Go has). It is local state, not part of the replica. It waits for the actor (6c) to land and goes to Evan as a Call before it is built.
- **V4, rule 6 does not check `checkpoint_seq`.** The plan's rule 6 is amended, not the code. Real SQLite (found by 6d) writes the restarting writer's own per-connection checkpoint count into the WAL header, so with more than one connection the header's `checkpoint_seq` is not the old value plus one. `salt1 + 1` lives in the shared wal-index header and is the proof. The review's test is replaced by one that asserts a restart with an unrelated `checkpoint_seq` is still `WalRestarted`.
- **V5, `NotRestarted { fully_backfilled }` loses the field.** Nothing reads it.

Noted, not built:
- A pinned position at the committed end whose WAL an application TRUNCATE cut to zero bytes goes to `Snapshot { WalMissing }`. Nothing was lost: while the read-mark-0 read lock is held no frame can be backfilled, so a TRUNCATE can only succeed when nothing was committed after the read lock. Following it (wait for the next header, then rule 6) would save a snapshot for every explicit application TRUNCATE. It matters only for an application that runs its own TRUNCATE checkpoints; decide it with the application integration.
- A database file longer than its header's page count (`SQLITE_FCNTL_CHUNK_SIZE`) gives an empty-WAL snapshot the file's length as its commit size. The restore reproduces the source file byte for byte, padding included; harmless, recorded so nobody is surprised.


### Task 6h: actor follow-ups (branch `actor-followup`)

Found when 6c landed. Rulings:

- **W1, a passed deadline is work, not a wait.** `Tick::next` is never later than the earliest deadline any task holds (retry waits, the checkpoint gate, the maintenance schedule, a level's backoff in `MaintainState`). A deadline at or before now means the next tick runs that task; `OnWalChange` is returned only when no task holds a deadline. Property test over random task states: `next` is at most the minimum deadline, and a tick with a passed deadline does that task's work or reports why not.
- **W2, one source for a wait.** `Event::Retrying` reports the instant the retry is due, read from where the wait is kept (`MaintainState` for maintenance jobs, the task for the rest), not an attempt count kept beside it.
- **W3, staged level-0 files are pruned.** Nothing deleted them, so the staging directory grew without bound. The actor deletes a staged level-0 file when all three hold: the uploader has confirmed it at the replica; its last transaction is at or below the last transaction of the replica's level 1 in the listing the maintenance task already holds (level-1 compaction reads staged inputs first, R3, so a file leaves only after it was compacted); it is not the newest staged file (boot's rule 9 and V3 compare the newest staged file with the WAL). At most `MAX_DELETE` per tick, oldest first, after uploads and before maintenance; a failed delete is `Fault::from_staging` through the policy and blocks nothing else. A staged file that vanishes is not an error here. Tests: a mixed staging directory with exactly who goes and who stays; the newest staged file stays even when compacted; nothing is pruned before the first maintenance listing; a crash between two prunes boots clean.

As built: a throttle's `retry_after` no longer replaces the backoff, as "Semantics" item 2 under the fault policy and the 6g2 table's "wait honoured exactly" read. Every retry wait, on an actor task or a `MaintainState` level, is `fault::backoff_wait`: the later of the doubling backoff and the store's `retry_after`, never shorter than either. The failure count still drives the doubling.


### Task 6i: the simulator's first two findings (branch `sim-findings`)

Found by 6f's fixed seeds (`review_` tests in `crates/sim/tests/sim/fixed_seeds.rs`). Rulings:

- **P1, the page size is fixed for the life of a replica.** SQLite cannot change the page size in WAL mode; it takes leaving WAL mode, a `VACUUM` and coming back. A replica that mixed page sizes could not be compacted or restored by Go, and the replica is the contract. So a database whose page size differs from the replica's is a typed stop at the source, `Stop(PageSizeChanged { was, now })`, raised by boot and by the sync step before anything of the new size is staged, never `RemoteCorrupt` from a later compaction. Everything staged and uploaded before the change stays restorable. The operator's way out is a new replica prefix. Before building, read what Go does in `.upstream`'s source on a page-size change and write it into this block in two lines; if Go follows the change in a way the replica shows, stop and report instead of building the stop. The simulator keeps `vacuum_resizes` on: the model expects this stop, expects no file of the new size in staging or the replica, and checks the last acknowledged state before the change still restores.
- **T1, a staged file that does not verify is lost, not corrupt.** A staged file is derived local state: the database can always produce a snapshot in its place. A power loss can tear a staged file that was not yet uploaded. Today boot sees the unreadable newest staged file, snapshots one transaction higher and leaves the torn file, and the upload task stops on it with `StagedCorrupt`. Rule: a staged level-0 file that fails verification, at boot or when the upload task reads it, is treated exactly as a missing one (`StagedLost`). The file and every staged file after it are deleted from staging (uploads go in order, so none of those is at the replica unless the boot listing shows it); the next file is a snapshot that takes the first transaction the replica does not hold, so the level-0 chain has no hole. If the boot listing shows the replica already holds the torn file's transaction, the local copy is deleted and the snapshot takes the next transaction. `StagedCorrupt` stops being a stop; if nothing else produces it the variant goes. A staged file that verifies but differs from what the replica holds under the same key stays what it is today (R1). First settle, by reading the fs stager and `Stage::power_loss`, whether the torn file was synced before it was treated as staged: if the stager returns before the file and its directory entry are durable, that is its own defect, fixed first in its own commit; if the simulator tears synced files, say so and keep T1 as the handling. Tests: the two replay seeds as named tests; a mixed staging directory (readable, torn in the middle, readable after) with exactly what is deleted and which transaction the snapshot takes, for both "replica holds it" and "replica lacks it"; the upload task meeting a file that turns unreadable mid-run; a crash between the delete and the snapshot boots clean.


### Task 7c: compatibility in CI, against every 0.5 release (branch `compat-ci`)

Asked for by the owner: CI runs the compat harness against as many Go versions as make sense. Rulings:

- **K1, three sets.** *Pinned* (today's five: v0.5.0, v0.5.15, v0.5.16, v0.5.18, 041a8c3): committed fixtures plus live tests, on every push and pull request. *Releases*: every upstream tag `v0.5.N` with no pre-release suffix (19 today), live tests only (Go writes and Rust restores; Rust writes and Go restores; handover both ways), no committed fixtures (the five pinned sets are 5 MB already). *Floating*: upstream `main`'s newest staged file and any release tag newer than the table, so a new upstream release is exercised the night it ships with no change here. Out of scope and said so in the compat crate's `//!`: 0.3.x and 0.4.x (a different replica format; the port reads and writes the 0.5 LTX layout only).
- **K2, one table, no silent skip.** `GoVersion` stays the closed pinned set (fixture paths need static names). Live tests take a `GoBuild { name, era }` instead; `GoVersion` converts into it. `builds_under_test()` returns the pinned five, or the names in `LSR_COMPAT_VERSIONS` (space separated) when set. `Era::{Frames, Blocks}` is derived from the name: `v0.5.N` with N ≤ 15 is `Frames`, anything else (a later tag, a commit, `main`) is `Blocks`; every per-version expectation is a `match` on the era (`REFUSERS` becomes "the Frames era refuses a Rust chain"). A release that differs from its era in a way a test sees goes in one const table (name, closed `Quirk` enum, the reason, the upstream commit or issue if found), and the test asserts the quirk's behaviour instead of skipping. A tag the current Go toolchain cannot build (try the default `GOTOOLCHAIN` first, no source patch) is listed in the same table as unbuildable with the error line. A test over the clone's tags asserts every `v0.5.N` tag is either buildable-and-tested or listed, so nothing drops out by accident.
- **K3, recipes are the interface.** `scripts/build-go-litestream.sh` learns `releases` (every tag of K1) and `main` (always rebuilt, named `litestream-main`; `scripts/fetch-upstream.sh` keeps checking out the read lock, the build reads `origin/main`). New `just compat-live <names...>` builds those and runs the live tests with `LSR_COMPAT_VERSIONS` set; `just compat-releases` is `compat-live` over `releases`. `just compat` is unchanged. Workflow steps call only `just` recipes and the two scripts, so a local run is the CI run.
- **K4, workflows.** `ci.yml` gains a `compat` job beside `check`: `actions/setup-go` (`stable`), `just upstream`, cache `.upstream/bin` keyed on runner OS, the build script's hash and the version names, `just compat`. New `compat-releases.yml`, on a nightly cron and `workflow_dispatch`, never on push: a first job resolves the list (the release tags from `git ls-remote --tags` plus `main`) into a matrix, `fail-fast: false`, one job per name running `just compat-live <name>`; a last job fails naming any upstream tag the table does not list. Both keep `permissions: contents: read`, no secrets, no write step. A failure on `main` or on an unlisted tag is upstream moving, and never blocks a push here because it is a separate workflow.
- **K5, also in `ci.yml`, its own commit:** a `test-s3` job running `just test-s3` (MinIO in Docker, the public local default credentials already in the recipe; nothing else).
- **K6, verification without a remote.** No remote exists and none is added. Lint both workflows with actionlint (the `rhysd/actionlint` Docker image is fine). Run `just compat-releases` locally and report per-version wall time and the total, so the nightly's cost in runner minutes is a known number. Run `just compat` once inside a Linux container (official `rust` image plus Go) to catch what macOS hides, timing margins above all; give it 20 minutes, and report "not verified on Linux" rather than burn more. A live test that proves timing-sensitive on a slow machine gets its wait made event-driven, not a longer sleep.

Task 6i, amended when 6f landed (`4b96b9a`): after the rebase onto 6h and 4d, a resizing vacuum no longer stops (200 seeds pass, `vacuum_resizes` is on by default), and nobody has said why. P1 therefore starts as a question, not a build: find which main commit changed the outcome and what the engine does now on a page-size change (which `SnapshotCause`, what the replica holds on each side of the change, whether a level 1 to 9 compaction can ever take inputs of two page sizes, what restore does to a target on each side). Read what Go does. Then: if the engine snapshots at the new size, never compacts across the change and Go restores the result, P1 is withdrawn and replaced by tests that read lock exactly that (a compaction window straddling the change; a restore to each side; Go restoring the Rust replica, in crates/compat, `#[ignore]`d behind `just compat`). If any of those does not hold, stop and report with the failing case; do not build the stop without a ruling. The torn-file replays are now `just sim-replay 0x25 44`, `0x2d 256` and `0x46 252`; 0x46 reaches `StagedCorrupt` through `LocalRewound` with no torn file in its last crash, so establish its cause separately before folding it into T1. When T1 lands, the soak stops ignoring `StagedCorrupt` and seeds 37 and 45 return to the fixed set.

Task 6i, P1 ruled after the investigation (`c7c9325`): the stop stands. Facts found: the simulator never achieved a resize (the engine's read lock makes SQLite refuse to leave WAL mode, 0 of 620 attempts); with the engine stopped a resize goes through, boot snapshots with `SnapshotCause::PageSizeChanged`, and from then on compaction meets inputs of two page sizes (`InputFault::PageSizeChanged` mapped to a `RemoteCorrupt` stop) and restore across the change fails or, on three seeds, returned wrong pages. Go does not follow a change either: it reads the page size once at init and its ltx compactor rejects mixed sizes. Rule:
- **P1a, one page size per replica prefix.** Boot compares the database's page size with the replica's (the newest file the boot listing or staging holds; an empty replica and empty staging accept any size). A difference is `Stop(PageSizeChanged { was, now })` before anything is staged or published. `SnapshotCause::PageSizeChanged` is deleted. The sync step's existing page-size check stays as the defence for a change the read lock should have made impossible, and raises the same stop. The operator's way out is a new prefix; the old one keeps every state up to the change. Call: stop, not follow (following needs a level-9 snapshot published at the change and compaction windows split at a boundary the keys do not show, for an event SQLite makes rare and Go cannot restore either; cost if wrong: an application that resizes must repoint its replica by hand. The invariant can be relaxed later; the reverse cannot).
- **P1b, wrong pages first.** Before building the stop, establish on the scratch tree why seeds 13, 30 and 38 restored wrong pages. If restore can emit a database from a listing whose files disagree on page size without a typed refusal, that is a restore defect whatever the writer does (a Go-written replica can hold such a listing): fix it at the root, test first, its own commit, with the listing as a unit test. If it was the model miscounting after a resize, say so.
- **P1c, the simulator models SQLite.** A resize succeeds only while the engine is down and is refused while it runs; a real-SQLite test pins that refusal with the engine's connection open. After a resize the model expects the stop at the next boot, no file of the new size in staging or at the replica, and every state acknowledged before the change still restoring. The wrong doc comment on `vacuum_resizes` goes.

Task 6i, P1 as built: P1b found no restore defect; the wrong pages were a scratch-patch artifact (a refused resize still cleared the runner's spilled frames, so the model lost a rollback). P1a stops at boot and in the step as ruled. P1c: the runner's resize stops the engine (a process crash), ends the other processes' readers, vacuums and boots again; with the app's writer held the vacuum is refused. `sqlite_rules::our_reader_blocks_leaving_wal_mode` pins against SQLite that an open read transaction keeps the database in WAL mode. The runner expects `PageSizeChanged` only when the database's size differs from the one page size every staged and remote file carries, and a second size in those files is a harness violation. The scenario `vacuum_page_size_change` asserts the stop and that every old state restores. A soak found a separate defect: before the boot, `ready_work` counted the upload task, so a boot failing on a retryable store fault spun `Next::Now` for its backoff (fixed, `a_failed_boot_waits_without_spinning`).

Task 6i, T1 as built: `Fault::StagedCorrupt` and `StopCause::{StagedCorrupt, StagedLost}` are gone. A staged level-0 file that is missing, does not decode, names another key, or carries a bad created-at time is `Fault::StagedLost`, whose policy is a doubling retry. At boot, `upload::sweep_staged` verifies every staged file from the replica's newest transaction on; the first that fails is deleted with every staged file after it (`drop_staged_from`, newest first, so a crash part way leaves a prefix), and the first sync is a `NewestStagedFileUnreadable` snapshot after `max(position, staged, confirmed)`: the first transaction the replica lacks, or the one after the torn file when the replica holds it. The boot listing's `tail_is_sound` judges the replica's object alone when the staged copy differs and does not verify. In the upload task, a `StagedLost` on the file it was about to upload deletes from that file on, resets the position and resnapshots at that transaction (`Event::Resnapshot`). The soak no longer filters `StagedCorrupt`, seeds 37 and 45 are back in the fixed set, the replays are named tests, and `newest_staged_file_byte_flip`/`newest_staged_file_truncated` expect no stop and a snapshot at transaction 3.


Task 6g2, amended before building (main at 6i): the table above predates several rulings. As built, and binding:
- The table is derived from the enum, not this page: `FaultKind` is exhaustive over today's `Fault` (which now has `UploadChecksumMismatch` and `StoreRefused` and no `StagedCorrupt`), and `every_fault_has_a_scenario` fails to compile or fails naming the variant when one lacks a scenario, as `every_cause_has_a_scenario` does for snapshot causes.
- `StagedLost` (T1): `staged_byte_flip_before_upload` and `meta_dir_deleted_mid_run` both expect a retry, the torn or missing file and every later staged file gone, and a snapshot at the first transaction the replica lacks; no stop.
- `PageSizeChanged` (P1a) is a stop; 6g1's `vacuum_page_size_change` already expects it and counts as its scenario.
- Throttling: one rule on every task, `fault::backoff_wait` = the later of the doubling backoff and `retry_after`; `store_429_retry_after` asserts `Event::Retrying { fault, until }` with `until` equal to that, and no remote call before it. Not "honoured exactly".
- `UploadChecksumMismatch`: `etag_mismatch_on_put` (the store deletes the key, the uploader writes the same key again without listing, the replica ends with the right bytes). `StoreRefused`: one scenario for its policy as `fault.rs` documents it.
- W1 to W3, V1 to V5 and R1 to R5 stand as built; a scenario that contradicts one of them is a finding against the scenario first.
- **The level 1 to 9 crash window:** build the scenario this block's "Open" note asks for, at level 1 and at level 9: a compaction publishes an object whose bytes are wrong and whose size is right, power is lost before the delete, the actor boots. Expected by the oracle: no restore ever returns the wrong bytes, and the level is repaired. If the scenario fails (boot accepts the object by size), commit it as an `#[ignore]`d `review_` test with a replay line and report it first with: which restores the oracle saw go wrong, whether a multipart upload can produce the window at all (parts are ETag-checked before the commit, so the suspicion is single PUTs only), and what a boot read-back of the newest file per level would cost in GETs and bytes. Do not build the read-back here; it is ruled after the evidence.

Task 6g2, the level 1 to 9 crash window, ruled after the evidence: the boot reads back, option B. Evidence, on the scenario as first built: every restore the oracle tried (transactions 1 to 3 and Latest) refused with a typed checksum error on the damaged file and none returned wrong pages, so the old stop was safe; the level-0 and level-1 files the damaged output was built from were all still listed; and nothing but an operator could have made the replica restorable again. The scenario named for level 1 damaged a level 2 output (level 1 was inside its interval, so the next upload was level 2's), and the stop came from the level 3 compaction reading that output as an input. A single PUT is the window: its ETag is compared after the object is visible. A multipart upload checks every part's ETag before the commit and so cannot publish wrong bytes it was told about; its completed ETag is checked for shape only, after the object is visible. Rule, as built (`crates/core/src/maintain/audit.rs`):
- The first maintenance listing after a boot reads back the newest file of every level above 0 through the LTX decoder (whole file, checksum, and the header's range against the key). Level 0 keeps R1's read-back.
- A file that does not verify is deleted when a restore to its last transaction still plans from the listing's other files (`restores_without`, the restore planner itself), announced as `MaintainedEvent::Repaired`. Compaction then builds the level again from the same inputs; a missing snapshot is due at once. Otherwise the file stays and the engine stops with `RemoteCorrupt`.
- A failed read is the maintenance task's retry and judges nothing. Sync and upload do not wait for the read-back.
- Cost: once per boot and never in steady state, no extra listing, one ranged GET per 256 KiB of each newest file (at least one per file), and the bytes of those files. With Go's default levels that is the newest of levels 1, 2, 3 and the newest snapshot, so a boot reads about one compressed copy of the database.
- Scenarios: `corrupt_l1_publish_then_power_loss` now damages the first level 1 output, `corrupt_l2_publish_then_power_loss` is the scenario as first built; both expect the repair, the rebuilt level and the oracle.

Task 6f, the nightly rotating set as built: `.github/workflows/sim-nightly.yml` runs `just sim-nightly` on a nightly cron and on demand, never on push, with `permissions: contents: read` and no secrets. The seeds follow from the UTC date: `lsr_sim::nightly_base("YYYYMMDD")` is the date as a number times a million, so no two nights share a seed and `just sim-nightly <seconds> <YYYYMMDD>` reruns a night. The soak prints every failure again at the end of the log with the line that replays it. A failing seed is filed as an ignored `review_` test with that line and joins the fixed set once fixed.
